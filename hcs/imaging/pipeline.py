"""Driving the blocks of one image (spec §8.1).

`core_regions` says where the blocks are; this walks them, materialises one at a time,
segments it, and keeps the objects it owns. Peak memory is one block of pixels plus one
block of labels, whatever the image measures — which is the flat RSS curve the WSI rail is
supposed to produce, and the linear aggregate the HCS rail is supposed to produce as the
worker count rises.

**Why a loop and not `map_overlap`.** Two reasons, both consequences of the block function
emitting rows instead of an array:

- `map_overlap` exists to add a halo, apply a function and trim the halo off the result.
  Its result here is a DataFrame, which has nothing to trim, so all that is left of it is
  the halo arithmetic — and that arithmetic is `core_regions`, which §8.1 already declares
  the authority for the centroid policy. Letting Dask derive a second, independent grid
  and then reconciling the two is the bug this module exists to avoid.
- Dask's threaded scheduler would multiply memory by its thread count *inside* a Celery
  worker that already has a memory limit, making the RSS measurement of §21 a measurement
  of two schedulers interacting. The unit of parallelism here is the work unit, not the
  block.

Dask still does the work that matters: the array handed in is lazy, so slicing a block
reads that block's bytes and no others. That laziness is where §8.1's memory argument
actually comes from.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
import numpy.typing as npt
import pandas as pd
from skimage.filters import gaussian, threshold_otsu

from hcs.geometry import CoreRegion, core_regions
from hcs.imaging.measure import measure_block
from hcs.imaging.qc import FieldQc, check_field
from hcs.imaging.segment import SegmentationConfig, segment_chunk
from hcs.metrics import CHUNKS_PROCESSED


class Sliceable(Protocol):
    """What this module needs of an image: a shape, and slicing that reads lazily.

    Both `dask.array.Array` and `numpy.ndarray` satisfy it, which is why the tests can use
    the second and production the first with no seam between them.
    """

    @property
    def shape(self) -> tuple[int, ...]: ...

    def __getitem__(self, key: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class ChunkConfig:
    """Block geometry. Hashed into the run by §12, because it can move the numbers."""

    chunk: int = 1024
    # Must exceed the diameter of the largest expected cell. Too small and an object
    # centred just inside a core is measured with its far side cut off by the halo edge —
    # the policy keeps it, but keeps a truncated version of it.
    overlap: int = 32


def measure_field(
    segmentation: Sliceable,
    intensities: Mapping[str, Sliceable],
    chunks: ChunkConfig,
    segmentation_config: SegmentationConfig | None = None,
    threshold: float | None = None,
) -> pd.DataFrame:
    """Segment and measure one image, block by block.

    Args:
        segmentation: The channel objects are found in — the DNA channel for HCS (§8.2).
        intensities: The channels measured inside that mask, including the segmentation
            channel itself if its intensity is wanted. Every array must match
            `segmentation` in shape.
        chunks: Block geometry.
        segmentation_config: Knobs of the classic path; defaults if omitted.
        threshold: The foreground level, taken from the whole image. Computed here when
            omitted. The WSI rail passes one derived from a low-resolution pyramid level
            instead, which is how a slide gets a global threshold without reading level 0.

    Returns:
        One row per cell, in full-image coordinates, ordered by block and then by the
        order `regionprops` found them in — deterministic, which the golden batch needs.
    """
    height, width = segmentation.shape[0], segmentation.shape[1]
    _reject_mismatched_channels(intensities, (height, width))
    seg_cfg = segmentation_config or SegmentationConfig()
    level = global_threshold(segmentation, seg_cfg) if threshold is None else threshold

    # §8.2: QC before the computation, not after. A field where everything died has no
    # nuclei for the threshold to have found, and segmenting it anyway is how one dead
    # well produced 2166 objects and inverted a dose-response curve.
    plane = _read(segmentation, 0, 0, height, width)
    qc = check_field(plane, level)
    # The x-axis of §14.2's anchor panel: decoded bytes of every channel this unit read.
    # Carried on the frame rather than returned, because the worker is the only caller that
    # wants it and the scripts would have to thread it through for nothing.
    input_bytes = plane.nbytes * max(1, len(intensities))
    if not qc.accepted:
        return _attach(_empty_like(intensities, qc), qc, input_bytes)

    regions = core_regions(height, width, chunks.chunk, chunks.overlap)
    frames = [
        _measure_one(region, segmentation, intensities, (height, width), seg_cfg, level)
        for region in regions
    ]
    CHUNKS_PROCESSED.inc(len(regions))
    return _attach(_number(pd.concat(frames, ignore_index=True)), qc, input_bytes)


def _empty_like(intensities: Mapping[str, Sliceable], qc: FieldQc) -> pd.DataFrame:
    """No rows, but the full schema and the verdict that explains why.

    A rejected field is not a failed one. It produced a real answer — this well is empty —
    and the caller needs to be able to tell that from a unit that never ran (§8.2).
    """
    from hcs.imaging.measure import empty_frame

    return empty_frame(tuple(intensities)).assign(label=pd.Series(dtype="int32"))


def _attach(frame: pd.DataFrame, qc: FieldQc, input_bytes: int = 0) -> pd.DataFrame:
    """Carry the field-level facts on the frame, out of band of the rows.

    Attributes rather than columns: they describe the field, and repeating them on each of
    a thousand cells would put them in the Parquet schema where §6.3 does not want them.

    **Scalars, not objects.** `to_parquet` JSON-encodes `attrs` into the file's metadata, so
    a dataclass here raises `TypeError` at write time — which is exactly how 480 units in a
    row failed while 200 tests stayed green: the state-machine tests stub the imaging out,
    and the imaging tests never write a file.
    """
    frame.attrs["qc_accepted"] = qc.accepted
    frame.attrs["qc_foreground_fraction"] = qc.foreground_fraction
    frame.attrs["input_bytes"] = input_bytes
    return frame


def _number(frame: pd.DataFrame) -> pd.DataFrame:
    """Give each cell the `label` §6.3 requires: unique within `(work_unit_id, run_id)`.

    It cannot come from the segmenter. Every block numbers its own objects from 1, so the
    labels only become unique once the blocks are side by side — which is here. Numbering
    follows block order, so the same image and the same chunking always produce the same
    labels, and a golden-batch diff points at a cell rather than at a permutation.
    """
    return frame.assign(label=np.arange(1, len(frame) + 1, dtype=np.int32))


def global_threshold(segmentation: Sliceable, cfg: SegmentationConfig | None = None) -> float:
    """The foreground level, computed once over the whole image.

    Smoothed first, with the same sigma the blocks use. A threshold has to be computed on
    the signal it will be compared against: the raw image of a synthetic disc is a
    two-spike histogram where Otsu lands just above zero, and comparing the *smoothed*
    pixels against that level swallows the entire blur halo — a disc of intensity 1000
    came back with a mean of 655. Real images are noisy enough that the two agree closely,
    which is exactly why this would have gone unnoticed.

    This exists because the obvious alternative is badly wrong. Otsu per block splits
    whatever it is given into two classes, and a block containing only background gets
    split down the middle of the noise — on `Week1_22161 D03 s2`, two empty blocks out of
    twenty invented 368 objects where the image held 77 cells. A per-block threshold does
    not make the count noisy; it makes it meaningless on any field with an empty corner,
    which is most of a plate under a cytotoxic compound.

    It reads the whole array, and on this rail that costs nothing: a BBBC021 channel is
    2.5 MB. A slide is not read this way — the WSI rail computes its level from a
    low-resolution pyramid page and hands it to `measure_field`, which is what every
    whole-slide pipeline does and is why the pyramid exists.
    """
    sigma = (cfg or SegmentationConfig()).sigma
    return float(
        threshold_otsu(gaussian(np.asarray(segmentation), sigma=sigma, preserve_range=True))
    )


def _measure_one(
    region: CoreRegion,
    segmentation: Sliceable,
    intensities: Mapping[str, Sliceable],
    image_shape: tuple[int, int],
    seg_cfg: SegmentationConfig,
    threshold: float,
) -> pd.DataFrame:
    """One block: read it, segment it, keep what this core owns.

    Everything allocated here goes out of scope with the frame it returns. That is the
    whole memory argument, and it is why the block is read inside the loop rather than
    gathered up front.
    """
    y0, x0, y1, x1 = region.block
    block = _read(segmentation, y0, x0, y1, x1)
    labels = segment_chunk(block, seg_cfg, threshold)
    pixels = {name: _read(array, y0, x0, y1, x1) for name, array in intensities.items()}
    return measure_block(labels, pixels, region, image_shape)


def _read(array: Sliceable, y0: int, x0: int, y1: int, x1: int) -> npt.NDArray[Any]:
    """Materialise one rectangle. For a Dask array this is where the file is touched."""
    return np.asarray(array[y0:y1, x0:x1])


def _reject_mismatched_channels(
    intensities: Mapping[str, Sliceable], shape: tuple[int, int]
) -> None:
    """A channel of a different size is a mis-paired file, not something to broadcast.

    Measuring channel B inside a mask derived from channel A only means anything if the
    two are the same field. Letting the shapes differ silently would produce rows that
    look ordinary and describe nothing.
    """
    wrong = {
        name: tuple(array.shape)
        for name, array in intensities.items()
        if tuple(array.shape[:2]) != shape
    }
    if wrong:
        raise ValueError(f"channels do not match the segmentation channel {shape}: {wrong}")
