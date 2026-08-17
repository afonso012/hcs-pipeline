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

from hcs.geometry import CoreRegion, core_regions
from hcs.imaging.measure import measure_block
from hcs.imaging.segment import SegmentationConfig, segment_chunk


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
) -> pd.DataFrame:
    """Segment and measure one image, block by block.

    Args:
        segmentation: The channel objects are found in — the DNA channel for HCS (§8.2).
        intensities: The channels measured inside that mask, including the segmentation
            channel itself if its intensity is wanted. Every array must match
            `segmentation` in shape.
        chunks: Block geometry.
        segmentation_config: Knobs of the classic path; defaults if omitted.

    Returns:
        One row per cell, in full-image coordinates, ordered by block and then by the
        order `regionprops` found them in — deterministic, which the golden batch needs.
    """
    height, width = segmentation.shape[0], segmentation.shape[1]
    _reject_mismatched_channels(intensities, (height, width))
    seg_cfg = segmentation_config or SegmentationConfig()

    frames = [
        _measure_one(region, segmentation, intensities, (height, width), seg_cfg)
        for region in core_regions(height, width, chunks.chunk, chunks.overlap)
    ]
    return pd.concat(frames, ignore_index=True)


def _measure_one(
    region: CoreRegion,
    segmentation: Sliceable,
    intensities: Mapping[str, Sliceable],
    image_shape: tuple[int, int],
    seg_cfg: SegmentationConfig,
) -> pd.DataFrame:
    """One block: read it, segment it, keep what this core owns.

    Everything allocated here goes out of scope with the frame it returns. That is the
    whole memory argument, and it is why the block is read inside the loop rather than
    gathered up front.
    """
    y0, x0, y1, x1 = region.block
    block = _read(segmentation, y0, x0, y1, x1)
    labels = segment_chunk(block, seg_cfg)
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
