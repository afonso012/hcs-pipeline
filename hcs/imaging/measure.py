"""Turning one block's labels into rows, under the centroid policy (spec §8.1, §8.2).

This is where the reconciliation rule is actually applied. Dask hands a worker a core plus
a halo; the same cell straddling a boundary is segmented twice, once in each of the two
blocks that can see it. The rule is that it belongs to the block whose **core** contains
its centroid, so each block keeps what it owns and drops the rest. No deduplication pass,
no double counting, and no block ever has to be compared against another block's output.

Measurement happens *before* the filter, and on the padded block, on purpose. A cell whose
centroid sits just inside this core may extend well into the halo, and measuring it on the
trimmed core would report a nucleus sliced in half. The halo exists precisely so the owner
sees the whole object.

Note what this module never receives: the image. It sees one block, and the rows it emits
are the only thing that outlives that block.
"""

from collections.abc import Mapping
from typing import Final

import numpy as np
import numpy.typing as npt
import pandas as pd
from skimage.measure import regionprops_table

from hcs.geometry import CoreRegion
from hcs.imaging.segment import LabelArray

# Shape descriptors, measured once per object however many channels there are.
_SHAPE_PROPERTIES: Final = (
    "label",
    "centroid",
    "bbox",
    "area",
    "perimeter",
    "eccentricity",
    "solidity",
)

# What §9 aggregates per well, and what a `touches_border` object is excluded from.
MORPHOLOGY_COLUMNS: Final = ("area", "perimeter", "eccentricity", "solidity")

COLUMNS: Final = ("centroid_y", "centroid_x", *MORPHOLOGY_COLUMNS, "touches_border")


def measure_block(
    labels: LabelArray,
    intensities: Mapping[str, npt.NDArray[np.floating | np.integer]],
    region: CoreRegion,
    image_shape: tuple[int, int],
) -> pd.DataFrame:
    """Measure the objects this block owns, in full-image coordinates.

    Args:
        labels: Labels for the padded block, as `segment_chunk` returned them.
        intensities: The block's pixels, one array per channel, same shape as `labels`.
            Each is measured inside the mask the DNA channel produced (§8.2).
        region: The core this block is responsible for, and the halo it carries.
        image_shape: The full image, needed only to tell an object clipped by the image
            border from one clipped by a block boundary.

    Returns:
        One row per owned object. Empty but fully columned when the block owns nothing, so
        that assembling a field never has to special-case a quiet corner of the plate.
    """
    channels = tuple(intensities)
    if not labels.any():
        return _empty(channels)

    frame = pd.DataFrame(regionprops_table(labels, properties=_SHAPE_PROPERTIES)).rename(
        columns={"centroid-0": "centroid_y", "centroid-1": "centroid_x"}
    )
    frame["centroid_y"], frame["centroid_x"] = _to_global(frame, region)

    owned = frame[
        [region.owns(y, x) for y, x in zip(frame["centroid_y"], frame["centroid_x"], strict=True)]
    ]
    if owned.empty:
        return _empty(channels)

    owned = owned.assign(touches_border=_touches_border(owned, labels.shape, region, image_shape))
    for channel, pixels in intensities.items():
        owned = _add_intensity(owned, labels, pixels, channel)

    return owned[[*COLUMNS, *_intensity_columns(channels)]].reset_index(drop=True)


def _to_global(frame: pd.DataFrame, region: CoreRegion) -> tuple[pd.Series, pd.Series]:
    """Lift block-local centroids into image coordinates.

    Every centroid passes through here before it meets `owns`, which is stated in image
    coordinates. Comparing a local centroid against it would make every block in the grid
    behave like the top-left one.
    """
    origin_y, origin_x = region.origin
    return frame["centroid_y"] + origin_y, frame["centroid_x"] + origin_x


def _touches_border(
    frame: pd.DataFrame,
    block_shape: tuple[int, ...],
    region: CoreRegion,
    image_shape: tuple[int, int],
) -> npt.NDArray[np.bool_]:
    """Mark objects clipped by the outer edge of the whole image (§8.1).

    They still count as cells — a nucleus half out of frame is a nucleus — but their
    morphology describes the crop rather than the object, so §9 keeps them in the count
    and out of the shape statistics.

    The distinction that matters is between the image border and a block boundary. An
    object at a block boundary is whole and the halo shows the rest of it. Only the image
    has nothing beyond it.
    """
    origin_y, origin_x = region.origin
    block_h, block_w = block_shape[0], block_shape[1]
    height, width = image_shape

    # Compare each side only where this block actually sits against that image edge.
    clipped = np.zeros(len(frame), dtype=bool)
    if origin_y == 0:
        clipped |= frame["bbox-0"].to_numpy() == 0
    if origin_x == 0:
        clipped |= frame["bbox-1"].to_numpy() == 0
    if origin_y + block_h == height:
        clipped |= frame["bbox-2"].to_numpy() == block_h
    if origin_x + block_w == width:
        clipped |= frame["bbox-3"].to_numpy() == block_w
    return clipped


def _add_intensity(
    frame: pd.DataFrame,
    labels: LabelArray,
    pixels: npt.NDArray[np.floating | np.integer],
    channel: str,
) -> pd.DataFrame:
    """Mean and integrated intensity of one channel, inside the DNA mask.

    Integrated intensity is mean times area rather than a second pass over the pixels: the
    two are the same number. Both are kept because they answer different questions — total
    protein content is what the biology reads, and the mean is what survives a change in
    cell size.
    """
    table = regionprops_table(
        labels, intensity_image=pixels, properties=("label", "intensity_mean")
    )
    means = pd.DataFrame(table).set_index("label")["intensity_mean"]
    aligned = frame["label"].map(means)
    return frame.assign(
        **{
            f"intensity_mean_{channel}": aligned,
            f"intensity_integrated_{channel}": aligned * frame["area"],
        }
    )


def _intensity_columns(channels: tuple[str, ...]) -> list[str]:
    return [
        f"{prefix}_{channel}"
        for channel in channels
        for prefix in ("intensity_mean", "intensity_integrated")
    ]


def _empty(channels: tuple[str, ...]) -> pd.DataFrame:
    """An empty frame carrying the columns a populated one would have.

    A field's rows are assembled by concatenating blocks, and a column that appeared only
    when a block happened to contain cells would make the Parquet schema depend on the
    data.
    """
    names = [*COLUMNS, *_intensity_columns(channels)]
    frame = pd.DataFrame({name: pd.Series(dtype="float64") for name in names})
    return frame.astype({"touches_border": "bool"})
