"""Core-region geometry for the centroid policy (spec §8.1).

This is not a tiler. Dask does the tiling; `dask.array.map_overlap` hands each worker a
block made of a core plus a halo of neighbouring pixels. What Dask does *not* do is
reconcile instance labels across blocks: each block relabels from 1, so the same cell
straddling a boundary is a different object in two blocks.

The adopted reconciliation rule is that an object belongs to the block whose **core**
contains its centroid; objects whose centroid falls in the halo belong to the neighbour
and are dropped. No post-hoc deduplication, no double counting. This module supplies the
geometry that rule needs: where each core sits, how much halo each block actually got,
and therefore how to lift a block-local centroid into full-image coordinates.
"""

from dataclasses import dataclass

Span = tuple[int, int]


@dataclass(frozen=True, slots=True)
class CoreRegion:
    """One core rectangle and the halo its block carries around it.

    Coordinates are half-open in full-image space: the core owns rows ``y0..y1-1``. The
    padding is what the halo actually amounts to *after* clipping at the image border,
    which is why it cannot be recomputed from `overlap` alone.
    """

    y0: int
    x0: int
    y1: int
    x1: int
    pad_top: int
    pad_left: int
    pad_bottom: int
    pad_right: int

    @property
    def block(self) -> tuple[int, int, int, int]:
        """The full block Dask hands the worker, core plus halo, in image coordinates."""
        return (
            self.y0 - self.pad_top,
            self.x0 - self.pad_left,
            self.y1 + self.pad_bottom,
            self.x1 + self.pad_right,
        )

    @property
    def origin(self) -> tuple[int, int]:
        """Image coordinates of the block's top-left pixel — the local frame's zero."""
        return self.y0 - self.pad_top, self.x0 - self.pad_left

    def owns(self, centroid_y: float, centroid_x: float) -> bool:
        """Whether an object at this full-image centroid belongs to this core."""
        return self.y0 <= centroid_y < self.y1 and self.x0 <= centroid_x < self.x1

    def to_global(self, local_y: float, local_x: float) -> tuple[float, float]:
        """Lift a block-local coordinate into full-image coordinates.

        `regionprops` measures centroids inside the padded block, so every centroid must
        pass through here before it is compared against a core or written to Parquet.
        """
        origin_y, origin_x = self.origin
        return local_y + origin_y, local_x + origin_x


def core_regions(h: int, w: int, chunk: int, overlap: int) -> list[CoreRegion]:
    """Enumerate the core rectangles of an ``h x w`` image, row-major.

    The cores tile the image exactly: every pixel belongs to one and only one of them.
    That invariant is what makes the centroid policy sound, and it is tested as a
    hypothesis property rather than with hand-picked examples.

    Args:
        h: Image height in pixels.
        w: Image width in pixels.
        chunk: Nominal core side. A trailing remainder shorter than `overlap` is merged
            into its predecessor — see `_spans`.
        overlap: Halo depth on each side, in pixels. Zero means no halo.

    Raises:
        ValueError: If the image or chunk is empty, if `overlap` is negative, or if
            `overlap` is deeper than the smallest resulting core.
    """
    if h <= 0 or w <= 0:
        raise ValueError(f"image must be non-empty, got {h}x{w}")
    if chunk <= 0:
        raise ValueError(f"chunk must be positive, got {chunk}")
    if overlap < 0:
        raise ValueError(f"overlap must be non-negative, got {overlap}")

    rows = _spans(h, chunk, overlap)
    cols = _spans(w, chunk, overlap)
    _reject_halo_deeper_than_smallest_core(rows + cols, overlap)

    return [
        CoreRegion(
            y0=y0,
            x0=x0,
            y1=y1,
            x1=x1,
            pad_top=min(overlap, y0),
            pad_left=min(overlap, x0),
            pad_bottom=min(overlap, h - y1),
            pad_right=min(overlap, w - x1),
        )
        for y0, y1 in rows
        for x0, x1 in cols
    ]


def _spans(length: int, chunk: int, overlap: int) -> list[Span]:
    """Split one axis into half-open core spans.

    The plain grid leaves whatever remainder the division produces, and a short remainder
    is a real failure mode rather than a cosmetic one: `map_overlap` refuses a depth
    larger than its smallest chunk, so 1024 pixels at chunk 500 (spans of 500, 500, 24)
    cannot carry a 32-pixel halo at all. Merging the short tail into its predecessor
    costs one branch and makes every `overlap < chunk` configuration valid.
    """
    spans: list[Span] = [(start, min(start + chunk, length)) for start in range(0, length, chunk)]
    tail_start, tail_end = spans[-1]
    if len(spans) > 1 and tail_end - tail_start < overlap:
        previous_start = spans[-2][0]
        return [*spans[:-2], (previous_start, length)]
    return spans


def _reject_halo_deeper_than_smallest_core(spans: list[Span], overlap: int) -> None:
    smallest = min(end - start for start, end in spans)
    if overlap > smallest:
        raise ValueError(
            f"overlap {overlap} is deeper than the smallest core ({smallest}); "
            "dask.array.map_overlap rejects a depth larger than its smallest chunk"
        )
