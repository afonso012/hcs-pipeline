"""Properties of the core-region geometry behind the centroid policy (spec §8.1, §13.1).

Instance segmentation is not a neighbourhood function: each Dask block relabels from 1,
so the same cell on a boundary is labelled differently in two blocks. The adopted policy
is that an object belongs to the block whose *core* contains its centroid. That policy is
only correct if the cores tile the image exactly — no holes, no overlaps — which is a
geometric invariant, and therefore tested with hypothesis instead of hand-picked examples.
"""

import numpy as np
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hcs.geometry import CoreRegion, core_regions


@st.composite
def grids(draw, max_side: int = 4096):
    """Valid (h, w, chunk, overlap) tuples.

    `overlap` is bounded by the smallest resulting core because Dask refuses a halo
    deeper than its smallest chunk. Drawing it inside the valid range keeps the
    coverage property about geometry rather than about input validation.
    """
    h = draw(st.integers(min_value=64, max_value=max_side))
    w = draw(st.integers(min_value=64, max_value=max_side))
    chunk = draw(st.integers(min_value=32, max_value=512))
    overlap = draw(st.integers(min_value=0, max_value=min(chunk, h, w)))
    return h, w, chunk, overlap


# The 1024x32 corner produces 32x32 regions per example, so the pixel-level coverage
# check is deliberately run on fewer examples with no per-example deadline.
slow_property = settings(
    max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)


class TestExactTiling:
    @slow_property
    @given(grids(max_side=1024))
    def test_every_pixel_belongs_to_exactly_one_core(self, grid):
        """No holes, no overlaps. The entire boundary bug class, in one assertion."""
        h, w, chunk, overlap = grid
        coverage = np.zeros((h, w), dtype=np.uint8)
        for region in core_regions(h, w, chunk, overlap):
            coverage[region.y0 : region.y1, region.x0 : region.x1] += 1
        assert coverage.min() == 1
        assert coverage.max() == 1

    @given(grids())
    def test_core_areas_sum_to_the_image_area(self, grid):
        """The cheap counterpart of the pixel test, run over the full size range."""
        h, w, chunk, overlap = grid
        total = sum((r.y1 - r.y0) * (r.x1 - r.x0) for r in core_regions(h, w, chunk, overlap))
        assert total == h * w

    @given(grids())
    def test_cores_stay_inside_the_image_and_are_non_empty(self, grid):
        h, w, chunk, overlap = grid
        for r in core_regions(h, w, chunk, overlap):
            assert 0 <= r.y0 < r.y1 <= h
            assert 0 <= r.x0 < r.x1 <= w


class TestDaskPrecondition:
    @given(grids())
    def test_no_core_is_smaller_than_the_overlap(self, grid):
        """`dask.array.map_overlap` rejects a depth larger than its smallest chunk.

        A naive grid violates this whenever the trailing remainder is short: 1024 with
        chunk 500 leaves a 24-pixel tail that no 32-pixel halo can fit. The tail is
        merged into its predecessor precisely so this holds for every valid input.
        """
        h, w, chunk, overlap = grid
        for r in core_regions(h, w, chunk, overlap):
            assert r.y1 - r.y0 >= overlap
            assert r.x1 - r.x0 >= overlap

    @given(grids())
    def test_padding_never_exceeds_the_overlap_nor_leaves_the_image(self, grid):
        h, w, chunk, overlap = grid
        for r in core_regions(h, w, chunk, overlap):
            assert 0 <= r.pad_top <= overlap
            assert 0 <= r.pad_left <= overlap
            assert 0 <= r.pad_bottom <= overlap
            assert 0 <= r.pad_right <= overlap
            by0, bx0, by1, bx1 = r.block
            assert 0 <= by0 <= r.y0 < r.y1 <= by1 <= h
            assert 0 <= bx0 <= r.x0 < r.x1 <= bx1 <= w

    @given(grids())
    def test_interior_cores_get_the_full_halo(self, grid):
        """Padding is clipped only at the image border, never in the interior."""
        h, w, chunk, overlap = grid
        for r in core_regions(h, w, chunk, overlap):
            assert r.pad_top == min(overlap, r.y0)
            assert r.pad_left == min(overlap, r.x0)
            assert r.pad_bottom == min(overlap, h - r.y1)
            assert r.pad_right == min(overlap, w - r.x1)


class TestCentroidOwnership:
    @given(grids(max_side=512), st.floats(0, 1), st.floats(0, 1))
    def test_exactly_one_core_owns_any_point(self, grid, fy, fx):
        h, w, chunk, overlap = grid
        cy, cx = fy * (h - 1), fx * (w - 1)
        owners = [r for r in core_regions(h, w, chunk, overlap) if r.owns(cy, cx)]
        assert len(owners) == 1

    @given(grids(max_side=512))
    def test_a_core_owns_its_own_corners_but_not_its_upper_bounds(self, grid):
        """Half-open rectangles: the pixel at y1 belongs to the next core, not this one."""
        h, w, chunk, overlap = grid
        for r in core_regions(h, w, chunk, overlap):
            assert r.owns(r.y0, r.x0)
            assert not r.owns(r.y1, r.x0)
            assert not r.owns(r.y0, r.x1)

    @given(grids(max_side=512))
    def test_local_coordinates_map_back_into_the_core(self, grid):
        """A centroid measured inside the core, in block-local coordinates, must land
        back on the same core once converted to full-image coordinates."""
        h, w, chunk, overlap = grid
        for r in core_regions(h, w, chunk, overlap):
            gy, gx = r.to_global(r.pad_top, r.pad_left)
            assert (gy, gx) == (r.y0, r.x0)
            assert r.owns(gy, gx)


class TestKnownShapes:
    def test_image_smaller_than_the_chunk_yields_one_core(self):
        regions = core_regions(300, 200, chunk=512, overlap=16)
        assert len(regions) == 1
        assert (regions[0].y0, regions[0].x0, regions[0].y1, regions[0].x1) == (0, 0, 300, 200)
        assert regions[0].block == (0, 0, 300, 200)  # nothing to pad against

    def test_short_tail_is_merged_into_its_predecessor(self):
        # 1024 / 500 -> 500, 500, 24. A 24-pixel tail cannot host a 32-pixel halo.
        rows = sorted({(r.y0, r.y1) for r in core_regions(1024, 512, chunk=500, overlap=32)})
        assert rows == [(0, 500), (500, 1024)]

    def test_short_tail_survives_when_there_is_no_halo(self):
        rows = sorted({(r.y0, r.y1) for r in core_regions(1024, 512, chunk=500, overlap=0)})
        assert rows == [(0, 500), (500, 1000), (1000, 1024)]

    def test_bbbc021_field_splits_only_along_its_long_axis(self):
        # 1280x1024 with chunk 1024: columns split 1024 + 256, rows stay whole.
        regions = core_regions(1024, 1280, chunk=1024, overlap=32)
        assert len(regions) == 2
        assert sorted((r.x0, r.x1) for r in regions) == [(0, 1024), (1024, 1280)]

    def test_regions_are_emitted_in_row_major_order(self):
        regions = core_regions(200, 200, chunk=100, overlap=0)
        assert [(r.y0, r.x0) for r in regions] == [(0, 0), (0, 100), (100, 0), (100, 100)]


class TestRejectedInput:
    @pytest.mark.parametrize(
        ("h", "w", "chunk", "overlap"),
        [
            (0, 100, 32, 0),  # empty image
            (100, 0, 32, 0),
            (-1, 100, 32, 0),
            (100, 100, 0, 0),  # chunk must be positive
            (100, 100, -32, 0),
            (100, 100, 32, -1),  # overlap must be non-negative
            (100, 100, 32, 40),  # halo deeper than the smallest core
            (64, 64, 512, 100),  # halo deeper than the whole image
        ],
    )
    def test_invalid_geometry_raises(self, h, w, chunk, overlap):
        with pytest.raises(ValueError):
            core_regions(h, w, chunk, overlap)


class TestCoreRegionValue:
    def test_regions_are_immutable(self):
        region = core_regions(100, 100, chunk=50, overlap=8)[0]
        assert isinstance(region, CoreRegion)
        with pytest.raises(AttributeError):
            region.y0 = 5  # type: ignore[misc]
