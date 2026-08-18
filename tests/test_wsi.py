"""The whole-slide rail (§8.1, §20 Fase 2).

Against real slides, not synthetic ones. A synthetic pyramid would test the arithmetic and
miss everything that makes a slide a slide: that brightfield is inverted, that
`read_region` mixes two coordinate systems, and that a vendor format is a format.

The slides live on the external SSD and CI has no SSD, so everything here is skipped when
they are absent. `make data-wsi` fetches them.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from hcs.imaging.pipeline import ChunkConfig, measure_field
from hcs.imaging.wsi import SlideLevel, measure_at, open_slide, thumbnail_threshold

SLIDES = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/OpenSlide")
SMALL = SLIDES / "Aperio" / "CMU-1-Small-Region.svs"
PYRAMID = SLIDES / "Aperio" / "CMU-1.svs"

pytestmark = [
    pytest.mark.wsi,
    pytest.mark.skipif(not SMALL.exists(), reason=f"no slides at {SLIDES}; run make data-wsi"),
]


@pytest.fixture(scope="module")
def small() -> Iterator[Any]:
    slide = open_slide(SMALL)
    yield slide
    slide.close()


class TestGeometry:
    def test_the_shape_is_numpy_order_not_openslide_order(self, small: Any) -> None:
        """OpenSlide reports `(width, height)` and NumPy expects `(height, width)`. Getting
        this backwards is invisible on a square image; this slide is 2220x2967."""
        width, height = small.level_dimensions[0]

        assert SlideLevel(small, 0).shape == (height, width)
        assert height != width  # or the test proves nothing

    def test_a_slice_returns_exactly_the_requested_rectangle(self, small: Any) -> None:
        assert SlideLevel(small, 0)[100:356, 200:712].shape == (256, 512)


class TestBrightfieldIsInverted:
    def test_glass_is_near_zero_and_tissue_is_not(self, small: Any) -> None:
        """The trap the whole reader exists to avoid. Raw brightfield has tissue *dark* on
        a light background, so a segmenter keeping what is above a threshold would keep the
        glass. Optical density puts it the right way round."""
        plane = SlideLevel(small, 0)[0:2967, 0:2220]

        assert float(np.median(plane)) < 0.05  # mostly glass
        assert float(plane.max()) > 1.0  # and some genuinely dense nuclei

    def test_the_threshold_sits_between_the_two(self, small: Any) -> None:
        plane = SlideLevel(small, 0)[0:2967, 0:2220]

        assert float(np.median(plane)) < thumbnail_threshold(small) < float(plane.max())


class TestChunking:
    """The same property as §13.1's second, on a slide rather than a field.

    Not exact here, unlike the synthetic discs: real nuclei touch, and the watershed splits
    a confluent clump marginally differently depending on how much of it a block can see.
    That residue is what the spec's ±1% was written for.
    """

    def test_the_nucleus_count_barely_moves_with_the_chunk(self, small: Any) -> None:
        level = SlideLevel(small, 0)
        threshold = thumbnail_threshold(small)
        counts = [
            len(
                measure_field(
                    level, {"H": level}, ChunkConfig(chunk=chunk, overlap=32), threshold=threshold
                )
            )
            for chunk in (256, 1024)
        ]

        # A sanity bound, not a target: it says the reader found tissue rather than
        # noise. The exact count follows `min_distance`, which is tuned against BBBC039
        # (§13.5), so pinning it here would make an unrelated file fail on a tuning change.
        assert counts[0] > 500
        assert abs(counts[0] - counts[1]) / max(counts) < 0.01


@pytest.mark.skipif(not PYRAMID.exists(), reason="CMU-1.svs not downloaded")
class TestPyramid:
    def test_the_threshold_comes_from_the_smallest_level(self) -> None:
        """§8.1's global threshold on a slide, and why the objection to it was wrong: the
        smallest level is a thumbnail, and level 0 is never read to compute it."""
        slide = open_slide(PYRAMID)
        try:
            smallest = slide.level_dimensions[-1]
            biggest = slide.level_dimensions[0]

            assert slide.level_count > 1
            assert smallest[0] * smallest[1] < biggest[0] * biggest[1] / 100
            assert thumbnail_threshold(slide) > 0
        finally:
            slide.close()

    def test_blocks_tile_level_zero_exactly(self) -> None:
        """`read_region` takes its location in level-0 coordinates and its size in the
        requested level's. Mixing the two reads the wrong tissue, successfully and
        silently — a single-level slide cannot catch it, so this one does.

        At level 0 the downsample is exactly 1, so the same pixels reached two ways are
        the same pixels, bit for bit.
        """
        slide = open_slide(PYRAMID)
        try:
            page = measure_at(slide)
            band = 512

            whole = page[0 : band * 2, 0:band]
            lower = page[band : band * 2, 0:band]

            assert slide.level_downsamples[0] == 1.0
            assert np.array_equal(whole[band:], lower)
        finally:
            slide.close()

    def test_the_downsamples_are_not_integers(self) -> None:
        """The reason measurement stays at level 0, recorded as a fact rather than a
        comment. Aperio reports 4.000121536 and 16.000486145, so above level 0 a block
        boundary falls between level-0 pixels and adjacent blocks do not tile exactly —
        measured at up to 0.04 in optical density over 1.7% of pixels.

        It is not worth fixing. A nucleus spans about 10 pixels at level 0 and 0.6 at
        level 2, so those levels are not somewhere nuclei can be segmented anyway.
        """
        slide = open_slide(PYRAMID)
        try:
            downsamples = slide.level_downsamples[1:]

            assert downsamples
            assert all(d != round(d) for d in downsamples)
        finally:
            slide.close()
