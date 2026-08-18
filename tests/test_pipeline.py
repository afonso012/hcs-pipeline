"""The centroid policy and its driver (§8.1, §13.1).

The second property of §13.1 lives here: **the same image measured with different chunk
sizes must give the same cell count.** It is the only test that can catch a wrong boundary
rule, because a wrong rule is invisible at any single chunk size — the counts are simply
off, and nothing says so.

The discs are placed deliberately, not at random. Several straddle a block boundary at
chunk 128 and 256, and one sits with its centroid exactly on a boundary line, which is the
case that decides whether `owns` needs to be half-open.
"""

import numpy as np
import numpy.typing as npt
import pandas as pd
import pytest

from hcs.geometry import CoreRegion, core_regions
from hcs.imaging.measure import measure_block
from hcs.imaging.pipeline import ChunkConfig, measure_field
from hcs.imaging.segment import SegmentationConfig, segment_chunk
from tests.test_segment import discs

SIZE = 512
RADIUS = 12

# A grid well clear of every boundary, plus five placed on the lines that chunk 128 and
# chunk 256 draw: one centred exactly on 256, and four straddling other boundaries.
CENTRES = [
    *[(y, x) for y in (40, 180, 330, 470) for x in (40, 180, 330, 470)],
    (256, 256),
    (128, 300),
    (300, 128),
    (384, 70),
    (70, 384),
]

Image = npt.NDArray[np.float64]


@pytest.fixture(scope="module")
def image() -> Image:
    return discs((SIZE, SIZE), CENTRES, radius=RADIUS)


def count_at(image: Image, chunk: int, overlap: int = 32) -> int:
    frame = measure_field(image, {"DNA": image}, ChunkConfig(chunk=chunk, overlap=overlap))
    return len(frame)


class TestCountIsInvariantToChunking:
    """§13.1, second property.

    The spec allows ±1%. On synthetic discs there is no tolerance to spend: they do not
    touch, so any difference at all is the boundary rule dropping or duplicating one.
    """

    @pytest.mark.parametrize("chunk", [128, 192, 256, 400, 512, 1024])
    def test_every_chunk_size_finds_every_disc(self, image: Image, chunk: int) -> None:
        assert count_at(image, chunk) == len(CENTRES)

    def test_the_spec_comparison_itself(self, image: Image) -> None:
        """ "A mesma imagem sintética com chunk=256 e chunk=1024 tem de dar a mesma
        contagem." 1024 exceeds the image, so it is the single-block case: the count that
        no chunking at all would give."""
        assert count_at(image, 256) == count_at(image, 1024)

    @pytest.mark.parametrize("overlap", [16, 32, 64])
    def test_the_halo_depth_does_not_change_the_count(self, image: Image, overlap: int) -> None:
        """The halo decides how much of a boundary object a block can see, never how many
        objects exist. If it moves the count, objects are being owned twice or not at
        all."""
        assert count_at(image, 128, overlap) == len(CENTRES)


class TestNoObjectIsCountedTwice:
    def test_each_disc_appears_once_at_its_own_centre(self, image: Image) -> None:
        """Counting is not enough: a rule could drop one object and duplicate another and
        still total correctly. This checks identity, not arithmetic."""
        frame = measure_field(image, {"DNA": image}, ChunkConfig(chunk=128, overlap=32))

        found = sorted(
            (round(y), round(x)) for y, x in zip(frame.centroid_y, frame.centroid_x, strict=True)
        )
        assert found == sorted(CENTRES)

    def test_a_disc_centred_on_a_boundary_belongs_to_exactly_one_core(self) -> None:
        """`owns` is half-open, so a centroid at exactly y=256 belongs to the core that
        starts there and not to the one that ends there. Either convention works; having
        no convention counts the cell twice."""
        regions = core_regions(SIZE, SIZE, 256, 32)

        owners = [region for region in regions if region.owns(256.0, 256.0)]

        assert len(owners) == 1
        assert (owners[0].y0, owners[0].x0) == (256, 256)


class TestBorderObjects:
    def test_a_disc_clipped_by_the_image_edge_is_marked(self) -> None:
        """§8.1: it counts as a cell, and its morphology describes the crop rather than
        the object, so §9 keeps it out of the shape statistics."""
        image = discs((256, 256), [(0, 128), (128, 128)], radius=RADIUS)

        frame = measure_field(image, {"DNA": image}, ChunkConfig(chunk=128, overlap=32))
        marked = dict(zip(frame.centroid_y.round().astype(int), frame.touches_border, strict=True))

        assert marked[128] == np.False_
        assert marked[max(k for k in marked if k != 128)] == np.True_

    def test_a_disc_on_a_block_boundary_is_not_marked(self, image: Image) -> None:
        """The distinction the flag exists to make. An object at a block boundary is
        whole — the halo shows the rest of it — and only the image has nothing beyond
        it."""
        frame = measure_field(image, {"DNA": image}, ChunkConfig(chunk=128, overlap=32))

        on_boundary = frame[frame.centroid_y.round() == 256]
        assert len(on_boundary) == 1
        assert not on_boundary.touches_border.iloc[0]


class TestMeasurement:
    def test_a_disc_is_measured_whole_even_when_it_crosses_into_the_halo(self) -> None:
        """Why measurement happens on the padded block and before the filter. The area of
        a boundary disc must equal that of an interior one; measuring on the trimmed core
        would report a nucleus sliced in half."""
        image = discs((256, 256), [(64, 64), (128, 128)], radius=RADIUS)

        frame = measure_field(image, {"DNA": image}, ChunkConfig(chunk=128, overlap=32))

        assert frame.area.nunique() == 1

    def test_intensity_is_measured_per_channel_inside_the_dna_mask(self) -> None:
        """§8.2: the DNA channel finds the objects, the others are measured inside that
        mask. A channel bright everywhere still reports only the pixels under a nucleus."""
        dna = discs((256, 256), [(64, 64)], radius=RADIUS)
        tubulin = np.full((256, 256), 500.0)

        frame = measure_field(dna, {"DNA": dna, "Tubulin": tubulin}, ChunkConfig(chunk=256))

        row = frame.iloc[0]
        assert row.feat_intensity_mean_Tubulin == pytest.approx(500.0)
        assert row.feat_intensity_integrated_Tubulin == pytest.approx(500.0 * row.area)
        assert row.feat_intensity_mean_DNA == pytest.approx(1000.0)

    def test_a_channel_of_a_different_shape_is_refused(self) -> None:
        """A mis-paired file. Measuring channel B inside a mask from channel A means
        something only if the two are the same field."""
        dna = discs((256, 256), [(64, 64)])

        with pytest.raises(ValueError, match="do not match"):
            measure_field(dna, {"Tubulin": np.zeros((128, 128))}, ChunkConfig(chunk=256))


class TestQuietImages:
    def test_an_empty_image_gives_no_rows_but_keeps_its_columns(self) -> None:
        """Most of a plate's border wells look like this. A schema that appeared only when
        a block happened to hold cells would make the Parquet file depend on the data."""
        empty = np.zeros((256, 256))

        frame = measure_field(empty, {"DNA": empty}, ChunkConfig(chunk=128))

        assert frame.empty
        assert "feat_intensity_mean_DNA" in frame.columns
        assert frame.touches_border.dtype == np.dtype("bool")

    def test_a_block_that_owns_nothing_contributes_nothing(self) -> None:
        """One disc, four blocks: three of them see it in their halo, and none of those
        three may claim it."""
        image = discs((256, 256), [(64, 64)], radius=RADIUS)
        regions = core_regions(256, 256, 128, 32)

        owned = [
            len(_measure(region, image)) for region in regions if (region.y0, region.x0) != (0, 0)
        ]

        assert owned == [0, 0, 0]


def _measure(region: CoreRegion, image: Image) -> pd.DataFrame:
    y0, x0, y1, x1 = region.block
    block = image[y0:y1, x0:x1]
    labels = segment_chunk(block, SegmentationConfig())
    return measure_block(labels, {"DNA": block}, region, (image.shape[0], image.shape[1]))
