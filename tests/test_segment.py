"""The classic segmentation path (§8.2).

Synthetic discs rather than real nuclei: the point of these tests is that the function
behaves the way the rest of the pipeline assumes, not that it segments biology well. How
well it segments biology is what the golden batch (§13.2) and the BBBC039 ground truth
(§13.5) are for, and neither belongs in a unit test.
"""

import numpy as np
import numpy.typing as npt
import pytest

from hcs.imaging.segment import SegmentationConfig, segment_chunk

CONFIG = SegmentationConfig()


def discs(
    shape: tuple[int, int], centres: list[tuple[int, int]], radius: int = 12
) -> npt.NDArray[np.float64]:
    """A dark field with bright discs on it, standing in for stained nuclei."""
    image = np.zeros(shape, dtype=np.float64)
    ys, xs = np.ogrid[: shape[0], : shape[1]]
    for centre_y, centre_x in centres:
        image[(ys - centre_y) ** 2 + (xs - centre_x) ** 2 <= radius**2] = 1000.0
    return image


class TestCounting:
    def test_separated_discs_are_separate_objects(self) -> None:
        image = discs((128, 128), [(30, 30), (30, 90), (90, 30), (90, 90)])

        labels = segment_chunk(image, CONFIG)

        assert len(np.unique(labels)) - 1 == 4

    def test_touching_discs_are_split_by_the_watershed(self) -> None:
        """A threshold alone merges confluent nuclei into one region. The distance
        transform peaks once per nucleus, which is what pulls them apart again."""
        image = discs((128, 128), [(64, 52), (64, 74)], radius=14)

        labels = segment_chunk(image, CONFIG)

        assert len(np.unique(labels)) - 1 == 2

    def test_debris_below_min_size_is_not_an_object(self) -> None:
        image = discs((128, 128), [(64, 64)], radius=2)  # area ~13, below min_size 50

        assert not segment_chunk(image, CONFIG).any()


class TestQuietBlocks:
    def test_a_constant_block_yields_nothing(self) -> None:
        """An empty well and the dark margin of a slide are both constant. Otsu on a
        constant array returns that constant, which would threshold every pixel into a
        single object the size of the block."""
        assert not segment_chunk(np.full((64, 64), 7.0), CONFIG).any()

    def test_an_empty_block_yields_nothing(self) -> None:
        assert segment_chunk(np.zeros((0, 0)), CONFIG).shape == (0, 0)

    @pytest.mark.parametrize("dtype", [np.uint8, np.uint16, np.float32])
    def test_the_integer_types_a_tiff_actually_uses_are_accepted(self, dtype: type) -> None:
        """BBBC021 TIFFs are 16-bit. A path that only worked on floats would be found at
        the first real file rather than here."""
        image = discs((128, 128), [(64, 64)]).astype(dtype)

        assert len(np.unique(segment_chunk(image, CONFIG))) - 1 == 1


class TestDeterminism:
    def test_the_same_block_twice_gives_the_same_labels(self) -> None:
        """§13.2 fails the build on a 2% drift in cell count. That threshold means
        anything only if the segmenter has no run-to-run variation of its own."""
        image = discs((256, 256), [(60, 60), (60, 180), (180, 60), (180, 180)])

        assert np.array_equal(segment_chunk(image, CONFIG), segment_chunk(image, CONFIG))
