"""The whole-slide rail (spec §8.1, §20 Fase 2).

A slide is not a bigger field, and two of the differences change the code rather than the
constants.

**Brightfield is inverted.** In fluorescence a nucleus is bright on a black background; in
a stained section the tissue is dark on white — the corner pixel of `CMU-1-Small-Region` is
`(221, 182, 221)`. A segmenter that keeps what is *above* a threshold would keep the glass.
So this rail hands `measure_field` optical density rather than intensity, and specifically
the haematoxylin channel of a colour deconvolution: haematoxylin is what stains nuclei, and
its density plane is high exactly where they are. One segmenter serves both rails because
the reader, not the segmenter, is what differs.

**The pyramid is the point.** `read_region` at level 0 of `CMU-1` would be 46000x32914
pixels, and the global threshold of §8.1 does not need them: it is computed on the smallest
level, which is a thumbnail. That is the answer to the objection that a global threshold
means reading the whole image — for a slide it never did.

The class below satisfies the `Sliceable` protocol of `hcs.imaging.pipeline`, so the block
loop, the centroid policy and the Parquet writer are the same code on both rails.
"""

from dataclasses import dataclass
from typing import Any, Final

import numpy as np
import numpy.typing as npt
import openslide
from skimage.color import rgb2hed
from skimage.filters import gaussian, threshold_otsu

# Index of the haematoxylin plane in scikit-image's H&E-DAB deconvolution.
HEMATOXYLIN: Final = 0

# read_region always yields RGBA; the alpha plane carries nothing for a region inside the
# slide, and colour deconvolution wants three channels.
_RGB: Final = slice(0, 3)


@dataclass(frozen=True, slots=True)
class SlideLevel:
    """One pyramid level of one slide, sliced like an array and read like a slide.

    Nothing is materialised on construction. Slicing reads exactly the requested rectangle
    through OpenSlide, which is where the flat RSS curve comes from: a block costs a block,
    whatever the slide measures.
    """

    slide: openslide.OpenSlide
    level: int = 0

    @property
    def shape(self) -> tuple[int, int]:
        """Height then width, in this level's own pixels — NumPy's order, not OpenSlide's."""
        width, height = self.slide.level_dimensions[self.level]
        return height, width

    def __getitem__(self, key: Any) -> npt.NDArray[np.float32]:
        """Read one rectangle as haematoxylin optical density.

        The trap this hides: `read_region` takes its **location in level-0 coordinates**
        and its **size in the requested level's coordinates**. Passing level coordinates
        for both reads the wrong part of the slide, and it reads it successfully — every
        block would come back looking plausible and describing the wrong tissue.

        A second trap, only above level 0: **Aperio downsamples are not integers.** CMU-1
        reports 4.000121536 and 16.000486145, so `y0 * downsample` lands between level-0
        pixels and the rectangle comes back resampled by a fraction of a pixel — measured
        at up to 0.04 in optical density across 1.7% of pixels, which means two adjacent
        blocks do not tile the level exactly.

        That is why `measure_at` refuses anything but level 0. It costs nothing: a nucleus
        is about 10 pixels across at level 0 and 0.6 at level 2, so a downsampled level is
        not somewhere nuclei can be segmented in the first place. Those levels are for
        thresholds and context, which is what `thumbnail_threshold` uses them for.
        """
        rows, cols = key
        height, width = self.shape
        y0, y1 = rows.indices(height)[:2]
        x0, x1 = cols.indices(width)[:2]

        downsample = self.slide.level_downsamples[self.level]
        location = (round(x0 * downsample), round(y0 * downsample))
        region = self.slide.read_region(location, self.level, (x1 - x0, y1 - y0))
        return _hematoxylin(np.asarray(region))


def measure_at(slide: openslide.OpenSlide) -> SlideLevel:
    """The level nuclei are measured on, which is always level 0.

    Stated as a function rather than left to the caller because the alternative is a
    plausible mistake. A downsampled level is smaller and faster and looks like a
    reasonable place to save time, and it is not: nuclei are unresolvable there, and the
    non-integer downsample means the blocks do not tile it exactly.

    Level 0 of `CMU-1` is 46000x32914, which is 1.5 gigapixels — and processing that
    without ever holding it is the entire claim of this rail, so declining to do it would
    be declining the demonstration.
    """
    return SlideLevel(slide, 0)


def open_slide(path: Any) -> openslide.OpenSlide:
    """Open a slide, letting OpenSlide decide the vendor from the file itself.

    No dispatch on the extension. The rail claims to read slides, and `.svs` and `.ndpi`
    are two of a dozen formats behind one interface — which is the reason to depend on
    OpenSlide here rather than on tifffile.
    """
    return openslide.OpenSlide(path)


def thumbnail_threshold(slide: openslide.OpenSlide, sigma: float = 1.0) -> float:
    """The global foreground level, computed on the smallest pyramid level.

    This is what makes §8.1's global threshold affordable on a slide. The smallest level of
    `CMU-1` is a few megapixels against level 0's 1.5 gigapixels, and a threshold that
    separates tissue from glass does not improve for being computed on a hundred times more
    of the same two populations.

    A slide with a single level — the small test slide is one — falls back to that level,
    which is then simply the whole image.
    """
    smallest = slide.level_count - 1
    width, height = slide.level_dimensions[smallest]
    plane = SlideLevel(slide, smallest)[0:height, 0:width]
    return float(threshold_otsu(gaussian(plane, sigma=sigma, preserve_range=True)))


def _hematoxylin(region: npt.NDArray[np.uint8]) -> npt.NDArray[np.float32]:
    """RGBA bytes to the haematoxylin density plane.

    `rgb2hed` returns optical densities, so the sign is already the way round the segmenter
    wants: dense nuclei are large positive numbers and empty glass is near zero. No explicit
    inversion is needed, and adding one anyway would be the bug this docstring exists to
    prevent.
    """
    deconvolved: npt.NDArray[np.float64] = rgb2hed(region[..., _RGB])
    return deconvolved[..., HEMATOXYLIN].astype(np.float32)
