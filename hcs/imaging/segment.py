"""The classic segmentation path (spec §8.2).

Gaussian, Otsu, small-object removal, distance transform, watershed. CPU and
deterministic — no random state anywhere, so the same block always yields the same
labels. That is what lets the golden batch of §13.2 fail on a 2% drift instead of on
noise.

This is the default until Phase 7 puts Cellpose behind the same signature. It takes
pixels and returns labels; it knows nothing about chunks, halos or plates, which is what
keeps it testable against a synthetic image with a known cell count.
"""

from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt
from scipy import ndimage
from skimage.feature import peak_local_max
from skimage.filters import gaussian, threshold_otsu
from skimage.morphology import remove_small_objects
from skimage.segmentation import watershed

# int32, not int64: a block never holds two billion objects, and the label array is the
# largest thing a worker allocates per block.
LabelArray = npt.NDArray[np.int32]

LABEL_DTYPE: Final = np.int32


@dataclass(frozen=True, slots=True)
class SegmentationConfig:
    """Knobs of the classic path.

    Defaults are for BBBC021 nuclei at 1024x1280, where a nucleus is roughly 20-40 px
    across. They are configuration rather than constants because Phase 2 exists partly to
    measure how the pipeline responds to them, and §12 hashes them into the run.
    """

    # Smoothing before thresholding. Enough to stop Otsu chasing single-pixel noise, not
    # so much that two adjacent nuclei melt into one blob.
    sigma: float = 1.0
    # Objects below this are debris: dust, staining artefacts, or a fragment of a nucleus
    # clipped by the block edge.
    min_size: int = 50
    # Minimum separation between watershed seeds, in pixels. Set below the radius of a
    # nucleus and one nucleus grows two seeds, then gets split down the middle.
    #
    # Chosen on BBBC039's **validation** split, once (`docs/tuning-bbbc039.md`). The
    # previous value of 10 over-segmented by 10%: mean F1 0.674 against 0.754 here, and
    # 110.6% of the annotated nucleus count against 96.7%. The optimum is bracketed rather
    # than sitting at the edge of the sweep — 15 scores 0.722 and 30 scores 0.715 — and 25
    # ties on mean F1 but under-segments by 13%, which is worse for a count.
    min_distance: int = 20


def segment_chunk(
    block: npt.NDArray[np.floating | np.integer],
    cfg: SegmentationConfig,
    threshold: float | None = None,
) -> LabelArray:
    """Label the objects in one block. Background is 0, objects are 1..n.

    `threshold` should come from the whole image. Omitting it falls back to Otsu on this
    block alone, which is only safe when the block is known to contain signal, because
    **Otsu on a block of pure background splits the noise down the middle**. That is not a
    defect of the implementation: Otsu is defined to find the best two-class split of
    whatever it is given, and it has no way to answer "there is nothing here".

    Measured on a real BBBC021 field, `Week1_22161 D03 s2`: two empty blocks out of twenty
    produced 210 and 158 objects between them, against 77 real cells in the whole image.

    Returns an all-zero array for a block with nothing in it. A block of constant
    intensity is an ordinary case rather than a corrupt one — an empty well, or the dark
    margin of a slide.
    """
    if block.size == 0 or block.min() == block.max():
        return np.zeros(block.shape, dtype=LABEL_DTYPE)

    smoothed = gaussian(block, sigma=cfg.sigma, preserve_range=True)
    level = threshold_otsu(smoothed) if threshold is None else threshold
    # `max_size` removes objects of area *less than or equal to* its value, where the
    # `min_size` it replaced removed those strictly below. Passing `min_size - 1` keeps
    # the threshold meaning what its name says: an object of exactly `min_size` survives.
    foreground = remove_small_objects(smoothed > level, max_size=cfg.min_size - 1)
    if not foreground.any():
        return np.zeros(block.shape, dtype=LABEL_DTYPE)

    return _split_touching(foreground, cfg)


def _split_touching(foreground: npt.NDArray[np.bool_], cfg: SegmentationConfig) -> LabelArray:
    """Separate nuclei that the threshold merged into one connected region.

    Confluent cells touch, and no threshold can tell two touching nuclei from one large
    one. The distance transform peaks at the centre of each nucleus, so its local maxima
    give one seed per nucleus, and watershed grows them back out to the threshold
    boundary.
    """
    distance = ndimage.distance_transform_edt(foreground)
    peaks = peak_local_max(
        distance, min_distance=cfg.min_distance, labels=foreground, exclude_border=False
    )

    markers = np.zeros(foreground.shape, dtype=LABEL_DTYPE)
    markers[tuple(peaks.T)] = np.arange(1, len(peaks) + 1, dtype=LABEL_DTYPE)

    labels: LabelArray = watershed(-distance, markers, mask=foreground).astype(LABEL_DTYPE)
    return labels
