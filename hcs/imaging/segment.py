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
    min_distance: int = 10


def segment_chunk(
    block: npt.NDArray[np.floating | np.integer], cfg: SegmentationConfig
) -> LabelArray:
    """Label the objects in one block. Background is 0, objects are 1..n.

    Returns an all-zero array for a block with nothing in it. A block of constant
    intensity is an ordinary case, not a corrupt one — an empty well, or the dark margin
    of a slide — and Otsu on a constant array returns that constant, which would
    otherwise threshold every pixel into one enormous object.
    """
    if block.size == 0 or block.min() == block.max():
        return np.zeros(block.shape, dtype=LABEL_DTYPE)

    smoothed = gaussian(block, sigma=cfg.sigma, preserve_range=True)
    # `max_size` removes objects of area *less than or equal to* its value, where the
    # `min_size` it replaced removed those strictly below. Passing `min_size - 1` keeps
    # the threshold meaning what its name says: an object of exactly `min_size` survives.
    foreground = remove_small_objects(
        smoothed > threshold_otsu(smoothed), max_size=cfg.min_size - 1
    )
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
