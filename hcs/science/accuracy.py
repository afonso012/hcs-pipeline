"""Scoring instance segmentation against annotation (spec §13.5).

The measurement everything else in this project rests on and none of it performs. The
golden batch proves the numbers do not drift, the chunking property proves they do not
depend on the block size, the negative control proves the memory limits fire — and all
three would hold just as well if the segmenter were counting the wrong things.

**Object-level, not pixel-level, and that is the whole design.** A binary mask scored
pixel-wise cannot tell a method that merges two touching nuclei from one that separates
them: both paint the same pixels. This pipeline's hardest problem is exactly that
separation — it is what the watershed exists for and what `min_distance` tunes — so the
metric has to be able to see it. Matching objects one to one can.

The protocol is the standard one for this task, and is worth stating because "F1" alone
means four different things in this literature:

1. Compute IoU between every predicted object and every true object.
2. Match them one to one, greedily by descending IoU.
3. At a threshold, a matched pair above it is a true positive; an unmatched prediction is a
   false positive; an unmatched annotation is a false negative.
4. F1 = 2TP / (2TP + FP + FN).

Reported at 0.5 (the conventional "found it") and at 0.75 (the stricter "found it and got
its boundary roughly right"), plus the mean over 0.50:0.95 in steps of 0.05, which is the
COCO convention and the one that punishes sloppy boundaries.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt

# The COCO sweep. Quoting a single threshold flatters a method that finds objects but
# outlines them badly, which is precisely the failure a watershed on a bad threshold has.
IOU_THRESHOLDS: Final = tuple(round(0.50 + 0.05 * step, 2) for step in range(10))

REPORTED: Final = (0.5, 0.75)


@dataclass(frozen=True, slots=True)
class Score:
    """One image scored at one threshold."""

    threshold: float
    true_positives: int
    false_positives: int
    false_negatives: int
    mean_iou_of_matches: float

    @property
    def precision(self) -> float:
        found = self.true_positives + self.false_positives
        return self.true_positives / found if found else 0.0

    @property
    def recall(self) -> float:
        annotated = self.true_positives + self.false_negatives
        return self.true_positives / annotated if annotated else 0.0

    @property
    def f1(self) -> float:
        denominator = 2 * self.true_positives + self.false_positives + self.false_negatives
        return 2 * self.true_positives / denominator if denominator else 0.0


def iou_matrix(
    predicted: npt.NDArray[np.integer], truth: npt.NDArray[np.integer]
) -> tuple[npt.NDArray[np.float64], list[int], list[int]]:
    """IoU between every predicted object and every true one.

    Built from a single 2-D histogram of the two label images rather than by looping over
    pairs. On a 520x696 field with 300 objects each that is the difference between one pass
    over the pixels and ninety thousand.
    """
    pred_ids = [int(i) for i in np.unique(predicted) if i != 0]
    true_ids = [int(i) for i in np.unique(truth) if i != 0]
    if not pred_ids or not true_ids:
        return np.zeros((len(pred_ids), len(true_ids))), pred_ids, true_ids

    pred_index = {label: i for i, label in enumerate(pred_ids)}
    true_index = {label: i for i, label in enumerate(true_ids)}

    overlap = np.zeros((len(pred_ids), len(true_ids)), dtype=np.int64)
    both = (predicted > 0) & (truth > 0)
    for p, t in zip(predicted[both].ravel(), truth[both].ravel(), strict=True):
        overlap[pred_index[int(p)], true_index[int(t)]] += 1

    pred_area = np.array([int((predicted == label).sum()) for label in pred_ids])[:, None]
    true_area = np.array([int((truth == label).sum()) for label in true_ids])[None, :]
    union = pred_area + true_area - overlap
    return (
        np.divide(overlap, union, out=np.zeros_like(overlap, dtype=float), where=union > 0),
        (pred_ids),
        true_ids,
    )


def score_image(
    predicted: npt.NDArray[np.integer],
    truth: npt.NDArray[np.integer],
    thresholds: Sequence[float] = IOU_THRESHOLDS,
) -> list[Score]:
    """Score one predicted label image against its annotation, at each threshold."""
    ious, pred_ids, true_ids = iou_matrix(predicted, truth)
    return [_score_at(ious, len(pred_ids), len(true_ids), threshold) for threshold in thresholds]


def _score_at(
    ious: npt.NDArray[np.float64], n_predicted: int, n_true: int, threshold: float
) -> Score:
    """Match greedily by descending IoU, then count.

    Greedy rather than optimal (Hungarian) matching. With a threshold at or above 0.5 the
    two agree by construction: two predictions cannot both exceed 0.5 IoU with the same
    annotation, because their overlaps would sum past its area. Below 0.5 they can differ,
    which is one more reason the reported figures start at 0.5.
    """
    matched_pred: set[int] = set()
    matched_true: set[int] = set()
    matched_ious: list[float] = []

    if ious.size:
        order = np.dstack(np.unravel_index(np.argsort(ious, axis=None)[::-1], ious.shape))[0]
        for p, t in order:
            value = float(ious[p, t])
            if value < threshold:
                break
            if int(p) in matched_pred or int(t) in matched_true:
                continue
            matched_pred.add(int(p))
            matched_true.add(int(t))
            matched_ious.append(value)

    return Score(
        threshold=threshold,
        true_positives=len(matched_ious),
        false_positives=n_predicted - len(matched_pred),
        false_negatives=n_true - len(matched_true),
        mean_iou_of_matches=float(np.mean(matched_ious)) if matched_ious else 0.0,
    )


def aggregate(per_image: Sequence[Sequence[Score]]) -> dict[float, Score]:
    """Pool counts across images, per threshold.

    Pooled rather than averaged over images. A mean of per-image F1 lets a field with three
    nuclei weigh as much as one with three hundred, which turns the metric into a statement
    about how many sparse fields the dataset happens to contain.
    """
    pooled: dict[float, Score] = {}
    for index, threshold in enumerate(IOU_THRESHOLDS):
        scores = [image[index] for image in per_image if index < len(image)]
        if not scores:
            continue
        matched = [s.mean_iou_of_matches for s in scores if s.true_positives]
        pooled[threshold] = Score(
            threshold=threshold,
            true_positives=sum(s.true_positives for s in scores),
            false_positives=sum(s.false_positives for s in scores),
            false_negatives=sum(s.false_negatives for s in scores),
            mean_iou_of_matches=float(np.mean(matched)) if matched else 0.0,
        )
    return pooled


def mean_f1(pooled: dict[float, Score]) -> float:
    """The COCO-style mean over the sweep — the single number worth quoting."""
    return float(np.mean([score.f1 for score in pooled.values()])) if pooled else 0.0
