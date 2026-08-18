"""The instance-segmentation metric (§13.5).

Tested against label images whose right answer is known by construction, because a metric
verified on real data is a metric verified against the thing it is supposed to judge.

The cases that matter are the ones a pixel-wise score cannot see: a prediction that merges
two touching nuclei, and one that splits a single nucleus in two. Both paint exactly the
right pixels. Both are wrong, and this pipeline's watershed exists precisely to get them
right — so the metric has to be able to fail them.
"""

import numpy as np
import numpy.typing as npt
import pytest

from hcs.science.accuracy import IOU_THRESHOLDS, aggregate, iou_matrix, mean_f1, score_image


def labelled(*boxes: tuple[int, int, int, int]) -> npt.NDArray[np.int32]:
    """A label image from rectangles: `(y0, x0, y1, x1)`, numbered from 1 in order."""
    canvas = np.zeros((64, 64), dtype=np.int32)
    for index, (y0, x0, y1, x1) in enumerate(boxes, start=1):
        canvas[y0:y1, x0:x1] = index
    return canvas


PERFECT = labelled((10, 10, 20, 20), (40, 40, 50, 50))


class TestTheEasyCases:
    def test_an_exact_prediction_scores_one(self) -> None:
        scores = score_image(PERFECT, PERFECT)

        assert all(score.f1 == 1.0 for score in scores)
        assert all(score.mean_iou_of_matches == 1.0 for score in scores)

    def test_predicting_nothing_scores_zero_and_counts_the_misses(self) -> None:
        scores = score_image(np.zeros_like(PERFECT), PERFECT)

        assert scores[0].f1 == 0.0
        assert scores[0].false_negatives == 2
        assert scores[0].false_positives == 0

    def test_inventing_objects_is_counted_as_false_positives(self) -> None:
        invented = labelled((10, 10, 20, 20), (40, 40, 50, 50), (0, 0, 5, 5))

        score = score_image(invented, PERFECT)[0]

        assert (score.true_positives, score.false_positives, score.false_negatives) == (2, 1, 0)


class TestWhatAPixelScoreCannotSee:
    """The two failures that motivate object-level matching."""

    def test_merging_two_touching_nuclei_is_penalised(self) -> None:
        """One prediction covering two annotations. Pixel-wise this is perfect; here it is
        one match, one false negative — which is what a watershed failure looks like."""
        truth = labelled((10, 10, 20, 20), (20, 10, 30, 20))
        merged = np.where(truth > 0, 1, 0).astype(np.int32)

        score = score_image(merged, truth)[0]

        assert score.true_positives <= 1
        assert score.false_negatives >= 1
        assert score.f1 < 1.0

    def test_splitting_one_nucleus_in_two_is_penalised(self) -> None:
        """The opposite error: `min_distance` set too low seeds twice inside one nucleus."""
        truth = labelled((10, 10, 30, 20))
        split = labelled((10, 10, 20, 20), (20, 10, 30, 20))

        score = score_image(split, truth)[0]

        assert score.false_positives >= 1
        assert score.f1 < 1.0


class TestTheThreshold:
    def test_a_roughly_right_boundary_passes_at_0_5_and_fails_at_0_75(self) -> None:
        """Why the sweep exists. A method that finds every object but outlines it loosely
        scores perfectly at 0.5 and badly higher up, and quoting only 0.5 would hide it."""
        truth = labelled((10, 10, 30, 30))
        loose = labelled((10, 10, 26, 26))  # 256 of 400 px, IoU = 0.64

        scores = {score.threshold: score for score in score_image(loose, truth)}

        assert scores[0.5].f1 == 1.0
        assert scores[0.75].f1 == 0.0

    def test_the_sweep_is_the_coco_one(self) -> None:
        assert IOU_THRESHOLDS[0] == 0.5
        assert IOU_THRESHOLDS[-1] == 0.95
        assert len(IOU_THRESHOLDS) == 10


class TestPooling:
    def test_counts_are_pooled_rather_than_averaged_over_images(self) -> None:
        """A mean of per-image F1 lets a field with two nuclei weigh as much as one with
        two hundred, which makes the metric a statement about the dataset's composition."""
        crowded = labelled(*[(i, 0, i + 2, 60) for i in range(0, 40, 4)])
        sparse = labelled((10, 10, 20, 20))

        pooled = aggregate(
            [score_image(crowded, crowded), score_image(np.zeros_like(sparse), sparse)]
        )

        assert pooled[0.5].true_positives == 10
        assert pooled[0.5].false_negatives == 1
        assert pooled[0.5].f1 == pytest.approx(2 * 10 / (2 * 10 + 0 + 1))


class TestTheMatrix:
    def test_background_is_not_an_object(self) -> None:
        """Label 0 is background in both images. Counting it would make every prediction
        share an enormous 'object' and inflate every score."""
        _, pred_ids, true_ids = iou_matrix(PERFECT, PERFECT)

        assert 0 not in pred_ids
        assert len(pred_ids) == len(true_ids) == 2

    def test_disjoint_objects_have_zero_overlap(self) -> None:
        elsewhere = labelled((50, 0, 60, 10))

        ious, _, _ = iou_matrix(elsewhere, PERFECT)

        assert ious.max() == 0.0


def test_the_headline_number_is_the_mean_over_the_sweep() -> None:
    assert mean_f1(aggregate([score_image(PERFECT, PERFECT)])) == 1.0
