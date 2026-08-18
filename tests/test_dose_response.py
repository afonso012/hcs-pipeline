"""The four-parameter logistic (§9.3).

Tested by recovery: build a curve from known parameters, fit it, and check the parameters
come back. That is the only honest test of a fit — asserting on a hand-picked IC50 from
real data would be asserting that today's answer is today's answer.
"""

import numpy as np
import pytest

from hcs.science.dose_response import (
    DoseResponse,
    DoseResponseError,
    fit_dose_response,
    four_parameter_logistic,
)

# The dose series BBBC021 actually uses: eight points, each about three times the last.
SERIES = np.array([0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0])


def curve(ic50: float, hill: float = 1.0, top: float = 100.0, bottom: float = 0.0) -> np.ndarray:
    return four_parameter_logistic(SERIES, bottom, top, ic50, hill)


class TestRecovery:
    @pytest.mark.parametrize("ic50", [0.01, 0.1, 0.5, 3.0])
    def test_the_ic50_it_was_built_from_comes_back(self, ic50: float) -> None:
        fit = fit_dose_response(SERIES, curve(ic50))

        assert fit.ic50 == pytest.approx(ic50, rel=1e-3)

    def test_the_hill_slope_comes_back_too(self) -> None:
        """A steep curve and a shallow one with the same IC50 are different pharmacology,
        and a fit that only reported the midpoint could not tell them apart."""
        steep = fit_dose_response(SERIES, curve(0.1, hill=3.0))
        shallow = fit_dose_response(SERIES, curve(0.1, hill=0.5))

        assert steep.hill == pytest.approx(3.0, rel=1e-2)
        assert shallow.hill == pytest.approx(0.5, rel=1e-2)
        assert steep.ic50 == pytest.approx(shallow.ic50, rel=1e-2)

    def test_a_curve_that_does_not_reach_zero_is_still_fitted(self) -> None:
        """Most compounds plateau above zero. The relative IC50 is the midpoint between
        the *fitted* asymptotes, so a floor at 40 does not move it."""
        fit = fit_dose_response(SERIES, curve(0.1, bottom=40.0))

        assert fit.ic50 == pytest.approx(0.1, rel=1e-3)
        assert fit.bottom == pytest.approx(40.0, abs=1e-6)
        assert fit.span == pytest.approx(60.0, abs=1e-6)


class TestNoise:
    def test_a_noisy_curve_still_lands_close(self) -> None:
        """Real wells scatter. A fit that only worked on clean data would be a fit that
        only worked in this file."""
        rng = np.random.default_rng(20260812)
        noisy = curve(0.1) + rng.normal(0.0, 3.0, SERIES.size)

        fit = fit_dose_response(SERIES, noisy)

        assert fit.ic50 == pytest.approx(0.1, rel=0.25)
        assert fit.r_squared > 0.98

    def test_a_flat_response_is_reported_as_one(self) -> None:
        """The compound that did nothing. The fit may still return a number; `span` and
        `r_squared` are what let a caller refuse to quote it."""
        fit = fit_dose_response(SERIES, np.full(SERIES.size, 100.0))

        assert abs(fit.span) < 1.0


class TestRefusals:
    def test_a_control_is_not_a_dose(self) -> None:
        """Zero has no place on a log-concentration axis, and `(0 / ic50) ** hill` is
        where a fit stops meaning anything. DMSO is what the response is expressed
        against (§9.3), not a point on the curve."""
        with pytest.raises(DoseResponseError, match="not a dose"):
            fit_dose_response([0.0, 0.1, 1.0, 10.0], [100.0, 80.0, 40.0, 10.0])

    def test_three_points_cannot_constrain_four_parameters(self) -> None:
        with pytest.raises(DoseResponseError, match="four parameters"):
            fit_dose_response([0.1, 1.0, 10.0], [90.0, 50.0, 10.0])

    def test_mismatched_lengths_are_refused(self) -> None:
        with pytest.raises(DoseResponseError, match="against"):
            fit_dose_response([0.1, 1.0, 10.0, 100.0], [90.0, 50.0])


class TestExtrapolation:
    def test_an_ic50_beyond_the_doses_tested_is_flagged(self) -> None:
        """The number a curve that never descended produces. It is arithmetic, not
        pharmacology, and the flag is what lets §13.2 decline to guard it."""
        barely = four_parameter_logistic(SERIES, 0.0, 100.0, 1000.0, 1.0)

        fit = fit_dose_response(SERIES, barely)

        assert not fit.within_tested_range

    def test_an_ic50_inside_them_is_not(self) -> None:
        assert fit_dose_response(SERIES, curve(0.3)).within_tested_range


def test_the_dataclass_is_frozen() -> None:
    """§13.2 stores these numbers as a reference. A fit result that could be edited after
    the fact is a reference that can drift without a diff."""
    fit: DoseResponse = fit_dose_response(SERIES, curve(0.1))

    with pytest.raises(AttributeError):
        fit.ic50 = 999.0  # type: ignore[misc]
