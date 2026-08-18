"""Fitting a dose-response curve and reading an IC50 off it (spec §9.3, §13.2).

The four-parameter logistic, which is what a dose-response is:

    response(x) = bottom + (top - bottom) / (1 + (x / ic50) ** hill)

`ic50` is the concentration at the midpoint between the fitted asymptotes — the *relative*
IC50, not the absolute one. The distinction matters and gets glossed over: the relative
IC50 is halfway between what this compound achieved at its top and bottom, and it is only
a half-kill if the curve actually reaches zero. A compound whose curve flattens at 60% of
control has a relative IC50 that is a real number and a meaningless one, which is why
`fit_dose_response` reports the span it fitted and lets the caller judge.

The controls do not enter the fit. DMSO sits at concentration zero, and zero has no place
on a log-concentration axis; it is the reference the response is expressed against, which
is §9.3's rule that normalisation is always against the plate's own negative controls.
"""

import math
from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt
from scipy.optimize import curve_fit

# Below four points a four-parameter fit has no degrees of freedom left and will happily
# return whatever it was initialised with.
MIN_POINTS: Final = 4

# The optimiser needs a starting point, and a bad one is how these fits fail silently.
# The Hill slope starts at 1 (an ordinary sigmoid) and the IC50 at the geometric middle of
# the doses tested, which is the only prior worth having.
INITIAL_HILL: Final = 1.0
MAX_ITERATIONS: Final = 10_000


class DoseResponseError(ValueError):
    """The curve could not be fitted, or was never fittable."""


@dataclass(frozen=True, slots=True)
class DoseResponse:
    """A fitted curve, with enough of the fit exposed to distrust it.

    `r_squared` and `span` are here so that a caller can refuse a number rather than
    quoting it. An IC50 from a curve that never descended is arithmetic, not pharmacology.
    """

    ic50: float
    hill: float
    top: float
    bottom: float
    r_squared: float
    n_points: int

    @property
    def span(self) -> float:
        """How far the response actually fell, in the units it was given."""
        return self.top - self.bottom

    @property
    def within_tested_range(self) -> bool:
        """Whether the IC50 is interpolated or extrapolated — set by the caller's doses."""
        return self._in_range

    _in_range: bool = True


def four_parameter_logistic(
    concentration: npt.NDArray[np.float64], bottom: float, top: float, ic50: float, hill: float
) -> npt.NDArray[np.float64]:
    """The model. Written out rather than imported so the parameters mean what they say."""
    return bottom + (top - bottom) / (1.0 + (concentration / ic50) ** hill)


def fit_dose_response(
    concentrations: npt.NDArray[np.float64] | list[float],
    responses: npt.NDArray[np.float64] | list[float],
) -> DoseResponse:
    """Fit the four-parameter logistic and return its IC50.

    Args:
        concentrations: Doses, strictly positive. Controls belong in the normalisation of
            `responses`, not here — a zero would make `(x / ic50) ** hill` undefined.
        responses: The readout at each dose, in whatever units the caller normalised to.

    Raises:
        DoseResponseError: fewer than four points, a non-positive concentration, or an
            optimiser that did not converge.
    """
    doses = np.asarray(concentrations, dtype=np.float64)
    values = np.asarray(responses, dtype=np.float64)

    if doses.size != values.size:
        raise DoseResponseError(f"{doses.size} concentrations against {values.size} responses")
    if doses.size < MIN_POINTS:
        raise DoseResponseError(
            f"{doses.size} points cannot constrain four parameters; need {MIN_POINTS}"
        )
    if np.any(doses <= 0):
        raise DoseResponseError("concentrations must be positive; the control is not a dose")

    guess = (float(values.min()), float(values.max()), _geometric_middle(doses), INITIAL_HILL)
    try:
        fitted, _ = curve_fit(
            four_parameter_logistic, doses, values, p0=guess, maxfev=MAX_ITERATIONS
        )
    except (RuntimeError, TypeError) as exc:
        raise DoseResponseError(f"the fit did not converge: {exc}") from exc

    bottom, top, ic50, hill = (float(value) for value in fitted)
    if not math.isfinite(ic50) or ic50 <= 0:
        raise DoseResponseError(f"the fit returned a nonsensical IC50: {ic50}")

    return DoseResponse(
        ic50=ic50,
        hill=hill,
        top=top,
        bottom=bottom,
        r_squared=_r_squared(values, four_parameter_logistic(doses, bottom, top, ic50, hill)),
        n_points=int(doses.size),
        _in_range=bool(doses.min() <= ic50 <= doses.max()),
    )


def _geometric_middle(doses: npt.NDArray[np.float64]) -> float:
    """The midpoint of a log-spaced series, which is where a sigmoid's centre usually is.

    An arithmetic mean would start the search near the top dose of a series spanning four
    orders of magnitude, and the optimiser would spend its iterations walking back down.
    """
    return float(np.exp(np.log(doses).mean()))


def _r_squared(observed: npt.NDArray[np.float64], predicted: npt.NDArray[np.float64]) -> float:
    """Coefficient of determination. Zero when the response never varied at all."""
    total = float(((observed - observed.mean()) ** 2).sum())
    if total == 0.0:
        return 0.0
    residual = float(((observed - predicted) ** 2).sum())
    return 1.0 - residual / total
