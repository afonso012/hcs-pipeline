"""Which wells are controls (spec §9.1).

The rule the whole plate normalisation rests on: **a control is a (compound,
concentration) pair, never a compound.** Taxol appears on `Week1_22141` at 0.03, 0.1,
0.3, 1 and 3 µM. Exactly one of those is the positive control; the rest are ordinary
points on taxol's own dose curve. Marking by compound would pull the entire curve into
the control set, μ_pos would become an average over five doses, and Z' would stop
measuring assay quality and start measuring the spread of a dose response.

The same reasoning applies to the vehicle. DMSO is the negative control at concentration
zero; DMSO at a non-zero concentration would be a treatment, not a vehicle. Keying both
on the pair keeps one rule instead of two.
"""

import enum
import math
from typing import Final


class ControlType(enum.StrEnum):
    NEGATIVE = "NEGATIVE"
    POSITIVE = "POSITIVE"


# (compound, concentration in µM). Lower-cased for comparison; the stored compound keeps
# whatever case the source CSV used.
NEGATIVE_CONTROL: Final[tuple[str, float]] = ("dmso", 0.0)
POSITIVE_CONTROL: Final[tuple[str, float]] = ("taxol", 0.3)

# Concentrations arrive from a CSV as decimal strings, so equality has to tolerate the
# float that "0.300000" parses to. A tolerance this tight cannot merge two real dose
# points: the series is separated by factors of about three.
CONCENTRATION_TOLERANCE: Final = 1e-6


def control_type(compound: str | None, concentration: float | None) -> ControlType | None:
    """Classify one well. `None` means "a treatment", which is most of the plate."""
    if compound is None or concentration is None:
        return None
    if _matches(NEGATIVE_CONTROL, compound, concentration):
        return ControlType.NEGATIVE
    if _matches(POSITIVE_CONTROL, compound, concentration):
        return ControlType.POSITIVE
    return None


def _matches(control: tuple[str, float], compound: str, concentration: float) -> bool:
    name, dose = control
    if compound.strip().lower() != name:
        return False
    return math.isclose(concentration, dose, rel_tol=CONCENTRATION_TOLERANCE, abs_tol=1e-12)
