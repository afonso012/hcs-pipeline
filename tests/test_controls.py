"""The third property of §13.1: `control_type` is never assigned by compound.

The other two properties were testable from Phase 1 because they are geometry. This one
needed a plate layout to exist, and it does now. It is the last of the three.

The number that would go wrong is Z'. With taxol marked positive at every concentration,
the positive mean becomes an average across a five-point dose curve and the positive
standard deviation becomes that curve's spread — so Z' would fall as the assay got
*better* at separating doses.
"""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hcs.science.controls import POSITIVE_CONTROL, ControlType, control_type

# The taxol series actually present on Week1_22141, from BBBC021_v1_image.csv.
TAXOL_SERIES = (0.03, 0.1, 0.3, 1.0, 3.0)


class TestTheSpecProperty:
    def test_exactly_one_taxol_concentration_is_positive(self) -> None:
        """§13.1, stated verbatim: given a layout with taxol at N concentrations, exactly
        one comes out POSITIVE."""
        marked = [control_type("taxol", dose) for dose in TAXOL_SERIES]

        assert marked.count(ControlType.POSITIVE) == 1
        assert marked[TAXOL_SERIES.index(POSITIVE_CONTROL[1])] is ControlType.POSITIVE

    @given(dose=st.sampled_from([d for d in TAXOL_SERIES if d != POSITIVE_CONTROL[1]]))
    def test_every_other_taxol_dose_is_a_treatment(self, dose: float) -> None:
        assert control_type("taxol", dose) is None


class TestTheVehicle:
    def test_dmso_at_zero_is_the_negative_control(self) -> None:
        assert control_type("DMSO", 0.0) is ControlType.NEGATIVE

    def test_dmso_at_a_real_concentration_is_a_treatment(self) -> None:
        """Keyed on the pair like everything else: DMSO carrying a dose is not a vehicle."""
        assert control_type("DMSO", 1.0) is None

    @pytest.mark.parametrize("spelling", ["DMSO", "dmso", " Dmso "])
    def test_the_spelling_in_the_csv_does_not_decide_it(self, spelling: str) -> None:
        assert control_type(spelling, 0.0) is ControlType.NEGATIVE


class TestEverythingElse:
    @given(
        compound=st.text(min_size=1, max_size=30).filter(
            lambda s: s.strip().lower() not in {"dmso", "taxol"}
        ),
        concentration=st.floats(min_value=0.0, max_value=100.0, allow_nan=False),
    )
    def test_no_other_compound_is_ever_a_control(self, compound: str, concentration: float) -> None:
        assert control_type(compound, concentration) is None

    @pytest.mark.parametrize(("compound", "concentration"), [(None, 0.3), ("taxol", None)])
    def test_a_missing_half_of_the_pair_classifies_nothing(
        self, compound: str | None, concentration: float | None
    ) -> None:
        """Wells with no annotation exist. Guessing from half a pair is how a blank well
        ends up in the control set."""
        assert control_type(compound, concentration) is None

    def test_a_concentration_that_only_looks_like_the_control_is_not_one(self) -> None:
        """0.3 and 0.33 are adjacent in a log series; they must not merge."""
        assert control_type("taxol", 0.33) is None
