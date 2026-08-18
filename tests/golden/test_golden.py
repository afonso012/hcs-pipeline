"""The golden batch, as a test (§13.2).

Two layers, because they fail for different reasons and only one of them can run anywhere.

**The reference is self-consistent.** Recompute the summary from the per-field counts
committed to this repository and check it still matches the summary committed beside them.
No pixels needed, so this runs in CI, and it catches drift in the aggregation and the curve
fit — a change to the median-per-well rule, or to `dose_response`, moves these numbers with
no image involved.

**The pixels still give those counts.** Re-segment the fifty-six fields and compare. This is
the guard §13.2 is really about, and it needs the TIFFs, which live on the SSD and not in
the repository — the reference is small and versioned, the images are not. So it skips where
the data is absent and runs under `make golden`.

The gap is real and worth naming: CI cannot fail on a segmentation change today. Closing it
needs the golden TIFFs reachable from a runner, which is an infrastructure decision rather
than a code one.
"""

import csv
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.golden_batch import (
    COUNT_TOLERANCE,
    DEFAULT_ROOT,
    VIABILITY_TOLERANCE,
    FieldResult,
    _read_plate,
    drift,
    measure,
    summarise,
)

GOLDEN = Path(__file__).parent
REFERENCE_CSV = GOLDEN / "reference.csv"
REFERENCE_JSON = GOLDEN / "reference.json"

HAVE_IMAGES = (DEFAULT_ROOT / "BBBC021_v1_image.csv").exists()


def stored_fields() -> list[FieldResult]:
    """The committed per-field counts, as the objects `summarise` consumes."""
    with REFERENCE_CSV.open() as handle:
        return [
            FieldResult(
                well=row["well"],
                field_index=int(row["field_index"]),
                compound=row["compound"],
                concentration=float(row["concentration"]),
                cells=int(row["cells"]),
                sha256=row["sha256"],
            )
            for row in csv.DictReader(handle)
        ]


@pytest.fixture(scope="module")
def reference() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(REFERENCE_JSON.read_text())
    return loaded


class TestTheReferenceIsSelfConsistent:
    """Runs anywhere, including CI. Guards the analysis, not the segmentation."""

    def test_the_summary_follows_from_the_counts(self, reference: dict[str, Any]) -> None:
        """Every derived number recomputed from the committed rows. A change to the
        median-per-well rule of §9.1, or to the curve fit, moves these without a pixel
        being read."""
        assert not list(drift(summarise(stored_fields()), reference))

    def test_the_committed_rows_are_the_batch_the_spec_asks_for(self) -> None:
        """A complete dose series plus the plate's controls, from a plate outside the
        development set — not twenty arbitrary fields (§13.2)."""
        rows = stored_fields()
        compounds = {row.compound.strip().lower() for row in rows}
        doses = {row.concentration for row in rows if row.compound.strip().lower() != "dmso"}

        assert compounds == {"mitoxantrone", "dmso"}
        assert len(doses) == 8
        assert len(rows) == 56

    def test_the_ic50_is_recorded_and_declared_unguarded(self, reference: dict[str, Any]) -> None:
        """§13.2 asks for an IC50 guarded at 10%; this plate cannot support one, and
        `tests/golden/IC50.md` sets out why. The reference says so in two places rather
        than staying quiet about it."""
        assert reference["fit_is_guarded"] is False
        assert reference["fit"]["within_tested_range"] is False
        assert (GOLDEN / "IC50.md").exists()

    def test_every_field_carries_the_digest_of_its_pixels(self) -> None:
        """What makes a row falsifiable. If a count moves, the digest says whether the
        images moved with it — a re-download that silently changed a file cannot pass as a
        regression in the code."""
        rows = stored_fields()

        assert all(len(row.sha256) == 64 for row in rows)
        assert len({row.sha256 for row in rows}) == len(rows)


@pytest.mark.golden
@pytest.mark.skipif(not HAVE_IMAGES, reason=f"no BBBC021 at {DEFAULT_ROOT}")
class TestThePixelsStillGiveThoseCounts:
    """The guard §13.2 is about. Needs the TIFFs, so it does not run in CI."""

    @pytest.fixture(scope="class")
    def measured(self) -> list[FieldResult]:
        return measure(_read_plate(DEFAULT_ROOT), DEFAULT_ROOT)

    def test_nothing_drifted(self, measured: list[FieldResult], reference: dict[str, Any]) -> None:
        """Total cells within 2%, and every dose's viability within 2%.

        When this fails on purpose, re-freeze in the same commit as the change that caused
        it, so the reference diff is the documentation of what moved.
        """
        assert not list(drift(summarise(measured), reference))

    def test_no_single_field_drifted(self, measured: list[FieldResult]) -> None:
        """A total can hold still while fields move in opposite directions. This is the
        check that says which field, which is what makes a failure debuggable."""
        stored = {(row.well, row.field_index): row.cells for row in stored_fields()}

        moved = [
            f"{row.well} s{row.field_index}: {stored[(row.well, row.field_index)]} -> {row.cells}"
            for row in measured
            if abs(row.cells - stored[(row.well, row.field_index)])
            > max(1, COUNT_TOLERANCE * stored[(row.well, row.field_index)])
        ]

        assert not moved

    def test_the_pixels_are_the_pixels_the_reference_was_built_from(
        self, measured: list[FieldResult]
    ) -> None:
        """Checked before blaming the code. A changed digest means the data changed, and
        no amount of reading this repository's diff would have said so."""
        stored = {(row.well, row.field_index): row.sha256 for row in stored_fields()}

        assert all(stored[(row.well, row.field_index)] == row.sha256 for row in measured)


def test_the_tolerances_are_the_ones_the_spec_names() -> None:
    """§13.2 sets 2% on the cell count. Loosening it quietly is exactly the drift the golden
    batch exists to prevent, so the number is asserted rather than trusted."""
    assert COUNT_TOLERANCE == 0.02
    assert VIABILITY_TOLERANCE == 0.02
