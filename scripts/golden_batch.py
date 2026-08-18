#!/usr/bin/env python
"""The golden batch (spec §13.2).

> Qualquer alteração que desloque a contagem de células mais de 2% ou o IC50 mais de 10%
> falha o CI.

A complete dose series plus the plate's DMSO controls, from **Week9_39282** — a plate
outside the development set, so that tuning against `Week1_22141` cannot flatter it.
Mitoxantrone, because it is the only compound on that plate whose curve reaches its lower
asymptote: it falls to 3% of the DMSO baseline where the next best stops at 22%, and an
IC50 read off a curve that never descended is arithmetic rather than pharmacology.

Fifty-six fields: eight doses at four fields each, and six DMSO wells at four each. Not
twenty. Twenty fields would verify cell counts and nothing else — the IC50 the reference
guards needs the whole series, and normalising it needs the controls.

**The rule is "never tune while looking at it", not "never process".** Reprocessing the
golden batch is expected; what is forbidden is adjusting a threshold until its numbers
improve. When a deviation is intentional, `--freeze` writes the new reference **in the
same commit** as the change that caused it, and the diff of the reference becomes the
documentation of what moved.

Nothing here touches Postgres or S3. A scientific reference that depended on a database
would stop being reproducible the first time somebody ran `make down`.

    make golden          # verify against the committed reference
    make golden-freeze   # rewrite it, deliberately
"""

import argparse
import csv
import hashlib
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import numpy as np
import tifffile

from hcs.config import get_settings
from hcs.datasets.bbbc021 import PlateRecord, plate_image_dir, read_moa_table, read_plates
from hcs.imaging.pipeline import ChunkConfig, measure_field
from hcs.science.dose_response import DoseResponse, fit_dose_response

DEFAULT_ROOT: Final = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/BBBC021")
GOLDEN_DIR: Final = Path("tests/golden")
REFERENCE_CSV: Final = "reference.csv"
REFERENCE_JSON: Final = "reference.json"

PLATE: Final = "Week9_39282"
COMPOUND: Final = "mitoxantrone"
CONTROL: Final = "dmso"

# §13.2's threshold. A count that moves 2% is a segmentation change, and 2% is wide enough
# that no floating-point difference reaches it. The same bound serves the per-dose
# viability, which is a ratio of counts.
COUNT_TOLERANCE: Final = 0.02
VIABILITY_TOLERANCE: Final = 0.02

IC50_NOTE: Final = """§13.2 asks for an IC50 guarded at 10%. This plate cannot support one,
and the reason corrects the spec's reasoning rather than working around it.

The spec argued that twenty fields were too few for an IC50 and that forty-five to fifty
would do. The limiting quantity is not fields, it is **wells**. BBBC021 puts one well per
dose on a plate, and four fields of one well are subsamples of that well rather than
replicates of the dose — they share whatever the well did.

Measured, not assumed. Fitted to the eight well medians, the four-parameter logistic returns
an IC50 of 1.6e9 uM at R2 0.64; fitted to all thirty-two fields, 2.8e14 uM at R2 0.38; with
the top asymptote pinned at the DMSO baseline as §9.1's normalisation implies, 1.0e25 uM.
Adding the two Week9 replicate plates — the true replication §21.1 points to — lifts R2 to
0.77 and leaves the IC50 at 9.3e8.

The curve is the reason. Mitoxantrone is already at 65% of control at the lowest dose
tested, wanders between 35% and 54% across two orders of magnitude, then falls from 17% to
2%. That is two transitions, and one sigmoid cannot describe it. Restricting the dose range
until a fit succeeded would be tuning while looking at the answer, which §13.2 forbids by
name.

So the reference records the fit together with its own verdict on itself —
`within_tested_range` is false — and guards what is reproducible instead: total cells, every
field's count, and every dose's normalised viability. Those pin the pipeline harder than one
derived number would.

`hcs.science.dose_response` is tested against synthetic curves and is ready for a readout
that supports an IC50. §9.3's morphology readout is the candidate, and that is Phase 6.

## What this guard actually catches

Measured by mutating segmentation parameters and re-running, because a guard nobody has
tried to defeat is a guard of unknown strength.

- `min_size` 50 -> 45 moves the total by 0.11% and is **not** caught. A 10% change to the
  debris threshold barely touches the count, so the 2% bound is loose against this
  parameter. Stated rather than fixed: tightening the bound until this trips would be
  choosing a tolerance to catch the mutation in front of me.
- `min_distance` 10 -> 8, which changes how touching nuclei are split, **is** caught — and
  caught by the per-dose viability, not by the total. The total stayed inside 2% while the
  1.0 uM dose moved 2.8% and the 3.0 uM dose 6.9%.

That second result is the case the per-dose guard was written for, and it earned its place
the first time it was tested: §13.2 as originally written, guarding only the total, would
have let that change through.
"""

FIELDS: Final = ("well", "field_index", "compound", "concentration", "cells", "sha256")


@dataclass(frozen=True, slots=True)
class FieldResult:
    well: str
    field_index: int
    compound: str
    concentration: float
    cells: int
    sha256: str

    def as_row(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in FIELDS}


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    plate = _read_plate(args.root)
    measured = measure(plate, args.root)
    summary = summarise(measured)

    if args.freeze:
        freeze(measured, summary, args.out)
        print(f"\nfroze {len(measured)} field(s) into {args.out}", file=sys.stderr)
        return 0

    return 0 if compare(measured, summary, args.out) else 1


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify or freeze the golden batch.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out", type=Path, default=GOLDEN_DIR)
    parser.add_argument(
        "--freeze",
        action="store_true",
        help="rewrite the reference; do this in the same commit as the change that moved it",
    )
    return parser.parse_args(argv)


# --- measuring ------------------------------------------------------------------------------


def _read_plate(root: Path) -> PlateRecord:
    moa = read_moa_table(root / "BBBC021_v1_moa.csv")
    return read_plates(root / "BBBC021_v1_image.csv", moa, [PLATE])[PLATE]


def golden_wells(plate: PlateRecord) -> dict[str, tuple[str, float]]:
    """The wells the reference is built from: one compound's whole series, plus controls."""
    return {
        well.well: (well.compound or "", well.concentration or 0.0)
        for well in plate.wells
        if (well.compound or "").strip().lower() in {COMPOUND, CONTROL}
    }


def measure(plate: PlateRecord, root: Path) -> list[FieldResult]:
    """Segment every golden field, in a fixed order.

    Sorted by `(well, field_index)` so the reference file is a stable diff. An unordered
    reference would show every row as changed the first time a dictionary iterated
    differently, and nobody would read the diff again after that.
    """
    settings = get_settings()
    chunks = ChunkConfig(chunk=settings.chunk_size, overlap=settings.chunk_overlap)
    wells = golden_wells(plate)
    directory = plate_image_dir(root, plate.barcode)

    results = []
    for field in plate.fields:
        if field.well not in wells:
            continue
        compound, concentration = wells[field.well]
        pixels, digest = _read_channels(directory, field)
        cells = measure_field(pixels[settings.segmentation_channel], pixels, chunks)
        results.append(
            FieldResult(
                well=field.well,
                field_index=field.field_index,
                compound=compound,
                concentration=concentration,
                cells=len(cells),
                sha256=digest,
            )
        )
        print(f"  {field.well} s{field.field_index}: {len(cells):>4} cells", file=sys.stderr)
    return sorted(results, key=lambda r: (r.well, r.field_index))


def _read_channels(directory: Path, field: Any) -> tuple[dict[str, Any], str]:
    """Load one field's channels, and a digest that identifies the pixels behind the row.

    The digest covers all three files in channel order. It is what makes a reference row
    falsifiable: if a number moves, this says whether the pixels moved with it, and a
    re-download that silently changed a file cannot masquerade as a regression in the
    code.
    """
    combined = hashlib.sha256()
    pixels = {}
    for source in field.files:
        path = directory / source.filename
        data = path.read_bytes()
        combined.update(hashlib.sha256(data).digest())
        pixels[source.channel] = tifffile.imread(path)
    return pixels, combined.hexdigest()


# --- summarising ------------------------------------------------------------------------------


def summarise(measured: Sequence[FieldResult]) -> dict[str, Any]:
    """Per-well medians, the DMSO baseline, and the IC50 fitted from them.

    The median per well and not the mean, for the reason §9.1 gives: field-to-field
    distributions are skewed and contaminated by segmentation artefacts, and one blown
    field would drag a mean.
    """
    per_well = _per_well(measured)
    baseline = _baseline(per_well)
    doses = sorted(
        (concentration, cells)
        for (compound, concentration), cells in per_well.items()
        if compound.strip().lower() == COMPOUND
    )
    viability = [100.0 * cells / baseline for _, cells in doses]
    fit = fit_dose_response([dose for dose, _ in doses], viability)

    return {
        "plate": PLATE,
        "compound": COMPOUND,
        "fields": len(measured),
        "total_cells": sum(result.cells for result in measured),
        "dmso_cells_per_well": round(baseline, 3),
        "doses": [{"concentration": d, "cells": round(c, 3)} for d, c in doses],
        "viability_pct": [round(v, 3) for v in viability],
        "fit": _fit_as_dict(fit),
        # Recorded, deliberately not guarded. `tests/golden/IC50.md` says why, and the
        # fit's own `within_tested_range` says the same thing in one boolean.
        "fit_is_guarded": False,
        "tolerances": {"cells": COUNT_TOLERANCE, "viability": VIABILITY_TOLERANCE},
    }


def _per_well(measured: Sequence[FieldResult]) -> dict[tuple[str, float], float]:
    grouped: dict[tuple[str, float], list[int]] = {}
    for result in measured:
        grouped.setdefault((result.compound, result.concentration), []).append(result.cells)
    return {key: float(np.median(counts)) for key, counts in grouped.items()}


def _baseline(per_well: dict[tuple[str, float], float]) -> float:
    controls = [
        cells for (compound, _), cells in per_well.items() if compound.strip().lower() == CONTROL
    ]
    if not controls:
        raise ValueError(f"{PLATE} has no {CONTROL} wells; the series cannot be normalised")
    return float(np.mean(controls))


def _fit_as_dict(fit: DoseResponse) -> dict[str, Any]:
    return {
        "ic50": round(fit.ic50, 6),
        "hill": round(fit.hill, 4),
        "top": round(fit.top, 3),
        "bottom": round(fit.bottom, 3),
        "r_squared": round(fit.r_squared, 5),
        "within_tested_range": fit.within_tested_range,
    }


# --- freezing and comparing --------------------------------------------------------------------


def freeze(measured: Sequence[FieldResult], summary: dict[str, Any], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "IC50.md").write_text(IC50_NOTE)
    with (out / REFERENCE_CSV).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(FIELDS))
        writer.writeheader()
        writer.writerows(result.as_row() for result in measured)
    (out / REFERENCE_JSON).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def compare(measured: Sequence[FieldResult], summary: dict[str, Any], out: Path) -> bool:
    reference = json.loads((out / REFERENCE_JSON).read_text())
    drifts = list(drift(summary, reference))

    for line in drifts:
        print(f"DRIFT: {line}", file=sys.stderr)
    if drifts:
        print(
            "\nIf this was intended, re-freeze in the same commit as the change that caused "
            "it, so the reference diff documents it.",
            file=sys.stderr,
        )
        return False

    print(
        f"golden batch holds: {summary['total_cells']} cells over {summary['fields']} fields, "
        f"{len(summary['viability_pct'])} doses within {VIABILITY_TOLERANCE:.0%}",
        file=sys.stderr,
    )
    return True


def drift(summary: dict[str, Any], reference: dict[str, Any]) -> Any:
    """What moved, and by how much. Empty means the reference holds.

    Three guards, all on numbers the pipeline produces deterministically from fixed pixels.
    The IC50 is recorded but not guarded — see `IC50.md`, written beside the reference.
    """
    if summary["fields"] != reference["fields"]:
        yield f"field count: {reference['fields']} -> {summary['fields']}"

    yield from _drifted(
        "total cells", summary["total_cells"], reference["total_cells"], COUNT_TOLERANCE
    )

    # Per dose, because a total can hold still while the curve underneath it rotates.
    for dose, now, before in zip(
        reference["doses"], summary["viability_pct"], reference["viability_pct"], strict=False
    ):
        yield from _drifted(
            f"viability at {dose['concentration']} uM", now, before, VIABILITY_TOLERANCE
        )


def _drifted(label: str, now: float, before: float, tolerance: float) -> Any:
    if before == 0:
        return
    moved = abs(now - before) / abs(before)
    if moved > tolerance:
        yield f"{label} moved {moved:.1%} ({before} -> {now}), tolerance {tolerance:.0%}"


if __name__ == "__main__":
    raise SystemExit(main())
