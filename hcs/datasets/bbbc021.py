"""Reading BBBC021 v1 into the pipeline's own vocabulary (§7.1).

Two CSVs describe the dataset. `BBBC021_v1_image.csv` has one row per imaging field and
names its three files; `BBBC021_v1_moa.csv` gives a mechanism of action to a subset of the
treatments. Neither is the pipeline's schema, and nothing outside this module should know
that `Image_Metadata_Plate_DAPI` exists.

Three details of the source are worth stating, because each is a place where a
reasonable-looking reader would be wrong:

- **The channel name is the column name.** `Image_FileName_DAPI` names the DAPI file. The
  `w1`/`w2`/`w4` infix inside the filename says the same thing in a code that has to be
  looked up, so this reader never reads it.
- **There is no field index column.** It is the `_s<N>_` infix, which §6.2 sanctions.
- **`Image_PathName_*` is not where the files are.** It records the archive layout as
  published (`Week1/Week1_22141`); the download extracts to
  `BBBC021_v1_images_<barcode>/<barcode>/`. Following the column finds nothing.
"""

import csv
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

# The three stains, in the order the assay names them. The DNA channel comes first because
# it is the one segmentation runs on; the other two are measured inside its mask (§8.2).
CHANNELS: Final = ("DAPI", "Tubulin", "Actin")

# BBBC021 is imaged on 96-well plates, of which only the inner 60 wells (B-G, 2-11) carry
# samples: the perimeter evaporates over the course of the assay and is left empty. The
# plate recorded is the physical one, not the imaged region — §9.4 reads well position to
# look for spatial batch effects, and a 6x10 plate would move every well's coordinates.
PLATE_ROWS: Final = 8
PLATE_COLS: Final = 12

_WELL_RE: Final = re.compile(r"\A([A-H])(\d{2})\Z")
_SITE_RE: Final = re.compile(r"_s(\d+)_w\d")

# Both CSVs write concentrations with six decimals. Rounding to that before using one as a
# dictionary key keeps the lookup from depending on how a decimal string happened to
# parse, without merging doses — the series is separated by factors of about three.
_CONCENTRATION_PLACES: Final = 6


class Bbbc021Error(ValueError):
    """The source data contradicts itself, or is not what this reader expects."""


@dataclass(frozen=True)
class ChannelSource:
    """One file: which channel it is and what it is called on disk."""

    channel: str
    filename: str


@dataclass(frozen=True)
class FieldRecord:
    well: str
    field_index: int
    files: tuple[ChannelSource, ...]


@dataclass(frozen=True)
class WellRecord:
    """What a well contains.

    Deliberately without a `control_type`. Whether a well is a control is decided by
    `hcs.science.controls` on the server, from the pair stored here, so the rule of §9.1
    lives in one place and no client can assert its way around it.
    """

    well: str
    compound: str | None
    concentration: float | None
    moa: str | None


@dataclass(frozen=True)
class PlateRecord:
    barcode: str
    n_rows: int
    n_cols: int
    wells: tuple[WellRecord, ...]
    fields: tuple[FieldRecord, ...]


MoaTable = Mapping[tuple[str, float], str]


def read_moa_table(path: Path) -> dict[tuple[str, float], str]:
    """Load the mechanism-of-action reference, keyed by `(compound, concentration)`.

    Keyed by the pair for the same reason `control_type` is (§9.1): the table annotates
    particular doses. Most treatments on a plate are absent from it — on `Week1_22141`, 14
    of 49 pairs are covered — and a missing MoA is missing, not inferable from another
    dose of the same compound.
    """
    with path.open(newline="") as handle:
        return {
            (row["compound"].strip().lower(), _concentration(row["concentration"])): row["moa"]
            for row in csv.DictReader(handle)
        }


def read_plates(
    image_csv: Path, moa: MoaTable, barcodes: Iterable[str] | None = None
) -> dict[str, PlateRecord]:
    """Read the image table into one `PlateRecord` per plate.

    `barcodes` selects a subset and is checked: asking for a plate the CSV does not
    contain raises rather than quietly returning less than was asked for, so a typo in
    `make seed` cannot look like a successful run over fewer plates.
    """
    wanted = None if barcodes is None else list(dict.fromkeys(barcodes))

    fields: dict[str, list[FieldRecord]] = {}
    treatments: dict[str, dict[str, tuple[str, float]]] = {}

    with image_csv.open(newline="") as handle:
        for row in csv.DictReader(handle):
            barcode = row["Image_Metadata_Plate_DAPI"].strip()
            if wanted is not None and barcode not in wanted:
                continue
            well = _well(row["Image_Metadata_Well_DAPI"].strip(), barcode)
            fields.setdefault(barcode, []).append(_field(row, well, barcode))
            _record_treatment(treatments.setdefault(barcode, {}), barcode, well, row)

    if wanted is not None:
        missing = [barcode for barcode in wanted if barcode not in fields]
        if missing:
            raise Bbbc021Error(f"{image_csv} has no rows for: {', '.join(missing)}")

    order = wanted if wanted is not None else sorted(fields)
    return {
        barcode: PlateRecord(
            barcode=barcode,
            n_rows=PLATE_ROWS,
            n_cols=PLATE_COLS,
            wells=_wells(treatments[barcode], moa),
            fields=tuple(sorted(fields[barcode], key=lambda f: (f.well, f.field_index))),
        )
        for barcode in order
    }


def plate_image_dir(root: Path, barcode: str) -> Path:
    """Where the TIFFs of one plate actually are, derived from the barcode.

    Raises if the directory is absent: the alternative is a manifest full of paths that
    resolve to nothing, discovered one upload at a time.
    """
    directory = root / f"BBBC021_v1_images_{barcode}" / barcode
    if not directory.is_dir():
        raise Bbbc021Error(f"no images for {barcode}: {directory} does not exist")
    return directory


def _field(row: Mapping[str, str], well: str, barcode: str) -> FieldRecord:
    files = tuple(
        ChannelSource(channel=channel, filename=row[f"Image_FileName_{channel}"].strip())
        for channel in CHANNELS
    )
    sites = {source.channel: _site(source.filename, barcode, well) for source in files}
    if len(set(sites.values())) != 1:
        raise Bbbc021Error(
            f"{barcode} {well}: the channels of one row disagree about the site: {sites}"
        )
    return FieldRecord(well=well, field_index=next(iter(sites.values())), files=files)


def _site(filename: str, barcode: str, well: str) -> int:
    match = _SITE_RE.search(filename)
    if match is None:
        raise Bbbc021Error(f"{barcode} {well}: no site in the filename {filename!r}")
    return int(match.group(1))


def _well(well: str, barcode: str) -> str:
    match = _WELL_RE.match(well)
    if match is None:
        raise Bbbc021Error(f"{barcode}: {well!r} is not a well label like 'B02'")
    row_index = ord(match.group(1)) - ord("A")
    column = int(match.group(2))
    if row_index >= PLATE_ROWS or not 1 <= column <= PLATE_COLS:
        raise Bbbc021Error(f"{barcode}: well {well} is outside a {PLATE_ROWS}x{PLATE_COLS} plate")
    return well


def _record_treatment(
    seen: dict[str, tuple[str, float]], barcode: str, well: str, row: Mapping[str, str]
) -> None:
    """Accumulate one well's treatment, refusing to let two rows disagree about it.

    Four rows describe each well, one per site, and all four must name the same treatment.
    If they did not, whichever row was read last would decide which wells are controls.
    """
    treatment = (
        row["Image_Metadata_Compound"].strip(),
        _concentration(row["Image_Metadata_Concentration"]),
    )
    previous = seen.setdefault(well, treatment)
    if previous != treatment:
        raise Bbbc021Error(f"{barcode}: well {well} is both {previous} and {treatment}")


def _wells(treatments: Mapping[str, tuple[str, float]], moa: MoaTable) -> tuple[WellRecord, ...]:
    return tuple(
        WellRecord(
            well=well,
            compound=compound or None,
            concentration=concentration,
            moa=moa.get((compound.strip().lower(), concentration)),
        )
        for well, (compound, concentration) in sorted(treatments.items())
    )


def _concentration(value: str) -> float:
    return round(float(value), _CONCENTRATION_PLACES)
