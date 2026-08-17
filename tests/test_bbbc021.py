"""The BBBC021 reader (§7.1, §9.1).

These tests run on a synthetic CSV rather than the real 13 200-row file. Two reasons: the
real file lives on an external SSD that CI does not have, and a fixture makes it possible
to write down the rows that *should not* exist — a well holding two compounds, a row whose
three channels come from different sites. Those are the cases the reader is for.
"""

import csv
from collections.abc import Callable
from pathlib import Path

import pytest

from hcs.datasets.bbbc021 import (
    CHANNELS,
    PLATE_COLS,
    PLATE_ROWS,
    Bbbc021Error,
    plate_image_dir,
    read_moa_table,
    read_plates,
)

IMAGE_COLUMNS = [
    "TableNumber",
    "ImageNumber",
    "Image_FileName_DAPI",
    "Image_PathName_DAPI",
    "Image_FileName_Tubulin",
    "Image_PathName_Tubulin",
    "Image_FileName_Actin",
    "Image_PathName_Actin",
    "Image_Metadata_Plate_DAPI",
    "Image_Metadata_Well_DAPI",
    "Replicate",
    "Image_Metadata_Compound",
    "Image_Metadata_Concentration",
]

# The channel-to-suffix pairing of the real files. The reader never uses it — the channel
# name comes from the column — but the fixture has to look like the data it stands for.
SUFFIXES = {"DAPI": "w1", "Tubulin": "w2", "Actin": "w4"}

ImageCsv = Callable[[list[dict[str, str]]], Path]
MoaCsv = Callable[[list[tuple[str, float, str]]], Path]


def image_row(
    plate: str,
    well: str,
    site: int,
    compound: str,
    concentration: float,
    *,
    sites: dict[str, int] | None = None,
) -> dict[str, str]:
    """One CSV row. `sites` overrides the site of individual channels, to build the
    corrupt row no honest export would contain."""
    row = {
        "TableNumber": "1",
        "ImageNumber": "1",
        "Image_Metadata_Plate_DAPI": plate,
        "Image_Metadata_Well_DAPI": well,
        "Replicate": "2",
        "Image_Metadata_Compound": compound,
        "Image_Metadata_Concentration": f"{concentration:.6f}",
    }
    for channel, suffix in SUFFIXES.items():
        channel_site = (sites or {}).get(channel, site)
        row[f"Image_FileName_{channel}"] = f"{plate}_{well}_s{channel_site}_{suffix}ABCD.tif"
        row[f"Image_PathName_{channel}"] = f"Week1/{plate}"
    return row


@pytest.fixture
def image_csv(tmp_path: Path) -> ImageCsv:
    def write(rows: list[dict[str, str]]) -> Path:
        path = tmp_path / "BBBC021_v1_image.csv"
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=IMAGE_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        return path

    return write


@pytest.fixture
def moa_csv(tmp_path: Path) -> MoaCsv:
    def write(rows: list[tuple[str, float, str]]) -> Path:
        path = tmp_path / "BBBC021_v1_moa.csv"
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["compound", "concentration", "moa"])
            writer.writerows((name, f"{dose:.6f}", moa) for name, dose, moa in rows)
        return path

    return write


class TestFields:
    def test_one_row_becomes_one_field_with_every_channel(self, image_csv: ImageCsv) -> None:
        plates = read_plates(image_csv([image_row("P1", "B02", 1, "dmso", 0.0)]), {})

        (field,) = plates["P1"].fields
        assert field.well == "B02"
        assert {source.channel for source in field.files} == set(CHANNELS)

    def test_the_field_index_is_the_site_number_in_the_filename(self, image_csv: ImageCsv) -> None:
        """There is no `field_index` column. It comes from the `_s<N>_` infix, kept as
        written so the number in the database still finds the file on disk."""
        rows = [image_row("P1", "B02", site, "dmso", 0.0) for site in (1, 2, 3, 4)]

        plates = read_plates(image_csv(rows), {})

        assert sorted(f.field_index for f in plates["P1"].fields) == [1, 2, 3, 4]

    def test_channels_disagreeing_about_the_site_is_an_error(self, image_csv: ImageCsv) -> None:
        """The failure this check exists for: a field whose DNA channel is site 1 and
        whose tubulin is site 2 would segment one image and measure another."""
        rows = [image_row("P1", "B02", 1, "dmso", 0.0, sites={"Tubulin": 2})]

        with pytest.raises(Bbbc021Error, match="site"):
            read_plates(image_csv(rows), {})

    def test_a_filename_with_no_site_is_an_error(self, image_csv: ImageCsv) -> None:
        row = image_row("P1", "B02", 1, "dmso", 0.0)
        row["Image_FileName_DAPI"] = "P1_B02_w1ABCD.tif"

        with pytest.raises(Bbbc021Error, match="site"):
            read_plates(image_csv([row]), {})


class TestPlateGeometry:
    def test_the_plate_is_the_physical_one_not_the_imaged_region(self, image_csv: ImageCsv) -> None:
        """BBBC021 images the inner 60 wells of a 96-well plate; the perimeter is left out
        because edge wells evaporate. Recording 6x10 would erase that — the batch effect
        analysis in §9.4 reads position, and B02 is an edge of the imaged region while
        being an interior well of the plate."""
        plates = read_plates(image_csv([image_row("P1", "B02", 1, "dmso", 0.0)]), {})

        assert (plates["P1"].n_rows, plates["P1"].n_cols) == (PLATE_ROWS, PLATE_COLS)

    @pytest.mark.parametrize("well", ["I02", "B13", "B0", "b02", "B2"])
    def test_a_well_outside_the_plate_is_an_error(self, image_csv: ImageCsv, well: str) -> None:
        with pytest.raises(Bbbc021Error, match="well"):
            read_plates(image_csv([image_row("P1", well, 1, "dmso", 0.0)]), {})

    def test_only_the_requested_plates_are_read(self, image_csv: ImageCsv) -> None:
        rows = [
            image_row("P1", "B02", 1, "dmso", 0.0),
            image_row("P2", "B02", 1, "dmso", 0.0),
        ]

        plates = read_plates(image_csv(rows), {}, barcodes=["P1"])

        assert list(plates) == ["P1"]

    def test_asking_for_a_plate_that_is_not_there_is_an_error(self, image_csv: ImageCsv) -> None:
        """Silently returning nothing would let `make seed` report success over a typo."""
        rows = [image_row("P1", "B02", 1, "dmso", 0.0)]

        with pytest.raises(Bbbc021Error, match="P9"):
            read_plates(image_csv(rows), {}, barcodes=["P1", "P9"])


class TestWellLayout:
    def test_every_well_is_reported_once_with_its_treatment(self, image_csv: ImageCsv) -> None:
        rows = [image_row("P1", "B02", site, "taxol", 0.3) for site in (1, 2, 3, 4)]

        (well,) = read_plates(image_csv(rows), {})["P1"].wells

        assert (well.well, well.compound, well.concentration) == ("B02", "taxol", 0.3)

    def test_the_moa_is_looked_up_by_the_pair(self, image_csv: ImageCsv, moa_csv: MoaCsv) -> None:
        """The same rule as `control_type` (§9.1): the reference table gives a MoA to
        specific doses, and keying on the compound alone would label doses it omits."""
        moa = read_moa_table(moa_csv([("taxol", 0.3, "Microtubule stabilizer")]))
        rows = [
            image_row("P1", "B02", 1, "taxol", 0.3),
            image_row("P1", "B03", 1, "taxol", 3.0),
        ]

        wells = {w.well: w.moa for w in read_plates(image_csv(rows), moa)["P1"].wells}

        assert wells == {"B02": "Microtubule stabilizer", "B03": None}

    def test_a_well_holding_two_compounds_is_an_error(self, image_csv: ImageCsv) -> None:
        """A well has one treatment. If the export disagrees with itself, whichever row
        happened to be last would decide which wells are controls."""
        rows = [
            image_row("P1", "B02", 1, "dmso", 0.0),
            image_row("P1", "B02", 2, "taxol", 0.3),
        ]

        with pytest.raises(Bbbc021Error, match="B02"):
            read_plates(image_csv(rows), {})

    def test_the_reader_does_not_decide_what_is_a_control(self, image_csv: ImageCsv) -> None:
        """`control_type` is deliberately absent from `WellRecord`. The server derives it
        from the pair it stores, so no client — this seed included — can declare a well to
        be a control."""
        (well,) = read_plates(image_csv([image_row("P1", "B02", 1, "dmso", 0.0)]), {})["P1"].wells

        assert not hasattr(well, "control_type")


class TestImageDirectory:
    def test_the_path_comes_from_the_barcode_not_from_the_csv(self, tmp_path: Path) -> None:
        """`Image_PathName_DAPI` says `Week1/Week1_22141`, which describes the archive as
        published. What the download actually extracts to is
        `BBBC021_v1_images_<barcode>/<barcode>/`, so the CSV column is ignored."""
        expected = tmp_path / "BBBC021_v1_images_Week1_22141" / "Week1_22141"
        expected.mkdir(parents=True)

        assert plate_image_dir(tmp_path, "Week1_22141") == expected

    def test_a_missing_plate_directory_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(Bbbc021Error, match="Week1_22141"):
            plate_image_dir(tmp_path, "Week1_22141")
