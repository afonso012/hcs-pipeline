"""The Parquet writer, against frames `measure_field` actually produced (§6.3).

This file exists because of a specific failure. `measure_field` carries field-level facts
on `DataFrame.attrs`, and `to_parquet` JSON-encodes `attrs` into the file's metadata — so
putting a dataclass there raised `TypeError` on every write. Four hundred and eighty units
failed in a row while two hundred tests stayed green, because the state-machine tests stub
the imaging out and the imaging tests never serialised anything.

So the rule these tests encode: the writer is exercised on the real output of the real
pipeline, not on a frame assembled by hand to have the right columns.
"""

import io
import uuid
from typing import Any

import pandas as pd
import pytest

from hcs.imaging.pipeline import ChunkConfig, measure_field
from hcs.imaging.writer import object_key, serialise_cell_metrics, write_cell_metrics
from tests.test_segment import discs

RUN = uuid.UUID("11111111-1111-5111-8111-111111111111")
UNIT = uuid.UUID("22222222-2222-5222-8222-222222222222")


class FakeS3:
    """Enough of the client for the writer, and a record of what it was asked to do."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        self.objects[Key] = Body

    def copy_object(self, Bucket: str, Key: str, CopySource: dict[str, str]) -> None:  # noqa: N803
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, Bucket: str, Key: str) -> None:  # noqa: N803
        self.deleted.append(Key)
        self.objects.pop(Key, None)


@pytest.fixture
def cells() -> pd.DataFrame:
    """A frame straight out of the pipeline, attrs and all."""
    image = discs((256, 256), [(64, 64), (180, 180)])
    return measure_field(image, {"DNA": image}, ChunkConfig(chunk=128, overlap=32))


class TestItSurvivesRealFrames:
    def test_a_frame_from_measure_field_serialises(self, cells: pd.DataFrame) -> None:
        """The regression. `attrs` is JSON-encoded into Parquet metadata, so anything that
        is not a scalar there fails at write time and only at write time."""
        assert serialise_cell_metrics(cells, RUN, UNIT)

    def test_the_field_level_attrs_are_scalars(self, cells: pd.DataFrame) -> None:
        """Stated directly, because the failure was invisible in the frame itself — it only
        appeared when pandas tried to encode it."""
        import json

        json.dumps(cells.attrs)

    def test_an_empty_field_still_writes_a_file(self) -> None:
        """§6.3: an empty file and a missing file mean different things. And an empty frame
        takes the QC-rejected path, which carries different attrs."""
        blank = discs((128, 128), [])

        frame = measure_field(blank, {"DNA": blank}, ChunkConfig(chunk=64, overlap=16))

        assert frame.empty
        assert serialise_cell_metrics(frame, RUN, UNIT)


class TestTheFileItWrites:
    def test_the_schema_is_the_one_section_6_3_specifies(self, cells: pd.DataFrame) -> None:
        body = serialise_cell_metrics(cells, RUN, UNIT)

        written = pd.read_parquet(io.BytesIO(body))

        assert list(written.columns)[:3] == ["work_unit_id", "run_id", "label"]
        assert written["label"].dtype == "int32"
        assert written["touches_border"].dtype == "bool"
        assert all(written[name].dtype == "float32" for name in ("area", "centroid_y"))

    def test_the_rows_survive_the_round_trip(self, cells: pd.DataFrame) -> None:
        written = pd.read_parquet(io.BytesIO(serialise_cell_metrics(cells, RUN, UNIT)))

        assert len(written) == len(cells)
        assert written["label"].tolist() == list(range(1, len(cells) + 1))

    def test_the_key_is_partitioned_by_run_and_named_by_unit(self) -> None:
        assert object_key(RUN, UNIT) == f"cell_metrics/run_id={RUN}/work_unit_id={UNIT}.parquet"


class TestIdempotencyByFile:
    def test_it_stages_then_replaces_then_cleans_up(self, cells: pd.DataFrame) -> None:
        """§16.2: an interrupted upload leaves a stray temporary object rather than a
        truncated file that would read as a short unit."""
        s3: Any = FakeS3()

        key = write_cell_metrics(s3, "results", cells, RUN, UNIT)

        assert key == object_key(RUN, UNIT)
        assert list(s3.objects) == [key]
        assert len(s3.deleted) == 1
        assert s3.deleted[0].startswith(key)

    def test_a_rerun_that_finds_fewer_cells_leaves_no_orphans(self, cells: pd.DataFrame) -> None:
        """The property that made file-level idempotency the choice over DELETE plus
        UPSERT: the file is replaced whole, so a shorter second pass cannot leave rows from
        the first behind."""
        s3: Any = FakeS3()
        write_cell_metrics(s3, "results", cells, RUN, UNIT)

        write_cell_metrics(s3, "results", cells.head(1), RUN, UNIT)

        written = pd.read_parquet(io.BytesIO(s3.objects[object_key(RUN, UNIT)]))
        assert len(written) == 1
