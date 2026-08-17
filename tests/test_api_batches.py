"""The batch lifecycle of §7, end to end against a real PostgreSQL.

The last test in this file is the goal of Phase 1 stated as an assertion: a unit enters
one side and comes out marked on the other.
"""

import uuid
from typing import Any

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from hcs.db import Batch, BatchStatus, HcsField, Run, RunStatus, UnitRun, UnitStatus, WorkUnit
from hcs.identity import hcs_field_id

pytestmark = pytest.mark.integration

BARCODE = "Week1_22141"
CHANNELS = ("DAPI", "Tubulin", "Actin")
DIGEST = "a1b2c3d4" * 8  # 64 hex characters, not a hash of anything real


def field(well: str, index: int, channels: tuple[str, ...] = CHANNELS) -> dict[str, Any]:
    return {
        "well": well,
        "field_index": index,
        "channels": {
            name: {"uri": f"s3://raw/{BARCODE}_{well}_s{index}_{name}.tif", "sha256": DIGEST}
            for name in channels
        },
    }


def manifest(*units: dict[str, Any], barcode: str = BARCODE) -> dict[str, Any]:
    return {
        "kind": "HCS_FIELD",
        "plate": {
            "barcode": barcode,
            "n_rows": 8,
            "n_cols": 12,
            "expected_channels": list(CHANNELS),
        },
        "units": list(units),
    }


def create_batch(client: Any) -> str:
    response = client.post("/v1/batches", json={"external_ref": "week1_dev"})
    assert response.status_code == 201
    batch_id: str = response.json()["batch_id"]
    return batch_id


class TestBatchCreation:
    def test_a_new_batch_is_a_draft_with_no_units(self, api_client: Any) -> None:
        body = api_client.post("/v1/batches", json={"external_ref": "week1_dev"}).json()
        assert body["status"] == BatchStatus.DRAFT
        assert body["total_units"] == 0
        assert body["external_ref"] == "week1_dev"

    def test_an_unknown_batch_is_404_not_500(self, api_client: Any) -> None:
        assert api_client.get(f"/v1/batches/{uuid.uuid4()}").status_code == 404


class TestManifest:
    def test_registering_fields_counts_them_on_the_batch(self, api_client: Any) -> None:
        batch_id = create_batch(api_client)
        response = api_client.post(
            f"/v1/batches/{batch_id}/manifest",
            json=manifest(field("B04", 1), field("B04", 2), field("C05", 1)),
        )

        assert response.status_code == 202
        assert response.json() == {"units_registered": 3, "duplicates_skipped": 0, "incomplete": 0}
        assert api_client.get(f"/v1/batches/{batch_id}").json()["total_units"] == 3

    def test_sending_the_same_manifest_twice_changes_nothing(self, api_client: Any) -> None:
        """§7.1: re-runnable by construction. The second pass is all duplicates."""
        batch_id = create_batch(api_client)
        payload = manifest(field("B04", 1), field("B04", 2))
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=payload)

        second = api_client.post(f"/v1/batches/{batch_id}/manifest", json=payload).json()

        assert second == {"units_registered": 0, "duplicates_skipped": 2, "incomplete": 0}
        assert api_client.get(f"/v1/batches/{batch_id}").json()["total_units"] == 2

    def test_the_unit_id_is_the_one_identity_predicts(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        """The API stores the deterministic id, not one of its own invention.

        Without this, a seed script could not compute a unit's id offline, and the
        idempotency above would be a property of one code path rather than of the data.
        """
        batch_id = create_batch(api_client)
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))

        with Session(clean_db) as session:
            stored = session.scalars(select(WorkUnit.id)).all()
        assert list(stored) == [hcs_field_id(BARCODE, "B04", 1)]

    def test_a_field_missing_a_channel_is_reported_and_not_registered(
        self, api_client: Any
    ) -> None:
        """A field is rejected whole (§5): two channels out of three are not two thirds of
        a measurement, and registering it would put a unit into `total_units` that can
        never be processed."""
        batch_id = create_batch(api_client)
        response = api_client.post(
            f"/v1/batches/{batch_id}/manifest",
            json=manifest(field("B04", 1), field("B04", 2, channels=("DAPI", "Actin"))),
        )

        assert response.json() == {"units_registered": 1, "duplicates_skipped": 0, "incomplete": 1}
        assert api_client.get(f"/v1/batches/{batch_id}").json()["total_units"] == 1

    def test_a_manifest_for_an_unknown_batch_is_404(self, api_client: Any) -> None:
        response = api_client.post(
            f"/v1/batches/{uuid.uuid4()}/manifest", json=manifest(field("B04", 1))
        )
        assert response.status_code == 404

    @pytest.mark.parametrize(
        ("mutation", "reason"),
        [
            ({"units": []}, "no units"),
            (
                {"plate": {"barcode": BARCODE, "n_rows": 8, "n_cols": 12, "expected_channels": []}},
                "no expected channels",
            ),
            ({"kind": "WSI_SLIDE"}, "wrong rail for this body"),
        ],
    )
    def test_malformed_manifests_are_rejected_at_the_boundary(
        self, api_client: Any, mutation: dict[str, Any], reason: str
    ) -> None:
        batch_id = create_batch(api_client)
        payload = manifest(field("B04", 1)) | mutation
        assert api_client.post(f"/v1/batches/{batch_id}/manifest", json=payload).status_code == 422

    def test_a_malformed_digest_never_reaches_the_database(self, api_client: Any) -> None:
        batch_id = create_batch(api_client)
        payload = manifest(field("B04", 1))
        payload["units"][0]["channels"]["DAPI"]["sha256"] = "not-a-digest"
        assert api_client.post(f"/v1/batches/{batch_id}/manifest", json=payload).status_code == 422


class TestOverlappingBatches:
    def test_the_same_plate_can_go_into_a_second_batch(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        """The scientific set includes plates the development batch already used.

        Before membership moved to `batch_units`, this registered nothing: the units
        existed, so the second batch came out empty and its submit answered 409. A run
        over 43 plates would quietly have been a run over 41.
        """
        development = create_batch(api_client)
        scientific = create_batch(api_client)
        payload = manifest(field("B04", 1), field("B04", 2))

        api_client.post(f"/v1/batches/{development}/manifest", json=payload)
        second = api_client.post(f"/v1/batches/{scientific}/manifest", json=payload).json()

        assert second == {"units_registered": 2, "duplicates_skipped": 0, "incomplete": 0}
        assert api_client.get(f"/v1/batches/{scientific}").json()["total_units"] == 2
        assert api_client.post(f"/v1/batches/{scientific}/submit").status_code == 202
        with Session(clean_db) as session:
            assert session.scalar(select(func.count()).select_from(WorkUnit)) == 2

    def test_two_runs_over_a_shared_unit_keep_separate_state(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        development = create_batch(api_client)
        scientific = create_batch(api_client)
        payload = manifest(field("B04", 1))
        api_client.post(f"/v1/batches/{development}/manifest", json=payload)
        api_client.post(f"/v1/batches/{scientific}/manifest", json=payload)

        api_client.post(f"/v1/batches/{development}/submit")
        api_client.post(f"/v1/batches/{scientific}/submit")

        with Session(clean_db) as session:
            # One unit, two runs, two rows of state. The primary key of unit_runs already
            # allowed this, which is why sharing a unit needed no change there.
            assert session.scalar(select(func.count()).select_from(WorkUnit)) == 1
            assert session.scalar(select(func.count()).select_from(UnitRun)) == 2


class TestPlateIsReferenceData:
    def test_a_plate_cannot_be_redeclared_with_different_channels(self, api_client: Any) -> None:
        """409, not 422: the body is well formed, it contradicts the record.

        Letting it through would redefine "complete" for fields already accepted under
        the previous definition, and `incomplete` would start counting against a moving
        target.
        """
        batch_id = create_batch(api_client)
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))

        contradiction = manifest(field("B04", 1))
        contradiction["plate"]["expected_channels"] = ["DAPI", "Tubulin"]
        response = api_client.post(f"/v1/batches/{batch_id}/manifest", json=contradiction)

        assert response.status_code == 409
        assert "Week1_22141" in response.json()["detail"]

    def test_the_channel_order_declared_is_not_part_of_the_plate(self, api_client: Any) -> None:
        """Same set, different order, is the same declaration — not a contradiction."""
        batch_id = create_batch(api_client)
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))

        reordered = manifest(field("B04", 1))
        reordered["plate"]["expected_channels"] = list(reversed(CHANNELS))
        assert (
            api_client.post(f"/v1/batches/{batch_id}/manifest", json=reordered).status_code == 202
        )


class TestSubmit:
    def test_submitting_an_empty_batch_is_409(self, api_client: Any) -> None:
        batch_id = create_batch(api_client)
        assert api_client.post(f"/v1/batches/{batch_id}/submit").status_code == 409

    def test_a_run_records_the_lineage_it_needs_to_be_reproducible(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        """§12: pipeline version, config hash and lockfile hash, none of them null."""
        batch_id = create_batch(api_client)
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))
        run_id = api_client.post(f"/v1/batches/{batch_id}/submit").json()["run_id"]

        with Session(clean_db) as session:
            run = session.get(Run, uuid.UUID(run_id))
            assert run is not None
            assert run.pipeline_version
            assert len(run.config_hash) == 64
            assert len(run.lockfile_hash) == 64
            assert run.config["chunk_overlap"] == 32
            assert run.total_units == 1

    def test_resubmitting_creates_a_second_run_beside_the_first(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        """ADR-006: reprocessing does not overwrite what the previous run observed."""
        batch_id = create_batch(api_client)
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))
        first = api_client.post(f"/v1/batches/{batch_id}/submit").json()["run_id"]
        second = api_client.post(f"/v1/batches/{batch_id}/submit").json()["run_id"]

        assert first != second
        with Session(clean_db) as session:
            assert session.scalar(select(func.count()).select_from(Run)) == 2
            assert session.scalar(select(func.count()).select_from(UnitRun)) == 2

    def test_a_unit_enters_one_side_and_comes_out_marked_on_the_other(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        """The goal of Phase 1 (§20), as one assertion.

        Manifest, submit, worker, marked unit, closed run. The task does no image work
        yet — that is Phase 2 — but every piece of state around it is real.
        """
        batch_id = create_batch(api_client)
        api_client.post(
            f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1), field("B04", 2))
        )
        submitted = api_client.post(f"/v1/batches/{batch_id}/submit").json()

        with Session(clean_db) as session:
            statuses = session.scalars(
                select(UnitRun.status).where(UnitRun.run_id == uuid.UUID(submitted["run_id"]))
            ).all()
            run = session.get(Run, uuid.UUID(submitted["run_id"]))
            batch = session.get(Batch, uuid.UUID(batch_id))
            fields = session.scalar(select(func.count()).select_from(HcsField))

        assert submitted["total_units"] == 2
        assert list(statuses) == [UnitStatus.DONE, UnitStatus.DONE]
        assert run is not None
        assert run.status is RunStatus.COMPLETED
        assert (run.processed_units, run.failed_units) == (2, 0)
        assert run.finished_at is not None
        assert batch is not None and batch.status is BatchStatus.READY
        assert fields == 2
