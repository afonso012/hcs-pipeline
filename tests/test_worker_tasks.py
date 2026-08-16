"""Failure semantics of the unit task (spec §16.2, §16.3).

§16.3 claims the transient/permanent distinction is verifiable. These are the checks that
make that true: a corrupt file must not be retried three times, a network blip must be,
and a unit that already finished must not be counted twice when the reconciliation sweep
of §16.4 re-enqueues it.
"""

from typing import Any

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from hcs.db import Run, RunStatus, UnitRun, UnitStatus
from hcs.worker import tasks

pytestmark = pytest.mark.integration

BARCODE = "Week1_22141"
DIGEST = "a1b2c3d4" * 8


def raises(exception: Exception) -> Any:
    """A stand-in for `perform` that fails. Lambdas cannot raise."""

    def _fail(*_: object) -> None:
        raise exception

    return _fail


def submit(client: Any, units: int = 2) -> None:
    """Register `units` fields and submit them. Tasks run inline under `api_client`."""
    batch_id = client.post("/v1/batches", json={"external_ref": "failures"}).json()["batch_id"]
    client.post(
        f"/v1/batches/{batch_id}/manifest",
        json={
            "kind": "HCS_FIELD",
            "plate": {"barcode": BARCODE, "n_rows": 8, "n_cols": 12},
            "required_channels": ["DAPI"],
            "units": [
                {
                    "well": "B04",
                    "field_index": index,
                    "channels": {"DAPI": {"uri": f"s3://raw/{index}.tif", "sha256": DIGEST}},
                }
                for index in range(units)
            ],
        },
    )
    client.post(f"/v1/batches/{batch_id}/submit")


def the_run(engine: Engine) -> Run:
    with Session(engine) as session:
        return session.scalars(select(Run)).one()


def unit_rows(engine: Engine) -> list[UnitRun]:
    with Session(engine) as session:
        return list(session.scalars(select(UnitRun).order_by(UnitRun.work_unit_id)).all())


class TestPermanentFailure:
    def test_a_corrupt_file_fails_once_and_is_not_retried(
        self, api_client: Any, clean_db: Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retrying a corrupt file three times is pure waste (§16.3).

        `attempts == 1` is the observable form of that: the claim ran once and nothing
        re-queued it.
        """
        monkeypatch.setattr(tasks, "perform", raises(ValueError("truncated TIFF")))
        submit(api_client, units=2)

        rows = unit_rows(clean_db)
        assert [row.status for row in rows] == [UnitStatus.FAILED, UnitStatus.FAILED]
        assert {row.error_class for row in rows} == {"PERMANENT"}
        assert {row.attempts for row in rows} == {1}
        assert "truncated TIFF" in (rows[0].last_error or "")

    def test_a_run_whose_units_all_failed_closes_as_partial_not_failed(
        self, api_client: Any, clean_db: Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failing ten thousand units over three corrupt files would be absurd (§16.4)."""
        monkeypatch.setattr(tasks, "perform", raises(ValueError("bad")))
        submit(api_client, units=2)

        run = the_run(clean_db)
        assert run.status is RunStatus.PARTIAL
        assert (run.processed_units, run.failed_units) == (0, 2)
        assert run.finished_at is not None


class TestTransientFailure:
    def test_a_network_blip_returns_the_unit_to_pending(
        self, api_client: Any, clean_db: Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PENDING, not FAILED: the retry has to be able to claim it again, and the claim
        only accepts PENDING or PROCESSING. Leaving it PROCESSING would make it
        indistinguishable from a unit a killed worker abandoned, and the reconciliation
        sweep would pick it up twice.
        """
        monkeypatch.setattr(tasks, "perform", raises(ConnectionError("minio unreachable")))
        # Eager mode propagates: the retry Celery would schedule surfaces here instead.
        with pytest.raises(Exception, match=r"minio unreachable|Retry"):
            submit(api_client, units=1)

        rows = unit_rows(clean_db)
        assert [row.status for row in rows] == [UnitStatus.PENDING]
        assert rows[0].error_class == "TRANSIENT"
        assert the_run(clean_db).failed_units == 0


class TestIdempotency:
    def test_reprocessing_a_finished_unit_does_not_count_it_twice(
        self, api_client: Any, clean_db: Engine
    ) -> None:
        """§16.2. This is what makes it safe for §16.4 to re-enqueue anything stuck: the
        sweep cannot tell a lost unit from a slow one, so re-running a finished unit has
        to be free."""
        submit(api_client, units=1)
        before = the_run(clean_db)
        unit_id = unit_rows(clean_db)[0].work_unit_id

        tasks.process_unit(str(unit_id), str(before.id))

        after = the_run(clean_db)
        assert (after.processed_units, after.failed_units) == (
            before.processed_units,
            before.failed_units,
        )
        assert unit_rows(clean_db)[0].status is UnitStatus.DONE
