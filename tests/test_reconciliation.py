"""The §16.4 reconciliation sweep.

> Um job periódico procura unidades em `PROCESSING` há mais tempo que
> `visibility_timeout` e reenfileira-as.

The safety net that makes Redis acceptable as a broker (ADR-001). Redis has no durable
queues, so a worker killed between claiming a unit and acknowledging it leaves the task
between the broker's redelivery timer and nowhere. Postgres is the source of truth, and
this is where recovery starts from it.

The chaos test of §13.4 is what made this necessary rather than tidy: killing one worker
left a unit in PROCESSING and the run never closed, 479 of 480.

Tested by forging the state a dead worker leaves behind rather than by killing a worker.
`scripts/chaos_test.py` does the killing; these tests pin the *rule*, and a test that
needs a container to die is a test that cannot run in CI.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from hcs.db import UnitRun, UnitStatus
from hcs.worker.tasks import reconcile_stuck_units
from tests.test_api_batches import create_batch, field, manifest

pytestmark = pytest.mark.integration


def stranded(engine: Engine, age_seconds: int) -> tuple[uuid.UUID, uuid.UUID]:
    """Put one unit into the state a killed worker leaves: claimed, never marked."""
    with Session(engine) as session:
        row = session.execute(select(UnitRun.work_unit_id, UnitRun.run_id)).first()
        assert row is not None, "the fixture registered no units"
        work_unit_id, run_id = row
        session.execute(
            UnitRun.__table__.update()
            .where(UnitRun.work_unit_id == work_unit_id, UnitRun.run_id == run_id)
            .values(
                status=UnitStatus.PROCESSING,
                started_at=datetime.now(UTC) - timedelta(seconds=age_seconds),
            )
        )
        session.commit()
    return work_unit_id, run_id


def status_of(engine: Engine, work_unit_id: uuid.UUID) -> UnitStatus:
    with Session(engine) as session:
        return session.scalars(
            select(UnitRun.status).where(UnitRun.work_unit_id == work_unit_id)
        ).one()


@pytest.fixture
def a_submitted_run(api_client: Any, no_image_work: dict[str, Any]) -> None:
    batch_id = create_batch(api_client)
    api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))
    api_client.post(f"/v1/batches/{batch_id}/submit")


class TestWhatItReclaims:
    def test_a_unit_stuck_past_the_timeout_is_re_enqueued(
        self, a_submitted_run: None, clean_db: Engine
    ) -> None:
        """The chaos test's broken invariant, as a unit test."""
        work_unit_id, _ = stranded(clean_db, age_seconds=7200)

        outcome = reconcile_stuck_units(older_than_seconds=3600)

        assert outcome["reclaimed"] == 1
        # Eager mode runs the republished task inline, so it comes back already finished.
        assert status_of(clean_db, work_unit_id) is UnitStatus.DONE

    def test_a_unit_claimed_moments_ago_is_left_alone(
        self, a_submitted_run: None, clean_db: Engine
    ) -> None:
        """The guard that matters most. A unit claimed two seconds ago is being worked on,
        and re-enqueuing it would put two workers on the same field — correct, by §16.2's
        idempotency, and twice the cost while the throughput metric lies about it."""
        work_unit_id, _ = stranded(clean_db, age_seconds=5)

        outcome = reconcile_stuck_units(older_than_seconds=3600)

        assert outcome["reclaimed"] == 0
        assert status_of(clean_db, work_unit_id) is UnitStatus.PROCESSING

    def test_finished_units_are_not_touched(self, a_submitted_run: None, clean_db: Engine) -> None:
        """The sweep looks only at PROCESSING. A DONE unit re-enqueued would be harmless
        and pointless, and `_claim` would refuse it anyway — but the query should not have
        found it in the first place."""
        outcome = reconcile_stuck_units(older_than_seconds=0)

        assert outcome["reclaimed"] == 0


class TestItIsSafeToRepeat:
    def test_running_it_twice_reclaims_once(self, a_submitted_run: None, clean_db: Engine) -> None:
        """A scheduled job runs forever. The second pass must find nothing, because the
        first moved the unit out of PROCESSING before republishing it — otherwise every
        sweep would queue the same unit again."""
        stranded(clean_db, age_seconds=7200)

        first = reconcile_stuck_units(older_than_seconds=3600)
        second = reconcile_stuck_units(older_than_seconds=3600)

        assert (first["reclaimed"], second["reclaimed"]) == (1, 0)

    def test_an_idle_system_reclaims_nothing(self, api_client: Any, clean_db: Engine) -> None:
        """The ordinary case, and the one that runs every two minutes forever.

        Takes `api_client` for the environment it configures, not for the client: the sweep
        reads settings, and CI has no `.env` for `get_settings()` to fall back on.
        """
        assert reconcile_stuck_units(older_than_seconds=3600)["reclaimed"] == 0
