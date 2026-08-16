"""The task that moves a unit through a run (spec §16.2, §16.3).

Phase 1 does no image work at all. What it does is establish the state machine Phase 2
will hang segmentation off: claim the unit, mark it, count it, and close the run when the
last unit lands. The goal of Phase 1 is that a unit enters one side and comes out marked
on the other, and this is the "comes out marked" half.

Idempotency is by construction (§16.2). Both arguments are stable, the state write is an
UPDATE keyed on `(work_unit_id, run_id)`, and re-running a unit that is already DONE is a
no-op rather than a double count — which is what makes it safe for the reconciliation
sweep of §16.4 to re-enqueue anything it finds stuck.
"""

import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, cast

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import CursorResult, Engine, create_engine, select, update
from sqlalchemy.orm import Session

from hcs.config import get_settings
from hcs.db import Run, RunStatus, UnitRun, UnitStatus
from hcs.worker.celery_app import celery_app

logger = logging.getLogger(__name__)

# Failures worth retrying: the network, S3, a timeout. Everything else is permanent, and
# retrying a corrupt file three times is pure waste (§16.3).
TRANSIENT_ERRORS = (OSError, ConnectionError, TimeoutError)

MAX_STORED_ERROR_CHARS = 2000


@lru_cache
def _engine() -> Engine:
    """One engine per worker process, created on first use.

    Not at import time: this module is imported by the API too, and an import that dials
    Postgres turns a database blip into a failure to start.
    """
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True, pool_size=2)


@contextmanager
def session_scope() -> Iterator[Session]:
    with Session(bind=_engine()) as session:
        yield session


@celery_app.task(
    bind=True,
    name="hcs.process_unit",
    max_retries=3,
    autoretry_for=TRANSIENT_ERRORS,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
def process_unit(self: Any, work_unit_id: str, run_id: str) -> str:
    """Process one work unit within one run.

    Returns the terminal status for the logs. Nothing reads the return value — there is
    no result backend, because `unit_runs` is the source of truth (ADR-001).
    """
    unit = uuid.UUID(work_unit_id)
    run = uuid.UUID(run_id)

    with session_scope() as session:
        if not _claim(session, unit, run):
            logger.info("unit %s already terminal in run %s, skipping", unit, run)
            return UnitStatus.DONE.value

    try:
        # Phase 2 replaces this line with the chunked read, the segmentation and the
        # Parquet write. Everything around it — the claim, the counters, the run closing
        # — is what Phase 1 is proving works.
        pass
    except SoftTimeLimitExceeded:
        _fail(unit, run, "soft time limit exceeded", permanent=False)
        raise
    except TRANSIENT_ERRORS as exc:
        _fail(unit, run, str(exc), permanent=False)
        raise
    except Exception as exc:  # a corrupt file, a missing channel, an invalid format
        _fail(unit, run, str(exc), permanent=True)
        return UnitStatus.FAILED.value

    with session_scope() as session:
        session.execute(
            update(UnitRun)
            .where(UnitRun.work_unit_id == unit, UnitRun.run_id == run)
            .values(status=UnitStatus.DONE, processed_at=datetime.now(UTC))
        )
        session.execute(
            update(Run).where(Run.id == run).values(processed_units=Run.processed_units + 1)
        )
        _close_run_if_finished(session, run)
        session.commit()
    return UnitStatus.DONE.value


def _claim(session: Session, unit: uuid.UUID, run: uuid.UUID) -> bool:
    """Move the unit to PROCESSING, unless it has already finished.

    The `status.in_(...)` guard is what makes a redelivered task harmless: Redis can
    redeliver, the reconciliation sweep can re-enqueue, and neither produces a second
    pass over a unit that is already DONE.
    """
    result = cast(
        "CursorResult[Any]",
        session.execute(
            update(UnitRun)
            .where(
                UnitRun.work_unit_id == unit,
                UnitRun.run_id == run,
                UnitRun.status.in_([UnitStatus.PENDING, UnitStatus.PROCESSING]),
            )
            .values(
                status=UnitStatus.PROCESSING,
                started_at=datetime.now(UTC),
                attempts=UnitRun.attempts + 1,
            )
        ),
    )
    session.commit()
    return bool(result.rowcount)


def _fail(unit: uuid.UUID, run: uuid.UUID, message: str, *, permanent: bool) -> None:
    """Record the failure in its own transaction, so a re-raise cannot roll it back."""
    with session_scope() as session:
        session.execute(
            update(UnitRun)
            .where(UnitRun.work_unit_id == unit, UnitRun.run_id == run)
            .values(
                # A transient failure goes back to PENDING: the retry has to be able to
                # claim it again, and the claim only accepts PENDING or PROCESSING.
                status=UnitStatus.FAILED if permanent else UnitStatus.PENDING,
                last_error=message[:MAX_STORED_ERROR_CHARS],
                error_class="PERMANENT" if permanent else "TRANSIENT",
                processed_at=datetime.now(UTC) if permanent else None,
            )
        )
        if permanent:
            session.execute(
                update(Run).where(Run.id == run).values(failed_units=Run.failed_units + 1)
            )
            _close_run_if_finished(session, run)
        session.commit()
    logger.warning("unit %s in run %s failed (permanent=%s): %s", unit, run, permanent, message)


def _close_run_if_finished(session: Session, run_id: uuid.UUID) -> None:
    """Close the run once every unit has landed.

    PARTIAL, not FAILED, when some units failed: failing ten thousand units over three
    corrupt files would be absurd (§16.4).
    """
    total, processed, failed = session.execute(
        select(Run.total_units, Run.processed_units, Run.failed_units).where(Run.id == run_id)
    ).one()
    if processed + failed < total:
        return
    session.execute(
        update(Run)
        .where(Run.id == run_id)
        .values(
            status=RunStatus.PARTIAL if failed else RunStatus.COMPLETED,
            finished_at=datetime.now(UTC),
        )
    )
