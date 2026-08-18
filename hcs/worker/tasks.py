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
from hcs.db import Run, RunStatus, UnitKind, UnitRun, UnitStatus, WorkUnit
from hcs.services.units import (
    IncompleteUnitError,
    UnitNotFoundError,
    load_hcs_field,
    load_wsi_slide,
)
from hcs.storage import s3_client
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


def perform(work_unit_id: uuid.UUID, run_id: uuid.UUID) -> dict[str, Any]:
    """Fetch, segment, measure and write one unit (§8.1, §8.2, §6.3).

    Returns a small QC summary, stored on `unit_runs.qc`. It is the cheapest possible
    answer to "what did this unit actually do" — a count and a key — and it is what makes
    a run inspectable without opening 480 Parquet files.

    The order matters. The Parquet write happens before the DONE mark, so a worker killed
    between the two leaves a unit that gets reprocessed and overwrites its own file (§6.3),
    rather than one marked DONE with nothing behind it.

    The imaging stack is imported here rather than at module scope, and that is a
    deployment constraint rather than a style choice. The API imports this module to
    enqueue, and the API container deliberately does not install the `imaging` extra —
    scikit-image, scipy and the rest belong to the worker. A module-level import would
    make `POST /submit` fail with an ImportError for a library the API has no business
    holding. The worker pays the cost once, on its first unit.
    """
    from hcs.imaging.writer import write_cell_metrics

    settings = get_settings()
    with session_scope() as session:
        kind = session.scalar(select(WorkUnit.kind).where(WorkUnit.id == work_unit_id))
    if kind is None:
        raise UnitNotFoundError(f"no work unit with id {work_unit_id}")

    cells = _measure_hcs(work_unit_id) if kind is UnitKind.HCS_FIELD else _measure_wsi(work_unit_id)
    key = write_cell_metrics(s3_client(), settings.bucket_results, cells, run_id, work_unit_id)

    return {
        "kind": kind.value,
        "cells": len(cells),
        "touching_border": int(cells["touches_border"].sum()),
        "results_key": key,
    }


def _measure_hcs(work_unit_id: uuid.UUID) -> Any:
    """One HCS field: three channels fetched whole, segmented on the DNA channel (§8.2)."""
    from hcs.imaging.pipeline import ChunkConfig, measure_field
    from hcs.imaging.source import load_field, segmentation_channel

    settings = get_settings()
    with session_scope() as session:
        field = load_hcs_field(session, work_unit_id)

    pixels = load_field(s3_client(), field.channels)
    return measure_field(
        segmentation_channel(pixels, settings.segmentation_channel),
        pixels,
        ChunkConfig(chunk=settings.chunk_size, overlap=settings.chunk_overlap),
    )


def _measure_wsi(work_unit_id: uuid.UUID) -> Any:
    """One slide, level 0, block by block.

    The slide is staged to a local file first, and that is a constraint rather than a
    preference: OpenSlide reads a path, not a stream, and it seeks — it reads the tiles a
    region needs and skips the rest, which is the whole reason a gigapixel level costs a
    block of memory. Handing it a buffer would mean holding the file to avoid holding the
    image, which is the wrong trade in the wrong direction.

    So one slide's bytes land on local disk, and nothing the size of the image ever lands
    in memory. §21.3's negative control is the measurement of that distinction: the same
    level loaded whole is OOMKilled at 1536m, and this path finishes.

    The temporary file is deleted whatever happens. A worker that leaked one slide per
    unit would fill its container's writable layer long before it ran out of memory.
    """
    import tempfile
    from pathlib import Path

    from hcs.imaging.pipeline import ChunkConfig, measure_field
    from hcs.imaging.wsi import measure_at, open_slide, thumbnail_threshold
    from hcs.storage import read_object

    settings = get_settings()
    with session_scope() as session:
        slide_source = load_wsi_slide(session, work_unit_id)

    with tempfile.NamedTemporaryFile(suffix=Path(slide_source.slide_uri).suffix) as staged:
        staged.write(read_object(s3_client(), slide_source.slide_uri))
        staged.flush()
        slide = open_slide(staged.name)
        try:
            level = measure_at(slide)
            return measure_field(
                level,
                {"Hematoxylin": level},
                ChunkConfig(chunk=settings.chunk_size, overlap=settings.chunk_overlap),
                threshold=thumbnail_threshold(slide),
            )
        finally:
            slide.close()


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
        qc = perform(unit, run)
    except SoftTimeLimitExceeded:
        _fail(unit, run, "soft time limit exceeded", permanent=False)
        raise
    except IncompleteUnitError as exc:
        # Not a failure of the pipeline: the plate says this field should have three
        # channels and storage has two. It is terminal, and it is the only place
        # `unit_status.INCOMPLETE` can ever be written (§6.2).
        _fail(unit, run, str(exc), permanent=True, status=UnitStatus.INCOMPLETE)
        return UnitStatus.INCOMPLETE.value
    except TRANSIENT_ERRORS as exc:
        _fail(unit, run, str(exc), permanent=False)
        raise
    except Exception as exc:  # a corrupt file, an unreadable format, a bad shape
        _fail(unit, run, str(exc), permanent=True)
        return UnitStatus.FAILED.value

    with session_scope() as session:
        session.execute(
            update(UnitRun)
            .where(UnitRun.work_unit_id == unit, UnitRun.run_id == run)
            .values(status=UnitStatus.DONE, processed_at=datetime.now(UTC), qc=qc)
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


def _fail(
    unit: uuid.UUID,
    run: uuid.UUID,
    message: str,
    *,
    permanent: bool,
    status: UnitStatus | None = None,
) -> None:
    """Record the failure in its own transaction, so a re-raise cannot roll it back.

    `status` overrides the terminal state for the one case that is permanent without being
    a failure of the code: a unit missing a channel is INCOMPLETE. It still counts towards
    `failed_units`, because otherwise a run containing one would never close.
    """
    with session_scope() as session:
        session.execute(
            update(UnitRun)
            .where(UnitRun.work_unit_id == unit, UnitRun.run_id == run)
            .values(
                # A transient failure goes back to PENDING: the retry has to be able to
                # claim it again, and the claim only accepts PENDING or PROCESSING.
                status=status or (UnitStatus.FAILED if permanent else UnitStatus.PENDING),
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
