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
import resource
import socket
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any, cast

from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import worker_ready
from sqlalchemy import CursorResult, Engine, create_engine, select, update
from sqlalchemy.orm import Session

from hcs.config import get_settings
from hcs.db import Run, RunStatus, UnitKind, UnitRun, UnitStatus, WorkUnit
from hcs.metrics import (
    CELLS_DETECTED,
    DLQ,
    QC_REJECTED,
    RECONCILED,
    TASK_DURATION,
    TASK_RETRIES,
    UNIT_BYTES,
    WORKER_RSS,
)
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

# How long the dispatcher waits before looking at the queue again. Short enough that a
# draining queue is refilled promptly, long enough that a full one is not polled in a
# tight loop.
DISPATCH_RETRY_SECONDS = 5

# How long to wait for workers to answer an `inspect active` broadcast.
INSPECT_TIMEOUT_SECONDS = 5

# Port the worker exposes its metrics on. Celery is not an HTTP server, so the process
# starts a tiny one of its own; `--concurrency=1` is what makes that safe, because a
# prefork pool would have every child racing for the same socket.
METRICS_PORT = 9100


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


@worker_ready.connect
def _serve_metrics(**_: Any) -> None:
    """Expose §14.1's worker metrics once the worker is actually up.

    On `worker_ready` rather than at import: this module is imported by the API too, and an
    import that bound a port would have two processes fighting over it for nothing.

    **Aggregated across the pool, which is the part that is easy to get wrong.** Celery's
    prefork pool runs every task in a forked child, and this server runs in the parent — so
    a counter incremented by a task lands in the child's registry and is never exported.
    `--concurrency=1` does not help: it means one child, not one process. The symptom is
    silent: the endpoint answers, the dashboard renders, and every worker series reads
    zero, which is what 480 processed units looked like before this was fixed.

    `prometheus_client`'s multiprocess mode is the fix: each process writes its samples to
    a shared directory and the exporter sums the files at scrape time.
    """
    from prometheus_client import CollectorRegistry, multiprocess, start_http_server

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    start_http_server(METRICS_PORT, registry=registry)
    _record_rss("init")
    logger.info("worker metrics on :%d (multiprocess)", METRICS_PORT)


def _record_rss(phase: str) -> None:
    """Resident set size, labelled by phase (§14.2 panel 2).

    Init and steady are separated because the interesting number is the difference: the
    imaging stack costs ~186 MiB the moment it is imported, and reporting one figure would
    make every worker look heavy regardless of what it was asked to do.
    """
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes. The workers run on Linux; the guard is for a
    # developer running the task directly on a Mac and wondering why the number is absurd.
    scale = 1 if sys.platform == "darwin" else 1024
    WORKER_RSS.labels(worker_id=socket.gethostname(), phase=phase).set(usage * scale)


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

    # §14.2 panel 2 plots memory against this. Decoded bytes rather than file bytes,
    # because a compressed slide and an uncompressed field of the same file size are not
    # the same amount of work — and work is what the panel is asking about.
    accepted = bool(cells.attrs.get("qc_accepted", True))
    UNIT_BYTES.labels(kind=kind.value).observe(cells.attrs.get("input_bytes", 0))
    CELLS_DETECTED.inc(len(cells))
    if not accepted:
        QC_REJECTED.labels(reason="empty_field").inc()
    _record_rss("steady")

    return {
        "kind": kind.value,
        "cells": len(cells),
        "touching_border": int(cells["touches_border"].sum()),
        "results_key": key,
        "qc_accepted": accepted,
        "qc_foreground_fraction": round(float(cells.attrs.get("qc_foreground_fraction", 0.0)), 4),
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

    started = time.perf_counter()
    try:
        qc = perform(unit, run)
    except SoftTimeLimitExceeded:
        _observe(started, UnitStatus.PENDING)
        TASK_RETRIES.labels(task=self.name, error_class="TRANSIENT").inc()
        _fail(unit, run, "soft time limit exceeded", permanent=False)
        raise
    except IncompleteUnitError as exc:
        # Not a failure of the pipeline: the plate says this field should have three
        # channels and storage has two. It is terminal, and it is the only place
        # `unit_status.INCOMPLETE` can ever be written (§6.2).
        _observe(started, UnitStatus.INCOMPLETE)
        _fail(unit, run, str(exc), permanent=True, status=UnitStatus.INCOMPLETE)
        return UnitStatus.INCOMPLETE.value
    except TRANSIENT_ERRORS as exc:
        _observe(started, UnitStatus.PENDING)
        TASK_RETRIES.labels(task=self.name, error_class="TRANSIENT").inc()
        if self.request.retries >= self.max_retries:
            # Out of retries: this is what the DLQ counter of §14.1 counts, and a rising
            # line here is a broker or a storage problem rather than a data one.
            DLQ.inc()
        _fail(unit, run, str(exc), permanent=False)
        raise
    except Exception as exc:  # a corrupt file, an unreadable format, a bad shape
        _observe(started, UnitStatus.FAILED)
        TASK_RETRIES.labels(task=self.name, error_class="PERMANENT").inc()
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
    _observe(started, UnitStatus.DONE)
    return UnitStatus.DONE.value


def _observe(started: float, status: UnitStatus) -> None:
    """Record how long the unit took and how it ended.

    Labelled by outcome because the distributions differ by more than noise: a permanent
    failure is fast, a success is slow, and averaging them would describe neither.
    """
    TASK_DURATION.labels(task="hcs.process_unit", status=status.value).observe(
        time.perf_counter() - started
    )


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


@celery_app.task(name="hcs.dispatch_run", bind=True, max_retries=None)
def dispatch_run(self: Any, run_id: str, batch_id: str, offset: int = 0) -> str:
    """Publish one run's units a block at a time, holding the queue between the marks.

    A task rather than a thread in the API, for three reasons. It survives an API restart,
    which a thread does not. It carries no state beyond its own arguments, so a redelivery
    re-publishes a block that `process_unit` will find already done and skip (§16.2). And
    it puts the waiting where the workers are, so `POST /submit` still answers in
    milliseconds — which is objective A.

    The offset walks a deterministic order (`units_in_batch` sorts by id), so block N is
    the same block on every attempt.
    """
    from hcs.services.queue import next_block, queue_depth, watermarks
    from hcs.services.runs import units_in_batch

    settings = get_settings()
    marks = watermarks(settings)
    with session_scope() as session:
        unit_ids = units_in_batch(session, uuid.UUID(batch_id))

    depth = queue_depth(settings)
    block = next_block(unit_ids, offset, marks, depth)

    if not block and offset < len(unit_ids):
        # The queue is still above the low mark. Come back rather than publishing into it;
        # `countdown` is what keeps this from becoming a spin loop against Redis.
        raise self.retry(countdown=DISPATCH_RETRY_SECONDS)

    for unit_id in block:
        process_unit.delay(str(unit_id), run_id)

    published = offset + len(block)
    if published < len(unit_ids):
        dispatch_run.apply_async(args=[run_id, batch_id, published], countdown=0)
        return f"dispatched {published}/{len(unit_ids)}"

    logger.info("run %s fully dispatched: %d units", run_id, len(unit_ids))
    return f"dispatched {len(unit_ids)}/{len(unit_ids)}"


@celery_app.task(name="hcs.reconcile")
def reconcile(older_than_seconds: int | None = None) -> dict[str, Any]:
    """Re-enqueue units a dead worker left claimed (§16.4).

    > Um job periódico procura unidades em `PROCESSING` há mais tempo que
    > `visibility_timeout` e reenfileira-as.

    This is the safety net that makes Redis acceptable as a broker (ADR-001). Redis has no
    durable queues: a worker killed between claiming a unit and acknowledging it leaves the
    task somewhere between the broker's redelivery timer and nowhere. Postgres is the source
    of truth and the queue is only transport, so the truth is where the recovery starts.

    The chaos test of §13.4 is what made this non-optional rather than nice to have. Killing
    one worker left a unit in PROCESSING and the run never closed — 479 of 480 — because
    recovery otherwise waits for a `visibility_timeout` set to an hour.

    **The cutoff is "nobody is running it", not "the broker gave up".** §16.4 says to wait
    out `visibility_timeout`, and taken literally that is an hour — while §22 asks for zero
    stuck units five minutes after a run ends. The two cannot both be satisfied by a timer,
    and the chaos test showed it: with an hour-long cutoff the sweep correctly declined to
    touch a unit that had been dead for fifteen minutes.

    So the sweep asks the workers what they are actually executing. A unit that is
    PROCESSING and that no live worker holds is not being worked on, whatever any timer
    says. The broker's timeout stays as the backstop for the case where the workers cannot
    be reached at all.

    **The grace period is still needed**, and is short. There is a window between `_claim`
    committing PROCESSING and the task appearing in a worker's active list; reclaiming
    inside it would put two workers on the same field. Idempotency (§16.2) keeps that
    correct, but it doubles the cost and makes the throughput metric lie.

    Returns what it did, so a scheduled job that quietly reconciles thousands of units a day
    is visible rather than merely effective.
    """
    settings = get_settings()
    grace = older_than_seconds if older_than_seconds is not None else settings.reconcile_grace
    cutoff = datetime.now(UTC) - timedelta(seconds=grace)
    running = _units_in_flight()

    with session_scope() as session:
        candidates = session.execute(
            select(UnitRun.work_unit_id, UnitRun.run_id)
            .where(UnitRun.status == UnitStatus.PROCESSING, UnitRun.started_at < cutoff)
            .with_for_update(skip_locked=True)
        ).all()
        # `None` means the workers could not be reached; falling back to the broker's
        # timeout is the conservative answer, because reclaiming everything on the strength
        # of a failed inspect would re-run a whole live fleet's work.
        stuck = (
            [row for row in candidates if (str(row[0]), str(row[1])) not in running]
            if running is not None
            else [
                row
                for row in candidates
                if row is not None and _older_than(session, row, settings.visibility_timeout)
            ]
        )

        for work_unit_id, run_id in stuck:
            # Back to PENDING before republishing, because `_claim` only accepts PENDING or
            # PROCESSING — and leaving it PROCESSING would let two sweeps queue it twice.
            session.execute(
                update(UnitRun)
                .where(UnitRun.work_unit_id == work_unit_id, UnitRun.run_id == run_id)
                .values(status=UnitStatus.PENDING, last_error="reclaimed by reconciliation")
            )
        session.commit()

    for work_unit_id, run_id in stuck:
        process_unit.delay(str(work_unit_id), str(run_id))
        RECONCILED.inc()

    if stuck:
        logger.warning("reconciliation re-enqueued %d stuck unit(s)", len(stuck))
    return {
        "reclaimed": len(stuck),
        "examined": len(candidates),
        "workers_answered": running is not None,
    }


def _units_in_flight() -> set[tuple[str, str]] | None:
    """What the workers say they are executing right now, or `None` if none answered.

    `inspect active` is a broadcast with a timeout: every live worker replies with the tasks
    it holds. That is a far better definition of "in progress" than elapsed time, because a
    worker that died is not in a position to reply on behalf of the unit it was holding.

    `None` and "nobody is running anything" are deliberately different answers. An empty
    fleet legitimately runs nothing; an unreachable fleet knows nothing, and treating the
    second as the first would reclaim every unit in flight.
    """
    try:
        active = celery_app.control.inspect(timeout=INSPECT_TIMEOUT_SECONDS).active()
    except Exception as exc:  # a broker that is down is not a reason to crash the sweep
        logger.warning("could not inspect workers: %s", exc)
        return None
    if not active:
        return None

    running: set[tuple[str, str]] = set()
    for tasks in active.values():
        for task in tasks or []:
            args = task.get("args") or []
            if len(args) >= 2:
                running.add((str(args[0]), str(args[1])))
    return running


def _older_than(session: Session, row: Any, seconds: int) -> bool:
    """Fallback path: the broker's timeout, used only when the fleet did not answer."""
    started = session.scalar(
        select(UnitRun.started_at).where(UnitRun.work_unit_id == row[0], UnitRun.run_id == row[1])
    )
    return started is not None and started < datetime.now(UTC) - timedelta(seconds=seconds)
