"""Batch lifecycle endpoints (spec §7.1, §7.2)."""

import logging
import uuid

from fastapi import APIRouter, HTTPException, Response, status

from hcs.api.deps import SessionDep, SettingsDep
from hcs.api.schemas import (
    BatchCreate,
    BatchOut,
    HcsManifestIn,
    ManifestIn,
    ManifestOut,
    SubmitOut,
)
from hcs.db import Batch
from hcs.services.manifest import (
    PlateMismatchError,
    register_hcs_manifest,
    register_wsi_manifest,
)
from hcs.services.queue import QueueSaturatedError, check_admission
from hcs.services.runs import BatchNotFoundError, EmptyBatchError, submit_batch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/batches", tags=["batches"])


def _as_out(batch: Batch) -> BatchOut:
    return BatchOut(
        batch_id=batch.id,
        status=batch.status,
        total_units=batch.total_units,
        external_ref=batch.external_ref,
    )


@router.post("", status_code=status.HTTP_201_CREATED)
def create_batch(payload: BatchCreate, session: SessionDep) -> BatchOut:
    batch = Batch(external_ref=payload.external_ref)
    session.add(batch)
    session.commit()
    return _as_out(batch)


@router.get("/{batch_id}")
def get_batch(batch_id: uuid.UUID, session: SessionDep) -> BatchOut:
    batch = session.get(Batch, batch_id)
    if batch is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"no such batch: {batch_id}")
    return _as_out(batch)


@router.post("/{batch_id}/manifest", status_code=status.HTTP_202_ACCEPTED)
def register_manifest(
    batch_id: uuid.UUID, manifest: ManifestIn, session: SessionDep
) -> ManifestOut:
    """Register units by reference: metadata crosses the wire, bytes do not (ADR-002).

    Safe to repeat. Sending the same manifest twice reports every unit under
    `duplicates_skipped` and changes nothing in the database.
    """
    if session.get(Batch, batch_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"no such batch: {batch_id}")

    try:
        outcome = (
            register_hcs_manifest(session, batch_id, manifest)
            if isinstance(manifest, HcsManifestIn)
            else register_wsi_manifest(session, batch_id, manifest)
        )
    except PlateMismatchError as exc:
        # 409, not 422: the body is well formed, it just contradicts what the plate
        # already says. Accepting it would redefine "complete" for fields already
        # registered under the previous definition.
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    session.commit()
    return ManifestOut(
        units_registered=outcome.units_registered,
        duplicates_skipped=outcome.duplicates_skipped,
        incomplete=outcome.incomplete,
    )


@router.post("/{batch_id}/submit", status_code=status.HTTP_202_ACCEPTED)
def submit(
    batch_id: uuid.UUID, session: SessionDep, settings: SettingsDep, response: Response
) -> SubmitOut:
    """Create a run over the batch and hand it to the dispatcher (§7.2, §10).

    Admission is checked before the run row exists. Creating a run and then refusing to
    dispatch it would leave a QUEUED run nobody is working on, which the reconciliation
    sweep of §16.4 would later have to explain.
    """
    try:
        check_admission(settings)
    except QueueSaturatedError as exc:
        # 429 with a Retry-After the client can actually obey, computed from the depth
        # rather than picked. §10 wants the queue defended, not the client punished.
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail=exc.as_problem(),
            headers={"Retry-After": str(exc.drain_seconds)},
        ) from exc

    try:
        run = submit_batch(session, batch_id, settings)
    except BatchNotFoundError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"no such batch: {batch_id}") from exc
    except EmptyBatchError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "batch has no work units; register a manifest first"
        ) from exc

    run_id, total_units = run.id, run.total_units
    session.commit()

    # Publish only after the commit. A worker is faster than a transaction: publish first
    # and a task can arrive to find no `unit_runs` row to claim, then fail as a missing
    # unit that in fact exists.
    _dispatch(run_id, batch_id)
    return SubmitOut(run_id=run_id, total_units=total_units)


def _dispatch(run_id: uuid.UUID, batch_id: uuid.UUID) -> None:
    """Hand the run to the block dispatcher (§10).

    One task published here rather than one per unit. A ten-thousand-unit batch used to
    mean ten thousand `.delay()` calls inside the request, which is both a slow response
    and the flood the watermarks exist to prevent.
    """
    # Imported here, not at module scope: importing the task pulls in the Celery app and
    # opens a broker connection, which would make the API unable to start whenever Redis
    # is down — exactly when /readyz needs to be able to say so.
    #
    # It stays a `.delay` rather than a `send_task` by name because `send_task` ignores
    # `task_always_eager`, and eager mode is what lets the tests drive submit through to a
    # marked unit in one process. The import is cheap because `perform` defers the imaging
    # stack — see the note there.
    from hcs.worker.tasks import dispatch_run

    dispatch_run.delay(str(run_id), str(batch_id))
    logger.info("dispatching run %s over batch %s", run_id, batch_id)
