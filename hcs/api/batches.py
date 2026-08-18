"""Batch lifecycle endpoints (spec §7.1, §7.2)."""

import logging
import uuid
from collections.abc import Sequence

from fastapi import APIRouter, HTTPException, status

from hcs.api.deps import SessionDep, SettingsDep
from hcs.api.schemas import BatchCreate, BatchOut, ManifestIn, ManifestOut, SubmitOut
from hcs.db import Batch
from hcs.services.manifest import PlateMismatchError, register_hcs_manifest
from hcs.services.runs import BatchNotFoundError, EmptyBatchError, submit_batch, units_in_batch

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
        outcome = register_hcs_manifest(session, batch_id, manifest)
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
def submit(batch_id: uuid.UUID, session: SessionDep, settings: SettingsDep) -> SubmitOut:
    """Create a run over the batch and enqueue its units."""
    try:
        run = submit_batch(session, batch_id, settings)
    except BatchNotFoundError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"no such batch: {batch_id}") from exc
    except EmptyBatchError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "batch has no work units; register a manifest first"
        ) from exc

    run_id, total_units = run.id, run.total_units
    unit_ids = units_in_batch(session, batch_id)
    session.commit()

    # Enqueue only after the commit. A worker is faster than a transaction: publish first
    # and a task can arrive to find no `unit_runs` row to claim, then fail as a missing
    # unit that in fact exists.
    _enqueue(unit_ids, run_id)
    return SubmitOut(run_id=run_id, total_units=total_units)


def _enqueue(unit_ids: Sequence[uuid.UUID], run_id: uuid.UUID) -> None:
    # Imported here, not at module scope: importing the task pulls in the Celery app and
    # opens a broker connection, which would make the API unable to start whenever Redis
    # is down — exactly when /readyz needs to be able to say so.
    #
    # It stays a `.delay` rather than a `send_task` by name because `send_task` ignores
    # `task_always_eager`, and eager mode is what lets the tests drive submit through to a
    # marked unit in one process. The import is cheap because `perform` defers the imaging
    # stack — see the note there.
    from hcs.worker.tasks import process_unit

    for unit_id in unit_ids:
        process_unit.delay(str(unit_id), str(run_id))
    logger.info("enqueued %d units for run %s", len(unit_ids), run_id)
