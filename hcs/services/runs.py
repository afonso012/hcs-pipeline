"""Turning a batch into a run (spec §7.2, §12).

A run is not "the batch, started". It is one attempt at the batch under a recorded
environment, which is why every unit gets a fresh `unit_runs` row rather than having its
status overwritten: reprocessing with a different pipeline version leaves both sides
intact for `GET /runs/{a}/compare/{b}` to subtract.
"""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hcs.config import Settings
from hcs.db import Batch, BatchStatus, BatchUnit, Run, UnitRun, UnitStatus
from hcs.lineage import config_hash, lockfile_hash


class BatchNotFoundError(LookupError):
    pass


class EmptyBatchError(ValueError):
    pass


def units_in_batch(session: Session, batch_id: uuid.UUID) -> Sequence[uuid.UUID]:
    """The units this batch contains, in a stable order.

    Membership comes from `batch_units`, not from a column on the unit: a field belongs
    to as many batches as have registered it, and the scientific set including a plate
    the development batch already used is the normal case, not a duplicate.
    """
    return session.scalars(
        select(BatchUnit.work_unit_id)
        .where(BatchUnit.batch_id == batch_id)
        .order_by(BatchUnit.work_unit_id)
    ).all()


def submit_batch(session: Session, batch_id: uuid.UUID, settings: Settings) -> Run:
    """Create a run over every unit in the batch. The caller commits and enqueues."""
    batch = session.get(Batch, batch_id)
    if batch is None:
        raise BatchNotFoundError(str(batch_id))

    unit_ids = units_in_batch(session, batch_id)
    if not unit_ids:
        raise EmptyBatchError(str(batch_id))

    run = _create_run(session, batch_id, settings, total_units=len(unit_ids))
    _create_unit_runs(session, run.id, unit_ids)
    batch.status = BatchStatus.READY
    return run


def _create_run(session: Session, batch_id: uuid.UUID, settings: Settings, total_units: int) -> Run:
    # Only the knobs that change what a number comes out as. Putting the log level in here
    # would make two runs look different when nothing measurable differed between them.
    config = {
        "chunk_size": settings.chunk_size,
        "chunk_overlap": settings.chunk_overlap,
        "pipeline_version": settings.pipeline_version,
        "random_seed": settings.random_seed,
        # Here rather than only in the environment because the Otsu threshold is computed
        # per block (§8.1): a cell count is a function of the image *and* the chunking, so
        # a run is comparable only with one that agrees on both.
        "segmentation_channel": settings.segmentation_channel,
    }
    run = Run(
        batch_id=batch_id,
        pipeline_version=settings.pipeline_version,
        image_digest=settings.image_digest,
        config=config,
        config_hash=config_hash(config),
        lockfile_hash=lockfile_hash(),
        random_seed=settings.random_seed,
        total_units=total_units,
    )
    session.add(run)
    session.flush()  # assigns run.id before the unit_runs insert references it
    return run


def _create_unit_runs(session: Session, run_id: uuid.UUID, unit_ids: Sequence[uuid.UUID]) -> None:
    session.execute(
        insert(UnitRun)
        .values(
            [
                {"work_unit_id": uid, "run_id": run_id, "status": UnitStatus.PENDING}
                for uid in unit_ids
            ]
        )
        .on_conflict_do_nothing(index_elements=["work_unit_id", "run_id"])
    )
