"""Registering a manifest (spec §7.1).

The endpoint is re-runnable **by construction**, not by deduplication logic. Every insert
here is `ON CONFLICT DO NOTHING` over a key the caller's own data determines, so running
the same manifest twice is running it once, and no ordering of the statements can leave a
`work_units` row without its subtype.

That property depends entirely on `hcs.identity`: with a generated id, the second pass
would insert a fresh parent, collide on `UNIQUE (plate_id, well, field_index)` in the
subtype, and leave an orphan that is invisible to every query and still counted in
`total_units`.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hcs.api.schemas import HcsFieldIn, ManifestIn, PlateIn
from hcs.db import Batch, HcsChannelFile, HcsField, Plate, UnitKind, WorkUnit
from hcs.identity import hcs_field_id


@dataclass(frozen=True)
class ManifestOutcome:
    units_registered: int
    duplicates_skipped: int
    incomplete: int


def register_hcs_manifest(
    session: Session, batch_id: uuid.UUID, manifest: ManifestIn
) -> ManifestOutcome:
    """Register the fields of one plate into a batch. The caller commits."""
    plate_id = _upsert_plate(session, manifest.plate)

    required = set(manifest.required_channels)
    # A field missing a channel is rejected whole (§5): the DNA channel finds the nuclei
    # and the others are measured *inside* that mask, so two channels out of three do not
    # make two thirds of a measurement. It is reported, not registered — registering it
    # would put a unit into `total_units` that can never be processed.
    complete = [unit for unit in manifest.units if required <= unit.channels.keys()]
    incomplete = len(manifest.units) - len(complete)
    if not complete:
        return ManifestOutcome(0, 0, incomplete)

    unit_ids = [
        hcs_field_id(manifest.plate.barcode, unit.well, unit.field_index) for unit in complete
    ]

    registered = _insert_work_units(session, batch_id, unit_ids)
    _insert_fields(session, plate_id, unit_ids, complete)
    _insert_channel_files(session, unit_ids, complete)
    _refresh_total_units(session, batch_id)

    return ManifestOutcome(
        units_registered=registered,
        duplicates_skipped=len(complete) - registered,
        incomplete=incomplete,
    )


def _upsert_plate(session: Session, plate: PlateIn) -> uuid.UUID:
    """Get the plate's id, creating the row if this is the first batch to mention it.

    `DO UPDATE` rather than `DO NOTHING` only so RETURNING yields a row on the conflicting
    path too; the update itself is a no-op on the barcode.
    """
    statement = (
        insert(Plate)
        .values(id=uuid.uuid4(), barcode=plate.barcode, n_rows=plate.n_rows, n_cols=plate.n_cols)
        .on_conflict_do_update(index_elements=["barcode"], set_={"barcode": plate.barcode})
        .returning(Plate.id)
    )
    return session.scalars(statement).one()


def _insert_work_units(session: Session, batch_id: uuid.UUID, unit_ids: Sequence[uuid.UUID]) -> int:
    """Insert the parents, returning how many were actually new.

    The count comes from RETURNING rather than from `rowcount` or a prior SELECT: it is
    the only number that reflects what the database really did, which is what
    `duplicates_skipped` claims to report.
    """
    statement = (
        insert(WorkUnit)
        .values([{"id": uid, "batch_id": batch_id, "kind": UnitKind.HCS_FIELD} for uid in unit_ids])
        .on_conflict_do_nothing(index_elements=["id"])
        .returning(WorkUnit.id)
    )
    return len(session.scalars(statement).all())


def _insert_fields(
    session: Session,
    plate_id: uuid.UUID,
    unit_ids: Sequence[uuid.UUID],
    units: Sequence[HcsFieldIn],
) -> None:
    rows = [
        {
            "work_unit_id": uid,
            "plate_id": plate_id,
            "well": unit.well,
            "field_index": unit.field_index,
        }
        for uid, unit in zip(unit_ids, units, strict=True)
    ]
    # Bare DO NOTHING, not `index_elements=["work_unit_id"]`: this table carries a second
    # unique constraint on (plate_id, well, field_index), and naming only the primary key
    # would turn a conflict on the other one into a 500 instead of a no-op.
    session.execute(insert(HcsField).values(rows).on_conflict_do_nothing())


def _insert_channel_files(
    session: Session, unit_ids: Sequence[uuid.UUID], units: Sequence[HcsFieldIn]
) -> None:
    rows = [
        {
            "work_unit_id": uid,
            "channel": name,
            "storage_uri": channel.uri,
            "content_sha256": channel.sha256.lower(),
            "width": channel.width,
            "height": channel.height,
            "size_bytes": channel.size_bytes,
        }
        for uid, unit in zip(unit_ids, units, strict=True)
        for name, channel in unit.channels.items()
    ]
    session.execute(
        insert(HcsChannelFile)
        .values(rows)
        .on_conflict_do_nothing(index_elements=["work_unit_id", "channel"])
    )


def _refresh_total_units(session: Session, batch_id: uuid.UUID) -> None:
    """Recount rather than add.

    Incrementing by the number just inserted would drift the moment a manifest is
    partially re-sent. Counting is one cheap query against an indexed column and cannot.
    """
    total = session.scalar(
        select(func.count()).select_from(WorkUnit).where(WorkUnit.batch_id == batch_id)
    )
    session.execute(update(Batch).where(Batch.id == batch_id).values(total_units=total))
