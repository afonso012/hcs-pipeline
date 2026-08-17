"""Registering a manifest (spec §7.1).

The endpoint is re-runnable **by construction**, not by deduplication logic. Every insert
here is `ON CONFLICT DO NOTHING` over a key the caller's own data determines, so running
the same manifest twice is running it once, and no ordering of the statements can leave a
`work_units` row without its subtype.

That property depends entirely on `hcs.identity`: with a generated id, the second pass
would insert a fresh parent, collide on `UNIQUE (plate_id, well, field_index)` in the
subtype, and leave an orphan that is invisible to every query and still counted in
`total_units`.

Because a unit's identity is global, the same field can be registered into any number of
batches — the development batch and the scientific set both contain `Week1_22141`, and
neither is a duplicate of the other. `units_registered` therefore counts new rows in
`batch_units`: new *to this batch*, not new to the database.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import case, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hcs.api.schemas import HcsFieldIn, ManifestIn, PlateIn, WellLayoutIn
from hcs.db import (
    Batch,
    BatchUnit,
    HcsChannelFile,
    HcsField,
    Plate,
    UnitKind,
    WellLayout,
    WorkUnit,
)
from hcs.identity import hcs_field_id
from hcs.science.controls import control_type


class PlateMismatchError(ValueError):
    """The manifest describes a plate the database already knows differently."""


@dataclass(frozen=True)
class ManifestOutcome:
    units_registered: int
    duplicates_skipped: int
    incomplete: int


def register_hcs_manifest(
    session: Session, batch_id: uuid.UUID, manifest: ManifestIn
) -> ManifestOutcome:
    """Register the fields of one plate into a batch. The caller commits."""
    plate_id, expected = _upsert_plate(session, manifest.plate)
    # Before the early return below: the layout describes the plate, not the submission,
    # and a manifest whose every field turned out to be incomplete still told us what is
    # in the wells.
    _upsert_well_layout(session, plate_id, manifest.plate.wells)

    # A field missing a channel is rejected whole (§5): the DNA channel finds the nuclei
    # and the others are measured *inside* that mask, so two channels out of three do not
    # make two thirds of a measurement. It is reported, not registered — registering it
    # would put a unit into `total_units` that can never be processed.
    complete = [unit for unit in manifest.units if set(expected) <= unit.channels.keys()]
    incomplete = len(manifest.units) - len(complete)
    if not complete:
        return ManifestOutcome(0, 0, incomplete)

    unit_ids = [
        hcs_field_id(manifest.plate.barcode, unit.well, unit.field_index) for unit in complete
    ]

    _insert_work_units(session, unit_ids)
    _insert_fields(session, plate_id, unit_ids, complete)
    _insert_channel_files(session, unit_ids, complete)
    registered = _add_to_batch(session, batch_id, unit_ids)
    _refresh_total_units(session, batch_id)

    return ManifestOutcome(
        units_registered=registered,
        duplicates_skipped=len(complete) - registered,
        incomplete=incomplete,
    )


def _upsert_plate(session: Session, plate: PlateIn) -> tuple[uuid.UUID, list[str]]:
    """Get the plate's id and its expected channel set, creating the row if it is new.

    An empty `expected_channels` means "not yet declared" — the state a plate registered
    before that column existed is in — and this manifest fills it. A plate that already
    declares something different is a contradiction rather than an update: silently
    picking one of the two would let a later manifest redefine what "complete" means for
    fields that were already accepted under the old definition.
    """
    statement = insert(Plate).values(
        id=uuid.uuid4(),
        barcode=plate.barcode,
        n_rows=plate.n_rows,
        n_cols=plate.n_cols,
        expected_channels=sorted(plate.expected_channels),
    )
    upsert = statement.on_conflict_do_update(
        index_elements=["barcode"],
        set_={
            "expected_channels": case(
                (
                    func.cardinality(Plate.expected_channels) == 0,
                    statement.excluded.expected_channels,
                ),
                else_=Plate.expected_channels,
            )
        },
    ).returning(Plate.id, Plate.n_rows, Plate.n_cols, Plate.expected_channels)

    plate_id, n_rows, n_cols, expected = session.execute(upsert).one()

    declared = (plate.n_rows, plate.n_cols, sorted(plate.expected_channels))
    if (n_rows, n_cols, sorted(expected)) != declared:
        raise PlateMismatchError(
            f"plate {plate.barcode} is already registered as {n_rows}x{n_cols} with "
            f"channels {sorted(expected)}; this manifest declares "
            f"{plate.n_rows}x{plate.n_cols} with {declared[2]}"
        )
    return plate_id, list(expected)


def _upsert_well_layout(
    session: Session, plate_id: uuid.UUID, wells: Sequence[WellLayoutIn]
) -> None:
    """Record what each named well contains, deciding `control_type` here.

    The derivation is server-side on purpose (§9.1). The request carries the pair and
    nothing else, so the rule that taxol is the positive control at 0.3 uM and an ordinary
    dose point at the other seven concentrations is applied in one place, by one function,
    for every client that will ever exist.

    Unnamed wells are untouched. A named well is overwritten, including with NULLs: the
    manifest states the layout of the wells it lists, and a correction that could not
    clear a wrong compound would not be a correction.
    """
    if not wells:
        return

    rows = [
        {
            "plate_id": plate_id,
            "well": well.well,
            "compound": well.compound,
            "concentration": well.concentration,
            "moa": well.moa,
            "control_type": control_type(well.compound, well.concentration),
        }
        for well in wells
    ]
    statement = insert(WellLayout).values(rows)
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["plate_id", "well"],
            set_={
                column: statement.excluded[column]
                for column in ("compound", "concentration", "moa", "control_type")
            },
        )
    )


def _insert_work_units(session: Session, unit_ids: Sequence[uuid.UUID]) -> None:
    """Register the units themselves. Existing ones are left alone and not counted.

    A unit already present may belong to another batch entirely, which is not a duplicate
    of anything — `_add_to_batch` is what decides whether *this* batch already had it.
    """
    session.execute(
        insert(WorkUnit)
        .values([{"id": uid, "kind": UnitKind.HCS_FIELD} for uid in unit_ids])
        .on_conflict_do_nothing(index_elements=["id"])
    )


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


def _add_to_batch(session: Session, batch_id: uuid.UUID, unit_ids: Sequence[uuid.UUID]) -> int:
    """Put the units in the batch, returning how many were not already in it.

    The count comes from RETURNING rather than from `rowcount` or a prior SELECT: it is
    the only number that reflects what the database really did, which is what
    `duplicates_skipped` claims to report.
    """
    statement = (
        insert(BatchUnit)
        .values([{"batch_id": batch_id, "work_unit_id": uid} for uid in unit_ids])
        .on_conflict_do_nothing(index_elements=["batch_id", "work_unit_id"])
        .returning(BatchUnit.work_unit_id)
    )
    return len(session.scalars(statement).all())


def _refresh_total_units(session: Session, batch_id: uuid.UUID) -> None:
    """Recount rather than add.

    Incrementing by the number just inserted would drift the moment a manifest is
    partially re-sent. Counting is one cheap query against the primary key and cannot.
    """
    total = session.scalar(
        select(func.count()).select_from(BatchUnit).where(BatchUnit.batch_id == batch_id)
    )
    session.execute(update(Batch).where(Batch.id == batch_id).values(total_units=total))
