"""What the schema of spec §6 must guarantee, checked against a real PostgreSQL.

These are not tests of SQLAlchemy. Each one pins a claim the design makes and that a
plausible schema change would quietly break: that the manifest is idempotent by
construction, that two files may share a digest, that reprocessing does not overwrite the
previous run, and that a plate outlives the batches referencing it.
"""

import uuid

import pytest
from alembic import command
from alembic.config import Config
from conftest import alembic_config
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from hcs.db import (
    Batch,
    HcsChannelFile,
    HcsField,
    Plate,
    Run,
    RunStatus,
    UnitKind,
    UnitRun,
    UnitStatus,
    WorkUnit,
)
from hcs.identity import hcs_field_id

pytestmark = pytest.mark.integration

ENUM_TYPE_NAMES = ("batch_status", "run_status", "unit_status", "unit_kind")


def make_run(session: Session, batch: Batch) -> Run:
    """A run with the lineage columns §12 requires, none of which is nullable."""
    run = Run(
        batch_id=batch.id,
        pipeline_version="classic-0.0.1",
        image_digest="sha256:" + "0" * 64,
        config_hash="a" * 64,
        config={"chunk": 1024, "overlap": 32},
        lockfile_hash="b" * 64,
        random_seed=1234,
    )
    session.add(run)
    session.flush()
    return run


def register_field(
    session: Session, batch: Batch, plate: Plate, well: str, field_index: int
) -> uuid.UUID:
    """Register one field the way the manifest endpoint of §7.1 will.

    Parent first, subtype second — the foreign key allows no other order, and it is
    precisely the order that used to orphan a `work_units` row: the subtype insert
    collided on `UNIQUE (plate_id, well, field_index)` while the parent had already gone
    in. The deterministic id is what makes both statements no-ops on the second pass.
    """
    unit_id = hcs_field_id(plate.barcode, well, field_index)
    session.execute(
        insert(WorkUnit)
        .values(id=unit_id, batch_id=batch.id, kind=UnitKind.HCS_FIELD)
        .on_conflict_do_nothing(index_elements=["id"])
    )
    session.execute(
        insert(HcsField)
        .values(work_unit_id=unit_id, plate_id=plate.id, well=well, field_index=field_index)
        .on_conflict_do_nothing(index_elements=["work_unit_id"])
    )
    return unit_id


@pytest.fixture
def batch(session: Session) -> Batch:
    created = Batch(external_ref="test")
    session.add(created)
    session.flush()
    return created


@pytest.fixture
def plate(session: Session) -> Plate:
    created = Plate(barcode="Week1_22141", n_rows=8, n_cols=12)
    session.add(created)
    session.flush()
    return created


class TestMigrationMatchesModels:
    def test_no_drift_between_the_models_and_the_migration(self, alembic_cfg: Config) -> None:
        """`alembic check` is the only thing standing between a hand-edited migration and
        a schema that no longer matches what the code expects."""
        command.check(alembic_cfg)

    def test_round_trip_leaves_no_orphan_enum_types(self, fresh_postgres: str) -> None:
        """A native enum survives the DROP TABLE that created it.

        Without an explicit drop, downgrading and upgrading again dies on
        "type batch_status already exists" — and the first person to hit it is whoever is
        trying to roll a bad migration back.
        """
        cfg = alembic_config(fresh_postgres)
        engine = create_engine(fresh_postgres)

        command.upgrade(cfg, "head")
        command.downgrade(cfg, "base")
        with engine.connect() as connection:
            leftover = connection.execute(
                text("SELECT count(*) FROM pg_type WHERE typname = ANY(:names)"),
                {"names": list(ENUM_TYPE_NAMES)},
            ).scalar_one()
        assert leftover == 0

        command.upgrade(cfg, "head")  # must not raise
        engine.dispose()


class TestManifestIdempotency:
    """Spec §7.1: re-running a manifest is a no-op *by construction*, not by lookup."""

    def test_registering_the_same_field_twice_changes_nothing(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        first = register_field(session, batch, plate, "B04", 1)
        second = register_field(session, batch, plate, "B04", 1)
        session.flush()

        assert first == second
        assert session.scalar(select(func.count()).select_from(WorkUnit)) == 1
        assert session.scalar(select(func.count()).select_from(HcsField)) == 1

    def test_no_work_unit_is_left_without_its_subtype(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        """The failure the deterministic id was introduced to make impossible.

        With a generated id, the second registration conflicts on
        `UNIQUE (plate_id, well, field_index)` in `hcs_fields` while the `work_units` row
        inserts cleanly — leaving a parent with no subtype, invisible to every query and
        still counted in `total_units`.
        """
        for _ in range(3):
            register_field(session, batch, plate, "B04", 1)
        session.flush()

        orphans = session.scalar(
            select(func.count())
            .select_from(WorkUnit)
            .outerjoin(HcsField, HcsField.work_unit_id == WorkUnit.id)
            .where(HcsField.work_unit_id.is_(None))
        )
        assert orphans == 0

    def test_the_same_field_on_another_plate_is_another_unit(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        other = Plate(barcode="Week1_22161", n_rows=8, n_cols=12)
        session.add(other)
        session.flush()

        assert register_field(session, batch, plate, "B04", 1) != register_field(
            session, batch, other, "B04", 1
        )


class TestChannelFiles:
    def test_two_files_may_share_a_content_digest(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        """`content_sha256` is indexed for integrity, deliberately not unique.

        The v3 schema had `UNIQUE (content_sha256)`, which deduplicated nothing — it
        rejected the second row that legitimately referenced the same bytes.
        """
        unit_a = register_field(session, batch, plate, "B04", 1)
        unit_b = register_field(session, batch, plate, "B04", 2)
        digest = "c" * 64
        session.add_all(
            [
                HcsChannelFile(
                    work_unit_id=unit_a, channel="DAPI", storage_uri="s3://a", content_sha256=digest
                ),
                HcsChannelFile(
                    work_unit_id=unit_b, channel="DAPI", storage_uri="s3://b", content_sha256=digest
                ),
            ]
        )
        session.flush()

        assert session.scalar(select(func.count()).select_from(HcsChannelFile)) == 2

    def test_a_field_cannot_hold_the_same_channel_twice(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        unit = register_field(session, batch, plate, "B04", 1)
        session.add(
            HcsChannelFile(
                work_unit_id=unit, channel="DAPI", storage_uri="s3://a", content_sha256="d" * 64
            )
        )
        session.flush()
        session.add(
            HcsChannelFile(
                work_unit_id=unit, channel="DAPI", storage_uri="s3://b", content_sha256="e" * 64
            )
        )
        with pytest.raises(DBAPIError):
            session.flush()


class TestStatePerRun:
    def test_reprocessing_does_not_overwrite_the_previous_run(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        """ADR-006: state lives per `(unit, run)`, so two runs of the same unit coexist
        and `GET /runs/{a}/compare/{b}` has two intact sides to compare."""
        unit = register_field(session, batch, plate, "B04", 1)
        first, second = make_run(session, batch), make_run(session, batch)
        session.add_all(
            [
                UnitRun(work_unit_id=unit, run_id=first.id, status=UnitStatus.DONE, attempts=1),
                UnitRun(work_unit_id=unit, run_id=second.id, status=UnitStatus.FAILED, attempts=3),
            ]
        )
        session.flush()

        states = dict(
            session.execute(
                select(UnitRun.run_id, UnitRun.status).where(UnitRun.work_unit_id == unit)
            ).all()
        )
        assert states == {first.id: UnitStatus.DONE, second.id: UnitStatus.FAILED}

    def test_an_unknown_status_is_rejected_by_the_database(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        """Native enums, not text with a convention: the database is the last line."""
        unit = register_field(session, batch, plate, "B04", 1)
        run = make_run(session, batch)
        with pytest.raises(DBAPIError):
            session.execute(
                text(
                    "INSERT INTO unit_runs (work_unit_id, run_id, status) "
                    "VALUES (:u, :r, 'ALMOST_DONE')"
                ),
                {"u": unit, "r": run.id},
            )


class TestLifetimes:
    def test_deleting_a_batch_leaves_the_plate_standing(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        """A plate is reference data. It outlives the batches that reference it, which is
        why `plates` has no `batch_id` — see the DDL comment in §6.2."""
        register_field(session, batch, plate, "B04", 1)
        session.flush()

        session.delete(batch)
        session.flush()

        assert session.scalar(select(func.count()).select_from(WorkUnit)) == 0
        assert session.scalar(select(func.count()).select_from(HcsField)) == 0
        assert session.get(Plate, plate.id) is not None

    def test_deleting_a_run_leaves_the_work_units_standing(
        self, session: Session, batch: Batch, plate: Plate
    ) -> None:
        unit = register_field(session, batch, plate, "B04", 1)
        run = make_run(session, batch)
        session.add(UnitRun(work_unit_id=unit, run_id=run.id, status=UnitStatus.DONE))
        session.flush()

        session.delete(run)
        session.flush()

        assert session.scalar(select(func.count()).select_from(UnitRun)) == 0
        assert session.get(WorkUnit, unit) is not None


class TestDefaults:
    def test_a_new_run_starts_queued_with_zeroed_counters(
        self, session: Session, batch: Batch
    ) -> None:
        run = make_run(session, batch)
        session.refresh(run)

        assert run.status is RunStatus.QUEUED
        assert (run.total_units, run.processed_units, run.failed_units) == (0, 0, 0)
        assert run.started_at is not None
