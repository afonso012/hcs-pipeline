"""Operational core of the schema (spec §6.1).

Four tables carry the whole execution model: a batch is *what* to process, a run is *one
attempt* at processing it with a recorded environment, a work unit is the indivisible
piece of work, and `unit_runs` holds the state of one unit within one run.

The separation between `work_units` and `unit_runs` is ADR-006. State lives per
`(unit, run)` rather than on the unit, so reprocessing a batch never overwrites what the
previous run observed, and `GET /runs/{a}/compare/{b}` has two intact sides to compare.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hcs.db.base import Base
from hcs.db.enums import BatchStatus, RunStatus, UnitKind, UnitStatus, pg_enum


class Batch(Base):
    """A set of work units submitted together."""

    __tablename__ = "batches"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    external_ref: Mapped[str | None] = mapped_column(Text)
    status: Mapped[BatchStatus] = mapped_column(
        pg_enum(BatchStatus, "batch_status"),
        nullable=False,
        server_default=BatchStatus.DRAFT.value,
    )
    total_units: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Run(Base):
    """One execution of the pipeline over a batch, with reproducible context (§12).

    The lineage columns are the point of the table. `image_digest` is the container
    digest and never the tag, because a tag is a moving pointer and a digest is not:
    a run that cannot name the exact image that produced it is not reproducible.
    """

    __tablename__ = "runs"
    __table_args__ = (
        # Newest run of a batch first — the query every progress view starts from.
        Index("runs_batch_ix", "batch_id", text("started_at DESC")),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("batches.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[RunStatus] = mapped_column(
        pg_enum(RunStatus, "run_status"), nullable=False, server_default=RunStatus.QUEUED.value
    )
    pipeline_version: Mapped[str] = mapped_column(Text, nullable=False)  # 'classic-0.3.1'
    image_digest: Mapped[str] = mapped_column(Text, nullable=False)  # sha256, never a tag
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    lockfile_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # sha256 of uv.lock
    random_seed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mlflow_run_id: Mapped[str | None] = mapped_column(Text)
    total_units: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    processed_units: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    failed_units: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkUnit(Base):
    """The indivisible piece of work: one HCS field or one WSI slide (§5).

    Deliberately thin — no domain columns, and no batch either. Everything that differs
    between the two rails lives in a subtype table with its own constraints, so neither
    rail has to carry nullable columns belonging to the other.

    `id` is not generated here. It is the deterministic UUIDv5 of `hcs.identity`, which is
    what makes the manifest endpoint idempotent by construction (§7.1).
    """

    __tablename__ = "work_units"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    kind: Mapped[UnitKind] = mapped_column(pg_enum(UnitKind, "unit_kind"), nullable=False)


class BatchUnit(Base):
    """Which units a batch contains.

    Not an extra table — the batch's actual content. It used to be smeared across a
    `batch_id` column on `work_units`, which made a field belong to exactly one batch
    forever, for the same reason a `batch_id` on `plates` would have made a plate belong
    to one batch: membership was stored on the thing instead of expressed as a relation.

    The damage was concrete. The development batch registers `Week1_22141`; the
    scientific set of 43 plates includes it again, and would have silently registered 480
    fewer fields — so a run over 41 plates would report itself as a run over 43, and the
    dose curves for those two plates would vanish without a number moving.
    """

    __tablename__ = "batch_units"
    __table_args__ = (
        # The forward direction is covered by the primary key. This is the reverse: which
        # batches contain a given unit, which is how a shared unit is recognised as shared
        # rather than deleted along with the first batch that drops it.
        Index("batch_units_unit_ix", "work_unit_id"),
    )

    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("batches.id", ondelete="CASCADE"), primary_key=True
    )
    work_unit_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("work_units.id", ondelete="CASCADE"), primary_key=True
    )


class UnitRun(Base):
    """State of one work unit within one run."""

    __tablename__ = "unit_runs"
    __table_args__ = (
        Index("unit_runs_progress_ix", "run_id", "status"),
        # Partial index behind the reconciliation sweep of §16.4: find units a killed
        # worker left mid-flight. Indexing only PROCESSING keeps it tiny, because that is
        # the only status the sweep ever asks about.
        Index(
            "unit_runs_stuck_ix",
            "status",
            "started_at",
            postgresql_where=text("status = 'PROCESSING'"),
        ),
    )

    work_unit_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("work_units.id", ondelete="CASCADE"), primary_key=True
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True
    )
    status: Mapped[UnitStatus] = mapped_column(
        pg_enum(UnitStatus, "unit_status"),
        nullable=False,
        server_default=UnitStatus.PENDING.value,
    )
    attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    # 'TRANSIENT' | 'PERMANENT' — see hcs.db.enums.ErrorClass and §16.3.
    error_class: Mapped[str | None] = mapped_column(Text)
    qc: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
