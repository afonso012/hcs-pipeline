"""Aggregated results and API credentials (spec §6.4).

Cell-level metrics do not live here — 13.2 M rows per run go to Parquet on MinIO (§6.3,
ADR-007). Postgres holds what is already aggregated and small: one row per well, per
plate, per fitted curve.

Every table in this module is created in the Phase 1 migration even though Phases 6 and 7
are the first to write to them. A schema that grows a table per phase produces a
migration per phase, and each one is a chance to get a foreign key wrong on a database
that already holds results.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    REAL,
    Boolean,
    DateTime,
    ForeignKey,
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


class WellProfile(Base):
    """The aggregated morphological profile of one well in one run (§9.1).

    Median rather than mean: cell distributions are skewed and contaminated by
    segmentation artefacts. Normalisation is always within the plate and against that
    plate's own negative controls — normalising against another plate lets batch effects
    destroy the comparison.
    """

    __tablename__ = "well_profiles"

    plate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("plates.id", ondelete="CASCADE"), primary_key=True
    )
    well: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True
    )
    cell_count: Mapped[int | None] = mapped_column(Integer)
    profile: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)  # ~300 floats
    viability_readout: Mapped[float | None] = mapped_column(REAL)  # primary   (§9.2)
    morphology_distance: Mapped[float | None] = mapped_column(REAL)  # secondary (§9.2)


class PlateQc(Base):
    """Assay quality of one plate in one run (§9.3)."""

    __tablename__ = "plate_qc"

    plate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("plates.id", ondelete="CASCADE"), primary_key=True
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True
    )
    z_prime: Mapped[float | None] = mapped_column(REAL)
    cv_negative: Mapped[float | None] = mapped_column(REAL)
    n_negative: Mapped[int | None] = mapped_column(Integer)
    n_positive: Mapped[int | None] = mapped_column(Integer)
    passed: Mapped[bool | None] = mapped_column(Boolean)


class DoseResponse(Base):
    """One fitted dose-response curve, per plate (§9.4).

    The IC50 is keyed by plate rather than by compound alone because 110 of 113 compounds
    carry their full eight-point series on a single plate (§21.1). Each plate therefore
    yields an independent fit, and the replicate plates give error bars — which is the
    whole reason the target plate set is 43 rather than 24.
    """

    __tablename__ = "dose_response"

    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True
    )
    plate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("plates.id", ondelete="CASCADE"), primary_key=True
    )
    compound: Mapped[str] = mapped_column(Text, primary_key=True)
    readout: Mapped[str] = mapped_column(Text, primary_key=True)  # 'viability' | 'morphology'
    ic50: Mapped[float | None] = mapped_column(REAL)
    hill_slope: Mapped[float | None] = mapped_column(REAL)
    top: Mapped[float | None] = mapped_column(REAL)
    bottom: Mapped[float | None] = mapped_column(REAL)
    r_squared: Mapped[float | None] = mapped_column(REAL)
    n_points: Mapped[int | None] = mapped_column(SmallInteger)
    in_range: Mapped[bool | None] = mapped_column(Boolean)  # IC50 inside the tested range
    is_hit: Mapped[bool | None] = mapped_column(Boolean)


class BatchEffect(Base):
    """Variance decomposition of the taxol 0.3 probe — the Phase 6 result (§9.5).

    The probe is present on all 55 plates across 10 weeks, which is what makes batch
    effect measurable at all. A ratio that falls as correction is applied is proof that
    normalisation does what it claims; a ratio that does not fall is equally a result.
    """

    __tablename__ = "batch_effect"

    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True
    )
    probe: Mapped[str] = mapped_column(Text, primary_key=True)  # 'taxol@0.3'
    correction: Mapped[str] = mapped_column(Text, primary_key=True)  # 'none'|'mad_robustize'|...
    grouping: Mapped[str] = mapped_column(Text, primary_key=True)  # 'week' | 'plate'
    variance_between: Mapped[float | None] = mapped_column(REAL)
    variance_within: Mapped[float | None] = mapped_column(REAL)
    ratio: Mapped[float | None] = mapped_column(REAL)  # between/within; lower is better
    n_groups: Mapped[int | None] = mapped_column(Integer)
    n_wells: Mapped[int | None] = mapped_column(Integer)


class MoaPrediction(Base):
    """Mechanism-of-action prediction. Phase 7, optional."""

    __tablename__ = "moa_predictions"

    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True
    )
    compound: Mapped[str] = mapped_column(Text, primary_key=True)
    concentration: Mapped[float] = mapped_column(REAL, primary_key=True)
    moa_true: Mapped[str | None] = mapped_column(Text)
    moa_predicted: Mapped[str | None] = mapped_column(Text)
    neighbour: Mapped[str | None] = mapped_column(Text)  # the chosen nearest treatment
    similarity: Mapped[float | None] = mapped_column(REAL)


class ApiKey(Base):
    """An API credential. Only the digest is stored; the key itself never is."""

    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)  # sha256
    label: Mapped[str] = mapped_column(Text, nullable=False)
    rate_limit: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("100")
    )  # requests per minute
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
