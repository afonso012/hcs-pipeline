"""Rail-specific subtype tables (spec §6.2, ADR-006).

One table per kind of work unit, each keeping the constraints only its own rail can
state. An HCS field must be unique within `(plate, well, field_index)`; a slide has no
plate coordinates at all. Folding both into `work_units` would mean nullable columns that
neither rail can constrain, which is how a schema stops defending its own invariants.
"""

import uuid

from sqlalchemy import (
    REAL,
    BigInteger,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hcs.db.base import Base
from hcs.db.core import WorkUnit


class Plate(Base):
    """A physical assay plate — reference data, not batch data.

    A plate outlives any number of batches: the development batch registers
    `Week1_22141` and the scientific set includes it again. Batch membership therefore
    flows through `work_units.batch_id -> hcs_fields.plate_id` and has no business being
    a column here. Putting it here would duplicate the plate per batch and fragment
    `well_layout`, `plate_qc` and `dose_response` across the copies.
    """

    __tablename__ = "plates"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    barcode: Mapped[str] = mapped_column(Text, nullable=False, unique=True)  # 'Week1_22141'
    n_rows: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_cols: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    # The channels the imaging protocol produces for every field of this plate, stored
    # sorted. It lives here rather than in the manifest body because it is a property of
    # the assay, not of a submission: two manifests for the same plate must not be able
    # to disagree about what a complete field looks like.
    #
    # It is also what makes `unit_status.INCOMPLETE` reachable at all. Before this column
    # that value could never be set — at manifest time there is no run to write a status
    # to, and at processing time nothing knew what to expect. Now a worker that fetches a
    # field and finds a channel gone from storage has something to compare against.
    #
    # An empty array means "not yet declared", not "expects nothing": the first manifest
    # after this column existed fills it in.
    expected_channels: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)


class HcsField(Base):
    """One imaging field: the N channel files sharing `(plate, well, field_index)`."""

    __tablename__ = "hcs_fields"
    __table_args__ = (UniqueConstraint("plate_id", "well", "field_index"),)

    work_unit_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("work_units.id", ondelete="CASCADE"), primary_key=True
    )
    plate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("plates.id", ondelete="CASCADE"), nullable=False
    )
    well: Mapped[str] = mapped_column(Text, nullable=False)  # 'B04'
    field_index: Mapped[int] = mapped_column(SmallInteger, nullable=False)  # from the _s<N> suffix

    # Loading a field means loading its plate and all of its channels; a segmentation
    # task that fetched them one at a time would be an N+1 per work unit. `raise_on_sql`
    # turns that mistake into an exception instead of a silent slowdown.
    plate: Mapped[Plate] = relationship(lazy="raise_on_sql")
    work_unit: Mapped[WorkUnit] = relationship(lazy="raise_on_sql")
    channel_files: Mapped[list["HcsChannelFile"]] = relationship(
        back_populates="field", lazy="raise_on_sql", cascade="all, delete-orphan"
    )


class HcsChannelFile(Base):
    """One file: one channel of one field.

    The channel name comes from the source CSV, which has `Image_FileName_DAPI`,
    `_Tubulin` and `_Actin` as its own columns. No code in this repository maps `w1`,
    `w2` or `w4` onto a channel — that mapping was deleted rather than fixed (§7.1).
    """

    __tablename__ = "hcs_channel_files"
    __table_args__ = (
        # Integrity checks only, deliberately NOT unique. A global unique digest would
        # not deduplicate storage — it would reject the second row that legitimately
        # references the same bytes. At 41 GB, deduplication is not worth a level of
        # indirection.
        Index("hcs_files_sha_ix", "content_sha256"),
    )

    work_unit_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("hcs_fields.work_unit_id", ondelete="CASCADE"), primary_key=True
    )
    channel: Mapped[str] = mapped_column(Text, primary_key=True)  # 'DAPI' | 'Tubulin' | 'Actin'
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)

    field: Mapped[HcsField] = relationship(back_populates="channel_files", lazy="raise_on_sql")


class WsiSlide(Base):
    """One whole-slide image. Its content digest is its identity (§7.1)."""

    __tablename__ = "wsi_slides"
    __table_args__ = (Index("wsi_sha_ix", "content_sha256"),)

    work_unit_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("work_units.id", ondelete="CASCADE"), primary_key=True
    )
    slide_uri: Mapped[str] = mapped_column(Text, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    mpp: Mapped[float | None] = mapped_column(REAL)  # microns per pixel at level 0
    level_count: Mapped[int | None] = mapped_column(SmallInteger)
    width: Mapped[int | None] = mapped_column(BigInteger)
    height: Mapped[int | None] = mapped_column(BigInteger)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)

    work_unit: Mapped[WorkUnit] = relationship(lazy="raise_on_sql")


class WellLayout(Base):
    """What each well of a plate contains.

    `control_type` is derived from the `(compound, concentration)` pair and never from the
    compound alone (§9.1). Taxol at 0.3 uM is the positive control; taxol at every other
    concentration is an ordinary point on its own dose curve. Marking by compound would
    put the curve inside the controls and turn Z' into a measurement of noise.
    """

    __tablename__ = "well_layout"

    plate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("plates.id", ondelete="CASCADE"), primary_key=True
    )
    well: Mapped[str] = mapped_column(Text, primary_key=True)
    compound: Mapped[str | None] = mapped_column(Text)
    concentration: Mapped[float | None] = mapped_column(REAL)
    moa: Mapped[str | None] = mapped_column(Text)  # reference label, where one exists
    control_type: Mapped[str | None] = mapped_column(Text)  # NULL | 'NEGATIVE' | 'POSITIVE'
