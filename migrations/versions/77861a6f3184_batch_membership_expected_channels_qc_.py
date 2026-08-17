"""batch membership, expected channels, qc per readout

Three corrections to §6, all consequences of the same mistake made in different places:
describing a relation as a column on one of its ends.

1. `work_units.batch_id` made a field belong to one batch forever, so the scientific set
   could not include a plate the development batch had already used. Membership moves to
   `batch_units`.
2. `plates.expected_channels` gives "a field missing a channel" something stable to be
   missing from, and makes `unit_status.INCOMPLETE` reachable at processing time.
3. `plate_qc.readout` joins the primary key so Z' can be recorded on the raw readout as
   well as the normalised one — the only way the number says anything about what
   normalisation bought.

Autogenerate produced the skeleton and got three things wrong, each fixed below: it
dropped `batch_id` without carrying the memberships over, it added NOT NULL columns with
no default to tables that may already hold rows, and it did not notice the primary key
change at all.

Revision ID: 77861a6f3184
Revises: f6560a1d3090
Create Date: 2026-08-17 15:18:34.918453

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "77861a6f3184"
down_revision: str | Sequence[str] | None = "f6560a1d3090"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "batch_units",
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("work_unit_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["batch_id"], ["batches.id"], name=op.f("batch_units_batch_id_fk"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["work_unit_id"],
            ["work_units.id"],
            name=op.f("batch_units_work_unit_id_fk"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("batch_id", "work_unit_id", name=op.f("batch_units_pk")),
    )
    op.create_index("batch_units_unit_ix", "batch_units", ["work_unit_id"], unique=False)

    # Carry the existing memberships over BEFORE the column holding them disappears.
    # Autogenerate cannot know this and simply dropped the column, which would have
    # emptied every batch already in the database.
    op.execute(
        "INSERT INTO batch_units (batch_id, work_unit_id) "
        "SELECT batch_id, id FROM work_units "
        "ON CONFLICT DO NOTHING"
    )

    op.drop_index(op.f("work_units_batch_ix"), table_name="work_units")
    op.drop_constraint(op.f("work_units_batch_id_fk"), "work_units", type_="foreignkey")
    op.drop_column("work_units", "batch_id")

    # An empty array means "not yet declared", which is exactly the state a plate
    # registered before this column existed is in. The next manifest for that plate fills
    # it in; see `_upsert_plate`. The default is dropped immediately so new rows have to
    # say what they expect instead of inheriting a silent blank.
    op.add_column(
        "plates",
        sa.Column(
            "expected_channels", postgresql.ARRAY(sa.Text()), nullable=False, server_default="{}"
        ),
    )
    op.alter_column("plates", "expected_channels", server_default=None)

    # 'viability' is what any pre-existing row was measuring, so it is the honest
    # backfill. Same treatment: a default only long enough to fill the column.
    op.add_column(
        "plate_qc", sa.Column("readout", sa.Text(), nullable=False, server_default="viability")
    )
    op.alter_column("plate_qc", "readout", server_default=None)
    op.drop_constraint(op.f("plate_qc_pk"), "plate_qc", type_="primary")
    op.create_primary_key("plate_qc_pk", "plate_qc", ["plate_id", "run_id", "readout"])


def downgrade() -> None:
    op.drop_constraint(op.f("plate_qc_pk"), "plate_qc", type_="primary")
    # Collapsing the key would duplicate (plate_id, run_id) across readouts, so keep only
    # the normalised one. Losing the raw Z' is the cost of going back.
    op.execute("DELETE FROM plate_qc WHERE readout <> 'viability'")
    op.create_primary_key("plate_qc_pk", "plate_qc", ["plate_id", "run_id"])
    op.drop_column("plate_qc", "readout")

    op.drop_column("plates", "expected_channels")

    # A unit can be in several batches; only one survives the column. Picking the lowest
    # batch id is arbitrary but deterministic, which is the most an irreversible narrowing
    # can offer. A unit in no batch at all cannot be represented and is deleted.
    op.add_column(
        "work_units", sa.Column("batch_id", sa.UUID(), autoincrement=False, nullable=True)
    )
    op.execute(
        "UPDATE work_units SET batch_id = ("
        "  SELECT min(batch_id::text)::uuid FROM batch_units WHERE work_unit_id = work_units.id"
        ")"
    )
    op.execute("DELETE FROM work_units WHERE batch_id IS NULL")
    op.alter_column("work_units", "batch_id", nullable=False)
    op.create_foreign_key(
        op.f("work_units_batch_id_fk"),
        "work_units",
        "batches",
        ["batch_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(op.f("work_units_batch_ix"), "work_units", ["batch_id", "kind"], unique=False)

    op.drop_index("batch_units_unit_ix", table_name="batch_units")
    op.drop_table("batch_units")
