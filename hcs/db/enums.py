"""The enumerated types of spec §6.1.

These are native PostgreSQL enums rather than text with a check constraint: the database
rejects an unknown status at write time, which is the guarantee that makes the
reconciliation query in §16.4 trustworthy.

`StrEnum` members carry their own SQL spelling, so the Python name and the stored value
can never drift apart.
"""

import enum

from sqlalchemy import Enum as SqlEnum


class BatchStatus(enum.StrEnum):
    DRAFT = "DRAFT"
    READY = "READY"
    CANCELLED = "CANCELLED"


class RunStatus(enum.StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"  # some units failed, the run still produced results
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class UnitStatus(enum.StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    FAILED = "FAILED"
    QC_REJECTED = "QC_REJECTED"  # out of focus or empty well, never queued for analysis
    INCOMPLETE = "INCOMPLETE"  # a channel is missing, so the field is rejected whole


class UnitKind(enum.StrEnum):
    HCS_FIELD = "HCS_FIELD"
    WSI_SLIDE = "WSI_SLIDE"


class ErrorClass(enum.StrEnum):
    """Whether a failure is worth retrying (§16.3).

    Stored as plain text rather than a native enum, matching `unit_runs.error_class` in
    the spec: the retry policy is application logic and adding a class should not need a
    migration.
    """

    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"


def pg_enum(python_enum: type[enum.StrEnum], name: str) -> SqlEnum:
    """Bind a Python enum to a named PostgreSQL type, storing values not member names."""
    return SqlEnum(
        python_enum,
        name=name,
        values_callable=lambda members: [member.value for member in members],
    )
