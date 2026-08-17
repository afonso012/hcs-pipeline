"""The schema of spec §6.

Importing this package is what populates `Base.metadata`. Alembic autogenerate compares
metadata against the live database, so a model module that nobody imports is a table
Alembic will happily propose dropping.
"""

from hcs.db.aggregates import (
    ApiKey,
    BatchEffect,
    DoseResponse,
    MoaPrediction,
    PlateQc,
    WellProfile,
)
from hcs.db.base import Base
from hcs.db.core import Batch, BatchUnit, Run, UnitRun, WorkUnit
from hcs.db.enums import BatchStatus, ErrorClass, RunStatus, UnitKind, UnitStatus
from hcs.db.subtypes import HcsChannelFile, HcsField, Plate, WellLayout, WsiSlide

__all__ = [
    "ApiKey",
    "Base",
    "Batch",
    "BatchEffect",
    "BatchStatus",
    "BatchUnit",
    "DoseResponse",
    "ErrorClass",
    "HcsChannelFile",
    "HcsField",
    "MoaPrediction",
    "Plate",
    "PlateQc",
    "Run",
    "RunStatus",
    "UnitKind",
    "UnitRun",
    "UnitStatus",
    "WellLayout",
    "WellProfile",
    "WorkUnit",
    "WsiSlide",
]
