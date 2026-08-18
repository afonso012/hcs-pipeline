"""Reading one work unit back for processing (spec §6.2, §8.1).

The manifest wrote the unit; this reads it. Joined rather than lazy — a worker that
fetched the plate and then each channel row separately would issue an N+1 per unit, which
at 13 200 units is the difference between a run and a morning.
"""

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from hcs.db import HcsChannelFile, HcsField, Plate


class UnitNotFoundError(LookupError):
    """No HCS field with this id. A permanent failure, not something to retry."""


class IncompleteUnitError(ValueError):
    """The unit is missing a channel its plate says every field should have.

    This is the failure `plates.expected_channels` exists to make detectable, and the only
    thing that makes `unit_status.INCOMPLETE` reachable at all: at manifest time there is
    no run to write a status to, and here there is.
    """


@dataclass(frozen=True, slots=True)
class HcsFieldSource:
    """Everything the imaging layer needs about one field, and nothing more."""

    work_unit_id: uuid.UUID
    barcode: str
    well: str
    field_index: int
    channels: dict[str, str]


def load_hcs_field(session: Session, work_unit_id: uuid.UUID) -> HcsFieldSource:
    """Look up one field's channel URIs, checked against its plate's protocol.

    Raises:
        UnitNotFoundError: no such field.
        IncompleteUnitError: a channel the plate expects is not registered.
    """
    row = session.execute(
        select(HcsField.well, HcsField.field_index, Plate.barcode, Plate.expected_channels)
        .join(Plate, Plate.id == HcsField.plate_id)
        .where(HcsField.work_unit_id == work_unit_id)
    ).one_or_none()
    if row is None:
        raise UnitNotFoundError(f"no HCS field with id {work_unit_id}")

    well, field_index, barcode, expected = row
    channels: dict[str, str] = {
        channel: uri
        for channel, uri in session.execute(
            select(HcsChannelFile.channel, HcsChannelFile.storage_uri).where(
                HcsChannelFile.work_unit_id == work_unit_id
            )
        ).all()
    }

    missing = sorted(set(expected) - channels.keys())
    if missing:
        raise IncompleteUnitError(
            f"{barcode} {well} field {field_index} is missing {missing}; "
            f"the plate expects {sorted(expected)}"
        )

    return HcsFieldSource(
        work_unit_id=work_unit_id,
        barcode=barcode,
        well=well,
        field_index=field_index,
        channels=channels,
    )
