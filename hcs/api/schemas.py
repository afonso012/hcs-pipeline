"""Request and response bodies for §7.1.

Validation here is the system boundary. Everything past this module may assume a well
formed digest, a non-negative field index and a channel map that is not empty — which is
why `hcs.identity` can afford to raise instead of guess.
"""

import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from hcs.db import BatchStatus, UnitKind

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-fA-F]{64}$")]


class BatchCreate(BaseModel):
    external_ref: str | None = Field(
        default=None, max_length=200, description="Caller's own name for this batch"
    )


class BatchOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    batch_id: uuid.UUID
    status: BatchStatus
    total_units: int
    external_ref: str | None


class WellLayoutIn(BaseModel):
    """What one well contains (§6.2).

    There is no `control_type` field, and that omission is the point. Whether a well is a
    control follows from the `(compound, concentration)` pair by the rule in §9.1, and the
    server applies that rule itself. A client that could declare `control_type` could
    label taxol a positive control at all eight of its concentrations, and Z' would then
    measure the spread of a dose curve while still looking like a quality metric.
    """

    well: str = Field(min_length=1, max_length=8, examples=["B02"])
    compound: str | None = Field(default=None, max_length=200)
    concentration: float | None = Field(default=None, ge=0)
    moa: str | None = Field(
        default=None,
        max_length=200,
        description="Reference label where the dataset provides one; most wells have none",
    )


class PlateIn(BaseModel):
    """A plate is reference data: registering it twice is registering it once (§6.2).

    Declaring it a second time with different values is not a re-registration, it is a
    contradiction, and the endpoint answers 409 rather than picking a winner.
    """

    barcode: str = Field(min_length=1, max_length=100, examples=["Week1_22141"])
    n_rows: int = Field(gt=0, le=64)
    n_cols: int = Field(gt=0, le=64)
    # The plate layout, optional because a manifest may legitimately carry only images.
    # When present it is applied whole: the wells named here are set to what is declared,
    # including back to NULL. Unlike `expected_channels`, which defines what "complete"
    # means for units already accepted, the layout only affects analysis still to come —
    # so a correction is allowed to be a correction rather than a 409.
    wells: list[WellLayoutIn] = Field(default_factory=list, max_length=1536)
    # What a complete field looks like for this plate. It belongs to the plate rather
    # than to the manifest because it describes the imaging protocol and not the
    # submission — and because `incomplete` needs something stable to be incomplete
    # against.
    expected_channels: list[str] = Field(min_length=1, examples=[["DAPI", "Tubulin", "Actin"]])


class ChannelFileIn(BaseModel):
    uri: str = Field(min_length=1, examples=["s3://raw/Week1_22141_B04_s1_w1.tif"])
    sha256: Sha256
    width: int | None = Field(default=None, gt=0)
    height: int | None = Field(default=None, gt=0)
    size_bytes: int | None = Field(default=None, ge=0)


class HcsFieldIn(BaseModel):
    """One imaging field: the N channel files that share (plate, well, field_index).

    The channel *names* are the map's keys and come from the source CSV, which has
    `Image_FileName_DAPI`, `_Tubulin` and `_Actin` as columns of its own. Nothing in this
    codebase translates `w1`, `w2` or `w4` into a channel name — that mapping was deleted
    rather than fixed (§7.1).
    """

    well: str = Field(min_length=1, max_length=8, examples=["B04"])
    field_index: int = Field(ge=0, le=32767)
    channels: dict[str, ChannelFileIn] = Field(min_length=1)


class ManifestIn(BaseModel):
    kind: Literal[UnitKind.HCS_FIELD]
    plate: PlateIn
    units: list[HcsFieldIn] = Field(min_length=1, max_length=5000)


class ManifestOut(BaseModel):
    units_registered: int
    duplicates_skipped: int
    incomplete: int


class SubmitOut(BaseModel):
    run_id: uuid.UUID
    total_units: int
