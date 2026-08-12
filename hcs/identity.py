"""Deterministic identity for work units (spec §7.1).

A work unit's primary key is a pure function of its logical identity, never a counter.
That single choice makes the manifest endpoint re-runnable by construction: inserting
`work_units` and its subtype row both become `ON CONFLICT (id) DO NOTHING`, so no
insertion order can leave an orphaned parent row counting towards `total_units`.

It is also the same principle as the lineage design in §12 — identity follows content.
"""

import re
import uuid
from typing import Final

# Frozen on 2026-08-12. Every work unit id in every database derives from this value,
# so changing it is a data migration, not a refactor.
NS_HCS: Final = uuid.UUID("93918048-52d0-4a00-ae2b-068533a34aa9")

# Reserved as the field separator in the name string, and therefore forbidden inside a
# field. Without it, ("A|B", "C") and ("A", "B|C") would hash to the same id.
_SEPARATOR: Final = "|"

_SHA256_RE: Final = re.compile(r"\A[0-9a-f]{64}\Z")


def hcs_field_id(barcode: str, well: str, field_index: int) -> uuid.UUID:
    """Identity of one imaging field: the plate barcode, the well, and the site index.

    Args:
        barcode: Plate barcode as it appears in the assay, e.g. ``Week1_22141``.
        well: Well label, e.g. ``B04``. Taken verbatim from the source CSV — case and
            zero padding are part of the identity, because normalising them here would
            silently merge wells that the source data keeps apart.
        field_index: Site index within the well, non-negative.

    Raises:
        ValueError: If any component is empty, negative, or contains the separator.
    """
    _reject_empty_or_separated(barcode, "barcode")
    _reject_empty_or_separated(well, "well")
    if field_index < 0:
        raise ValueError(f"field_index must be non-negative, got {field_index}")
    return uuid.uuid5(NS_HCS, f"HCS_FIELD|{barcode}|{well}|{field_index}")


def wsi_slide_id(content_sha256: str) -> uuid.UUID:
    """Identity of one whole-slide image: the digest of its bytes.

    A slide has no plate coordinates to be identified by, so its content is its identity.
    Digest case carries no meaning and is normalised, unlike the HCS components above.

    Raises:
        ValueError: If the digest is not 64 hexadecimal characters.
    """
    digest = content_sha256.strip().lower()
    if not _SHA256_RE.match(digest):
        raise ValueError(
            f"content_sha256 must be 64 hexadecimal characters, got {content_sha256!r}"
        )
    return uuid.uuid5(NS_HCS, f"WSI_SLIDE|{digest}")


def _reject_empty_or_separated(value: str, name: str) -> None:
    if not value:
        raise ValueError(f"{name} must not be empty")
    if _SEPARATOR in value:
        raise ValueError(f"{name} must not contain {_SEPARATOR!r}, got {value!r}")
