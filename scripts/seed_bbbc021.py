#!/usr/bin/env python
"""Load BBBC021 plates into the pipeline (§7.1, §15).

One pass per plate: upload its TIFFs to `s3://<bucket_raw>/<barcode>/<filename>`, then
publish a single manifest describing every field and the plate layout. Bytes go to
storage, metadata goes to the API, and the two never travel together (ADR-002).

Nothing here writes to PostgreSQL. The layout enters through the manifest like everything
else, which is what keeps §9.1 enforceable: this script sends `(compound, concentration)`
pairs and the server decides which of them are controls. A seed that wrote `well_layout`
directly could label a control however it liked.

Re-running is safe and resumes on both sides:

- **Storage** — every object carries the digest of its contents as metadata. A file whose
  digest and size already match is not uploaded again, so an interrupted seed costs a
  re-read of the SSD rather than a re-upload.
- **The manifest** — idempotent by construction (see `hcs.services.manifest`), because
  each unit's id is derived from `(barcode, well, field_index)` and not generated.

`width` and `height` are left unset. Reading them means opening 720 TIFFs per plate with
the imaging stack, which this script deliberately does not depend on; Phase 2's worker
opens every file anyway and is where those columns get filled.

    uv run python scripts/seed_bbbc021.py --plates Week1_22141,Week1_22161
    uv run python scripts/seed_bbbc021.py --plates all --batch-id <uuid>
"""

import argparse
import hashlib
import logging
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import httpx
from botocore.exceptions import ClientError

from hcs.config import get_settings
from hcs.datasets.bbbc021 import (
    Bbbc021Error,
    FieldRecord,
    PlateRecord,
    plate_image_dir,
    read_moa_table,
    read_plates,
)

logger = logging.getLogger("seed")

DEFAULT_ROOT = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/BBBC021")
IMAGE_CSV = "BBBC021_v1_image.csv"
MOA_CSV = "BBBC021_v1_moa.csv"

# Big enough that syscall overhead disappears against SSD throughput, small enough that a
# 1.5 GB worker limit is never the reason a seed fails.
READ_CHUNK = 1024 * 1024

# Uploads and the manifest POST are slow by nature: 720 objects and a 240-unit body.
TIMEOUT = httpx.Timeout(300.0)

HTTP_NOT_FOUND = 404


@dataclass
class PlateReport:
    barcode: str
    files_uploaded: int = 0
    files_present: int = 0
    files_missing: int = 0
    bytes_uploaded: int = 0
    units_registered: int = 0
    duplicates_skipped: int = 0
    incomplete: int = 0
    wells: int = 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", stream=sys.stdout
    )

    settings = get_settings()
    barcodes = None if args.plates == "all" else [p.strip() for p in args.plates.split(",") if p]

    try:
        moa = read_moa_table(args.root / MOA_CSV)
        plates = read_plates(args.root / IMAGE_CSV, moa, barcodes)
    except (Bbbc021Error, FileNotFoundError) as exc:
        logger.error("%s", exc)
        return 1

    logger.info(
        "%d plate(s), %d field(s), %d MoA annotation(s) available",
        len(plates),
        sum(len(plate.fields) for plate in plates.values()),
        len(moa),
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
        region_name=settings.s3_region,
    )

    with httpx.Client(base_url=args.api, timeout=TIMEOUT) as client:
        batch_id = args.batch_id or create_batch(client, args.external_ref, args.dry_run)
        reports = [
            seed_plate(plate, args.root, s3, settings.bucket_raw, client, batch_id, args.dry_run)
            for plate in plates.values()
        ]

    report(reports, batch_id, args.dry_run)
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load BBBC021 plates into the pipeline.",
        epilog="Re-running is safe: uploads skip matching objects and manifests are idempotent.",
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="the BBBC021 download")
    parser.add_argument(
        "--plates", default="Week1_22141", help="comma-separated barcodes, or 'all'"
    )
    parser.add_argument("--batch-id", help="an existing batch; omit to create one")
    parser.add_argument("--external-ref", default="bbbc021-seed", help="name for a new batch")
    parser.add_argument("--api", default="http://localhost:8080", help="base URL of the API")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read and check everything, upload and register nothing",
    )
    return parser.parse_args(argv)


def seed_plate(
    plate: PlateRecord,
    root: Path,
    s3: Any,
    bucket: str,
    client: httpx.Client,
    batch_id: str,
    dry_run: bool,
) -> PlateReport:
    """Upload one plate's files, then publish one manifest for all of it."""
    directory = plate_image_dir(root, plate.barcode)
    tally = PlateReport(barcode=plate.barcode, wells=len(plate.wells))
    units = []

    for field in plate.fields:
        channels = build_channels(field, plate.barcode, directory, s3, bucket, tally, dry_run)
        if channels is None:
            continue
        units.append({"well": field.well, "field_index": field.field_index, "channels": channels})

    logger.info(
        "%s: %d uploaded, %d already stored, %d missing",
        plate.barcode,
        tally.files_uploaded,
        tally.files_present,
        tally.files_missing,
    )
    if units:
        publish(plate, units, client, batch_id, tally, dry_run)
    return tally


def build_channels(
    field: FieldRecord,
    barcode: str,
    directory: Path,
    s3: Any,
    bucket: str,
    tally: PlateReport,
    dry_run: bool,
) -> dict[str, dict[str, Any]] | None:
    """The channel map for one field, or `None` if a file it names is not on disk.

    A field short of a channel is skipped here rather than sent. The manifest would count
    it under `incomplete` anyway (§5), and sending it would file a missing download under
    a number that also counts fields whose absence is expected.
    """
    channels: dict[str, dict[str, Any]] = {}
    for source in field.files:
        path = directory / source.filename
        if not path.is_file():
            logger.warning("missing file, field skipped: %s", path)
            tally.files_missing += 1
            return None

        digest, size = digest_of(path)
        key = f"{barcode}/{source.filename}"
        if store(s3, bucket, key, path, digest, size, dry_run):
            tally.files_uploaded += 1
            tally.bytes_uploaded += size
        else:
            tally.files_present += 1

        channels[source.channel] = {
            "uri": f"s3://{bucket}/{key}",
            "sha256": digest,
            "size_bytes": size,
        }
    return channels


def digest_of(path: Path) -> tuple[str, int]:
    """SHA-256 and size, computed as the bytes stream past.

    Never the whole file at once: §21 measures memory, and a seed that read 1.8 GB into
    RAM to hash it would be the first thing the limits caught.
    """
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(READ_CHUNK):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def store(
    s3: Any, bucket: str, key: str, path: Path, digest: str, size: int, dry_run: bool
) -> bool:
    """Upload unless the object already holds exactly these bytes. True if it uploaded.

    The comparison is against metadata the previous upload wrote, not against the ETag: a
    multipart ETag is a hash of part hashes and equals no digest of the content.
    """
    if already_stored(s3, bucket, key, digest, size):
        return False
    if dry_run:
        return True
    with path.open("rb") as handle:
        s3.upload_fileobj(handle, bucket, key, ExtraArgs={"Metadata": {"sha256": digest}})
    return True


def already_stored(s3: Any, bucket: str, key: str, digest: str, size: int) -> bool:
    """Only a 404 means "not there yet".

    Catching every `ClientError` would turn a wrong key or a missing bucket into a silent
    re-upload of 41 GB that then fails one object at a time, with the real cause three
    thousand log lines back.
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_NOT_FOUND:
            return False
        raise
    return bool(head["ContentLength"] == size and head.get("Metadata", {}).get("sha256") == digest)


def publish(
    plate: PlateRecord,
    units: list[dict[str, Any]],
    client: httpx.Client,
    batch_id: str,
    tally: PlateReport,
    dry_run: bool,
) -> None:
    body = {
        "kind": "HCS_FIELD",
        "plate": {
            "barcode": plate.barcode,
            "n_rows": plate.n_rows,
            "n_cols": plate.n_cols,
            "expected_channels": sorted({s.channel for f in plate.fields for s in f.files}),
            "wells": [
                {
                    "well": well.well,
                    "compound": well.compound,
                    "concentration": well.concentration,
                    "moa": well.moa,
                }
                for well in plate.wells
            ],
        },
        "units": units,
    }
    if dry_run:
        logger.info("%s: would register %d unit(s)", plate.barcode, len(units))
        tally.units_registered = len(units)
        return

    response = client.post(f"/v1/batches/{batch_id}/manifest", json=body)
    response.raise_for_status()
    outcome = response.json()
    tally.units_registered = outcome["units_registered"]
    tally.duplicates_skipped = outcome["duplicates_skipped"]
    tally.incomplete = outcome["incomplete"]
    logger.info("%s: %s", plate.barcode, outcome)


def create_batch(client: httpx.Client, external_ref: str, dry_run: bool) -> str:
    if dry_run:
        return "00000000-0000-0000-0000-000000000000"
    response = client.post("/v1/batches", json={"external_ref": external_ref})
    response.raise_for_status()
    batch_id: str = response.json()["batch_id"]
    return batch_id


def report(reports: list[PlateReport], batch_id: str, dry_run: bool) -> None:
    total = PlateReport(barcode="total")
    for plate in reports:
        for name in vars(total):
            if name != "barcode":
                setattr(total, name, getattr(total, name) + getattr(plate, name))

    logger.info(
        "%s batch %s: %d unit(s) registered, %d already in the batch, %d incomplete, "
        "%d well(s) of layout, %d file(s) uploaded (%.1f GiB), %d missing",
        "would seed" if dry_run else "seeded",
        batch_id,
        total.units_registered,
        total.duplicates_skipped,
        total.incomplete,
        total.wells,
        total.files_uploaded,
        total.bytes_uploaded / 1024**3,
        total.files_missing,
    )
    if not dry_run:
        logger.info("resume or extend with --batch-id %s", batch_id)


if __name__ == "__main__":
    raise SystemExit(main())
