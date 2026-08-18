#!/usr/bin/env python
"""Load whole-slide images into the pipeline (§7.1, ADR-002).

The WSI counterpart of `seed_bbbc021.py`, and shorter for a reason: a slide has no plate,
no wells and no channels. It has a file and a digest, and the digest is its identity.

Upload to `s3://<bucket_raw>/slides/<vendor>/<file>`, then one manifest naming them all.
The optical metadata comes from OpenSlide reading the file, never from the filename or
from a guess — a manifest that invented `mpp` would put a wrong scale under every
measurement downstream, and nothing later could tell.

Re-running is safe on both sides, the same way the plate seed is: the object is skipped
when its digest already matches, and a slide's unit id is `uuid5` of its content, so the
manifest is idempotent by construction.

    uv run python scripts/seed_slides.py --only CMU-1-Small-Region CMU-1.svs
"""

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import httpx

from hcs.config import get_settings
from hcs.storage import client_for

DEFAULT_ROOT: Final = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/OpenSlide")
INDEX_NAME: Final = "slides.json"
KEY_PREFIX: Final = "slides"
TIMEOUT: Final = httpx.Timeout(300.0)
HTTP_NOT_FOUND: Final = 404


@dataclass(frozen=True, slots=True)
class Staged:
    relative: str
    uri: str
    sha256: str
    size_bytes: int
    uploaded: bool


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    index = _load_index(args.root / INDEX_NAME)
    if not index:
        print(f"no {INDEX_NAME} under {args.root}; run `make data-wsi` first", file=sys.stderr)
        return 1

    chosen = [rel for rel in sorted(index) if not args.only or any(o in rel for o in args.only)]
    if not chosen:
        print(f"nothing in {INDEX_NAME} matches {args.only}", file=sys.stderr)
        return 1

    settings = get_settings()
    s3 = client_for(settings)
    staged = [stage(s3, settings.bucket_raw, args.root, rel, index[rel], args) for rel in chosen]

    if args.dry_run:
        for slide in staged:
            print(f"{slide.relative}: would register {slide.uri}")
        return 0

    with httpx.Client(base_url=args.api, timeout=TIMEOUT) as client:
        batch_id = args.batch_id or create_batch(client, args.external_ref)
        outcome = publish(client, batch_id, staged, args.root)

    print(f"\nbatch {batch_id}: {outcome}")
    print(f"submit it with: curl -XPOST {args.api}/v1/batches/{batch_id}/submit")
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload and register whole-slide images.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--only", nargs="*", default=[], help="substrings of the slide paths")
    parser.add_argument("--batch-id", help="an existing batch; omit to create one")
    parser.add_argument("--external-ref", default="wsi-dev", help="name for a new batch")
    parser.add_argument("--api", default="http://localhost:8080")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def stage(
    s3: Any, bucket: str, root: Path, relative: str, entry: dict[str, Any], args: Any
) -> Staged:
    """Put one slide in object storage, skipping the upload if it is already there."""
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(f"{path} is in {INDEX_NAME} but not on disk")

    key = f"{KEY_PREFIX}/{relative}"
    digest, size = entry["sha256"], entry["bytes"]
    uploaded = False

    if not args.dry_run and not _already_stored(s3, bucket, key, digest, size):
        print(f"{relative}: uploading {size / 1024**2:.1f} MiB", flush=True)
        with path.open("rb") as handle:
            s3.upload_fileobj(handle, bucket, key, ExtraArgs={"Metadata": {"sha256": digest}})
        uploaded = True
    else:
        print(f"{relative}: already stored", flush=True)

    return Staged(relative, f"s3://{bucket}/{key}", digest, size, uploaded)


def publish(
    client: httpx.Client, batch_id: str, staged: Sequence[Staged], root: Path
) -> dict[str, Any]:
    body = {
        "kind": "WSI_SLIDE",
        "slides": [
            {"uri": slide.uri, "sha256": slide.sha256, "size_bytes": slide.size_bytes}
            | _optics(root / slide.relative)
            for slide in staged
        ],
    }
    response = client.post(f"/v1/batches/{batch_id}/manifest", json=body)
    response.raise_for_status()
    outcome: dict[str, Any] = response.json()
    return outcome


def _optics(path: Path) -> dict[str, Any]:
    """Read `mpp`, the level count and the level-0 size from the slide itself.

    From the file rather than from the manifest author, because these are the numbers every
    measurement downstream is scaled by. A slide that reports no `mpp` — some vendors do
    not — reports none here either, rather than a plausible default that would silently
    become the scale bar.
    """
    from hcs.imaging.wsi import open_slide

    slide = open_slide(path)
    try:
        width, height = slide.level_dimensions[0]
        mpp = slide.properties.get("openslide.mpp-x")
        return {
            "mpp": float(mpp) if mpp else None,
            "level_count": slide.level_count,
            "width": width,
            "height": height,
        }
    finally:
        slide.close()


def _already_stored(s3: Any, bucket: str, key: str, digest: str, size: int) -> bool:
    from botocore.exceptions import ClientError

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_NOT_FOUND:
            return False
        raise
    return bool(head["ContentLength"] == size and head.get("Metadata", {}).get("sha256") == digest)


def create_batch(client: httpx.Client, external_ref: str) -> str:
    response = client.post("/v1/batches", json={"external_ref": external_ref})
    response.raise_for_status()
    batch_id: str = response.json()["batch_id"]
    return batch_id


def _load_index(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    loaded: dict[str, dict[str, Any]] = json.loads(path.read_text())
    return loaded


if __name__ == "__main__":
    raise SystemExit(main())
