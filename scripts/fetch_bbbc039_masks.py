#!/usr/bin/env python
"""Fetch the BBBC039 ground truth (spec §13.5).

The 200 images are already on the SSD; the annotations are not, and the annotations are the
whole point. Everything else this project measures is self-consistency — the golden batch
pins the numbers, the negative control proves the limits fire, the chunking property proves
the count does not move. None of them asks whether the objects being counted are nuclei.

Two archives, both small:

- `masks.zip` (2.6 MB) — one PNG per image, each pixel carrying the integer id of the
  nucleus it belongs to. Instance labels, not a binary mask, which is what makes it
  possible to score *this* pipeline: a method that merges two touching nuclei into one is
  wrong in a way a binary mask cannot express.
- `metadata.zip` (20 KB) — the published training/validation/test split. Kept because
  quoting an accuracy figure over a set somebody else calls "training" is how numbers stop
  being comparable to the literature.

Resumable and length-checked, like the slide fetch. Nothing here is idempotent by luck:
re-running skips what already matches.

    make data-bbbc039
"""

import argparse
import io
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import httpx

BASE_URL: Final = "https://data.broadinstitute.org/bbbc/BBBC039"
DEFAULT_DEST: Final = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/BBBC039")
ARCHIVES: Final = ("masks.zip", "metadata.zip")
TIMEOUT: Final = httpx.Timeout(30.0, read=300.0)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    _reject_unmounted(args.dest)
    args.dest.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
        for name in ARCHIVES:
            extracted = fetch(client, name, args.dest, dry_run=args.dry_run)
            print(f"{name}: {extracted} file(s)", file=sys.stderr)

    images = len(list(args.dest.glob("*.tif")))
    masks = len(list(args.dest.rglob("*.png")))
    print(f"\n{images} image(s), {masks} mask(s) under {args.dest}")
    if images and masks and images != masks:
        print(
            f"WARNING: {images} images against {masks} masks — §13.5 scores pairs, so an "
            "unmatched file is a field that will be silently skipped",
            file=sys.stderr,
        )
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download the BBBC039 ground truth.")
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def fetch(client: httpx.Client, name: str, dest: Path, dry_run: bool) -> int:
    """Download one archive and unpack it, skipping the work if it is already unpacked.

    Held in memory rather than staged to disk: these are megabytes, and a temporary file
    would be one more thing to clean up after an interrupted run.
    """
    target = dest / name.removesuffix(".zip")
    if target.is_dir() and any(target.iterdir()):
        return len(list(target.rglob("*.*")))
    if dry_run:
        print(f"{name}: would download and unpack into {target}", file=sys.stderr)
        return 0

    response = client.get(f"{BASE_URL}/{name}")
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        members = [m for m in archive.namelist() if not m.startswith("__MACOSX")]
        archive.extractall(dest, members=members)
    return len(members)


def _reject_unmounted(dest: Path) -> None:
    """Refuse to write under an absent mount point.

    `mkdir -p` on an unmounted `/Volumes/SSD` silently creates it on the internal disk and
    then stops the real volume from mounting. Guarded the same way as the MinIO path.
    """
    parts = dest.parts
    if len(parts) > 2 and parts[1] == "Volumes" and not Path(*parts[:3]).is_dir():
        raise SystemExit(f"{Path(*parts[:3])} is not mounted")


if __name__ == "__main__":
    raise SystemExit(main())
