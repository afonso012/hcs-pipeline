#!/usr/bin/env python
"""Fetch the OpenSlide test slides for the WSI rail (spec §21, Fase 2).

Four slides, chosen rather than mirrored. The archive holds dozens and most of them would
prove nothing this pipeline needs to prove:

- `CMU-1-Small-Region.svs` (1.9 MB) — small enough for a test to open in milliseconds.
- `CMU-1.svs` (177 MB) — the canonical slide, and the negative control of §21.3: level 0 is
  about 46000x32914 pixels, roughly 4.5 GB decoded, so loading it whole under a 1.5 GB
  limit has to fail. A control that cannot fail is not a control.
- `CMU-2.svs` (373 MB) — a second slide, so that a batch is a batch.
- `Hamamatsu/CMU-1.ndpi` (189 MB) — a different vendor format. The rail claims to read
  slides, not to read Aperio, and one format cannot tell those two claims apart.

Resumable and byte-exact. An interrupted download continues with a Range request instead
of starting over, and every file is checked against the length the server declares.

The SHA-256 is computed as the bytes arrive rather than afterwards: it is the slide's
identity under `hcs.identity.wsi_slide_id`, so the index cannot be written without it.

    uv run python scripts/fetch_openslide_slides.py
"""

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import httpx

BASE_URL: Final = "https://openslide.cs.cmu.edu/download/openslide-testdata"
DEFAULT_DEST: Final = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/OpenSlide")
INDEX_NAME: Final = "slides.json"

READ_CHUNK: Final = 1024 * 1024
TIMEOUT: Final = httpx.Timeout(30.0, read=300.0)


@dataclass(frozen=True, slots=True)
class Slide:
    path: str  # vendor/file, as it sits on the server and as it lands on disk
    purpose: str


SLIDES: Final = (
    Slide("Aperio/CMU-1-Small-Region.svs", "unit tests: opens in milliseconds"),
    Slide("Aperio/CMU-1.svs", "the WSI rail, and the negative control of §21.3"),
    Slide("Aperio/CMU-2.svs", "a second slide, so that a batch is a batch"),
    Slide("Hamamatsu/CMU-1.ndpi", "a second vendor format"),
)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    wanted = [s for s in SLIDES if not args.only or any(o in s.path for o in args.only)]
    if not wanted:
        print(f"nothing matches {args.only}", file=sys.stderr)
        return 1

    _reject_unmounted(args.dest)
    index: dict[str, dict[str, object]] = _load_index(args.dest / INDEX_NAME)

    with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
        for slide in wanted:
            entry = fetch(client, slide, args.dest, dry_run=args.dry_run)
            if entry is not None:
                index[slide.path] = entry

    if not args.dry_run:
        (args.dest / INDEX_NAME).write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    print(f"\n{len(index)} slide(s) recorded in {args.dest / INDEX_NAME}")
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download the OpenSlide test slides.")
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--only", nargs="*", default=[], help="substrings of the slide paths")
    parser.add_argument("--dry-run", action="store_true", help="report sizes, download nothing")
    return parser.parse_args(argv)


def fetch(
    client: httpx.Client, slide: Slide, dest: Path, dry_run: bool
) -> dict[str, object] | None:
    """Download one slide unless it is already here, returning its index entry."""
    url = f"{BASE_URL}/{slide.path}"
    target = dest / slide.path
    expected = _remote_size(client, url)
    have = target.stat().st_size if target.exists() else 0

    if have == expected:
        print(f"{slide.path}: already here ({_mib(expected)})", flush=True)
        return None if dry_run else _entry(target, expected)
    if have > expected:
        raise ValueError(f"{target} is larger than the server's copy; delete it and retry")
    if dry_run:
        print(f"{slide.path}: would fetch {_mib(expected - have)} of {_mib(expected)}", flush=True)
        return None

    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"{slide.path}: fetching {_mib(expected - have)} — {slide.purpose}", flush=True)
    _download(client, url, target, resume_from=have)

    actual = target.stat().st_size
    if actual != expected:
        raise ValueError(f"{target}: got {actual} bytes, the server declared {expected}")
    return _entry(target, actual)


def _download(client: httpx.Client, url: str, target: Path, resume_from: int) -> None:
    """Stream to disk, resuming a partial file rather than starting over.

    Appending on resume is only safe because the server confirms the range: a 206 means it
    is sending from where we stopped, and anything else means the bytes already on disk
    describe a file that is no longer what lives at that URL.
    """
    headers = {"Range": f"bytes={resume_from}-"} if resume_from else {}
    with client.stream("GET", url, headers=headers) as response:
        response.raise_for_status()
        if resume_from and response.status_code != httpx.codes.PARTIAL_CONTENT:
            raise ValueError(f"{url}: server ignored the resume request ({response.status_code})")
        with target.open("ab" if resume_from else "wb") as handle:
            for chunk in response.iter_bytes(READ_CHUNK):
                handle.write(chunk)


def _entry(target: Path, size: int) -> dict[str, object]:
    return {"bytes": size, "sha256": _digest(target)}


def _digest(target: Path) -> str:
    """SHA-256 of a file on disk, a megabyte at a time.

    This is the slide's identity under `hcs.identity.wsi_slide_id`, so it has to be exact
    and cheap enough to recompute — which rules out holding a 373 MB slide in memory in
    order to hash it.
    """
    digest = hashlib.sha256()
    with target.open("rb") as handle:
        while chunk := handle.read(READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_size(client: httpx.Client, url: str) -> int:
    response = client.head(url)
    response.raise_for_status()
    return int(response.headers["content-length"])


def _load_index(path: Path) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    loaded: dict[str, dict[str, object]] = json.loads(path.read_text())
    return loaded


def _reject_unmounted(dest: Path) -> None:
    """Refuse to write under an absent mount point.

    `mkdir -p` on an unmounted `/Volumes/SSD` silently creates it on the internal disk,
    fills that instead, and then stops the real volume from mounting. The Makefile guards
    the MinIO path the same way and for the same reason.
    """
    parts = dest.parts
    if len(parts) > 2 and parts[1] == "Volumes" and not Path(*parts[:3]).is_dir():
        raise SystemExit(f"{Path(*parts[:3])} is not mounted")


def _mib(count: int) -> str:
    return f"{count / 1024**2:.1f} MiB"


if __name__ == "__main__":
    raise SystemExit(main())
