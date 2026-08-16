"""What a run has to record about itself to be reproducible (spec §12).

A scientific result that cannot be reproduced is not a result. Three hashes stand between
this pipeline and that failure: the container image, the resolved dependency set, and the
configuration. This module owns the last two; the image digest is injected from outside,
because a container cannot learn its own digest without the Docker socket.
"""

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

# Written by the deps stage of both Dockerfiles. Baked rather than read from uv.lock at
# runtime because the lockfile is not copied into the image — and should not be, since a
# lockfile present at runtime is a lockfile that can disagree with what was installed.
BAKED_LOCKFILE_DIGEST: Final = Path("/app/lockfile.sha256")

# Outside a container, hash the real thing.
REPO_LOCKFILE: Final = Path(__file__).resolve().parent.parent / "uv.lock"


def config_hash(config: Mapping[str, Any]) -> str:
    """Digest of the canonicalised configuration.

    Canonical means sorted keys and no insignificant whitespace, so two configurations
    that differ only in dict ordering hash the same. Without that, the same configuration
    would produce a different `config_hash` on every process and comparing two runs by
    configuration would be impossible.
    """
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def lockfile_hash() -> str:
    """Digest of the resolved dependency set.

    Raises rather than returning a placeholder. A run row that claims a lineage it does
    not have is worse than a run that refuses to start: the first is discovered months
    later, by someone trying to reproduce a number and failing.
    """
    if BAKED_LOCKFILE_DIGEST.is_file():
        return BAKED_LOCKFILE_DIGEST.read_text().strip()
    if REPO_LOCKFILE.is_file():
        return hashlib.sha256(REPO_LOCKFILE.read_bytes()).hexdigest()
    raise RuntimeError(
        f"no lockfile digest: neither {BAKED_LOCKFILE_DIGEST} nor {REPO_LOCKFILE} exists, "
        "so this run could not record what it was built from"
    )
