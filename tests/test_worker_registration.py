"""The worker must know the task exists (spec §16).

Regression test for a failure the eager-mode API tests are structurally unable to find.
`celery -A hcs.worker.celery_app worker` imports one module. If that module does not pull
in the task module, the API publishes happily, returns 202, and every message comes back
on the worker as "Received unregistered task" while the run sits at QUEUED forever. Eager
mode imports the task directly, so it never notices.

It runs in a subprocess because import side effects are not undoable: by the time any
other test has run, `hcs.worker.tasks` is already in `sys.modules` and the check would
pass for the wrong reason.
"""

import subprocess
import sys
from pathlib import Path

# `import_default_modules()` is the exact call the worker makes on startup to honour the
# `imports` setting. Checking `celery_app.tasks` without it would test a stricter thing
# than the worker actually needs, and fail on a configuration that works.
PROBE = (
    "from hcs.worker.celery_app import celery_app;"
    "celery_app.loader.import_default_modules();"
    "print('hcs.process_unit' in celery_app.tasks)"
)

REPO_ROOT = Path(__file__).resolve().parent.parent

# Enough to satisfy Settings. Nothing here is dialled; the process only imports.
FAKE_ENV = {
    "DATABASE_URL": "postgresql+psycopg://u:p@127.0.0.1:1/none",
    "REDIS_URL": "redis://127.0.0.1:1/0",
    "S3_ENDPOINT_URL": "http://127.0.0.1:1",
    "S3_ACCESS_KEY": "unused",
    "S3_SECRET_KEY": "unused",
}


def test_importing_the_celery_app_alone_registers_the_task(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", PROBE],
        capture_output=True,
        text=True,
        # cwd is a temporary directory so a developer's local .env cannot supply the
        # settings and hide a variable that CI would be missing.
        cwd=tmp_path,
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(REPO_ROOT), **FAKE_ENV},
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True", (
        "the worker command imports only hcs.worker.celery_app; add the task module to "
        "its `imports` setting"
    )
