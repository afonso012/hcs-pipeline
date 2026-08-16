"""Celery application and its failure semantics (spec §16.1).

Every option below exists because its default is wrong for this workload.

`task_acks_late` moves the acknowledgement to *after* the task finishes, so a SIGKILLed
worker returns its unit to the queue instead of losing it — the invariant the chaos test
of §13.4 checks. `worker_prefetch_multiplier = 1` stops one worker hoarding units it will
not get to; with 2.6 MB fields and a 1.5 GB budget, prefetching is how a worker OOMs on
work it has not started.

`visibility_timeout` **must exceed the longest task.** If it does not, Redis redelivers a
task that is still running and two workers process the same unit, wasting twice the
compute and racing to write the same Parquet path.
"""

from celery import Celery

from hcs.config import get_settings

settings = get_settings()

TASK_TIME_LIMIT_SECONDS = 1800  # hard kill
TASK_SOFT_TIME_LIMIT_SECONDS = 1500  # catchable, leaves room to clean up
VISIBILITY_TIMEOUT_SECONDS = 3600  # > TASK_TIME_LIMIT_SECONDS, see the module docstring

celery_app = Celery("hcs", broker=settings.redis_url)
celery_app.conf.update(
    # The worker starts as `celery -A hcs.worker.celery_app`, which imports this module
    # and nothing else. Without this line the task module is never loaded on the worker
    # side, so every published task comes back as "Received unregistered task" while the
    # API reports 202 and the run sits at QUEUED forever. Eager-mode tests cannot catch
    # it, because they import the task directly.
    imports=("hcs.worker.tasks",),
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_time_limit=TASK_TIME_LIMIT_SECONDS,
    task_soft_time_limit=TASK_SOFT_TIME_LIMIT_SECONDS,
    broker_transport_options={"visibility_timeout": VISIBILITY_TIMEOUT_SECONDS},
    # Progress is read from `unit_runs` in Postgres, the source of truth (ADR-001).
    # A result backend would be a second, weaker copy of the same state.
    result_backend=None,
    task_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)


@celery_app.task(name="hcs.ping")
def ping() -> str:
    """Smallest possible round trip through the broker.

    Exists so `make smoke` can prove the queue actually carries work, rather than proving
    that two containers happened to start.
    """
    return "pong"
