"""Backpressure: admission, depth, and dispatch in blocks (spec §10).

Three things stop a ten-thousand-unit submission from flooding the broker, and they are
different mechanisms for different failure modes.

**Admission.** `POST /submit` reads the queue depth and refuses above the high watermark
with `429` and a `Retry-After` computed from observed throughput rather than guessed. A
client that is told "come back in 120 seconds" and comes back in 120 seconds is a client
that never had to be dropped.

**Dispatch in blocks.** The units of an accepted run are published a block at a time,
keeping the depth between the low and high watermarks. The run progresses without the
broker ever holding more than a block's worth beyond what the workers are chewing on.

**Redis `maxmemory` with `noeviction`**, which lives in the compose file rather than here:
a noisy write failure beats silent eviction, because an evicted task is an image that
disappears and nobody notices.

The depth is read with `LLEN`, which is what Celery's Redis transport actually stores a
queue as. Asking Celery for it would mean `inspect`, which round-trips to every worker and
answers about what they hold rather than about what is waiting.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

import redis

from hcs.config import Settings
from hcs.metrics import INGESTION_REJECTED, QUEUE_DEPTH

# Celery's default queue name, and the list key it uses in Redis.
DEFAULT_QUEUE: Final = "celery"

# Used only when nothing has been measured yet. A first submission on a cold system has no
# throughput history, and telling the client to come back immediately would have it
# hammer a saturated queue.
FALLBACK_DRAIN_RATE: Final = 2.0  # units per second


class QueueSaturatedError(RuntimeError):
    """The queue is above its high watermark. Carries what the 429 body needs (§10)."""

    def __init__(self, depth: int, limit: int, drain_seconds: int) -> None:
        super().__init__(f"queue depth {depth} is above the limit of {limit}")
        self.depth = depth
        self.limit = limit
        self.drain_seconds = drain_seconds

    def as_problem(self) -> dict[str, object]:
        """The body §10 specifies, in RFC 9457 shape."""
        return {
            "type": "/errors/queue-saturated",
            "title": "Fila saturada",
            "queue_depth": self.depth,
            "limit": self.limit,
            "estimated_drain_seconds": self.drain_seconds,
        }


@dataclass(frozen=True, slots=True)
class Watermarks:
    """Where the dispatcher holds the queue.

    Two marks rather than one, because a single threshold makes the dispatcher oscillate:
    it would publish, cross the line, stop, drain by one unit, publish again. The gap is
    hysteresis.
    """

    high: int
    low: int
    block: int


def watermarks(settings: Settings) -> Watermarks:
    return Watermarks(
        high=settings.queue_high_watermark,
        low=settings.queue_low_watermark,
        block=settings.dispatch_block_size,
    )


def queue_depth(settings: Settings, queue: str = DEFAULT_QUEUE) -> int:
    """How many tasks are waiting, straight from the broker.

    Also records the gauge, so that the number a client was refused on and the number the
    dashboard shows are the same read rather than two reads that disagree.
    """
    client = redis.Redis.from_url(settings.redis_url)
    try:
        depth = int(client.llen(queue))  # type: ignore[arg-type]
    finally:
        client.close()
    QUEUE_DEPTH.labels(queue=queue).set(depth)
    return depth


def check_admission(settings: Settings, throughput: float | None = None) -> None:
    """Refuse the submission if the queue is already saturated (§10).

    Raises:
        QueueSaturatedError: depth is above the high watermark.
    """
    depth = queue_depth(settings)
    limit = settings.queue_high_watermark
    if depth <= limit:
        return

    rate = throughput if throughput and throughput > 0 else FALLBACK_DRAIN_RATE
    INGESTION_REJECTED.labels(reason="queue_saturated").inc()
    raise QueueSaturatedError(depth=depth, limit=limit, drain_seconds=int(depth / rate) + 1)


def next_block(
    unit_ids: Sequence[uuid.UUID], offset: int, marks: Watermarks, depth: int
) -> Sequence[uuid.UUID]:
    """The slice to publish now — empty when the queue is still busy enough.

    Pure, so the dispatch policy can be tested without a broker. Everything that makes it
    hard to test lives in the caller.
    """
    if offset >= len(unit_ids) or depth > marks.low:
        return ()
    return unit_ids[offset : offset + marks.block]
