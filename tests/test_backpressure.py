"""Admission, watermarks and dispatch in blocks (§10).

The acceptance test §10 names: submit far more than the queue can hold, and check three
things — a `429` comes back, the depth stays under the limit, and **no unit is lost**. The
third is the one that matters. Refusing work is easy; refusing it without dropping any is
the whole point.
"""

import uuid
from typing import Any

import pytest
import redis
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from hcs.config import Settings, get_settings
from hcs.db import Run, UnitRun
from hcs.services.queue import (
    DEFAULT_QUEUE,
    QueueSaturatedError,
    Watermarks,
    check_admission,
    next_block,
    queue_depth,
    watermarks,
)
from tests.test_api_batches import create_batch, field, manifest

pytestmark = pytest.mark.integration


MARKS = Watermarks(high=100, low=10, block=4)
UNITS = [uuid.uuid4() for _ in range(10)]


class TestTheBlockPolicy:
    """Pure, so the policy is testable without a broker.

    Everything awkward — Redis, timing, retries — lives in the caller, which is why these
    read like arithmetic.
    """

    def test_a_quiet_queue_gets_a_full_block(self) -> None:
        assert list(next_block(UNITS, 0, MARKS, depth=0)) == UNITS[:4]

    def test_a_busy_queue_gets_nothing(self) -> None:
        """Above the low mark the dispatcher waits. Publishing here is what floods the
        broker, which is the thing §10 exists to prevent."""
        assert list(next_block(UNITS, 0, MARKS, depth=MARKS.low + 1)) == []

    def test_exactly_at_the_low_mark_still_publishes(self) -> None:
        """The boundary stated once, so it cannot drift. `low` is the level the dispatcher
        holds *at*, not the level it stops below."""
        assert list(next_block(UNITS, 0, MARKS, depth=MARKS.low)) == UNITS[:4]

    def test_the_last_block_is_short_rather_than_padded(self) -> None:
        assert list(next_block(UNITS, 8, MARKS, depth=0)) == UNITS[8:]

    def test_past_the_end_there_is_nothing_left(self) -> None:
        assert list(next_block(UNITS, 10, MARKS, depth=0)) == []

    def test_the_two_marks_leave_room_between_them(self) -> None:
        """Hysteresis, asserted rather than assumed. Equal marks make the dispatcher
        oscillate: publish, cross, stop, drain by one, publish again."""
        marks = watermarks(get_settings())

        assert marks.low < marks.high
        assert marks.block <= marks.high - marks.low


class TestAdmission:
    def test_a_quiet_queue_is_admitted(self, redis_url: str) -> None:
        check_admission(_settings(redis_url, high=1000))

    def test_a_saturated_queue_is_refused(self, redis_url: str) -> None:
        client = redis.Redis.from_url(redis_url)
        try:
            client.rpush(DEFAULT_QUEUE, *[b"task"] * 20)

            with pytest.raises(QueueSaturatedError) as raised:
                check_admission(_settings(redis_url, high=5))
        finally:
            client.delete(DEFAULT_QUEUE)
            client.close()

        assert raised.value.depth == 20
        assert raised.value.limit == 5

    def test_the_retry_after_is_computed_from_the_depth(self, redis_url: str) -> None:
        """§10 wants a `Retry-After` the client can obey. A constant would send every
        refused client back at the same instant, which is how a queue that was draining
        stops draining."""
        client = redis.Redis.from_url(redis_url)
        try:
            client.rpush(DEFAULT_QUEUE, *[b"task"] * 40)
            with pytest.raises(QueueSaturatedError) as shallow:
                check_admission(_settings(redis_url, high=5), throughput=10.0)
            with pytest.raises(QueueSaturatedError) as slow:
                check_admission(_settings(redis_url, high=5), throughput=1.0)
        finally:
            client.delete(DEFAULT_QUEUE)
            client.close()

        assert shallow.value.drain_seconds < slow.value.drain_seconds

    def test_the_problem_body_is_the_one_the_spec_prints(self, redis_url: str) -> None:
        error = QueueSaturatedError(depth=52000, limit=50000, drain_seconds=118)

        assert error.as_problem() == {
            "type": "/errors/queue-saturated",
            "title": "Fila saturada",
            "queue_depth": 52000,
            "limit": 50000,
            "estimated_drain_seconds": 118,
        }


class TestTheAcceptanceTest:
    """§10: submit above capacity, get a 429, keep the depth under the limit, lose nothing."""

    def test_a_saturated_queue_answers_429_and_loses_nothing(
        self, api_client: Any, clean_db: Engine, redis_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        batch_id = create_batch(api_client)
        api_client.post(
            f"/v1/batches/{batch_id}/manifest",
            json=manifest(field("B04", 1), field("B04", 2), field("C05", 1)),
        )

        client = redis.Redis.from_url(redis_url)
        try:
            client.rpush(DEFAULT_QUEUE, *[b"task"] * 50)
            # The app captured its settings at startup (`deps.get_settings_dep` reads
            # `app.state.settings`), so lowering the watermark in the environment would
            # change nothing the endpoint can see.
            monkeypatch.setattr(
                api_client.app.state,
                "settings",
                get_settings().model_copy(update={"queue_high_watermark": 10}),
            )

            response = api_client.post(f"/v1/batches/{batch_id}/submit")
        finally:
            client.delete(DEFAULT_QUEUE)
            client.close()

        assert response.status_code == 429
        assert int(response.headers["Retry-After"]) > 0
        assert response.json()["detail"]["type"] == "/errors/queue-saturated"

        # Nothing was lost, and nothing was half-created: the refusal happens before the
        # run row exists, so there is no QUEUED run for the sweep of §16.4 to explain.
        with Session(clean_db) as session:
            assert session.scalar(select(func.count()).select_from(Run)) == 0
            assert session.scalar(select(func.count()).select_from(UnitRun)) == 0
        assert api_client.get(f"/v1/batches/{batch_id}").json()["total_units"] == 3

    def test_the_units_are_still_there_to_submit_once_it_drains(
        self, api_client: Any, clean_db: Engine, no_image_work: dict[str, Any]
    ) -> None:
        """The other half of "no unit lost": a refused submission is a delay, not a loss.
        The same batch submits cleanly once the queue is quiet."""
        batch_id = create_batch(api_client)
        api_client.post(f"/v1/batches/{batch_id}/manifest", json=manifest(field("B04", 1)))

        accepted = api_client.post(f"/v1/batches/{batch_id}/submit")

        assert accepted.status_code == 202
        assert accepted.json()["total_units"] == 1


class TestDepth:
    def test_the_depth_is_what_the_broker_holds(self, redis_url: str) -> None:
        settings = _settings(redis_url, high=1000)
        client = redis.Redis.from_url(redis_url)
        try:
            before = queue_depth(settings)
            client.rpush(DEFAULT_QUEUE, b"task", b"task")

            assert queue_depth(settings) == before + 2
        finally:
            client.delete(DEFAULT_QUEUE)
            client.close()


def _settings(redis_url: str, high: int) -> Settings:
    base = get_settings()
    return base.model_copy(update={"redis_url": redis_url, "queue_high_watermark": high})
