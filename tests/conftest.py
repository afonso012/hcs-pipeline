"""Fixtures backed by a real PostgreSQL (spec §13.3).

Mocked storage hides exactly the bugs worth catching. For the schema specifically, an
in-memory SQLite stand-in would accept a native enum as free text, ignore the partial
index, and silently pass a migration that PostgreSQL rejects — so the tests that assert
anything about §6 run against the same major version the compose stack runs.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from docker.errors import DockerException
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
POSTGRES_IMAGE = "postgres:17-alpine"


def _start_postgres() -> PostgresContainer:
    try:
        return PostgresContainer(POSTGRES_IMAGE, driver="psycopg").start()
    except DockerException as exc:  # pragma: no cover - depends on the host
        pytest.skip(f"Docker is not available: {exc}")


def alembic_config(url: str) -> Config:
    """Point Alembic at `url`.

    `migrations/env.py` reads DATABASE_URL rather than alembic.ini, so the environment is
    the only place to set it — which is the same path the compose stack and CI take.
    """
    os.environ["DATABASE_URL"] = url
    return Config(str(PROJECT_ROOT / "alembic.ini"))


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """A PostgreSQL shared by the whole session, migrated to head exactly once."""
    container = _start_postgres()
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A real Redis, because §10's backpressure reads a real queue.

    It used to be a closed port on purpose — nothing dialled the broker, and pointing at
    `127.0.0.1:1` proved it. Admission changed that: `POST /submit` now reads the queue
    depth before accepting anything, so the depth has to be a number somebody could have
    got wrong. §13.3 asked for this container from the start.
    """
    from testcontainers.redis import RedisContainer

    container = RedisContainer("redis:7.4-alpine")
    container.start()
    try:
        yield f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"
    finally:
        container.stop()


@pytest.fixture
def alembic_cfg(migrated_engine: Engine, postgres_url: str) -> Config:
    """Alembic pointed at the already-migrated session database."""
    return alembic_config(postgres_url)


@pytest.fixture(scope="session")
def migrated_engine(postgres_url: str) -> Iterator[Engine]:
    command.upgrade(alembic_config(postgres_url), "head")
    engine = create_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def session(migrated_engine: Engine) -> Iterator[Session]:
    """A session whose work is rolled back, so tests share one migrated database."""
    connection = migrated_engine.connect()
    transaction = connection.begin()
    try:
        with Session(bind=connection, join_transaction_mode="create_savepoint") as db:
            yield db
    finally:
        transaction.rollback()
        connection.close()


@pytest.fixture
def clean_db(migrated_engine: Engine) -> Iterator[Engine]:
    """For tests that must commit, because a second connection has to see their writes.

    The rollback trick in `session` cannot be used when the code under test opens its own
    connection — an eagerly executed Celery task does exactly that, and would find an
    empty database. Truncating afterwards is the honest alternative: `batches` and
    `plates` cascade to everything else in §6.
    """
    yield migrated_engine
    with migrated_engine.begin() as connection:
        # work_units is listed explicitly: it no longer hangs off batches, so truncating
        # batches alone leaves every unit behind and the next test starts dirty.
        connection.execute(text("TRUNCATE batches, plates, work_units, api_keys CASCADE"))


@pytest.fixture
def api_client(
    clean_db: Engine, postgres_url: str, redis_url: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Any]:
    """The real app, wired to the test database, with tasks running inline.

    `task_always_eager` turns `.delay()` into a direct call, so one HTTP request exercises
    the whole Phase 1 path — manifest, run, worker, marked unit — without a broker. It is
    not a substitute for the chaos test of §13.4, which needs real workers to kill.
    """
    from fastapi.testclient import TestClient

    monkeypatch.setenv("DATABASE_URL", postgres_url)
    # A real broker: `POST /submit` reads the queue depth before admitting anything (§10),
    # so a closed port would make every submission a 500 rather than a decision.
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("S3_ACCESS_KEY", "test")
    monkeypatch.setenv("S3_SECRET_KEY", "test")

    from hcs.config import get_settings
    from hcs.worker import tasks
    from hcs.worker.celery_app import celery_app

    get_settings.cache_clear()
    tasks._engine.cache_clear()
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True

    from hcs.api.main import app

    try:
        with TestClient(app) as client:
            yield client
    finally:
        celery_app.conf.task_always_eager = False
        celery_app.conf.task_eager_propagates = False
        tasks._engine.cache_clear()
        get_settings.cache_clear()


@pytest.fixture
def no_image_work(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stand in for `perform`, so state-machine tests stay tests of the state machine.

    Since Phase 2, `perform` fetches TIFFs from object storage and writes Parquet back.
    The tests that assert on statuses, counters and run closure are not about that; they
    want a unit of work that succeeds. The imaging path has its own tests, and the real
    thing is exercised against the running stack rather than against a fake S3.

    Not autouse. The failure tests patch `perform` themselves to make it raise, and a
    fixture that quietly won the race would let those tests pass for the wrong reason.
    """
    from hcs.worker import tasks

    qc = {"cells": 42, "touching_border": 3, "results_key": "cell_metrics/stub.parquet"}
    monkeypatch.setattr(tasks, "perform", lambda unit, run: qc)
    return qc


@pytest.fixture
def fresh_postgres() -> Iterator[str]:
    """An empty PostgreSQL of its own, for tests that migrate the schema up and down."""
    container = _start_postgres()
    try:
        yield container.get_connection_url()
    finally:
        container.stop()
