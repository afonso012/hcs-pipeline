"""Fixtures backed by a real PostgreSQL (spec §13.3).

Mocked storage hides exactly the bugs worth catching. For the schema specifically, an
in-memory SQLite stand-in would accept a native enum as free text, ignore the partial
index, and silently pass a migration that PostgreSQL rejects — so the tests that assert
anything about §6 run against the same major version the compose stack runs.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from docker.errors import DockerException
from sqlalchemy import Engine, create_engine
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
def fresh_postgres() -> Iterator[str]:
    """An empty PostgreSQL of its own, for tests that migrate the schema up and down."""
    container = _start_postgres()
    try:
        yield container.get_connection_url()
    finally:
        container.stop()
