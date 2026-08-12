"""Alembic environment for the schema of spec §6.

The connection URL comes from `DATABASE_URL`, never from `alembic.ini`, so the same
migrations run unchanged against the compose stack, a throwaway testcontainer and CI —
and no credential is ever committed.

`compare_type` and `compare_server_default` are both on. They make `alembic check` strict
enough to be worth running: without them a column that silently changed from `REAL` to
`DOUBLE PRECISION`, or lost its default, would pass as unchanged.
"""

import os
from logging.config import fileConfig
from typing import Final

from alembic import context
from sqlalchemy import create_engine, pool

from hcs.db import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Matches the compose stack of §15. Overridden by DATABASE_URL everywhere else.
DEFAULT_DATABASE_URL: Final = "postgresql+psycopg://hcs:hcs@localhost:5432/hcs"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def run_migrations_offline() -> None:
    """Render migrations as SQL without connecting, for review and for `--sql` output."""
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(database_url(), poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
