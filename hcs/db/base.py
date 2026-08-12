"""Declarative base for the schema of spec §6.

Every constraint gets its name from a convention rather than from PostgreSQL. Alembic
compares constraints *by name*: with server-invented names, `alembic check` reports drift
that does not exist and every generated migration carries noise. Naming them here makes
the comparison meaningful, which is what lets a test assert that the models and the
migration have not diverged.
"""

from typing import Final

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# `column_0_N_name` concatenates every column in the constraint, so composite keys get
# distinct names instead of colliding on the first column.
NAMING_CONVENTION: Final[dict[str, str]] = {
    "ix": "%(table_name)s_%(column_0_N_name)s_ix",
    "uq": "%(table_name)s_%(column_0_N_name)s_uq",
    "ck": "%(table_name)s_%(constraint_name)s_ck",
    "fk": "%(table_name)s_%(column_0_N_name)s_fk",
    "pk": "%(table_name)s_pk",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
