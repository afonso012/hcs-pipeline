"""Request-scoped dependencies.

One session per request, and the *route* decides when to commit. Committing here would
mean a handler that raised halfway through a manifest could still leave the plate row
behind — the kind of partial write that makes an "idempotent" endpoint stop being one.
"""

from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.orm import Session

from hcs.config import Settings


def get_session(request: Request) -> Iterator[Session]:
    with Session(bind=request.app.state.engine) as session:
        yield session


def get_settings_dep(request: Request) -> Settings:
    """The settings the app started with, not a fresh read of the environment."""
    settings: Settings = request.app.state.settings
    return settings


SessionDep = Annotated[Session, Depends(get_session)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
