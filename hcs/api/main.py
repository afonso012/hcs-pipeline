"""The HTTP surface (spec §7).

Invariant from §4: **the API never opens an image file.** It streams bytes into MinIO and
writes rows into Postgres; every heavy operation belongs to a worker. Anything here that
starts decoding a TIFF is a bug in the architecture, not an optimisation opportunity.

Phase 1 ships the two probes the compose stack needs to be trustworthy. `/healthz` says
the process is alive and deliberately touches nothing else — a liveness probe that fails
when Postgres blinks gets the container killed for someone else's outage. `/readyz` is
the opposite: it fails loudly when a dependency is missing, so the load balancer stops
sending traffic to an instance that cannot serve it.
"""

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from fastapi import FastAPI, Response, status
from prometheus_fastapi_instrumentator import Instrumentator
from redis import Redis
from sqlalchemy import Engine, create_engine, text

from hcs import __version__
from hcs.api import batches
from hcs.config import Settings, get_settings
from hcs.services.queue import queue_depth

logger = logging.getLogger(__name__)


def _s3_client(settings: Settings) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
        region_name=settings.s3_region,
        # Fail fast: a readiness probe that hangs for 60s never reports anything useful.
        config=BotoConfig(connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}),
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the dependency clients once, not per request."""
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    app.state.settings = settings
    # pool_pre_ping because restarting the Postgres container otherwise leaves this pool
    # holding sockets that look open and fail on first use.
    app.state.engine = create_engine(settings.database_url, pool_pre_ping=True, pool_size=5)
    app.state.redis = Redis.from_url(settings.redis_url)
    app.state.s3 = _s3_client(settings)
    try:
        yield
    finally:
        app.state.redis.close()
        app.state.engine.dispose()


app = FastAPI(title="HCS pipeline", version=__version__, lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")
app.include_router(batches.router)


@app.middleware("http")
async def _refresh_queue_depth(request: Any, call_next: Any) -> Any:
    """Read the queue depth when Prometheus asks, not on a timer.

    §14.1 wants `celery_queue_depth` as a gauge. A background task refreshing it would
    have the dashboard show a number up to its own interval old, and would keep dialling
    Redis on an idle system. Reading it as the scrape arrives makes the gauge exactly as
    fresh as the scrape and costs one `LLEN`.

    Failures are swallowed on purpose, and only here: a broker that is down should make
    `/readyz` say so, not make `/metrics` disappear at the moment somebody is looking at a
    dashboard to find out why.
    """
    if request.url.path == "/metrics":
        with suppress(Exception):
            queue_depth(request.app.state.settings)
    response: Any = await call_next(request)
    return response


@app.get("/healthz", status_code=status.HTTP_200_OK)
def healthz() -> dict[str, str]:
    """Liveness. Answers from the process alone, on purpose."""
    return {"status": "alive", "version": __version__}


@app.get("/readyz")
def readyz(response: Response) -> dict[str, Any]:
    """Readiness: can this instance actually serve a request?

    Every dependency is reported by name. A single boolean would tell whoever is on the
    other end that something is broken without telling them what, which is the difference
    between a probe and a diagnostic.
    """
    checks = {
        "postgres": _probe(lambda: _check_postgres(app.state.engine)),
        "redis": _probe(lambda: app.state.redis.ping()),
        "object_storage": _probe(lambda: _check_bucket(app.state.s3, app.state.settings)),
    }
    ready = all(result == "ok" for result in checks.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"ready": ready, "checks": checks}


def _probe(check: Callable[[], object]) -> str:
    try:
        check()
    except Exception as exc:
        logger.warning("readiness check failed: %s", exc)
        return f"error: {type(exc).__name__}"
    return "ok"


def _check_postgres(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))


def _check_bucket(client: Any, settings: Settings) -> None:
    client.head_bucket(Bucket=settings.bucket_results)
