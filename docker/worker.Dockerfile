# syntax=docker/dockerfile:1.7
#
# The worker image: everything the API image refuses to carry.
#
# openslide-python is a binding, not an implementation — without libopenslide0 from apt
# the import fails at runtime rather than at build time, which is the worst place for it
# to fail. The runtime stage installs the shared library; no headers or compilers are
# kept, because nothing is compiled here.
#
# This image is NOT read-only. Dask spills to disk under memory pressure, and that is a
# feature rather than a leak: the flat-RSS graph of Phase 2 depends on it being allowed
# to.

ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim-bookworm AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# prometheus_client's multiprocess mode writes one file per process here, and the exporter
# in the parent sums them at scrape time. Without it, every metric a forked task increments
# is invisible — see the note on `_serve_metrics`. Owned by the unprivileged user because
# the pool children write to it.
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus

FROM base AS deps
COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /bin/uv
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --extra imaging
# The digest of what was actually resolved, baked so hcs.lineage can record it
# without the lockfile being present at runtime (a lockfile in the image is a
# lockfile that can disagree with what was installed).
RUN sha256sum uv.lock | cut -d' ' -f1 > /app/lockfile.sha256

FROM base AS runtime
RUN apt-get update \
    && apt-get install --no-install-recommends -y libopenslide0 \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 10001 hcs \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin hcs

COPY --from=deps /app/.venv /app/.venv
COPY --from=deps /app/lockfile.sha256 /app/lockfile.sha256
ENV PATH="/app/.venv/bin:${PATH}"

# Dask writes spill files here, owned by the runtime user so the container does not need
# to be root to survive memory pressure.
RUN mkdir -p /var/tmp/dask && chown 10001:10001 /var/tmp/dask
ENV DASK_TEMPORARY_DIRECTORY=/var/tmp/dask

COPY hcs ./hcs

USER 10001:10001

# `celery inspect ping` asks the worker itself, not the broker: a worker wedged on a task
# while holding a live Redis connection would pass any check that only looked at Redis.
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD celery -A hcs.worker.celery_app inspect ping -d "celery@$HOSTNAME"

RUN mkdir -p /tmp/prometheus && chown 10001:10001 /tmp/prometheus

CMD ["celery", "-A", "hcs.worker.celery_app", "worker", \
     "--loglevel=INFO", "--concurrency=1", "--without-gossip", "--without-mingle"]
