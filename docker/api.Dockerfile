# syntax=docker/dockerfile:1.7
#
# The API image, deliberately without the imaging stack.
#
# §4 states the invariant: the API never opens an image file. Leaving scikit-image, dask
# and openslide out of this image is how that invariant stops being a promise — the
# import would fail. It also keeps the image small enough that `read_only: true` and a
# non-root user cost nothing to enforce.

ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim-bookworm AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

FROM base AS deps
COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /bin/uv
# Only the lock and the manifest, so a code change does not invalidate the dependency
# layer. --frozen fails loudly if uv.lock is stale instead of silently resolving anew,
# which is what makes lockfile_hash (§12) mean anything.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM base AS runtime
RUN groupadd --gid 10001 hcs \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin hcs
COPY --from=deps /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:${PATH}"

COPY hcs ./hcs
COPY alembic.ini ./
COPY migrations ./migrations

USER 10001:10001
EXPOSE 8000

# No curl in a slim image, and installing one just to run a health check is a poor trade.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD ["python", "-c", "import urllib.request as r; r.urlopen('http://127.0.0.1:8000/healthz')"]

CMD ["uvicorn", "hcs.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
