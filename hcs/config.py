"""Runtime configuration, read from the environment (spec §15).

Secrets live in `.env`, outside the compose file, and nothing here carries a usable
default for one. A missing credential raises at startup naming the variable that is
missing, rather than surfacing as an opaque 403 from MinIO on the first upload.
"""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    # --- state ---
    database_url: str = Field(description="SQLAlchemy URL, psycopg driver")
    redis_url: str = Field(description="Celery broker; also the backpressure counter store")

    # --- object storage ---
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: SecretStr
    s3_region: str = "us-east-1"
    bucket_raw: str = "raw"
    bucket_results: str = "results"

    # --- processing (§8.1) ---
    # Chunk and overlap are configuration, not constants: Phase 2 exists to measure how
    # RSS responds to them. The overlap must exceed the diameter of the largest expected
    # cell, or the centroid policy drops objects it should have kept.
    chunk_size: int = 1024
    chunk_overlap: int = 32
    # The channel objects are found in (§8.2). An assay decision, not a constant: DAPI for
    # BBBC021, something else for another stain set. It belongs in the run config because
    # changing it changes every row downstream.
    segmentation_channel: str = "DAPI"

    # --- backpressure (§10) ---
    # Two marks, not one. A single threshold makes the dispatcher oscillate across it —
    # publish, cross, stop, drain by one, publish again — and the gap between these is the
    # hysteresis that stops it. The block is how much gets published at a time, so the
    # broker never holds more than one block beyond what the workers are chewing on.
    queue_high_watermark: int = 50_000
    queue_low_watermark: int = 5_000
    dispatch_block_size: int = 500

    # --- lineage (§12) ---
    pipeline_version: str = "classic-0.0.1"
    random_seed: int = 20260812
    # Digest of the running container image, injected by compose. Never a tag: a tag is a
    # moving pointer and cannot identify what produced a run.
    image_digest: str = "unknown"

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Cached so every module in a process sees the same values."""
    return Settings()  # type: ignore[call-arg]  # every field comes from the environment
