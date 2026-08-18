"""Talking to object storage (spec §15).

One place that knows how to build an S3 client and how to read the `s3://` URIs the
manifest stores. Both the seed and the worker come through here, so the endpoint, the
credentials and the URI grammar are stated once.
"""

from functools import lru_cache
from typing import Any, Final
from urllib.parse import urlparse

import boto3

from hcs.config import Settings, get_settings

S3_SCHEME: Final = "s3"


@lru_cache
def s3_client() -> Any:
    """One client per process, built from the environment.

    Cached because botocore's client construction parses its bundled service model, which
    is slow enough to notice when it happens once per work unit instead of once per
    worker.
    """
    return client_for(get_settings())


def client_for(settings: Settings) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
        region_name=settings.s3_region,
    )


def split_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/key` into its two halves.

    Raises rather than guessing. A URI that arrived malformed would otherwise surface as a
    404 on a bucket named after a typo, three layers away from the manifest that carried
    it.
    """
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != S3_SCHEME or not parsed.netloc or not key:
        raise ValueError(f"not an s3 object URI: {uri!r}")
    return parsed.netloc, key


def read_object(s3: Any, uri: str) -> bytes:
    """Fetch one object whole.

    Whole is right for an HCS channel: a BBBC021 TIFF is 2.5 MB, striped three rows at a
    time and uncompressed, so ranged reads would fetch the same bytes in more round trips.
    The WSI rail does not come through here — OpenSlide reads its own regions.
    """
    bucket, key = split_uri(uri)
    body: bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return body
