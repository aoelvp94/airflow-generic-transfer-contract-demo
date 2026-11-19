"""S3 connection for S3Mock (default) or MinIO.

fsspec handle: ``s3fs.S3FileSystem`` with path-style addressing so the
emulator does not need virtual-host buckets.
"""
from __future__ import annotations

import os
from typing import Any


def storage_options() -> dict[str, Any]:
    backend = os.environ.get("EMULATOR_S3_BACKEND", "s3mock")
    if backend == "minio":
        endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:19000")
        key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    else:
        endpoint = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090")
        key = os.environ.get("S3_ACCESS_KEY", "any")
        secret = os.environ.get("S3_SECRET_KEY", "any")
    return {
        "key": key,
        "secret": secret,
        "client_kwargs": {
            "endpoint_url": endpoint,
            "region_name": os.environ.get("S3_REGION", "us-east-1"),
        },
        "config_kwargs": {"s3": {"addressing_style": "path"}},
        "use_listings_cache": False,
    }


def open_filesystem(*, bucket: str | None = None) -> tuple[Any, str, str, str, dict[str, Any]]:
    """Return ``(fs, bucket, root_url, endpoint, storage_options)``."""
    import s3fs

    opts = storage_options()
    name = bucket or os.environ.get("S3_SRC_BUCKET", "src-bucket")
    fs = s3fs.S3FileSystem(**opts)
    endpoint = opts["client_kwargs"]["endpoint_url"]
    return fs, name, f"s3://{name}", endpoint, opts
