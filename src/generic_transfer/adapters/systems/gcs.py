"""GCS connection for fake-gcs-server's HTTP JSON API.

``gcsfs`` 2026.10+ re-exports ``ExtendedGcsFileSystem`` (gRPC). That client
cannot talk to fake-gcs-server, so this module pins ``gcsfs.core.GCSFileSystem``.
"""
from __future__ import annotations

import os
from typing import Any


def storage_options() -> dict[str, Any]:
    endpoint = os.environ.get("GCS_ENDPOINT", "http://127.0.0.1:14443")
    # google-cloud-storage accepts ``host:port`` or ``http://host:port``.
    host = os.environ.get("STORAGE_EMULATOR_HOST") or endpoint
    if "://" not in host:
        host = f"http://{host}"
    os.environ["STORAGE_EMULATOR_HOST"] = host
    return {
        "endpoint_url": endpoint,
        "project": os.environ.get("GCS_PROJECT", "demo"),
        "token": "anon",
        "use_listings_cache": False,
    }


def open_filesystem(*, bucket: str | None = None) -> tuple[Any, str, str, str, dict[str, Any]]:
    """Return ``(fs, bucket, root_url, endpoint, storage_options)``."""
    from gcsfs.core import GCSFileSystem

    opts = storage_options()
    name = bucket or os.environ.get("GCS_SRC_BUCKET", "src-bucket")
    fs = GCSFileSystem(**opts)
    return fs, name, f"gs://{name}", opts["endpoint_url"], opts
