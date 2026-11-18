"""Azure Blob connection for Azurite.

fsspec handle: ``adlfs.AzureBlobFileSystem``. The default connection string
is Azurite's well-known development account (not a secret).
"""
from __future__ import annotations

import os
from typing import Any

_DEFAULT_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def storage_options() -> dict[str, Any]:
    conn = os.environ.get("AZURE_CONNECTION_STRING", _DEFAULT_CONNECTION_STRING)
    return {
        "connection_string": conn,
        "account_name": os.environ.get("AZURE_ACCOUNT_NAME", "devstoreaccount1"),
        "use_listings_cache": False,
    }


def open_filesystem(*, bucket: str | None = None) -> tuple[Any, str, str, str, dict[str, Any]]:
    """Return ``(fs, container, root_url, endpoint, storage_options)``."""
    import adlfs

    opts = storage_options()
    name = bucket or os.environ.get("AZURE_SRC_CONTAINER", "src-container")
    fs = adlfs.AzureBlobFileSystem(**opts)
    endpoint = os.environ.get(
        "AZURE_BLOB_ENDPOINT", "http://127.0.0.1:10000/devstoreaccount1"
    )
    return fs, name, f"abfs://{name}", endpoint, opts
