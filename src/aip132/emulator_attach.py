"""Attach local object-store emulators for a scheduler run.

``ObjectStoragePath`` opens the filesystem registered for a protocol and
connection id. The emulators need path-style S3 and gcsfs's HTTP client
(``gcsfs.core.GCSFileSystem``). A stock Airflow connection does not select
those, and gcsfs 2026.10 defaults to a gRPC client that fake-gcs-server does
not answer.

``copy_files`` calls :func:`attach_emulators` when ``AIP132_ATTACH_EMULATORS``
is ``1``, before it resolves a path. Endpoints default to the hosts in
``emulators/emulators.env`` (``S3_ENDPOINT``, ``GCS_ENDPOINT``,
``STORAGE_EMULATOR_HOST``, ``AZURE_CONNECTION_STRING``).

The Azure connection string default is Azurite's published development
account, the same value ``emulators/emulators.env`` ships.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

_attached: set[tuple[str, str | None]] = set()

# Azurite's well-known development account. Not a credential.
_AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def _scheme(path: Any) -> str:
    if isinstance(path, str):
        if "://" not in path:
            return "file"
        return path.split("://", 1)[0].lower()
    return str(getattr(path, "protocol", None) or "file").lower()


def _s3_fs() -> Any:
    import s3fs

    return s3fs.S3FileSystem(
        key=os.environ.get("S3_ACCESS_KEY", "any"),
        secret=os.environ.get("S3_SECRET_KEY", "any"),
        client_kwargs={
            "endpoint_url": os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090"),
            "region_name": os.environ.get("S3_REGION", "us-east-1"),
        },
        config_kwargs={"s3": {"addressing_style": "path"}},
        use_listings_cache=False,
    )


def _gcs_fs() -> Any:
    from gcsfs.core import GCSFileSystem

    endpoint = os.environ.get("GCS_ENDPOINT", "http://127.0.0.1:14443")
    host = os.environ.get("STORAGE_EMULATOR_HOST") or endpoint
    if "://" not in host:
        host = f"http://{host}"
    os.environ["STORAGE_EMULATOR_HOST"] = host
    return GCSFileSystem(
        endpoint_url=endpoint,
        project=os.environ.get("GCS_PROJECT", "demo"),
        token="anon",
        use_listings_cache=False,
    )


def _azure_fs() -> Any:
    import adlfs

    return adlfs.AzureBlobFileSystem(
        connection_string=os.environ.get("AZURE_CONNECTION_STRING", _AZURITE_CONNECTION_STRING),
        account_name=os.environ.get("AZURE_ACCOUNT_NAME", "devstoreaccount1"),
        use_listings_cache=False,
    )


_BUILDERS: dict[str, tuple[str, Callable[[], Any]]] = {
    "s3": ("s3", _s3_fs),
    "s3a": ("s3", _s3_fs),
    "gs": ("gs", _gcs_fs),
    "gcs": ("gs", _gcs_fs),
    "abfs": ("abfs", _azure_fs),
    "abfss": ("abfs", _azure_fs),
    "wasb": ("wasb", _azure_fs),
    "wasbs": ("wasb", _azure_fs),
}


def attach_emulators(
    src: Any,
    dst: Any,
    source_conn_id: str | None,
    dest_conn_id: str | None,
) -> None:
    """Attach emulator filesystems for the schemes of ``src`` and ``dst``."""
    from airflow.sdk.io import attach

    for path, conn_id in ((src, source_conn_id), (dst, dest_conn_id)):
        spec = _BUILDERS.get(_scheme(path))
        if spec is None:
            continue
        protocol, builder = spec
        key = (protocol, conn_id)
        if key in _attached:
            continue
        attach(protocol=protocol, conn_id=conn_id, fs=builder())
        _attached.add(key)
