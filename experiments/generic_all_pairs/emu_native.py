"""Native-SDK emulator setup that does not depend on s3fs/gcsfs/adlfs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from generic_transfer.adapters.emulators import (
    _azure_storage_options,
    _gcs_storage_options,
    _s3_storage_options,
    load_emulator_env,
)


@dataclass
class NativeEmu:
    flavour: str
    bucket: str
    root_url: str
    endpoint: str
    storage_options: dict[str, Any]
    fs: Any = None


def make_native_emu(flavour: str, bucket: str, *, repo_env=None) -> NativeEmu:
    if repo_env is not None:
        load_emulator_env(repo_env)
    else:
        load_emulator_env()
    flavour = flavour.lower()
    if flavour == "s3":
        opts = _s3_storage_options()
        return NativeEmu(
            flavour="s3",
            bucket=bucket,
            root_url=f"s3://{bucket}",
            endpoint=opts["client_kwargs"]["endpoint_url"],
            storage_options=opts,
        )
    if flavour == "gcs":
        opts = _gcs_storage_options()
        return NativeEmu(
            flavour="gcs",
            bucket=bucket,
            root_url=f"gs://{bucket}",
            endpoint=opts["endpoint_url"],
            storage_options=opts,
        )
    if flavour == "azure":
        opts = _azure_storage_options()
        return NativeEmu(
            flavour="azure",
            bucket=bucket,
            root_url=f"wasb://{bucket}",
            endpoint=os.environ.get("AZURE_BLOB_ENDPOINT", "http://127.0.0.1:10000/devstoreaccount1"),
            storage_options=opts,
        )
    raise ValueError(flavour)


def native_client(emu: NativeEmu):
    from generic_transfer.adapters.emulators import _native_client

    return _native_client(emu)  # type: ignore[arg-type]


def ensure_native_bucket(emu: NativeEmu) -> None:
    client = native_client(emu)
    if emu.flavour == "s3":
        try:
            client.create_bucket(Bucket=emu.bucket)
        except Exception:
            pass
        return
    if emu.flavour == "gcs":
        try:
            client.create_bucket(emu.bucket)
        except Exception:
            pass
        return
    if emu.flavour == "azure":
        try:
            client.create_container(emu.bucket)
        except Exception:
            pass
        return
    raise ValueError(emu.flavour)


def wipe_backend(backend, bucket: str) -> None:
    from contract import ListQuery, ObjectRef

    for obj in list(backend.list_objects(ListQuery(bucket, ""))):
        try:
            backend.delete(ObjectRef(bucket, obj.key, backend.flavour))
        except Exception:
            pass
