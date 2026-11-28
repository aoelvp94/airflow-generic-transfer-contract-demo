"""Run the storage contract on S3, GCS, and Azure emulators.

Same behaviours as ``tests/conformance/test_storage_contract.py``. Each
flavour is a :class:`tests.conformance.harness.Root`. Same-store pairs share
a protocol and connection id. The other six pairs stream.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("AIRFLOW_HOME", str(_ROOT / ".airflow-aip132"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

import pytest

pytest.importorskip("airflow.providers.common.io.operators.file_transfer")

from airflow.sdk.io import attach

from generic_transfer.adapters.emulators import (
    clear_prefix,
    ensure_bucket,
    get_bytes,
    make_fs,
    put_bytes,
)
from tests.conformance.contract import StorageContract
from tests.conformance.harness import Root, StorePair

pytestmark = [pytest.mark.integration, pytest.mark.aip132]

FLAVOURS = ("s3", "gcs", "azure")
PROTO = {"s3": "s3", "gcs": "gs", "azure": "abfs"}
CONN = {"s3": "aip132_s3", "gcs": "aip132_gcs", "azure": "aip132_azure"}
SRC_BUCKET = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
DST_BUCKET = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}
PAIRS = [(src, dst) for src in FLAVOURS for dst in FLAVOURS]
SAME = [(flavour, flavour) for flavour in FLAVOURS]
CROSS = [(src, dst) for src in FLAVOURS for dst in FLAVOURS if src != dst]


def _pair_id(value: tuple[str, str]) -> str:
    return f"{value[0]}-{value[1]}"


@pytest.fixture(scope="module", autouse=True)
def _attach_emulators(require_emulators):
    for flavour in FLAVOURS:
        src = make_fs(flavour, bucket=SRC_BUCKET[flavour])
        dst = make_fs(flavour, bucket=DST_BUCKET[flavour])
        ensure_bucket(src)
        ensure_bucket(dst)
        attach(protocol=PROTO[flavour], conn_id=CONN[flavour], fs=src.fs)


class RemoteRoot(Root):
    """One emulator prefix. Markers are real zero-byte keys."""

    def __init__(self, flavour: str, bucket: str, key_prefix: str, conn_id: str) -> None:
        uri = f"{PROTO[flavour]}://{bucket}/{key_prefix}/"
        super().__init__(uri, conn_id, object_markers=True)
        self.flavour = flavour
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.emu = make_fs(flavour, bucket=bucket)

    def write(self, relative: str, data: bytes) -> None:
        put_bytes(self.emu, f"{self.key_prefix}/{relative.lstrip('/')}", data)
        self.refresh()

    def read(self, relative: str) -> bytes:
        self.refresh()
        key = f"{self.key_prefix}/{relative.lstrip('/')}"
        try:
            return get_bytes(self.emu, key)
        except Exception:
            alt = key[:-1] if key.endswith("/") else key + "/"
            return get_bytes(self.emu, alt)

    def mkdir(self, relative: str) -> None:
        self.write_marker(relative)

    def write_marker(self, relative: str) -> None:
        rel = relative.strip("/")
        put_bytes(self.emu, f"{self.key_prefix}/{rel}/", b"")
        self.refresh()

    def refresh(self) -> None:
        from aip132.path_ops import resolve_storage_path

        for fs in (
            self.emu.fs,
            resolve_storage_path(self.uri, self.conn_id).fs,
        ):
            try:
                fs.invalidate_cache()
            except Exception:
                pass

    def sibling(self) -> RemoteRoot:
        return _remote_root(self.flavour, self.bucket, self.conn_id)


def _remote_root(flavour: str, bucket: str, conn_id: str) -> RemoteRoot:
    key_prefix = f"aip132-contract/{uuid.uuid4().hex[:10]}"
    root = RemoteRoot(flavour, bucket, key_prefix, conn_id)
    clear_prefix(root.emu, key_prefix)
    root.refresh()
    return root


def _remote_pair(src_flavour: str, dst_flavour: str) -> StorePair:
    return StorePair(
        _remote_root(src_flavour, SRC_BUCKET[src_flavour], CONN[src_flavour]),
        _remote_root(dst_flavour, DST_BUCKET[dst_flavour], CONN[dst_flavour]),
    )


class TestObjectStoreContract(StorageContract):
    """S3, GCS, and Azure, including the six cross-store pairs."""

    __test__ = True

    @pytest.fixture(params=PAIRS, ids=_pair_id)
    def pair(self, request):
        return _remote_pair(*request.param)

    @pytest.fixture(params=SAME, ids=_pair_id)
    def same_pair(self, request):
        return _remote_pair(*request.param)

    @pytest.fixture(params=CROSS, ids=_pair_id)
    def cross_pair(self, request):
        return _remote_pair(*request.param)
