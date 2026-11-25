"""PrefixFileTransferOperator on S3, GCS, and Azure emulators.

Same checks as the file:// and memory:// unit tests: prefix copy, glob,
overwrite false, flatten versus keep, folder markers, returned keys, and
single-file parity with FileTransferOperator. Same-store copies must call
``ObjectStoragePath.copy``. Cross-store copies must stream.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("AIRFLOW_HOME", str(_ROOT / ".airflow-aip132"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

import pytest

pytest.importorskip("airflow.providers.common.io.operators.file_transfer")

from airflow.providers.common.io.operators.file_transfer import FileTransferOperator
from airflow.sdk import DAG, ObjectStoragePath
from airflow.sdk.io import attach

from aip132.file_transfer import PrefixFileTransferOperator
from generic_transfer.adapters.emulators import (
    clear_prefix,
    ensure_bucket,
    get_bytes,
    list_keys_native,
    make_fs,
    put_bytes,
    seed_tree_with_markers,
)

pytestmark = [pytest.mark.integration, pytest.mark.aip132]

FLAVOURS = ("s3", "gcs", "azure")
PROTO = {"s3": "s3", "gcs": "gs", "azure": "abfs"}
CONN = {"s3": "aip132_s3", "gcs": "aip132_gcs", "azure": "aip132_azure"}
SRC_BUCKET = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
DST_BUCKET = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}
PAIRS = [(src, dst) for src in FLAVOURS for dst in FLAVOURS]


@pytest.fixture(scope="module", autouse=True)
def _attach_emulators(require_emulators):
    for flavour in FLAVOURS:
        src = make_fs(flavour, bucket=SRC_BUCKET[flavour])
        dst = make_fs(flavour, bucket=DST_BUCKET[flavour])
        ensure_bucket(src)
        ensure_bucket(dst)
        # One filesystem per flavour sees both buckets. Attach that handle so
        # ObjectStoragePath uses the emulator rather than a public endpoint.
        attach(protocol=PROTO[flavour], conn_id=CONN[flavour], fs=src.fs)


def _execute(operator_cls, src, dst, **kwargs):
    dag_id = "aip132_" + uuid.uuid4().hex[:12]
    with DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
    ):
        operator = operator_cls(task_id="copy", src=src, dst=dst, **kwargs)
    return operator.execute({})


def _url(flavour: str, bucket: str, key: str) -> str:
    suffix = key.lstrip("/")
    return f"{PROTO[flavour]}://{bucket}/{suffix}"


def _invalidate(flavour: str) -> None:
    fs = make_fs(flavour, bucket=SRC_BUCKET[flavour]).fs
    try:
        fs.invalidate_cache()
    except Exception:
        pass


@pytest.mark.parametrize("src_flavour,dst_flavour", PAIRS)
def test_prefix_matrix(src_flavour, dst_flavour, monkeypatch):
    token = uuid.uuid4().hex[:8]
    base = f"aip132/{src_flavour}-{dst_flavour}-{token}"
    src_emu = make_fs(src_flavour, bucket=SRC_BUCKET[src_flavour])
    dst_emu = make_fs(dst_flavour, bucket=DST_BUCKET[dst_flavour])
    clear_prefix(src_emu, base)
    clear_prefix(dst_emu, base)
    seed_tree_with_markers(src_emu, base)
    put_bytes(src_emu, f"{base}/empty.txt", b"")
    put_bytes(src_emu, f"{base}/note.txt", b"note")
    put_bytes(src_emu, f"{base}/skip.bin", b"nope")
    _invalidate(src_flavour)
    _invalidate(dst_flavour)

    src_url = _url(src_flavour, SRC_BUCKET[src_flavour], f"{base}/")
    full_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/full/")
    calls: list[str] = []
    real_copy = ObjectStoragePath.copy

    def spy(self, dest, recursive=False, **kwargs):
        calls.append(str(getattr(self, "protocol", "")))
        return real_copy(self, dest, recursive=recursive, **kwargs)

    monkeypatch.setattr(ObjectStoragePath, "copy", spy)
    copied = _execute(
        PrefixFileTransferOperator,
        src_url,
        full_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
        keep_structure=True,
        folder_markers="skip",
    )
    if src_flavour == dst_flavour:
        assert calls, "same-store prefix copy should call ObjectStoragePath.copy"
    else:
        assert calls == [], calls
    monkeypatch.undo()

    _invalidate(dst_flavour)
    native = list_keys_native(dst_emu, f"{base}/full")
    files = sorted(key for key in native if not key.endswith("/"))
    expected = sorted(
        [
            f"{base}/full/a.json",
            f"{base}/full/nested/b.json",
            f"{base}/full/empty.txt",
            f"{base}/full/note.txt",
            f"{base}/full/skip.bin",
        ]
    )
    assert files == expected, native
    assert not any(key.endswith("/") for key in native)
    assert sorted(copied) == expected
    assert get_bytes(dst_emu, f"{base}/full/a.json") == b'{"id":1}'
    assert get_bytes(dst_emu, f"{base}/full/nested/b.json") == b'{"id":2}'
    assert get_bytes(dst_emu, f"{base}/full/empty.txt") == b""

    glob_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/glob/")
    globbed = _execute(
        PrefixFileTransferOperator,
        src_url,
        glob_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
        match_glob="*.json",
    )
    _invalidate(dst_flavour)
    glob_native = sorted(
        key for key in list_keys_native(dst_emu, f"{base}/glob") if not key.endswith("/")
    )
    assert glob_native == sorted([f"{base}/glob/a.json", f"{base}/glob/nested/b.json"])
    assert sorted(globbed) == glob_native

    keep_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/keep/")
    flat_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/flat/")
    _execute(
        PrefixFileTransferOperator,
        src_url,
        keep_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
        keep_structure=True,
        match_glob="*.json",
    )
    flat = _execute(
        PrefixFileTransferOperator,
        src_url,
        flat_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
        keep_structure=False,
        match_glob="*.json",
    )
    _invalidate(dst_flavour)
    keep_native = sorted(
        key for key in list_keys_native(dst_emu, f"{base}/keep") if not key.endswith("/")
    )
    flat_native = sorted(
        key for key in list_keys_native(dst_emu, f"{base}/flat") if not key.endswith("/")
    )
    assert keep_native == sorted([f"{base}/keep/a.json", f"{base}/keep/nested/b.json"])
    assert flat_native == sorted([f"{base}/flat/a.json", f"{base}/flat/b.json"])
    assert sorted(flat) == flat_native
    assert get_bytes(dst_emu, f"{base}/flat/b.json") == b'{"id":2}'

    ow_dst_key = f"{base}/ow"
    ow_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{ow_dst_key}/")
    first = _execute(
        PrefixFileTransferOperator,
        src_url,
        ow_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
        match_glob="*.txt",
    )
    assert sorted(first) == sorted([f"{ow_dst_key}/empty.txt", f"{ow_dst_key}/note.txt"])
    put_bytes(dst_emu, f"{ow_dst_key}/note.txt", b"STALE")
    put_bytes(src_emu, f"{base}/extra.txt", b"extra")
    _invalidate(src_flavour)
    _invalidate(dst_flavour)
    skipped = _execute(
        PrefixFileTransferOperator,
        src_url,
        ow_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=False,
        match_glob="*.txt",
    )
    assert sorted(skipped) == [f"{ow_dst_key}/extra.txt"]
    assert get_bytes(dst_emu, f"{ow_dst_key}/note.txt") == b"STALE"
    assert get_bytes(dst_emu, f"{ow_dst_key}/extra.txt") == b"extra"
    put_bytes(src_emu, f"{base}/note.txt", b"FRESH")
    _invalidate(src_flavour)
    replaced = _execute(
        PrefixFileTransferOperator,
        src_url,
        ow_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
        match_glob="note.txt",
    )
    assert replaced == [f"{ow_dst_key}/note.txt"]
    assert get_bytes(dst_emu, f"{ow_dst_key}/note.txt") == b"FRESH"

    file_src = _url(src_flavour, SRC_BUCKET[src_flavour], f"{base}/a.json")
    upstream_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/single-upstream")
    prefix_dst = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/single-prefix")
    upstream = _execute(
        FileTransferOperator,
        file_src,
        upstream_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
    )
    prefixed = _execute(
        PrefixFileTransferOperator,
        file_src,
        prefix_dst,
        source_conn_id=CONN[src_flavour],
        dest_conn_id=CONN[dst_flavour],
        overwrite=True,
    )
    assert upstream is None
    assert prefixed is None
    _invalidate(dst_flavour)
    assert get_bytes(dst_emu, f"{base}/single-upstream") == b'{"id":1}'
    assert get_bytes(dst_emu, f"{base}/single-prefix") == b'{"id":1}'
    shared = _url(dst_flavour, DST_BUCKET[dst_flavour], f"{base}/single-shared")
    assert (
        _execute(
            FileTransferOperator,
            file_src,
            shared,
            source_conn_id=CONN[src_flavour],
            dest_conn_id=CONN[dst_flavour],
            overwrite=True,
        )
        is None
    )
    with pytest.raises(ValueError, match="already exists") as upstream_err:
        _execute(
            FileTransferOperator,
            file_src,
            shared,
            source_conn_id=CONN[src_flavour],
            dest_conn_id=CONN[dst_flavour],
            overwrite=False,
        )
    with pytest.raises(ValueError, match="already exists") as prefix_err:
        _execute(
            PrefixFileTransferOperator,
            file_src,
            shared,
            source_conn_id=CONN[src_flavour],
            dest_conn_id=CONN[dst_flavour],
            overwrite=False,
        )
    assert str(prefix_err.value) == str(upstream_err.value)
