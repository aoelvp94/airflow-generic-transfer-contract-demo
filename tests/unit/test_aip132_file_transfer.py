"""PrefixFileTransferOperator on file:// and memory://. No Docker."""

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
from airflow.sdk import BaseOperator, DAG, ObjectStoragePath

from aip132.file_transfer import PrefixFileTransferOperator
from aip132.path_ops import (
    _attach_memory,
    _copy_stream,
    child_path,
    classify_folder_markers,
    collapse_marker_aliases,
    copy_files,
    copy_path,
    list_destination_names,
    list_prefix_once,
    resolve_storage_path,
)

pytestmark = pytest.mark.aip132


@pytest.fixture(autouse=True)
def _memory_backend():
    _attach_memory(None)


@pytest.fixture(params=["file", "memory"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[str, str, str]:
    token = uuid.uuid4().hex[:10]
    if request.param == "file":
        base = tmp_path / token
        src = base / "src"
        dst = base / "dst"
        src.mkdir(parents=True)
        dst.mkdir(parents=True)
        return request.param, _prefix_uri(src), _prefix_uri(dst)
    return request.param, f"memory:///{token}/src/", f"memory:///{token}/dst/"


def _prefix_uri(path: Path) -> str:
    uri = path.resolve().as_uri()
    return uri if uri.endswith("/") else uri + "/"


def _root(url: str) -> ObjectStoragePath:
    return resolve_storage_path(url, None)


def _write(root: str, relative: str, data: bytes) -> None:
    path = child_path(_root(root), relative)
    if (path.protocol or "file") in {"file", "local"}:
        Path(path.path).parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)


def _mkdir(root: str, relative: str) -> None:
    path = child_path(_root(root), relative)
    path.mkdir(parents=True, exist_ok=True)


def _read(root: str, relative: str) -> bytes:
    path = child_path(_root(root), relative)
    with path.open("rb") as handle:
        return handle.read()


def _rels(dst: str, keys: list[str] | None) -> list[str]:
    assert keys is not None
    root_key = _root(dst).key.lstrip("/").rstrip("/")
    out: list[str] = []
    for key in keys:
        text = key.lstrip("/")
        if root_key and text.startswith(root_key + "/"):
            out.append(text[len(root_key) + 1 :])
        else:
            out.append(text)
    return sorted(out)


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


def _seed(src: str) -> None:
    _mkdir(src, "nested")
    _write(src, "a.txt", b"alpha")
    _write(src, "nested/b.txt", b"beta")
    _write(src, "empty.txt", b"")
    _write(src, "note.json", b'{"id":1}')
    _write(src, "nested/c.json", b'{"id":2}')
    _write(src, "skip.bin", b"nope")


def test_operator_subclasses_file_transfer_and_base_operator():
    from aip132.file_transfer import FileTransferOperator as DemoFileTransferOperator

    assert PrefixFileTransferOperator is DemoFileTransferOperator
    assert issubclass(PrefixFileTransferOperator, FileTransferOperator)
    assert issubclass(PrefixFileTransferOperator, BaseOperator)
    assert copy_files is copy_path


def test_classify_folder_markers_uses_the_listing_only():
    entries = [
        {"name": "pre/", "size": 0, "type": "directory"},
        {"name": "pre/a.txt", "size": 4, "type": "file"},
        {"name": "pre/nested", "size": 0, "type": "file"},
        {"name": "pre/nested/b.txt", "size": 4, "type": "file"},
        {"name": "pre/empty.txt", "size": 0, "type": "file"},
        {"name": "pre/dir", "size": 0, "type": "directory"},
    ]
    flags = {row["name"]: row["is_marker"] for row in classify_folder_markers(entries)}
    assert flags == {
        "pre/": True,
        "pre/a.txt": False,
        "pre/nested": True,
        "pre/nested/b.txt": False,
        "pre/empty.txt": False,
        "pre/dir": True,
    }


def test_collapse_marker_aliases_keeps_one_object_key():
    """A withdirs directory and the zero-byte object are one marker.

    The object name is the one that ends with ``/``. A directory row alone
    (adlfs drops the slash) gets that slash back. A real file is unchanged.
    """
    both = collapse_marker_aliases(
        [
            {"name": "pre/nested", "size": 0, "type": "directory", "is_marker": True},
            {"name": "pre/nested/", "size": 0, "type": "file", "is_marker": True},
            {"name": "pre/a.txt", "size": 4, "type": "file", "is_marker": False},
        ]
    )
    assert [row["name"] for row in both] == ["pre/a.txt", "pre/nested/"]
    azure = collapse_marker_aliases(
        [{"name": "pre/nested", "size": 0, "type": "directory", "is_marker": True}]
    )
    assert azure[0]["name"] == "pre/nested/"
    empty = collapse_marker_aliases(
        [{"name": "pre/hold", "size": 0, "type": "file", "is_marker": True}]
    )
    assert empty[0]["name"] == "pre/hold"


def test_list_prefix_once_does_not_stat_each_key():
    class _FS:
        def __init__(self) -> None:
            self.finds = 0

        def find(self, prefix, detail=True, **kwargs):
            self.finds += 1
            return {
                "bucket/pre/": {"name": "bucket/pre/", "size": 0, "type": "directory"},
                "bucket/pre/a.txt": {"name": "bucket/pre/a.txt", "size": 3, "type": "file"},
                "bucket/pre/nested": {"name": "bucket/pre/nested", "size": 0, "type": "file"},
                "bucket/pre/nested/b.txt": {
                    "name": "bucket/pre/nested/b.txt",
                    "size": 1,
                    "type": "file",
                },
                "bucket/pre/empty.txt": {"name": "bucket/pre/empty.txt", "size": 0, "type": "file"},
            }

        def info(self, *args, **kwargs):
            raise AssertionError("info/HEAD per key")

        def exists(self, *args, **kwargs):
            raise AssertionError("exists/HEAD per key")

    fs = _FS()

    class _Path:
        path = "bucket/pre"

    path = _Path()
    path.fs = fs

    rows = list_prefix_once(path)  # type: ignore[arg-type]
    assert fs.finds == 1
    markers = {row["name"] for row in rows if row["is_marker"]}
    files = {row["name"] for row in rows if not row["is_marker"]}
    assert "bucket/pre/nested" in markers
    assert "bucket/pre/empty.txt" in files
    assert "bucket/pre/a.txt" in files


def test_destination_listing_does_not_stat_each_key():
    class _FS:
        def find(self, prefix, detail=True, **kwargs):
            return ["bucket/dst/a.txt", "bucket/dst/nested/b.txt"]

        def info(self, *args, **kwargs):
            raise AssertionError("info")

        def exists(self, *args, **kwargs):
            raise AssertionError("exists")

    class _Path:
        path = "bucket/dst"
        fs = _FS()

    names = list_destination_names(_Path())  # type: ignore[arg-type]
    assert "bucket/dst/a.txt" in names
    assert "bucket/dst/nested/b.txt" in names


def test_prefix_copy_returns_keys_and_keeps_bytes(store):
    _backend, src, dst = store
    _seed(src)
    copied = _execute(PrefixFileTransferOperator, src, dst, overwrite=True)
    assert _rels(dst, copied) == [
        "a.txt",
        "empty.txt",
        "nested/b.txt",
        "nested/c.json",
        "note.json",
        "skip.bin",
    ]
    assert _read(dst, "a.txt") == b"alpha"
    assert _read(dst, "nested/b.txt") == b"beta"
    assert _read(dst, "empty.txt") == b""
    assert _read(dst, "note.json") == b'{"id":1}'
    assert not any(key.endswith("/") for key in copied)


def test_folder_markers_copy_keeps_real_marker_keys(store, monkeypatch):
    """``copy`` writes an empty object for a real marker key and returns it.

    Directory entries on ``file://`` and ``memory://`` are not objects, so
    they stay out of the returned keys. A zero-byte key that the listing
    classified as a marker is copied as-is (no extra slash added).
    """
    from aip132 import path_ops

    _backend, src, dst = store
    dst_skip = dst[:-1] + "-skip/"
    if dst.startswith("file:"):
        Path(ObjectStoragePath(dst_skip).path).mkdir(parents=True, exist_ok=True)
    _mkdir(src, "nested")
    _write(src, "a.txt", b"alpha")
    _write(src, "nested/b.txt", b"beta")
    prefix = path_ops._fs_prefix(_root(src).fs, _root(src).path)
    real_list = path_ops.list_entries

    def _listing(path, _real=real_list, _prefix=prefix):
        rows = _real(path)
        rows.append({"name": f"{_prefix}/hold", "size": 0, "type": "file", "is_marker": True})
        rows.append(
            {"name": f"{_prefix}/dirmark", "size": 0, "type": "directory", "is_marker": True}
        )
        return rows

    monkeypatch.setattr(path_ops, "list_entries", _listing)
    copied = _execute(
        PrefixFileTransferOperator,
        src,
        dst,
        overwrite=True,
        folder_markers="copy",
    )
    rels = _rels(dst, copied)
    assert "a.txt" in rels
    assert "nested/b.txt" in rels
    assert "hold" in rels
    assert "dirmark" not in rels
    assert not any(rel.endswith("/") for rel in rels)
    assert _read(dst, "hold") == b""
    assert _read(dst, "a.txt") == b"alpha"
    assert _read(dst, "nested/b.txt") == b"beta"
    skipped = _execute(
        PrefixFileTransferOperator,
        src,
        dst_skip,
        overwrite=True,
        folder_markers="skip",
    )
    skip_rels = _rels(dst_skip, skipped)
    assert "hold" not in skip_rels
    assert "dirmark" not in skip_rels
    assert "a.txt" in skip_rels and "nested/b.txt" in skip_rels


def test_folder_markers_copy_skips_directory_entries_on_file_and_memory(store):
    _backend, src, dst = store
    _mkdir(src, "nested")
    _write(src, "a.txt", b"alpha")
    _write(src, "nested/b.txt", b"beta")
    _write(src, "empty.txt", b"")
    copied = _execute(
        PrefixFileTransferOperator,
        src,
        dst,
        overwrite=True,
        folder_markers="copy",
    )
    assert _rels(dst, copied) == ["a.txt", "empty.txt", "nested/b.txt"]
    assert not any(key.endswith("/") for key in copied)


def test_folder_markers_are_omitted(store):
    _backend, src, dst = store
    _mkdir(src, "nested")
    _write(src, "a.txt", b"alpha")
    _write(src, "nested/b.txt", b"beta")
    _write(src, "empty.txt", b"")
    listed = list_prefix_once(_root(src))
    assert any(row["is_marker"] for row in listed), listed
    copied = _execute(
        PrefixFileTransferOperator,
        src,
        dst,
        overwrite=True,
        folder_markers="skip",
    )
    rels = _rels(dst, copied)
    assert rels == ["a.txt", "empty.txt", "nested/b.txt"]
    assert "nested" not in rels
    assert not any(rel.endswith("/") for rel in rels)


def test_match_glob_filters(store):
    _backend, src, dst = store
    _seed(src)
    copied = _execute(
        PrefixFileTransferOperator,
        src,
        dst,
        overwrite=True,
        match_glob="*.json",
    )
    assert _rels(dst, copied) == ["nested/c.json", "note.json"]
    dst_nested = dst[:-1] + "-nested/"
    if dst.startswith("file:"):
        Path(ObjectStoragePath(dst_nested).path).mkdir(parents=True, exist_ok=True)
    copied_nested = _execute(
        PrefixFileTransferOperator,
        src,
        dst_nested,
        overwrite=True,
        match_glob="nested/*",
    )
    assert _rels(dst_nested, copied_nested) == ["nested/b.txt", "nested/c.json"]


def test_overwrite_false_skips_existing_keys(store):
    _backend, src, dst = store
    _write(src, "a.txt", b"first")
    _write(src, "b.txt", b"second")
    first = _execute(PrefixFileTransferOperator, src, dst, overwrite=True)
    assert _rels(dst, first) == ["a.txt", "b.txt"]
    _write(src, "a.txt", b"changed")
    _write(src, "c.txt", b"third")
    # Stale destination bytes must survive the skip.
    _write(dst, "a.txt", b"STALE")
    skipped = _execute(PrefixFileTransferOperator, src, dst, overwrite=False)
    assert _rels(dst, skipped) == ["c.txt"]
    assert _read(dst, "a.txt") == b"STALE"
    assert _read(dst, "b.txt") == b"second"
    assert _read(dst, "c.txt") == b"third"
    replaced = _execute(PrefixFileTransferOperator, src, dst, overwrite=True)
    assert "a.txt" in _rels(dst, replaced)
    assert _read(dst, "a.txt") == b"changed"


def test_flatten_versus_keep_structure(store):
    _backend, src, dst_keep = store
    dst_flat = dst_keep[:-1] + "-flat/"
    if dst_keep.startswith("file:"):
        Path(ObjectStoragePath(dst_flat).path).mkdir(parents=True, exist_ok=True)
    _mkdir(src, "nested")
    _write(src, "a.txt", b"alpha")
    _write(src, "nested/b.txt", b"beta")
    kept = _execute(PrefixFileTransferOperator, src, dst_keep, overwrite=True, keep_structure=True)
    flat = _execute(PrefixFileTransferOperator, src, dst_flat, overwrite=True, keep_structure=False)
    assert _rels(dst_keep, kept) == ["a.txt", "nested/b.txt"]
    assert _rels(dst_flat, flat) == ["a.txt", "b.txt"]
    assert _read(dst_flat, "b.txt") == b"beta"
    assert _read(dst_keep, "nested/b.txt") == b"beta"


def test_flatten_collision_skips_later_key_when_overwrite_is_false(store):
    _backend, src, dst = store
    _mkdir(src, "left")
    _mkdir(src, "right")
    _write(src, "left/a.txt", b"left")
    _write(src, "right/a.txt", b"right")
    copied = _execute(
        PrefixFileTransferOperator,
        src,
        dst,
        overwrite=False,
        keep_structure=False,
    )
    assert _rels(dst, copied) == ["a.txt"]
    # Source keys sort left/ before right/, so the first write wins.
    assert _read(dst, "a.txt") == b"left"


def test_single_file_matches_upstream(store):
    _backend, src, dst = store
    _write(src, "one.txt", b"payload")
    src_file = str(child_path(_root(src), "one.txt"))
    upstream_dst = str(child_path(_root(dst), "upstream.txt"))
    prefix_dst = str(child_path(_root(dst), "prefix.txt"))
    upstream = _execute(FileTransferOperator, src_file, upstream_dst, overwrite=False)
    prefixed = _execute(PrefixFileTransferOperator, src_file, prefix_dst, overwrite=False)
    assert upstream is None
    assert prefixed is None
    assert _read(dst, "upstream.txt") == b"payload"
    assert _read(dst, "prefix.txt") == b"payload"
    with pytest.raises(ValueError, match="already exists"):
        _execute(FileTransferOperator, src_file, upstream_dst, overwrite=False)
    with pytest.raises(ValueError, match="already exists"):
        _execute(PrefixFileTransferOperator, src_file, prefix_dst, overwrite=False)
    # Exact parity when both operators target the same destination path.
    shared = str(child_path(_root(dst), "shared.txt"))
    assert _execute(FileTransferOperator, src_file, shared, overwrite=True) is None
    with pytest.raises(ValueError) as again_upstream:
        _execute(FileTransferOperator, src_file, shared, overwrite=False)
    with pytest.raises(ValueError) as again_prefix:
        _execute(PrefixFileTransferOperator, src_file, shared, overwrite=False)
    assert str(again_prefix.value) == str(again_upstream.value)
    _execute(PrefixFileTransferOperator, src_file, shared, overwrite=True)
    assert _read(dst, "shared.txt") == b"payload"


def test_same_store_prefix_uses_object_storage_copy(store, monkeypatch):
    _backend, src, dst = store
    _write(src, "a.txt", b"alpha")
    _write(src, "b.txt", b"beta")
    calls: list[str] = []
    real = ObjectStoragePath.copy

    def spy(self, dest, recursive=False, **kwargs):
        calls.append(str(self))
        return real(self, dest, recursive=recursive, **kwargs)

    monkeypatch.setattr(ObjectStoragePath, "copy", spy)
    copied = _execute(PrefixFileTransferOperator, src, dst, overwrite=True)
    assert _rels(dst, copied) == ["a.txt", "b.txt"]
    assert len(calls) == 2
    assert _read(dst, "a.txt") == b"alpha"


def test_cross_store_streams_without_object_storage_copy(tmp_path: Path, monkeypatch):
    token = uuid.uuid4().hex[:10]
    src_dir = tmp_path / token / "src"
    src_dir.mkdir(parents=True)
    src = _prefix_uri(src_dir)
    dst = f"memory:///{token}/dst/"
    _write(src, "a.txt", b"abcdefghijklmnopqrstuvwxyz")
    calls: list[str] = []
    real = ObjectStoragePath.copy

    def spy(self, dest, recursive=False, **kwargs):
        calls.append("copy")
        return real(self, dest, recursive=recursive, **kwargs)

    monkeypatch.setattr(ObjectStoragePath, "copy", spy)
    copied = _execute(PrefixFileTransferOperator, src, dst, overwrite=True, chunk_size=4)
    assert calls == []
    assert _rels(dst, copied) == ["a.txt"]
    assert _read(dst, "a.txt") == b"abcdefghijklmnopqrstuvwxyz"


def test_stream_reads_fixed_size_chunks():
    import io

    source = io.BytesIO(b"abcdefghij")
    target = io.BytesIO()
    reads: list[int] = []
    real = source.read

    def read(n=-1):
        data = real(n)
        reads.append((n, len(data)))
        return data

    source.read = read  # type: ignore[method-assign]
    _copy_stream(source, target, 4)
    assert target.getvalue() == b"abcdefghij"
    assert reads == [(4, 4), (4, 4), (4, 2), (4, 0)]


def test_helper_matches_operator(store):
    _backend, src, dst = store
    dst_op = dst[:-1] + "-op/"
    if dst.startswith("file:"):
        Path(ObjectStoragePath(dst_op).path).mkdir(parents=True, exist_ok=True)
    _write(src, "a.txt", b"alpha")
    _mkdir(src, "nested")
    _write(src, "nested/b.txt", b"beta")
    via_helper = copy_files(src, dst, overwrite=True, folder_markers="skip")
    via_operator = _execute(PrefixFileTransferOperator, src, dst_op, overwrite=True, folder_markers="skip")
    assert _rels(dst, via_helper) == _rels(dst_op, via_operator) == ["a.txt", "nested/b.txt"]
    assert _read(dst, "nested/b.txt") == _read(dst_op, "nested/b.txt") == b"beta"


def test_accepts_object_storage_path_instances(store):
    _backend, src, dst = store
    _write(src, "a.txt", b"alpha")
    copied = _execute(
        PrefixFileTransferOperator,
        _root(src),
        _root(dst),
        overwrite=True,
    )
    assert _rels(dst, copied) == ["a.txt"]
    assert _read(dst, "a.txt") == b"alpha"


def test_bad_folder_marker_policy_raises():
    with pytest.raises(ValueError, match="folder_markers"):
        PrefixFileTransferOperator(
            task_id="copy",
            src="memory:///bad/src/",
            dst="memory:///bad/dst/",
            folder_markers="maybe",
        )


def test_demo_dag_copies_s3_prefix_to_gcs():
    import examples.aip132_s3_prefix_to_gcs as demo

    assert demo.dag.dag_id == "aip132_s3_prefix_to_gcs"
    task = demo.dag.get_task("copy_s3_prefix_to_gcs")
    assert isinstance(task, PrefixFileTransferOperator)
    assert str(task.src).startswith("s3://")
    assert str(task.dst).startswith("gs://")
    assert task.source_conn_id == "aip132_s3"
    assert task.dest_conn_id == "aip132_gcs"
    assert task.folder_markers == "skip"
    assert task.keep_structure is True
