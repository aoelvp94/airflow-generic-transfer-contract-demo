"""Behaviour every AIP-132 storage backend has to share.

Subclass :class:`StorageContract` and provide three fixtures:

* ``pair`` — every ordered pair this backend should copy, including same-store
* ``same_pair`` — pairs whose two ends share a protocol and connection id
* ``cross_pair`` — pairs whose ends do not

The tests call :func:`aip132.path_ops.copy_path`. One test also runs
``PrefixFileTransferOperator`` and checks it returns the same keys, which is
the whole of the operator's job.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from airflow.providers.common.io.operators.file_transfer import (
    FileTransferOperator as UpstreamFileTransferOperator,
)
from airflow.sdk import DAG, ObjectStoragePath

from aip132.file_transfer import PrefixFileTransferOperator
from aip132.path_ops import _fs_prefix, copy_path, list_entries, relative_key, resolve_storage_path
from tests.conformance.harness import Root, child_uri, relatives


class StorageContract:
    """Prefix-copy behaviour. Not collected on its own (``__test__`` is false)."""

    __test__ = False

    def test_single_file_matches_upstream(self, pair) -> None:
        pair.src.write("one.txt", b"payload")
        src_file = child_uri(pair.src, "one.txt")
        upstream_dst = child_uri(pair.dst, "upstream.txt")
        path_dst = child_uri(pair.dst, "path.txt")
        upstream = _execute_upstream(pair, src_file, upstream_dst, overwrite=False)
        copied = _copy_files(pair.src, pair.dst, src_file, path_dst, overwrite=False)
        assert upstream is None
        assert copied is None
        assert pair.dst.read("upstream.txt") == b"payload"
        assert pair.dst.read("path.txt") == b"payload"
        shared = child_uri(pair.dst, "shared.txt")
        assert _execute_upstream(pair, src_file, shared, overwrite=True) is None
        try:
            _execute_upstream(pair, src_file, shared, overwrite=False)
        except ValueError as upstream_err:
            upstream_message = str(upstream_err)
        else:
            raise AssertionError("upstream overwrite=False did not raise")
        try:
            _copy_files(pair.src, pair.dst, src_file, shared, overwrite=False)
        except ValueError as path_err:
            assert str(path_err) == upstream_message
        else:
            raise AssertionError("copy_path overwrite=False did not raise")

    def test_prefix_copy_returns_keys_and_bytes(self, pair) -> None:
        _seed_files(pair.src)
        copied = _copy(pair.src, pair.dst, overwrite=True)
        assert relatives(pair.dst, copied) == [
            "a.txt",
            "empty.txt",
            "nested/b.txt",
            "nested/c.json",
            "note.json",
            "skip.bin",
        ]
        assert copied == sorted(copied)
        assert pair.dst.read("a.txt") == b"alpha"
        assert pair.dst.read("nested/b.txt") == b"beta"
        assert pair.dst.read("empty.txt") == b""
        assert not any(key.endswith("/") for key in copied)

    def test_match_glob_filters(self, pair) -> None:
        _seed_files(pair.src)
        copied = _copy(pair.src, pair.dst, overwrite=True, match_glob="*.json")
        assert relatives(pair.dst, copied) == ["nested/c.json", "note.json"]
        nested_dst = pair.dst.sibling()
        nested = _copy(pair.src, nested_dst, overwrite=True, match_glob="nested/*")
        assert relatives(nested_dst, nested) == ["nested/b.txt", "nested/c.json"]

    def test_overwrite_false_skips_existing_keys(self, pair) -> None:
        pair.src.write("a.txt", b"first")
        pair.src.write("b.txt", b"second")
        first = _copy(pair.src, pair.dst, overwrite=True)
        assert relatives(pair.dst, first) == ["a.txt", "b.txt"]
        pair.src.write("a.txt", b"changed")
        pair.src.write("c.txt", b"third")
        pair.dst.write("a.txt", b"STALE")
        skipped = _copy(pair.src, pair.dst, overwrite=False)
        assert relatives(pair.dst, skipped) == ["c.txt"]
        assert pair.dst.read("a.txt") == b"STALE"
        assert pair.dst.read("b.txt") == b"second"
        assert pair.dst.read("c.txt") == b"third"
        replaced = _copy(pair.src, pair.dst, overwrite=True)
        assert "a.txt" in relatives(pair.dst, replaced)
        assert pair.dst.read("a.txt") == b"changed"

    def test_keep_structure(self, pair) -> None:
        pair.src.write("a.txt", b"alpha")
        pair.src.write("nested/b.txt", b"beta")
        flat_dst = pair.dst.sibling()
        kept = _copy(pair.src, pair.dst, overwrite=True, keep_structure=True)
        flat = _copy(pair.src, flat_dst, overwrite=True, keep_structure=False)
        assert relatives(pair.dst, kept) == ["a.txt", "nested/b.txt"]
        assert relatives(flat_dst, flat) == ["a.txt", "b.txt"]
        assert pair.dst.read("nested/b.txt") == b"beta"
        assert flat_dst.read("b.txt") == b"beta"

    def test_folder_markers_skip_copies_zero_byte_file(self, pair) -> None:
        _seed_with_marker(pair.src)
        copied = _copy(pair.src, pair.dst, overwrite=True, folder_markers="skip")
        rels = relatives(pair.dst, copied)
        assert "a.txt" in rels
        assert "empty.txt" in rels
        assert "nested/b.txt" in rels
        assert "nested" not in rels
        assert "nested/" not in rels
        assert pair.dst.read("empty.txt") == b""
        assert not any(key.endswith("/") for key in copied)

    def test_folder_markers_copy(self, pair) -> None:
        _seed_with_marker(pair.src)
        copied = _copy(pair.src, pair.dst, overwrite=True, folder_markers="copy")
        rels = relatives(pair.dst, copied)
        assert "a.txt" in rels
        assert "empty.txt" in rels
        assert "nested/b.txt" in rels
        assert pair.dst.read("empty.txt") == b""
        assert pair.dst.read("a.txt") == b"alpha"
        marker_rels = [rel for rel in rels if rel.rstrip("/") == "nested"]
        if pair.src.object_markers:
            assert marker_rels == ["nested/"]
            assert pair.dst.read("nested/") == b""
        else:
            # A local directory is not an object key, so it is not copied.
            assert marker_rels == []
            assert not any(rel.endswith("/") for rel in rels)

    def test_returned_keys_match_destination_listing(self, pair) -> None:
        pair.src.write("a.txt", b"alpha")
        pair.src.write("nested/b.txt", b"beta")
        pair.src.write("empty.txt", b"")
        copied = _copy(pair.src, pair.dst, overwrite=True, folder_markers="skip")
        rels = relatives(pair.dst, copied)
        assert rels == ["a.txt", "empty.txt", "nested/b.txt"]
        pair.dst.refresh()
        assert _listed_files(pair.dst) == rels
        assert pair.dst.read("empty.txt") == b""

    def test_operator_matches_copy_path(self, pair) -> None:
        pair.src.write("a.txt", b"alpha")
        pair.src.write("nested/b.txt", b"beta")
        via_helper = _copy(pair.src, pair.dst, overwrite=True, folder_markers="skip")
        other = pair.dst.sibling()
        via_operator = _execute(pair.src, other, overwrite=True, folder_markers="skip")
        assert relatives(pair.dst, via_helper) == relatives(other, via_operator)
        assert pair.dst.read("nested/b.txt") == other.read("nested/b.txt") == b"beta"

    def test_same_store_uses_object_storage_copy(self, same_pair, monkeypatch) -> None:
        same_pair.src.write("a.txt", b"alpha")
        same_pair.src.write("b.txt", b"beta")
        calls: list[str] = []
        real = ObjectStoragePath.copy

        def spy(self, dest, recursive=False, **kwargs):
            calls.append(str(self))
            return real(self, dest, recursive=recursive, **kwargs)

        monkeypatch.setattr(ObjectStoragePath, "copy", spy)
        copied = _copy(same_pair.src, same_pair.dst, overwrite=True)
        assert relatives(same_pair.dst, copied) == ["a.txt", "b.txt"]
        assert len(calls) == 2
        assert same_pair.dst.read("a.txt") == b"alpha"

    def test_cross_store_streams(self, cross_pair, monkeypatch) -> None:
        cross_pair.src.write("a.txt", b"abcdefghijklmnopqrstuvwxyz")
        calls: list[str] = []
        real = ObjectStoragePath.copy

        def spy(self, dest, recursive=False, **kwargs):
            calls.append("copy")
            return real(self, dest, recursive=recursive, **kwargs)

        monkeypatch.setattr(ObjectStoragePath, "copy", spy)
        copied = _copy(cross_pair.src, cross_pair.dst, overwrite=True, chunk_size=4)
        assert calls == []
        assert relatives(cross_pair.dst, copied) == ["a.txt"]
        assert cross_pair.dst.read("a.txt") == b"abcdefghijklmnopqrstuvwxyz"


def _seed_files(src: Root) -> None:
    src.write("a.txt", b"alpha")
    src.write("nested/b.txt", b"beta")
    src.write("empty.txt", b"")
    src.write("note.json", b'{"id":1}')
    src.write("nested/c.json", b'{"id":2}')
    src.write("skip.bin", b"nope")


def _seed_with_marker(src: Root) -> None:
    src.write("a.txt", b"alpha")
    src.write("nested/b.txt", b"beta")
    src.write("empty.txt", b"")
    src.write_marker("nested")


def _copy(src: Root, dst: Root, **kwargs):
    dst.refresh()
    src.refresh()
    return copy_path(
        src.uri,
        dst.uri,
        source_conn_id=src.conn_id,
        dest_conn_id=dst.conn_id,
        **kwargs,
    )


def _copy_files(src: Root, dst: Root, src_uri: str, dst_uri: str, **kwargs):
    return copy_path(
        src_uri,
        dst_uri,
        source_conn_id=src.conn_id,
        dest_conn_id=dst.conn_id,
        **kwargs,
    )


def _execute(src: Root, dst: Root, **kwargs):
    dag_id = "aip132_contract_" + uuid.uuid4().hex[:12]
    with DAG(dag_id=dag_id, schedule=None, start_date=datetime(2026, 1, 1), catchup=False):
        operator = PrefixFileTransferOperator(
            task_id="copy",
            src=src.uri,
            dst=dst.uri,
            source_conn_id=src.conn_id,
            dest_conn_id=dst.conn_id,
            **kwargs,
        )
    return operator.execute({})


def _execute_upstream(pair, src_uri: str, dst_uri: str, **kwargs):
    dag_id = "aip132_upstream_" + uuid.uuid4().hex[:12]
    with DAG(dag_id=dag_id, schedule=None, start_date=datetime(2026, 1, 1), catchup=False):
        operator = UpstreamFileTransferOperator(
            task_id="copy",
            src=src_uri,
            dst=dst_uri,
            source_conn_id=pair.src.conn_id,
            dest_conn_id=pair.dst.conn_id,
            **kwargs,
        )
    return operator.execute({})


def _listed_files(root: Root) -> list[str]:
    path = resolve_storage_path(root.uri, root.conn_id)
    prefix = _fs_prefix(path.fs, path.path)
    found: list[str] = []
    for row in list_entries(path):
        if row["is_marker"]:
            continue
        rel = relative_key(str(row["name"]), prefix)
        if rel:
            found.append(rel)
    return sorted(found)
