"""Folder-marker rule: implemented once on the generic file path."""
from __future__ import annotations

from pathlib import Path

from generic_transfer import GenericRecordTransfer
from generic_transfer.adapters import (
    DictObjectStore,
    ObjectStoragePath,
    ObjectStoreCopySink,
    is_folder_marker,
    list_transfer_keys,
)


def _seed_store_with_markers() -> DictObjectStore:
    """S3-like layout: zero-byte folder markers coexist with child keys."""
    store = DictObjectStore()
    store.put("data/", b"")  # folder marker
    store.put("data/a.json", b'{"id":1}')
    store.put("data/nested/", b"")  # nested folder marker
    store.put("data/nested/b.json", b'{"id":2}')
    return store


def test_is_folder_marker_detects_empty_prefix_with_children():
    store = _seed_store_with_markers()
    assert is_folder_marker(store, "data/") is True
    assert is_folder_marker(store, "data/nested/") is True
    assert is_folder_marker(store, "data/a.json") is False
    keys = list_transfer_keys(store, "data")
    assert keys == ["data/a.json", "data/nested/b.json"]


def test_generic_copy_skips_folder_markers():
    """One generic file path handles the marker rule that took four pair-operator PRs."""
    source = _seed_store_with_markers()
    dest = DictObjectStore()

    class EmptySource:
        def read_batches(self, **params):
            yield from ()

    sink = ObjectStoreCopySink(dest)
    result = GenericRecordTransfer(
        source_hook=EmptySource(),
        destination_hook=sink,
        destination_hook_params={"source_fs": source, "source_prefix": "data"},
    ).execute()

    assert result["count"] == 2
    assert sorted(result["copied"]) == ["a.json", "nested/b.json"]
    assert list(dest.find("")) == ["a.json", "nested/b.json"]
    assert dest.get("a.json") == b'{"id":1}'
    assert dest.get("nested/b.json") == b'{"id":2}'
    # Markers were not copied.
    assert not dest.exists("data/")
    assert not dest.exists("")
    assert not any(k.endswith("/") for k in dest.find(""))


def test_pair_style_naive_copy_would_include_markers():
    """Document why the four PRs existed: naive list+copy keeps markers."""
    source = _seed_store_with_markers()
    naive = [k for k in source.find("data")]
    assert "data/" in naive and "data/nested/" in naive
    # Generic path strips them.
    assert "data/" not in list_transfer_keys(source, "data")


def test_local_path_object_storage_roundtrip(tmp_path: Path):
    p = ObjectStoragePath(str(tmp_path / "out" / "x.json"))
    p.write_bytes(b'[{"id":1}]')
    assert p.read_bytes() == b'[{"id":1}]'
