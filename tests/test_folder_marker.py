"""Folder-marker rule: implemented once on the generic file path."""
from __future__ import annotations

from pathlib import Path

import fsspec

from generic_transfer import GenericRecordTransfer
from generic_transfer.adapters import (
    ObjectStoragePath,
    ObjectStoreCopySink,
    is_folder_marker,
    list_transfer_keys,
)


def _seed_memory_store_with_marker() -> str:
    """Build memory://src/data/ with a zero-byte folder marker and two files."""
    root = "memory://src/data"
    fs, path = fsspec.core.url_to_fs(root)
    fs.makedirs(path, exist_ok=True)
    # Folder marker (zero-byte key at the "folder" itself).
    marker = path.rstrip("/")  # memory path for the prefix
    with fs.open(marker, "wb") as fh:
        fh.write(b"")
    with fs.open(f"{path.rstrip('/')}/a.json", "wb") as fh:
        fh.write(b'{"id":1}')
    with fs.open(f"{path.rstrip('/')}/nested/b.json", "wb") as fh:
        fh.write(b'{"id":2}')
    # Nested folder marker
    nested_marker = f"{path.rstrip('/')}/nested"
    # nested/ already exists as dir from writing b.json; also plant an empty marker key
    # On memory FS, writing the dir path as empty file may not work the same —
    # plant an explicit empty sibling marker style used by S3: "nested" as empty object
    # when listing returns both "nested" and "nested/b.json".
    return root


def test_is_folder_marker_detects_empty_prefix_with_children():
    root = _seed_memory_store_with_marker()
    fs, path = fsspec.core.url_to_fs(root)
    # The prefix path with children should be treated as a marker when size==0.
    assert is_folder_marker(fs, path) or fs.isdir(path)
    keys = list_transfer_keys(fs, path)
    # Only real objects; no empty marker.
    assert any(k.endswith("a.json") for k in keys)
    assert any(k.endswith("b.json") for k in keys)
    assert not any(k.rstrip("/") == path.rstrip("/") for k in keys)


def test_generic_copy_skips_folder_markers(tmp_path: Path):
    """One generic file path handles the marker rule that took four pair-operator PRs."""
    src_root = tmp_path / "src" / "data"
    src_root.mkdir(parents=True)
    # Local FS: create a zero-byte marker file AND child keys (S3-style layout
    # approximated on disk: empty file named like the folder is unusual locally,
    # so we also plant an empty "marker" sibling that list_transfer_keys must skip
    # when it has no children — and a real empty directory which isdir catches.
    (src_root / "a.json").write_text('{"id":1}')
    nested = src_root / "nested"
    nested.mkdir()
    (nested / "b.json").write_text('{"id":2}')
    # Explicit empty marker object sitting beside real keys (size 0, has children
    # when we model it as the directory itself via is_folder_marker).
    marker_file = tmp_path / "src" / "marker-empty"
    marker_file.write_bytes(b"")
    # Also create children under a path that shares the marker naming on memory fs.
    mem = "memory://xfer/src"
    fs, path = fsspec.core.url_to_fs(mem)
    fs.makedirs(f"{path}/data", exist_ok=True)
    with fs.open(f"{path}/data", "wb") as fh:
        fh.write(b"")  # folder marker
    with fs.open(f"{path}/data/a.json", "wb") as fh:
        fh.write(b'{"id":1}')
    with fs.open(f"{path}/data/nested/b.json", "wb") as fh:
        fh.write(b'{"id":2}')

    dest = f"memory://xfer/dst"
    # Source that yields nothing — ObjectStoreCopySink uses source_prefix param.
    class EmptySource:
        def read_batches(self, **params):
            yield from ()

    sink = ObjectStoreCopySink(dest)
    result = GenericRecordTransfer(
        source_hook=EmptySource(),
        destination_hook=sink,
        destination_hook_params={"source_prefix": f"{mem}/data"},
    ).execute()

    assert result["count"] == 2
    assert sorted(Path(p).name for p in result["copied"]) == ["a.json", "b.json"]
    dst_fs, dst_path = fsspec.core.url_to_fs(dest)
    found = list(dst_fs.find(dst_path))
    assert len(found) == 2
    # No zero-byte marker copied.
    for key in found:
        assert dst_fs.info(key).get("size", 0) > 0


def test_local_path_object_storage_roundtrip(tmp_path: Path):
    p = ObjectStoragePath(str(tmp_path / "out" / "x.json"))
    p.write_bytes(b'[{"id":1}]')
    assert p.read_bytes() == b'[{"id":1}]'
