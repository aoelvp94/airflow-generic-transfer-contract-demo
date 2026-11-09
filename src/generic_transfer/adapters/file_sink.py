"""File / object-store sink via an ObjectStoragePath-like fsspec facade.

Implements BatchWritable. Also exposes list/copy helpers that skip object-store
"folder markers" (zero-byte keys that have keys beneath them) once, so every
pair that lands on a registered filesystem shares the rule.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import fsspec


def is_folder_marker(fs: Any, key: str, *, prefix: str | None = None) -> bool:
    """Return True when ``key`` is a zero-byte object with keys under it.

    Object stores often create an empty marker for every "folder". A transfer
    that lists and copies key-by-key must skip those markers; otherwise it
    either fails or writes empty objects. This helper is the single place that
    rule lives in this demo.
    """
    try:
        info = fs.info(key)
    except FileNotFoundError:
        return False
    size = info.get("size", 0) or 0
    if size != 0:
        return False
    # Directory-typed entries from local/memory filesystems.
    if info.get("type") == "directory":
        return True
    child_prefix = key if key.endswith("/") else key + "/"
    if prefix is not None and not child_prefix.startswith(prefix.rstrip("/") + "/") and child_prefix != prefix:
        # still fine — we just look under the key itself
        pass
    try:
        children = fs.ls(child_prefix, detail=False)
    except (FileNotFoundError, NotImplementedError, OSError):
        # Some filesystems reject ls on a file key; probe with find/glob.
        try:
            children = [p for p in fs.find(child_prefix) if p != key]
        except Exception:
            return False
    return any(c != key for c in children)


def list_transfer_keys(fs: Any, prefix: str) -> list[str]:
    """List object keys under ``prefix``, skipping folder markers."""
    prefix = prefix.rstrip("/")
    try:
        found = fs.find(prefix)
    except FileNotFoundError:
        return []
    keys: list[str] = []
    for key in found:
        if key.rstrip("/") == prefix.rstrip("/"):
            # the prefix itself when it is a marker
            if is_folder_marker(fs, key):
                continue
        if is_folder_marker(fs, key, prefix=prefix):
            continue
        keys.append(key)
    return sorted(keys)


class ObjectStoragePath:
    """Tiny stand-in for ``airflow.io.path.ObjectStoragePath``.

    Resolves ``file://``, bare local paths, and any fsspec URL (``memory://``,
    ``s3://``, …). Enough to show that file destinations go through one path.
    """

    def __init__(self, url: str):
        self.url = url
        parsed = urlparse(url)
        if parsed.scheme in ("", "file"):
            self._fs, self._path = fsspec.core.url_to_fs(url if parsed.scheme else f"file://{url}")
        else:
            self._fs, self._path = fsspec.core.url_to_fs(url)

    @property
    def fs(self) -> Any:
        return self._fs

    @property
    def path(self) -> str:
        return self._path

    def joinpath(self, *parts: str) -> "ObjectStoragePath":
        base = self.url.rstrip("/")
        joined = "/".join(p.strip("/") for p in parts if p)
        return ObjectStoragePath(f"{base}/{joined}")

    def write_bytes(self, data: bytes) -> None:
        parent = self._path.rsplit("/", 1)[0] if "/" in self._path else ""
        if parent:
            self._fs.makedirs(parent, exist_ok=True)
        with self._fs.open(self._path, "wb") as fh:
            fh.write(data)

    def read_bytes(self) -> bytes:
        with self._fs.open(self._path, "rb") as fh:
            return fh.read()

    def mkdir(self, exist_ok: bool = True) -> None:
        self._fs.makedirs(self._path, exist_ok=exist_ok)

    def touch_marker(self) -> None:
        """Create a zero-byte folder marker at this path (object-store style)."""
        parent = self._path.rsplit("/", 1)[0] if "/" in self._path else ""
        if parent:
            self._fs.makedirs(parent, exist_ok=True)
        with self._fs.open(self._path, "wb") as fh:
            fh.write(b"")


class FileBatchSink:
    """Write record batches as one JSON (or NDJSON) object under a store root."""

    def __init__(self, root: str):
        self.root = ObjectStoragePath(root)

    def write_batches(
        self,
        batches: Iterator[list[dict[str, Any]]],
        *,
        key: str = "raw/out.json",
        content_type: str = "json_array",
        **_: Any,
    ) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        batch_count = 0
        for batch in batches:
            batch_count += 1
            records.extend(batch)
        if content_type == "ndjson":
            data = "".join(json.dumps(r) + "\n" for r in records).encode()
        else:
            data = json.dumps(records).encode()
        target = self.root.joinpath(key)
        target.write_bytes(data)
        return {
            "key": key,
            "url": target.url,
            "records": len(records),
            "batches": batch_count,
            "bytes": len(data),
        }


class ObjectStoreCopySink:
    """Copy keys from a source ObjectStoragePath prefix into a destination.

    Skips folder markers via :func:`list_transfer_keys`. This is the file-to-file
    half of the thesis: one rule, every registered filesystem.
    """

    def __init__(self, destination_root: str):
        self.destination = ObjectStoragePath(destination_root)

    def write_batches(
        self,
        batches: Iterator[list[dict[str, Any]]],
        *,
        source_prefix: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        # File-to-file mode ignores record batches; source is an object prefix.
        # Accept an empty iterator so the generic operator still type-checks.
        for _ in batches:
            pass
        if not source_prefix:
            raise ValueError("source_prefix is required for object-store copy")
        source = ObjectStoragePath(source_prefix)
        keys = list_transfer_keys(source.fs, source.path)
        copied: list[str] = []
        for key in keys:
            rel = key[len(source.path) :].lstrip("/") if key.startswith(source.path) else key
            dest = self.destination.joinpath(rel)
            with source.fs.open(key, "rb") as rf:
                data = rf.read()
            dest.write_bytes(data)
            copied.append(rel)
        return {"copied": copied, "count": len(copied)}
