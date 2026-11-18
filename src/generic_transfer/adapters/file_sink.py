"""File / object-store sink via an ObjectStoragePath-like fsspec facade.

Implements BatchWritable. Also exposes list/copy helpers that skip object-store
"folder markers" (zero-byte keys that have keys beneath them) once, so every
pair that lands on a registered filesystem shares the rule.
"""
from __future__ import annotations

import io
import json
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import fsspec


class DictObjectStore:
    """In-memory S3-like flat keyspace (keys are strings, values are bytes).

    Unlike fsspec's memory FS, this allows a zero-byte key ``data/`` to coexist
    with ``data/a.json`` — the layout object stores use for folder markers.
    """

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        self._objects[key] = data

    def get(self, key: str) -> bytes:
        return self._objects[key]

    def exists(self, key: str) -> bool:
        return key in self._objects

    def info(self, key: str) -> dict[str, Any]:
        if key not in self._objects:
            raise FileNotFoundError(key)
        data = self._objects[key]
        return {"name": key, "size": len(data), "type": "file"}

    def find(self, prefix: str = "") -> list[str]:
        if not prefix:
            return sorted(self._objects)
        return sorted(k for k in self._objects if k == prefix or k.startswith(prefix))

    def ls(self, path: str, detail: bool = False) -> list:
        path = path if path.endswith("/") else path + "/"
        # immediate children under path
        names: set[str] = set()
        for key in self._objects:
            if not key.startswith(path) and key != path.rstrip("/"):
                continue
            rest = key[len(path) :] if key.startswith(path) else ""
            if rest == "" and key in self._objects:
                continue
            child = rest.split("/", 1)[0]
            if child:
                names.add(path + child)
        result = sorted(names)
        if detail:
            return [{"name": n, "size": len(self._objects.get(n, b"")), "type": "file"} for n in result]
        return result

    def open(self, key: str, mode: str = "rb"):
        if "r" in mode:
            return io.BytesIO(self._objects[key])
        buf = io.BytesIO()
        store = self

        class _Writer(io.BytesIO):
            def close(self_inner):  # noqa: N805
                store._objects[key] = self_inner.getvalue()
                super().close()

            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                self_inner.close()
                return False

        return _Writer()

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        # Flat keyspace: no directories to create.
        return None


def is_folder_marker(fs: Any, key: str, *, prefix: str | None = None) -> bool:
    """Return True when ``key`` is a zero-byte object with keys under it.

    Object stores often create an empty marker for every "folder". A transfer
    that lists and copies key-by-key must skip those markers; otherwise it
    either fails or writes empty objects. This helper is the single place that
    rule lives in this demo.
    """
    del prefix  # reserved for scoped listings
    try:
        info = fs.info(key)
    except FileNotFoundError:
        # Local/fsspec dirs may not appear as keys; treat isdir as marker-like.
        isdir = getattr(fs, "isdir", None)
        if callable(isdir) and isdir(key):
            return True
        return False
    size = info.get("size", 0) or 0
    if size != 0:
        return False
    if info.get("type") == "directory":
        return True
    child_prefix = key if key.endswith("/") else key + "/"
    children: list[str] = []
    try:
        found = fs.find(child_prefix)
        children = [c for c in found if c != key]
    except Exception:
        try:
            children = [c for c in fs.ls(child_prefix, detail=False) if c != key]
        except Exception:
            return False
    return len(children) > 0


def list_transfer_keys(fs: Any, prefix: str) -> list[str]:
    """List object keys under ``prefix``, skipping folder markers."""
    try:
        found = list(fs.find(prefix))
    except FileNotFoundError:
        return []
    keys: list[str] = []
    for key in found:
        if is_folder_marker(fs, key):
            continue
        keys.append(key)
    return sorted(keys)


class ObjectStoragePath:
    """Tiny stand-in for ``airflow.io.path.ObjectStoragePath``.

    Resolves ``file://``, bare local paths, and any fsspec URL (``memory://``,
    ``s3://``, ``gs://``, ``abfs://``, …). Pass ``storage_options`` (or a
    pre-built ``fs``) when pointing at emulators that need endpoint/creds.
    """

    def __init__(
        self,
        url: str,
        *,
        storage_options: dict[str, Any] | None = None,
        fs: Any | None = None,
    ):
        self.url = url
        self._storage_options = dict(storage_options or {})
        if fs is not None:
            parsed = urlparse(url)
            # path inside the fs: strip scheme://authority/
            if parsed.scheme in ("", "file"):
                self._path = parsed.path
            else:
                # s3://bucket/key -> bucket/key ; abfs://container/key -> container/key
                self._path = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
                if parsed.path in ("", "/"):
                    self._path = parsed.netloc
            self._fs = fs
            return
        parsed = urlparse(url)
        opts = self._storage_options or None
        if parsed.scheme in ("", "file"):
            self._fs, self._path = fsspec.core.url_to_fs(
                url if parsed.scheme else f"file://{url}",
                **(opts or {}),
            )
        else:
            self._fs, self._path = fsspec.core.url_to_fs(url, **(opts or {}))

    @property
    def fs(self) -> Any:
        return self._fs

    @property
    def path(self) -> str:
        return self._path

    def joinpath(self, *parts: str) -> "ObjectStoragePath":
        base = self.url.rstrip("/")
        joined = "/".join(p.strip("/") for p in parts if p)
        return ObjectStoragePath(
            f"{base}/{joined}",
            storage_options=self._storage_options,
            fs=self._fs,
        )

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
    """Copy keys from a source store prefix into a destination DictObjectStore
    or ObjectStoragePath root.

    Skips folder markers via :func:`list_transfer_keys`. This is the file-to-file
    half of the thesis: one rule, every registered filesystem.
    """

    def __init__(self, destination: Any):
        """``destination`` is a DictObjectStore or an ObjectStoragePath/url str."""
        if isinstance(destination, str):
            self.destination: Any = ObjectStoragePath(destination)
            self._mode = "path"
        else:
            self.destination = destination
            self._mode = "store"

    def write_batches(
        self,
        batches: Iterator[list[dict[str, Any]]],
        *,
        source_fs: Any | None = None,
        source_prefix: str = "",
        **_: Any,
    ) -> dict[str, Any]:
        for _ in batches:
            pass
        if source_fs is None:
            raise ValueError("source_fs is required for object-store copy")
        keys = list_transfer_keys(source_fs, source_prefix)
        copied: list[str] = []
        for key in keys:
            rel = key[len(source_prefix) :].lstrip("/") if source_prefix and key.startswith(source_prefix) else key
            with source_fs.open(key, "rb") as rf:
                data = rf.read()
            if self._mode == "store":
                self.destination.put(rel, data)
            else:
                self.destination.joinpath(rel).write_bytes(data)
            copied.append(rel)
        return {"copied": copied, "count": len(copied)}
