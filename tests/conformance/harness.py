"""A storage root the conformance suite can write, read, and copy between.

A backend supplies objects with this surface. ``file`` and ``memory`` are
built here. Object-store emulators build their own roots in the integration
module and only need :class:`Root` and :class:`StorePair`.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from aip132.path_ops import _attach_memory, child_path, resolve_storage_path


class Root:
    """One prefix a conformance case can fill and read back."""

    def __init__(self, uri: str, conn_id: str | None, *, object_markers: bool) -> None:
        self.uri = uri
        self.conn_id = conn_id
        self.object_markers = object_markers

    def write(self, relative: str, data: bytes) -> None:
        raise NotImplementedError

    def read(self, relative: str) -> bytes:
        raise NotImplementedError

    def mkdir(self, relative: str) -> None:
        raise NotImplementedError

    def write_marker(self, relative: str) -> None:
        """A directory on local stores, a zero-byte marker key on object stores."""
        self.mkdir(relative.strip("/"))

    def refresh(self) -> None:
        return None

    def sibling(self) -> Root:
        """Another empty prefix on the same backend."""
        raise NotImplementedError


class StorePair:
    """Source and destination roots for one conformance case."""

    def __init__(self, src: Root, dst: Root) -> None:
        self.src = src
        self.dst = dst


def child_uri(root: Root, relative: str) -> str:
    """File URI under ``root``. No trailing slash, so the copy treats it as one object."""
    return root.uri.rstrip("/") + "/" + relative.strip("/")


def relatives(root: Root, keys: list[str] | None) -> list[str]:
    """Keys relative to ``root``, keeping a trailing slash the copy returned."""
    assert keys is not None
    root_key = resolve_storage_path(root.uri, root.conn_id).key.lstrip("/").rstrip("/")
    out: list[str] = []
    for key in keys:
        text = key.lstrip("/")
        if root_key and text.startswith(root_key + "/"):
            out.append(text[len(root_key) + 1 :])
        elif root_key and text.rstrip("/") == root_key:
            continue
        else:
            out.append(text)
    return sorted(out)


class LocalRoot(Root):
    """``file://`` or ``memory://`` prefix."""

    def __init__(self, kind: str, uri: str, tmp_path: Path) -> None:
        super().__init__(uri, None, object_markers=False)
        self.kind = kind
        self.tmp_path = tmp_path

    def write(self, relative: str, data: bytes) -> None:
        path = child_path(resolve_storage_path(self.uri, self.conn_id), relative)
        if (path.protocol or "file") in {"file", "local"}:
            Path(path.path).parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            handle.write(data)

    def read(self, relative: str) -> bytes:
        path = child_path(resolve_storage_path(self.uri, self.conn_id), relative)
        with path.open("rb") as handle:
            return handle.read()

    def mkdir(self, relative: str) -> None:
        path = child_path(resolve_storage_path(self.uri, self.conn_id), relative)
        path.mkdir(parents=True, exist_ok=True)

    def sibling(self) -> LocalRoot:
        return local_root(self.kind, self.tmp_path)


def local_root(kind: str, tmp_path: Path) -> LocalRoot:
    _attach_memory(None)
    token = uuid.uuid4().hex[:10]
    if kind == "file":
        path = tmp_path / token
        path.mkdir(parents=True)
        uri = path.resolve().as_uri()
        if not uri.endswith("/"):
            uri += "/"
        return LocalRoot(kind, uri, tmp_path)
    if kind == "memory":
        return LocalRoot(kind, f"memory:///{token}/", tmp_path)
    raise ValueError(f"unknown local store {kind!r}")


def local_pair(src_kind: str, dst_kind: str, tmp_path: Path) -> StorePair:
    return StorePair(local_root(src_kind, tmp_path), local_root(dst_kind, tmp_path))
