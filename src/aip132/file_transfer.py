"""Prefix copy for AIP-132 phase 1, on Airflow 3 ``ObjectStoragePath``.

``FileTransferOperator`` copies one object. ``PrefixFileTransferOperator``
keeps that path for a single file and, for a prefix, lists once, drops folder
markers, and returns the destination keys that were copied.

The functions here do the work. An Airflow ``@task`` can call ``copy_files``
directly; the operator is a thin facilitator around the same functions.

.. code-block:: python

    from airflow.sdk import task
    from aip132.file_transfer import copy_files

    @task
    def copy_sales(src: str, dst: str) -> list[str] | None:
        return copy_files(src, dst, source_conn_id="aws", dest_conn_id="gcp")
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from airflow.providers.common.io.operators.file_transfer import FileTransferOperator
from airflow.sdk import ObjectStoragePath

#: Cross-store streaming reads and writes this many bytes at a time.
DEFAULT_CHUNK_SIZE = 1024 * 1024

_MARKER_SKIP = "skip"
_MARKER_COPY = "copy"
_MARKER_POLICIES = (_MARKER_SKIP, _MARKER_COPY)
_FILE_PROTOCOLS = {"file", "local", "memory"}
_DIRECTORY_TYPES = {"directory", "dir"}


@dataclass(frozen=True)
class PlannedCopy:
    """One object selected for copy."""

    source: ObjectStoragePath
    destination: ObjectStoragePath
    is_marker: bool


def _attach_memory(conn_id: str | None) -> None:
    """Register fsspec's ``memory`` filesystem for ``ObjectStoragePath``.

    Airflow registers ``file`` itself. ``memory://`` is a test backend and is
    attached on first use. ``attach`` keeps the first registration, so this
    must run before ``.fs`` is touched for that connection id.
    """
    import fsspec
    from airflow.sdk.io import attach

    attach(protocol="memory", conn_id=conn_id, fs=fsspec.filesystem("memory"))


def resolve_storage_path(path: str | ObjectStoragePath, conn_id: str | None) -> ObjectStoragePath:
    """Return an ``ObjectStoragePath``, attaching ``memory://`` when needed."""
    if isinstance(path, str):
        scheme = path.split("://", 1)[0] if "://" in path else "file"
        if scheme == "memory":
            _attach_memory(conn_id)
        return ObjectStoragePath(path, conn_id=conn_id)
    if getattr(path, "protocol", None) == "memory":
        _attach_memory(path.conn_id)
    return path


def same_store(src: ObjectStoragePath, dst: ObjectStoragePath) -> bool:
    """True when ``ObjectStoragePath.copy`` stays inside one filesystem.

    Same protocol and same connection id. That is the path ``copy`` implements
    with ``fs.copy`` (server-side for object stores that support it).
    """
    return bool(src.samestore(dst))


def _fs_prefix(fs: Any, path: str) -> str:
    raw = path
    strip = getattr(fs, "_strip_protocol", None)
    if callable(strip):
        try:
            raw = strip(path)
        except Exception:
            raw = path
    return str(raw).rstrip("/")


def _find_once(fs: Any, prefix: str) -> Any:
    """One ``find``. ``withdirs=True`` includes directory entries when supported."""
    try:
        return fs.find(prefix, detail=True, withdirs=True)
    except TypeError:
        return fs.find(prefix, detail=True)


def _coerce_find(found: Any) -> list[dict[str, Any]]:
    if isinstance(found, dict):
        rows: list[dict[str, Any]] = []
        for name, info in found.items():
            info = info or {}
            rows.append(
                {
                    "name": str(info.get("name") or name),
                    "size": info.get("size"),
                    "type": info.get("type") or "file",
                }
            )
        return rows
    return [{"name": str(name), "size": None, "type": "file"} for name in found]


def _size(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def classify_folder_markers(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Mark folder markers using only the rows from one listing.

    A row is a marker when it is a directory, its name ends with ``/``, or it
    is empty (or of unknown size) and another listed name sits under it.
    A zero-byte object with no children is a real empty file and is kept.
    This function does not call ``info`` or any other per-key stat.
    """
    rows = sorted(entries, key=lambda entry: str(entry.get("name", "")))
    names = [str(entry.get("name", "")) for entry in rows]
    out: list[dict[str, Any]] = []
    for idx, entry in enumerate(rows):
        name = names[idx]
        size = _size(entry.get("size"))
        typ = str(entry.get("type") or "file")
        is_marker = typ in _DIRECTORY_TYPES or name.endswith("/")
        if not is_marker and (size is None or size == 0):
            child_prefix = name.rstrip("/") + "/"
            for other in names[idx + 1 :]:
                if not other.startswith(name.rstrip("/")):
                    break
                if other.startswith(child_prefix) and other.rstrip("/") != name.rstrip("/"):
                    is_marker = True
                    break
        out.append({"name": name, "size": size, "type": typ, "is_marker": is_marker})
    return out


def _is_under(name: str, prefix: str) -> bool:
    name_l = name.lstrip("/").rstrip("/")
    pref_l = prefix.lstrip("/").rstrip("/")
    if not pref_l:
        return True
    return name_l == pref_l or name.lstrip("/").startswith(pref_l + "/")


def relative_key(name: str, prefix: str) -> str:
    """Path of ``name`` relative to ``prefix``. Empty when ``name`` is the prefix."""
    name_l = name.lstrip("/")
    pref_l = prefix.lstrip("/").rstrip("/")
    if name_l.rstrip("/") == pref_l:
        return ""
    token = pref_l + "/"
    if pref_l and name_l.startswith(token):
        return name_l[len(token) :]
    return name_l


def list_prefix_once(src: ObjectStoragePath) -> list[dict[str, Any]]:
    """List ``src`` with one ``find`` and classify folder markers.

    Missing prefixes return an empty list. ``info`` is not called.
    """
    fs = src.fs
    prefix = _fs_prefix(fs, src.path)
    try:
        found = _find_once(fs, prefix)
    except FileNotFoundError:
        return []
    rows = [row for row in _coerce_find(found) if _is_under(str(row["name"]), prefix)]
    return classify_folder_markers(rows)


def _basename(key: str) -> str:
    key = key.rstrip("/")
    if "/" not in key:
        return key
    return key.rsplit("/", 1)[-1]


def _glob_match(relative: str, match_glob: str | None) -> bool:
    if not match_glob:
        return True
    leaf = _basename(relative)
    return fnmatch.fnmatch(relative, match_glob) or fnmatch.fnmatch(leaf, match_glob)


def child_path(root: ObjectStoragePath, relative_key_text: str) -> ObjectStoragePath:
    """Join ``relative_key_text`` onto ``root``, keeping protocol and connection."""
    keep_slash = relative_key_text.endswith("/")
    joined: Any = root
    for part in relative_key_text.split("/"):
        if part:
            joined = joined / part
    if not isinstance(joined, ObjectStoragePath):
        joined = ObjectStoragePath(str(joined), conn_id=root.conn_id)
    if keep_slash and not str(joined).endswith("/"):
        joined = ObjectStoragePath(str(joined) + "/", conn_id=joined.conn_id or root.conn_id)
    return joined


def _forms(value: str) -> set[str]:
    if value is None or value == "":
        return set()
    trimmed = value.rstrip("/")
    bare = trimmed.lstrip("/")
    return {value, trimmed, bare, "/" + bare}


def list_destination_names(dst: ObjectStoragePath) -> set[str]:
    """Names already under ``dst``, from one listing. ``exists`` is not called."""
    fs = dst.fs
    prefix = _fs_prefix(fs, dst.path)
    try:
        found = _find_once(fs, prefix)
    except FileNotFoundError:
        return set()
    if isinstance(found, dict):
        names: Iterable[Any] = found.keys()
    else:
        names = found
    forms: set[str] = set()
    for name in names:
        forms |= _forms(str(name))
    return forms


def _known(path: ObjectStoragePath, existing: set[str]) -> bool:
    return bool((_forms(path.path) | _forms(path.key)) & existing)


def _layout_relative(relative: str, *, keep_structure: bool, marker: bool) -> str:
    if keep_structure:
        dest_rel = relative
    else:
        dest_rel = _basename(relative)
    if marker and dest_rel and not dest_rel.endswith("/"):
        dest_rel += "/"
    return dest_rel


def plan_prefix_copy(
    src: ObjectStoragePath,
    dst: ObjectStoragePath,
    *,
    match_glob: str | None = None,
    keep_structure: bool = True,
    folder_markers: str = _MARKER_SKIP,
) -> list[PlannedCopy]:
    """Select objects under ``src``. Does not copy and does not stat each key."""
    if folder_markers not in _MARKER_POLICIES:
        raise ValueError(f"folder_markers must be 'skip' or 'copy', got {folder_markers!r}")
    prefix = _fs_prefix(src.fs, src.path)
    protocol = src.protocol or "file"
    planned: list[PlannedCopy] = []
    for entry in list_prefix_once(src):
        rel = relative_key(str(entry["name"]), prefix)
        if not rel:
            continue
        if not _glob_match(rel, match_glob):
            continue
        is_marker = bool(entry["is_marker"])
        if is_marker and folder_markers == _MARKER_SKIP:
            continue
        if (
            is_marker
            and folder_markers == _MARKER_COPY
            and entry["type"] in _DIRECTORY_TYPES
            and protocol in _FILE_PROTOCOLS
        ):
            # file:// and memory:// directories are not objects. Object-store
            # markers typed as directories are still copied (empty key).
            continue
        dest_rel = _layout_relative(rel, keep_structure=keep_structure, marker=is_marker)
        if not dest_rel:
            continue
        planned.append(
            PlannedCopy(
                source=child_path(src, rel),
                destination=child_path(dst, dest_rel),
                is_marker=is_marker,
            )
        )
    planned.sort(key=lambda item: item.source.key)
    return planned


def _prepare_file_parent(dst: ObjectStoragePath) -> None:
    if (dst.protocol or "file") not in {"file", "local"}:
        return
    parent = dst.parent
    mkdir = getattr(parent, "mkdir", None)
    if callable(mkdir):
        mkdir(parents=True, exist_ok=True)


def _copy_stream(reader: Any, writer: Any, chunk_size: int) -> None:
    """Copy until EOF. Peak held here is one chunk."""
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")
    while True:
        chunk = reader.read(chunk_size)
        if not chunk:
            break
        writer.write(chunk)


def stream_copy(
    src: ObjectStoragePath,
    dst: ObjectStoragePath,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """Stream ``src`` to ``dst`` in fixed-size chunks."""
    _prepare_file_parent(dst)
    with src.open("rb") as reader, dst.open("wb") as writer:
        _copy_stream(reader, writer, chunk_size)


def copy_object(
    src: ObjectStoragePath,
    dst: ObjectStoragePath,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    marker: bool = False,
) -> None:
    """Copy one object.

    Same-store copies go through ``ObjectStoragePath.copy`` (``fs.copy``).
    Cross-store copies stream. A folder marker is written as an empty object;
    there is no body to stream.
    """
    if marker:
        _prepare_file_parent(dst)
        with dst.open("wb") as writer:
            writer.write(b"")
        return
    if same_store(src, dst):
        _prepare_file_parent(dst)
        src.copy(dst)
        return
    stream_copy(src, dst, chunk_size=chunk_size)


def _is_prefix(src: ObjectStoragePath) -> bool:
    """A trailing slash is a prefix. Otherwise one directory stat decides.

    That stat is the source root only, not one stat per key. An object-store
    prefix that is neither a directory nor written with a trailing slash is
    treated as a single key, which is the upstream single-file path.
    """
    if src.key.endswith("/") or str(src).endswith("/"):
        return True
    try:
        return bool(src.is_dir())
    except (FileNotFoundError, OSError):
        return False


def copy_single_file(src: ObjectStoragePath, dst: ObjectStoragePath, *, overwrite: bool) -> None:
    """Copy one object the way ``FileTransferOperator.execute`` does.

    ``overwrite=False`` raises when the destination file already exists.
    The copy itself is ``src.copy(dst)``. The caller returns ``None``.
    """
    if not overwrite:
        if dst.exists() and dst.is_file():
            raise ValueError(f"Destination {dst} already exists")
    src.copy(dst)


def copy_files(
    src: str | ObjectStoragePath,
    dst: str | ObjectStoragePath,
    *,
    source_conn_id: str | None = None,
    dest_conn_id: str | None = None,
    match_glob: str | None = None,
    overwrite: bool = False,
    keep_structure: bool = True,
    folder_markers: str = _MARKER_SKIP,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> list[str] | None:
    """Copy ``src`` to ``dst``.

    A single file follows :func:`copy_single_file` and returns ``None``.
    A prefix returns the destination keys that were copied, sorted. Keys
    already present are left unchanged when ``overwrite`` is false, and those
    keys are omitted from the returned list. ``keep_structure`` false flattens
    each object to its basename under ``dst``.
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")
    if folder_markers not in _MARKER_POLICIES:
        raise ValueError(f"folder_markers must be 'skip' or 'copy', got {folder_markers!r}")
    src_path = resolve_storage_path(src, source_conn_id)
    dst_path = resolve_storage_path(dst, dest_conn_id)
    if not _is_prefix(src_path):
        copy_single_file(src_path, dst_path, overwrite=overwrite)
        return None
    planned = plan_prefix_copy(
        src_path,
        dst_path,
        match_glob=match_glob,
        keep_structure=keep_structure,
        folder_markers=folder_markers,
    )
    existing = None if overwrite else list_destination_names(dst_path)
    copied: list[str] = []
    for item in planned:
        if existing is not None and _known(item.destination, existing):
            continue
        copy_object(
            item.source,
            item.destination,
            chunk_size=chunk_size,
            marker=item.is_marker,
        )
        copied.append(item.destination.key)
        if existing is not None:
            existing |= _forms(item.destination.path)
            existing |= _forms(item.destination.key)
    return sorted(set(copied))


class PrefixFileTransferOperator(FileTransferOperator):
    """Copy one object or a prefix between stores that ``ObjectStoragePath`` can open.

    A single-file ``src`` keeps ``FileTransferOperator`` behaviour: ``overwrite``
    false raises when the destination file exists, the bytes move through
    ``ObjectStoragePath.copy``, and ``execute`` returns ``None``.

    A prefix (trailing slash, or a directory) is listed once. Folder markers
    are classified from that listing. ``folder_markers`` defaults to ``skip``.
    ``overwrite`` false skips destination keys that already exist instead of
    raising. ``keep_structure`` false flattens to basenames. ``match_glob``
    filters the relative key and its basename (Python ``fnmatch``; ``*``
    matches across ``/``).

    ``execute`` returns the copied destination keys (``ObjectStoragePath.key``).
    Airflow pushes that list as the task XCom. Same-store pairs use
    ``ObjectStoragePath.copy``. Cross-store pairs stream in ``chunk_size``
    chunks.

    :param src: Source file or prefix. A prefix should end with ``/``.
    :param dst: Destination file or prefix.
    :param source_conn_id: Connection id for ``src`` when ``src`` is a string.
    :param dest_conn_id: Connection id for ``dst`` when ``dst`` is a string.
    :param match_glob: Optional fnmatch pattern. ``None`` copies every object.
    :param overwrite: When false, skip existing prefix keys. Single files still
        raise, matching ``FileTransferOperator``.
    :param keep_structure: When true, keep the relative path under ``dst``.
        When false, place each object at ``dst / basename``.
    :param folder_markers: ``skip`` (default) or ``copy``. ``copy`` writes an
        empty object for a marker that is a real key. Directory entries on
        ``file://`` and ``memory://`` are not keys and are always skipped.
    :param chunk_size: Bytes per read on the cross-store stream path.
    """

    template_fields: Sequence[str] = (
        "src",
        "dst",
        "source_conn_id",
        "dest_conn_id",
        "match_glob",
    )

    def __init__(
        self,
        *,
        src: str | ObjectStoragePath,
        dst: str | ObjectStoragePath,
        source_conn_id: str | None = None,
        dest_conn_id: str | None = None,
        match_glob: str | None = None,
        overwrite: bool = False,
        keep_structure: bool = True,
        folder_markers: str = _MARKER_SKIP,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        **kwargs: Any,
    ) -> None:
        if folder_markers not in _MARKER_POLICIES:
            raise ValueError(f"folder_markers must be 'skip' or 'copy', got {folder_markers!r}")
        if chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        super().__init__(
            src=src,
            dst=dst,
            source_conn_id=source_conn_id,
            dest_conn_id=dest_conn_id,
            overwrite=overwrite,
            **kwargs,
        )
        # Parent stores the destination connection as ``dst_conn_id``.
        self.dest_conn_id = dest_conn_id
        self.match_glob = match_glob
        self.keep_structure = keep_structure
        self.folder_markers = folder_markers
        self.chunk_size = chunk_size

    def execute(self, context: Any) -> list[str] | None:
        return copy_files(
            self.src,
            self.dst,
            source_conn_id=self.source_conn_id,
            dest_conn_id=self.dest_conn_id,
            match_glob=self.match_glob,
            overwrite=self.overwrite,
            keep_structure=self.keep_structure,
            folder_markers=self.folder_markers,
            chunk_size=self.chunk_size,
        )
