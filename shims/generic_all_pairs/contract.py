"""Shared file-transfer contract for the all-pairs generic operator.

This is the experiment contract: listing with an explicit marker policy,
streaming read/write, optional server-side copy, optional delete (move),
optional mtime filter, overwrite, and path layout. It is not a port of
any pair-operator listing helper.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ObjectRef:
    bucket: str
    key: str
    flavour: str = ""


@dataclass(frozen=True, slots=True)
class ListedObject:
    key: str
    size: int | None = None
    last_modified: datetime | None = None


@dataclass(frozen=True, slots=True)
class ListQuery:
    bucket: str
    prefix: str = ""
    match_glob: str | None = None


@dataclass(frozen=True, slots=True)
class TransferItem:
    source: ObjectRef
    destination: ObjectRef


@runtime_checkable
class ObjectLister(Protocol):
    def list_objects(self, query: ListQuery) -> list[ListedObject]: ...


@runtime_checkable
class ObjectExists(Protocol):
    def exists(self, ref: ObjectRef) -> bool: ...


@runtime_checkable
class ObjectReader(Protocol):
    def read_bytes(self, ref: ObjectRef) -> bytes: ...


@runtime_checkable
class ObjectWriter(Protocol):
    def write_bytes(self, ref: ObjectRef, data: bytes, *, overwrite: bool = True) -> None: ...


@runtime_checkable
class ServerSideCopy(Protocol):
    def server_side_copy(self, source: ObjectRef, destination: ObjectRef) -> None: ...


@runtime_checkable
class ObjectDeleter(Protocol):
    def delete(self, ref: ObjectRef) -> None: ...
