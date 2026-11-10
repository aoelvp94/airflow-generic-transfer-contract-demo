"""Prototype file-transfer contract used by the GCSToGCS shim.

Protocols are intentionally small. GCSToGCSOperator only needs list,
exists, optional mtime filtering, optional server-side copy, and optional
delete (move). Reader/writer are part of the contract so a generic path
can fall back to stream-copy when server_side_copy is unavailable; this
experiment does not exercise that fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ObjectRef:
    bucket: str
    key: str

    def uri(self) -> str:
        return f"gs://{self.bucket}/{self.key}"


@dataclass(frozen=True, slots=True)
class ListQuery:
    """Listing request, including GCS prefix/delimiter/glob marker policy.

    When omit_match_glob is True, the backend must not pass match_glob at
    all. Upstream wildcard listing calls hook.list(bucket, prefix, delimiter)
    without that keyword; tests assert the exact call shape.
    """

    bucket: str
    prefix: str = ""
    delimiter: str | None = None
    match_glob: str | None = None
    omit_match_glob: bool = False


@dataclass(frozen=True, slots=True)
class TransferItem:
    source: ObjectRef
    destination: ObjectRef


@runtime_checkable
class ObjectLister(Protocol):
    def list_objects(self, query: ListQuery) -> list[str]:
        """Return object keys under *query*, honouring the backend marker policy."""


@runtime_checkable
class ObjectExists(Protocol):
    def exists(self, ref: ObjectRef) -> bool: ...


@runtime_checkable
class ObjectReader(Protocol):
    def open_reader(self, ref: ObjectRef) -> Any: ...


@runtime_checkable
class ObjectWriter(Protocol):
    def open_writer(self, ref: ObjectRef) -> Any: ...


@runtime_checkable
class ServerSideCopy(Protocol):
    def server_side_copy(self, source: ObjectRef, destination: ObjectRef, **extra: Any) -> None: ...


@runtime_checkable
class ObjectDeleter(Protocol):
    def delete(self, ref: ObjectRef) -> None: ...


@runtime_checkable
class MtimeFilter(Protocol):
    def accept(self, ref: ObjectRef) -> bool:
        """Return True if *ref* should be copied under the configured mtime window."""
