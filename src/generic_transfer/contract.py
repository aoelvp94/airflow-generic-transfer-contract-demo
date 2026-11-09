"""Hook capability contracts for record and file transfer.

Hooks implement these methods structurally. They do not need to import this
module: ``isinstance(hook, BatchReadable)`` works via ``@runtime_checkable``
as long as the method names and call shapes match (PEP 544).

Batch payloads are intentionally untyped at the Protocol boundary (``Any``):
row dicts, ``pyarrow.RecordBatch``, or other batch shapes are all valid. Hooks
that care about negotiation advertise ``batch_format`` / ``describe_params``.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BatchReadable(Protocol):
    """Source side: yield batches of records. Pagination lives in the hook."""

    def read_batches(self, **params: Any) -> Iterator[Any]:
        """Yield successive batches.

        The hook owns pagination (offset, cursor, next-link, …). The operator
        only iterates the batches and never sees page tokens. Batch element
        type is hook-defined (list[dict], RecordBatch, …).
        """
        ...


@runtime_checkable
class BatchWritable(Protocol):
    """Destination side: consume batches of records."""

    def write_batches(self, batches: Iterator[Any], **params: Any) -> Any:
        """Consume ``batches`` and return a destination-specific result.

        Implementations may stream, buffer, or re-chunk. The operator does not
        inspect the result shape beyond returning it to the caller.
        """
        ...


@runtime_checkable
class FileCopyable(Protocol):
    """File / object-store side: copy keys without going through record batches.

    Separates file-to-file transfers from ``write_batches`` so record sinks are
    not overloaded with ``source_fs`` / ``source_prefix`` params.
    """

    def copy_files(self, **params: Any) -> Any:
        """Copy objects from a source prefix into this destination.

        Typical params: ``source_fs``, ``source_prefix``, ``dest_prefix``,
        ``workers``, ``chunk_size``, ``resume_from``.
        """
        ...


@runtime_checkable
class LineageProvider(Protocol):
    """Optional: hooks declare datasets touched by a transfer."""

    def get_lineage_datasets(self, **params: Any) -> list[Any]:
        """Return lineage facets / dataset refs for the given params."""
        ...


@runtime_checkable
class ParamsDescribable(Protocol):
    """Optional: hooks advertise the kwargs their transfer methods accept."""

    def describe_params(self, role: str = "source") -> dict[str, Any]:
        """Return a JSON-schema-like dict for forwarded hook params.

        ``role`` is ``\"source\"``, ``\"destination\"``, or ``\"copy\"``.
        """
        ...


@runtime_checkable
class AsyncBatchReadable(Protocol):
    """Optional async form for deferrable / trigger-friendly sources."""

    def aread_batches(self, **params: Any) -> AsyncIterator[Any]:
        """Async iterator of batches (same payload rules as ``read_batches``)."""
        ...


@runtime_checkable
class AsyncBatchWritable(Protocol):
    """Optional async form for deferrable destinations."""

    async def awrite_batches(self, batches: AsyncIterator[Any], **params: Any) -> Any:
        ...


def batch_format(hook: Any) -> str:
    """Best-effort capability probe: ``rows``, ``record_batch``, or ``unknown``."""
    fmt = getattr(hook, "batch_format", None)
    if callable(fmt):
        return str(fmt())
    if isinstance(fmt, str):
        return fmt
    return "unknown"
