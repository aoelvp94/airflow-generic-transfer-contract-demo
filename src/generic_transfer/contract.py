"""Hook capability contracts for record transfer.

Hooks implement these methods structurally. They do not need to import this
module: ``isinstance(hook, BatchReadable)`` works via ``@runtime_checkable``
as long as the method names and call shapes match (PEP 544).
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BatchReadable(Protocol):
    """Source side: yield batches of records. Pagination lives in the hook."""

    def read_batches(self, **params: Any) -> Iterator[list[dict[str, Any]]]:
        """Yield successive batches of row dicts.

        The hook owns pagination (offset, cursor, next-link, …). The operator
        only iterates the batches and never sees page tokens.
        """
        ...


@runtime_checkable
class BatchWritable(Protocol):
    """Destination side: consume batches of records."""

    def write_batches(self, batches: Iterator[list[dict[str, Any]]], **params: Any) -> Any:
        """Consume ``batches`` and return a destination-specific result.

        Implementations may stream, buffer, or re-chunk. The operator does not
        inspect the result shape beyond returning it to the caller.
        """
        ...
