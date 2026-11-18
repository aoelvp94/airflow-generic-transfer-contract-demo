"""One generic record-transfer operator on hook contracts.

Mirrors the shape David Blain sketched for extending GenericTransfer: the
operator forwards ``source_hook_params`` / ``destination_hook_params`` to each
hook and validates capabilities at task run time. It knows neither vocabulary.
"""
from __future__ import annotations

from typing import Any

from .contract import BatchReadable, BatchWritable


class GenericRecordTransfer:
    """Connect a BatchReadable source to a BatchWritable destination.

    Not an Airflow ``BaseOperator`` subclass: this demo stays dependency-light.
    The public surface matches what a real operator would expose
    (templated param dicts, run-time validation, no knowledge of either side).
    """

    template_fields: tuple[str, ...] = (
        "source_hook_params",
        "destination_hook_params",
    )

    def __init__(
        self,
        *,
        source_hook: Any,
        destination_hook: Any,
        source_hook_params: dict[str, Any] | None = None,
        destination_hook_params: dict[str, Any] | None = None,
    ) -> None:
        self.source_hook = source_hook
        self.destination_hook = destination_hook
        self.source_hook_params = dict(source_hook_params or {})
        self.destination_hook_params = dict(destination_hook_params or {})

    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Validate contracts, stream batches source → destination, return result."""
        del context  # reserved for Airflow task context when this graduates
        source = self.source_hook
        dest = self.destination_hook
        if not isinstance(source, BatchReadable):
            raise TypeError(
                f"source_hook {type(source).__name__!r} does not implement "
                "read_batches(**params) -> Iterator[list[dict]]"
            )
        if not isinstance(dest, BatchWritable):
            raise TypeError(
                f"destination_hook {type(dest).__name__!r} does not implement "
                "write_batches(batches, **params)"
            )
        batches = source.read_batches(**self.source_hook_params)
        return dest.write_batches(batches, **self.destination_hook_params)
