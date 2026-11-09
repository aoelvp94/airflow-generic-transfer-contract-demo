"""One generic record-transfer operator on hook contracts.

Mirrors the shape David Blain sketched for extending GenericTransfer: the
operator forwards ``source_hook_params`` / ``destination_hook_params`` to each
hook and validates capabilities at task run time. It knows neither vocabulary.
"""
from __future__ import annotations

from typing import Any

from .contract import (
    BatchReadable,
    BatchWritable,
    FileCopyable,
    LineageProvider,
    ParamsDescribable,
)


class ParamValidationError(ValueError):
    """Raised when forwarded hook params fail the hook's ``describe_params`` schema."""


def _validate_params(hook: Any, params: dict[str, Any], *, role: str) -> None:
    if not isinstance(hook, ParamsDescribable):
        return
    schema = hook.describe_params(role) or {}
    props = schema.get("properties") or {}
    required = list(schema.get("required") or [])
    additional = schema.get("additionalProperties", True)
    unknown = [k for k in params if k not in props] if props and additional is False else []
    missing = [k for k in required if k not in params]
    type_errors: list[str] = []
    # JSON Schema "object" is intentionally *not* restricted to dict here:
    # hooks pass filesystem handles / EmulatorFS / custom stores as opaque objects.
    type_map = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
    }
    for key, value in params.items():
        spec = props.get(key) if props else None
        if not isinstance(spec, dict):
            continue
        expected = spec.get("type")
        if expected is None or expected == "object":
            continue
        py = type_map.get(expected)
        if py is not None and not isinstance(value, py):
            type_errors.append(f"{key}: expected {expected}, got {type(value).__name__}")
    if missing or unknown or type_errors:
        parts = []
        if missing:
            parts.append(f"missing required: {missing}")
        if unknown:
            parts.append(f"unknown params: {unknown}")
        if type_errors:
            parts.append("type errors: " + "; ".join(type_errors))
        raise ParamValidationError(
            f"{type(hook).__name__} ({role}) param validation failed: " + "; ".join(parts)
        )


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
        collect_lineage: bool = True,
    ) -> None:
        self.source_hook = source_hook
        self.destination_hook = destination_hook
        self.source_hook_params = dict(source_hook_params or {})
        self.destination_hook_params = dict(destination_hook_params or {})
        self.collect_lineage = collect_lineage
        self.inlets: list[Any] = []
        self.outlets: list[Any] = []

    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Validate contracts, stream batches source → destination, return result."""
        del context  # reserved for Airflow task context when this graduates
        source = self.source_hook
        dest = self.destination_hook
        if not isinstance(source, BatchReadable):
            raise TypeError(
                f"source_hook {type(source).__name__!r} does not implement "
                "read_batches(**params) -> Iterator"
            )
        if not isinstance(dest, BatchWritable):
            raise TypeError(
                f"destination_hook {type(dest).__name__!r} does not implement "
                "write_batches(batches, **params)"
            )
        _validate_params(source, self.source_hook_params, role="source")
        _validate_params(dest, self.destination_hook_params, role="destination")
        if self.collect_lineage:
            self.inlets = _lineage(source, self.source_hook_params)
            self.outlets = _lineage(dest, self.destination_hook_params)
        batches = source.read_batches(**self.source_hook_params)
        return dest.write_batches(batches, **self.destination_hook_params)


class GenericFileTransfer:
    """Connect a FileCopyable destination to a source filesystem/prefix.

    The destination hook owns the copy implementation (sequential, parallel,
    or same-provider server-side). Source is passed via ``copy_params``.
    """

    template_fields: tuple[str, ...] = ("copy_params",)

    def __init__(
        self,
        *,
        destination_hook: Any,
        copy_params: dict[str, Any] | None = None,
        collect_lineage: bool = True,
    ) -> None:
        self.destination_hook = destination_hook
        self.copy_params = dict(copy_params or {})
        self.collect_lineage = collect_lineage
        self.inlets: list[Any] = []
        self.outlets: list[Any] = []

    def execute(self, context: dict[str, Any] | None = None) -> Any:
        del context
        dest = self.destination_hook
        if not isinstance(dest, FileCopyable):
            raise TypeError(
                f"destination_hook {type(dest).__name__!r} does not implement "
                "copy_files(**params)"
            )
        _validate_params(dest, self.copy_params, role="copy")
        if self.collect_lineage:
            self.outlets = _lineage(dest, self.copy_params)
            src = self.copy_params.get("source_hook")
            if src is not None:
                self.inlets = _lineage(src, self.copy_params)
        return dest.copy_files(**self.copy_params)


class GenericAsyncRecordTransfer:
    """Minimal deferrable-shaped path using async iterators.

    Not a real Airflow Trigger/Deferral: demonstrates that the contract can
    grow an async form without changing the sync Protocols. See README.
    """

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

    async def execute(self, context: dict[str, Any] | None = None) -> Any:
        from .contract import AsyncBatchReadable, AsyncBatchWritable

        del context
        source = self.source_hook
        dest = self.destination_hook
        if not isinstance(source, AsyncBatchReadable):
            raise TypeError(
                f"source_hook {type(source).__name__!r} does not implement aread_batches"
            )
        if not isinstance(dest, AsyncBatchWritable):
            raise TypeError(
                f"destination_hook {type(dest).__name__!r} does not implement awrite_batches"
            )
        batches = source.aread_batches(**self.source_hook_params)
        return await dest.awrite_batches(batches, **self.destination_hook_params)


def _lineage(hook: Any, params: dict[str, Any]) -> list[Any]:
    if isinstance(hook, LineageProvider):
        return list(hook.get_lineage_datasets(**params) or [])
    return []
