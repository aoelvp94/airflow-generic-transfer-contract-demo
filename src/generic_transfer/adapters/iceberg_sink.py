"""Iceberg append sink (pyiceberg + SQLite catalog).

Satisfies BatchWritable. Reuses the append pattern from the personal
http-to-object transfer demo: align to table schema, drop unknown keys,
reload + retry on commit conflict. Schema-drift policy is explicit:
extra source columns are dropped; missing columns become null.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any


class IcebergAppendSink:
    """Append record batches to a local Iceberg table."""

    batch_format = "rows"

    def __init__(self, warehouse: str | Path, *, retries: int = 3):
        self.warehouse = Path(warehouse).resolve()
        self.retries = retries

    def describe_params(self, role: str = "destination") -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "table": {"type": "string"},
            },
            "required": [],
            "additionalProperties": False,
        }

    def get_lineage_datasets(self, **params: Any) -> list[Any]:
        ns = params.get("namespace", "raw")
        table = params.get("table", "records")
        return [{"uri": f"iceberg://{ns}.{table}", "role": "outlet"}]

    def write_batches(
        self,
        batches: Iterator[Any],
        *,
        namespace: str = "raw",
        table: str = "records",
        **_: Any,
    ) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        for batch in batches:
            records.extend(_coerce_rows(batch))
        if not records:
            return {"namespace": namespace, "table": table, "records": 0}
        written = _append_records(
            records,
            self.warehouse,
            namespace,
            table,
            retries=self.retries,
        )
        return {"namespace": namespace, "table": table, "records": written}

    async def awrite_batches(
        self,
        batches: AsyncIterator[Any],
        *,
        namespace: str = "raw",
        table: str = "records",
        **params: Any,
    ) -> dict[str, Any]:
        collected: list[Any] = []

        async def _drain():
            async for batch in batches:
                collected.append(batch)

        await _drain()

        def _iter():
            yield from collected

        return self.write_batches(_iter(), namespace=namespace, table=table, **params)


def _coerce_rows(batch: Any) -> list[dict[str, Any]]:
    if batch is None:
        return []
    # pyarrow.RecordBatch / Table
    if hasattr(batch, "to_pylist"):
        return list(batch.to_pylist())
    if isinstance(batch, list):
        return list(batch)
    raise TypeError(f"unsupported batch type: {type(batch)!r}")


def _append_records(
    records: list[dict],
    warehouse: Path,
    namespace: str,
    table: str,
    *,
    retries: int,
) -> int:
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import CommitFailedException, NamespaceAlreadyExistsError, NoSuchTableError

    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass
    ident = f"{namespace}.{table}"
    try:
        tbl = catalog.load_table(ident)
    except NoSuchTableError:
        tbl = catalog.create_table(ident, schema=pa.Table.from_pylist(records).schema)
    for attempt in range(retries):
        schema = tbl.schema().as_arrow()
        # Schema drift: keep table columns only; missing -> None; extras dropped.
        rows = [{f.name: r.get(f.name) for f in schema} for r in records]
        try:
            tbl.append(pa.Table.from_pylist(rows, schema=schema))
            return len(rows)
        except CommitFailedException:
            if attempt == retries - 1:
                raise
            tbl = catalog.load_table(ident)
    return 0
