"""ADBC / Arrow adapters for the generic transfer contract.

Evaluates whether ``BatchReadable`` / ``BatchWritable`` accommodate Arrow
RecordBatches end-to-end. This module is evidence only.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any


class AdbcSqliteSource:
    """Read a SQL query via ADBC SQLite as ``pyarrow.RecordBatch`` batches."""

    batch_format = "record_batch"

    def __init__(self, db_path: str | Path, *, batch_size: int = 65536):
        self.db_path = str(db_path)
        self.batch_size = batch_size

    def describe_params(self, role: str = "source") -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "batch_size": {"type": "integer"},
            },
            "required": ["sql"],
            "additionalProperties": False,
        }

    def get_lineage_datasets(self, **params: Any) -> list[Any]:
        return [{"uri": f"adbc+sqlite://{self.db_path}", "role": "inlet"}]

    def read_batches(self, *, sql: str, batch_size: int | None = None, **_: Any) -> Iterator[Any]:
        import adbc_driver_sqlite.dbapi

        size = batch_size or self.batch_size
        if size <= 0:
            raise ValueError("batch_size must be positive")
        # One query. The ADBC reader is iterated; batches are re-chunked to
        # ``size`` rows and never concatenated into a table of the full result.
        # The connection stays open until the caller finishes the iterator.
        with adbc_driver_sqlite.dbapi.connect(self.db_path) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                reader = cur.fetch_record_batch()
                yield from _chunks_of(reader, size)


class AdbcSqliteSink:
    """Ingest Arrow batches into a SQLite table via ADBC."""

    batch_format = "record_batch"

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)

    def describe_params(self, role: str = "destination") -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "mode": {"type": "string"},
            },
            "required": ["table"],
            "additionalProperties": False,
        }

    def get_lineage_datasets(self, **params: Any) -> list[Any]:
        return [{"uri": f"adbc+sqlite://{self.db_path}/{params.get('table')}", "role": "outlet"}]

    def write_batches(
        self,
        batches: Iterator[Any],
        *,
        table: str,
        mode: str = "append",
        **_: Any,
    ) -> dict[str, Any]:
        import adbc_driver_sqlite.dbapi

        batch_count = 0
        rows = 0
        ingested = False
        schema = None
        conn = None
        try:
            for batch in batches:
                batch_count += 1
                arrow = _batch_to_table(batch)
                if arrow.num_rows == 0:
                    continue
                if schema is None:
                    schema = arrow.schema
                elif not arrow.schema.equals(schema):
                    arrow = _align_table(arrow, schema)
                if conn is None:
                    conn = adbc_driver_sqlite.dbapi.connect(self.db_path)
                use_mode = mode if not ingested else "append"
                with conn.cursor() as cur:
                    cur.adbc_ingest(table, arrow, mode=use_mode)
                conn.commit()
                ingested = True
                rows += arrow.num_rows
        finally:
            if conn is not None:
                conn.close()
        if not ingested:
            return {"table": table, "records": 0, "batches": 0}
        return {"table": table, "records": rows, "batches": batch_count, "mode": mode}


def _chunks_of(reader: Any, size: int) -> Iterator[Any]:
    """Re-chunk a reader to at most ``size`` rows without buffering the query."""
    import pyarrow as pa

    pending: list[Any] = []
    pending_rows = 0
    for batch in _iter_reader(reader):
        offset = 0
        n = batch.num_rows
        while offset < n:
            take = min(size - pending_rows, n - offset)
            pending.append(batch.slice(offset, take))
            pending_rows += take
            offset += take
            if pending_rows >= size:
                yield _combine_batches(pending, pa)
                pending = []
                pending_rows = 0
    if pending:
        yield _combine_batches(pending, pa)


def _combine_batches(batches: list[Any], pa: Any) -> Any:
    if len(batches) == 1:
        return batches[0]
    schema = batches[0].schema
    # One RecordBatch of at most ``batch_size`` rows. The inputs are slices of
    # the current reader batch, not the whole query.
    arrays = [pa.concat_arrays([b.column(i) for b in batches]) for i in range(len(schema))]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _iter_reader(reader: Any) -> Iterator[Any]:
    """Yield record batches from an ADBC reader without collecting them."""
    if reader is None:
        return
    import pyarrow as pa

    if isinstance(reader, pa.RecordBatch):
        if reader.num_rows:
            yield reader
        return
    for batch in reader:
        if batch is not None and getattr(batch, "num_rows", 0):
            yield batch


def _batch_to_table(batch: Any) -> Any:
    import pyarrow as pa

    if hasattr(batch, "num_rows") and hasattr(batch, "num_columns"):
        return pa.Table.from_batches([batch])
    if hasattr(batch, "to_pylist") and not hasattr(batch, "num_columns"):
        return pa.Table.from_pylist(list(batch.to_pylist()))
    if isinstance(batch, list):
        return pa.Table.from_pylist(batch)
    raise TypeError(type(batch))


def _align_table(table: Any, schema: Any) -> Any:
    """Keep ``schema`` columns only. Missing columns become null. Extras are dropped."""
    import pyarrow as pa

    columns = []
    for field in schema:
        if field.name in table.schema.names:
            columns.append(table.column(field.name).cast(field.type))
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(columns, schema=schema)


class DictRowsSource:
    """Yield list[dict] batches — baseline for Arrow vs rows comparison."""

    batch_format = "rows"

    def __init__(self, rows: list[dict[str, Any]], *, batch_size: int = 10_000):
        self.rows = rows
        self.batch_size = batch_size

    def read_batches(self, **_: Any) -> Iterator[list[dict[str, Any]]]:
        for i in range(0, len(self.rows), self.batch_size):
            yield self.rows[i : i + self.batch_size]


class DictRowsSink:
    """Collect list[dict] (and coerce RecordBatch via to_pylist)."""

    batch_format = "rows"

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def write_batches(self, batches: Iterator[Any], **_: Any) -> dict[str, Any]:
        n = 0
        for batch in batches:
            if hasattr(batch, "to_pylist"):
                rows = batch.to_pylist()
            else:
                rows = list(batch)
            self.rows.extend(rows)
            n += len(rows)
        return {"records": n}
