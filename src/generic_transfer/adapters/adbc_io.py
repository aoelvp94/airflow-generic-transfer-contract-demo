"""ADBC / Arrow adapters for the generic transfer contract.

Evaluates whether ``BatchReadable`` / ``BatchWritable`` accommodate Arrow
RecordBatches end-to-end (Aldo's separate ADBC proposal). This module is
evidence only — it is not that proposal.
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
        import pyarrow as pa

        size = batch_size or self.batch_size
        with adbc_driver_sqlite.dbapi.connect(self.db_path) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                reader = cur.fetch_record_batch(size)
                # fetch_record_batch may return a RecordBatchReader
                if hasattr(reader, "__iter__"):
                    for batch in reader:
                        if batch is not None and batch.num_rows:
                            yield batch
                elif reader is not None and getattr(reader, "num_rows", 0):
                    yield reader


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
        import pyarrow as pa

        tables: list[pa.Table] = []
        batch_count = 0
        rows = 0
        for batch in batches:
            batch_count += 1
            if hasattr(batch, "to_pylist") and not hasattr(batch, "num_columns"):
                # list[dict] fallback
                tables.append(pa.Table.from_pylist(batch))
                rows += len(batch)
            elif hasattr(batch, "num_rows"):
                # RecordBatch
                tables.append(pa.Table.from_batches([batch]))
                rows += batch.num_rows
            elif isinstance(batch, list):
                tables.append(pa.Table.from_pylist(batch))
                rows += len(batch)
            else:
                raise TypeError(type(batch))
        if not tables:
            return {"table": table, "records": 0, "batches": 0}
        combined = pa.concat_tables(tables, promote_options="default")
        with adbc_driver_sqlite.dbapi.connect(self.db_path) as conn:
            with conn.cursor() as cur:
                cur.adbc_ingest(table, combined, mode=mode)
            conn.commit()
        return {"table": table, "records": rows, "batches": batch_count, "mode": mode}


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
