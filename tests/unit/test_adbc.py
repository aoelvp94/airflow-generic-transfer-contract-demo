"""ADBC / Arrow fit check against the Protocol contract."""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("adbc_driver_sqlite")
pytest.importorskip("pyarrow")

from generic_transfer import BatchReadable, BatchWritable, GenericRecordTransfer, batch_format
from generic_transfer.adapters.adbc_io import (
    AdbcSqliteSink,
    AdbcSqliteSource,
    DictRowsSink,
    DictRowsSource,
)


def _seed_sqlite(path: Path, n: int = 1000) -> None:
    import adbc_driver_sqlite.dbapi
    import pyarrow as pa

    table = pa.table({"id": list(range(n)), "value": [f"v{i}" for i in range(n)]})
    with adbc_driver_sqlite.dbapi.connect(str(path)) as conn:
        with conn.cursor() as cur:
            cur.adbc_ingest("src", table, mode="create")
        conn.commit()


def test_adbc_adapters_satisfy_protocols(tmp_path: Path):
    db = tmp_path / "t.db"
    _seed_sqlite(db, 100)
    src = AdbcSqliteSource(db, batch_size=40)
    dst_db = tmp_path / "out.db"
    dst = AdbcSqliteSink(dst_db)
    assert isinstance(src, BatchReadable)
    assert isinstance(dst, BatchWritable)
    assert batch_format(src) == "record_batch"
    assert batch_format(dst) == "record_batch"
    result = GenericRecordTransfer(
        source_hook=src,
        destination_hook=dst,
        source_hook_params={"sql": "SELECT id, value FROM src ORDER BY id"},
        destination_hook_params={"table": "dst", "mode": "create"},
    ).execute()
    assert result["records"] == 100
    assert result["batches"] >= 2


def test_arrow_end_to_end_preserves_order(tmp_path: Path):
    db = tmp_path / "t.db"
    _seed_sqlite(db, 250)
    op = GenericRecordTransfer(
        source_hook=AdbcSqliteSource(db, batch_size=100),
        destination_hook=DictRowsSink(),
        source_hook_params={"sql": "SELECT id, value FROM src ORDER BY id"},
    )
    result = op.execute()
    assert result["records"] == 250
    assert op.destination_hook.rows[0]["id"] == 0
    assert op.destination_hook.rows[-1]["id"] == 249


def test_rows_path_still_works_alongside_arrow():
    src = DictRowsSource([{"id": i} for i in range(5)], batch_size=2)
    dst = DictRowsSink()
    assert batch_format(src) == "rows"
    result = GenericRecordTransfer(source_hook=src, destination_hook=dst).execute()
    assert result["records"] == 5
