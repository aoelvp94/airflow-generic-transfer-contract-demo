"""Peak RSS of the record path tracks batch size, not row count.

Local SQLite only. No object-store emulator. The measurement
runs in a child process so VmHWM starts clean.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

pytestmark = pytest.mark.local


def _seed(path: Path, n: int, width: int) -> None:
    payload = "x" * width
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE src (id INTEGER, value TEXT)")
        step = 2_000
        for start in range(0, n, step):
            rows = [(i, payload) for i in range(start, min(start + step, n))]
            conn.executemany("INSERT INTO src (id, value) VALUES (?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


def _measure_transfer(spec: dict) -> dict:
    """Child entry. Import libraries before the before-mark."""
    import adbc_driver_sqlite.dbapi  # noqa: F401
    import pyarrow as pa  # noqa: F401

    from generic_transfer import GenericRecordTransfer
    from generic_transfer.adapters.adbc_io import AdbcSqliteSink, AdbcSqliteSource
    from generic_transfer.rss import read_peak_rss

    before, label = read_peak_rss()
    dst = Path(spec["dst"])
    if dst.exists():
        dst.unlink()
    result = GenericRecordTransfer(
        source_hook=AdbcSqliteSource(spec["db"], batch_size=int(spec["batch_size"])),
        destination_hook=AdbcSqliteSink(dst),
        source_hook_params={"sql": "SELECT id, value FROM src ORDER BY id"},
        destination_hook_params={"table": "dst", "mode": "create"},
    ).execute()
    peak, label = read_peak_rss()
    return {
        "before": before,
        "peak": peak,
        "label": label,
        "records": result["records"],
    }


def test_record_path_rss_grows_less_than_data_size(tmp_path: Path):
    """N and 4N rows, same batch size. Peak RSS stays within 1.5x.

    Rows are wide on purpose. An implementation that materialises the whole
    query would move peak RSS with the row count; a batch-sized path does not.
    """
    pytest.importorskip("adbc_driver_sqlite")
    pytest.importorskip("pyarrow")
    from generic_transfer.rss import run_in_child

    n = 40_000
    width = 1024
    batch_size = 2_000
    db_n = tmp_path / "n.db"
    db_4n = tmp_path / "n4.db"
    _seed(db_n, n, width)
    _seed(db_4n, n * 4, width)

    def _run(db: Path, dst: Path, rows: int) -> dict:
        return run_in_child(
            "tests.unit.test_rss_bound",
            "_measure_transfer",
            {"db": str(db), "dst": str(dst), "batch_size": batch_size, "n": rows},
        )

    small = _run(db_n, tmp_path / "out_n.db", n)
    large = _run(db_4n, tmp_path / "out_4n.db", n * 4)
    assert small["records"] == n
    assert large["records"] == n * 4
    assert small["label"] == large["label"]
    assert small["peak"] > 0
    ratio = large["peak"] / small["peak"]
    assert ratio < 1.5, {
        "n": small,
        "4n": large,
        "ratio": ratio,
        "batch_size": batch_size,
        "width": width,
    }
