#!/usr/bin/env python3
"""Compare Python row dicts vs Arrow RecordBatch end-to-end on the same contract.

Peak memory is the child process VmHWM (high-water resident set) around the
transfer, after imports. macOS and hosts without /proc/self/status use
ru_maxrss; the report names the source. Both the before and after marks are
recorded for each path.
"""
from __future__ import annotations

import json
import os
import platform
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _host() -> dict:
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except Exception:
        cpu = platform.processor()
    mem_gb = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                mem_gb = round(int(line.split()[1]) / 1024 / 1024, 1)
    except Exception:
        pass
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_model": cpu,
        "cpu_count": os.cpu_count(),
        "mem_total_gb": mem_gb,
    }


def measure_adbc(spec: dict) -> dict:
    """Child entry: one transfer, VmHWM before and after that transfer."""
    import adbc_driver_sqlite.dbapi  # noqa: F401
    import pyarrow as pa  # noqa: F401

    from generic_transfer import GenericRecordTransfer
    from generic_transfer.adapters.adbc_io import (
        AdbcSqliteSink,
        AdbcSqliteSource,
        DictRowsSink,
        DictRowsSource,
    )
    from generic_transfer.rss import read_peak_rss

    path = spec["path"]
    db = spec["db"]
    batch_size = int(spec["batch_size"])
    sql = spec["sql"]
    # Libraries are imported above. This mark is the floor before the transfer.
    before, label = read_peak_rss()
    import time

    t0 = time.perf_counter()
    if path == "arrow_adbc_e2e":
        dst = Path(spec["dst"])
        if dst.exists():
            dst.unlink()
        result = GenericRecordTransfer(
            source_hook=AdbcSqliteSource(db, batch_size=batch_size),
            destination_hook=AdbcSqliteSink(dst),
            source_hook_params={"sql": sql},
            destination_hook_params={"table": "dst", "mode": "create"},
        ).execute()
    elif path == "arrow_to_rows_then_sink":
        class ArrowToRows:
            batch_format = "rows"

            def read_batches(self, **params):
                for batch in AdbcSqliteSource(db, batch_size=batch_size).read_batches(**params):
                    yield batch.to_pylist()

        sink = DictRowsSink()
        result = GenericRecordTransfer(
            source_hook=ArrowToRows(),
            destination_hook=sink,
            source_hook_params={"sql": sql},
        ).execute()
        sink.rows.clear()
    elif path == "pure_rows":
        rows_n = int(spec["rows_n"])
        pure_rows = [{"id": i, "value": i * 2} for i in range(rows_n)]
        sink = DictRowsSink()
        result = GenericRecordTransfer(
            source_hook=DictRowsSource(pure_rows, batch_size=batch_size),
            destination_hook=sink,
        ).execute()
        sink.rows.clear()
    else:
        raise ValueError(path)
    elapsed = time.perf_counter() - t0
    peak, label = read_peak_rss()
    summary = {
        "records": result.get("records") if isinstance(result, dict) else None,
        "batches": result.get("batches") if isinstance(result, dict) else None,
    }
    return {
        "before_bytes": before,
        "peak_bytes": peak,
        "rss_source": label,
        "seconds": elapsed,
        "result": summary,
    }


def main() -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--batch-size", type=int, default=50_000)
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()

    import pyarrow as pa
    import adbc_driver_sqlite.dbapi

    from generic_transfer import batch_format
    from generic_transfer.adapters.adbc_io import AdbcSqliteSink, AdbcSqliteSource
    from generic_transfer.rss import run_in_child

    out_dir = ROOT / "benchmarks/results"
    out_dir.mkdir(exist_ok=True)
    db = out_dir / "adbc_src.db"
    if db.exists():
        db.unlink()

    print(f"Seeding {args.rows} rows into SQLite via ADBC…", flush=True)
    table = pa.table({
        "id": pa.array(range(args.rows), type=pa.int64()),
        "value": pa.array([i * 2 for i in range(args.rows)], type=pa.int64()),
    })
    with adbc_driver_sqlite.dbapi.connect(str(db)) as conn:
        with conn.cursor() as cur:
            cur.adbc_ingest("src", table, mode="create")
        conn.commit()
    del table

    def time_path(name: str, path_key: str, *, rows_n: int | None = None) -> dict:
        samples = []
        peaks = []
        befores = []
        last = None
        source = None
        for i in range(args.rounds):
            spec = {
                "path": path_key,
                "db": str(db),
                "dst": str(out_dir / f"adbc_dst_{path_key}_{i}.db"),
                "batch_size": args.batch_size,
                "rows_n": rows_n if rows_n is not None else args.rows,
                "sql": "SELECT id, value FROM src ORDER BY id",
            }
            measured = run_in_child("benchmarks.bench_adbc", "measure_adbc", spec)
            samples.append(measured["seconds"])
            peaks.append(measured["peak_bytes"] / (1024 * 1024))
            befores.append(measured["before_bytes"] / (1024 * 1024))
            last = measured["result"]
            source = measured["rss_source"]
        return {
            "name": name,
            "median_s": statistics.median(samples),
            "samples_s": samples,
            "rss_before_mb_median": statistics.median(befores),
            "rss_peak_mb_median": statistics.median(peaks),
            "rss_before_mb": befores,
            "rss_peak_mb": peaks,
            "rss_source": source,
            "last": last,
        }

    rows_n = min(args.rows, 200_000)
    arrow_stats = time_path("arrow_adbc_e2e", "arrow_adbc_e2e")
    rows_stats = time_path("arrow_to_rows_then_sink", "arrow_to_rows_then_sink")
    pure_stats = time_path(f"pure_rows_{rows_n}", "pure_rows", rows_n=rows_n)

    doc = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": _host(),
        "rows": args.rows,
        "batch_size": args.batch_size,
        "rounds": args.rounds,
        "rss_source": arrow_stats["rss_source"],
        "batch_format_source": batch_format(AdbcSqliteSource(db)),
        "batch_format_sink": batch_format(AdbcSqliteSink(out_dir / "adbc_dst_arrow_adbc_e2e_0.db")),
        "results": [arrow_stats, rows_stats, pure_stats],
        "fit": {
            "works": [
                "Protocols accept Any batches; isinstance checks are structural.",
                "AdbcSqliteSource.batch_format == 'record_batch'.",
                "GenericRecordTransfer streams RecordBatch end-to-end without knowing Arrow.",
            ],
            "decisions_needed": [
                "batch type negotiation when source is Arrow and sink wants rows (or vice versa).",
                "schema handling / cast policy at the operator vs hook boundary.",
                "whether FileCopyable and BatchReadable share one operator or stay split.",
            ],
        },
    }
    json_path = out_dir / "BENCH_ADBC.json"
    md_path = out_dir / "BENCH_ADBC.md"
    json_path.write_text(json.dumps(doc, indent=2) + "\n")
    host = doc["host"]
    lines = [
        "# ADBC / Arrow fit bench",
        "",
        f"- Rows: **{args.rows}** | batch_size: **{args.batch_size}** | rounds: **{args.rounds}**",
        f"- Captured (UTC): `{doc['timestamp_utc']}`",
        f"- Host: `{host['cpu_model']}` x{host['cpu_count']}, {host['mem_total_gb']} GiB RAM, "
        f"`{host['platform']}`, Python `{host['python']}`",
        f"- Peak RSS source: `{doc['rss_source']}` "
        "(child process, `/proc/self/status` VmHWM; `ru_maxrss` where /proc is absent). "
        "Before is the mark after imports and before the transfer; after is the mark when "
        "the transfer returns. Medians are over the rounds.",
        "",
        "| Path | Median (s) | RSS before MB | RSS peak MB | Records (last) |",
        "|---|---:|---:|---:|---:|",
    ]
    for r in doc["results"]:
        lines.append(
            f"| `{r['name']}` | {r['median_s']:.3f} | {r['rss_before_mb_median']:.1f} | "
            f"{r['rss_peak_mb_median']:.1f} | {(r['last'] or {}).get('records')} |"
        )
    lines += [
        "",
        "## Does the contract fit?",
        "",
        "Works:",
    ]
    for w in doc["fit"]["works"]:
        lines.append(f"- {w}")
    lines += ["", "Needs a decision:", ""]
    for d in doc["fit"]["decisions_needed"]:
        lines.append(f"- {d}")
    lines += ["", "Reproduce: `just bench-adbc`", ""]
    md_path.write_text("\n".join(lines))
    print(md_path.read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
