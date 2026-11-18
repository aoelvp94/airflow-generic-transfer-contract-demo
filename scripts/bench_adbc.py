#!/usr/bin/env python3
"""Compare Python row dicts vs Arrow RecordBatch end-to-end on the same contract."""
from __future__ import annotations

import json
import statistics
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    sys.path.insert(0, str(ROOT / "src"))
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--batch-size", type=int, default=50_000)
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()

    import pyarrow as pa
    import adbc_driver_sqlite.dbapi

    from generic_transfer import GenericRecordTransfer, batch_format
    from generic_transfer.adapters.adbc_io import (
        AdbcSqliteSink,
        AdbcSqliteSource,
        DictRowsSink,
        DictRowsSource,
    )

    out_dir = ROOT / "bench-out"
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

    def time_path(name, fn):
        samples = []
        peaks = []
        last = None
        for _ in range(args.rounds):
            tracemalloc.start()
            t0 = time.perf_counter()
            last = fn()
            elapsed = time.perf_counter() - t0
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            samples.append(elapsed)
            peaks.append(peak / (1024 * 1024))
        return {
            "name": name,
            "median_s": statistics.median(samples),
            "samples_s": samples,
            "peak_mb_median": statistics.median(peaks),
            "last": last,
        }

    # Arrow end-to-end: ADBC source → ADBC sink
    dst_arrow = out_dir / "adbc_dst_arrow.db"

    def arrow_e2e():
        if dst_arrow.exists():
            dst_arrow.unlink()
        return GenericRecordTransfer(
            source_hook=AdbcSqliteSource(db, batch_size=args.batch_size),
            destination_hook=AdbcSqliteSink(dst_arrow),
            source_hook_params={"sql": "SELECT id, value FROM src ORDER BY id"},
            destination_hook_params={"table": "dst", "mode": "create"},
        ).execute()

    # Rows path: materialize dict batches from Arrow, write via DictRowsSink
    def rows_path():
        # Pull via ADBC then coerce to rows mid-flight (worst of both worlds) —
        # and a pure-rows baseline from an in-memory list for the same N would
        # OOM at 1M on some boxes, so we stream Arrow→rows→list sink.
        class ArrowToRows:
            batch_format = "rows"

            def read_batches(self, **params):
                for batch in AdbcSqliteSource(db, batch_size=args.batch_size).read_batches(**params):
                    yield batch.to_pylist()

        sink = DictRowsSink()
        result = GenericRecordTransfer(
            source_hook=ArrowToRows(),
            destination_hook=sink,
            source_hook_params={"sql": "SELECT id, value FROM src ORDER BY id"},
        ).execute()
        # Drop rows to free memory between rounds
        sink.rows.clear()
        return result

    # Pure in-memory rows for a smaller N (cap at 200k for memory)
    rows_n = min(args.rows, 200_000)
    pure_rows = [{"id": i, "value": i * 2} for i in range(rows_n)]

    def pure_rows_path():
        sink = DictRowsSink()
        result = GenericRecordTransfer(
            source_hook=DictRowsSource(pure_rows, batch_size=args.batch_size),
            destination_hook=sink,
        ).execute()
        sink.rows.clear()
        return result

    arrow_stats = time_path("arrow_adbc_e2e", arrow_e2e)
    rows_stats = time_path("arrow_to_rows_then_sink", rows_path)
    pure_stats = time_path(f"pure_rows_{rows_n}", pure_rows_path)

    doc = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows": args.rows,
        "batch_size": args.batch_size,
        "rounds": args.rounds,
        "batch_format_source": batch_format(AdbcSqliteSource(db)),
        "batch_format_sink": batch_format(AdbcSqliteSink(dst_arrow)),
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
    lines = [
        "# ADBC / Arrow fit bench",
        "",
        f"- Rows: **{args.rows}** | batch_size: **{args.batch_size}** | rounds: **{args.rounds}**",
        f"- Captured (UTC): `{doc['timestamp_utc']}`",
        "",
        "| Path | Median (s) | Peak MB (tracemalloc med) | Records (last) |",
        "|---|---:|---:|---:|",
    ]
    for r in doc["results"]:
        lines.append(
            f"| `{r['name']}` | {r['median_s']:.3f} | {r['peak_mb_median']:.1f} | "
            f"{(r['last'] or {}).get('records')} |"
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
