#!/usr/bin/env python3
"""Small bench: stock HttpToS3-style (page 1) vs generic paginated transfer.

Reports wall time and HTTP request counts against the in-process fake API.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_ROOT / "src"), str(_ROOT)]

import statistics
import time
from pathlib import Path

from fake_api.server import ORDERS, start
from generic_transfer import GenericRecordTransfer
from generic_transfer.adapters import FileBatchSink, PaginatedHttpSourceHook
from generic_transfer.pagination import offset_pagination
from tests.baseline_httptos3 import HttpToObjectBaseline


def _run_stock(base: str, out: Path, rounds: int) -> dict:
    times = []
    requests = []
    for i in range(rounds):
        store: dict[str, bytes] = {}
        op = HttpToObjectBaseline(
            endpoint=f"{base}/orders",
            store_put=lambda k, b: store.__setitem__(k, b),
            key="raw/orders.json",
            params={"limit": 10},
            max_pages=1,
        )
        t0 = time.perf_counter()
        op.execute()
        times.append(time.perf_counter() - t0)
        requests.append(op.request_count)
    return {
        "label": "HttpToObjectBaseline (stock, page 1 only)",
        "records_landed": len(json.loads(store["raw/orders.json"]).get("data", [])),
        "records_available": len(ORDERS),
        "wall_s_mean": statistics.mean(times),
        "wall_s_stdev": statistics.pstdev(times) if rounds > 1 else 0.0,
        "requests_mean": statistics.mean(requests),
        "rounds": rounds,
        "complete": False,
    }


def _run_generic(base: str, out: Path, rounds: int) -> dict:
    times = []
    requests = []
    last_records = 0
    for i in range(rounds):
        sink_root = out / f"generic-{i}"
        source = PaginatedHttpSourceHook(
            base,
            pagination=offset_pagination(records_key="data"),
            records_key="data",
            max_pages=50,
        )
        sink = FileBatchSink(str(sink_root))
        op = GenericRecordTransfer(
            source_hook=source,
            destination_hook=sink,
            source_hook_params={"path": "/orders", "params": {"limit": 10}},
            destination_hook_params={"key": "raw/orders.json"},
        )
        t0 = time.perf_counter()
        result = op.execute()
        times.append(time.perf_counter() - t0)
        requests.append(source.request_count)
        last_records = result["records"]
    return {
        "label": "GenericRecordTransfer (paginated HTTP → file)",
        "records_landed": last_records,
        "records_available": len(ORDERS),
        "wall_s_mean": statistics.mean(times),
        "wall_s_stdev": statistics.pstdev(times) if rounds > 1 else 0.0,
        "requests_mean": statistics.mean(requests),
        "rounds": rounds,
        "complete": last_records == len(ORDERS),
    }


def main() -> None:
    out = Path("bench-out")
    out.mkdir(exist_ok=True)
    server, base = start()
    try:
        rounds = 5
        stock = _run_stock(base, out, rounds)
        generic = _run_generic(base, out, rounds)
        report = {
            "api": "Acme Orders (25 records, limit=10 → 3 pages)",
            "stock": stock,
            "generic": generic,
        }
        (out / "bench.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        print()
        print(
            f"stock:   {stock['wall_s_mean']*1000:.1f} ms, "
            f"{stock['requests_mean']:.0f} req, "
            f"{stock['records_landed']}/{stock['records_available']} records (incomplete)"
        )
        print(
            f"generic: {generic['wall_s_mean']*1000:.1f} ms, "
            f"{generic['requests_mean']:.0f} req, "
            f"{generic['records_landed']}/{generic['records_available']} records (complete)"
        )
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
