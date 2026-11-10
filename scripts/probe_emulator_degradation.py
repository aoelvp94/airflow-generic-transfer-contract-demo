#!/usr/bin/env python3
"""Probe: does emulator PUT latency grow with the number of stored objects?

Background: the quick scale bench writes a fresh destination prefix every round
and never deleted it, so an emulator that has been up for hours accumulates
tens of thousands of objects. This probe fills one bucket per provider in steps
and measures the median latency of 4 KiB PUT/GET at each step, so a reviewer
can see whether later pairs in a long bench run were penalised by the emulator,
not by the code under test.

Usage (start from fresh emulators, restart them afterwards):
    just down && just up
    .venv/bin/python scripts/probe_emulator_degradation.py --no-cleanup
    just down && just up
Writes bench-out/EMULATOR_DEGRADATION.{json,md}.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    from generic_transfer.adapters.emulators import (
        clear_prefix, ensure_bucket, get_bytes, load_emulator_env, make_fs, put_bytes,
    )

    p = argparse.ArgumentParser()
    p.add_argument("--steps", default="0,2000,5000,10000,20000")
    p.add_argument("--providers", default="s3,gcs,azure")
    p.add_argument("--samples", type=int, default=30)
    p.add_argument("--no-cleanup", action="store_true",
                   help="skip deleting the filler objects (deleting 20k keys on a "
                        "degraded S3Mock takes longer than filling); restart the "
                        "emulators afterwards with `just down && just up`")
    args = p.parse_args()
    load_emulator_env()
    steps = [int(x) for x in args.steps.split(",")]
    buckets = {"s3": "probe-bucket", "gcs": "probe-bucket", "azure": "probe-container"}
    payload = b"x" * 4096
    out: dict = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "steps": steps,
        "samples": args.samples,
        "results": {},
    }
    for fl in args.providers.split(","):
        emu = make_fs(fl, bucket=buckets[fl])
        ensure_bucket(emu)
        stored = 0
        rows = []
        for target in steps:
            todo = [f"fill/{i:06d}.bin" for i in range(stored, target)]
            with ThreadPoolExecutor(max_workers=16) as pool:
                list(pool.map(lambda k: put_bytes(emu, k, payload), todo))
            stored = max(stored, target)
            put_ms, get_ms = [], []
            for i in range(args.samples):
                key = f"probe/{target}-{i}.bin"
                t0 = time.perf_counter()
                put_bytes(emu, key, payload)
                put_ms.append((time.perf_counter() - t0) * 1000)
                t0 = time.perf_counter()
                get_bytes(emu, key)
                get_ms.append((time.perf_counter() - t0) * 1000)
            row = {
                "objects_in_bucket": stored,
                "put_median_ms": statistics.median(put_ms),
                "get_median_ms": statistics.median(get_ms),
            }
            rows.append(row)
            print(fl, row, flush=True)
        out["results"][fl] = rows
        if not args.no_cleanup:
            clear_prefix(emu, "fill")
            clear_prefix(emu, "probe")

    bench = ROOT / "bench-out"
    bench.mkdir(exist_ok=True)
    (bench / "EMULATOR_DEGRADATION.json").write_text(json.dumps(out, indent=2) + "\n")
    lines = [
        "# Emulator PUT/GET latency vs objects stored",
        "",
        f"- Captured (UTC): `{out['timestamp_utc']}`  samples per step: {args.samples}",
        "- 4 KiB objects, native SDK PUT, fsspec GET, one bucket per provider.",
        "",
        "| Provider | Objects in bucket | PUT median (ms) | GET median (ms) |",
        "|---|---:|---:|---:|",
    ]
    for fl, rows in out["results"].items():
        for r in rows:
            lines.append(
                f"| {fl} | {r['objects_in_bucket']} | {r['put_median_ms']:.1f} | {r['get_median_ms']:.1f} |"
            )
    (bench / "EMULATOR_DEGRADATION.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
