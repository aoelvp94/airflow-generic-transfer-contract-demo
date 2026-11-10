#!/usr/bin/env python3
"""Profile the generic copier per pair: where do requests and time go?

For each pair, seeds N small objects once, then runs (median of --rounds):

* naive pair copy (native SDK list/GET/PUT)
* generic sequential, ``listing="per_key"``  (pre-06: info()/HEAD per key,
  then fs.open() -> another HEAD -> GET)
* generic sequential, ``listing="single"``   (06: one listing, cat_file GET)

and reports wall time, the listing phase alone, and real HTTP requests per
provider/operation (``generic_transfer.wire``). ``--cprofile`` dumps the top
cumulative functions of the per_key run for the first pair.

Writes bench-out/PROFILE_LISTING.{json,md}.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import json
import os
import pstats
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

SRC = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
DST = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}


def main() -> int:
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    from bench_scale import _wipe
    from generic_transfer import wire
    from generic_transfer.adapters.emulators import (
        ensure_bucket, load_emulator_env, make_fs, seed_many_small,
    )
    from generic_transfer.adapters.file_copy import copy_files_sequential
    from generic_transfer.adapters.file_sink import (
        _list_transfer_keys_per_key, list_transfer_entries,
    )
    from generic_transfer.adapters.emulators import put_bytes
    from generic_transfer.adapters.pair_baseline import naive_pair_copy

    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=1000)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--pairs", default="azure->gcs,azure->s3,s3->gcs,gcs->s3,s3->azure,gcs->azure")
    p.add_argument("--cprofile", action="store_true")
    p.add_argument("--latency-ms", type=float, default=0.0)
    args = p.parse_args()
    load_emulator_env()
    wire.install()

    out: dict = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
                 "n": args.n, "rounds": args.rounds, "latency_ms": args.latency_ms,
                 "pairs": {}}
    profiled = False
    for spec in args.pairs.split(","):
        s, d = spec.split("->")
        src = make_fs(s, bucket=SRC[s]); dst = make_fs(d, bucket=DST[d])
        ensure_bucket(src); ensure_bucket(dst)
        prefix = f"prof-{s}-{d}"
        seed_many_small(src, prefix, n=args.n, size=4096, marker_every=100)
        base = f"{src.bucket}/{prefix}"

        def put(key, data, _dst=dst):
            put_bytes(_dst, key, data)

        runs = {
            "naive": lambda dp: naive_pair_copy(src, dst, source_prefix=prefix, dest_prefix=dp),
            "generic_per_key": lambda dp: copy_files_sequential(
                source_fs=src.fs, source_bucket=src.bucket, source_prefix=prefix,
                put=put, dest_prefix=dp, chunk_size=1 << 20, listing="per_key"),
            "generic_single": lambda dp: copy_files_sequential(
                source_fs=src.fs, source_bucket=src.bucket, source_prefix=prefix,
                put=put, dest_prefix=dp, chunk_size=1 << 20, listing="single"),
            "list_per_key": lambda dp: {"count": len(_list_transfer_keys_per_key(src.fs, base))},
            "list_single": lambda dp: {"count": len(list_transfer_entries(src.fs, base))},
        }
        row: dict = {}
        for name, fn in runs.items():
            secs, http = [], {}
            for r in range(args.rounds):
                dp = f"prof-out-{name}-{prefix}-r{r}"
                wire.reset(); wire.set_latency_ms(args.latency_ms)
                prof = None
                if args.cprofile and not profiled and name == "generic_per_key" and r == args.rounds - 1:
                    prof = cProfile.Profile(); prof.enable()
                t0 = time.perf_counter()
                try:
                    res = fn(dp)
                finally:
                    secs.append(time.perf_counter() - t0)
                    wire.set_latency_ms(0)
                if prof is not None:
                    prof.disable(); profiled = True
                    buf = io.StringIO()
                    pstats.Stats(prof, stream=buf).sort_stats("cumulative").print_stats(25)
                    (ROOT / "bench-out" / "PROFILE_LISTING_cprofile.txt").write_text(
                        f"# cProfile: generic_per_key {spec} n={args.n}\n" + buf.getvalue())
                http = wire.snapshot()
                if not name.startswith("list"):
                    _wipe(dst, dp)
            row[name] = {"median_s": statistics.median(secs), "samples_s": secs,
                         "count": res.get("count"), "http": http,
                         "http_total": sum(http.values())}
            print(spec, name, f"{row[name]['median_s']:.3f}s", row[name]["http_total"], http, flush=True)
        _wipe(src, prefix)
        out["pairs"][spec] = row

    bench = ROOT / "bench-out"
    bench.mkdir(exist_ok=True)
    suffix = f"_lat{int(args.latency_ms)}" if args.latency_ms else ""
    (bench / f"PROFILE_LISTING{suffix}.json").write_text(json.dumps(out, indent=2) + "\n")
    lines = [f"# Listing/read profile per pair (n={args.n}, rounds={args.rounds}, "
             f"injected latency {args.latency_ms} ms/request)", "",
             f"- Captured (UTC): `{out['timestamp_utc']}`", "",
             "| Pair | Path | Median (s) | Objects | HTTP total | HTTP breakdown (last round) |",
             "|---|---|---:|---:|---:|---|"]
    for spec, row in out["pairs"].items():
        for name, r in row.items():
            br = ", ".join(f"{k}={v}" for k, v in r["http"].items())
            lines.append(f"| `{spec}` | {name} | {r['median_s']:.3f} | {r['count']} | {r['http_total']} | {br} |")
    (bench / f"PROFILE_LISTING{suffix}.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
