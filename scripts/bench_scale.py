#!/usr/bin/env python3
"""Large-scale + latency emulator bench for the generic transfer contract.

Scenarios
---------
* one large object (default 100 MiB)
* 1000 small (~4 KiB) objects with folder markers
* mixed tree (markers + unicode + spaces + 0-byte real file)
* all 6 ordered cross-cloud pairs
* modes: (a) naive sequential (b) generic sequential (c) generic parallel
  (d) same-provider server-side where supported
* cold/warm, median of N rounds, request counts, peak tracemalloc
* optional simulated per-request latency (shows why request count dominates)

Honest caveat (also written into BENCH_SCALE.md): emulators on localhost do
**not** represent real cloud latency or throughput. Use ``--latency-ms`` to
illustrate the request-count effect under synthetic RTT.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import statistics
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _rss_mb() -> float:
    # Linux: ru_maxrss is KiB
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--size-mb", type=int, default=100)
    p.add_argument("--small-n", type=int, default=1000)
    p.add_argument("--small-bytes", type=int, default=4096)
    p.add_argument("--workers", default="8",
                   help="comma-separated worker counts for parallel mode "
                        "(default 8; use 4,8,16 for the full matrix)")
    p.add_argument("--chunk-size", type=int, default=1024 * 1024)
    p.add_argument("--latency-ms", type=float, default=0.0)
    p.add_argument("--latency-demo-ms", type=float, default=25.0,
                   help="separate small-tree latency demo (default 25ms/req)")
    p.add_argument("--skip-large", action="store_true")
    p.add_argument("--skip-small", action="store_true")
    p.add_argument("--skip-mixed", action="store_true")
    p.add_argument("--skip-server-side", action="store_true")
    p.add_argument("--quick", action="store_true",
                   help="size-mb=8, small-n=100, rounds=3 — faster local loop")
    args = p.parse_args()
    if args.quick:
        args.size_mb = min(args.size_mb, 8)
        args.small_n = min(args.small_n, 100)
        args.rounds = min(args.rounds, 3)
        if args.workers == "8":
            args.workers = "4,8,16"

    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    sys.path.insert(0, str(ROOT / "src"))
    sys.path.insert(0, str(ROOT))

    from generic_transfer.adapters.emulators import (
        clear_prefix,
        ensure_bucket,
        load_emulator_env,
        make_fs,
        put_bytes,
        seed_many_small,
        seed_mixed_tree,
        seed_one_large,
        seed_tree_with_markers,
    )
    from generic_transfer.adapters.file_copy import (
        ObjectStoreFileCopyHook,
        RequestCounter,
        copy_files_parallel,
        copy_files_sequential,
        server_side_copy_same_provider,
    )
    from generic_transfer.adapters.file_sink import list_transfer_keys
    from generic_transfer.adapters.pair_baseline import (
        PAIRS,
        SAME_PROVIDER_PAIRS,
        naive_pair_copy,
    )
    from tests.conftest import emulators_available

    load_emulator_env()
    if not emulators_available():
        raise SystemExit("emulators not up — run: just up")

    src_bucket = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
    dst_bucket = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}
    for fl in ("s3", "gcs", "azure"):
        ensure_bucket(make_fs(fl, bucket=src_bucket[fl]))
        ensure_bucket(make_fs(fl, bucket=dst_bucket[fl]))

    workers_list = [int(x) for x in args.workers.split(",") if x.strip()]
    results: dict = {
        "env": _env(args),
        "caveat": (
            "Emulators on localhost do not represent real cloud latency or "
            "throughput. Median times here are mostly client/emulator overhead. "
            "Use latency_demo to see how request count dominates under RTT."
        ),
        "scenarios": {},
    }

    if not args.skip_mixed:
        results["scenarios"]["mixed_tree"] = _bench_pairs(
            scenario="mixed_tree",
            pairs=PAIRS,
            rounds=args.rounds,
            seed_fn=lambda emu, prefix: seed_mixed_tree(emu, prefix),
            src_bucket=src_bucket,
            dst_bucket=dst_bucket,
            workers_list=workers_list,
            chunk_size=args.chunk_size,
            latency_s=args.latency_ms / 1000.0,
            expect_objects=5,  # a, hello world, café, empty, nested/b — markers skipped
        )

    if not args.skip_small:
        results["scenarios"]["small_1000"] = _bench_pairs(
            scenario="small_objects",
            pairs=PAIRS,
            rounds=args.rounds,
            seed_fn=lambda emu, prefix: seed_many_small(
                emu, prefix, n=args.small_n, size=args.small_bytes, marker_every=100
            ),
            src_bucket=src_bucket,
            dst_bucket=dst_bucket,
            workers_list=workers_list,
            chunk_size=args.chunk_size,
            latency_s=0.0,
            expect_objects=args.small_n,
        )

    if not args.skip_large:
        # Large object: still all 6 pairs, but only seq + parallel(8); skip naive
        # marker logic (single key). Server-side covered separately.
        results["scenarios"]["large_object"] = _bench_large(
            pairs=PAIRS,
            rounds=args.rounds,
            size_mb=args.size_mb,
            src_bucket=src_bucket,
            dst_bucket=dst_bucket,
            workers_list=workers_list,
            chunk_size=args.chunk_size,
        )

    if not args.skip_server_side:
        results["scenarios"]["server_side_same_provider"] = _bench_server_side(
            pairs=SAME_PROVIDER_PAIRS,
            rounds=args.rounds,
            src_bucket=src_bucket,
            dst_bucket=dst_bucket,
        )

    # Latency demo on the tiny marker tree (4 keys native / 2 transfer).
    results["scenarios"]["latency_demo"] = _latency_demo(
        src_bucket=src_bucket,
        dst_bucket=dst_bucket,
        latency_ms=args.latency_demo_ms,
        rounds=max(3, args.rounds),
    )

    out_dir = ROOT / "bench-out"
    out_dir.mkdir(exist_ok=True)
    json_path = out_dir / "BENCH_SCALE.json"
    md_path = out_dir / "BENCH_SCALE.md"
    json_path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    md_path.write_text(_render(results))
    print(md_path.read_text())
    print(f"\nWrote {json_path} and {md_path}")
    return 0


def _env(args) -> dict:
    import importlib

    pkgs = {}
    for label, mod in {
        "fsspec": "fsspec",
        "s3fs": "s3fs",
        "gcsfs": "gcsfs",
        "adlfs": "adlfs",
        "boto3": "boto3",
    }.items():
        try:
            pkgs[label] = getattr(importlib.import_module(mod), "__version__", "?")
        except Exception:
            pkgs[label] = "missing"
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "rounds": args.rounds,
        "size_mb": args.size_mb,
        "small_n": args.small_n,
        "workers": args.workers,
        "chunk_size": args.chunk_size,
        "latency_ms": args.latency_ms,
        "latency_demo_ms": args.latency_demo_ms,
        "s3_backend": os.environ.get("EMULATOR_S3_BACKEND", "s3mock"),
        "docker_host": os.environ.get("DOCKER_HOST", ""),
        "packages": pkgs,
    }


def _timed(fn, rounds: int) -> dict:
    samples = []
    payloads = []
    peaks = []
    rss = []
    for round_i in range(rounds):
        tracemalloc.start()
        t0 = time.perf_counter()
        try:
            payload = fn(round_i)
        except TypeError:
            payload = fn()
        elapsed = time.perf_counter() - t0
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        payload["seconds"] = elapsed
        payload["tracemalloc_peak_mb"] = peak / (1024 * 1024)
        payload["rss_max_mb"] = _rss_mb()
        samples.append(elapsed)
        payloads.append(payload)
        peaks.append(payload["tracemalloc_peak_mb"])
        rss.append(payload["rss_max_mb"])
    return {
        "cold_s": samples[0],
        "warm_median_s": statistics.median(samples[1:] or samples),
        "median_s": statistics.median(samples),
        "samples_s": samples,
        "tracemalloc_peak_median_mb": statistics.median(peaks),
        "rss_max_mb": max(rss),
        "last": payloads[-1],
    }


def _bench_pairs(*, scenario, pairs, rounds, seed_fn, src_bucket, dst_bucket,
                 workers_list, chunk_size, latency_s, expect_objects) -> list:
    from generic_transfer.adapters.emulators import make_fs, clear_prefix, put_bytes
    from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook
    from generic_transfer.adapters.pair_baseline import naive_pair_copy
    from generic_transfer import GenericFileTransfer

    out = []
    for src_fl, dst_fl in pairs:
        prefix = f"scale-{scenario}-{src_fl}-{dst_fl}"
        row = {"pair": f"{src_fl}->{dst_fl}", "modes": {}}

        # Seed source ONCE per pair (re-seeding 1000 keys every round dominated wall time).
        src = make_fs(src_fl, bucket=src_bucket[src_fl])
        seed_fn(src, prefix)

        # Unique dest prefix per round avoids a 1000-key clear_prefix each time.
        # (a) naive
        def run_naive(round_i=0, _src=src, _dst_fl=dst_fl, _prefix=prefix):
            dst = make_fs(_dst_fl, bucket=dst_bucket[_dst_fl])
            dest_prefix = f"naive-{_prefix}-r{round_i}"
            return naive_pair_copy(
                _src, dst, source_prefix=_prefix, dest_prefix=dest_prefix,
                skip_markers=False,
            )

        row["modes"]["naive_sequential"] = _timed(run_naive, rounds)

        # (b) generic sequential
        def run_generic_seq(round_i=0, _src=src, _dst_fl=dst_fl, _prefix=prefix):
            dst = make_fs(_dst_fl, bucket=dst_bucket[_dst_fl])
            dest_prefix = f"gen-seq-{_prefix}-r{round_i}"
            hook = ObjectStoreFileCopyHook(dst)
            return GenericFileTransfer(
                destination_hook=hook,
                copy_params={
                    "source_fs": _src.fs,
                    "source_bucket": _src.bucket,
                    "source_prefix": _prefix,
                    "dest_prefix": dest_prefix,
                    "mode": "sequential",
                    "chunk_size": chunk_size,
                    "latency_s": latency_s,
                },
            ).execute()

        row["modes"]["generic_sequential"] = _timed(run_generic_seq, rounds)

        # (c) parallel for each worker count
        for w in workers_list:
            def run_par(round_i=0, workers=w, _src=src, _dst_fl=dst_fl, _prefix=prefix):
                dst = make_fs(_dst_fl, bucket=dst_bucket[_dst_fl])
                dest_prefix = f"gen-par{workers}-{_prefix}-r{round_i}"
                hook = ObjectStoreFileCopyHook(dst)
                return GenericFileTransfer(
                    destination_hook=hook,
                    copy_params={
                        "source_fs": _src.fs,
                        "source_bucket": _src.bucket,
                        "source_prefix": _prefix,
                        "dest_prefix": dest_prefix,
                        "mode": "parallel",
                        "workers": workers,
                        "chunk_size": chunk_size,
                    },
                ).execute()

            row["modes"][f"generic_parallel_{w}"] = _timed(run_par, rounds)

        out.append(row)
        print(f"  [{scenario}] {row['pair']} done", flush=True)
    return out


def _bench_large(*, pairs, rounds, size_mb, src_bucket, dst_bucket, workers_list, chunk_size):
    from generic_transfer.adapters.emulators import make_fs, clear_prefix, seed_one_large
    from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook
    from generic_transfer import GenericFileTransfer

    out = []
    for src_fl, dst_fl in pairs:
        prefix = f"large-{src_fl}-{dst_fl}"
        row = {"pair": f"{src_fl}->{dst_fl}", "size_mb": size_mb, "modes": {}}

        src = make_fs(src_fl, bucket=src_bucket[src_fl])
        seed_one_large(src, prefix, size_mb=size_mb)

        def run_seq(round_i=0, _src=src, _dst_fl=dst_fl, _prefix=prefix):
            dst = make_fs(_dst_fl, bucket=dst_bucket[_dst_fl])
            return GenericFileTransfer(
                destination_hook=ObjectStoreFileCopyHook(dst),
                copy_params={
                    "source_fs": _src.fs,
                    "source_bucket": _src.bucket,
                    "source_prefix": _prefix,
                    "dest_prefix": f"lseq-{_prefix}-r{round_i}",
                    "mode": "sequential",
                    "chunk_size": chunk_size,
                },
            ).execute()

        row["modes"]["generic_sequential"] = _timed(run_seq, rounds)
        for w in workers_list[:1]:  # one worker setting for large (single object)
            def run_par(round_i=0, workers=w, _src=src, _dst_fl=dst_fl, _prefix=prefix):
                dst = make_fs(_dst_fl, bucket=dst_bucket[_dst_fl])
                return GenericFileTransfer(
                    destination_hook=ObjectStoreFileCopyHook(dst),
                    copy_params={
                        "source_fs": _src.fs,
                        "source_bucket": _src.bucket,
                        "source_prefix": _prefix,
                        "dest_prefix": f"lpar-{_prefix}-r{round_i}",
                        "mode": "parallel",
                        "workers": workers,
                        "chunk_size": chunk_size,
                    },
                ).execute()

            row["modes"][f"generic_parallel_{w}"] = _timed(run_par, rounds)
        out.append(row)
        print(f"  [large {size_mb}MiB] {row['pair']} done", flush=True)
    return out


def _bench_server_side(*, pairs, rounds, src_bucket, dst_bucket):
    from generic_transfer.adapters.emulators import (
        make_fs, clear_prefix, seed_tree_with_markers,
    )
    from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook
    from generic_transfer import GenericFileTransfer

    out = []
    for src_fl, dst_fl in pairs:
        prefix = f"ss-{src_fl}"
        row = {"pair": f"{src_fl}->{dst_fl}", "modes": {}}

        def run_ss():
            src = make_fs(src_fl, bucket=src_bucket[src_fl])
            dst = make_fs(dst_fl, bucket=dst_bucket[dst_fl])
            seed_tree_with_markers(src, prefix)
            clear_prefix(dst, f"ss-out-{prefix}")
            try:
                return GenericFileTransfer(
                    destination_hook=ObjectStoreFileCopyHook(dst),
                    copy_params={
                        "source_fs": src.fs,
                        "source_prefix": prefix,
                        "source_emu": src,
                        "dest_prefix": f"ss-out-{prefix}",
                        "mode": "server_side",
                    },
                ).execute()
            except Exception as exc:
                return {
                    "error": f"{type(exc).__name__}: {exc}",
                    "count": 0,
                    "request_total": 0,
                    "mode": "server_side",
                }

        def run_rw():
            src = make_fs(src_fl, bucket=src_bucket[src_fl])
            dst = make_fs(dst_fl, bucket=dst_bucket[dst_fl])
            seed_tree_with_markers(src, prefix)
            clear_prefix(dst, f"rw-out-{prefix}")
            return GenericFileTransfer(
                destination_hook=ObjectStoreFileCopyHook(dst),
                copy_params={
                    "source_fs": src.fs,
                    "source_bucket": src.bucket,
                    "source_prefix": prefix,
                    "dest_prefix": f"rw-out-{prefix}",
                    "mode": "sequential",
                },
            ).execute()

        row["modes"]["server_side"] = _timed(run_ss, rounds)
        row["modes"]["read_write"] = _timed(run_rw, rounds)
        out.append(row)
        print(f"  [server_side] {row['pair']} done", flush=True)
    return out


def _latency_demo(*, src_bucket, dst_bucket, latency_ms, rounds):
    """Tiny marker tree with injected RTT — request count dominates wall time."""
    from generic_transfer.adapters.emulators import make_fs, clear_prefix, seed_tree_with_markers
    from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook
    from generic_transfer.adapters.pair_baseline import naive_pair_copy
    from generic_transfer import GenericFileTransfer

    src_fl, dst_fl = "s3", "gcs"
    prefix = "latdemo"
    latency_s = latency_ms / 1000.0

    def run_naive():
        src = make_fs(src_fl, bucket=src_bucket[src_fl])
        dst = make_fs(dst_fl, bucket=dst_bucket[dst_fl])
        seed_tree_with_markers(src, prefix)
        clear_prefix(dst, "lat-naive")
        t0 = time.perf_counter()
        # Approximate latency: sleep per native get+put (4+4) outside the helper
        # by wrapping through generic path with skip_markers False count.
        result = naive_pair_copy(
            src, dst, source_prefix=prefix, dest_prefix="lat-naive", skip_markers=False
        )
        # Inject after the fact proportional to request_total for illustration
        time.sleep(latency_s * result["request_total"])
        result["seconds"] = time.perf_counter() - t0
        result["injected_latency_s"] = latency_s * result["request_total"]
        return result

    def run_generic():
        src = make_fs(src_fl, bucket=src_bucket[src_fl])
        dst = make_fs(dst_fl, bucket=dst_bucket[dst_fl])
        seed_tree_with_markers(src, prefix)
        clear_prefix(dst, "lat-gen")
        hook = ObjectStoreFileCopyHook(dst)
        t0 = time.perf_counter()
        result = GenericFileTransfer(
            destination_hook=hook,
            copy_params={
                "source_fs": src.fs,
                "source_bucket": src.bucket,
                "source_prefix": prefix,
                "dest_prefix": "lat-gen",
                "mode": "sequential",
                "latency_s": latency_s,
            },
        ).execute()
        result["seconds"] = time.perf_counter() - t0
        result["injected_latency_s"] = latency_s * result.get("request_total", 0)
        return result

    return {
        "pair": "s3->gcs",
        "latency_ms_per_request": latency_ms,
        "naive": _timed(run_naive, rounds),
        "generic": _timed(run_generic, rounds),
        "note": (
            "Naive copies 4 objects (9 req); generic copies 2 (5 req). "
            "With injected per-request latency, wall time tracks request_total."
        ),
    }


def _render(results: dict) -> str:
    env = results["env"]
    lines = [
        "# Large-scale emulator bench",
        "",
        f"- Captured (UTC): `{env['timestamp_utc']}`",
        f"- Platform: `{env['platform']}` Python `{env['python']}`",
        f"- Rounds: **{env['rounds']}** | large object: **{env['size_mb']} MiB** | "
        f"small N: **{env['small_n']}** | workers: `{env['workers']}` | "
        f"chunk_size: `{env['chunk_size']}`",
        f"- S3 backend: `{env['s3_backend']}`",
        "",
        f"> **Caveat:** {results['caveat']}",
        "",
    ]
    for name, payload in results["scenarios"].items():
        lines += [f"## Scenario: `{name}`", ""]
        if name == "latency_demo":
            n = payload["naive"]
            g = payload["generic"]
            lines += [
                f"Injected latency: **{payload['latency_ms_per_request']} ms/request** "
                f"on `{payload['pair']}`.",
                "",
                payload["note"],
                "",
                "| Path | Median (s) | Requests (last) | Objects | Injected latency (s) |",
                "|---|---:|---:|---:|---:|",
                f"| naive | {n['median_s']:.3f} | {n['last'].get('request_total')} | "
                f"{n['last'].get('count')} | {n['last'].get('injected_latency_s', 0):.3f} |",
                f"| generic | {g['median_s']:.3f} | {g['last'].get('request_total')} | "
                f"{g['last'].get('count')} | {g['last'].get('injected_latency_s', 0):.3f} |",
                "",
            ]
            continue
        lines += [
            "| Pair | Mode | Cold (s) | Warm med (s) | Median (s) | Objects | Reqs | Peak MB (tracemalloc) |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
        for row in payload:
            for mode, stats in row["modes"].items():
                last = stats["last"]
                lines.append(
                    f"| `{row['pair']}` | {mode} | {stats['cold_s']:.4f} | "
                    f"{stats['warm_median_s']:.4f} | {stats['median_s']:.4f} | "
                    f"{last.get('count', last.get('error', '?'))} | "
                    f"{last.get('request_total', '?')} | "
                    f"{stats['tracemalloc_peak_median_mb']:.1f} |"
                )
        lines.append("")
    lines += [
        "## Reproduce",
        "",
        "```bash",
        "just up && just bench-scale          # full (100 MiB + 1000 objs)",
        "just up && just bench-scale-quick    # 8 MiB + 100 objs",
        "```",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
