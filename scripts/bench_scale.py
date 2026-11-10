#!/usr/bin/env python3
"""Large-scale + latency emulator bench for the generic transfer contract.

Scenarios
---------
* ``small_objects``  N small (~4 KiB) objects with periodic folder markers
* ``large_object``   one large object (default 100 MiB)
* ``mixed_tree``     markers + unicode + spaces + 0-byte real file
* ``server_side_same_provider``  copy_object / copy_blob / start_copy_from_url
* ``wire_latency``   small objects with a fixed delay injected before **every**
  HTTP request (list, HEAD, GET, PUT) -- the realistic case for parallelism

Modes per pair
--------------
* ``naive_sequential``            today's pair-operator shape: native SDK list,
                                  GET, PUT key by key (markers copied)
* ``generic_sequential_per_key``  generic copier with the pre-06 listing
                                  (one ``info()``/HEAD per key + HEAD in open)
* ``generic_sequential``          generic copier, single listing, ``cat_file``
* ``generic_parallel_{w}``        same, bounded thread pool of ``w`` workers

Every timed run records cold (round 0) and warm median, tracemalloc peak and
two request counts: ``logical`` (what the code thinks it did: list/get/put)
and ``http`` (real requests on the wire, counted at the aiohttp / urllib3
transport, see ``generic_transfer.wire``).

Destination prefixes are wiped after every round (outside the timed region) so
emulator object counts stay bounded; see bench-out/EMULATOR_DEGRADATION.md for
why that matters with S3Mock.

Honest caveat (also in the snapshot): localhost emulators do not represent
real cloud latency or throughput.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

SRC_BUCKET = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
DST_BUCKET = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}


def _rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--size-mb", type=int, default=100)
    p.add_argument("--small-n", type=int, default=1000)
    p.add_argument("--small-bytes", type=int, default=4096)
    p.add_argument("--workers", default="4,8,16")
    p.add_argument("--chunk-size", type=int, default=1024 * 1024)
    p.add_argument("--latency-ms", type=float, default=20.0,
                   help="per-HTTP-request delay for the wire_latency scenario")
    p.add_argument("--latency-n", type=int, default=200,
                   help="objects in the wire_latency scenario")
    p.add_argument("--latency-pairs", default="all",
                   help="'all' or comma list like s3->gcs,azure->s3")
    p.add_argument("--pairs", default="all", help="'all' or comma list")
    p.add_argument("--skip-large", action="store_true")
    p.add_argument("--skip-small", action="store_true")
    p.add_argument("--skip-mixed", action="store_true")
    p.add_argument("--skip-server-side", action="store_true")
    p.add_argument("--skip-latency", action="store_true")
    p.add_argument("--quick", action="store_true",
                   help="size-mb=8, small-n=100, latency-n=50 -- fast local loop")
    p.add_argument("--out-name", default=None,
                   help="snapshot basename in bench-out/ (default BENCH_SCALE, "
                        "or BENCH_SCALE_QUICK with --quick)")
    args = p.parse_args()
    if args.quick:
        args.size_mb = min(args.size_mb, 8)
        args.small_n = min(args.small_n, 100)
        args.latency_n = min(args.latency_n, 50)
    out_name = args.out_name or ("BENCH_SCALE_QUICK" if args.quick else "BENCH_SCALE")

    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")

    from generic_transfer import wire
    from generic_transfer.adapters.emulators import ensure_bucket, load_emulator_env, make_fs
    from generic_transfer.adapters.pair_baseline import PAIRS, SAME_PROVIDER_PAIRS
    from tests.conftest import emulators_available

    load_emulator_env()
    if not emulators_available():
        raise SystemExit("emulators not up -- run: just up")
    wire.install()

    for fl in ("s3", "gcs", "azure"):
        ensure_bucket(make_fs(fl, bucket=SRC_BUCKET[fl]))
        ensure_bucket(make_fs(fl, bucket=DST_BUCKET[fl]))

    pairs = _select_pairs(PAIRS, args.pairs)
    lat_pairs = _select_pairs(PAIRS, args.latency_pairs)
    workers_list = [int(x) for x in args.workers.split(",") if x.strip()]
    results: dict = {
        "env": _env(args),
        "caveat": (
            "Emulators on localhost do not represent real cloud latency or "
            "throughput. Times without injected latency are mostly client and "
            "emulator CPU. The wire_latency scenario adds a fixed delay before "
            "every HTTP request to model round-trip time."
        ),
        "scenarios": {},
    }
    t_start = time.perf_counter()

    if not args.skip_mixed:
        from generic_transfer.adapters.emulators import seed_mixed_tree
        results["scenarios"]["mixed_tree"] = _bench_pairs(
            scenario="mixed", pairs=pairs, rounds=args.rounds,
            seed_fn=lambda emu, prefix: seed_mixed_tree(emu, prefix),
            workers_list=workers_list, chunk_size=args.chunk_size, latency_ms=0.0,
        )
    if not args.skip_small:
        from generic_transfer.adapters.emulators import seed_many_small
        results["scenarios"]["small_objects"] = _bench_pairs(
            scenario="small", pairs=pairs, rounds=args.rounds,
            seed_fn=lambda emu, prefix: seed_many_small(
                emu, prefix, n=args.small_n, size=args.small_bytes, marker_every=100),
            workers_list=workers_list, chunk_size=args.chunk_size, latency_ms=0.0,
        )
    if not args.skip_large:
        results["scenarios"]["large_object"] = _bench_large(
            pairs=pairs, rounds=args.rounds, size_mb=args.size_mb,
            workers_list=workers_list, chunk_size=args.chunk_size,
        )
    if not args.skip_server_side:
        results["scenarios"]["server_side_same_provider"] = _bench_server_side(
            pairs=SAME_PROVIDER_PAIRS, rounds=args.rounds,
        )
    if not args.skip_latency and args.latency_ms > 0:
        from generic_transfer.adapters.emulators import seed_many_small
        results["scenarios"]["wire_latency"] = _bench_pairs(
            scenario="lat", pairs=lat_pairs, rounds=args.rounds,
            seed_fn=lambda emu, prefix: seed_many_small(
                emu, prefix, n=args.latency_n, size=args.small_bytes, marker_every=50),
            workers_list=workers_list, chunk_size=args.chunk_size,
            latency_ms=args.latency_ms,
        )

    results["env"]["wall_clock_s"] = time.perf_counter() - t_start
    out_dir = ROOT / "bench-out"
    out_dir.mkdir(exist_ok=True)
    json_path = out_dir / f"{out_name}.json"
    md_path = out_dir / f"{out_name}.md"
    json_path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    md_path.write_text(_render(results, quick=args.quick))
    print(md_path.read_text())
    print(f"\nWrote {json_path} and {md_path}")
    return 0


def _select_pairs(all_pairs, spec: str):
    if spec == "all":
        return list(all_pairs)
    wanted = {s.strip() for s in spec.split(",") if s.strip()}
    return [pr for pr in all_pairs if f"{pr[0]}->{pr[1]}" in wanted]


def _sh(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return ""


def _env(args) -> dict:
    import importlib

    pkgs = {}
    for mod in ("fsspec", "s3fs", "gcsfs", "adlfs", "boto3", "aiohttp", "urllib3",
                "azure.storage.blob", "google.cloud.storage"):
        try:
            m = importlib.import_module(mod)
            pkgs[mod] = getattr(m, "__version__", getattr(m, "VERSION", "?"))
        except Exception:
            pkgs[mod] = "missing"
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
    images = {}
    for name in ("gtc-s3", "gtc-gcs", "gtc-azure"):
        images[name] = _sh(["docker", "inspect", "-f", "{{.Config.Image}}", name])
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_model": cpu,
        "cpu_count": os.cpu_count(),
        "mem_total_gb": mem_gb,
        "docker_server": _sh(["docker", "version", "--format", "{{.Server.Version}}"]),
        "emulator_images": images,
        "rounds": args.rounds,
        "size_mb": args.size_mb,
        "small_n": args.small_n,
        "small_bytes": args.small_bytes,
        "workers": args.workers,
        "chunk_size": args.chunk_size,
        "latency_ms": args.latency_ms,
        "latency_n": args.latency_n,
        "s3_backend": os.environ.get("EMULATOR_S3_BACKEND", "s3mock"),
        "packages": pkgs,
    }


def _wipe(emu, prefix: str) -> None:
    """Fast untimed cleanup of a destination prefix via native SDK."""
    from generic_transfer.adapters.emulators import _native_client, list_keys_native

    keys = list_keys_native(emu, prefix)
    if not keys:
        return
    client = _native_client(emu)
    if emu.flavour == "s3":
        for i in range(0, len(keys), 1000):
            client.delete_objects(
                Bucket=emu.bucket,
                Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]},
            )
        return
    if emu.flavour == "gcs":
        bucket = client.bucket(emu.bucket)
        fn = lambda k: bucket.blob(k).delete()  # noqa: E731
    else:
        container = client.get_container_client(emu.bucket)
        fn = lambda k: container.delete_blob(k)  # noqa: E731
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(fn, keys))


def _timed(fn, rounds: int, *, latency_ms: float = 0.0) -> dict:
    """Run ``fn(round_i)`` ``rounds`` times.

    ``fn`` returns ``(payload, cleanup)``; cleanup runs after timing and after
    latency injection is switched off.
    """
    from generic_transfer import wire

    samples, peaks, rss, http_totals = [], [], [], []
    last = {}
    for round_i in range(rounds):
        wire.reset()
        wire.set_latency_ms(latency_ms)
        tracemalloc.start()
        t0 = time.perf_counter()
        try:
            payload, cleanup = fn(round_i)
        finally:
            elapsed = time.perf_counter() - t0
            _cur, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            wire.set_latency_ms(0)
        http = wire.snapshot()
        payload = {k: v for k, v in payload.items() if k != "copied"}
        payload["seconds"] = elapsed
        payload["http_requests"] = http
        payload["http_total"] = sum(http.values())
        samples.append(elapsed)
        peaks.append(peak / (1024 * 1024))
        rss.append(_rss_mb())
        http_totals.append(payload["http_total"])
        last = payload
        if cleanup is not None:
            cleanup()
    return {
        "cold_s": samples[0],
        "warm_median_s": statistics.median(samples[1:] or samples),
        "median_s": statistics.median(samples),
        "samples_s": samples,
        "tracemalloc_peak_median_mb": statistics.median(peaks),
        "rss_max_mb": max(rss),
        "http_total_median": statistics.median(http_totals),
        "last": last,
    }


def _generic(src, dst, *, src_prefix, dest_prefix, mode, workers=1, chunk_size,
             listing="single"):
    from generic_transfer import GenericFileTransfer
    from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook

    params = {
        "source_fs": src.fs,
        "source_bucket": src.bucket,
        "source_prefix": src_prefix,
        "dest_prefix": dest_prefix,
        "mode": mode,
        "chunk_size": chunk_size,
        "listing": listing,
    }
    if mode == "parallel":
        params["workers"] = workers
    return GenericFileTransfer(
        destination_hook=ObjectStoreFileCopyHook(dst), copy_params=params
    ).execute()


def _bench_pairs(*, scenario, pairs, rounds, seed_fn, workers_list, chunk_size,
                 latency_ms) -> list:
    from generic_transfer.adapters.emulators import make_fs
    from generic_transfer.adapters.pair_baseline import naive_pair_copy

    out = []
    for src_fl, dst_fl in pairs:
        prefix = f"scale-{scenario}-{src_fl}-{dst_fl}"
        row = {"pair": f"{src_fl}->{dst_fl}", "modes": {}}
        src = make_fs(src_fl, bucket=SRC_BUCKET[src_fl])
        dst = make_fs(dst_fl, bucket=DST_BUCKET[dst_fl])
        seed_fn(src, prefix)

        def run_naive(round_i, _src=src, _dst=dst, _prefix=prefix):
            dp = f"naive-{_prefix}-r{round_i}"
            res = naive_pair_copy(_src, _dst, source_prefix=_prefix, dest_prefix=dp,
                                  skip_markers=False)
            return res, (lambda: _wipe(_dst, dp))

        def make_generic(mode, workers=1, listing="single", tag=""):
            def run(round_i, _src=src, _dst=dst, _prefix=prefix):
                dp = f"gen-{tag}-{_prefix}-r{round_i}"
                res = _generic(_src, _dst, src_prefix=_prefix, dest_prefix=dp,
                               mode=mode, workers=workers, chunk_size=chunk_size,
                               listing=listing)
                return res, (lambda: _wipe(_dst, dp))
            return run

        row["modes"]["naive_sequential"] = _timed(run_naive, rounds, latency_ms=latency_ms)
        row["modes"]["generic_sequential_per_key"] = _timed(
            make_generic("sequential", listing="per_key", tag="seqpk"), rounds,
            latency_ms=latency_ms)
        row["modes"]["generic_sequential"] = _timed(
            make_generic("sequential", tag="seq"), rounds, latency_ms=latency_ms)
        for w in workers_list:
            row["modes"][f"generic_parallel_{w}"] = _timed(
                make_generic("parallel", workers=w, tag=f"par{w}"), rounds,
                latency_ms=latency_ms)
        _wipe(src, prefix)
        out.append(row)
        print(f"  [{scenario}] {row['pair']} done", flush=True)
    return out


def _bench_large(*, pairs, rounds, size_mb, workers_list, chunk_size) -> list:
    from generic_transfer.adapters.emulators import make_fs, seed_one_large
    from generic_transfer.adapters.pair_baseline import naive_pair_copy

    out = []
    for src_fl, dst_fl in pairs:
        prefix = f"large-{src_fl}-{dst_fl}"
        row = {"pair": f"{src_fl}->{dst_fl}", "size_mb": size_mb, "modes": {}}
        src = make_fs(src_fl, bucket=SRC_BUCKET[src_fl])
        dst = make_fs(dst_fl, bucket=DST_BUCKET[dst_fl])
        seed_one_large(src, prefix, size_mb=size_mb)

        def run_naive(round_i, _src=src, _dst=dst, _prefix=prefix):
            dp = f"lnaive-{_prefix}-r{round_i}"
            res = naive_pair_copy(_src, _dst, source_prefix=_prefix, dest_prefix=dp)
            return res, (lambda: _wipe(_dst, dp))

        def run_gen(round_i, mode="sequential", workers=1, _src=src, _dst=dst, _prefix=prefix):
            dp = f"l{mode[:3]}{workers}-{_prefix}-r{round_i}"
            res = _generic(_src, _dst, src_prefix=_prefix, dest_prefix=dp, mode=mode,
                           workers=workers, chunk_size=chunk_size)
            return res, (lambda: _wipe(_dst, dp))

        row["modes"]["naive_sequential"] = _timed(run_naive, rounds)
        row["modes"]["generic_sequential"] = _timed(run_gen, rounds)
        w = workers_list[0]
        row["modes"][f"generic_parallel_{w}"] = _timed(
            lambda r, _w=w: run_gen(r, mode="parallel", workers=_w), rounds)
        _wipe(src, prefix)
        out.append(row)
        print(f"  [large {size_mb}MiB] {row['pair']} done", flush=True)
    return out


def _bench_server_side(*, pairs, rounds) -> list:
    from generic_transfer import GenericFileTransfer
    from generic_transfer.adapters.emulators import make_fs, seed_tree_with_markers
    from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook

    out = []
    for src_fl, dst_fl in pairs:
        prefix = f"ss-{src_fl}"
        row = {"pair": f"{src_fl}->{dst_fl}", "modes": {}}
        src = make_fs(src_fl, bucket=SRC_BUCKET[src_fl])
        dst = make_fs(dst_fl, bucket=DST_BUCKET[dst_fl])
        seed_tree_with_markers(src, prefix)

        def run_ss(round_i):
            dp = f"ss-out-{prefix}-r{round_i}"
            try:
                res = GenericFileTransfer(
                    destination_hook=ObjectStoreFileCopyHook(dst),
                    copy_params={"source_fs": src.fs, "source_prefix": prefix,
                                 "source_emu": src, "dest_prefix": dp,
                                 "mode": "server_side"},
                ).execute()
            except Exception as exc:
                res = {"error": f"{type(exc).__name__}: {exc}", "count": 0,
                       "request_total": 0, "mode": "server_side"}
            return res, (lambda: _wipe(dst, dp))

        def run_rw(round_i):
            dp = f"rw-out-{prefix}-r{round_i}"
            res = _generic(src, dst, src_prefix=prefix, dest_prefix=dp,
                           mode="sequential", chunk_size=None)
            return res, (lambda: _wipe(dst, dp))

        row["modes"]["server_side"] = _timed(run_ss, rounds)
        row["modes"]["read_write"] = _timed(run_rw, rounds)
        out.append(row)
        print(f"  [server_side] {row['pair']} done", flush=True)
    return out


SCENARIO_TITLES = {
    "mixed_tree": "Mixed tree (markers, unicode, spaces, 0-byte real file)",
    "small_objects": "Many small objects",
    "large_object": "One large object",
    "server_side_same_provider": "Same-provider server-side copy vs read/write",
    "wire_latency": "Small objects with injected per-request latency",
}


def _render(results: dict, *, quick: bool) -> str:
    env = results["env"]
    title = "Scale bench (QUICK run)" if quick else "Scale bench (full run)"
    lines = [
        f"# {title}",
        "",
        f"- Captured (UTC): `{env['timestamp_utc']}`; wall clock {env.get('wall_clock_s', 0) / 60:.1f} min",
        f"- Host: `{env['cpu_model']}` x{env['cpu_count']}, {env['mem_total_gb']} GiB RAM, "
        f"`{env['platform']}`, Python `{env['python']}`, Docker `{env['docker_server']}`",
        f"- Emulators: {', '.join(f'`{v}`' for v in env['emulator_images'].values() if v)}",
        f"- Packages: {', '.join(f'{k} `{v}`' for k, v in env['packages'].items())}",
        f"- Rounds: **{env['rounds']}** (round 0 = cold) | large object: **{env['size_mb']} MiB** | "
        f"small objects: **{env['small_n']} x {env['small_bytes']} B** | workers: `{env['workers']}` | "
        f"chunk_size: `{env['chunk_size']}`",
        f"- wire_latency: **{env['latency_ms']} ms** before every HTTP request, "
        f"**{env['latency_n']}** objects",
        "",
        f"> **Caveat:** {results['caveat']}",
        "",
        "Columns: *HTTP reqs* = real requests counted at the transport (median over rounds); "
        "*Logical* = the copier's own list/get/put tally (last round). Peak MB = tracemalloc "
        "peak (Python allocations only, median over rounds).",
        "",
    ]
    for name, payload in results["scenarios"].items():
        lines += [f"## `{name}`: {SCENARIO_TITLES.get(name, name)}", ""]
        lines += [
            "| Pair | Mode | Cold (s) | Warm med (s) | Median (s) | Objects | HTTP reqs | Logical | Peak MB |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for row in payload:
            for mode, stats in row["modes"].items():
                last = stats["last"]
                lines.append(
                    f"| `{row['pair']}` | {mode} | {stats['cold_s']:.3f} | "
                    f"{stats['warm_median_s']:.3f} | {stats['median_s']:.3f} | "
                    f"{last.get('count', last.get('error', '?'))} | "
                    f"{stats['http_total_median']:.0f} | "
                    f"{last.get('request_total', '?')} | "
                    f"{stats['tracemalloc_peak_median_mb']:.1f} |"
                )
        lines.append("")
    lines += [
        "## Reproduce",
        "",
        "```bash",
        "just up && just bench-scale          # full run -> bench-out/BENCH_SCALE.*",
        "just up && just bench-scale-quick    # quick run -> bench-out/BENCH_SCALE_QUICK.*",
        "```",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
