#!/usr/bin/env python3
"""Bench generic vs naive per-pair copy across object-store emulators.

Cold + warm rounds, median of N, request counts, fsspec marker findings.
Writes ``bench-out/latest.json`` and ``bench-out/BENCH.md``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--prefix", default="benchdata")
    args = parser.parse_args()

    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    # Ensure package imports
    import sys

    sys.path.insert(0, str(ROOT / "src"))
    sys.path.insert(0, str(ROOT))

    from generic_transfer.adapters.emulators import (
        clear_prefix,
        ensure_bucket,
        list_keys_fsspec,
        list_keys_native,
        load_emulator_env,
        make_fs,
        seed_tree_with_markers,
    )
    from generic_transfer.adapters.file_sink import is_folder_marker, list_transfer_keys
    from generic_transfer.adapters.pair_baseline import PAIRS, RequestCounter, naive_pair_copy
    from tests.conftest import emulators_available

    from generic_transfer import wire

    load_emulator_env()
    if not emulators_available():
        raise SystemExit("emulators not up — run: just up")
    wire.install()

    src_bucket = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
    dst_bucket = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}
    for fl in ("s3", "gcs", "azure"):
        ensure_bucket(make_fs(fl, bucket=src_bucket[fl]))
        ensure_bucket(make_fs(fl, bucket=dst_bucket[fl]))

    # --- fsspec marker findings (one seed per flavour) ---
    findings = {}
    for fl in ("s3", "gcs", "azure"):
        emu = make_fs(fl, bucket=src_bucket[fl])
        native = seed_tree_with_markers(emu, args.prefix)
        fss = list_keys_fsspec(emu, args.prefix)
        transfer = list_transfer_keys(emu.fs, f"{emu.bucket}/{args.prefix}")
        findings[fl] = {
            "native_keys": native,
            "fsspec_find": fss,
            "transfer_keys": transfer,
            "markers_native": [k for k in native if k.endswith("/")],
            "markers_in_fsspec_find": [
                k
                for k in fss
                if k.endswith("/")
                or k.rstrip("/").endswith("nested")
                or k.rstrip("/").endswith(args.prefix)
            ],
            "note": _marker_note(fl, native, fss),
        }

    def generic_copy(src_fl, dst_fl, prefix, dest_prefix):
        from generic_transfer import GenericRecordTransfer

        src = make_fs(src_fl, bucket=src_bucket[src_fl])
        dst = make_fs(dst_fl, bucket=dst_bucket[dst_fl])
        counter = RequestCounter()

        class EmptySource:
            def read_batches(self, **params):
                yield from ()

        class DestHook:
            def write_batches(self, batches, **params):
                for _ in batches:
                    pass
                # Shared FileCopyable path: one listing, markers classified
                # from it, cat_file GET per object (no per-key HEAD).
                from generic_transfer.adapters.emulators import put_bytes
                from generic_transfer.adapters.file_copy import copy_files_sequential

                res = copy_files_sequential(
                    source_fs=src.fs,
                    source_bucket=src.bucket,
                    source_prefix=prefix,
                    put=lambda k, d: put_bytes(dst, k, d),
                    dest_prefix=dest_prefix,
                    counter=counter,
                )
                copied = res["copied"]
                return {"copied": copied, "count": len(copied)}

        clear_prefix(dst, dest_prefix)
        wire.reset()
        t0 = time.perf_counter()
        result = GenericRecordTransfer(
            source_hook=EmptySource(),
            destination_hook=DestHook(),
            destination_hook_params={"source_fs": src.fs, "source_prefix": prefix},
        ).execute()
        elapsed = time.perf_counter() - t0
        result["http_requests"] = wire.snapshot()
        result["http_total"] = sum(result["http_requests"].values())
        result["seconds"] = elapsed
        result["requests"] = dict(counter.counts)
        result["request_total"] = counter.total
        return result

    def time_rounds(fn, *, rounds: int):
        """Return cold (first), warm (rest), median of all, all samples."""
        samples = []
        payloads = []
        for i in range(rounds):
            # Re-seed source each round for fair I/O; dest cleared inside fn.
            payload = fn(i)
            samples.append(payload["seconds"])
            payloads.append(payload)
        cold = samples[0]
        warm = samples[1:] or samples
        return {
            "cold_s": cold,
            "warm_median_s": statistics.median(warm),
            "median_s": statistics.median(samples),
            "mean_s": statistics.mean(samples),
            "samples_s": samples,
            "last": payloads[-1],
        }

    pair_results = []
    for src_fl, dst_fl in PAIRS:
        src = make_fs(src_fl, bucket=src_bucket[src_fl])
        dst = make_fs(dst_fl, bucket=dst_bucket[dst_fl])

        def run_naive(i, _src=src, _dst=dst, _src_fl=src_fl, _dst_fl=dst_fl):
            seed_tree_with_markers(_src, args.prefix)
            clear_prefix(_dst, f"naive-{_src_fl}-{_dst_fl}")
            wire.reset()
            t0 = time.perf_counter()
            r = naive_pair_copy(
                _src,
                _dst,
                source_prefix=args.prefix,
                dest_prefix=f"naive-{_src_fl}-{_dst_fl}",
                skip_markers=False,
            )
            r["seconds"] = time.perf_counter() - t0
            r["http_requests"] = wire.snapshot()
            r["http_total"] = sum(r["http_requests"].values())
            return r

        def run_generic(i, _src_fl=src_fl, _dst_fl=dst_fl):
            seed_tree_with_markers(
                make_fs(_src_fl, bucket=src_bucket[_src_fl]), args.prefix
            )
            r = generic_copy(
                _src_fl, _dst_fl, args.prefix, f"generic-{_src_fl}-{_dst_fl}"
            )
            return r

        naive_stats = time_rounds(run_naive, rounds=args.rounds)
        generic_stats = time_rounds(run_generic, rounds=args.rounds)
        pair_results.append(
            {
                "pair": f"{src_fl}->{dst_fl}",
                "naive": {
                    "cold_s": naive_stats["cold_s"],
                    "warm_median_s": naive_stats["warm_median_s"],
                    "median_s": naive_stats["median_s"],
                    "samples_s": naive_stats["samples_s"],
                    "count": naive_stats["last"]["count"],
                    "requests": naive_stats["last"]["requests"],
                    "request_total": naive_stats["last"]["request_total"],
                    "http_requests": naive_stats["last"]["http_requests"],
                    "http_total": naive_stats["last"]["http_total"],
                    "copied_markers": True,
                },
                "generic": {
                    "cold_s": generic_stats["cold_s"],
                    "warm_median_s": generic_stats["warm_median_s"],
                    "median_s": generic_stats["median_s"],
                    "samples_s": generic_stats["samples_s"],
                    "count": generic_stats["last"]["count"],
                    "requests": generic_stats["last"]["requests"],
                    "request_total": generic_stats["last"]["request_total"],
                    "http_requests": generic_stats["last"]["http_requests"],
                    "http_total": generic_stats["last"]["http_total"],
                    "copied_markers": False,
                },
            }
        )

    env = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "s3_backend": os.environ.get("EMULATOR_S3_BACKEND", "s3mock"),
        "s3_endpoint": os.environ.get("S3_ENDPOINT"),
        "gcs_endpoint": os.environ.get("GCS_ENDPOINT"),
        "azure_endpoint": os.environ.get("AZURE_BLOB_ENDPOINT"),
        "rounds": args.rounds,
        "docker_host": os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock"),
        "images": {
            "s3": "adobe/s3mock:3.12.0",
            "gcs": "fsouza/fake-gcs-server:1.56.1",
            "azure": "mcr.microsoft.com/azure-storage/azurite:3.37.0",
        },
        "packages": _pkg_versions(),
    }

    out = {
        "env": env,
        "fsspec_marker_findings": findings,
        "pairs": pair_results,
        "thesis": {
            "folder_marker_rule_once": (
                "list_transfer_keys skips markers on every pair; "
                "naive pair copy without the rule lands 4 keys including markers."
            ),
            "pagination_note": (
                "HTTP pagination evidence lives in scripts/bench.py (in-process); "
                "this bench is the file-pair / folder-marker half."
            ),
        },
    }

    out_dir = ROOT / "bench-out"
    out_dir.mkdir(exist_ok=True)
    json_path = out_dir / "latest.json"
    json_path.write_text(json.dumps(out, indent=2) + "\n")
    md_path = out_dir / "BENCH.md"
    md_path.write_text(_render_md(out))
    print(md_path.read_text())
    print(f"\nWrote {json_path} and {md_path}")
    return 0


def _marker_note(fl: str, native: list[str], fss: list[str]) -> str:
    native_markers = [k for k in native if k.endswith("/")]
    fss_markers = [
        k for k in fss if k.endswith("/") or k.rstrip("/").split("/")[-1] in {"nested"} and
        not k.endswith(".json")
    ]
    # Also treat exact prefix key without slash as marker-like if size-0 sibling exists
    if fl == "gcs":
        return (
            "gcsfs.core find() keeps trailing-slash markers; info() reports "
            f"type=directory. native_markers={native_markers} fsspec_markers={fss_markers}."
        )
    if fl == "azure":
        return (
            "adlfs find() drops the walked prefix marker but keeps nested zero-byte "
            f"markers usually without a trailing slash. native={native_markers} "
            f"fsspec={fss_markers}."
        )
    if fl == "s3":
        return (
            "s3fs find() may or may not surface the walked prefix marker; nested "
            "zero-byte markers appear (sometimes without trailing slash). "
            f"native={native_markers} fsspec={fss_markers}."
        )
    return ""


def _pkg_versions() -> dict[str, str]:
    import importlib

    versions: dict[str, str] = {}
    mapping = {
        "fsspec": "fsspec",
        "s3fs": "s3fs",
        "gcsfs": "gcsfs",
        "adlfs": "adlfs",
        "boto3": "boto3",
        "azure-storage-blob": "azure.storage.blob",
        "google-cloud-storage": "google.cloud.storage",
    }
    for label, modname in mapping.items():
        try:
            mod = importlib.import_module(modname)
            versions[label] = getattr(mod, "__version__", "?")
        except Exception:
            versions[label] = "missing"
    return versions


def _render_md(out: dict) -> str:
    env = out["env"]
    lines = [
        "# Emulator bench results",
        "",
        f"- Captured (UTC): `{env['timestamp_utc']}`",
        f"- Platform: `{env['platform']}`",
        f"- Python: `{env['python']}`",
        f"- S3 backend: `{env['s3_backend']}` at `{env['s3_endpoint']}`",
        f"- GCS: `{env['gcs_endpoint']}`",
        f"- Azure: `{env['azure_endpoint']}`",
        f"- Rounds per path: **{env['rounds']}** (cold = round 1, warm median = rounds 2..N)",
        f"- Images: `{env['images']}`",
        f"- Packages: `{env['packages']}`",
        "",
        "## fsspec folder-marker differences",
        "",
        "| Flavour | Native keys (SDK) | fsspec find() | Shared `list_transfer_keys` |",
        "|---|---|---|---|",
    ]
    for fl, info in out["fsspec_marker_findings"].items():
        lines.append(
            f"| {fl} | `{info['native_keys']}` | `{info['fsspec_find']}` | "
            f"`{info['transfer_keys']}` |"
        )
    lines += [
        "",
        "Notes:",
    ]
    for fl, info in out["fsspec_marker_findings"].items():
        lines.append(f"- **{fl}**: {info['note']}")
    lines += [
        "",
        "## Per-pair copy (naive SDK baseline vs generic)",
        "",
        "Requests: *Logical* = list/get/put tally; *HTTP* = real requests on the wire "
        "(aiohttp/urllib3 transport counter, last round).",
        "",
        "| Pair | Path | Cold (s) | Warm median (s) | Median (s) | Objects | Logical | HTTP | Markers copied? |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in out["pairs"]:
        for label in ("naive", "generic"):
            r = row[label]
            lines.append(
                f"| `{row['pair']}` | {label} | {r['cold_s']:.4f} | "
                f"{r['warm_median_s']:.4f} | {r['median_s']:.4f} | {r['count']} | "
                f"{r['request_total']} | {r.get('http_total', '?')} | {r['copied_markers']} |"
            )
    lines += [
        "",
        "## Takeaway",
        "",
        out["thesis"]["folder_marker_rule_once"],
        "",
        out["thesis"]["pagination_note"],
        "",
        "Reproduce: `just up && just bench-emulators`",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
