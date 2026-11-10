#!/usr/bin/env python3
"""Probe: cost of building a native SDK client per GET vs reusing one.

Before 06 the naive baseline (``get_bytes_native``) built a new boto3 /
google-cloud-storage / BlobServiceClient per object. This measures the median
time of a 4 KiB GET with a fresh client vs the cached client, per provider,
to quantify how much that biased the old naive numbers (and why the bias was
largest for S3 and GCS sources and smallest for Azure).

Writes bench-out/CLIENT_COST.{json,md}.
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main(samples: int = 50) -> int:
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    import boto3
    from azure.storage.blob import BlobServiceClient
    from botocore.client import Config
    from google.cloud import storage as gcs_storage

    from generic_transfer.adapters.emulators import ensure_bucket, load_emulator_env, make_fs, put_bytes
    from generic_transfer.adapters.pair_baseline import get_bytes_native

    load_emulator_env()
    out = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "samples": samples, "results": {}}
    for fl, bucket in (("s3", "src-bucket"), ("gcs", "src-bucket"), ("azure", "src-container")):
        emu = make_fs(fl, bucket=bucket)
        ensure_bucket(emu)
        put_bytes(emu, "client-cost/x.bin", b"x" * 4096)

        def fresh():
            if fl == "s3":
                o = emu.storage_options
                c = boto3.client(
                    "s3", endpoint_url=o["client_kwargs"]["endpoint_url"],
                    aws_access_key_id=o["key"], aws_secret_access_key=o["secret"],
                    region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
                return c.get_object(Bucket=bucket, Key="client-cost/x.bin")["Body"].read()
            if fl == "gcs":
                c = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
                return c.bucket(bucket).blob("client-cost/x.bin").download_as_bytes()
            c = BlobServiceClient.from_connection_string(emu.storage_options["connection_string"])
            return c.get_container_client(bucket).download_blob("client-cost/x.bin").readall()

        def cached():
            return get_bytes_native(emu, "client-cost/x.bin")

        row = {}
        for name, fn in (("fresh_client_ms", fresh), ("cached_client_ms", cached)):
            fn()
            ts = []
            for _ in range(samples):
                t0 = time.perf_counter()
                fn()
                ts.append((time.perf_counter() - t0) * 1000)
            row[name] = statistics.median(ts)
        row["overhead_ms"] = row["fresh_client_ms"] - row["cached_client_ms"]
        out["results"][fl] = row
        print(fl, row, flush=True)

    bench = ROOT / "bench-out"
    (bench / "CLIENT_COST.json").write_text(json.dumps(out, indent=2) + "\n")
    lines = ["# Native SDK client construction cost per GET", "",
             f"- Captured (UTC): `{out['timestamp_utc']}`; median of {samples}; 4 KiB object", "",
             "| Provider | New client + GET (ms) | Cached client GET (ms) | Overhead per object (ms) |",
             "|---|---:|---:|---:|"]
    for fl, r in out["results"].items():
        lines.append(f"| {fl} | {r['fresh_client_ms']:.1f} | {r['cached_client_ms']:.1f} | {r['overhead_ms']:.1f} |")
    (bench / "CLIENT_COST.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
