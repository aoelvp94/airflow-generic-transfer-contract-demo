"""Naive per-pair object copy using native cloud SDKs.

Mirrors the shape of today's ``S3ToGCSOperator`` / ``GCSToS3Operator`` /
``S3ToAzureBlobStorageOperator`` / ``AzureBlobStorageToS3Operator`` before the
folder-marker fixes: list every key under a prefix, copy each one. Zero-byte
folder markers are copied unless the caller filters them.

Request counting is best-effort (botocore events / simple counters) so benches
can compare per-pair vs generic paths.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable

from .emulators import EmulatorFS, Flavour, get_bytes, list_keys_native, put_bytes


@dataclass
class RequestCounter:
    """Simple mutable counter used by benches and tests."""

    counts: dict[str, int] = field(default_factory=dict)

    def bump(self, label: str, n: int = 1) -> None:
        self.counts[label] = self.counts.get(label, 0) + n

    @property
    def total(self) -> int:
        return sum(self.counts.values())


def naive_pair_copy(
    source: EmulatorFS,
    dest: EmulatorFS,
    *,
    source_prefix: str = "data",
    dest_prefix: str = "data",
    skip_markers: bool = False,
    is_marker: Callable[[EmulatorFS, str], bool] | None = None,
    counter: RequestCounter | None = None,
) -> dict[str, Any]:
    """Copy ``source_prefix`` → ``dest_prefix`` key-by-key via native SDKs.

    When ``skip_markers`` is False (default), behaves like the pre-fix pair
    operators and copies folder markers. When True, applies ``is_marker`` (or a
    size==0-and-endswith-/ heuristic) once — the rule the four PRs each re-did.
    """
    counter = counter or RequestCounter()
    keys = list_keys_native(source, source_prefix)
    counter.bump(f"list:{source.flavour}", 1)
    copied: list[str] = []
    skipped: list[str] = []
    for key in keys:
        if skip_markers:
            marker = False
            if is_marker is not None:
                # is_marker expects an fsspec-like fs + full path
                marker = is_marker(source.fs, f"{source.bucket}/{key}")
            else:
                marker = key.endswith("/") and _native_size(source, key) == 0
            if marker:
                skipped.append(key)
                continue
        data = get_bytes_native(source, key)
        counter.bump(f"get:{source.flavour}", 1)
        rel = key[len(source_prefix) :].lstrip("/") if key.startswith(source_prefix) else key
        dest_key = f"{dest_prefix.rstrip('/')}/{rel}" if rel else dest_prefix
        # Preserve trailing slash on markers.
        if key.endswith("/") and not dest_key.endswith("/"):
            dest_key += "/"
        put_bytes(dest, dest_key, data)
        counter.bump(f"put:{dest.flavour}", 1)
        copied.append(dest_key)
    return {
        "copied": copied,
        "skipped": skipped,
        "count": len(copied),
        "requests": dict(counter.counts),
        "request_total": counter.total,
        "pair": f"{source.flavour}->{dest.flavour}",
    }


def get_bytes_native(emu: EmulatorFS, key: str) -> bytes:
    key = key.lstrip("/")
    if emu.flavour == "s3":
        import boto3
        from botocore.client import Config

        opts = emu.storage_options
        client = boto3.client(
            "s3",
            endpoint_url=opts["client_kwargs"]["endpoint_url"],
            aws_access_key_id=opts["key"],
            aws_secret_access_key=opts["secret"],
            region_name=opts["client_kwargs"].get("region_name", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}),
        )
        return client.get_object(Bucket=emu.bucket, Key=key)["Body"].read()
    if emu.flavour == "gcs":
        from google.cloud import storage as gcs_storage

        client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
        return client.bucket(emu.bucket).blob(key).download_as_bytes()
    if emu.flavour == "azure":
        from azure.storage.blob import BlobServiceClient

        client = BlobServiceClient.from_connection_string(
            emu.storage_options["connection_string"]
        )
        return client.get_container_client(emu.bucket).download_blob(key).readall()
    raise ValueError(emu.flavour)


def _native_size(emu: EmulatorFS, key: str) -> int:
    return len(get_bytes_native(emu, key))


PAIRS: list[tuple[Flavour, Flavour]] = [
    ("s3", "gcs"),
    ("gcs", "s3"),
    ("s3", "azure"),
    ("azure", "s3"),
    ("gcs", "azure"),
    ("azure", "gcs"),
]

SAME_PROVIDER_PAIRS: list[tuple[Flavour, Flavour]] = [
    ("s3", "s3"),
    ("gcs", "gcs"),
    ("azure", "azure"),
]
