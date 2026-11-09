"""File-copy contract implementations (separate from record write_batches).

Provides sequential, bounded-parallel, streaming, and same-provider
server-side copy paths used by GenericFileTransfer and the large-scale bench.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

from generic_transfer.adapters.file_sink import list_transfer_keys


@dataclass
class RequestCounter:
    counts: dict[str, int] = field(default_factory=dict)

    def bump(self, label: str, n: int = 1) -> None:
        self.counts[label] = self.counts.get(label, 0) + n

    @property
    def total(self) -> int:
        return sum(self.counts.values())


@dataclass
class LatencyInjector:
    """Wrap get/put callables with a fixed per-request delay (simulated RTT)."""

    delay_s: float = 0.0
    counter: RequestCounter | None = None

    def wrap(self, label: str, fn: Callable[..., Any]) -> Callable[..., Any]:
        delay = self.delay_s
        counter = self.counter

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if delay:
                time.sleep(delay)
            if counter is not None:
                counter.bump(label, 1)
            return fn(*args, **kwargs)

        return wrapped


def _rel_key(full_key: str, source_prefix: str, bucket: str | None = None) -> str:
    key = full_key
    if bucket and key.startswith(bucket + "/"):
        key = key[len(bucket) + 1 :]
    if source_prefix and key.startswith(source_prefix):
        key = key[len(source_prefix) :].lstrip("/")
    return key


def copy_files_sequential(
    *,
    source_fs: Any,
    source_prefix: str,
    put: Callable[[str, bytes], None],
    source_bucket: str | None = None,
    dest_prefix: str = "",
    chunk_size: int | None = None,
    resume_from: set[str] | None = None,
    counter: RequestCounter | None = None,
    get_open: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """List (skipping markers) and copy key-by-key, optionally streaming chunks."""
    counter = counter or RequestCounter()
    base = (
        f"{source_bucket}/{source_prefix}".rstrip("/")
        if source_bucket
        else source_prefix
    )
    keys = list_transfer_keys(source_fs, base)
    counter.bump("list", 1)
    copied: list[str] = []
    skipped: list[str] = []
    resume_from = resume_from or set()
    for full_key in keys:
        rel = _rel_key(full_key, source_prefix, source_bucket)
        dest_key = f"{dest_prefix.rstrip('/')}/{rel}" if dest_prefix else rel
        if dest_key in resume_from or rel in resume_from:
            skipped.append(dest_key)
            continue
        if get_open is not None:
            fh = get_open(full_key)
        else:
            fh = source_fs.open(full_key, "rb")
        try:
            if chunk_size and chunk_size > 0:
                buf = bytearray()
                while True:
                    piece = fh.read(chunk_size)
                    if not piece:
                        break
                    buf.extend(piece)
                data = bytes(buf)
            else:
                data = fh.read()
        finally:
            fh.close()
        counter.bump("get", 1)
        put(dest_key, data)
        counter.bump("put", 1)
        copied.append(dest_key)
    return {
        "copied": copied,
        "skipped": skipped,
        "count": len(copied),
        "requests": dict(counter.counts),
        "request_total": counter.total,
        "mode": "sequential",
    }


def copy_files_parallel(
    *,
    source_fs: Any,
    source_prefix: str,
    put: Callable[[str, bytes], None],
    source_bucket: str | None = None,
    dest_prefix: str = "",
    workers: int = 8,
    chunk_size: int | None = None,
    resume_from: set[str] | None = None,
    counter: RequestCounter | None = None,
) -> dict[str, Any]:
    """Bounded thread-pool copy. Request counting is approximate under threads."""
    counter = counter or RequestCounter()
    base = (
        f"{source_bucket}/{source_prefix}".rstrip("/")
        if source_bucket
        else source_prefix
    )
    keys = list_transfer_keys(source_fs, base)
    counter.bump("list", 1)
    resume_from = resume_from or set()
    work: list[tuple[str, str]] = []
    skipped: list[str] = []
    for full_key in keys:
        rel = _rel_key(full_key, source_prefix, source_bucket)
        dest_key = f"{dest_prefix.rstrip('/')}/{rel}" if dest_prefix else rel
        if dest_key in resume_from or rel in resume_from:
            skipped.append(dest_key)
            continue
        work.append((full_key, dest_key))

    copied: list[str] = []

    def _one(item: tuple[str, str]) -> str:
        full_key, dest_key = item
        with source_fs.open(full_key, "rb") as fh:
            if chunk_size and chunk_size > 0:
                buf = bytearray()
                while True:
                    piece = fh.read(chunk_size)
                    if not piece:
                        break
                    buf.extend(piece)
                data = bytes(buf)
            else:
                data = fh.read()
        counter.bump("get", 1)
        put(dest_key, data)
        counter.bump("put", 1)
        return dest_key

    workers = max(1, int(workers))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_one, item) for item in work]
        for fut in as_completed(futures):
            copied.append(fut.result())
    return {
        "copied": sorted(copied),
        "skipped": skipped,
        "count": len(copied),
        "requests": dict(counter.counts),
        "request_total": counter.total,
        "mode": f"parallel:{workers}",
    }


def server_side_copy_same_provider(
    *,
    flavour: str,
    source_emu: Any,
    dest_emu: Any,
    source_prefix: str,
    dest_prefix: str,
    counter: RequestCounter | None = None,
) -> dict[str, Any]:
    """Same-provider server-side copy where the emulator supports it.

    S3: copy_object. GCS: rewrite / copy_blob. Azure: start_copy_from_url.
    Falls back to raising NotImplementedError when unsupported.
    """
    from generic_transfer.adapters.emulators import list_keys_native
    from generic_transfer.adapters.file_sink import is_folder_marker

    counter = counter or RequestCounter()
    keys = list_keys_native(source_emu, source_prefix)
    counter.bump("list", 1)
    copied: list[str] = []
    skipped: list[str] = []
    for key in keys:
        # Skip markers via size/children on fsspec view when possible.
        full = f"{source_emu.bucket}/{key}"
        try:
            if is_folder_marker(source_emu.fs, full):
                skipped.append(key)
                continue
        except Exception:
            if key.endswith("/") :
                skipped.append(key)
                continue
        rel = key[len(source_prefix) :].lstrip("/") if key.startswith(source_prefix) else key
        dest_key = f"{dest_prefix.rstrip('/')}/{rel}" if dest_prefix else rel
        if flavour == "s3":
            import boto3
            from botocore.client import Config

            opts = source_emu.storage_options
            client = boto3.client(
                "s3",
                endpoint_url=opts["client_kwargs"]["endpoint_url"],
                aws_access_key_id=opts["key"],
                aws_secret_access_key=opts["secret"],
                region_name=opts["client_kwargs"].get("region_name", "us-east-1"),
                config=Config(s3={"addressing_style": "path"}),
            )
            client.copy_object(
                Bucket=dest_emu.bucket,
                Key=dest_key,
                CopySource={"Bucket": source_emu.bucket, "Key": key},
            )
            counter.bump("copy_object", 1)
        elif flavour == "gcs":
            from google.cloud import storage as gcs_storage
            import os

            client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
            src_blob = client.bucket(source_emu.bucket).blob(key)
            dst_bucket = client.bucket(dest_emu.bucket)
            dst_bucket.copy_blob(src_blob, dst_bucket, new_name=dest_key)
            counter.bump("copy_blob", 1)
        elif flavour == "azure":
            from azure.storage.blob import BlobServiceClient

            client = BlobServiceClient.from_connection_string(
                source_emu.storage_options["connection_string"]
            )
            src = client.get_blob_client(source_emu.bucket, key)
            dst = client.get_blob_client(dest_emu.bucket, dest_key)
            dst.start_copy_from_url(src.url)
            counter.bump("start_copy_from_url", 1)
        else:
            raise NotImplementedError(flavour)
        copied.append(dest_key)
    return {
        "copied": copied,
        "skipped": skipped,
        "count": len(copied),
        "requests": dict(counter.counts),
        "request_total": counter.total,
        "mode": f"server_side:{flavour}",
    }


class ObjectStoreFileCopyHook:
    """FileCopyable destination backed by an EmulatorFS or put callable."""

    def __init__(
        self,
        dest_emu: Any | None = None,
        *,
        put: Callable[[str, bytes], None] | None = None,
        lineage_dataset: str | None = None,
    ) -> None:
        self.dest_emu = dest_emu
        self._put = put
        self.lineage_dataset = lineage_dataset

    def describe_params(self, role: str = "copy") -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "source_fs": {"type": "object"},
                "source_prefix": {"type": "string"},
                "source_bucket": {"type": "string"},
                "dest_prefix": {"type": "string"},
                "workers": {"type": "integer"},
                "chunk_size": {"type": "integer"},
                "resume_from": {"type": "array"},
                "mode": {"type": "string"},
                "source_emu": {"type": "object"},
                "latency_s": {"type": "number"},
            },
            "required": ["source_fs", "source_prefix"],
            "additionalProperties": False,
        }

    def get_lineage_datasets(self, **params: Any) -> list[Any]:
        ds = self.lineage_dataset or (
            f"{self.dest_emu.flavour}://{self.dest_emu.bucket}/{params.get('dest_prefix', '')}"
            if self.dest_emu is not None
            else "file://destination"
        )
        return [{"uri": ds, "role": "outlet"}]

    def copy_files(self, **params: Any) -> dict[str, Any]:
        from generic_transfer.adapters.emulators import put_bytes

        source_fs = params["source_fs"]
        source_prefix = params["source_prefix"]
        source_bucket = params.get("source_bucket")
        dest_prefix = params.get("dest_prefix", "")
        workers = int(params.get("workers") or 1)
        chunk_size = params.get("chunk_size")
        resume_from = set(params.get("resume_from") or [])
        mode = params.get("mode") or ("parallel" if workers > 1 else "sequential")
        latency_s = float(params.get("latency_s") or 0.0)
        counter = RequestCounter()

        if self._put is not None:
            put = self._put
        elif self.dest_emu is not None:
            def put(key: str, data: bytes, _emu=self.dest_emu) -> None:
                put_bytes(_emu, key, data)
        else:
            raise ValueError("dest_emu or put is required")

        if latency_s > 0:
            injector = LatencyInjector(delay_s=latency_s, counter=None)
            real_put = put

            def put(key: str, data: bytes, _p=real_put, _i=injector) -> None:  # noqa: F811
                time.sleep(_i.delay_s)
                _p(key, data)

        if mode == "server_side":
            source_emu = params.get("source_emu")
            if source_emu is None or self.dest_emu is None:
                raise ValueError("server_side mode needs source_emu and dest_emu")
            if source_emu.flavour != self.dest_emu.flavour:
                raise ValueError("server_side copy requires same provider")
            return server_side_copy_same_provider(
                flavour=source_emu.flavour,
                source_emu=source_emu,
                dest_emu=self.dest_emu,
                source_prefix=source_prefix,
                dest_prefix=dest_prefix,
                counter=counter,
            )
        if mode == "parallel" or workers > 1:
            return copy_files_parallel(
                source_fs=source_fs,
                source_prefix=source_prefix,
                put=put,
                source_bucket=source_bucket,
                dest_prefix=dest_prefix,
                workers=workers,
                chunk_size=chunk_size,
                resume_from=resume_from,
                counter=counter,
            )
        # sequential with optional latency on get via monkeypatch-style wrapper
        if latency_s > 0:
            real_open = source_fs.open

            class _FSProxy:
                def __init__(self, fs, delay):
                    self._fs = fs
                    self._delay = delay

                def open(self, *a, **k):
                    time.sleep(self._delay)
                    return self._fs.open(*a, **k)

                def __getattr__(self, name):
                    return getattr(self._fs, name)

            source_fs = _FSProxy(source_fs, latency_s)
        return copy_files_sequential(
            source_fs=source_fs,
            source_prefix=source_prefix,
            put=put,
            source_bucket=source_bucket,
            dest_prefix=dest_prefix,
            chunk_size=chunk_size,
            resume_from=resume_from,
            counter=counter,
        )
