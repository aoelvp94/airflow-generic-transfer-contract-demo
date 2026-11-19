"""Build fsspec filesystems pointed at local object-store emulators.

Each flavour returns an :class:`EmulatorFS` with a configured fsspec handle.
Connection details live in :mod:`generic_transfer.adapters.systems` (one
module per system). This module loads ``emulators/emulators.env``, builds
the filesystem, and owns seeding / listing helpers. Callers stay
flavour-agnostic.

Notes discovered while wiring the emulators (see README findings table):

* ``gcsfs`` 2026.10+ re-exports ``ExtendedGcsFileSystem`` (gRPC). That client
  cannot talk to fake-gcs-server's HTTP JSON API, so we pin the call site to
  ``gcsfs.core.GCSFileSystem``.
* ``s3fs`` / ``adlfs`` ``find()`` drop the walked prefix marker (``data/``) but
  keep nested zero-byte markers, usually **without** a trailing slash
  (``data/nested``). ``gcsfs.core`` keeps both markers **with** trailing
  slashes and reports ``type=directory``.
* ``is_folder_marker`` / ``list_transfer_keys`` still collapse all three to the
  same two real keys — evidence that the rule belongs in one shared place.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

# Re-exported under the old private names. Shim code imports these from here.
from .systems.azure import storage_options as _azure_storage_options  # noqa: F401
from .systems.gcs import storage_options as _gcs_storage_options  # noqa: F401
from .systems.s3 import storage_options as _s3_storage_options  # noqa: F401

Flavour = Literal["s3", "gcs", "azure"]


def _strip_quotes(val: str) -> str:
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        return val[1:-1]
    return val


def load_emulator_env(path: str | Path | None = None) -> None:
    """Load ``emulators/emulators.env`` into ``os.environ`` (does not override existing)."""
    if path is None:
        path = Path(__file__).resolve().parents[3] / "emulators" / "emulators.env"
    path = Path(path)
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), _strip_quotes(val.strip()))


@dataclass(frozen=True)
class EmulatorFS:
    flavour: Flavour
    fs: Any
    bucket: str
    root_url: str
    endpoint: str
    storage_options: dict[str, Any]


def make_fs(flavour: Flavour, *, bucket: str | None = None) -> EmulatorFS:
    """Return a configured fsspec filesystem for ``flavour``."""
    load_emulator_env()
    if flavour == "s3":
        from .systems.s3 import open_filesystem
    elif flavour == "gcs":
        from .systems.gcs import open_filesystem
    elif flavour == "azure":
        from .systems.azure import open_filesystem
    else:
        raise ValueError(f"unknown flavour: {flavour!r}")
    fs, name, root_url, endpoint, opts = open_filesystem(bucket=bucket)
    return EmulatorFS(
        flavour=flavour,
        fs=fs,
        bucket=name,
        root_url=root_url,
        endpoint=endpoint,
        storage_options=opts,
    )


def ensure_bucket(emu: EmulatorFS) -> None:
    """Create the bucket/container if the emulator does not already have it."""
    if emu.flavour == "azure":
        from azure.storage.blob import BlobServiceClient

        client = BlobServiceClient.from_connection_string(
            emu.storage_options["connection_string"]
        )
        try:
            client.create_container(emu.bucket)
        except Exception:
            pass
        return
    if emu.flavour == "gcs":
        from google.cloud import storage as gcs_storage

        client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
        try:
            client.create_bucket(emu.bucket)
        except Exception:
            pass
        return
    # s3
    try:
        if not emu.fs.exists(emu.bucket):
            emu.fs.mkdir(emu.bucket)
    except Exception:
        try:
            emu.fs.mkdir(emu.bucket)
        except Exception:
            pass


def clear_prefix(emu: EmulatorFS, prefix: str) -> None:
    """Delete every key under ``bucket/prefix`` (best-effort)."""
    base = f"{emu.bucket}/{prefix}".rstrip("/")
    try:
        emu.fs.invalidate_cache()
    except Exception:
        pass
    try:
        if emu.fs.exists(base):
            emu.fs.rm(base, recursive=True)
    except FileNotFoundError:
        return
    except Exception:
        try:
            for key in list(emu.fs.find(base)):
                try:
                    emu.fs.rm(key)
                except Exception:
                    pass
        except Exception:
            return
    # Native SDK sweep for markers fsspec may not delete.
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
        token = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": emu.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            objs = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
            if objs:
                client.delete_objects(Bucket=emu.bucket, Delete={"Objects": objs})
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    elif emu.flavour == "gcs":
        from google.cloud import storage as gcs_storage

        client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
        bucket = client.bucket(emu.bucket)
        for blob in bucket.list_blobs(prefix=prefix):
            blob.delete()
    elif emu.flavour == "azure":
        from azure.storage.blob import BlobServiceClient

        client = BlobServiceClient.from_connection_string(
            emu.storage_options["connection_string"]
        )
        container = client.get_container_client(emu.bucket)
        for blob in container.list_blobs(name_starts_with=prefix):
            container.delete_blob(blob.name)


_client_cache: dict[str, Any] = {}

#: HTTP connection pool size for cached native clients. The default (10) is
#: below the 16-worker parallel mode, which made urllib3 discard connections
#: ("Connection pool is full") and reconnect on every PUT.
NATIVE_POOL_SIZE = 32


def _widen_requests_pool(session: Any) -> None:
    import requests.adapters

    for scheme in ("http://", "https://"):
        session.mount(
            scheme,
            requests.adapters.HTTPAdapter(
                pool_connections=NATIVE_POOL_SIZE, pool_maxsize=NATIVE_POOL_SIZE
            ),
        )


def _native_client(emu: EmulatorFS):
    """Reuse native SDK clients per flavour+endpoint (seed/bench hot path)."""
    if emu.flavour == "s3":
        key = f"s3:{emu.storage_options['client_kwargs']['endpoint_url']}"
        if key not in _client_cache:
            import boto3
            from botocore.client import Config

            opts = emu.storage_options
            _client_cache[key] = boto3.client(
                "s3",
                endpoint_url=opts["client_kwargs"]["endpoint_url"],
                aws_access_key_id=opts["key"],
                aws_secret_access_key=opts["secret"],
                region_name=opts["client_kwargs"].get("region_name", "us-east-1"),
                config=Config(
                    s3={"addressing_style": "path"},
                    max_pool_connections=NATIVE_POOL_SIZE,
                ),
            )
        return _client_cache[key]
    if emu.flavour == "gcs":
        key = f"gcs:{os.environ.get('STORAGE_EMULATOR_HOST', '')}"
        if key not in _client_cache:
            from google.cloud import storage as gcs_storage

            client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
            _widen_requests_pool(client._http)
            _client_cache[key] = client
        return _client_cache[key]
    if emu.flavour == "azure":
        key = f"azure:{emu.storage_options.get('connection_string', '')[:40]}"
        if key not in _client_cache:
            from azure.storage.blob import BlobServiceClient

            import requests
            from azure.core.pipeline.transport import RequestsTransport

            session = requests.Session()
            _widen_requests_pool(session)
            _client_cache[key] = BlobServiceClient.from_connection_string(
                emu.storage_options["connection_string"],
                transport=RequestsTransport(session=session, session_owner=False),
            )
        return _client_cache[key]
    raise ValueError(emu.flavour)


# S3 rejects every multipart part except the last when it is under 5 MiB.
_S3_MIN_PART = 5 * 1024 * 1024


class _UploadSink:
    """Writable sink that uploads one object without holding it all.

    A single ``write_all`` (the ``cat_file`` path for a known-small object) is
    one ``put_bytes``. Streamed ``write`` calls stay in a buffer until
    ``chunk_size`` (or 5 MiB on S3) and then flush to a multipart / block /
    resumable upload. ``close`` sends a short tail with one PUT when the
    object never grew past that threshold.
    """

    def __init__(self, emu: EmulatorFS, key: str, *, chunk_size: int) -> None:
        self.emu = emu
        self.key = key.lstrip("/")
        self.chunk_size = max(1, int(chunk_size))
        self._promote_at = self.chunk_size
        if emu.flavour == "s3":
            self._promote_at = max(self.chunk_size, _S3_MIN_PART)
        self._pending = bytearray()
        self._oneshot: bytes | None = None
        self._stream: Any = None
        self._closed = False

    def write_all(self, data: bytes) -> None:
        self._oneshot = bytes(data)

    def write(self, data: bytes) -> int:
        if not data:
            return 0
        if self._stream is not None:
            self._stream.write(data)
            return len(data)
        self._pending.extend(data)
        if len(self._pending) >= self._promote_at:
            self._start_stream()
        return len(data)

    def _start_stream(self) -> None:
        pending = bytes(self._pending)
        self._pending.clear()
        client = _native_client(self.emu)
        if self.emu.flavour == "s3":
            self._stream = _S3Multipart(client, self.emu.bucket, self.key, self._promote_at)
        elif self.emu.flavour == "gcs":
            self._stream = _GcsStream(client, self.emu.bucket, self.key, self.chunk_size)
        elif self.emu.flavour == "azure":
            self._stream = _AzureBlockStream(client, self.emu.bucket, self.key)
        else:
            raise ValueError(self.emu.flavour)
        if pending:
            self._stream.write(pending)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._oneshot is not None:
            put_bytes(self.emu, self.key, self._oneshot)
            return
        if self._stream is None:
            put_bytes(self.emu, self.key, bytes(self._pending))
            self._pending.clear()
            return
        if self._pending:
            self._stream.write(bytes(self._pending))
            self._pending.clear()
        self._stream.close()

    def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        stream = self._stream
        self._stream = None
        if stream is not None:
            stream.abort()


class _S3Multipart:
    def __init__(self, client: Any, bucket: str, key: str, part_size: int) -> None:
        self._client = client
        self._bucket = bucket
        self._key = key
        self._part_size = part_size
        self._buf = bytearray()
        resp = client.create_multipart_upload(Bucket=bucket, Key=key)
        self._uid = resp["UploadId"]
        self._parts: list[dict[str, Any]] = []
        self._n = 1
        self._done = False

    def write(self, data: bytes) -> None:
        self._buf.extend(data)
        while len(self._buf) >= self._part_size:
            self._upload(self._part_size)

    def _upload(self, n: int) -> None:
        body = bytes(self._buf[:n])
        del self._buf[:n]
        resp = self._client.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._uid,
            PartNumber=self._n,
            Body=body,
        )
        self._parts.append({"ETag": resp["ETag"], "PartNumber": self._n})
        self._n += 1

    def close(self) -> None:
        if self._buf:
            self._upload(len(self._buf))
        self._client.complete_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._uid,
            MultipartUpload={"Parts": self._parts},
        )
        self._done = True

    def abort(self) -> None:
        if self._done:
            return
        self._client.abort_multipart_upload(Bucket=self._bucket, Key=self._key, UploadId=self._uid)
        self._done = True


class _GcsStream:
    def __init__(self, client: Any, bucket: str, key: str, chunk_size: int) -> None:
        blob = client.bucket(bucket).blob(key)
        # Resumable chunks must be a multiple of 256 KiB.
        step = max(256 * 1024, int(chunk_size))
        step = ((step + 256 * 1024 - 1) // (256 * 1024)) * (256 * 1024)
        blob.chunk_size = step
        self._fh = blob.open("wb", chunk_size=step)
        self._done = False

    def write(self, data: bytes) -> None:
        self._fh.write(data)

    def close(self) -> None:
        self._fh.close()
        self._done = True

    def abort(self) -> None:
        if self._done:
            return
        # close() would finalize the resumable upload and leave a partial
        # object. terminate() cancels the session instead.
        self._done = True
        self._fh.terminate()


class _AzureBlockStream:
    def __init__(self, client: Any, container: str, key: str) -> None:
        self._blob = client.get_blob_client(container, key)
        self._ids: list[str] = []
        self._done = False

    def write(self, data: bytes) -> None:
        if not data:
            return
        import base64

        block_id = base64.b64encode(f"{len(self._ids):08d}".encode()).decode()
        self._blob.stage_block(block_id=block_id, data=data, length=len(data))
        self._ids.append(block_id)

    def close(self) -> None:
        from azure.storage.blob import BlobBlock

        if not self._ids:
            self._blob.upload_blob(b"", overwrite=True)
        else:
            self._blob.commit_block_list([BlobBlock(block_id=bid) for bid in self._ids])
        self._done = True

    def abort(self) -> None:
        self._done = True


@contextmanager
def open_upload(emu: EmulatorFS, key: str, *, chunk_size: int = 1024 * 1024) -> Iterator[_UploadSink]:
    """Context manager yielding a writer that uploads ``key`` in chunks."""
    sink = _UploadSink(emu, key, chunk_size=chunk_size)
    try:
        yield sink
    except BaseException:
        sink.abort()
        raise
    else:
        sink.close()


def put_bytes(emu: EmulatorFS, key: str, data: bytes) -> str:
    """Write ``data`` to ``bucket/key`` via the **native SDK**.

    Using the native SDK (not fsspec) guarantees zero-byte folder markers with
    trailing slashes are stored exactly as the cloud APIs store them — the
    layout the four Airflow pair-operator PRs had to special-case.
    """
    key = key.lstrip("/")
    client = _native_client(emu)
    if emu.flavour == "s3":
        client.put_object(Bucket=emu.bucket, Key=key, Body=data)
    elif emu.flavour == "gcs":
        client.bucket(emu.bucket).blob(key).upload_from_string(data)
    elif emu.flavour == "azure":
        client.get_container_client(emu.bucket).upload_blob(
            key, data, overwrite=True
        )
    return f"{emu.bucket}/{key}"


def get_bytes(emu: EmulatorFS, key: str) -> bytes:
    key = key.lstrip("/")
    path = f"{emu.bucket}/{key}"
    with emu.fs.open(path, "rb") as fh:
        return fh.read()


def list_keys_native(emu: EmulatorFS, prefix: str = "") -> list[str]:
    """List keys via the native SDK (exact cloud listing, including markers)."""
    prefix = prefix.lstrip("/")
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
        out: list[str] = []
        token = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": emu.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            out.extend(o["Key"] for o in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(out)
    if emu.flavour == "gcs":
        from google.cloud import storage as gcs_storage

        client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))
        return sorted(
            b.name for b in client.bucket(emu.bucket).list_blobs(prefix=prefix)
        )
    if emu.flavour == "azure":
        from azure.storage.blob import BlobServiceClient

        client = BlobServiceClient.from_connection_string(
            emu.storage_options["connection_string"]
        )
        container = client.get_container_client(emu.bucket)
        return sorted(
            b.name for b in container.list_blobs(name_starts_with=prefix or None)
        )
    raise ValueError(emu.flavour)


def list_keys_fsspec(emu: EmulatorFS, prefix: str = "") -> list[str]:
    """List keys via fsspec ``find`` as bucket-relative paths."""
    base = f"{emu.bucket}/{prefix}".rstrip("/") if prefix else emu.bucket
    try:
        emu.fs.invalidate_cache()
    except Exception:
        pass
    try:
        found = list(emu.fs.find(base))
    except FileNotFoundError:
        return []
    prefix_full = emu.bucket.rstrip("/") + "/"
    out: list[str] = []
    for p in found:
        rel = p[len(prefix_full) :] if p.startswith(prefix_full) else p
        out.append(rel)
    return sorted(out)


def seed_tree_with_markers(emu: EmulatorFS, prefix: str = "data") -> list[str]:
    """Seed a canonical tree including zero-byte folder markers via native SDK.

    Layout::

        {prefix}/                 (0 bytes — folder marker)
        {prefix}/a.json           {"id":1}
        {prefix}/nested/          (0 bytes — nested folder marker)
        {prefix}/nested/b.json    {"id":2}

    Returns the native-SDK key list under ``prefix``.
    """
    clear_prefix(emu, prefix)
    put_bytes(emu, f"{prefix}/", b"")
    put_bytes(emu, f"{prefix}/a.json", b'{"id":1}')
    put_bytes(emu, f"{prefix}/nested/", b"")
    put_bytes(emu, f"{prefix}/nested/b.json", b'{"id":2}')
    return list_keys_native(emu, prefix)



def seed_many_small(
    emu: EmulatorFS,
    prefix: str,
    n: int = 1000,
    *,
    size: int = 4096,
    marker_every: int = 100,
) -> list[str]:
    """Seed ``n`` ~size-byte objects under ``prefix``, with periodic folder markers."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    clear_prefix(emu, prefix)
    put_bytes(emu, f"{prefix}/", b"")
    payload = b"x" * size
    # Markers first (sequential — rare).
    if marker_every:
        for i in range(0, n, marker_every):
            put_bytes(emu, f"{prefix}/bucket_{i // marker_every}/", b"")
    jobs = [
        (f"{prefix}/bucket_{i // max(marker_every, 1)}/obj_{i:04d}.bin", payload)
        for i in range(n)
    ]
    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(put_bytes, emu, k, d) for k, d in jobs]
        for f in as_completed(futs):
            f.result()
    return list_keys_native(emu, prefix)


def seed_one_large(emu: EmulatorFS, prefix: str, *, size_mb: int = 100) -> str:
    """Seed a single large object (``size_mb`` MiB). Returns the key."""
    clear_prefix(emu, prefix)
    key = f"{prefix}/large.bin"
    # Build in chunks to avoid a giant intermediate peak beyond the object itself.
    chunk = b"0123456789abcdef" * (64 * 1024 // 16)  # 64 KiB
    target = size_mb * 1024 * 1024
    buf = bytearray()
    while len(buf) < target:
        buf.extend(chunk)
    put_bytes(emu, key, bytes(buf[:target]))
    return key


def seed_mixed_tree(emu: EmulatorFS, prefix: str) -> list[str]:
    """Mixed tree: markers, unicode, spaces, 0-byte real file, nested."""
    clear_prefix(emu, prefix)
    put_bytes(emu, f"{prefix}/", b"")
    put_bytes(emu, f"{prefix}/a.json", b'{"id":1}')
    put_bytes(emu, f"{prefix}/hello world.json", b'{"id":2}')
    put_bytes(emu, f"{prefix}/caf\u00e9.json", b'{"id":3}')
    put_bytes(emu, f"{prefix}/empty.txt", b"")
    put_bytes(emu, f"{prefix}/nested/", b"")
    put_bytes(emu, f"{prefix}/nested/b.json", b'{"id":4}')
    return list_keys_native(emu, prefix)
