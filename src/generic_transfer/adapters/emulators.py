"""Build fsspec filesystems pointed at local object-store emulators.

Each flavour returns an :class:`EmulatorFS` with a configured fsspec handle.
This module is the single place that knows how S3Mock/MinIO, fake-gcs-server,
and Azurite differ. Callers stay flavour-agnostic.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

Flavour = Literal["s3", "gcs", "azure"]


def _strip_quotes(val: str) -> str:
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        return val[1:-1]
    return val


def load_emulator_env(path: str | Path | None = None) -> None:
    """Load ``emulators.env`` into ``os.environ`` (does not override existing)."""
    if path is None:
        path = Path(__file__).resolve().parents[3] / "emulators.env"
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


def _s3_storage_options() -> dict[str, Any]:
    backend = os.environ.get("EMULATOR_S3_BACKEND", "s3mock")
    if backend == "minio":
        endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:19000")
        key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    else:
        endpoint = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090")
        key = os.environ.get("S3_ACCESS_KEY", "any")
        secret = os.environ.get("S3_SECRET_KEY", "any")
    return {
        "key": key,
        "secret": secret,
        "client_kwargs": {
            "endpoint_url": endpoint,
            "region_name": os.environ.get("S3_REGION", "us-east-1"),
        },
        "config_kwargs": {"s3": {"addressing_style": "path"}},
        "use_listings_cache": False,
    }


def _gcs_storage_options() -> dict[str, Any]:
    endpoint = os.environ.get("GCS_ENDPOINT", "http://127.0.0.1:14443")
    # google-cloud-storage accepts ``host:port`` or ``http://host:port``.
    host = os.environ.get("STORAGE_EMULATOR_HOST") or endpoint
    if "://" not in host:
        host = f"http://{host}"
    os.environ["STORAGE_EMULATOR_HOST"] = host
    return {
        "endpoint_url": endpoint,
        "project": os.environ.get("GCS_PROJECT", "demo"),
        "token": "anon",
        "use_listings_cache": False,
    }


def _azure_storage_options() -> dict[str, Any]:
    conn = os.environ.get(
        "AZURE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
    )
    return {
        "connection_string": conn,
        "account_name": os.environ.get("AZURE_ACCOUNT_NAME", "devstoreaccount1"),
        "use_listings_cache": False,
    }


def make_fs(flavour: Flavour, *, bucket: str | None = None) -> EmulatorFS:
    """Return a configured fsspec filesystem for ``flavour``."""
    load_emulator_env()
    if flavour == "s3":
        import s3fs

        opts = _s3_storage_options()
        bucket = bucket or os.environ.get("S3_SRC_BUCKET", "src-bucket")
        fs = s3fs.S3FileSystem(**opts)
        return EmulatorFS(
            flavour="s3",
            fs=fs,
            bucket=bucket,
            root_url=f"s3://{bucket}",
            endpoint=opts["client_kwargs"]["endpoint_url"],
            storage_options=opts,
        )
    if flavour == "gcs":
        # Pin to the HTTP JSON client. gcsfs 2026.10+ defaults to the gRPC
        # ExtendedGcsFileSystem, which cannot reach fake-gcs-server.
        from gcsfs.core import GCSFileSystem

        opts = _gcs_storage_options()
        bucket = bucket or os.environ.get("GCS_SRC_BUCKET", "src-bucket")
        fs = GCSFileSystem(**opts)
        return EmulatorFS(
            flavour="gcs",
            fs=fs,
            bucket=bucket,
            root_url=f"gs://{bucket}",
            endpoint=opts["endpoint_url"],
            storage_options=opts,
        )
    if flavour == "azure":
        import adlfs

        opts = _azure_storage_options()
        bucket = bucket or os.environ.get("AZURE_SRC_CONTAINER", "src-container")
        fs = adlfs.AzureBlobFileSystem(**opts)
        return EmulatorFS(
            flavour="azure",
            fs=fs,
            bucket=bucket,
            root_url=f"abfs://{bucket}",
            endpoint=os.environ.get(
                "AZURE_BLOB_ENDPOINT", "http://127.0.0.1:10000/devstoreaccount1"
            ),
            storage_options=opts,
        )
    raise ValueError(f"unknown flavour: {flavour!r}")


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
