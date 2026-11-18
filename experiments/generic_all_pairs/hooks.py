"""Emulator-backed hook facades so upstream pair operators can run as references.

These are test harness adapters, not the generic operator.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, IO

from backends import AzureBackend, GCSBackend, S3Backend
from contract import ListQuery, ObjectRef


def _aware(ts: datetime | None) -> datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


class EmulatorS3Hook:
    def __init__(self, backend: S3Backend) -> None:
        self.backend = backend
        self.client = backend.client

    def get_conn(self):
        return self.client

    def copy_object(
        self,
        source_bucket_key,
        dest_bucket_key,
        source_bucket_name=None,
        dest_bucket_name=None,
        source_version_id=None,
        acl_policy=None,
        meta_data_directive=None,
        kms_key_id=None,
        kms_encryption_type=None,
        **kwargs,
    ) -> None:
        src_b, src_k = self.get_s3_bucket_key(
            source_bucket_name, source_bucket_key, "source_bucket_name", "source_bucket_key"
        )
        dst_b, dst_k = self.get_s3_bucket_key(
            dest_bucket_name, dest_bucket_key, "dest_bucket_name", "dest_bucket_key"
        )
        self.client.copy_object(
            Bucket=dst_b, Key=dst_k, CopySource={"Bucket": src_b, "Key": src_k}
        )

    def get_s3_bucket_key(self, bucket, key, bucket_param, key_param):
        if isinstance(key, str) and key.startswith("s3://"):
            if bucket:
                raise ValueError(f"bucket and s3 url both set for {bucket_param}/{key_param}")
            rest = key[len("s3://") :]
            bucket, _, key = rest.partition("/")
            return bucket, key
        return bucket, key

    @staticmethod
    def parse_s3_url(url: str):
        if url.startswith("s3://"):
            rest = url[len("s3://") :]
            bucket, _, key = rest.partition("/")
            return bucket, key
        raise ValueError(url)

    def list_keys(self, bucket_name, prefix="", delimiter="", apply_wildcard=False, **kwargs):
        del delimiter, apply_wildcard, kwargs
        return [o.key for o in self.backend.list_objects(ListQuery(bucket_name, prefix or ""))]

    def check_for_bucket(self, bucket_name) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except Exception:
            return False

    def load_file(self, filename, key, bucket_name=None, replace=True, **kwargs):
        bucket, key = self.get_s3_bucket_key(bucket_name, key, "bucket_name", "key")
        data = Path(filename).read_bytes()
        self.backend.write_bytes(ObjectRef(bucket, key, "s3"), data, overwrite=replace)

    def get_key(self, key, bucket_name=None):
        bucket, key = self.get_s3_bucket_key(bucket_name, key, "bucket_name", "key")
        obj = self.client.get_object(Bucket=bucket, Key=key)
        class _Key:
            def __init__(self, body):
                self._body = body["Body"].read()
            def get(self):
                return {"Body": type("B", (), {"read": lambda s: self._body})()}
            def download_fileobj(self, fh):
                fh.write(self._body)
        return _Key(obj)

    def download_file(self, bucket, key, filename):
        data = self.backend.read_bytes(ObjectRef(bucket, key, "s3"))
        Path(filename).write_bytes(data)


class EmulatorGCSHook:
    def __init__(self, backend: GCSBackend) -> None:
        self.backend = backend
        self.client = backend.client
        self.project_id = os.environ.get("GCS_PROJECT", "demo")

    def list(self, bucket_name, prefix=None, delimiter=None, match_glob=None, user_project=None, **kwargs):
        keys = [o.key for o in self.backend.list_objects(ListQuery(bucket_name, prefix or ""))]
        if delimiter:
            keys = [k for k in keys if k.endswith(delimiter) or delimiter in k]
        return keys

    def exists(self, bucket_name, object_name):
        return self.backend.exists(ObjectRef(bucket_name, object_name, "gcs"))

    def rewrite(self, source_bucket, source_object, dest_bucket, dest_object, **kwargs):
        self.backend.server_side_copy(
            ObjectRef(source_bucket, source_object, "gcs"),
            ObjectRef(dest_bucket, dest_object, "gcs"),
        )

    def delete(self, bucket_name, object_name, ignore_error=False):
        try:
            self.backend.delete(ObjectRef(bucket_name, object_name, "gcs"))
        except Exception:
            if not ignore_error:
                raise

    def upload(self, bucket_name, object_name, filename=None, data=None, gzip=False, **kwargs):
        if filename:
            payload = Path(filename).read_bytes()
        else:
            payload = data if isinstance(data, bytes) else (data or "").encode()
        self.backend.write_bytes(ObjectRef(bucket_name, object_name, "gcs"), payload, overwrite=True)

    def download(self, bucket_name, object_name, filename=None):
        data = self.backend.read_bytes(ObjectRef(bucket_name, object_name, "gcs"))
        if filename:
            Path(filename).write_bytes(data)
        return data


    @contextmanager
    def provide_file(
        self,
        bucket_name: str | None = None,
        object_name: str | None = None,
        object_url: str | None = None,
        dir: str | None = None,
        user_project: str | None = None,
    ) -> Iterator[IO[bytes]]:
        del object_url, user_project
        if object_name is None:
            raise ValueError("Object name can not be empty")
        data = self.download(bucket_name, object_name)
        suffix = object_name.rsplit("/", 1)[-1] or "obj"
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, dir=dir, delete=True)
        tmp.write(data)
        tmp.flush()
        try:
            yield tmp
        finally:
            tmp.close()

    def is_older_than(self, bucket_name, object_name, seconds: int) -> bool:
        objs = {o.key: o for o in self.backend.list_objects(ListQuery(bucket_name, object_name))}
        obj = objs.get(object_name)
        if obj is None or obj.last_modified is None:
            return False
        ts = _aware(obj.last_modified)
        return (datetime.now(timezone.utc) - ts).total_seconds() >= seconds

    def is_updated_after(self, bucket_name, object_name, ts: datetime) -> bool:
        objs = {o.key: o for o in self.backend.list_objects(ListQuery(bucket_name, object_name))}
        obj = objs.get(object_name)
        if obj is None or obj.last_modified is None:
            return False
        return _aware(obj.last_modified) >= _aware(ts)

    def is_updated_before(self, bucket_name, object_name, ts: datetime) -> bool:
        objs = {o.key: o for o in self.backend.list_objects(ListQuery(bucket_name, object_name))}
        obj = objs.get(object_name)
        if obj is None or obj.last_modified is None:
            return False
        return _aware(obj.last_modified) <= _aware(ts)

    def is_updated_between(self, bucket_name, object_name, ts1, ts2) -> bool:
        return self.is_updated_after(bucket_name, object_name, ts1) and self.is_updated_before(
            bucket_name, object_name, ts2
        )


class EmulatorWasbHook:
    def __init__(self, backend: AzureBackend) -> None:
        self.backend = backend
        self.client = backend.client

    def get_conn(self):
        return self.client

    def load_file(self, file_path, container_name, blob_name, create_container=False, **kwargs):
        overwrite = kwargs.pop("overwrite", True)
        data = Path(file_path).read_bytes()
        try:
            self.backend.write_bytes(
                ObjectRef(container_name, blob_name, "azure"), data, overwrite=overwrite
            )
        except Exception:
            raise

    def get_file(self, file_path, container_name, blob_name, **kwargs):
        data = self.backend.read_bytes(ObjectRef(container_name, blob_name, "azure"))
        Path(file_path).write_bytes(data)

    def check_for_blob(self, container_name, blob_name) -> bool:
        return self.backend.exists(ObjectRef(container_name, blob_name, "azure"))

    def get_blobs_list_recursive(self, container_name, prefix=None, endswith=None, **kwargs):
        keys = [o.key for o in self.backend.list_objects(ListQuery(container_name, prefix or ""))]
        if endswith:
            keys = [k for k in keys if k.endswith(endswith)]
        return keys
