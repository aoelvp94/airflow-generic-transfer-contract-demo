"""S3, GCS, and Azure Blob backends on the shared contract, via native SDKs."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from urllib.parse import quote

from contract import ListedObject, ListQuery, ObjectRef


def backend_for(flavour: str, emu=None, client=None) -> Any:
    flavour = flavour.lower()
    if flavour in {"s3", "amazon"}:
        return S3Backend(emu=emu, client=client)
    if flavour in {"gcs", "google"}:
        return GCSBackend(emu=emu, client=client)
    if flavour in {"azure", "wasb", "azureblob"}:
        return AzureBackend(emu=emu, client=client)
    raise ValueError(f"unknown flavour {flavour!r}")


class S3Backend:
    flavour = "s3"

    def __init__(self, emu=None, client=None) -> None:
        self.emu = emu
        if client is not None:
            self.client = client
        elif emu is not None:
            from generic_transfer.adapters.emulators import _native_client

            self.client = _native_client(emu)
        else:
            raise TypeError("S3Backend requires emu or client")

    def list_objects(self, query: ListQuery) -> list[ListedObject]:
        out: list[ListedObject] = []
        token = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": query.bucket, "Prefix": query.prefix or ""}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents") or []:
                out.append(
                    ListedObject(
                        key=obj["Key"],
                        size=obj.get("Size"),
                        last_modified=obj.get("LastModified"),
                    )
                )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return out

    def exists(self, ref: ObjectRef) -> bool:
        try:
            self.client.head_object(Bucket=ref.bucket, Key=ref.key)
            return True
        except Exception:
            return False

    def read_bytes(self, ref: ObjectRef) -> bytes:
        return self.client.get_object(Bucket=ref.bucket, Key=ref.key)["Body"].read()

    def write_bytes(self, ref: ObjectRef, data: bytes, *, overwrite: bool = True) -> None:
        if not overwrite and self.exists(ref):
            raise FileExistsError(ref.key)
        self.client.put_object(Bucket=ref.bucket, Key=ref.key, Body=data)

    def server_side_copy(self, source: ObjectRef, destination: ObjectRef) -> None:
        self.client.copy_object(
            Bucket=destination.bucket,
            Key=destination.key,
            CopySource={"Bucket": source.bucket, "Key": source.key},
        )

    def delete(self, ref: ObjectRef) -> None:
        self.client.delete_object(Bucket=ref.bucket, Key=ref.key)


class GCSBackend:
    flavour = "gcs"

    def __init__(self, emu=None, client=None) -> None:
        self.emu = emu
        if client is not None:
            self.client = client
        elif emu is not None:
            from generic_transfer.adapters.emulators import _native_client

            self.client = _native_client(emu)
        else:
            from google.cloud import storage as gcs_storage

            self.client = gcs_storage.Client(project=os.environ.get("GCS_PROJECT", "demo"))

    def list_objects(self, query: ListQuery) -> list[ListedObject]:
        bucket = self.client.bucket(query.bucket)
        out: list[ListedObject] = []
        for blob in bucket.list_blobs(prefix=query.prefix or ""):
            out.append(
                ListedObject(
                    key=blob.name,
                    size=blob.size,
                    last_modified=blob.updated,
                )
            )
        return out

    def exists(self, ref: ObjectRef) -> bool:
        return self.client.bucket(ref.bucket).blob(ref.key).exists()

    def read_bytes(self, ref: ObjectRef) -> bytes:
        return self.client.bucket(ref.bucket).blob(ref.key).download_as_bytes()

    def write_bytes(self, ref: ObjectRef, data: bytes, *, overwrite: bool = True) -> None:
        blob = self.client.bucket(ref.bucket).blob(ref.key)
        if not overwrite and blob.exists():
            raise FileExistsError(ref.key)
        blob.upload_from_string(data)

    def server_side_copy(self, source: ObjectRef, destination: ObjectRef) -> None:
        src = self.client.bucket(source.bucket).blob(source.key)
        dest = self.client.bucket(destination.bucket).blob(destination.key)
        token, _, _ = dest.rewrite(source=src)
        while token is not None:
            token, _, _ = dest.rewrite(source=src, token=token)

    def delete(self, ref: ObjectRef) -> None:
        self.client.bucket(ref.bucket).blob(ref.key).delete()


class AzureBackend:
    flavour = "azure"

    def __init__(self, emu=None, client=None) -> None:
        self.emu = emu
        if client is not None:
            self.client = client
        elif emu is not None:
            from generic_transfer.adapters.emulators import _native_client

            self.client = _native_client(emu)
        else:
            raise TypeError("AzureBackend requires emu or client")

    def _cc(self, bucket: str):
        return self.client.get_container_client(bucket)

    def list_objects(self, query: ListQuery) -> list[ListedObject]:
        out: list[ListedObject] = []
        for blob in self._cc(query.bucket).list_blobs(name_starts_with=query.prefix or ""):
            out.append(
                ListedObject(
                    key=blob.name,
                    size=getattr(blob, "size", None),
                    last_modified=getattr(blob, "last_modified", None),
                )
            )
        return out

    def exists(self, ref: ObjectRef) -> bool:
        return self._cc(ref.bucket).get_blob_client(ref.key).exists()

    def read_bytes(self, ref: ObjectRef) -> bytes:
        return self._cc(ref.bucket).download_blob(ref.key).readall()

    def write_bytes(self, ref: ObjectRef, data: bytes, *, overwrite: bool = True) -> None:
        self._cc(ref.bucket).upload_blob(ref.key, data, overwrite=overwrite)

    def server_side_copy(self, source: ObjectRef, destination: ObjectRef) -> None:
        # Same-account copy via the source blob URL. Azurite accepts this
        # for copies inside the emulator account.
        src = self._cc(source.bucket).get_blob_client(source.key)
        dest = self._cc(destination.bucket).get_blob_client(destination.key)
        dest.start_copy_from_url(src.url)

    def delete(self, ref: ObjectRef) -> None:
        self._cc(ref.bucket).delete_blob(ref.key)
