"""S3Hook-backed implementation of the prototype file-transfer contract."""

from __future__ import annotations

from typing import Any

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gcs_to_gcs_shim"))
from contract import ListQuery, ObjectRef  # noqa: E402


class S3HookBackend:
    def __init__(self, hook: Any) -> None:
        self.hook = hook

    def list_objects(self, query: ListQuery) -> list[str]:
        # Same listing path as S3CopyPrefixOperator: list_objects_v2 paginator.
        s3_client = self.hook.get_conn()
        paginator = s3_client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=query.bucket, Prefix=query.prefix or ""):
            for obj in page.get("Contents") or []:
                keys.append(obj["Key"])
        return keys

    def exists(self, ref: ObjectRef) -> bool:
        return self.hook.check_for_key(ref.key, ref.bucket)

    def server_side_copy(self, source: ObjectRef, destination: ObjectRef, **extra: Any) -> None:
        self.hook.copy_object(
            source_bucket_key=source.key,
            dest_bucket_key=destination.key,
            source_bucket_name=source.bucket or None,
            dest_bucket_name=destination.bucket or None,
            **extra,
        )

    def copy_object_positional(
        self,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: str | None = None,
        dest_bucket_name: str | None = None,
        source_version_id: str | None = None,
        acl_policy: str | None = None,
        meta_data_directive: str | None = None,
        kms_key_id: str | None = None,
        kms_encryption_type: str | None = None,
    ) -> None:
        """Match S3CopyObjectOperator.execute call shape (positional)."""
        self.hook.copy_object(
            source_bucket_key,
            dest_bucket_key,
            source_bucket_name,
            dest_bucket_name,
            source_version_id,
            acl_policy,
            meta_data_directive,
            kms_key_id,
            kms_encryption_type,
        )
