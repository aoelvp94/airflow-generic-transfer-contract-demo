"""S3CopyObjectOperator / S3CopyPrefixOperator shims on the file contract."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gcs_to_gcs_shim"))
from contract import ListQuery, ObjectRef, TransferItem  # noqa: E402
from transfer import transfer_objects  # noqa: E402

from s3_backend import S3HookBackend  # noqa: E402


def make_copy_object_shim(s3_mod: Any) -> type:
    Upstream = s3_mod.S3CopyObjectOperator

    class S3CopyObjectOperatorShim(Upstream):
        def execute(self, context) -> None:
            backend = S3HookBackend(self.hook)
            # Keep the pair's positional copy_object call so KMS monkeypatches
            # and hook-call-shape tests see the same arguments, including
            # unresolved s3:// keys when bucket names are omitted.
            backend.copy_object_positional(
                self.source_bucket_key,
                self.dest_bucket_key,
                self.source_bucket_name,
                self.dest_bucket_name,
                self.source_version_id,
                self.acl_policy,
                self.meta_data_directive,
                self.kms_key_id,
                self.kms_encryption_type,
            )

    S3CopyObjectOperatorShim.__name__ = "S3CopyObjectOperator"
    S3CopyObjectOperatorShim.__qualname__ = "S3CopyObjectOperator"
    return S3CopyObjectOperatorShim


def make_copy_prefix_shim(s3_mod: Any) -> type:
    Upstream = s3_mod.S3CopyPrefixOperator

    class S3CopyPrefixOperatorShim(Upstream):
        def execute(self, context) -> None:
            source_bucket_name, source_bucket_prefix = self.hook.get_s3_bucket_key(
                self.source_bucket_name,
                self.source_bucket_prefix,
                "source_bucket_name",
                "source_bucket_prefix",
            )
            dest_bucket_name, dest_bucket_prefix = self.hook.get_s3_bucket_key(
                self.dest_bucket_name,
                self.dest_bucket_prefix,
                "dest_bucket_name",
                "dest_bucket_prefix",
            )
            backend = S3HookBackend(self.hook)
            keys = backend.list_objects(
                ListQuery(bucket=source_bucket_name, prefix=source_bucket_prefix)
            )
            extra = {
                "kms_key_id": self.kms_key_id,
                "kms_encryption_type": self.kms_encryption_type,
                "acl_policy": self.acl_policy,
                "meta_data_directive": self.meta_data_directive,
            }
            copied = 0
            failed = 0
            for source_key in keys:
                dest_key = dest_bucket_prefix + source_key[len(source_bucket_prefix) :]
                item = TransferItem(
                    source=ObjectRef(source_bucket_name, source_key),
                    destination=ObjectRef(dest_bucket_name, dest_key),
                )
                try:
                    transfer_objects([item], copier=backend, copy_extra=extra, log=self.log)
                    copied += 1
                except Exception as e:
                    if self.continue_on_failure:
                        self.log.error("Failed to copy %s: %s", source_key, e)
                        failed += 1
                    else:
                        raise RuntimeError(f"Failed to copy {source_key}: {e}") from e
            self.log.info("Successfully copied %s object(s)", copied)
            if failed > 0:
                raise RuntimeError(f"Failed to copy {failed} object(s)")

    S3CopyPrefixOperatorShim.__name__ = "S3CopyPrefixOperator"
    S3CopyPrefixOperatorShim.__qualname__ = "S3CopyPrefixOperator"
    return S3CopyPrefixOperatorShim
