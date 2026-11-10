"""Thin compatibility wrappers: pair constructor args -> generic operator.

Used only to drive unmodified upstream unit tests. Not the operator under test.
The wrapper does not subclass any pair operator and does not port pair listing.
"""

from __future__ import annotations

from typing import Any

from listing import LAYOUT_JOIN_SOURCE, LAYOUT_PRESERVE
from generic_operator import GenericFileTransferOperator
from pair_policy import PAIR_POLICY


def _store(obj: Any, kwargs: dict[str, Any]) -> None:
    for key, value in kwargs.items():
        setattr(obj, key, value)


def _generic_from_mapped(**kw) -> GenericFileTransferOperator:
    return GenericFileTransferOperator(**kw)


class _BaseWrapper:
    """Stores pair kwargs as attributes; execute maps them onto the generic operator."""

    def __init__(self, *, task_id: str = "wrapper", **kwargs: Any) -> None:
        self.task_id = task_id
        self.log = _NullLog()
        _store(self, kwargs)

    def execute(self, context=None):
        raise NotImplementedError


class _NullLog:
    def info(self, *a, **k):  # noqa: ANN001
        return None

    def warning(self, *a, **k):  # noqa: ANN001
        return None

    def error(self, *a, **k):  # noqa: ANN001
        return None

    def debug(self, *a, **k):  # noqa: ANN001
        return None


def _need_backends():
    raise RuntimeError(
        "GenericFileTransferOperator requires contract backends; this thin wrapper "
        "does not construct provider hooks or call hook.list/rewrite/copy_object."
    )


class GCSToGCSWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        source = getattr(self, "source_object", None) or (
            (getattr(self, "source_objects", None) or [""])[0]
        )
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "source_bucket", ""),
            dest_bucket=getattr(self, "destination_bucket", "") or getattr(self, "source_bucket", ""),
            source_prefix=source or "",
            dest_prefix=getattr(self, "destination_object", None) or "",
            replace=getattr(self, "replace", True),
            move=getattr(self, "move_object", False),
            match_glob=getattr(self, "match_glob", None),
            exact_match=getattr(self, "exact_match", False),
            last_modified_time=getattr(self, "last_modified_time", None),
            maximum_modified_time=getattr(self, "maximum_modified_time", None),
            is_older_than=getattr(self, "is_older_than", None),
            marker_policy=PAIR_POLICY[("gcs", "gcs")]["marker_policy"],
            path_layout=PAIR_POLICY[("gcs", "gcs")]["path_layout"],
        ).execute(None)


class S3CopyPrefixWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "source_bucket_name", "") or "",
            dest_bucket=getattr(self, "dest_bucket_name", "") or "",
            source_prefix=getattr(self, "source_bucket_prefix", "") or "",
            dest_prefix=getattr(self, "dest_bucket_prefix", "") or "",
            replace=True,
            marker_policy=PAIR_POLICY[("s3", "s3")]["marker_policy"],
            path_layout=PAIR_POLICY[("s3", "s3")]["path_layout"],
        ).execute(None)


class S3CopyObjectWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "source_bucket_name", "") or "",
            dest_bucket=getattr(self, "dest_bucket_name", "") or "",
            source_prefix=getattr(self, "source_bucket_key", "") or "",
            dest_prefix=getattr(self, "dest_bucket_key", "") or "",
            exact_match=True,
            replace=True,
            marker_policy="copy",
            path_layout=LAYOUT_PRESERVE,
        ).execute(None)


class S3ToGCSWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        dest = getattr(self, "dest_gcs", "") or ""
        dest_prefix = dest.split("://", 1)[-1].split("/", 1)[-1] if dest else ""
        layout = LAYOUT_PRESERVE if getattr(self, "apply_gcs_prefix", False) else LAYOUT_JOIN_SOURCE
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "bucket", ""),
            dest_bucket="",
            source_prefix=getattr(self, "prefix", "") or "",
            dest_prefix=dest_prefix,
            replace=getattr(self, "replace", False),
            marker_policy=PAIR_POLICY[("s3", "gcs")]["marker_policy"],
            path_layout=layout,
        ).execute(None)


class GCSToS3Wrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        dest = getattr(self, "dest_s3_key", "") or ""
        dest_prefix = dest.split("://", 1)[-1].split("/", 1)[-1] if dest else ""
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "gcs_bucket", ""),
            dest_bucket="",
            source_prefix=getattr(self, "prefix", "") or "",
            dest_prefix=dest_prefix,
            replace=getattr(self, "replace", False),
            match_glob=getattr(self, "match_glob", None),
            marker_policy=PAIR_POLICY[("gcs", "s3")]["marker_policy"],
            path_layout=LAYOUT_JOIN_SOURCE,
        ).execute(None)


class S3ToAzureWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "s3_bucket", ""),
            dest_bucket=getattr(self, "container_name", ""),
            source_prefix=getattr(self, "s3_prefix", "") or getattr(self, "s3_key", "") or "",
            dest_prefix=getattr(self, "blob_prefix", "") or getattr(self, "blob_name", "") or "",
            replace=getattr(self, "replace", False),
            marker_policy=PAIR_POLICY[("s3", "azure")]["marker_policy"],
            path_layout=PAIR_POLICY[("s3", "azure")]["path_layout"],
        ).execute(None)


class AzureToS3Wrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        dest = getattr(self, "dest_s3_key", "") or ""
        dest_prefix = dest.split("://", 1)[-1].split("/", 1)[-1] if dest else ""
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "container_name", ""),
            dest_bucket="",
            source_prefix=getattr(self, "prefix", "") or "",
            dest_prefix=dest_prefix,
            replace=getattr(self, "replace", False),
            marker_policy=PAIR_POLICY[("azure", "s3")]["marker_policy"],
            path_layout=PAIR_POLICY[("azure", "s3")]["path_layout"],
        ).execute(None)


class GCSToAzureWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "gcs_bucket", ""),
            dest_bucket=getattr(self, "container_name", ""),
            source_prefix=getattr(self, "prefix", "") or "",
            dest_prefix=getattr(self, "blob_prefix", "") or "",
            replace=getattr(self, "replace", False),
            match_glob=getattr(self, "match_glob", None),
            marker_policy=PAIR_POLICY[("gcs", "azure")]["marker_policy"],
            path_layout=PAIR_POLICY[("gcs", "azure")]["path_layout"],
        ).execute(None)


class AzureToGCSWrapper(_BaseWrapper):
    def execute(self, context=None):
        del context
        _need_backends()
        return _generic_from_mapped(
            source_backend=None,
            dest_backend=None,
            source_bucket=getattr(self, "container_name", ""),
            dest_bucket=getattr(self, "bucket_name", ""),
            source_prefix=getattr(self, "blob_name", ""),
            dest_prefix=getattr(self, "object_name", ""),
            exact_match=True,
            replace=True,
            marker_policy="copy",
            path_layout=LAYOUT_PRESERVE,
        ).execute(None)


WRAPPERS = {
    "GCSToGCSOperator": ("airflow.providers.google.cloud.transfers.gcs_to_gcs", "GCSToGCSOperator", GCSToGCSWrapper),
    "S3CopyPrefixOperator": ("airflow.providers.amazon.aws.operators.s3", "S3CopyPrefixOperator", S3CopyPrefixWrapper),
    "S3CopyObjectOperator": ("airflow.providers.amazon.aws.operators.s3", "S3CopyObjectOperator", S3CopyObjectWrapper),
    "S3ToGCSOperator": ("airflow.providers.google.cloud.transfers.s3_to_gcs", "S3ToGCSOperator", S3ToGCSWrapper),
    "GCSToS3Operator": ("airflow.providers.amazon.aws.transfers.gcs_to_s3", "GCSToS3Operator", GCSToS3Wrapper),
    "S3ToAzureBlobStorageOperator": (
        "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
        "S3ToAzureBlobStorageOperator",
        S3ToAzureWrapper,
    ),
    "AzureBlobStorageToS3Operator": (
        "airflow.providers.amazon.aws.transfers.azure_blob_to_s3",
        "AzureBlobStorageToS3Operator",
        AzureToS3Wrapper,
    ),
    "GCSToAzureBlobStorageOperator": (
        "airflow.providers.microsoft.azure.transfers.gcs_to_wasb",
        "GCSToAzureBlobStorageOperator",
        GCSToAzureWrapper,
    ),
    "AzureBlobStorageToGCSOperator": (
        "airflow.providers.google.cloud.transfers.azure_blob_to_gcs",
        "AzureBlobStorageToGCSOperator",
        AzureToGCSWrapper,
    ),
}
