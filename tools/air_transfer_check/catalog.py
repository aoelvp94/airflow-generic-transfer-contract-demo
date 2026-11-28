"""Operators this repository maps, and which calls may be rewritten.

The safe autofix is deliberately one shape. Prefix layout, ``replace``,
flatten collisions, and folder-marker policy differ across the other
operators, so those calls are flagged and left in the file.
"""

from __future__ import annotations

from dataclasses import dataclass

# Not an assigned Ruff code. Published AIR rules stop at AIR321
# (airflow31-moved). A real rule would take whatever code the AIR
# maintainers assign, and it would be implemented in Rust.
RULE = "AIR331"

TARGET_MODULE = "airflow.providers.common.io.operators.file_transfer"
TARGET_NAME = "FileTransferOperator"


@dataclass(frozen=True)
class OperatorSpec:
    """One deprecated per-pair operator."""

    name: str
    module: str
    hint: str
    fix: str | None = None


def _spec(name: str, module: str, hint: str, fix: str | None = None) -> OperatorSpec:
    return OperatorSpec(name=name, module=module, hint=hint, fix=fix)


OPERATORS: dict[str, OperatorSpec] = {
    spec.name: spec
    for spec in (
        _spec(
            "S3CopyObjectOperator",
            "airflow.providers.amazon.aws.operators.s3",
            "Single-object S3 copy. Autofix only when the call is task_id, "
            "bucket, and key literals (or a full s3:// key with the bucket "
            "argument omitted), plus aws_conn_id. The rewrite sets "
            "overwrite=True because CopyObject replaces the destination and "
            "the single-file operator raises when overwrite is false. "
            "A version id, ACL, metadata directive, KMS key, verify, or any "
            "other keyword is not rewritten: FileTransferOperator has no "
            "equivalent. A non-literal key may be a prefix, and a prefix "
            "returns a list of keys instead of None.",
            fix="s3_copy_object",
        ),
        _spec(
            "S3CopyPrefixOperator",
            "airflow.providers.amazon.aws.operators.s3",
            "Flag only. The prefix copy always replaces, copies marker keys, "
            "and returns None. copy_path skips markers by default, overwrites "
            "only when asked, and returns destination keys.",
        ),
        _spec(
            "GCSToGCSOperator",
            "airflow.providers.google.cloud.transfers.gcs_to_gcs",
            "Flag only. replace defaults to True, marker keys are copied, "
            "move_object can delete the source, and execute returns "
            "destination URIs. A single file here returns None, and "
            "overwrite defaults to False. exact_match is false by default, "
            "so a source_object is a prefix unless the call says otherwise.",
        ),
        _spec(
            "S3ToGCSOperator",
            "airflow.providers.google.cloud.transfers.s3_to_gcs",
            "Flag only. apply_gcs_prefix defaults to False, which joins the "
            "full source key onto the destination prefix. keep_structure "
            "strips the source prefix instead. Marker policy is "
            "skip-overlapping, not skip or copy. replace=False does not "
            "match that layout's planned keys. execute returns S3 keys "
            "unless return_gcs_uris is true. gzip, delimiter, wildcard, and "
            "deferrable have no equivalent.",
        ),
        _spec(
            "GCSToS3Operator",
            "airflow.providers.amazon.aws.transfers.gcs_to_s3",
            "Flag only. keep_directory_structure defaults to True and this "
            "repo records that layout as joining the full object name, not "
            "as stripping the GCS prefix. flatten_structure can map two "
            "objects onto one basename; copy_path raises before copying. "
            "Marker policy is skip-overlapping.",
        ),
        _spec(
            "S3ToAzureBlobStorageOperator",
            "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
            "Flag only. replace=False subtracts full destination blob names "
            "from relative source names, so an existing out/a.txt is not "
            "treated as the image of pre/a.txt. copy_path skips the planned "
            "destination key. Marker policy is skip-overlapping. "
            "create_container has no equivalent.",
        ),
        _spec(
            "AzureBlobStorageToS3Operator",
            "airflow.providers.amazon.aws.transfers.azure_blob_to_s3",
            "Flag only. The destination key joins dest_s3_key with the full "
            "blob name. Marker policy is skip-overlapping. delimiter and "
            "ACL arguments have no equivalent.",
        ),
        _spec(
            "GCSToAzureBlobStorageOperator",
            "airflow.providers.microsoft.azure.transfers.gcs_to_wasb",
            "Flag only. The destination key joins the blob prefix with the "
            "full object name, and every trailing-slash key is dropped. "
            "keep_directory_structure and flatten_structure are not "
            "keep_structure. create_container has no equivalent.",
        ),
        _spec(
            "AzureBlobStorageToGCSOperator",
            "airflow.providers.google.cloud.transfers.azure_blob_to_gcs",
            "Flag only. This copies one WASB blob. ObjectStoragePath does "
            "not construct wasb, and gzip, filename, and unwrap_single have "
            "no equivalent. execute returns a GCS URI.",
        ),
        _spec(
            "LocalFilesystemToWasbOperator",
            "airflow.providers.microsoft.azure.transfers.local_to_wasb",
            "Flag only. The upload overwrites one blob on the WASB service. "
            "ObjectStoragePath does not construct wasb. create_container "
            "and load_options have no equivalent.",
        ),
    )
}


def lookup(module: str, name: str) -> OperatorSpec | None:
    """Return the spec when ``module.name`` is a mapped operator."""
    spec = OPERATORS.get(name)
    if spec is None or spec.module != module:
        return None
    return spec
