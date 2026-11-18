"""Per-pair marker and path-layout policy that matches each pre-registered reference.

These are generic-operator settings, not ports of pair listing helpers.
"""

from __future__ import annotations

# path_layout:
#   preserve         — dest = dest_prefix + key with source_prefix stripped
#   join_source_key  — dest = dest_prefix + full source key
#   flatten          — dest = dest_prefix + basename
#
# marker_policy:
#   copy              — keep trailing-slash keys
#   skip              — drop every trailing-slash key
#   skip_overlapping  — drop trailing-slash keys that prefix another listed key

PAIR_POLICY: dict[tuple[str, str], dict[str, str]] = {
    ("s3", "s3"): {"marker_policy": "copy", "path_layout": "preserve", "reference": "S3CopyPrefixOperator"},
    ("gcs", "gcs"): {"marker_policy": "copy", "path_layout": "preserve", "reference": "GCSToGCSOperator"},
    ("azure", "azure"): {"marker_policy": "copy", "path_layout": "preserve", "reference": "naive_pair_copy"},
    ("s3", "gcs"): {
        "marker_policy": "skip_overlapping",
        "path_layout": "join_source_key",  # S3ToGCSOperator apply_gcs_prefix=False (default)
        "reference": "S3ToGCSOperator",
    },
    ("gcs", "s3"): {
        "marker_policy": "skip_overlapping",
        "path_layout": "join_source_key",  # GCSToS3Operator keep_directory_structure=True
        "reference": "GCSToS3Operator",
    },
    ("s3", "azure"): {
        "marker_policy": "skip_overlapping",
        "path_layout": "preserve",  # S3ToAzureBlobStorageOperator strips s3_prefix
        "reference": "S3ToAzureBlobStorageOperator",
    },
    ("azure", "s3"): {
        "marker_policy": "skip_overlapping",
        "path_layout": "join_source_key",  # AzureBlobStorageToS3Operator joins dest_s3_key + blob name
        "reference": "AzureBlobStorageToS3Operator",
    },
    ("gcs", "azure"): {
        "marker_policy": "skip",  # GCSToAzureBlobStorageOperator drops every trailing-slash key
        "path_layout": "join_source_key",
        "reference": "GCSToAzureBlobStorageOperator",
    },
    ("azure", "gcs"): {
        "marker_policy": "copy",
        "path_layout": "preserve",  # prefix scenarios vs naive_pair_copy
        "reference": "naive_pair_copy / AzureBlobStorageToGCSOperator (single-blob)",
    },
}

# References that expose a replace / overwrite flag.
HAS_REPLACE = {
    ("gcs", "gcs"),
    ("s3", "gcs"),
    ("gcs", "s3"),
    ("s3", "azure"),
    ("azure", "s3"),
    ("gcs", "azure"),
}

HAS_MOVE = {("gcs", "gcs")}
