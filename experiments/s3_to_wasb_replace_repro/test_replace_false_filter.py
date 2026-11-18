"""Unit test for S3ToAzureBlobStorageOperator replace=False dest-key comparison.

Fails against the unpatched operator (main SHA 057f0099 / s3_to_wasb.py at
2b59699b). Passes after s3_to_wasb_replace_false.patch is applied to the
operator class.

Style matches the existing mocked-hook tests in
providers/microsoft/azure/tests/unit/microsoft/azure/transfers/test_s3_to_wasb.py,
but lists destination blobs the way WasbHook.get_blobs_list_recursive actually
does: full names under blob_prefix, not names relative to it.

The current parametrized tests mock get_blobs_list_recursive with relative
names (e.g. MOCK_FILES) and therefore do not catch this bug.
"""

from __future__ import annotations

from unittest import mock

from airflow.providers.microsoft.azure.transfers.s3_to_wasb import S3ToAzureBlobStorageOperator


def test_replace_false_skips_existing_blob_under_blob_prefix():
    """replace=False must skip a dest blob already present at blob_prefix/rel.

    Source object is s3://bucket/pre/a.txt. The mapped dest is
    ``out/a.txt``. WasbHook.get_blobs_list_recursive(prefix="out") returns
    that full name. get_files_to_move() should return an empty list.
    """
    with (
        mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook") as s3_hook_cls,
        mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook") as wasb_hook_cls,
    ):
        s3_hook_cls.return_value.list_keys.return_value = ["pre/a.txt"]
        wasb_hook_cls.return_value.get_blobs_list_recursive.return_value = ["out/a.txt"]

        operator = S3ToAzureBlobStorageOperator(
            task_id="repro-replace-false",
            s3_bucket="src-bucket",
            s3_prefix="pre",
            container_name="dst-container",
            blob_prefix="out",
            replace=False,
        )
        assert operator.get_files_to_move() == []
        wasb_hook_cls.return_value.get_blobs_list_recursive.assert_called_once_with(
            container_name="dst-container", prefix="out"
        )
