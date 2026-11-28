"""AIR331 prototype: flag per-pair operators, autofix only a 1:1 object copy."""

from __future__ import annotations

from pathlib import Path

from tools.air_transfer_check import check_source, fix_source
from tools.air_transfer_check.checker import main
from tools.air_transfer_check.catalog import RULE

_SAFE = '''\
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator

copy = S3CopyObjectOperator(
    task_id="copy_one",
    source_bucket_name="src-bucket",
    source_bucket_key="incoming/a.txt",
    dest_bucket_name="dst-bucket",
    dest_bucket_key="archive/a.txt",
)
'''

_SAFE_FIXED = '''\
from airflow.providers.common.io.operators.file_transfer import FileTransferOperator

copy = FileTransferOperator(
    task_id="copy_one",
    src="s3://src-bucket/incoming/a.txt",
    dst="s3://dst-bucket/archive/a.txt",
    source_conn_id="aws_default",
    dest_conn_id="aws_default",
    overwrite=True,
)
'''

_SAFE_URL = '''\
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator

copy = S3CopyObjectOperator(
    task_id=TASK,
    source_bucket_key="s3://src-bucket/incoming/a.txt",
    dest_bucket_key="s3://dst-bucket/archive/a.txt",
    aws_conn_id=conn,
)
'''

_SAFE_URL_FIXED = '''\
from airflow.providers.common.io.operators.file_transfer import FileTransferOperator

copy = FileTransferOperator(
    task_id=TASK,
    src="s3://src-bucket/incoming/a.txt",
    dst="s3://dst-bucket/archive/a.txt",
    source_conn_id=conn,
    dest_conn_id=conn,
    overwrite=True,
)
'''


def _operators(source: str) -> set[str]:
    return {finding.operator for finding in check_source(source)}


def test_safe_object_copy_is_rewritten() -> None:
    findings = check_source(_SAFE)
    assert {finding.operator for finding in findings} == {"S3CopyObjectOperator"}
    assert any(finding.fixable for finding in findings)
    assert all(finding.rule == RULE for finding in findings)
    rewritten, _ = fix_source(_SAFE)
    assert rewritten == _SAFE_FIXED
    assert check_source(rewritten) == []
    assert fix_source(rewritten)[0] == rewritten


def test_full_s3_url_keeps_the_connection_expression() -> None:
    rewritten, findings = fix_source(_SAFE_URL)
    assert any(finding.fixable for finding in findings)
    assert rewritten == _SAFE_URL_FIXED


def test_acl_keyword_is_not_rewritten() -> None:
    source = _SAFE.replace(
        'dest_bucket_key="archive/a.txt",\n',
        'dest_bucket_key="archive/a.txt",\n    acl_policy="public-read",\n',
    )
    findings = check_source(source)
    assert findings
    assert not any(finding.fixable for finding in findings)
    assert "ACL" in findings[-1].message
    assert fix_source(source)[0] == source


def test_prefix_key_and_dynamic_key_are_not_rewritten() -> None:
    prefix = _SAFE.replace('source_bucket_key="incoming/a.txt"', 'source_bucket_key="incoming/"')
    dynamic = _SAFE.replace('source_bucket_key="incoming/a.txt"', "source_bucket_key=key")
    both = _SAFE.replace(
        'source_bucket_name="src-bucket"',
        'source_bucket_name="src-bucket"',
    ).replace(
        'source_bucket_key="incoming/a.txt"',
        'source_bucket_key="s3://src-bucket/incoming/a.txt"',
    )
    for source in (prefix, dynamic, both):
        assert not any(finding.fixable for finding in check_source(source))
        assert fix_source(source)[0] == source


def test_s3_to_gcs_is_flagged_and_left_unchanged() -> None:
    source = '''\
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

S3ToGCSOperator(
    task_id="sales",
    bucket="src",
    prefix="sales/",
    dest_gcs="gs://dst/sales/",
    replace=False,
)
'''
    findings = check_source(source)
    assert _operators(source) == {"S3ToGCSOperator"}
    assert not any(finding.fixable for finding in findings)
    assert "apply_gcs_prefix" in findings[-1].message
    assert fix_source(source)[0] == source


def test_other_mapped_operators_are_flag_only() -> None:
    samples = {
        "S3CopyPrefixOperator": (
            "airflow.providers.amazon.aws.operators.s3",
            "S3CopyPrefixOperator(task_id='p', source_bucket_prefix='a/', dest_bucket_prefix='b/')",
            "returns None",
        ),
        "GCSToGCSOperator": (
            "airflow.providers.google.cloud.transfers.gcs_to_gcs",
            "GCSToGCSOperator(task_id='g', source_bucket='b', source_object='a.txt', exact_match=True, replace=True)",
            "returns",
        ),
        "GCSToS3Operator": (
            "airflow.providers.amazon.aws.transfers.gcs_to_s3",
            "GCSToS3Operator(task_id='g', gcs_bucket='b', dest_s3_key='s3://d/k', flatten_structure=True)",
            "basename",
        ),
        "S3ToAzureBlobStorageOperator": (
            "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
            "S3ToAzureBlobStorageOperator(task_id='a', s3_bucket='b', container_name='c', replace=False)",
            "out/a.txt",
        ),
        "LocalFilesystemToWasbOperator": (
            "airflow.providers.microsoft.azure.transfers.local_to_wasb",
            "LocalFilesystemToWasbOperator(task_id='u', file_path='/tmp/a', container_name='c', blob_name='a')",
            "wasb",
        ),
    }
    for name, (module, call, needle) in samples.items():
        source = f"from {module} import {name}\n\n{call}\n"
        findings = check_source(source)
        assert _operators(source) == {name}
        assert not any(finding.fixable for finding in findings)
        assert needle in findings[-1].message
        assert fix_source(source)[0] == source


def test_mixed_file_rewrites_only_the_safe_call() -> None:
    source = '''\
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

safe = S3CopyObjectOperator(
    task_id="copy_one",
    source_bucket_name="src-bucket",
    source_bucket_key="incoming/a.txt",
    dest_bucket_name="dst-bucket",
    dest_bucket_key="archive/a.txt",
)
kept = S3ToGCSOperator(
    task_id="sales",
    bucket="src",
    prefix="sales/",
    dest_gcs="gs://dst/sales/",
)
'''
    rewritten, findings = fix_source(source)
    assert any(finding.fixable and finding.operator == "S3CopyObjectOperator" for finding in findings)
    assert any(not finding.fixable and finding.operator == "S3ToGCSOperator" for finding in findings)
    assert "S3ToGCSOperator(" in rewritten
    assert "S3CopyObjectOperator(" not in rewritten
    assert 'src="s3://src-bucket/incoming/a.txt"' in rewritten
    assert "from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator" in rewritten
    assert "FileTransferOperator" in rewritten
    assert check_source(rewritten) == [finding for finding in check_source(rewritten) if finding.operator == "S3ToGCSOperator"]


def test_alias_and_attribute_calls_are_rewritten() -> None:
    aliased = '''\
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator as CopyObj

CopyObj(
    task_id="copy_one",
    source_bucket_name="src-bucket",
    source_bucket_key="a.txt",
    dest_bucket_name="dst-bucket",
    dest_bucket_key="b.txt",
)
'''
    rewritten, _ = fix_source(aliased)
    assert "CopyObj" not in rewritten
    assert 'src="s3://src-bucket/a.txt"' in rewritten

    attribute = '''\
import airflow.providers.amazon.aws.operators.s3 as s3ops

s3ops.S3CopyObjectOperator(
    task_id="copy_one",
    source_bucket_name="src-bucket",
    source_bucket_key="a.txt",
    dest_bucket_name="dst-bucket",
    dest_bucket_key="b.txt",
)
'''
    rewritten, findings = fix_source(attribute)
    assert any(finding.fixable for finding in findings)
    assert "import airflow.providers.amazon.aws.operators.s3 as s3ops" in rewritten
    assert "s3ops.S3CopyObjectOperator" not in rewritten
    assert 'dst="s3://dst-bucket/b.txt"' in rewritten


def test_multiline_import_keeps_the_unsafe_sibling() -> None:
    source = '''\
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CopyPrefixOperator,
)

S3CopyObjectOperator(
    task_id="copy_one",
    source_bucket_name="src-bucket",
    source_bucket_key="a.txt",
    dest_bucket_name="dst-bucket",
    dest_bucket_key="b.txt",
)
S3CopyPrefixOperator(
    task_id="prefix",
    source_bucket_prefix="in/",
    dest_bucket_prefix="out/",
)
'''
    rewritten, _ = fix_source(source)
    assert "S3CopyObjectOperator" not in rewritten
    assert "from airflow.providers.amazon.aws.operators.s3 import S3CopyPrefixOperator" in rewritten
    assert "S3CopyPrefixOperator(" in rewritten
    assert "FileTransferOperator(" in rewritten


def test_file_transfer_operator_and_the_example_dag_are_quiet() -> None:
    source = '''\
from airflow.providers.common.io.operators.file_transfer import FileTransferOperator
from aip132.file_transfer import PrefixFileTransferOperator

FileTransferOperator(task_id="c", src="s3://a/b", dst="s3://c/d", overwrite=True)
PrefixFileTransferOperator(task_id="p", src="s3://a/in/", dst="gs://b/out/")
'''
    assert check_source(source) == []
    example = Path("examples/aip132_s3_prefix_to_gcs.py").read_text(encoding="utf-8")
    assert check_source(example) == []


def test_cli_fixes_a_file_and_keeps_a_flag(tmp_path: Path, capsys) -> None:
    path = tmp_path / "dag.py"
    path.write_text(_SAFE + "\nS3ToGCSOperator = None\n", encoding="utf-8")
    # The assignment above is not an import, so it is not a diagnostic.
    assert main(["--fix", str(path)]) == 0
    assert path.read_text(encoding="utf-8") == _SAFE_FIXED + "\nS3ToGCSOperator = None\n"
    fixed_out = capsys.readouterr().out
    assert RULE in fixed_out
    assert "fixable" in fixed_out

    flagged = tmp_path / "flagged.py"
    flagged.write_text(
        "from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator\n",
        encoding="utf-8",
    )
    original = flagged.read_text(encoding="utf-8")
    assert main(["--fix", str(flagged)]) == 1
    assert flagged.read_text(encoding="utf-8") == original
    assert RULE in capsys.readouterr().out
