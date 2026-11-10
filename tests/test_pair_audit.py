"""Unit tests for the transfer-operator classifier (synthetic modules only)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
import audit_transfers as audit


def _write(root: Path, rel: str, body: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


CLEAN_FILE = '''
class BaseOperator:
    pass

class LocalFilesystemToS3Operator(BaseOperator):
    template_fields = ("filename", "dest_key", "aws_conn_id")

    def __init__(self, filename: str, dest_key: str, dest_bucket: str | None = None,
                 aws_conn_id: str = "aws_default", replace: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.filename = filename

    def execute(self, context):
        pass
'''

CAPABILITY_FILE = '''
class BaseOperator:
    pass

class SFTPToS3Operator(BaseOperator):
    template_fields = ("sftp_path", "s3_key")

    def __init__(self, sftp_path: str, s3_bucket: str, s3_key: str,
                 sftp_conn_id: str = "sftp_default", aws_conn_id: str = "aws_default",
                 replace: bool = False, gzip: bool = False, acl_policy: str | None = None, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pass
'''

SPECIAL_SQL = '''
class BaseOperator:
    pass

class BaseSQLToGCSOperator(BaseOperator):
    template_fields = ("sql", "bucket", "filename", "schema_filename", "partition_columns")

    def __init__(self, sql: str, bucket: str, filename: str,
                 schema_filename: str | None = None,
                 approx_max_file_size_bytes: int = 1900000000,
                 export_format: str = "json", gzip: bool = False,
                 schema: list | None = None, partition_columns: list | None = None,
                 gcp_conn_id: str = "google_cloud_default", **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pass
'''

SPECIAL_CHILD = '''
from .sql_to_gcs import BaseSQLToGCSOperator

class MySQLToGCSOperator(BaseSQLToGCSOperator):
    template_fields = (*BaseSQLToGCSOperator.template_fields, "mysql_conn_id")

    def __init__(self, mysql_conn_id: str = "mysql_default", **kwargs):
        super().__init__(**kwargs)
'''

NOTIFICATION = '''
class BaseOperator:
    pass

class SqlToSlackWebhookOperator(BaseOperator):
    template_fields = ("sql", "slack_message")

    def __init__(self, sql: str, sql_conn_id: str, slack_webhook_conn_id: str,
                 slack_message: str, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pass
'''

SERVER_SIDE_FILE = '''
class BaseOperator:
    pass

class S3ToGCSOperator(BaseOperator):
    """Optionally submits a Google Cloud Storage Transfer Service job."""

    def __init__(self, bucket: str, dest_gcs: str, prefix: str | None = None,
                 replace: bool = False, gzip: bool = False, deferrable: bool = False, **kwargs):
        super().__init__(**kwargs)

    def transfer_files_async(self):
        pass

    def submit_transfer_jobs(self):
        pass

    def execute(self, context):
        pass

    def execute_complete(self, context, event):
        pass
'''

WAREHOUSE = '''
class BaseOperator:
    pass

class S3ToRedshiftOperator(BaseOperator):
    def __init__(self, table: str, s3_bucket: str, s3_key: str,
                 copy_options: list | None = None, upsert_keys: list | None = None, **kwargs):
        super().__init__(**kwargs)

    def _build_copy_query(self):
        return "COPY table FROM 's3://...'"

    def execute(self, context):
        pass
'''

LISTING_ONLY = '''
class BaseOperator:
    pass

class GCSToS3Operator(BaseOperator):
    def __init__(self, gcs_bucket: str, prefix: str, dest_s3_key: str,
                 gcp_conn_id: str = "google_cloud_default",
                 dest_aws_conn_id: str = "aws_default",
                 replace: bool = False, match_glob: str | None = None, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pass
'''

CSV_DELIMITER = '''
class BaseOperator:
    pass

class OracleToAzureDataLakeOperator(BaseOperator):
    def __init__(self, filename: str, azure_data_lake_path: str, sql: str,
                 oracle_conn_id: str = "oracle_default",
                 azure_data_lake_conn_id: str = "azure_data_lake_default",
                 delimiter: str = ",", encoding: str = "utf-8", **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pass
'''


@pytest.fixture
def providers_root(tmp_path: Path) -> Path:
    """A tiny providers/ tree with one module per classification bucket."""
    root = tmp_path / "providers"
    _write(root, "amazon/src/airflow/providers/amazon/aws/transfers/local_to_s3.py", CLEAN_FILE)
    _write(root, "amazon/src/airflow/providers/amazon/aws/transfers/sftp_to_s3.py", CAPABILITY_FILE)
    _write(root, "amazon/src/airflow/providers/amazon/aws/transfers/s3_to_redshift.py", WAREHOUSE)
    _write(root, "google/src/airflow/providers/google/cloud/transfers/sql_to_gcs.py", SPECIAL_SQL)
    _write(root, "google/src/airflow/providers/google/cloud/transfers/mysql_to_gcs.py", SPECIAL_CHILD)
    _write(root, "google/src/airflow/providers/google/cloud/transfers/s3_to_gcs.py", SERVER_SIDE_FILE)
    _write(root, "google/src/airflow/providers/google/cloud/transfers/gcs_to_s3_thin.py", LISTING_ONLY)
    _write(root, "slack/src/airflow/providers/slack/transfers/sql_to_slack_webhook.py", NOTIFICATION)
    _write(
        root,
        "microsoft/azure/src/airflow/providers/microsoft/azure/transfers/oracle_to_azure_data_lake.py",
        CSV_DELIMITER,
    )
    _write(
        root,
        "common/io/src/airflow/providers/common/io/operators/file_transfer.py",
        '''
class BaseOperator:
    pass

class FileTransferOperator(BaseOperator):
    template_fields = ("src", "dst", "source_conn_id")

    def __init__(self, src: str, dst: str, source_conn_id: str | None = None,
                 dest_conn_id: str | None = None, overwrite: bool = False, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pass
''',
    )
    # Noise that must be ignored
    _write(root, "amazon/src/airflow/providers/amazon/aws/transfers/__init__.py", "")
    _write(
        root,
        "amazon/tests/unit/amazon/aws/transfers/test_local_to_s3.py",
        "class FakeToS3Operator:\n    pass\n",
    )
    _write(
        root,
        "google/src/airflow/providers/google/cloud/operators/speech_to_text.py",
        "class CloudSpeechToTextRecognizeSpeechOperator:\n    pass\n",
    )
    _write(
        root,
        "edge3/src/airflow/providers/edge3/migrations/versions/0002_3_2_0_add_concurrency_to_edge_worker.py",
        "def upgrade():\n    pass\n",
    )
    return root


def _records_by_name(providers_root: Path) -> dict[str, object]:
    records: list = []
    module_texts: dict[str, str] = {}
    included, skipped = audit.discover_modules(providers_root)
    for path in included:
        rel = path.relative_to(providers_root).as_posix()
        module_texts[rel] = path.read_text(encoding="utf-8")
        records.extend(audit.parse_module(path, providers_root=providers_root))
    audit.apply_inheritance(records, module_texts=module_texts)
    return {r.class_name: r for r in records}, included, skipped


def test_discover_includes_transfers_and_file_transfer_skips_noise(providers_root: Path):
    included, skipped = audit.discover_modules(providers_root)
    rels = {p.relative_to(providers_root).as_posix() for p in included}
    assert any(p.endswith("transfers/local_to_s3.py") for p in rels)
    assert any(p.endswith("operators/file_transfer.py") for p in rels)
    assert not any("tests/" in p or p.endswith("__init__.py") for p in rels)
    reasons = " ".join(s["path"] + s["reason"] for s in skipped)
    assert "speech_to_text" in reasons
    assert "0002_3_2_0_add_concurrency_to_edge_worker" in reasons


def test_maps_cleanly_for_thin_file_pairs(providers_root: Path):
    by_name, _, _ = _records_by_name(providers_root)
    clean = by_name["LocalFilesystemToS3Operator"]
    assert clean.classification == "maps_cleanly"
    assert clean.kind == "file→file"
    assert "replace" in clean.flags
    listing = by_name["GCSToS3Operator"]
    assert listing.classification == "maps_cleanly"
    assert "wildcard_or_prefix" in listing.flags
    assert "replace" in listing.flags
    ft = by_name["FileTransferOperator"]
    assert ft.classification == "maps_cleanly"
    assert ft.kind == "file→file"
    # CSV delimiter must not be mistaken for a listing delimiter, and stays clean.
    oracle = by_name["OracleToAzureDataLakeOperator"]
    assert oracle.classification == "maps_cleanly"
    assert "wildcard_or_prefix" not in oracle.flags


def test_needs_capability_for_gzip_acl_and_server_side_copy(providers_root: Path):
    by_name, _, _ = _records_by_name(providers_root)
    sftp = by_name["SFTPToS3Operator"]
    assert sftp.classification == "needs_capability"
    assert "compression" in sftp.classification_reasons
    assert "acl_or_extra_args" in sftp.classification_reasons
    # Storage Transfer Service on a file→file operator is a capability, not warehouse-special.
    s3gcs = by_name["S3ToGCSOperator"]
    assert s3gcs.classification == "needs_capability"
    assert "server_side_copy" in s3gcs.flags
    assert "deferrable" in s3gcs.flags
    assert s3gcs.deferrable is True
    assert "warehouse_copy_job" not in s3gcs.classification_reasons


def test_special_for_partitioning_notification_and_warehouse(providers_root: Path):
    by_name, _, _ = _records_by_name(providers_root)
    base = by_name["BaseSQLToGCSOperator"]
    assert base.is_base
    assert base.classification == "special"
    assert "partitioning_or_splitting" in base.classification_reasons
    # Subclass only adds a conn id; inherited partition/schema args still make it special.
    child = by_name["MySQLToGCSOperator"]
    assert child.is_base is False
    assert child.classification == "special"
    assert "partition_columns" in child.inherited_arg_names
    assert "partitioning_or_splitting" in child.classification_reasons
    assert "schema_filename" in child.template_fields
    slack = by_name["SqlToSlackWebhookOperator"]
    assert slack.classification == "special"
    assert slack.kind == "sql→message"
    assert "notification" in slack.classification_reasons
    rs = by_name["S3ToRedshiftOperator"]
    assert rs.classification == "special"
    assert "warehouse_copy_job" in rs.classification_reasons
    assert rs.kind == "file→sql"


def test_classify_priority_is_special_then_capability_then_clean():
    assert audit.classify(["compression", "partitioning_or_splitting"]) == (
        "special",
        ["partitioning_or_splitting"],
    )
    assert audit.classify(["compression", "replace"]) == ("needs_capability", ["compression"])
    assert audit.classify(["replace", "wildcard_or_prefix"]) == ("maps_cleanly", [])
