"""OpenLineage facets for PrefixFileTransferOperator. No Docker."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("AIRFLOW_HOME", str(_ROOT / ".airflow-aip132"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

import pytest

pytest.importorskip("airflow.providers.common.io.operators.file_transfer")
pytest.importorskip("airflow.providers.openlineage.extractors")

from airflow.providers.common.io.operators.file_transfer import FileTransferOperator
from airflow.sdk import DAG, ObjectStoragePath

from aip132.file_transfer import PrefixFileTransferOperator, child_path, resolve_storage_path
from aip132.lineage import identity_uri, openlineage_identity

pytestmark = pytest.mark.aip132


def _ids(lineage) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    return (
        [(item.namespace, item.name) for item in lineage.inputs],
        [(item.namespace, item.name) for item in lineage.outputs],
    )


def _operator(src: str, dst: str, **kwargs) -> PrefixFileTransferOperator:
    dag_id = "aip132_ol_" + uuid.uuid4().hex[:12]
    with DAG(dag_id=dag_id, schedule=None, start_date=datetime(2026, 1, 1), catchup=False):
        return PrefixFileTransferOperator(task_id="copy", src=src, dst=dst, **kwargs)


class _XCom:
    def __init__(self, value):
        self.value = value

    def xcom_pull(self, task_ids=None, key="return_value", **kwargs):
        assert key == "return_value"
        return self.value


def test_single_file_start_matches_upstream(tmp_path: Path):
    src = tmp_path / "one.txt"
    dst = tmp_path / "out.txt"
    src.write_bytes(b"payload")
    parent = FileTransferOperator(task_id="up", src=src.as_uri(), dst=dst.as_uri(), overwrite=True)
    op = _operator(src.as_uri(), dst.as_uri(), overwrite=True)
    assert _ids(op.get_openlineage_facets_on_start()) == _ids(parent.get_openlineage_facets_on_start())
    assert op.execute({}) is None
    assert _ids(op.get_openlineage_facets_on_complete(None)) == _ids(
        parent.get_openlineage_facets_on_start()
    )


def test_s3_and_gs_single_file_matches_upstream_and_the_spec():
    src = "s3://src-bucket/dir/a.txt"
    dst = "gs://dst-bucket/dir/b.txt"
    parent = FileTransferOperator(task_id="up", src=src, dst=dst)
    op = _operator(src, dst)
    assert _ids(op.get_openlineage_facets_on_start()) == _ids(parent.get_openlineage_facets_on_start())
    assert _ids(op.get_openlineage_facets_on_start()) == (
        [("s3://src-bucket", "dir/a.txt")],
        [("gs://dst-bucket", "dir/b.txt")],
    )


def test_prefix_start_is_one_dataset_per_side_and_does_not_list():
    op = _operator("s3://src-bucket/sales/", "gs://dst-bucket/sales/")
    inputs, outputs = _ids(op.get_openlineage_facets_on_start())
    assert inputs == [("s3://src-bucket", "sales/")]
    assert outputs == [("gs://dst-bucket", "sales/")]


def test_gcs_scheme_is_reported_as_gs():
    op = _operator("gcs://bucket/dir/a.txt", "s3a://bucket/dir/b.txt")
    inputs, outputs = _ids(op.get_openlineage_facets_on_start())
    assert inputs == [("gs://bucket", "dir/a.txt")]
    assert outputs == [("s3://bucket", "dir/b.txt")]


def test_abfs_and_wasb_follow_the_openlineage_schemes():
    # Parsed from the URI text. ObjectStoragePath treats the container as a
    # connection id, and ``wasb`` is not a filesystem it can construct.
    assert identity_uri("abfs://container@devstoreaccount1.dfs.core.windows.net/dir/a.txt") == (
        "abfss://container@devstoreaccount1.dfs.core.windows.net",
        "dir/a.txt",
    )
    assert identity_uri("wasb://container@devstoreaccount1.blob.core.windows.net/dir/a.txt") == (
        "wasbs://container@devstoreaccount1.blob.core.windows.net",
        "dir/a.txt",
    )
    assert identity_uri("abfs://src-container/dir/a.txt") == ("abfss://src-container", "dir/a.txt")
    assert identity_uri("wasbs://src-container/dir/a.txt") == ("wasbs://src-container", "dir/a.txt")
    op = _operator(
        "abfs://container@devstoreaccount1.dfs.core.windows.net/dir/a.txt",
        "wasb://container@devstoreaccount1.blob.core.windows.net/out/b.txt",
    )
    assert _ids(op.get_openlineage_facets_on_start()) == (
        [("abfss://container@devstoreaccount1.dfs.core.windows.net", "dir/a.txt")],
        [("wasbs://container@devstoreaccount1.blob.core.windows.net", "out/b.txt")],
    )
    bare = ObjectStoragePath("abfs://src-container/dir/a.txt")
    assert openlineage_identity(bare) == ("abfss://src-container", "dir/a.txt")

    class _FS:
        account_name = "devstoreaccount1"

    bare.__wrapped__._fs_cached = _FS()
    assert openlineage_identity(bare) == (
        "abfss://src-container@devstoreaccount1.dfs.core.windows.net",
        "dir/a.txt",
    )


def test_complete_names_each_copied_file_and_skips_directories(tmp_path: Path):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    (src / "nested").mkdir(parents=True)
    dst.mkdir()
    (src / "a.txt").write_bytes(b"alpha")
    (src / "nested" / "b.txt").write_bytes(b"beta")
    src_uri = src.resolve().as_uri() + "/"
    dst_uri = dst.resolve().as_uri() + "/"
    op = _operator(src_uri, dst_uri, overwrite=True, folder_markers="copy")
    copied = op.execute({})
    assert copied is not None
    _inputs, outputs = _ids(op.get_openlineage_facets_on_complete(None))
    assert [name for _ns, name in outputs] == copied
    assert all(not name.endswith("/") for _ns, name in outputs)
    assert outputs[0][0] == "file"
    assert any(name.endswith("a.txt") for _ns, name in _inputs)
    assert any(name.endswith("nested/b.txt") for _ns, name in _inputs)


def test_complete_includes_a_copied_marker_key(tmp_path: Path, monkeypatch):
    from aip132 import file_transfer as ft

    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    (src / "a.txt").write_bytes(b"alpha")
    src_uri = src.resolve().as_uri() + "/"
    dst_uri = dst.resolve().as_uri() + "/"
    root = resolve_storage_path(src_uri, None)
    prefix = ft._fs_prefix(root.fs, root.path)
    real_list = ft.list_prefix_once

    def _listing(path, _real=real_list, _prefix=prefix):
        rows = _real(path)
        rows.append({"name": f"{_prefix}/hold", "size": 0, "type": "file", "is_marker": True})
        return rows

    monkeypatch.setattr(ft, "list_prefix_once", _listing)
    op = _operator(src_uri, dst_uri, overwrite=True, folder_markers="copy")
    copied = op.execute({})
    assert copied is not None
    assert any(key.endswith("hold") for key in copied)
    assert not any(key.endswith("hold/") for key in copied)
    _inputs, outputs = _ids(op.get_openlineage_facets_on_complete(None))
    assert [name for _ns, name in outputs] == copied
    assert any(name.endswith("hold") for _ns, name in outputs)
    assert (dst / "hold").read_bytes() == b""


def test_complete_reconstructs_pairs_from_xcom_without_execute(tmp_path: Path):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    src_uri = src.resolve().as_uri() + "/"
    dst_uri = dst.resolve().as_uri() + "/"
    dest = child_path(resolve_storage_path(dst_uri, None), "a.txt")
    op = _operator(src_uri, dst_uri, overwrite=True, keep_structure=True)
    inputs, outputs = _ids(op.get_openlineage_facets_on_complete(_XCom([dest.key])))
    assert outputs == [("file", dest.key)]
    assert inputs[0][0] == "file"
    assert inputs[0][1].endswith("a.txt")
    # No return value and no execute: the start datasets (the prefixes).
    fresh = _operator(src_uri, dst_uri, overwrite=True)
    assert _ids(fresh.get_openlineage_facets_on_complete(_XCom(None))) == _ids(
        fresh.get_openlineage_facets_on_start()
    )
