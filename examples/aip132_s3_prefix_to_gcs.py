"""Demo Dag: copy an S3 prefix to GCS with folder markers skipped.

The task is ``PrefixFileTransferOperator``. A ``@task`` can call the same
helper without the operator:

.. code-block:: python

    from airflow.sdk import task

    from examples.aip132_s3_prefix_to_gcs import copy_s3_prefix_to_gcs

    copy_s3_prefix_to_gcs = task(copy_s3_prefix_to_gcs)

Run this file to execute the copy against the local emulators (``just up``,
then ``just aip132-demo``). Importing the module only defines the Dag.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
# These defaults apply when the file is executed as a script (`just aip132-demo`).
# Importing the Dag into a scheduler must not turn unit-test mode on.
if __name__ == "__main__":
    os.environ.setdefault("AIRFLOW_HOME", str(_ROOT / ".airflow-aip132"))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

from airflow.sdk import DAG

from aip132.file_transfer import PrefixFileTransferOperator, copy_files

PREFIX = "aip132-demo/sales"
SRC_URL = f"s3://src-bucket/{PREFIX}/"
DST_URL = f"gs://dst-bucket/{PREFIX}/"


def copy_s3_prefix_to_gcs(
    src: str = SRC_URL,
    dst: str = DST_URL,
    *,
    source_conn_id: str | None = "aip132_s3",
    dest_conn_id: str | None = "aip132_gcs",
) -> list[str] | None:
    """Body of a ``@task``. Same arguments and behaviour as the operator below."""
    return copy_files(
        src,
        dst,
        source_conn_id=source_conn_id,
        dest_conn_id=dest_conn_id,
        overwrite=True,
        keep_structure=True,
        folder_markers="skip",
    )


dag = DAG(
    dag_id="aip132_s3_prefix_to_gcs",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Copy an S3 prefix to GCS. Folder markers are skipped. Copied keys are returned.",
)
with dag:
    copy_s3_prefix_to_gcs_task = PrefixFileTransferOperator(
        task_id="copy_s3_prefix_to_gcs",
        src=SRC_URL,
        dst=DST_URL,
        source_conn_id="aip132_s3",
        dest_conn_id="aip132_gcs",
        overwrite=True,
        keep_structure=True,
        folder_markers="skip",
    )


def _emulators_ready() -> bool:
    import urllib.request

    checks = (
        "http://127.0.0.1:19090/",
        "http://127.0.0.1:14443/storage/v1/b",
        "http://127.0.0.1:10000/",
    )
    for url in checks:
        try:
            urllib.request.urlopen(url, timeout=1)  # noqa: S310
        except Exception as exc:
            code = getattr(exc, "code", None)
            if code not in {400, 403, 404, 405}:
                return False
    return True


def run_demo() -> list[str] | None:
    """Seed an S3 prefix, copy it to GCS, and check markers were not copied.

    Returns the copied destination keys, or ``None`` when the emulators are down.
    """
    print(f"dag_id={dag.dag_id}")
    print(f"task_id={copy_s3_prefix_to_gcs_task.task_id}")
    print(f"src={SRC_URL}")
    print(f"dst={DST_URL}")
    if not _emulators_ready():
        print("S3 to GCS copy was not run: object-store emulators are not up (just up).")
        return None

    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
    from airflow.sdk.io import attach

    from generic_transfer.adapters.emulators import (
        clear_prefix,
        ensure_bucket,
        get_bytes,
        list_keys_native,
        load_emulator_env,
        make_fs,
        seed_tree_with_markers,
    )

    load_emulator_env()
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    src = make_fs("s3", bucket="src-bucket")
    dst = make_fs("gcs", bucket="dst-bucket")
    ensure_bucket(src)
    ensure_bucket(dst)
    attach(protocol="s3", conn_id="aip132_s3", fs=src.fs)
    attach(protocol="gs", conn_id="aip132_gcs", fs=dst.fs)
    clear_prefix(src, PREFIX)
    clear_prefix(dst, PREFIX)
    seed_tree_with_markers(src, PREFIX)
    try:
        src.fs.invalidate_cache()
        dst.fs.invalidate_cache()
    except Exception:
        pass

    copied = copy_s3_prefix_to_gcs_task.execute({})
    assert isinstance(copied, list)
    native = list_keys_native(dst, PREFIX)
    files = sorted(key for key in native if not key.endswith("/"))
    markers = [key for key in native if key.endswith("/")]
    expected = sorted([f"{PREFIX}/a.json", f"{PREFIX}/nested/b.json"])
    print(f"copied={copied}")
    print(f"dest_files={files}")
    print(f"dest_markers={markers}")
    if files != expected or markers:
        raise SystemExit(
            f"demo copy mismatch: files={files} markers={markers} copied={copied} expected={expected}"
        )
    if get_bytes(dst, f"{PREFIX}/a.json") != b'{"id":1}':
        raise SystemExit("demo copy mismatch: a.json bytes")
    if get_bytes(dst, f"{PREFIX}/nested/b.json") != b'{"id":2}':
        raise SystemExit("demo copy mismatch: nested/b.json bytes")
    print("demo ok: S3 prefix copied to GCS, folder markers skipped, keys returned")
    return copied


if __name__ == "__main__":
    result = run_demo()
    if result is None:
        raise SystemExit(0)
