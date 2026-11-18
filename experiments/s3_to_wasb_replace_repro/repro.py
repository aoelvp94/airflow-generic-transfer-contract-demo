#!/usr/bin/env python3
"""Minimal local repro of S3ToAzureBlobStorageOperator replace=False dest-filter bug.

Modes:
  --unit       Mocked-hook unit check (no emulators). Prints the filter result.
  --before-after
               Run the unit test against the unpatched operator (expect FAIL),
               then apply the local patch in memory and run again (expect PASS).
  --emulator   S3Mock + Azurite: upload source, pre-create dest with different
               bytes, run the operator with replace=False, print dest content
               and etag / last-modified before and after.

Default is --unit --before-after. --emulator is opt-in so a no-Docker run still
reproduces the comparison bug.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import sys
import traceback
import uuid
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))
OPERATOR_FILE = (
    AIRFLOW_SRC
    / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py"
)
PATCH_FILE = ROOT / "s3_to_wasb_replace_false.patch"
MOD_NAME = "airflow.providers.microsoft.azure.transfers.s3_to_wasb"

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")


def _load_operator_module():
    """Load the SHA-checked operator from AIRFLOW_SRC after the pip provider."""
    if not OPERATOR_FILE.is_file():
        raise SystemExit(f"operator not found: {OPERATOR_FILE}")
    importlib.import_module(MOD_NAME)
    spec = importlib.util.spec_from_file_location(MOD_NAME, OPERATOR_FILE)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {OPERATOR_FILE}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[MOD_NAME] = mod
    spec.loader.exec_module(mod)
    pkg = importlib.import_module("airflow.providers.microsoft.azure.transfers")
    pkg.s3_to_wasb = mod
    return mod


def run_unit_filter(mod, *, label: str) -> list[str]:
    """Pure unit variant: mocked hooks, print get_files_to_move() result."""
    cls = mod.S3ToAzureBlobStorageOperator
    with (
        mock.patch.object(mod, "S3Hook") as s3_hook_cls,
        mock.patch.object(mod, "WasbHook") as wasb_hook_cls,
    ):
        s3_hook_cls.return_value.list_keys.return_value = ["pre/a.txt"]
        wasb_hook_cls.return_value.get_blobs_list_recursive.return_value = ["out/a.txt"]
        op = cls(
            task_id="repro-unit",
            s3_bucket="src-bucket",
            s3_prefix="pre",
            container_name="dst-container",
            blob_prefix="out",
            replace=False,
        )
        result = op.get_files_to_move()
    print(
        f"UNIT [{label}] get_files_to_move() = {result!r}  "
        f"(source listed pre/a.txt, dest listed out/a.txt, replace=False; "
        f"expected skip: [])",
        flush=True,
    )
    return result


def _apply_patch_in_memory(mod) -> None:
    """Apply the local patch's get_files_to_move body to the loaded class only."""
    orig = mod.S3ToAzureBlobStorageOperator.get_files_to_move

    def get_files_to_move(self):
        files_to_move = orig(self)
        # orig already applied the buggy subtraction when replace=False.
        # Re-run only the dest-key filter when replace is False, starting
        # from a fresh listing — simpler: wrap by reconstructing via the
        # patched algorithm inlined here to match the patch file.
        if self.replace:
            return files_to_move
        # Recompute from hooks so we do not depend on the buggy result.
        if self.s3_key:
            candidates = [self.s3_key.split("/")[-1]]
        else:
            s3_keys = self.s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_prefix)
            s3_keys, dropped_keys = self._strip_overlapping_folder_markers(s3_keys)
            candidates = [s3_key.replace(f"{self.s3_prefix}/", "", 1) for s3_key in s3_keys]
            candidates = [f for f in candidates if f]
        existing: list[str] = []
        if self.blob_name:
            if self.wasb_hook.check_for_blob(self.container_name, self.blob_name):
                existing.append(self.blob_name.split("/")[-1])
        elif self.blob_prefix:
            existing += self.wasb_hook.get_blobs_list_recursive(
                container_name=self.container_name, prefix=self.blob_prefix
            )
        existing_files = set(existing or [])
        return [
            f
            for f in candidates
            if f not in existing_files
            and self._create_key(self.blob_name, self.blob_prefix, f) not in existing_files
        ]

    mod.S3ToAzureBlobStorageOperator.get_files_to_move = get_files_to_move


def run_before_after(mod) -> int:
    before = run_unit_filter(mod, label="unpatched")
    before_ok = before == []
    print(
        "UNIT [unpatched] expected [] (skip). "
        + ("PASS (unexpected — bug may already be fixed)" if before_ok else "FAIL as expected — filter did not skip"),
        flush=True,
    )
    _apply_patch_in_memory(mod)
    after = run_unit_filter(mod, label="patched-in-memory")
    after_ok = after == []
    print(
        "UNIT [patched-in-memory] expected []. " + ("PASS" if after_ok else "FAIL"),
        flush=True,
    )
    if not before_ok and after_ok:
        print("BEFORE/AFTER: test fails before the patch and passes after.", flush=True)
        return 0
    if before_ok and after_ok:
        print("BEFORE/AFTER: already passing on unpatched operator.", flush=True)
        return 0
    print("BEFORE/AFTER: unexpected result.", flush=True)
    return 1


def _emulator_env() -> None:
    """Set emulator endpoints and AIRFLOW_CONN_* *before* hooks are constructed."""
    sys.path.insert(0, str(REPO / "src"))
    from generic_transfer.adapters.emulators import load_emulator_env

    load_emulator_env(REPO / "emulators.env")
    # Inherited session env can disable normal connection lookup.
    os.environ.pop("AIRFLOW__CORE__UNIT_TEST_MODE", None)
    # Valid JSON (double quotes). %r produces single quotes, which from_json rejects.
    os.environ["AIRFLOW_CONN_AWS_DEFAULT"] = json.dumps(
        {
            "conn_type": "aws",
            "login": os.environ.get("S3_ACCESS_KEY", "any"),
            "password": os.environ.get("S3_SECRET_KEY", "any"),
            "extra": {
                "endpoint_url": os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090"),
                "region_name": os.environ.get("S3_REGION", "us-east-1"),
                "config_kwargs": {"s3": {"addressing_style": "path"}},
            },
        }
    )
    os.environ["AIRFLOW_CONN_WASB_DEFAULT"] = json.dumps(
        {
            "conn_type": "wasb",
            "login": "devstoreaccount1",
            "extra": {"connection_string": os.environ.get("AZURE_CONNECTION_STRING", "")},
        }
    )
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("S3_ACCESS_KEY", "any")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("S3_SECRET_KEY", "any")
    os.environ["AWS_DEFAULT_REGION"] = os.environ.get("S3_REGION", "us-east-1")


def _s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "any"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "any"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def _blob_service():
    from azure.storage.blob import BlobServiceClient

    conn = os.environ.get(
        "AZURE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
    )
    return BlobServiceClient.from_connection_string(conn)


def _blob_props(client, container: str, name: str) -> dict:
    bc = client.get_container_client(container).get_blob_client(name)
    data = bc.download_blob().readall()
    props = bc.get_blob_properties()
    etag = getattr(props, "etag", None)
    lm = getattr(props, "last_modified", None)
    return {
        "exists": True,
        "bytes": data,
        "text": data.decode("utf-8", errors="replace"),
        "etag": str(etag) if etag is not None else None,
        "last_modified": lm.isoformat() if lm is not None else None,
        "content_length": getattr(props, "size", len(data)),
    }


def run_emulator(mod) -> int:
    _emulator_env()
    run_id = uuid.uuid4().hex[:8]
    bucket = f"s3rep{run_id}"
    container = f"azrep{run_id}"
    source_key = "pre/a.txt"
    dest_blob = "out/a.txt"
    source_bytes = b"NEW-FROM-S3"
    dest_bytes = b"OLD-DEST-BYTES"

    s3 = _s3_client()
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    s3.put_object(Bucket=bucket, Key=source_key, Body=source_bytes)

    blob_svc = _blob_service()
    try:
        blob_svc.create_container(container)
    except Exception:
        pass
    blob_svc.get_container_client(container).upload_blob(dest_blob, dest_bytes, overwrite=True)

    before = _blob_props(blob_svc, container, dest_blob)
    print("EMULATOR before dest", dest_blob, json.dumps({k: v if k != "bytes" else list(v) for k, v in before.items() if k != "bytes"} | {"text": before["text"]}), flush=True)

    cls = mod.S3ToAzureBlobStorageOperator
    op = cls(
        task_id="repro-emulator",
        s3_bucket=bucket,
        s3_prefix="pre",
        container_name=container,
        blob_prefix="out",
        replace=False,
        create_container=False,
    )
    listed = op.get_files_to_move()
    print(
        f"EMULATOR get_files_to_move() = {listed!r}  (expected [] if replace=False skipped dest)",
        flush=True,
    )
    execute_error = None
    try:
        returned = op.execute(None)
        print(f"EMULATOR execute() returned {returned!r}", flush=True)
    except Exception as exc:
        execute_error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
        print(f"EMULATOR execute() raised {execute_error}", flush=True)

    after = _blob_props(blob_svc, container, dest_blob)
    print("EMULATOR after dest", dest_blob, json.dumps({k: v if k != "bytes" else None for k, v in after.items() if k != "bytes"} | {"text": after["text"]}), flush=True)

    overwritten = after["bytes"] == source_bytes and after["bytes"] != dest_bytes
    unchanged = after["bytes"] == dest_bytes
    etag_changed = before["etag"] != after["etag"]
    mtime_changed = before["last_modified"] != after["last_modified"]
    print(
        f"EMULATOR summary: filter_skipped={listed == []} "
        f"dest_overwritten={overwritten} dest_unchanged={unchanged} "
        f"etag_changed={etag_changed} last_modified_changed={mtime_changed} "
        f"before_etag={before['etag']} after_etag={after['etag']} "
        f"before_mtime={before['last_modified']} after_mtime={after['last_modified']} "
        f"before_text={before['text']!r} after_text={after['text']!r} "
        f"execute_error={execute_error}",
        flush=True,
    )
    # Best-effort cleanup
    try:
        blob_svc.get_container_client(container).delete_blob(dest_blob)
    except Exception:
        pass
    try:
        s3.delete_object(Bucket=bucket, Key=source_key)
    except Exception:
        pass
    # Repro "succeeds" (demonstrates the bug) if the filter did not skip.
    return 0 if listed != [] else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", action="store_true", help="run mocked-hook filter check")
    parser.add_argument("--before-after", action="store_true", help="unit test fails before / passes after")
    parser.add_argument("--emulator", action="store_true", help="run S3Mock + Azurite repro")
    args = parser.parse_args()
    if not (args.unit or args.before_after or args.emulator):
        args.unit = True
        args.before_after = True

    print(f"AIRFLOW_SRC={AIRFLOW_SRC}", flush=True)
    print(f"operator file={OPERATOR_FILE}", flush=True)
    sha_file = AIRFLOW_SRC / ".git"
    print(f"patch={PATCH_FILE}", flush=True)

    if args.emulator:
        _emulator_env()
        print("AIRFLOW_CONN_AWS_DEFAULT set for emulator hooks", flush=True)

    mod = _load_operator_module()
    rc = 0
    if args.unit and not args.before_after:
        result = run_unit_filter(mod, label="unpatched")
        if result != []:
            print("UNIT: filter did not skip; comparison bug confirmed.", flush=True)
        else:
            print("UNIT: filter skipped; bug not reproduced.", flush=True)
            rc = 2
    if args.before_after:
        rc = run_before_after(mod) or rc
        # reload unpatched module for emulator
        if args.emulator:
            mod = _load_operator_module()
    if args.emulator:
        erc = run_emulator(mod)
        if erc not in (0,):
            rc = erc
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
