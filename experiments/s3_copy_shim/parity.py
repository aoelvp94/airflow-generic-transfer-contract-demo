#!/usr/bin/env python3
"""Behavioural parity: pair S3 copy operators vs shims on S3Mock."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(REPO / "experiments/file_contract"))
sys.path.insert(0, str(REPO / "src"))

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

from generic_transfer.adapters.emulators import load_emulator_env  # noqa: E402

load_emulator_env(REPO / "emulators.env")

# Real S3Hook talks to S3Mock through an env connection (no DB).
os.environ.setdefault(
    "AIRFLOW_CONN_AWS_DEFAULT",
    (
        '{"conn_type": "aws", "login": "'
        + os.environ.get("S3_ACCESS_KEY", "any")
        + '", "password": "'
        + os.environ.get("S3_SECRET_KEY", "any")
        + '", "extra": {"endpoint_url": "'
        + os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090")
        + '", "region_name": "'
        + os.environ.get("S3_REGION", "us-east-1")
        + '", "config_kwargs": {"s3": {"addressing_style": "path"}}}}'
    ),
)

from request_count import count_s3_client  # noqa: E402
from shim import make_copy_object_shim, make_copy_prefix_shim  # noqa: E402


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "any"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "any"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def _ensure_bucket(client, name: str) -> None:
    try:
        client.create_bucket(Bucket=name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists", "400"}:
            # S3Mock sometimes uses 200 / odd codes; retry head
            try:
                client.head_bucket(Bucket=name)
            except ClientError:
                raise exc


def _list_objects(client, bucket: str, prefix: str = "") -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            out[key] = body
    return out


def _wipe_prefix(client, bucket: str, prefix: str) -> None:
    keys = list(_list_objects(client, bucket, prefix))
    for key in keys:
        client.delete_object(Bucket=bucket, Key=key)


def _load_operators():
    import airflow.providers.amazon.aws.operators.s3 as _pip  # noqa: F401

    path = AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/operators/s3.py"
    name = "airflow.providers.amazon.aws.operators.s3"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    # Keep a pristine copy for the pair class, and a patched copy for the shim.
    # Loading twice from the same file into two module names.
    pair_name = "s3_ops_pair"
    shim_name = "s3_ops_shim"
    pair_spec = importlib.util.spec_from_file_location(pair_name, path)
    pair_mod = importlib.util.module_from_spec(pair_spec)
    sys.modules[pair_name] = pair_mod
    pair_spec.loader.exec_module(pair_mod)

    shim_spec = importlib.util.spec_from_file_location(shim_name, path)
    shim_mod = importlib.util.module_from_spec(shim_spec)
    sys.modules[shim_name] = shim_mod
    shim_spec.loader.exec_module(shim_mod)
    shim_mod.S3CopyObjectOperator = make_copy_object_shim(shim_mod)
    shim_mod.S3CopyPrefixOperator = make_copy_prefix_shim(shim_mod)
    return pair_mod, shim_mod


def _hook(client):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="aws_default")
    # Prefer the already-configured emulator client if get_conn works;
    # otherwise fall back to the boto3 client built in this script.
    try:
        hook.get_conn().list_buckets()
    except Exception:
        hook.get_conn = lambda: client  # type: ignore[method-assign]
    return hook


def _bind(op, hook):
    object.__setattr__(op, "hook", hook)
    # AwsBaseOperator may cache hook under extra names.
    for attr in ("_hook", "hook"):
        try:
            object.__setattr__(op, attr, hook)
        except Exception:
            pass
    return op


def _run_op(cls, hook, **kwargs):
    op = cls(task_id=kwargs.pop("task_id", "parity"), **kwargs)
    _bind(op, hook)
    with count_s3_client(hook.get_conn()) as counter:
        op.execute(None)
    return counter


def main() -> int:
    client = _client()
    # Probe
    client.list_buckets()

    run_id = uuid.uuid4().hex[:8]
    src = f"parity-src-{run_id}"
    dst = f"parity-dst-{run_id}"
    _ensure_bucket(client, src)
    _ensure_bucket(client, dst)

    pair_mod, shim_mod = _load_operators()
    hook = _hook(client)

    results = []

    def scenario(name: str, setup, run_pair, run_shim) -> None:
        setup()
        _wipe_prefix(client, dst, "")
        pair_counts = run_pair()
        pair_objs = _list_objects(client, dst)
        _wipe_prefix(client, dst, "")
        shim_counts = run_shim()
        shim_objs = _list_objects(client, dst)
        obj_match = pair_objs == shim_objs
        count_match = pair_counts.as_dict() == shim_counts.as_dict()
        results.append(
            {
                "scenario": name,
                "objects_match": obj_match,
                "request_counts_match": count_match,
                "pair_keys": sorted(pair_objs),
                "shim_keys": sorted(shim_objs),
                "pair_counts": pair_counts.as_dict(),
                "shim_counts": shim_counts.as_dict(),
                "byte_mismatch": [
                    k
                    for k in set(pair_objs) | set(shim_objs)
                    if pair_objs.get(k) != shim_objs.get(k)
                ],
            }
        )
        print(
            f"{name}: objects_match={obj_match} request_counts_match={count_match} "
            f"pair={pair_counts.as_dict()} shim={shim_counts.as_dict()}",
            flush=True,
        )

    # --- single-object copy ---
    def setup_single():
        _wipe_prefix(client, src, "")
        client.put_object(Bucket=src, Key="path1/data.txt", Body=b"input-one")

    scenario(
        "single_object_copy",
        setup_single,
        lambda: _run_op(
            pair_mod.S3CopyObjectOperator,
            hook,
            task_id="pair-single",
            source_bucket_name=src,
            source_bucket_key="path1/data.txt",
            dest_bucket_name=dst,
            dest_bucket_key="path2/data_copy.txt",
        ),
        lambda: _run_op(
            shim_mod.S3CopyObjectOperator,
            hook,
            task_id="shim-single",
            source_bucket_name=src,
            source_bucket_key="path1/data.txt",
            dest_bucket_name=dst,
            dest_bucket_key="path2/data_copy.txt",
        ),
    )

    # --- prefix copy including nested key ---
    def setup_prefix():
        _wipe_prefix(client, src, "")
        client.put_object(Bucket=src, Key="pre/a.txt", Body=b"aaa")
        client.put_object(Bucket=src, Key="pre/nested/b.txt", Body=b"bbb")
        client.put_object(Bucket=src, Key="other/c.txt", Body=b"ccc")

    scenario(
        "prefix_copy_nested",
        setup_prefix,
        lambda: _run_op(
            pair_mod.S3CopyPrefixOperator,
            hook,
            task_id="pair-prefix",
            source_bucket_name=src,
            source_bucket_prefix="pre/",
            dest_bucket_name=dst,
            dest_bucket_prefix="out/",
        ),
        lambda: _run_op(
            shim_mod.S3CopyPrefixOperator,
            hook,
            task_id="shim-prefix",
            source_bucket_name=src,
            source_bucket_prefix="pre/",
            dest_bucket_name=dst,
            dest_bucket_prefix="out/",
        ),
    )

    # --- overwrite: dest already has different bytes ---
    def setup_overwrite():
        _wipe_prefix(client, src, "")
        client.put_object(Bucket=src, Key="path1/data.txt", Body=b"new-bytes")

    def seed_dest_then(run):
        client.put_object(Bucket=dst, Key="path2/data_copy.txt", Body=b"OLD-BYTES")
        return run()

    scenario(
        "overwrite_existing_dest",
        setup_overwrite,
        lambda: seed_dest_then(
            lambda: _run_op(
                pair_mod.S3CopyObjectOperator,
                hook,
                task_id="pair-ow",
                source_bucket_name=src,
                source_bucket_key="path1/data.txt",
                dest_bucket_name=dst,
                dest_bucket_key="path2/data_copy.txt",
            )
        ),
        lambda: seed_dest_then(
            lambda: _run_op(
                shim_mod.S3CopyObjectOperator,
                hook,
                task_id="shim-ow",
                source_bucket_name=src,
                source_bucket_key="path1/data.txt",
                dest_bucket_name=dst,
                dest_bucket_key="path2/data_copy.txt",
            )
        ),
    )

    # --- folder marker under prefix ---
    def setup_marker():
        _wipe_prefix(client, src, "")
        client.put_object(Bucket=src, Key="pre/file.txt", Body=b"file")
        client.put_object(Bucket=src, Key="pre/dir/", Body=b"")

    scenario(
        "folder_marker_under_prefix",
        setup_marker,
        lambda: _run_op(
            pair_mod.S3CopyPrefixOperator,
            hook,
            task_id="pair-mark",
            source_bucket_name=src,
            source_bucket_prefix="pre/",
            dest_bucket_name=dst,
            dest_bucket_prefix="out/",
        ),
        lambda: _run_op(
            shim_mod.S3CopyPrefixOperator,
            hook,
            task_id="shim-mark",
            source_bucket_name=src,
            source_bucket_prefix="pre/",
            dest_bucket_name=dst,
            dest_bucket_prefix="out/",
        ),
    )

    # cleanup buckets (best-effort)
    for bkt in (src, dst):
        _wipe_prefix(client, bkt, "")
        try:
            client.delete_bucket(Bucket=bkt)
        except Exception:
            pass

    out_path = ROOT / "parity-results.json"
    out_path.write_text(json.dumps(results, indent=2) + "\n")
    print("Wrote", out_path, flush=True)
    failed = [r for r in results if not r["objects_match"] or not r["request_counts_match"]]
    if failed:
        print("PARITY FAIL:", [r["scenario"] for r in failed], flush=True)
        return 1
    print("PARITY PASS: all scored scenarios matched", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("PARITY ERROR:", type(exc).__name__, exc, flush=True)
        raise
