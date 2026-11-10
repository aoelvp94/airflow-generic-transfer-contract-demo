#!/usr/bin/env python3
"""Behavioural parity: closest Azure transfer operators vs shims on Azurite (+ S3Mock)."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import uuid
from collections import Counter
from pathlib import Path

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.core.pipeline.policies import SansIOHTTPPolicy
from azure.storage.blob import BlobServiceClient
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

os.environ.setdefault(
    "AIRFLOW_CONN_WASB_DEFAULT",
    (
        '{"conn_type": "wasb", "login": "devstoreaccount1",'
        ' "extra": {"connection_string": %r}}'
        % os.environ.get("AZURE_CONNECTION_STRING", "")
    ),
)
os.environ.setdefault(
    "AIRFLOW_CONN_AWS_DEFAULT",
    (
        '{"conn_type": "aws", "login": %r, "password": %r,'
        ' "extra": {"endpoint_url": %r, "region_name": %r,'
        ' "config_kwargs": {"s3": {"addressing_style": "path"}}}}'
        % (
            os.environ.get("S3_ACCESS_KEY", "any"),
            os.environ.get("S3_SECRET_KEY", "any"),
            os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090"),
            os.environ.get("S3_REGION", "us-east-1"),
        )
    ),
)

from request_count import RequestCounter, count_s3_client  # noqa: E402
from shim import make_local_to_wasb_shim, make_s3_to_wasb_shim  # noqa: E402


CONN = os.environ.get(
    "AZURE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
)


class AzureCountPolicy(SansIOHTTPPolicy):
    def __init__(self, counter: RequestCounter) -> None:
        self.counter = counter

    def on_request(self, request):  # type: ignore[no-untyped-def]
        http = request.http_request
        method = getattr(http, "method", "?")
        path = getattr(http, "url", "")
        self.counter.bump(f"{method}")
        return super().on_request(request)


def _blob_service(counter: RequestCounter | None = None) -> BlobServiceClient:
    kwargs = {}
    if counter is not None:
        kwargs["session"] = None
    client = BlobServiceClient.from_connection_string(CONN)
    if counter is not None:
        # Attach a policy by wrapping _pipeline
        try:
            client._config.user_agent_policy  # noqa: B018
            policies = list(client._pipeline._impl_policies)
            policies.insert(0, AzureCountPolicy(counter))
            client._pipeline._impl_policies = policies
        except Exception:
            pass
    return client


def _list_blobs(container: str) -> dict[str, bytes]:
    svc = BlobServiceClient.from_connection_string(CONN)
    cc = svc.get_container_client(container)
    out: dict[str, bytes] = {}
    for blob in cc.list_blobs():
        out[blob.name] = cc.download_blob(blob.name).readall()
    return out


def _wipe_container(container: str) -> None:
    svc = BlobServiceClient.from_connection_string(CONN)
    cc = svc.get_container_client(container)
    try:
        for blob in cc.list_blobs():
            cc.delete_blob(blob.name)
    except ResourceNotFoundError:
        pass


def _ensure_container(name: str) -> None:
    svc = BlobServiceClient.from_connection_string(CONN)
    try:
        svc.create_container(name)
    except ResourceExistsError:
        pass


def _s3_client():
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
    except ClientError:
        try:
            client.head_bucket(Bucket=name)
        except ClientError:
            raise


def _load_modules():
    def overlay(mod_name, path, maker, attr):
        import importlib

        importlib.import_module(mod_name)
        pair_name = mod_name + "_pair"
        shim_name = mod_name + "_shim"
        pair_spec = importlib.util.spec_from_file_location(pair_name, path)
        pair_mod = importlib.util.module_from_spec(pair_spec)
        sys.modules[pair_name] = pair_mod
        pair_spec.loader.exec_module(pair_mod)
        shim_spec = importlib.util.spec_from_file_location(shim_name, path)
        shim_mod = importlib.util.module_from_spec(shim_spec)
        sys.modules[shim_name] = shim_mod
        shim_spec.loader.exec_module(shim_mod)
        setattr(shim_mod, attr, maker(shim_mod))
        return pair_mod, shim_mod

    local_path = (
        AIRFLOW_SRC
        / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/local_to_wasb.py"
    )
    s3w_path = (
        AIRFLOW_SRC
        / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py"
    )
    local_pair, local_shim = overlay(
        "airflow.providers.microsoft.azure.transfers.local_to_wasb",
        local_path,
        make_local_to_wasb_shim,
        "LocalFilesystemToWasbOperator",
    )
    s3_pair, s3_shim = overlay(
        "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
        s3w_path,
        make_s3_to_wasb_shim,
        "S3ToAzureBlobStorageOperator",
    )
    return local_pair, local_shim, s3_pair, s3_shim


def _wasb_hook():
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    from airflow.sdk.definitions.connection import Connection

    class EmulatorWasbHook(WasbHook):
        def get_connection(self, conn_id):  # type: ignore[override]
            return Connection(
                conn_id=conn_id,
                conn_type="wasb",
                login="devstoreaccount1",
                extra='{"connection_string": %s}' % __import__("json").dumps(CONN),
            )

    return EmulatorWasbHook(wasb_conn_id="wasb_default")


def _s3_hook(client):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="aws_default")
    try:
        hook.get_conn().list_buckets()
    except Exception:
        hook.get_conn = lambda: client  # type: ignore[method-assign]
    return hook


def _count_wasb(hook) -> RequestCounter:
    counter = RequestCounter()
    # Wrap load_file / check_for_blob / get_blobs_list_recursive at the hook
    # AND count HTTP methods via a policy if we can attach one.
    orig_load = hook.load_file
    orig_check = getattr(hook, "check_for_blob", None)
    orig_list = getattr(hook, "get_blobs_list_recursive", None)

    def load_file(*a, **k):
        counter.bump("WasbHook.load_file")
        return orig_load(*a, **k)

    hook.load_file = load_file
    if orig_check:
        def check(*a, **k):
            counter.bump("WasbHook.check_for_blob")
            return orig_check(*a, **k)
        hook.check_for_blob = check
    if orig_list:
        def lst(*a, **k):
            counter.bump("WasbHook.get_blobs_list_recursive")
            return orig_list(*a, **k)
        hook.get_blobs_list_recursive = lst
    try:
        policies = list(hook.blob_service_client._pipeline._impl_policies)
        policies.insert(0, AzureCountPolicy(counter))
        hook.blob_service_client._pipeline._impl_policies = policies
    except Exception:
        pass
    return counter


def main() -> int:
    results = []
    local_pair, local_shim, s3_pair, s3_shim = _load_modules()
    run_id = uuid.uuid4().hex[:8]
    container = f"parity{run_id}"  # azurite container names: alphanumeric
    _ensure_container(container)

    # ---------- LocalFilesystemToWasb ----------
    wasb = _wasb_hook()

    def run_local(cls, file_path, blob, load_options=None):
        op = cls(
            task_id="parity-local",
            file_path=file_path,
            container_name=container,
            blob_name=blob,
            create_container=False,
            load_options=load_options or {},
        )
        # Default wasb_conn_id is wasb_default, provided via AIRFLOW_CONN_WASB_DEFAULT.
        hook = _wasb_hook()
        # Count on the hook instance execute will create: wrap WasbHook in the
        # module execute() imports.
        counter_box = {}

        def factory(*a, **k):
            h = hook
            counter_box["c"] = _count_wasb(h)
            return h

        # The pair imports WasbHook from its own module; the shim looks it up
        # on the same overlay module closed over at class-build time.
        for mod_name, mod in list(sys.modules.items()):
            if getattr(mod, "WasbHook", None) is not None and "local_to_wasb" in (mod_name or ""):
                setattr(mod, "WasbHook", factory)
        op.execute(None)
        return counter_box.get("c") or RequestCounter()

    with tempfile.NamedTemporaryFile("wb", delete=False) as fh:
        fh.write(b"hello-azurite")
        local_file = fh.name
    with tempfile.NamedTemporaryFile("wb", delete=False) as fh:
        fh.write(b"newer-azurite")
        local_file2 = fh.name

    def rec(name, pair_objs, shim_objs, pair_counts, shim_counts, extra=None):
        row = {
            "scenario": name,
            "objects_match": pair_objs == shim_objs,
            "request_counts_match": pair_counts == shim_counts,
            "pair_keys": sorted(pair_objs),
            "shim_keys": sorted(shim_objs),
            "pair_counts": pair_counts,
            "shim_counts": shim_counts,
            "byte_mismatch": [
                k for k in set(pair_objs) | set(shim_objs) if pair_objs.get(k) != shim_objs.get(k)
            ],
        }
        if extra:
            row.update(extra)
        results.append(row)
        print(
            f"{name}: objects_match={row['objects_match']} "
            f"request_counts_match={row['request_counts_match']} "
            f"pair={pair_counts} shim={shim_counts}",
            flush=True,
        )

    _wipe_container(container)
    pc = run_local(local_pair.LocalFilesystemToWasbOperator, local_file, "one/blob.txt")
    pair_objs = _list_blobs(container)
    _wipe_container(container)
    sc = run_local(local_shim.LocalFilesystemToWasbOperator, local_file, "one/blob.txt")
    shim_objs = _list_blobs(container)
    rec("local_upload_bytes", pair_objs, shim_objs, pc.as_dict(), sc.as_dict())

    # overwrite true (default upload_blob overwrite is often False on Azure;
    # WasbHook.upload typically passes overwrite=True? We score both sides
    # with the same load_options.)
    _wipe_container(container)
    run_local(local_pair.LocalFilesystemToWasbOperator, local_file, "one/blob.txt")
    try:
        pc = run_local(
            local_pair.LocalFilesystemToWasbOperator,
            local_file2,
            "one/blob.txt",
            load_options={"overwrite": True},
        )
        pair_err = None
    except Exception as exc:
        pc = RequestCounter()
        pair_err = type(exc).__name__
    pair_objs = _list_blobs(container)
    _wipe_container(container)
    run_local(local_shim.LocalFilesystemToWasbOperator, local_file, "one/blob.txt")
    try:
        sc = run_local(
            local_shim.LocalFilesystemToWasbOperator,
            local_file2,
            "one/blob.txt",
            load_options={"overwrite": True},
        )
        shim_err = None
    except Exception as exc:
        sc = RequestCounter()
        shim_err = type(exc).__name__
    shim_objs = _list_blobs(container)
    rec(
        "local_overwrite_true",
        pair_objs,
        shim_objs,
        pc.as_dict(),
        sc.as_dict(),
        extra={"pair_error": pair_err, "shim_error": shim_err},
    )

    _wipe_container(container)
    run_local(local_pair.LocalFilesystemToWasbOperator, local_file, "one/blob.txt")
    try:
        pc = run_local(
            local_pair.LocalFilesystemToWasbOperator,
            local_file2,
            "one/blob.txt",
            load_options={"overwrite": False},
        )
        pair_err = None
    except Exception as exc:
        pc = RequestCounter()
        pair_err = type(exc).__name__
    pair_objs = _list_blobs(container)
    # For overwrite=False, dest should still be hello-azurite if the hook refused.
    _wipe_container(container)
    run_local(local_shim.LocalFilesystemToWasbOperator, local_file, "one/blob.txt")
    try:
        sc = run_local(
            local_shim.LocalFilesystemToWasbOperator,
            local_file2,
            "one/blob.txt",
            load_options={"overwrite": False},
        )
        shim_err = None
    except Exception as exc:
        sc = RequestCounter()
        shim_err = type(exc).__name__
    shim_objs = _list_blobs(container)
    rec(
        "local_overwrite_false",
        pair_objs,
        shim_objs,
        pc.as_dict(),
        sc.as_dict(),
        extra={"pair_error": pair_err, "shim_error": shim_err},
    )

    # ---------- S3ToAzureBlobStorage ----------
    s3c = _s3_client()
    s3c.list_buckets()
    bucket = f"parity-src-{run_id}"
    _ensure_bucket(s3c, bucket)
    s3hook = _s3_hook(s3c)

    def seed_s3():
        for key, body in (
            ("pre/a.txt", b"aaa"),
            ("pre/nested/b.txt", b"bbb"),
            ("pre/dir/", b""),
            ("other/c.txt", b"ccc"),
        ):
            s3c.put_object(Bucket=bucket, Key=key, Body=body)

    def run_s3wasb(cls, **kwargs):
        op = cls(
            task_id="parity-s3wasb",
            s3_bucket=bucket,
            container_name=container,
            **kwargs,
        )
        # Inject emulator hooks via cached_property
        object.__setattr__(op, "s3_hook", s3hook)
        object.__setattr__(op, "wasb_hook", wasb)
        counter = _count_wasb(wasb)
        with count_s3_client(s3c) as s3counts:
            op.execute(None)
        merged = RequestCounter()
        merged.counts = Counter(counter.as_dict())
        merged.counts.update({f"s3.{k}": v for k, v in s3counts.as_dict().items()})
        return merged

    seed_s3()
    _wipe_container(container)
    pc = run_s3wasb(
        s3_pair.S3ToAzureBlobStorageOperator,
        s3_prefix="pre",
        blob_prefix="out",
        replace=True,
        create_container=False,
    )
    pair_objs = _list_blobs(container)
    _wipe_container(container)
    sc = run_s3wasb(
        s3_shim.S3ToAzureBlobStorageOperator,
        s3_prefix="pre",
        blob_prefix="out",
        replace=True,
        create_container=False,
    )
    shim_objs = _list_blobs(container)
    rec("s3_to_wasb_prefix_and_marker", pair_objs, shim_objs, pc.as_dict(), sc.as_dict())

    # replace=False skip when dest exists
    _wipe_container(container)
    # Pre-seed dest with a.txt
    wasb.load_file  # ensure hook works
    from io import BytesIO
    # upload existing dest blob via service
    svc = BlobServiceClient.from_connection_string(CONN)
    svc.get_blob_client(container, "out/a.txt").upload_blob(b"EXISTING", overwrite=True)

    pair_err = shim_err = None
    try:
        pc = run_s3wasb(
            s3_pair.S3ToAzureBlobStorageOperator,
            s3_prefix="pre",
            blob_prefix="out",
            replace=False,
            create_container=False,
        )
    except Exception as exc:
        pc = RequestCounter()
        pair_err = type(exc).__name__
    pair_objs = _list_blobs(container)
    _wipe_container(container)
    svc.get_blob_client(container, "out/a.txt").upload_blob(b"EXISTING", overwrite=True)
    try:
        sc = run_s3wasb(
            s3_shim.S3ToAzureBlobStorageOperator,
            s3_prefix="pre",
            blob_prefix="out",
            replace=False,
            create_container=False,
        )
    except Exception as exc:
        sc = RequestCounter()
        shim_err = type(exc).__name__
    shim_objs = _list_blobs(container)
    rec(
        "s3_to_wasb_replace_false",
        pair_objs,
        shim_objs,
        pc.as_dict(),
        sc.as_dict(),
        extra={"pair_error": pair_err, "shim_error": shim_err},
    )

    # cleanup
    _wipe_container(container)
    try:
        BlobServiceClient.from_connection_string(CONN).delete_container(container)
    except Exception:
        pass
    try:
        paginator = s3c.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents") or []:
                s3c.delete_object(Bucket=bucket, Key=obj["Key"])
        s3c.delete_bucket(Bucket=bucket)
    except Exception:
        pass
    for path in (local_file, local_file2):
        try:
            os.unlink(path)
        except OSError:
            pass

    out_path = ROOT / "parity-results.json"
    out_path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print("Wrote", out_path, flush=True)
    failed = [
        r
        for r in results
        if not r["objects_match"] or not r["request_counts_match"] or r.get("pair_error") != r.get("shim_error")
    ]
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
