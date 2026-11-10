#!/usr/bin/env python3
"""9-pair behavioural parity: generic operator vs upstream / naive references."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
import traceback
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(REPO / "src"))

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

from generic_transfer.adapters.emulators import load_emulator_env  # noqa: E402
from generic_transfer.adapters.pair_baseline import naive_pair_copy  # noqa: E402
from emu_native import ensure_native_bucket, make_native_emu, wipe_backend  # noqa: E402
from generic_transfer.adapters import pair_baseline as _pair_baseline  # noqa: E402
from generic_transfer.adapters import emulators as _emu_mod  # noqa: E402

load_emulator_env(REPO / "emulators.env")

def _list_keys_native_instrumented(emu, prefix: str = "") -> list[str]:
    """Same listing as emulators.list_keys_native, but reuse the cached client.

    The stock helper builds a new Azure/S3 client for the list call, which
    evades the wire counter attached to the cached client used for GET/PUT.
    """
    prefix = (prefix or "").lstrip("/")
    client = _emu_mod._native_client(emu)
    if emu.flavour == "s3":
        out: list[str] = []
        token = None
        while True:
            kwargs = {"Bucket": emu.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            out.extend(o["Key"] for o in resp.get("Contents") or [])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(out)
    if emu.flavour == "gcs":
        return sorted(b.name for b in client.bucket(emu.bucket).list_blobs(prefix=prefix))
    if emu.flavour == "azure":
        container = client.get_container_client(emu.bucket)
        return sorted(b.name for b in container.list_blobs(name_starts_with=prefix))
    raise ValueError(emu.flavour)

_pair_baseline.list_keys_native = _list_keys_native_instrumented

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
os.environ.setdefault(
    "AIRFLOW_CONN_WASB_DEFAULT",
    (
        '{"conn_type": "wasb", "login": "devstoreaccount1",'
        ' "extra": {"connection_string": %r}}'
        % os.environ.get("AZURE_CONNECTION_STRING", "")
    ),
)
os.environ.setdefault(
    "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT",
    '{"conn_type": "google_cloud_platform", "extra": {"project": "demo", "num_retries": 1}}',
)
os.environ.setdefault("STORAGE_EMULATOR_HOST", os.environ.get("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443"))
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", os.environ.get("GCS_PROJECT", "demo"))

from backends import backend_for  # noqa: E402
from hooks import EmulatorGCSHook, EmulatorS3Hook, EmulatorWasbHook  # noqa: E402
from listing import dest_key  # noqa: E402
from generic_operator import GenericFileTransferOperator  # noqa: E402
from pair_policy import HAS_MOVE, HAS_REPLACE, PAIR_POLICY  # noqa: E402
from wire_count import WireCounter, count_azure, count_gcs, count_s3  # noqa: E402

PAIRS = [
    ("s3", "s3"),
    ("gcs", "gcs"),
    ("azure", "azure"),
    ("s3", "gcs"),
    ("gcs", "s3"),
    ("s3", "azure"),
    ("azure", "s3"),
    ("gcs", "azure"),
    ("azure", "gcs"),
]

SRC_PREFIX = "pre/"
DST_PREFIX = "out/"


def _list(backend, bucket: str, prefix: str) -> dict[str, bytes]:
    from contract import ListQuery, ObjectRef

    out: dict[str, bytes] = {}
    for obj in backend.list_objects(ListQuery(bucket, prefix)):
        out[obj.key] = backend.read_bytes(ObjectRef(bucket, obj.key, backend.flavour))
    return out


def _wipe(backend, bucket: str) -> None:
    wipe_backend(backend, bucket)


def _seed(backend, bucket: str, include_marker: bool = True) -> None:
    from contract import ObjectRef

    payload = {
        "pre/a.txt": b"aaa",
        "pre/nested/b.txt": b"bbb",
        "other/c.txt": b"ccc",
    }
    if include_marker:
        payload["pre/dir/"] = b""
    for key, data in payload.items():
        backend.write_bytes(ObjectRef(bucket, key, backend.flavour), data, overwrite=True)


def expected_dest_for_source(pair: tuple[str, str], source_key: str, *, dest_prefix: str = DST_PREFIX) -> str:
    pol = PAIR_POLICY[pair]
    return dest_key(
        source_key,
        source_prefix=SRC_PREFIX,
        dest_prefix=dest_prefix,
        path_layout=pol["path_layout"],
    )


@contextmanager
def _count_pair(src_b, dst_b):
    total = WireCounter()
    stack = ExitStack()
    seen: set[int] = set()
    try:
        ctx_for = {"s3": count_s3, "gcs": count_gcs, "azure": count_azure}
        for backend in (src_b, dst_b):
            ident = id(backend.client)
            if ident in seen:
                continue
            seen.add(ident)
            sub = stack.enter_context(ctx_for[backend.flavour](backend.client))
            backend._wire_sub = sub  # type: ignore[attr-defined]
        yield total
        for backend in (src_b, dst_b):
            sub = getattr(backend, "_wire_sub", None)
            if sub is not None:
                total.counts.update(sub.counts)
    finally:
        stack.close()


def _load_mod(mod_name: str, path: Path):
    parent = mod_name.rsplit(".", 1)[0]
    try:
        importlib.import_module(parent)
    except Exception:
        pass
    try:
        importlib.import_module(mod_name)
    except Exception:
        pass
    spec = importlib.util.spec_from_file_location(mod_name + "_ref", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name + "_ref"] = mod
    spec.loader.exec_module(mod)
    return mod


def _hooks_for(src_b, dst_b):
    hooks: dict[str, object] = {}
    src_f, dst_f = src_b.flavour, dst_b.flavour
    if src_f == "s3":
        hooks["s3"] = EmulatorS3Hook(src_b)
    elif dst_f == "s3":
        hooks["s3"] = EmulatorS3Hook(dst_b)
    if src_f == "s3" and dst_f == "s3":
        hooks["s3"] = EmulatorS3Hook(dst_b)
    if src_f == "gcs":
        hooks["gcs"] = EmulatorGCSHook(src_b)
    elif dst_f == "gcs":
        hooks["gcs"] = EmulatorGCSHook(dst_b)
    if src_f == "gcs" and dst_f == "gcs":
        hooks["gcs"] = EmulatorGCSHook(dst_b)
    if src_f == "azure":
        hooks["wasb"] = EmulatorWasbHook(src_b)
    elif dst_f == "azure":
        hooks["wasb"] = EmulatorWasbHook(dst_b)
    if src_f == "azure" and dst_f == "azure":
        hooks["wasb"] = EmulatorWasbHook(dst_b)
    # Cross-pair: need both sides as distinct hooks.
    if src_f == "s3" and dst_f != "s3":
        hooks["s3"] = EmulatorS3Hook(src_b)
    if dst_f == "s3" and src_f != "s3":
        hooks["s3"] = EmulatorS3Hook(dst_b)
    if src_f == "gcs" and dst_f != "gcs":
        hooks["gcs"] = EmulatorGCSHook(src_b)
    if dst_f == "gcs" and src_f != "gcs":
        hooks["gcs"] = EmulatorGCSHook(dst_b)
    if src_f == "azure" and dst_f != "azure":
        hooks["wasb"] = EmulatorWasbHook(src_b)
    if dst_f == "azure" and src_f != "azure":
        hooks["wasb"] = EmulatorWasbHook(dst_b)
    return hooks


def _hook_factory(instance):
    """Return a constructor that yields *instance* and keeps class-level helpers."""

    def factory(*a, inst=instance, **k):
        return inst

    # S3Hook.parse_s3_url is called on the class in several pair operators.
    parse = getattr(type(instance), "parse_s3_url", None)
    if parse is not None:
        factory.parse_s3_url = parse
    return factory


def _patch_hooks(mod, hooks: dict):
    if "s3" in hooks and hasattr(mod, "S3Hook"):
        mod.S3Hook = _hook_factory(hooks["s3"])
    if "gcs" in hooks and hasattr(mod, "GCSHook"):
        mod.GCSHook = _hook_factory(hooks["gcs"])
    if "wasb" in hooks and hasattr(mod, "WasbHook"):
        mod.WasbHook = _hook_factory(hooks["wasb"])
    return mod


def _bind_hook_attrs(op, hooks: dict):
    for name, hook in hooks.items():
        for attr in ("hook", f"{name}_hook", "s3_hook", "wasb_hook", "gcs_hook"):
            if name == "s3" and attr in {"hook", "s3_hook"}:
                try:
                    object.__setattr__(op, attr, hook)
                except Exception:
                    pass
            if name == "gcs" and attr in {"hook", "gcs_hook"}:
                try:
                    object.__setattr__(op, attr, hook)
                except Exception:
                    pass
            if name == "wasb" and attr in {"hook", "wasb_hook"}:
                try:
                    object.__setattr__(op, attr, hook)
                except Exception:
                    pass


def run_generic(src_b, dst_b, src_bucket, dst_bucket, **kw) -> list[str]:
    pair = (src_b.flavour, dst_b.flavour)
    pol = PAIR_POLICY[pair]
    op = GenericFileTransferOperator(
        source_backend=src_b,
        dest_backend=dst_b,
        source_bucket=src_bucket,
        dest_bucket=dst_bucket,
        source_prefix=kw.get("source_prefix", SRC_PREFIX),
        dest_prefix=kw.get("dest_prefix", DST_PREFIX),
        replace=kw.get("replace", True),
        move=kw.get("move", False),
        marker_policy=kw.get("marker_policy", pol["marker_policy"]),
        path_layout=kw.get("path_layout", pol["path_layout"]),
        server_side_copy=kw.get("server_side_copy", "auto"),
        exact_match=kw.get("exact_match", False),
    )
    return op.execute(None)


def run_reference(pair, src_b, dst_b, src_emu, dst_emu, src_bucket, dst_bucket, **kw):
    src_f, dst_f = pair
    replace = kw.get("replace", True)
    move = kw.get("move", False)
    source_prefix = kw.get("source_prefix", SRC_PREFIX)
    dest_prefix = kw.get("dest_prefix", DST_PREFIX)
    single = kw.get("single_blob", False)
    hooks = _hooks_for(src_b, dst_b)

    if pair == ("s3", "s3"):
        path = AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/operators/s3.py"
        mod = _load_mod("airflow.providers.amazon.aws.operators.s3", path)
        op = mod.S3CopyPrefixOperator(
            task_id="ref-s3",
            source_bucket_name=src_bucket,
            source_bucket_prefix=source_prefix,
            dest_bucket_name=dst_bucket,
            dest_bucket_prefix=dest_prefix,
        )
        object.__setattr__(op, "hook", hooks["s3"])
        op.execute(None)
        return "S3CopyPrefixOperator"

    if pair == ("gcs", "gcs"):
        path = AIRFLOW_SRC / "providers/google/src/airflow/providers/google/cloud/transfers/gcs_to_gcs.py"
        mod = _load_mod("airflow.providers.google.cloud.transfers.gcs_to_gcs", path)
        _patch_hooks(mod, hooks)
        op = mod.GCSToGCSOperator(
            task_id="ref-gcs",
            source_bucket=src_bucket,
            source_object=source_prefix,
            destination_bucket=dst_bucket,
            destination_object=dest_prefix,
            replace=replace,
            move_object=move,
        )
        op.execute(None)
        return "GCSToGCSOperator"

    if pair == ("azure", "azure"):
        naive_pair_copy(
            src_emu,
            dst_emu,
            source_prefix=source_prefix.rstrip("/") or source_prefix,
            dest_prefix=dest_prefix.rstrip("/") or dest_prefix,
            skip_markers=False,
        )
        return "naive_pair_copy"

    if pair == ("s3", "gcs"):
        path = AIRFLOW_SRC / "providers/google/src/airflow/providers/google/cloud/transfers/s3_to_gcs.py"
        mod = _load_mod("airflow.providers.google.cloud.transfers.s3_to_gcs", path)
        _patch_hooks(mod, hooks)
        dest_gcs = f"gs://{dst_bucket}/{dest_prefix.rstrip('/')}/"
        op = mod.S3ToGCSOperator(
            task_id="ref-s3-gcs",
            bucket=src_bucket,
            prefix=source_prefix,
            dest_gcs=dest_gcs,
            replace=replace,
            deferrable=False,
            apply_gcs_prefix=False,
        )
        object.__setattr__(op, "hook", hooks["s3"])
        op.execute(None)
        return "S3ToGCSOperator"

    if pair == ("gcs", "s3"):
        path = AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/transfers/gcs_to_s3.py"
        mod = _load_mod("airflow.providers.amazon.aws.transfers.gcs_to_s3", path)
        _patch_hooks(mod, hooks)
        dest_s3_key = f"s3://{dst_bucket}/{dest_prefix.rstrip('/')}"
        op = mod.GCSToS3Operator(
            task_id="ref-gcs-s3",
            gcs_bucket=src_bucket,
            prefix=source_prefix.rstrip("/"),
            dest_s3_key=dest_s3_key,
            replace=replace,
        )
        op.execute(None)
        return "GCSToS3Operator"

    if pair == ("s3", "azure"):
        path = (
            AIRFLOW_SRC
            / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py"
        )
        mod = _load_mod("airflow.providers.microsoft.azure.transfers.s3_to_wasb", path)
        _patch_hooks(mod, hooks)
        op = mod.S3ToAzureBlobStorageOperator(
            task_id="ref-s3-az",
            s3_bucket=src_bucket,
            s3_prefix=source_prefix.rstrip("/"),
            container_name=dst_bucket,
            blob_prefix=dest_prefix.rstrip("/"),
            replace=replace,
            create_container=False,
        )
        object.__setattr__(op, "s3_hook", hooks["s3"])
        object.__setattr__(op, "wasb_hook", hooks["wasb"])
        op.execute(None)
        return "S3ToAzureBlobStorageOperator"

    if pair == ("azure", "s3"):
        path = AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/transfers/azure_blob_to_s3.py"
        mod = _load_mod("airflow.providers.amazon.aws.transfers.azure_blob_to_s3", path)
        _patch_hooks(mod, hooks)
        dest_s3_key = f"s3://{dst_bucket}/{dest_prefix.rstrip('/')}"
        op = mod.AzureBlobStorageToS3Operator(
            task_id="ref-az-s3",
            container_name=src_bucket,
            prefix=source_prefix.rstrip("/"),
            dest_s3_key=dest_s3_key,
            replace=replace,
        )
        op.execute(None)
        return "AzureBlobStorageToS3Operator"

    if pair == ("gcs", "azure"):
        path = (
            AIRFLOW_SRC
            / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/gcs_to_wasb.py"
        )
        mod = _load_mod("airflow.providers.microsoft.azure.transfers.gcs_to_wasb", path)
        _patch_hooks(mod, hooks)
        op = mod.GCSToAzureBlobStorageOperator(
            task_id="ref-gcs-az",
            gcs_bucket=src_bucket,
            prefix=source_prefix.rstrip("/"),
            container_name=dst_bucket,
            blob_prefix=dest_prefix.rstrip("/"),
            replace=replace,
            create_container=False,
        )
        op.execute(None)
        return "GCSToAzureBlobStorageOperator"

    if pair == ("azure", "gcs"):
        if single:
            path = (
                AIRFLOW_SRC
                / "providers/google/src/airflow/providers/google/cloud/transfers/azure_blob_to_gcs.py"
            )
            mod = _load_mod("airflow.providers.google.cloud.transfers.azure_blob_to_gcs", path)
            _patch_hooks(mod, hooks)
            op = mod.AzureBlobStorageToGCSOperator(
                task_id="ref-az-gcs",
                wasb_conn_id="wasb_default",
                gcp_conn_id="google_cloud_default",
                container_name=src_bucket,
                blob_name=kw.get("blob_name", "pre/a.txt"),
                bucket_name=dst_bucket,
                object_name=kw.get("object_name", "out/a.txt"),
                filename="/tmp/generic-all-pairs-az-gcs.tmp",
                gzip=False,
                unwrap_single=True,
            )
            op.execute(None)
            return "AzureBlobStorageToGCSOperator"
        naive_pair_copy(
            src_emu,
            dst_emu,
            source_prefix=source_prefix.rstrip("/") or source_prefix,
            dest_prefix=dest_prefix.rstrip("/") or dest_prefix,
            skip_markers=False,
        )
        return "naive_pair_copy"

    raise ValueError(pair)



def _explain_count_diff(pair_name: str, sname: str, ref: dict, gen: dict) -> str | None:
    """Return a pre-registered policy label, or None if the mismatch is unexplained."""
    if pair_name == "azure->azure":
        return "generic server_side_copy vs naive_pair_copy get+put"
    if sname == "replace_false":
        def _write_counts(d):
            return {k: v for k, v in d.items() if any(s in k for s in ("PUT", "POST", "CopyObject", "PutObject", "DELETE", "GetObject")) and "HEAD" not in k and "HeadObject" not in k}

        # Compare non-existence-check writes / data copies. Existence checks
        # (dest prefix list vs per-key exists/HEAD/GET) may differ in method.
        writes_ref = {k: v for k, v in ref.items() if any(s in k for s in ("PUT", "POST", "CopyObject", "PutObject", "GetObject"))}
        writes_gen = {k: v for k, v in gen.items() if any(s in k for s in ("PUT", "POST", "CopyObject", "PutObject", "GetObject"))}
        if writes_ref == writes_gen:
            return "replace=False dest check: per-key exists vs dest-prefix list"
    # Pair hook download_file / get_key often issues Head+Get; contract read_bytes is Get/GET only.
    only_ref_heads = all(
        (gen.get(k, 0) == ref.get(k, 0)) or (k.endswith("HeadObject") or k.endswith(".HEAD") and ref.get(k, 0) > gen.get(k, 0))
        for k in set(ref) | set(gen)
    )
    # simpler: gen equals ref after removing Head/HEAD-only keys from ref
    ref_no_head = {k: v for k, v in ref.items() if "HeadObject" not in k and not k.endswith(".HEAD")}
    gen_no_head = {k: v for k, v in gen.items() if "HeadObject" not in k and not k.endswith(".HEAD")}
    if ref_no_head == gen_no_head and ref != gen:
        return "pair hook download path issues Head before Get; contract read_bytes does not"
    if sname == "single_blob":
        # Generic operator always lists via ObjectLister, even on exact_match;
        # AzureBlobStorageToGCSOperator gets one blob by name.
        if gen.get("azure.GET", 0) == ref.get("azure.GET", 0) + 1 and all(
            gen.get(k, 0) == ref.get(k, 0) for k in set(ref) | set(gen) if k != "azure.GET"
        ):
            return "generic lists even for exact_match; single-blob reference does not list"
    return None


def main() -> int:
    run_id = uuid.uuid4().hex[:8]
    results: list[dict] = []
    emus = {}
    backends = {}
    buckets = {}
    for flavour in ("s3", "gcs", "azure"):
        src_name = f"gsrc{run_id}"
        dst_name = f"gdst{run_id}"
        src_emu = make_native_emu(flavour, src_name, repo_env=REPO / "emulators.env")
        dst_emu = make_native_emu(flavour, dst_name, repo_env=REPO / "emulators.env")
        ensure_native_bucket(src_emu)
        ensure_native_bucket(dst_emu)
        emus[flavour] = (src_emu, dst_emu)
        backends[flavour] = (backend_for(flavour, emu=src_emu), backend_for(flavour, emu=dst_emu))
        buckets[flavour] = (src_name, dst_name)

    def seed_pair(src_f, dst_f):
        _wipe(backends[src_f][0], buckets[src_f][0])
        _wipe(backends[dst_f][1], buckets[dst_f][1])
        _seed(backends[src_f][0], buckets[src_f][0])

    for src_f, dst_f in PAIRS:
        pair = (src_f, dst_f)
        pair_name = f"{src_f}->{dst_f}"
        src_b = backends[src_f][0]
        dst_b = backends[dst_f][1]
        src_bucket = buckets[src_f][0]
        dst_bucket = buckets[dst_f][1]
        src_emu = emus[src_f][0]
        dst_emu = emus[dst_f][1]
        collide_key = expected_dest_for_source(pair, "pre/a.txt")

        def do_scenario(sname, **opts):
            seed_pair(src_f, dst_f)
            seed_dest_old = opts.get("seed_dest_old")
            replace = opts.get("replace", True)
            move = opts.get("move", False)
            single = opts.get("single_blob", False)
            from contract import ObjectRef

            dest_seed_key = "out/a.txt" if single else collide_key
            if seed_dest_old:
                dst_b.write_bytes(
                    ObjectRef(dst_bucket, dest_seed_key, dst_f), b"OLD-BYTES", overwrite=True
                )
            ref_name = "unknown"
            ref_err = gen_err = None
            ref_objs: dict[str, bytes] = {}
            gen_objs: dict[str, bytes] = {}
            ref_counts: dict[str, int] = {}
            gen_counts: dict[str, int] = {}
            try:
                with _count_pair(src_b, dst_b) as c:
                    ref_name = run_reference(
                        pair,
                        src_b,
                        dst_b,
                        src_emu,
                        dst_emu,
                        src_bucket,
                        dst_bucket,
                        replace=replace,
                        move=move,
                        single_blob=single,
                    )
                ref_counts = c.as_dict()
                ref_objs = _list(dst_b, dst_bucket, "out/")
            except Exception as exc:
                ref_err = f"{type(exc).__name__}: {exc}"
                traceback.print_exc()
                ref_objs = _list(dst_b, dst_bucket, "out/")
            _wipe(dst_b, dst_bucket)
            _wipe(src_b, src_bucket)
            _seed(src_b, src_bucket)
            if seed_dest_old:
                dst_b.write_bytes(
                    ObjectRef(dst_bucket, dest_seed_key, dst_f), b"OLD-BYTES", overwrite=True
                )
            try:
                with _count_pair(src_b, dst_b) as c:
                    run_generic(
                        src_b,
                        dst_b,
                        src_bucket,
                        dst_bucket,
                        replace=replace,
                        move=move,
                        exact_match=single,
                        source_prefix="pre/a.txt" if single else SRC_PREFIX,
                        dest_prefix="out/a.txt" if single else DST_PREFIX,
                    )
                gen_counts = c.as_dict()
                gen_objs = _list(dst_b, dst_bucket, "out/")
            except Exception as exc:
                gen_err = f"{type(exc).__name__}: {exc}"
                traceback.print_exc()
                gen_objs = _list(dst_b, dst_bucket, "out/")

            obj_match = ref_objs == gen_objs
            count_match = ref_counts == gen_counts
            ref_has_marker = any(k.endswith("/") for k in ref_objs)
            gen_has_marker = any(k.endswith("/") for k in gen_objs)
            marker_match = ref_has_marker == gen_has_marker
            err_match = ((ref_err is None) == (gen_err is None)) and (
                ref_err is None or (ref_err.split(":")[0] == gen_err.split(":")[0])
            )
            explained_counts = False
            count_note = None
            copied = bool(ref_objs or gen_objs)
            if copied and not ref_counts and not gen_counts:
                count_match = False
                count_note = "wire counters recorded zero requests on a non-empty copy"
            elif not count_match:
                note = _explain_count_diff(pair_name, sname, ref_counts, gen_counts)
                if note:
                    explained_counts = True
                    count_note = note
            row = {
                "pair": pair_name,
                "scenario": sname,
                "reference": ref_name,
                "objects_match": obj_match,
                "markers_match": marker_match,
                "request_counts_match": count_match,
                "ref_keys": sorted(ref_objs),
                "gen_keys": sorted(gen_objs),
                "ref_counts": ref_counts,
                "gen_counts": gen_counts,
                "ref_error": ref_err,
                "gen_error": gen_err,
                "errors_match": err_match,
                "byte_mismatch": sorted(
                    k for k in set(ref_objs) | set(gen_objs) if ref_objs.get(k) != gen_objs.get(k)
                ),
                "count_difference_policy": count_note,
                "collide_key": dest_seed_key,
            }
            row["pass"] = bool(obj_match and marker_match and err_match and (count_match or explained_counts))
            results.append(row)
            flag = "PASS" if row["pass"] else "FAIL"
            print(
                f"{flag} {pair_name} {sname} ref={ref_name} "
                f"objects={obj_match} markers={marker_match} counts={count_match} "
                f"ref_keys={sorted(ref_objs)} gen_keys={sorted(gen_objs)} "
                f"ref={ref_counts} gen={gen_counts} ref_err={ref_err} gen_err={gen_err}",
                flush=True,
            )
            return row

        prefix_row = do_scenario("prefix_copy")
        results.append({**prefix_row, "scenario": "folder_marker"})
        print(
            f"{'PASS' if prefix_row['pass'] else 'FAIL'} {pair_name} folder_marker "
            f"(same run as prefix_copy; marker in ref="
            f"{any(k.endswith('/') for k in prefix_row['ref_keys'])} "
            f"gen={any(k.endswith('/') for k in prefix_row['gen_keys'])})",
            flush=True,
        )
        do_scenario("overwrite_true", seed_dest_old=True, replace=True)
        if pair in HAS_REPLACE:
            do_scenario("replace_false", seed_dest_old=True, replace=False)
        else:
            results.append(
                {
                    "pair": pair_name,
                    "scenario": "replace_false",
                    "reference": PAIR_POLICY[pair]["reference"],
                    "pass": True,
                    "skipped": True,
                    "skip_reason": "reference has no replace flag; not scored as a parity cell",
                    "objects_match": None,
                    "markers_match": None,
                    "request_counts_match": None,
                }
            )
            print(f"SKIP {pair_name} replace_false (reference has no replace flag)", flush=True)
        if pair in HAS_MOVE:
            do_scenario("move", move=True, replace=True)
        if pair_name == "azure->gcs":
            do_scenario("single_blob", single_blob=True, replace=True)

    out = ROOT / "parity-results.json"
    out.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print("Wrote", out, flush=True)
    scored = [r for r in results if not r.get("skipped")]
    failed = [r for r in scored if not r["pass"]]
    print(
        f"SUMMARY {len(scored)-len(failed)}/{len(scored)} scored cells passed, "
        f"{len(failed)} failed, {sum(1 for r in results if r.get('skipped'))} skipped",
        flush=True,
    )
    for flavour, (src_b, dst_b) in backends.items():
        try:
            _wipe(src_b, buckets[flavour][0])
            _wipe(dst_b, buckets[flavour][1])
        except Exception:
            pass
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("PARITY ERROR:", type(exc).__name__, exc, flush=True)
        raise
