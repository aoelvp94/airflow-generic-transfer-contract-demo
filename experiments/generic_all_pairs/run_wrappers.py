#!/usr/bin/env python3
"""Run unmodified upstream pair-operator unit tests against thin wrappers.

The wrapper maps pair constructor args onto GenericFileTransferOperator.
Tests are not edited. Operator modules are loaded from AIRFLOW_SRC after
the pip-installed providers are imported, then the pair class is replaced
by the wrapper before pytest collects the test module.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))

sys.path.insert(0, str(ROOT))

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

CORPORA = {
    "GCSToGCSOperator": {
        "tests": AIRFLOW_SRC / "providers/google/tests/unit/google/cloud/transfers/test_gcs_to_gcs.py",
        "operator": AIRFLOW_SRC / "providers/google/src/airflow/providers/google/cloud/transfers/gcs_to_gcs.py",
        "mod": "airflow.providers.google.cloud.transfers.gcs_to_gcs",
        "classes": ["GCSToGCSOperator"],
        "k": "TestGoogleCloudStorageToCloudStorageOperator",
        "methods": 45,
        "pip_import": "airflow.providers.google.cloud.transfers.gcs_to_gcs",
    },
    "S3CopyObject+Prefix": {
        "tests": AIRFLOW_SRC / "providers/amazon/tests/unit/amazon/aws/operators/test_s3.py",
        "operator": AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/operators/s3.py",
        "mod": "airflow.providers.amazon.aws.operators.s3",
        "classes": ["S3CopyObjectOperator", "S3CopyPrefixOperator"],
        "k": "TestS3CopyObjectOperator or TestS3CopyPrefixOperator",
        "methods": 16,
        "pip_import": "airflow.providers.amazon.aws.operators.s3",
    },
    "S3ToGCSOperator": {
        "tests": AIRFLOW_SRC / "providers/google/tests/unit/google/cloud/transfers/test_s3_to_gcs.py",
        "operator": AIRFLOW_SRC / "providers/google/src/airflow/providers/google/cloud/transfers/s3_to_gcs.py",
        "mod": "airflow.providers.google.cloud.transfers.s3_to_gcs",
        "classes": ["S3ToGCSOperator"],
        "k": None,
        "methods": 27,
        "pip_import": "airflow.providers.google.cloud.transfers.s3_to_gcs",
    },
    "GCSToS3Operator": {
        "tests": AIRFLOW_SRC / "providers/amazon/tests/unit/amazon/aws/transfers/test_gcs_to_s3.py",
        "operator": AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/transfers/gcs_to_s3.py",
        "mod": "airflow.providers.amazon.aws.transfers.gcs_to_s3",
        "classes": ["GCSToS3Operator"],
        "k": None,
        "methods": 20,
        "pip_import": "airflow.providers.amazon.aws.transfers.gcs_to_s3",
    },
    "S3ToAzureBlobStorageOperator": {
        "tests": AIRFLOW_SRC
        / "providers/microsoft/azure/tests/unit/microsoft/azure/transfers/test_s3_to_wasb.py",
        "operator": AIRFLOW_SRC
        / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py",
        "mod": "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
        "classes": ["S3ToAzureBlobStorageOperator"],
        "k": None,
        "methods": 10,
        "pip_import": "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
    },
    "AzureBlobStorageToS3Operator": {
        "tests": AIRFLOW_SRC / "providers/amazon/tests/unit/amazon/aws/transfers/test_azure_blob_to_s3.py",
        "operator": AIRFLOW_SRC
        / "providers/amazon/src/airflow/providers/amazon/aws/transfers/azure_blob_to_s3.py",
        "mod": "airflow.providers.amazon.aws.transfers.azure_blob_to_s3",
        "classes": ["AzureBlobStorageToS3Operator"],
        "k": None,
        "methods": 7,
        "pip_import": "airflow.providers.amazon.aws.transfers.azure_blob_to_s3",
    },
    "GCSToAzureBlobStorageOperator": {
        "tests": AIRFLOW_SRC
        / "providers/microsoft/azure/tests/unit/microsoft/azure/transfers/test_gcs_to_wasb.py",
        "operator": AIRFLOW_SRC
        / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/gcs_to_wasb.py",
        "mod": "airflow.providers.microsoft.azure.transfers.gcs_to_wasb",
        "classes": ["GCSToAzureBlobStorageOperator"],
        "k": None,
        "methods": 10,
        "pip_import": "airflow.providers.microsoft.azure.transfers.gcs_to_wasb",
    },
    "AzureBlobStorageToGCSOperator": {
        "tests": AIRFLOW_SRC / "providers/google/tests/unit/google/cloud/transfers/test_azure_blob_to_gcs.py",
        "operator": AIRFLOW_SRC
        / "providers/google/src/airflow/providers/google/cloud/transfers/azure_blob_to_gcs.py",
        "mod": "airflow.providers.google.cloud.transfers.azure_blob_to_gcs",
        "classes": ["AzureBlobStorageToGCSOperator"],
        "k": None,
        "methods": 3,
        "pip_import": "airflow.providers.google.cloud.transfers.azure_blob_to_gcs",
    },
}


def classify(failed_name: str, longrepr: str) -> str:
    text = (failed_name + "\n" + (longrepr or "")).lower()
    if any(
        s in text
        for s in (
            "assert_called",
            "assert_has_calls",
            "assert_not_called",
            "mock.call",
            "call_count",
            "expected to be called",
            "requires contract backends",
            "hook.list",
            "hook.rewrite",
            "copy_object",
        )
    ):
        return "hook_call_shape"
    if any(
        s in text
        for s in (
            "modulenotfound",
            "no module named",
            "tests_common",
            "import error",
            "importerror",
            "collection",
            "skipped",
        )
    ):
        return "env_issue"
    if any(
        s in text
        for s in (
            "not implemented",
            "capability",
            "does not have",
            "get_openlineage",
            "render_template_fields",
            "has no attribute",
        )
    ):
        if "get_openlineage" in text or "render_template_fields" in text or "has no attribute" in text:
            if "requires contract backends" in text:
                return "hook_call_shape"
            return "missing_capability"
        return "missing_capability"
    # AttributeError on hook methods is usually hook-call coupling
    if "attributeerror" in text and any(s in text for s in ("hook", "mock", "magicmock")):
        return "hook_call_shape"
    return "behavior_difference"


def _stub_tests_common() -> None:
    for tests_root in (
        AIRFLOW_SRC / "providers/amazon/tests",
        AIRFLOW_SRC / "providers/google/tests",
        AIRFLOW_SRC / "providers/microsoft/azure/tests",
    ):
        if tests_root.is_dir():
            sys.path.insert(0, str(tests_root))
    if "time_machine" not in sys.modules:
        tm = types.ModuleType("time_machine")

        def _travel(*a, **k):
            def deco(fn):
                return fn
            return deco

        tm.travel = _travel
        sys.modules["time_machine"] = tm
    def stub(name: str, **attrs):
        if name in sys.modules:
            return
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod

    stub("tests_common")
    stub("tests_common.test_utils")
    stub("tests_common.test_utils.dag", sync_dag_to_db=lambda dag: None)
    stub(
        "tests_common.test_utils.taskinstance",
        create_task_instance=lambda *a, **k: None,
        render_template_fields=lambda *a, **k: None,
    )
    stub("tests_common.test_utils.version_compat", AIRFLOW_V_3_0_PLUS=True)


def _load_and_wrap(meta: dict) -> None:
    from wrappers import WRAPPERS

    pip_name = meta["pip_import"]
    try:
        importlib.import_module(pip_name)
    except Exception as exc:
        print(f"pip import of {pip_name} failed (continuing with src overlay): {exc}", flush=True)
    spec = importlib.util.spec_from_file_location(meta["mod"], meta["operator"])
    if spec is None or spec.loader is None:
        raise ImportError(meta["operator"])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[meta["mod"]] = mod
    spec.loader.exec_module(mod)
    # rebind package attribute
    parent_name = meta["mod"].rsplit(".", 1)[0]
    attr = meta["mod"].rsplit(".", 1)[1]
    try:
        parent = importlib.import_module(parent_name)
        setattr(parent, attr, mod)
    except Exception:
        pass
    for cls_name in meta["classes"]:
        key = cls_name
        if key not in WRAPPERS:
            raise KeyError(key)
        _mod_name, _cls, wrapper = WRAPPERS[key]
        setattr(mod, cls_name, wrapper)
        print(f"Wrapped {meta['mod']}.{cls_name} -> {wrapper.__name__}", flush=True)


def _read_junit(path: Path):
    if not path.is_file():
        return 0, 0, 0, 0, {}, 0
    import xml.etree.ElementTree as ET

    tree = ET.parse(path)
    root = tree.getroot()
    suites = root.findall("testsuite") or [root]
    passed = failed = errors = skipped = 0
    cats: dict[str, int] = {}
    items = 0
    for ts in suites:
        for case in ts.findall("testcase"):
            items += 1
            fail = case.find("failure")
            err = case.find("error")
            if fail is not None:
                failed += 1
                cat = classify(case.get("name") or "", fail.text or "")
                cats[cat] = cats.get(cat, 0) + 1
            elif err is not None:
                errors += 1
                cat = classify(case.get("name") or "", err.text or "")
                cats[cat] = cats.get(cat, 0) + 1
            elif case.find("skipped") is not None:
                skipped += 1
            else:
                passed += 1
    return passed, failed, errors, skipped, cats, items


def main() -> int:
    _stub_tests_common()
    import pytest
    from wrappers import WRAPPERS  # noqa: F401  # ensure importable

    report = {}
    for name, meta in CORPORA.items():
        tests = meta["tests"]
        if not tests.is_file() or not Path(meta["operator"]).is_file():
            report[name] = {
                "error": "missing test or operator file",
                "category": "env_issue",
                "passed": 0,
                "total": meta["methods"],
            }
            print(name, report[name], flush=True)
            continue
        try:
            _load_and_wrap(meta)
        except Exception as exc:
            report[name] = {
                "error": f"{type(exc).__name__}: {exc}",
                "category": "env_issue",
                "passed": 0,
                "total": meta["methods"],
            }
            print(name, report[name], flush=True)
            continue
        junit = ROOT / f"pytest-wrapper-{name.replace('+', '_')}.xml"
        args = [
            str(tests),
            "-q",
            "--tb=line",
            f"--junitxml={junit}",
            "-p",
            "no:cacheprovider",
            "--noconftest",
            "-c",
            str(ROOT / "pytest.ini"),
            "-o",
            "addopts=",
        ]
        if meta["k"]:
            args.extend(["-k", meta["k"]])
        print("=======", name, "=======", flush=True)
        rc = pytest.main(args)
        passed, failed, errors, skipped, cats, items = _read_junit(junit)
        report[name] = {
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "skipped": skipped,
            "items": items,
            "pre_registered_methods": meta["methods"],
            "pytest_rc": rc,
            "categories": cats,
            "pass_rate": (f"{passed}/{items}" if items else "0/0"),
        }
        print(name, report[name], flush=True)

    (ROOT / "wrapper-results.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Wrote", ROOT / "wrapper-results.json", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
