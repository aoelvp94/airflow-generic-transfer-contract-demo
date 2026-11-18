#!/usr/bin/env python3
"""Run unmodified upstream Azure closest-transfer unit tests against the shims."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))

CORPORA = {
    "local": {
        "mod": "airflow.providers.microsoft.azure.transfers.local_to_wasb",
        "file": AIRFLOW_SRC
        / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/local_to_wasb.py",
        "tests": AIRFLOW_SRC
        / "providers/microsoft/azure/tests/unit/microsoft/azure/transfers/test_local_to_wasb.py",
        "patch": "local",
    },
    "s3_to_wasb": {
        "mod": "airflow.providers.microsoft.azure.transfers.s3_to_wasb",
        "file": AIRFLOW_SRC
        / "providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py",
        "tests": AIRFLOW_SRC
        / "providers/microsoft/azure/tests/unit/microsoft/azure/transfers/test_s3_to_wasb.py",
        "patch": "s3_to_wasb",
    },
}

which = sys.argv[1] if sys.argv[1:] and not sys.argv[1].startswith("-") else "both"
pytest_args = sys.argv[2:] if which in CORPORA or which == "both" else sys.argv[1:]
if which not in (*CORPORA, "both"):
    which = "both"
    pytest_args = sys.argv[1:]

sys.path.insert(0, str(ROOT))
os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

import pytest  # noqa: E402


def _overlay(mod_name: str, path: Path):
    import importlib

    # Ensure parent packages come from the pip-installed provider first.
    importlib.import_module(mod_name)
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    parent_name, _, attr = mod_name.rpartition(".")
    parent = importlib.import_module(parent_name)
    setattr(parent, attr, mod)
    return mod


def _run(key: str) -> int:
    from shim import make_local_to_wasb_shim, make_s3_to_wasb_shim

    meta = CORPORA[key]
    mod = _overlay(meta["mod"], meta["file"])
    if key == "local":
        mod.LocalFilesystemToWasbOperator = make_local_to_wasb_shim(mod)
        print("Patched LocalFilesystemToWasbOperator", flush=True)
    else:
        mod.S3ToAzureBlobStorageOperator = make_s3_to_wasb_shim(mod)
        print("Patched S3ToAzureBlobStorageOperator", flush=True)
    print("Test file:", meta["tests"], flush=True)
    args = [
        str(meta["tests"]),
        "-vv",
        "--tb=short",
        f"--junitxml={ROOT / f'pytest-{key}.xml'}",
        "-p",
        "no:cacheprovider",
        "--noconftest",
        "-c",
        str(ROOT / "pytest.ini"),
        "-o",
        "addopts=",
        *pytest_args,
    ]
    return pytest.main(args)


keys = list(CORPORA) if which == "both" else [which]
rc = 0
for key in keys:
    this = _run(key)
    if this:
        rc = this
sys.exit(rc)
