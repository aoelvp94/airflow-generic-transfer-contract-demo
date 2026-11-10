#!/usr/bin/env python3
"""Run unmodified upstream S3CopyObject/S3CopyPrefix unit tests against the shims."""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))
TEST_FILE = AIRFLOW_SRC / "providers/amazon/tests/unit/amazon/aws/operators/test_s3.py"
OPERATOR_FILE = AIRFLOW_SRC / "providers/amazon/src/airflow/providers/amazon/aws/operators/s3.py"

if not OPERATOR_FILE.is_file() or not TEST_FILE.is_file():
    sys.stderr.write(f"Missing operator or tests under {AIRFLOW_SRC}\n")
    sys.exit(2)

sys.path.insert(0, str(ROOT))
# So `from unit.amazon.aws.utils.test_template_fields import validate_template_fields` works.
sys.path.insert(0, str(AIRFLOW_SRC / "providers/amazon/tests"))

# The providers-only checkout has no tests_common. Stub just enough for
# test_s3.py to import. The scored CopyObject/CopyPrefix tests do not call
# these helpers except validate_template_fields (loaded from amazon/tests).
import types

def _stub(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod

_stub("tests_common")
_stub("tests_common.test_utils")
_stub("tests_common.test_utils.dag", sync_dag_to_db=lambda dag: None)
_stub(
    "tests_common.test_utils.taskinstance",
    create_task_instance=lambda *a, **k: None,
    render_template_fields=lambda *a, **k: None,
)
_stub("tests_common.test_utils.version_compat", AIRFLOW_V_3_0_PLUS=True)

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

# Import pip amazon provider, then overlay the SHA-matched operators.s3 module.
import airflow.providers.amazon.aws.operators.s3 as _pip_s3  # noqa: E402,F401

MOD_NAME = "airflow.providers.amazon.aws.operators.s3"
spec = importlib.util.spec_from_file_location(MOD_NAME, OPERATOR_FILE)
s3_mod = importlib.util.module_from_spec(spec)
sys.modules[MOD_NAME] = s3_mod
spec.loader.exec_module(s3_mod)
import airflow.providers.amazon.aws.operators as _ops  # noqa: E402

_ops.s3 = s3_mod

from shim import make_copy_object_shim, make_copy_prefix_shim  # noqa: E402

s3_mod.S3CopyObjectOperator = make_copy_object_shim(s3_mod)
s3_mod.S3CopyPrefixOperator = make_copy_prefix_shim(s3_mod)

import pytest  # noqa: E402

args = [
    str(TEST_FILE),
    "-vv",
    "--tb=short",
    "-k",
    "TestS3CopyObjectOperator or TestS3CopyPrefixOperator",
    f"--junitxml={ROOT / 'pytest-junit.xml'}",
    "-p",
    "no:cacheprovider",
    "--noconftest",
    "-c",
    str(ROOT / "pytest.ini"),
    "-o",
    "addopts=",
]
args.extend(sys.argv[1:])
print("Patched S3CopyObjectOperator and S3CopyPrefixOperator with shims", flush=True)
print("Test file:", TEST_FILE, flush=True)
sys.exit(pytest.main(args))
