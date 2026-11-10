#!/usr/bin/env python3
"""Run unmodified upstream GCSToGCSOperator unit tests against the shim.

Must be started with the experiment venv. The SHA-matched operator module
is loaded from AIRFLOW_SRC after the pip-installed Airflow package is
imported, so `providers/google/src` is *not* put on PYTHONPATH (that path
contains a namespace `airflow/` package that would shadow airflow-core).

This script patches the operator class on the provider module *before*
pytest collects the test module, so
`from ...gcs_to_gcs import GCSToGCSOperator` binds to the shim. Tests
themselves are not modified.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
AIRFLOW_SRC = Path(os.environ.get("AIRFLOW_SRC", "/workspace/airflow-src"))
TEST_FILE = AIRFLOW_SRC / "providers/google/tests/unit/google/cloud/transfers/test_gcs_to_gcs.py"
OPERATOR_FILE = (
    AIRFLOW_SRC / "providers/google/src/airflow/providers/google/cloud/transfers/gcs_to_gcs.py"
)

if not OPERATOR_FILE.is_file():
    sys.stderr.write(f"AIRFLOW_SRC operator not found: {OPERATOR_FILE}\n")
    sys.exit(2)
if not TEST_FILE.is_file():
    sys.stderr.write(f"Upstream test file not found: {TEST_FILE}\n")
    sys.exit(2)

sys.path.insert(0, str(ROOT))

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

# Import the pip-installed google provider first (so `airflow` is airflow-core),
# then overlay the SHA-matched GCSToGCS operator module.
import airflow.providers.google.cloud.transfers.gcs_to_gcs as _pip_gcs  # noqa: E402,F401

MOD_NAME = "airflow.providers.google.cloud.transfers.gcs_to_gcs"
spec = importlib.util.spec_from_file_location(MOD_NAME, OPERATOR_FILE)
if spec is None or spec.loader is None:
    sys.stderr.write(f"Could not load operator module from {OPERATOR_FILE}\n")
    sys.exit(2)
gcs_mod = importlib.util.module_from_spec(spec)
sys.modules[MOD_NAME] = gcs_mod
spec.loader.exec_module(gcs_mod)
# Rebind the package attribute as well.
import airflow.providers.google.cloud.transfers as _transfers_pkg  # noqa: E402

_transfers_pkg.gcs_to_gcs = gcs_mod

from shim import make_shim_class  # noqa: E402

gcs_mod.GCSToGCSOperator = make_shim_class(gcs_mod)


def _bridge_structlog_to_stdlib() -> None:
    """Let pytest caplog see Airflow 3 operator logs.

    Upstream tests load tests_common.pytest_plugin, which is not in this
    providers-only checkout. Without a stdlib bridge, Airflow's structlog
    writes the same warning to stdout but caplog.records stays empty — the
    original GCSToGCSOperator fails test_executes_with_source_objects_not_found
    in that harness too. This is test-environment setup, not a test edit.
    """
    import logging

    import structlog
    from structlog.processors import TimeStamper
    from structlog.stdlib import BoundLogger, LoggerFactory

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=LoggerFactory(),
        wrapper_class=BoundLogger,
        cache_logger_on_first_use=False,
    )
    logging.basicConfig(level=logging.INFO)


_bridge_structlog_to_stdlib()

import pytest  # noqa: E402

junit = ROOT / "pytest-junit.xml"
args = [
    str(TEST_FILE),
    "-vv",
    "--tb=short",
    f"--junitxml={junit}",
    "-p",
    "no:cacheprovider",
    "--confcutdir",
    str(ROOT),
    "--noconftest",
    "-c",
    str(ROOT / "pytest.ini"),
    "-o",
    "addopts=",
]
args.extend(sys.argv[1:])
print("Loaded operator from", OPERATOR_FILE, flush=True)
print("Patched", MOD_NAME + ".GCSToGCSOperator", "with shim", flush=True)
print("Test file:", TEST_FILE, flush=True)
rc = pytest.main(args)
sys.exit(rc)
