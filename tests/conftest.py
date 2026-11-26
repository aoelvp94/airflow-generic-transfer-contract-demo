from __future__ import annotations

import os
import urllib.request
from pathlib import Path

# Set before any test module imports Airflow. Harmless for the other suites.
_REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("AIRFLOW_HOME", str(_REPO_ROOT / ".airflow-aip132"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

import pytest  # noqa: E402

from generic_transfer.adapters.emulators import load_emulator_env


def _http_up(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1) as resp:  # noqa: S310
            return True
    except Exception:
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=1) as resp:  # noqa: S310
                return True
        except Exception as exc:
            # Azurite root returns 400 — still means the server is up.
            return getattr(exc, "code", None) in {400, 403, 404, 405}


def emulators_available() -> bool:
    load_emulator_env()
    s3 = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090")
    gcs_host = os.environ.get("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    if "://" not in gcs_host:
        gcs_host = f"http://{gcs_host}"
    azure = "http://127.0.0.1:10000/"
    return _http_up(f"{s3}/") and _http_up(f"{gcs_host}/storage/v1/b") and _http_up(azure)


@pytest.fixture(scope="session")
def require_emulators():
    if not emulators_available():
        pytest.skip("object-store emulators not running (just up)")
    load_emulator_env()
    # Ensure gcsfs/google see the HTTP emulator host.
    host = os.environ.get("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    if "://" not in host:
        host = f"http://{host}"
    os.environ["STORAGE_EMULATOR_HOST"] = host
