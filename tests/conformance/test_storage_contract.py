"""Run the storage contract on ``file://`` and ``memory://``. No Docker."""

from __future__ import annotations

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("AIRFLOW_HOME", str(_ROOT / ".airflow-aip132"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

import pytest

pytest.importorskip("airflow.providers.common.io.operators.file_transfer")

from aip132.path_ops import _attach_memory
from tests.conformance.contract import StorageContract
from tests.conformance.harness import local_pair

pytestmark = pytest.mark.aip132

_PAIRS = (
    ("file", "file"),
    ("memory", "memory"),
    ("file", "memory"),
    ("memory", "file"),
)
_SAME = (("file", "file"), ("memory", "memory"))
_CROSS = (("file", "memory"), ("memory", "file"))


def _pair_id(value: tuple[str, str]) -> str:
    return f"{value[0]}-{value[1]}"


class TestFileAndMemoryContract(StorageContract):
    """Local backends. Cross-store here is ``file`` against ``memory``."""

    __test__ = True

    @pytest.fixture(autouse=True)
    def _memory_backend(self):
        _attach_memory(None)

    @pytest.fixture(params=_PAIRS, ids=_pair_id)
    def pair(self, request, tmp_path: Path):
        src_kind, dst_kind = request.param
        return local_pair(src_kind, dst_kind, tmp_path)

    @pytest.fixture(params=_SAME, ids=_pair_id)
    def same_pair(self, request, tmp_path: Path):
        src_kind, dst_kind = request.param
        return local_pair(src_kind, dst_kind, tmp_path)

    @pytest.fixture(params=_CROSS, ids=_pair_id)
    def cross_pair(self, request, tmp_path: Path):
        src_kind, dst_kind = request.param
        return local_pair(src_kind, dst_kind, tmp_path)
