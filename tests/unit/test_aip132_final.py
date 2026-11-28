"""``FileTransferOperator`` is ``@final``. The prefix name stays an alias.

mypy is installed by ``just aip132-final-check``. This file is not part of
``just aip132-test``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow.providers.common.io.operators.file_transfer")
pytest.importorskip("mypy")

REPO = Path(__file__).resolve().parents[2]
REJECTED = REPO / "tests" / "typing" / "final_operator_rejected.py"
ALIAS_OK = REPO / "tests" / "typing" / "final_operator_alias_ok.py"

pytestmark = pytest.mark.aip132


def _mypy(path: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["MYPYPATH"] = "src"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--follow-imports=silent",
            "--show-error-codes",
            "--no-error-summary",
            "--disable-error-code=import-untyped",
            str(path),
        ],
        cwd=REPO,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _class_line(path: Path, class_name: str) -> int:
    prefix = f"class {class_name}"
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.startswith(prefix):
            return lineno
    raise AssertionError(f"{class_name} not found in {path}")


def test_prefix_alias_is_the_same_final_class() -> None:
    from aip132.file_transfer import FileTransferOperator, PrefixFileTransferOperator

    assert PrefixFileTransferOperator is FileTransferOperator
    assert FileTransferOperator.__final__ is True


def test_mypy_rejects_per_backend_subclass_and_override() -> None:
    result = _mypy(REJECTED)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    # Class-level @final reports the subclass line. S3ToGCSOperator overrides
    # execute; GCSToS3Operator only replaces __init__. Both are rejected.
    for class_name in ("S3ToGCSOperator", "GCSToS3Operator"):
        lineno = _class_line(REJECTED, class_name)
        needle = f"final_operator_rejected.py:{lineno}: error: Cannot inherit from final class"
        assert needle in output, output


def test_mypy_accepts_the_prefix_alias() -> None:
    result = _mypy(ALIAS_OK)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
