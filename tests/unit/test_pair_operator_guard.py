"""AST guard for a new ``*To*Operator`` class."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.pair_operator_guard import find_violations, is_pair_operator_name, load_allowlist, main
from tools.pair_operator_guard.guard import SourceParseError

REPO = Path(__file__).resolve().parents[2]
ALLOWLIST = REPO / "tools" / "pair_operator_guard" / "allowlist.txt"


def test_flags_new_pair_operator_class() -> None:
    source = '''
class FileTransferOperator:
    pass

class S3ToGCSOperator(FileTransferOperator):
    def execute(self, context):
        return None

def factory():
    class SqlToS3Operator:
        pass
'''
    found = find_violations(Path("transfers/s3_to_gcs.py"), source, allowlist=set())
    assert [(item.name, item.lineno) for item in found] == [
        ("S3ToGCSOperator", 5),
        ("SqlToS3Operator", 10),
    ]


def test_allowlisted_class_is_ignored_and_a_new_sibling_is_flagged() -> None:
    source = '''
class S3ToGCSOperator:
    pass

class NewStoreToS3Operator:
    pass
'''
    path = Path("src/airflow/providers/amazon/transfers/s3_to_gcs.py")
    by_name = find_violations(path, source, allowlist={"S3ToGCSOperator"})
    assert [item.name for item in by_name] == ["NewStoreToS3Operator"]

    qualified = f"{path.as_posix()}:S3ToGCSOperator"
    by_path = find_violations(path, source, allowlist={qualified}, rel=path.as_posix())
    assert [item.name for item in by_path] == ["NewStoreToS3Operator"]

    # The same class name in another file is not covered by a path-qualified entry.
    other = Path("src/airflow/providers/amazon/transfers/extra.py")
    elsewhere = find_violations(other, source, allowlist={qualified}, rel=other.as_posix())
    assert [item.name for item in elsewhere] == ["S3ToGCSOperator", "NewStoreToS3Operator"]


def test_non_matching_names_are_ignored() -> None:
    source = '''
class FileTransferOperator:
    pass

class PrefixFileTransferOperator:
    pass

class S3ToGCS:
    pass

class GCSToGCSOperatorShim:
    pass

class AutoOperator:
    pass

class CopyOperator:
    pass
'''
    assert find_violations(Path("src/aip132/file_transfer.py"), source, allowlist=set()) == []
    assert is_pair_operator_name("S3ToGCSOperator")
    assert is_pair_operator_name("ToOperator")
    assert not is_pair_operator_name("FileTransferOperator")
    assert not is_pair_operator_name("AutoOperator")
    assert not is_pair_operator_name("GCSToGCSOperatorShim")


def test_allowlist_ignores_comments_and_blank_lines() -> None:
    text = """
# historical operators
S3ToGCSOperator

src/example.py:GCSToS3Operator  # one file
"""
    assert load_allowlist(text) == {"S3ToGCSOperator", "src/example.py:GCSToS3Operator"}


def test_syntax_error_is_reported(tmp_path: Path) -> None:
    broken = tmp_path / "broken.py"
    broken.write_text("class S3ToGCSOperator(\n", encoding="utf-8")
    with pytest.raises(SourceParseError):
        find_violations(broken, broken.read_text(encoding="utf-8"), allowlist=set())
    assert main([str(broken), "--allowlist", str(ALLOWLIST), "--root", str(tmp_path)]) == 2


def test_cli_flags_and_allowlist(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    module = tmp_path / "new_pair.py"
    module.write_text("class HttpToS3Operator:\n    pass\n", encoding="utf-8")
    allow = tmp_path / "allowlist.txt"
    allow.write_text("# empty\n", encoding="utf-8")
    assert main([str(module), "--allowlist", str(allow), "--root", str(tmp_path)]) == 1
    captured = capsys.readouterr()
    assert "HttpToS3Operator" in captured.out
    assert "not allowlisted" in captured.out

    allow.write_text("HttpToS3Operator\n", encoding="utf-8")
    assert main([str(module), "--allowlist", str(allow), "--root", str(tmp_path)]) == 0


def test_src_tree_matches_the_committed_allowlist() -> None:
    assert main(["src", "--allowlist", str(ALLOWLIST), "--root", str(REPO)]) == 0
