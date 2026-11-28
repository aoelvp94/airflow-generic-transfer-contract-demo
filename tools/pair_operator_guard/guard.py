"""Fail when a class matching ``*To*Operator`` is defined outside an allowlist.

The scan is an AST walk. It does not import the module and it does not
execute it. A class created by assigning ``__name__`` after the class
statement is not a new class statement and is not reported.

Names are case-sensitive. ``S3ToGCSOperator`` matches. ``FileTransferOperator``,
``S3ToGCS``, and ``GCSToGCSOperatorShim`` do not.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# Glob *To*Operator, case-sensitive. Either side of "To" may be empty.
_PAIR_OPERATOR_NAME = re.compile(r".*To.*Operator\Z")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_ALLOWLIST = Path(__file__).resolve().parent / "allowlist.txt"
_SKIP_DIRS = frozenset({"__pycache__", ".venv", ".venv-aip132", ".git"})


@dataclass(frozen=True)
class Violation:
    """One class statement whose name matches and is not allowlisted."""

    path: Path
    lineno: int
    name: str

    def format(self) -> str:
        return (
            f"{self.path}:{self.lineno}: class {self.name} matches *To*Operator "
            "and is not allowlisted"
        )


class SourceParseError(Exception):
    """``path`` is not valid Python."""

    def __init__(self, path: Path, exc: SyntaxError) -> None:
        self.path = path
        self.exc = exc
        super().__init__(f"{path}:{exc.lineno or 1}: {exc.msg}")


def is_pair_operator_name(name: str) -> bool:
    """True when ``name`` matches the ``*To*Operator`` glob."""
    return _PAIR_OPERATOR_NAME.fullmatch(name) is not None


def load_allowlist(text: str) -> set[str]:
    """Parse allowlist text. Blank lines and ``#`` comments are ignored."""
    entries: set[str] = set()
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            entries.add(line)
    return entries


def _allowed(allowlist: set[str], rel: str, name: str) -> bool:
    rel_key = rel.replace("\\", "/")
    return name in allowlist or f"{rel_key}:{name}" in allowlist


def find_violations(path: Path, source: str, allowlist: set[str], *, rel: str | None = None) -> list[Violation]:
    """Return class statements in ``source`` that match and are not allowlisted.

    ``rel`` is the path used for path-qualified allowlist entries. It defaults
    to ``path`` rendered with forward slashes.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise SourceParseError(path, exc) from exc
    rel_key = (rel if rel is not None else path.as_posix()).replace("\\", "/")
    found: list[Violation] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and is_pair_operator_name(node.name):
            if not _allowed(allowlist, rel_key, node.name):
                found.append(Violation(path, node.lineno, node.name))
    found.sort(key=lambda item: (item.lineno, item.name))
    return found


def iter_python_files(path: Path) -> list[Path]:
    """Return ``path`` when it is a Python file, or the ``*.py`` files under a directory."""
    if path.is_file():
        return [path] if path.suffix == ".py" else []
    files: list[Path] = []
    for candidate in sorted(path.rglob("*.py")):
        if _SKIP_DIRS.intersection(candidate.parts):
            continue
        files.append(candidate)
    return files


def scan(paths: list[Path], allowlist: set[str], *, root: Path) -> tuple[list[Violation], list[SourceParseError]]:
    """Scan ``paths``. Relative allowlist keys are relative to ``root``."""
    violations: list[Violation] = []
    errors: list[SourceParseError] = []
    for path in paths:
        for file in iter_python_files(path):
            try:
                rel = file.resolve().relative_to(root.resolve()).as_posix()
            except ValueError:
                rel = file.as_posix()
            try:
                violations.extend(
                    find_violations(file, file.read_text(encoding="utf-8"), allowlist, rel=rel)
                )
            except SourceParseError as exc:
                errors.append(exc)
            except OSError as exc:
                errors.append(SourceParseError(file, SyntaxError(str(exc))))
    return violations, errors


def main(argv: list[str] | None = None) -> int:
    """Scan paths from argv. Exit 1 on a disallowed class, 2 on a parse error."""
    parser = argparse.ArgumentParser(
        description="Fail when a new *To*Operator class is defined outside the allowlist."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Files or directories. Default: src/",
    )
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=_DEFAULT_ALLOWLIST,
        help="Allowlist file (default: tools/pair_operator_guard/allowlist.txt)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=_REPO_ROOT,
        help="Root for path-qualified allowlist entries",
    )
    args = parser.parse_args(argv)
    paths = args.paths or [_REPO_ROOT / "src"]
    try:
        allow_text = args.allowlist.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"cannot read allowlist {args.allowlist}: {exc}", file=sys.stderr)
        return 2
    violations, errors = scan(paths, load_allowlist(allow_text), root=args.root)
    for error in errors:
        print(error, file=sys.stderr)
    for violation in violations:
        print(violation.format())
    if errors:
        return 2
    if violations:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
