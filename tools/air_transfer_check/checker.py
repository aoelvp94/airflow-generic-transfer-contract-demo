"""AST check and a narrow autofix for mapped per-pair transfer operators."""

from __future__ import annotations

import ast
import difflib
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from tools.air_transfer_check.catalog import (
    OPERATORS,
    RULE,
    TARGET_MODULE,
    TARGET_NAME,
    OperatorSpec,
    lookup,
)

_SKIP_DIRS = {
    ".git",
    ".venv",
    ".venv-aip132",
    "__pycache__",
    "build",
    "dist",
    ".airflow-aip132",
}
_S3_COPY_ALLOWED = {
    "task_id",
    "source_bucket_key",
    "dest_bucket_key",
    "source_bucket_name",
    "dest_bucket_name",
    "aws_conn_id",
}


@dataclass(frozen=True)
class Finding:
    """One AIR331 diagnostic."""

    rule: str
    line: int
    col: int
    operator: str
    message: str
    fixable: bool

    def format(self, path: str) -> str:
        kind = "fixable" if self.fixable else "flag-only"
        return f"{path}:{self.line}:{self.col}: {self.rule} [{kind}] {self.message}"


def check_source(source: str) -> list[Finding]:
    """Return diagnostics for ``source``. The text is not modified."""
    return _analyze(source)[0]


def fix_source(source: str) -> tuple[str, list[Finding]]:
    """Rewrite safe calls. Flag-only calls stay as they were."""
    findings, edits = _analyze(source)
    if not edits:
        return source, findings
    rewritten = _apply(source, edits)
    return _cleanup_imports(rewritten), findings


def _analyze(source: str) -> tuple[list[Finding], list[tuple[int, int, str]]]:
    tree = ast.parse(source)
    names, modules = _bindings(tree)
    target = _target_alias(tree) or TARGET_NAME
    findings: list[Finding] = []
    edits: list[tuple[int, int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            findings.extend(_import_findings(node, names))
            continue
        if not isinstance(node, ast.Call):
            continue
        spec = _resolve_call(node.func, names, modules)
        if spec is None:
            continue
        replacement = _replacement(spec, node, source, target)
        fixable = replacement is not None
        findings.append(_finding(node, spec, fixable=fixable))
        if replacement is not None:
            edits.append((*_span(source, node), replacement))
    return findings, edits


def _import_findings(node: ast.ImportFrom, names: dict[str, OperatorSpec]) -> list[Finding]:
    found: list[Finding] = []
    module = node.module or ""
    for alias in node.names:
        spec = lookup(module, alias.name)
        if spec is None:
            continue
        local = alias.asname or alias.name
        # A name bound from this import is flagged even when nothing calls it.
        if names.get(local) is not spec:
            continue
        found.append(_finding(alias, spec, fixable=False, imported=True))
    return found


def _finding(
    node: ast.AST, spec: OperatorSpec, *, fixable: bool, imported: bool = False
) -> Finding:
    if fixable:
        tail = (
            " Autofix rewrites this call to FileTransferOperator with "
            "overwrite=True and s3:// URIs."
        )
    elif imported:
        tail = " The import is removed only when every use of this name is rewritten."
    else:
        tail = " No autofix."
    return Finding(
        rule=RULE,
        line=node.lineno or 1,
        col=(node.col_offset or 0) + 1,
        operator=spec.name,
        message=f"{spec.name} is a per-pair transfer operator. {spec.hint}{tail}",
        fixable=fixable,
    )


def _bindings(
    tree: ast.AST,
) -> tuple[dict[str, OperatorSpec], dict[str, str]]:
    names: dict[str, OperatorSpec] = {}
    modules: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                spec = lookup(module, alias.name)
                if spec is not None:
                    names[alias.asname or alias.name] = spec
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    modules[alias.asname] = alias.name
    return names, modules


def _resolve_call(
    func: ast.AST,
    names: dict[str, OperatorSpec],
    modules: dict[str, str],
) -> OperatorSpec | None:
    if isinstance(func, ast.Name):
        return names.get(func.id)
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        module = modules.get(func.value.id)
        if module is not None:
            return lookup(module, func.attr)
    dotted = _dotted(func)
    if dotted is None:
        return None
    for spec in OPERATORS.values():
        if dotted == f"{spec.module}.{spec.name}":
            return spec
    return None


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        if base is None:
            return None
        return f"{base}.{node.attr}"
    return None


def _replacement(
    spec: OperatorSpec, call: ast.Call, source: str, target: str
) -> str | None:
    if spec.fix != "s3_copy_object":
        return None
    fields = _s3_copy_object_fields(call, source)
    if fields is None:
        return None
    return _render_call(target, fields, _line_indent(source, call))


def _s3_copy_object_fields(
    call: ast.Call, source: str
) -> list[tuple[str, str]] | None:
    if call.args or any(kw.arg is None for kw in call.keywords):
        return None
    kwargs = {kw.arg: kw.value for kw in call.keywords}
    if set(kwargs) - _S3_COPY_ALLOWED:
        return None
    if "task_id" not in kwargs or "source_bucket_key" not in kwargs or "dest_bucket_key" not in kwargs:
        return None
    src_uri = _s3_uri(kwargs.get("source_bucket_name"), kwargs["source_bucket_key"])
    dst_uri = _s3_uri(kwargs.get("dest_bucket_name"), kwargs["dest_bucket_key"])
    if src_uri is None or dst_uri is None:
        return None
    task = ast.get_source_segment(source, kwargs["task_id"])
    if not task:
        return None
    if "aws_conn_id" in kwargs:
        conn = ast.get_source_segment(source, kwargs["aws_conn_id"])
        if not conn:
            return None
    else:
        conn = '"aws_default"'
    return [
        ("task_id", task),
        ("src", _py_string(src_uri)),
        ("dst", _py_string(dst_uri)),
        ("source_conn_id", conn),
        ("dest_conn_id", conn),
        ("overwrite", "True"),
    ]


def _s3_uri(bucket: ast.AST | None, key: ast.AST) -> str | None:
    """Build one object URI, or return None when the call is not that shape."""
    if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
        return None
    key_text = key.value
    if not key_text or "*" in key_text or key_text.endswith("/"):
        return None
    bucket_is_set = bucket is not None and not _is_none(bucket)
    if key_text.startswith("s3://"):
        if bucket_is_set:
            return None
        rest = key_text[5:]
        bucket_part, sep, obj = rest.partition("/")
        if not sep or not bucket_part or not obj:
            return None
        return key_text
    if not bucket_is_set or not isinstance(bucket, ast.Constant) or not isinstance(bucket.value, str):
        return None
    if not bucket.value or key_text.startswith("/") or "://" in key_text:
        return None
    return f"s3://{bucket.value}/{key_text}"


def _is_none(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _py_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _render_call(name: str, fields: Sequence[tuple[str, str]], indent: int) -> str:
    base = " " * indent
    inner = base + "    "
    lines = [f"{name}("]
    for key, value in fields:
        lines.append(f"{inner}{key}={value},")
    lines.append(f"{base})")
    return "\n".join(lines)


def _line_indent(source: str, node: ast.AST) -> int:
    line = source.splitlines()[node.lineno - 1]
    return len(line) - len(line.lstrip(" "))


def _span(source: str, node: ast.AST) -> tuple[int, int]:
    lines = source.splitlines(keepends=True)
    start = sum(len(lines[i]) for i in range(node.lineno - 1)) + node.col_offset
    end = sum(len(lines[i]) for i in range(node.end_lineno - 1)) + node.end_col_offset
    return start, end


def _apply(source: str, edits: Iterable[tuple[int, int, str]]) -> str:
    for start, end, text in sorted(edits, key=lambda item: item[0], reverse=True):
        source = source[:start] + text + source[end:]
    return source


def _target_alias(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == TARGET_MODULE:
            for alias in node.names:
                if alias.name == TARGET_NAME:
                    return alias.asname or alias.name
    return None


def _cleanup_imports(source: str) -> str:
    tree = ast.parse(source)
    loaded = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    edits: list[tuple[int, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        deprecated = [alias for alias in node.names if lookup(node.module, alias.name)]
        if not deprecated:
            continue
        kept = []
        for alias in node.names:
            local = alias.asname or alias.name
            if lookup(node.module, alias.name) and local not in loaded:
                continue
            kept.append(alias)
        if len(kept) == len(node.names):
            continue
        start, end = _statement_span(source, node)
        if kept:
            edits.append((start, end, _render_from(node.module, kept) + "\n"))
        else:
            edits.append((start, end, ""))
    source = _apply(source, edits)
    tree = ast.parse(source)
    if TARGET_NAME not in {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }:
        return source
    if _target_alias(tree):
        return source
    point = _insert_point(source, tree)
    line = f"from {TARGET_MODULE} import {TARGET_NAME}\n"
    return source[:point] + line + source[point:]


def _render_from(module: str, aliases: Sequence[ast.alias]) -> str:
    parts = []
    for alias in aliases:
        if alias.asname:
            parts.append(f"{alias.name} as {alias.asname}")
        else:
            parts.append(alias.name)
    return f"from {module} import {', '.join(parts)}"


def _statement_span(source: str, node: ast.AST) -> tuple[int, int]:
    start, end = _span(source, node)
    if end < len(source) and source[end] == "\n":
        end += 1
    return start, end


def _insert_point(source: str, tree: ast.Module) -> int:
    last: ast.AST | None = None
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            last = node
        elif (
            last is None
            and isinstance(node, ast.Expr)
            and isinstance(getattr(node, "value", None), ast.Constant)
            and isinstance(node.value.value, str)
        ):
            last = node
    if last is None:
        return 0
    _, end = _span(source, last)
    if end < len(source) and source[end] == "\n":
        end += 1
    return end


def iter_python_files(paths: Sequence[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_file() and path.suffix == ".py":
            files.append(path)
            continue
        if not path.is_dir():
            continue
        for candidate in sorted(path.rglob("*.py")):
            if any(part in _SKIP_DIRS for part in candidate.parts):
                continue
            files.append(candidate)
    return files


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="tools.air_transfer_check",
        description="Flag mapped per-pair transfer operators (AIR331 prototype).",
    )
    parser.add_argument("paths", nargs="*", default=["."])
    parser.add_argument("--fix", action="store_true", help="apply safe 1:1 rewrites")
    parser.add_argument("--diff", action="store_true", help="print a unified diff of safe rewrites")
    args = parser.parse_args(list(argv) if argv is not None else None)
    exit_code = 0
    files = iter_python_files(args.paths)
    if args.paths and not files:
        print("no Python files found", file=sys.stderr)
        return 1
    for path in files:
        source = path.read_text(encoding="utf-8")
        try:
            findings = check_source(source)
        except SyntaxError as exc:
            print(f"{path}:{exc.lineno}: syntax error: {exc.msg}", file=sys.stderr)
            exit_code = 1
            continue
        rewritten, _ = fix_source(source)
        if args.diff and rewritten != source:
            diff = difflib.unified_diff(
                source.splitlines(keepends=True),
                rewritten.splitlines(keepends=True),
                fromfile=str(path),
                tofile=str(path),
            )
            sys.stdout.writelines(diff)
        if args.fix and rewritten != source:
            path.write_text(rewritten, encoding="utf-8")
        for finding in findings:
            print(finding.format(str(path)))
        if args.fix:
            try:
                remaining = check_source(rewritten)
            except SyntaxError:
                remaining = findings
            if remaining:
                exit_code = 1
        elif findings:
            exit_code = 1
    return exit_code
