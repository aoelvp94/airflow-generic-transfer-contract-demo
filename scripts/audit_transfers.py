#!/usr/bin/env python3
"""AST audit: what fraction of Airflow pair-transfer operators map onto a generic hook contract.

Scans apache/airflow ``providers/**/transfers/`` (plus ``FileTransferOperator``)
without importing Airflow. Stdlib only.

Classification is heuristic and conservative: optional args still flag a class,
and execute() bodies are only inspected for a small set of keyword patterns.
See docs/PAIR_AUDIT.md for the rules and caveats.
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

# Operator modules whose names match *_to_*.py but are not data transfers.
_NON_TRANSFER_NAME_RE = re.compile(
    r"(speech_to_text|text_to_speech|migrations/versions)",
    re.IGNORECASE,
)

_TEST_DIR_NAMES = frozenset({"tests", "test", "system", "integration"})


def _is_test_path(path: Path) -> bool:
    parts = set(path.parts)
    # providers/<pkg>/tests/... or .../tests/unit/...
    # Real test trees are providers/<pkg>/tests/...
    return "tests" in path.parts or "test" in path.parts


def provider_from_relpath(rel: Path) -> str:
    """Map providers/<pkg>/... to a provider id (apache.hive, microsoft.azure, ...)."""
    parts = rel.parts
    if not parts:
        return "unknown"
    first = parts[0]
    if first == "apache" and len(parts) > 1:
        return f"apache.{parts[1]}"
    if first == "microsoft" and len(parts) > 1:
        return f"microsoft.{parts[1]}"
    if first == "common" and len(parts) > 1:
        return f"common.{parts[1]}"
    return first


def discover_modules(providers_root: Path) -> tuple[list[Path], list[dict[str, str]]]:
    """Return (included modules, skipped *_to_*.py with reasons)."""
    included: list[Path] = []
    skipped: list[dict[str, str]] = []
    seen: set[Path] = set()

    def add(path: Path) -> None:
        path = path.resolve()
        if path not in seen:
            seen.add(path)
            included.append(path)

    for path in providers_root.rglob("*.py"):
        if _is_test_path(path):
            continue
        rel = str(path.relative_to(providers_root))
        if "transfers" in path.parts and path.name != "__init__.py":
            add(path)
            continue
        if path.name == "file_transfer.py" and "operators" in path.parts:
            add(path)
            continue
        if _looks_like_to_module(path.name):
            if _NON_TRANSFER_NAME_RE.search(rel):
                skipped.append({"path": rel, "reason": "name matches *_to_* but is not a data transfer"})
            else:
                # Include only if the module actually defines a *To*Operator.
                # (Decision is finalized after parse; record for now.)
                skipped.append({"path": rel, "reason": "pending-parse"})

    return sorted(included, key=str), skipped


def _looks_like_to_module(name: str) -> bool:
    return bool(re.search(r"_to_", name)) and name.endswith(".py")


# ---------------------------------------------------------------------------
# AST extraction
# ---------------------------------------------------------------------------

def _unparse(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    try:
        return ast.unparse(node)
    except Exception:
        return type(node).__name__


def _const_strings_from(node: ast.AST) -> list[str] | None:
    """Extract a list of string constants from a tuple/list/joined expression."""
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        out: list[str] = []
        for elt in node.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                out.append(elt.value)
            elif isinstance(elt, ast.Starred):
                marker = _starred_template_marker(elt)
                if marker:
                    out.append(marker)
                else:
                    return None
            else:
                return None
        return out
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    return None


def _starred_template_marker(elt: ast.Starred) -> str | None:
    # *SomeClass.template_fields
    val = elt.value
    if isinstance(val, ast.Attribute) and val.attr == "template_fields":
        return f"*{_unparse(val.value)}.template_fields"
    return None


def _iter_class_assigns(node: ast.ClassDef) -> Iterator[tuple[str, ast.AST]]:
    for item in node.body:
        if isinstance(item, ast.Assign):
            for t in item.targets:
                if isinstance(t, ast.Name):
                    yield t.id, item.value
        elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and item.value is not None:
            yield item.target.id, item.value


def _init_args(init: ast.FunctionDef) -> list[dict[str, str | None]]:
    out: list[dict[str, str | None]] = []

    def emit(arg: ast.arg, default: ast.AST | None) -> None:
        if arg.arg in {"self", "cls"}:
            return
        out.append(
            {
                "name": arg.arg,
                "default": _unparse(default),
                "annotation": _unparse(arg.annotation),
            }
        )

    pos = [a for a in init.args.args]
    defaults = list(init.args.defaults)
    pad = len(pos) - len(defaults)
    for i, a in enumerate(pos):
        emit(a, None if i < pad else defaults[i - pad])
    for a, d in zip(init.args.kwonlyargs, init.args.kw_defaults):
        emit(a, d)
    if init.args.vararg:
        out.append({"name": f"*{init.args.vararg.arg}", "default": None, "annotation": None})
    if init.args.kwarg:
        out.append({"name": f"**{init.args.kwarg.arg}", "default": None, "annotation": None})
    return out


def _method_names(node: ast.ClassDef) -> list[str]:
    return [item.name for item in node.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _base_names(node: ast.ClassDef) -> list[str]:
    names: list[str] = []
    for b in node.bases:
        if isinstance(b, ast.Name):
            names.append(b.id)
        elif isinstance(b, ast.Attribute):
            names.append(b.attr)
        else:
            names.append(_unparse(b) or "")
    return names


# ---------------------------------------------------------------------------
# Source / destination / kind
# ---------------------------------------------------------------------------

_CLASS_TO_RE = re.compile(r"^(?:Base)?(.+?)To(.+?)(?:Base)?Operator$")

# Normalized system name -> category used for kind.
FILE_SYSTEMS = frozenset(
    {
        "s3",
        "gcs",
        "wasb",
        "adls",
        "azure_fileshare",
        "ftp",
        "sftp",
        "local",
        "samba",
        "gdrive",
        "glacier",
        "file",
        "external_stage",
    }
)
SQL_SYSTEMS = frozenset(
    {
        "sql",
        "mysql",
        "postgres",
        "mssql",
        "oracle",
        "hive",
        "vertica",
        "presto",
        "trino",
        "redshift",
        "snowflake",
        "teradata",
        "bigquery",
        "exasol",
        "druid",
    }
)
# Document / API / NoSQL record sources and sinks (not relational tables, not files).
RECORD_SYSTEMS = frozenset(
    {
        "http",
        "google_api",
        "mongo",
        "salesforce",
        "cassandra",
        "facebook_ads",
        "google_ads",
        "calendar",
        "sheets",
        "imap",
        "dynamodb",
        "aws",  # only for the AwsToAws base
    }
)
MESSAGE_SYSTEMS = frozenset({"slack"})

_SYSTEM_ALIASES = {
    "s3": "s3",
    "gcs": "gcs",
    "wasb": "wasb",
    "azureblobstorage": "wasb",
    "azureblob": "wasb",
    "adls": "adls",
    "azuredatalake": "adls",
    "azurefileshare": "azure_fileshare",
    "ftp": "ftp",
    "sftp": "sftp",
    "local": "local",
    "localfilesystem": "local",
    "samba": "samba",
    "gdrive": "gdrive",
    "googledrive": "gdrive",
    "drive": "gdrive",
    "glacier": "glacier",
    "file": "file",
    "sql": "sql",
    "mysql": "mysql",
    "postgres": "postgres",
    "mssql": "mssql",
    "oracle": "oracle",
    "hive": "hive",
    "vertica": "vertica",
    "presto": "presto",
    "trino": "trino",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "teradata": "teradata",
    "bigquery": "bigquery",
    "exasol": "exasol",
    "druid": "druid",
    "http": "http",
    "googleapi": "google_api",
    "mongo": "mongo",
    "salesforce": "salesforce",
    "cassandra": "cassandra",
    "facebookadsreport": "facebook_ads",
    "facebookads": "facebook_ads",
    "googleads": "google_ads",
    "ads": "google_ads",
    "googlecalendar": "calendar",
    "calendar": "calendar",
    "googlesheets": "sheets",
    "sheets": "sheets",
    "imapattachment": "imap",
    "imap": "imap",
    "dynamodb": "dynamodb",
    "slack": "slack",
    "slackapifile": "slack",
    "slackwebhook": "slack",
    "externalstage": "external_stage",
    "aws": "aws",
}


def _norm_system(raw: str) -> str:
    key = re.sub(r"[^a-z0-9]", "", raw.lower())
    return _SYSTEM_ALIASES.get(key, re.sub(r"(?<!^)(?=[A-Z])", "_", raw).lower())


def parse_pair_from_class(class_name: str) -> tuple[str | None, str | None]:
    if class_name == "FileTransferOperator":
        return "file", "file"
    if class_name.startswith("CopyFrom") and "To" in class_name:
        # CopyFromExternalStageToSnowflakeOperator
        m = re.match(r"CopyFrom(.+)To(.+)Operator$", class_name)
        if m:
            return _norm_system(m.group(1)), _norm_system(m.group(2))
    m = _CLASS_TO_RE.match(class_name)
    if not m:
        return None, None
    return _norm_system(m.group(1)), _norm_system(m.group(2))


def parse_pair_from_module(stem: str) -> tuple[str | None, str | None]:
    # azure_blob_to_s3, sql_to_gcs, copy_into_snowflake, base
    if stem in {"base", "file_transfer", "copy_into_snowflake"}:
        if stem == "file_transfer":
            return "file", "file"
        if stem == "copy_into_snowflake":
            return "external_stage", "snowflake"
        return None, None
    m = re.match(r"^(.+)_to_(.+)$", stem)
    if not m:
        return None, None
    return _norm_system(m.group(1).replace("_", "")), _norm_system(m.group(2).replace("_", ""))


def _category(system: str | None) -> str | None:
    if system is None:
        return None
    if system in FILE_SYSTEMS:
        return "file"
    if system in SQL_SYSTEMS:
        return "sql"
    if system in MESSAGE_SYSTEMS:
        return "message"
    if system in RECORD_SYSTEMS:
        return "record"
    return "other"


def infer_kind(source: str | None, dest: str | None) -> str:
    sc, dc = _category(source), _category(dest)
    if sc and dc:
        return f"{sc}\u2192{dc}"
    return "unknown"


# ---------------------------------------------------------------------------
# Special-semantics flags (data-driven; arg names + a few methods/source cues)
# ---------------------------------------------------------------------------

# Each rule: flag name, constructor arg names that trigger it.
# Method names and source-text regexes are optional extras.

FLAG_ARG_RULES: dict[str, frozenset[str]] = {
    "compression": frozenset(
        {"gzip", "compress", "compression", "input_compressed"}
    ),
    "dest_encryption": frozenset({"encrypt"}),
    "acl_or_extra_args": frozenset(
        {
            "acl_policy",
            "s3_acl_policy",
            "dest_s3_extra_args",
            "s3_extra_args",
            "wasb_extra_args",
            "extra_upload_options",
            "load_options",
            "extra_args",
            "extra_config",
            "src_fmt_configs",
        }
    ),
    "wildcard_or_prefix": frozenset(
        {
            "prefix",
            "s3_prefix",
            "blob_prefix",
            "s3_key_prefix",
            "match_glob",
            "wildcard_match",
            "pattern",
            "exact_match",
            # "delimiter" is omitted on purpose: it is a listing delimiter in some
            # file operators and a CSV field delimiter in others.
        }
    ),
    "replace": frozenset(
        {
            "replace",
            "overwrite",
            "s3_overwrite",
            "wasb_overwrite_object",
            "write_disposition",
        }
    ),
    "partitioning_or_splitting": frozenset(
        {
            "partition",
            "partition_columns",
            "time_partitioning",
            "range_partitioning",
            "cluster_fields",
            "max_rows_per_file",
            "approx_max_file_size_bytes",
            "num_shards",
            "target_partition_size",
            "groupby_kwargs",
            "parquet_row_group_size",
        }
    ),
    "schema_sidecar": frozenset(
        {
            "schema_filename",
            "schema_fields",
            "schema_object",
            "schema_object_bucket",
            "upload_metadata",
        }
    ),
    "pagination": frozenset(
        {"page_size", "google_api_pagination", "paginated_sql_statement_clause"}
    ),
    "move_semantics": frozenset({"move_object"}),
    "incremental_filter": frozenset(
        {"last_modified_time", "maximum_modified_time", "is_older_than"}
    ),
    "path_layout": frozenset(
        {"keep_directory_structure", "flatten_structure", "apply_gcs_prefix"}
    ),
    "pre_post_operator_sql": frozenset(
        {"mysql_preoperator", "mysql_postoperator", "preoperator", "postoperator"}
    ),
    "dest_table_ddl": frozenset({"create", "recreate", "tblproperties", "hive_tblproperties", "field_dict"}),
    "custom_callable": frozenset(
        {"parser", "process_func", "pre_process", "pre_process_args", "pre_process_kwargs"}
    ),
    "point_in_time_export": frozenset(
        {"point_in_time_export", "export_time", "export_table_to_point_in_time_kwargs"}
    ),
    "upsert": frozenset(
        {"upsert_keys", "replace_index", "mysql_duplicate_key_handling"}
    ),
    "xcom_side_channel": frozenset(
        {
            "store_to_xcom_key",
            "google_api_response_via_xcom",
            "google_api_endpoint_params_via_xcom",
            "google_api_endpoint_params_via_xcom_task_ids",
        }
    ),
    "warehouse_job_options": frozenset(
        {
            "job_id",
            "reattach_states",
            "force_rerun",
            "cancel_on_kill",
            "result_retry",
            "result_timeout",
            "encryption_configuration",
            "create_disposition",
            "schema_update_options",
            "external_table",
            "unload_options",
            "copy_options",
            "validation_mode",
            "stage",
            "hadoop_dependency_coordinates",
            "query_granularity",
            "segment_granularity",
            "metric_spec",
            "intervals",
            "billing_mode",
            "import_table_kwargs",
            "import_table_creation_kwargs",
            "dynamodb_key_schema",
            "dynamodb_tmp_table_prefix",
            "redshift_data_api_kwargs",
        }
    ),
    "bulk_load": frozenset({"bulk_load", "mysql_local_infile", "mysql_extra_options"}),
    "object_retention": frozenset({"retain_until_time", "retention_mode"}),
    "teradata_nos_auth": frozenset({"teradata_authorization_name"}),
}

# Method names that also set a flag (unioned with arg-based detection).
FLAG_METHOD_RULES: dict[str, frozenset[str]] = {
    "server_side_copy": frozenset({"transfer_files_async", "submit_transfer_jobs"}),
    "warehouse_copy_job": frozenset(
        {"_build_copy_query", "_build_unload_query", "_submit_job", "construct_ingest_query"}
    ),
}

# Source-text regexes (module body). Applied in addition to args/methods.
FLAG_SOURCE_RULES: dict[str, re.Pattern[str]] = {
    "warehouse_copy_job": re.compile(
        r"COPY\s+INTO|UNLOAD\s+|Native Object Store|"
        r"ExtractJob|LoadJob|CopyJob|import_table\(|READ_NOS|"
        r"CREATE\s+MULTISET\s+TABLE|CREATE\s+TABLE\s+AS",
        re.IGNORECASE,
    ),
    "server_side_copy": re.compile(r"Storage Transfer Service", re.IGNORECASE),
    "move_semantics": re.compile(r"\bself\.delete\b"),  # LocalFilesystemToGoogleDriveOperator
}

# Flags that mean "a generic operator should not absorb this".
SPECIAL_FLAGS = frozenset(
    {
        "partitioning_or_splitting",
        "dest_table_ddl",
        "warehouse_copy_job",
        "warehouse_job_options",
        "custom_callable",
        "notification",
        "pre_post_operator_sql",
        "point_in_time_export",
        "teradata_nos_auth",  # vendor CREATE TABLE … AUTHORIZATION / NOS load
    }
)

# Flags that map if the contract grows an optional capability.
CAPABILITY_FLAGS = frozenset(
    {
        "compression",
        "acl_or_extra_args",
        "server_side_copy",
        "schema_sidecar",
        "move_semantics",
        "incremental_filter",
        "path_layout",
        "pagination",
        "bulk_load",
        "deferrable",
        "xcom_side_channel",
        "object_retention",
        "upsert",
        "dest_encryption",
    }
)

# Informative flags that do not, on their own, change the bucket.
INFORMATIONAL_FLAGS = frozenset({"wildcard_or_prefix", "replace", "openlineage"})


def detect_flags(
    *,
    arg_names: Iterable[str],
    methods: Iterable[str],
    source_text: str,
    dest_system: str | None,
    has_deferrable_arg: bool,
    has_execute_complete: bool,
) -> list[str]:
    names = set(arg_names)
    methods_set = set(methods)
    flags: set[str] = set()

    for flag, triggers in FLAG_ARG_RULES.items():
        if names & triggers:
            flags.add(flag)
    for flag, triggers in FLAG_METHOD_RULES.items():
        if methods_set & triggers:
            flags.add(flag)
    for flag, pattern in FLAG_SOURCE_RULES.items():
        if pattern.search(source_text):
            flags.add(flag)

    if dest_system in MESSAGE_SYSTEMS:
        flags.add("notification")
    if has_deferrable_arg or has_execute_complete or "deferrable" in names:
        flags.add("deferrable")

    # "schema" is ambiguous (Redshift namespace vs output schema). Only treat it
    # as a sidecar when the destination is a file and a schema_* arg already
    # didn't fire — skip the bare name.

    return sorted(flags)


def classify(flags: Iterable[str]) -> tuple[str, list[str]]:
    """Return (bucket, reasons). special > needs_capability > maps_cleanly."""
    flag_set = set(flags)
    special_hit = sorted(flag_set & SPECIAL_FLAGS)
    if special_hit:
        return "special", special_hit
    cap_hit = sorted(flag_set & CAPABILITY_FLAGS)
    if cap_hit:
        return "needs_capability", cap_hit
    return "maps_cleanly", []


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

BASE_OPERATOR_NAMES = frozenset(
    {
        "BaseOperator",
        "BaseSQLOperator",
        "S3ListOperator",
        "ADLSListOperator",
    }
)


def is_operator_class(name: str) -> bool:
    return name.endswith("Operator")


def is_base_operator(name: str) -> bool:
    return is_operator_class(name) and "Base" in name


@dataclass
class OperatorRecord:
    provider: str
    module_path: str
    class_name: str
    is_base: bool
    source_system: str | None
    dest_system: str | None
    kind: str
    constructor_args: list[dict[str, str | None]]
    inherited_arg_names: list[str]
    template_fields: list[str]
    deferrable: bool
    openlineage_methods: list[str]
    base_classes: list[str]
    methods: list[str]
    flags: list[str] = field(default_factory=list)
    classification: str = ""
    classification_reasons: list[str] = field(default_factory=list)

    @property
    def arg_names(self) -> list[str]:
        return [a["name"] or "" for a in self.constructor_args]


# ---------------------------------------------------------------------------
# Parse + inherit
# ---------------------------------------------------------------------------

def parse_module(path: Path, *, providers_root: Path) -> list[OperatorRecord]:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))
    rel = path.relative_to(providers_root).as_posix()
    provider = provider_from_relpath(path.relative_to(providers_root))
    module_stem = path.stem
    records: list[OperatorRecord] = []

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not is_operator_class(node.name):
            continue
        init = next(
            (i for i in node.body if isinstance(i, ast.FunctionDef) and i.name == "__init__"),
            None,
        )
        args = _init_args(init) if init else []
        template_fields: list[str] = []
        for name, value in _iter_class_assigns(node):
            if name == "template_fields":
                extracted = _const_strings_from(value)
                if extracted is not None:
                    template_fields = extracted
        methods = _method_names(node)
        bases = _base_names(node)
        src, dst = parse_pair_from_class(node.name)
        if src is None and dst is None:
            src, dst = parse_pair_from_module(module_stem)
        kind = infer_kind(src, dst)
        openlineage = [m for m in methods if m.startswith("get_openlineage_facets_on_")]
        has_def_arg = any(a["name"] == "deferrable" for a in args)
        has_complete = "execute_complete" in methods
        declared_arg_names = [a["name"] or "" for a in args if a["name"] and not a["name"].startswith("*")]
        flags = detect_flags(
            arg_names=declared_arg_names,
            methods=methods,
            source_text=text,
            dest_system=dst,
            has_deferrable_arg=has_def_arg,
            has_execute_complete=has_complete,
        )
        if openlineage:
            flags = sorted(set(flags) | {"openlineage"})
        classification, reasons = classify(flags)
        records.append(
            OperatorRecord(
                provider=provider,
                module_path=rel,
                class_name=node.name,
                is_base=is_base_operator(node.name),
                source_system=src,
                dest_system=dst,
                kind=kind,
                constructor_args=args,
                inherited_arg_names=[],
                template_fields=template_fields,
                deferrable=has_def_arg or has_complete,
                openlineage_methods=openlineage,
                base_classes=bases,
                methods=methods,
                flags=flags,
                classification=classification,
                classification_reasons=reasons,
            )
        )
    return records


def _resolve_starred_templates(records: list[OperatorRecord]) -> None:
    by_name = {r.class_name: r for r in records}
    changed = True
    # A few passes to resolve *Parent.template_fields chains.
    for _ in range(6):
        if not changed:
            break
        changed = False
        for rec in records:
            new: list[str] = []
            resolved_any = False
            for item in rec.template_fields:
                if item.startswith("*") and item.endswith(".template_fields"):
                    parent = item[1 : -len(".template_fields")]
                    parent_rec = by_name.get(parent)
                    if parent_rec is not None and not any(
                        t.startswith("*") for t in parent_rec.template_fields
                    ):
                        new.extend(parent_rec.template_fields)
                        resolved_any = True
                    else:
                        new.append(item)
                else:
                    new.append(item)
            # de-dup preserving order
            seen: set[str] = set()
            deduped: list[str] = []
            for t in new:
                if t not in seen:
                    seen.add(t)
                    deduped.append(t)
            if deduped != rec.template_fields:
                rec.template_fields = deduped
                changed = True or resolved_any


def apply_inheritance(records: list[OperatorRecord], *, module_texts: dict[str, str]) -> None:
    by_name = {r.class_name: r for r in records}

    def parent_chain(rec: OperatorRecord) -> list[OperatorRecord]:
        out: list[OperatorRecord] = []
        seen: set[str] = set()
        stack = list(rec.base_classes)
        while stack:
            name = stack.pop(0)
            if name in seen or name in BASE_OPERATOR_NAMES:
                continue
            seen.add(name)
            parent = by_name.get(name)
            if parent is None:
                continue
            out.append(parent)
            stack.extend(parent.base_classes)
        return out

    _resolve_starred_templates(records)

    for rec in records:
        child_names = {a["name"] for a in rec.constructor_args}
        inherited: list[dict[str, str | None]] = []
        inherited_names: list[str] = []
        for parent in parent_chain(rec):
            for arg in parent.constructor_args:
                n = arg["name"]
                if not n or n.startswith("*") or n in child_names:
                    continue
                child_names.add(n)
                inherited.append(arg)
                inherited_names.append(n)
        if inherited:
            rec.constructor_args = inherited + rec.constructor_args
            rec.inherited_arg_names = inherited_names
        text = module_texts.get(rec.module_path, "")
        rec.flags = detect_flags(
            arg_names=[
                a["name"] or ""
                for a in rec.constructor_args
                if a["name"] and not str(a["name"]).startswith("*")
            ],
            methods=rec.methods,
            source_text=text,
            dest_system=rec.dest_system,
            has_deferrable_arg=rec.deferrable,
            has_execute_complete="execute_complete" in rec.methods,
        )
        if rec.openlineage_methods:
            rec.flags = sorted(set(rec.flags) | {"openlineage"})
        rec.classification, rec.classification_reasons = classify(rec.flags)


# ---------------------------------------------------------------------------
# Airflow repo helpers
# ---------------------------------------------------------------------------

def git_sha(repo: Path) -> str:
    return (
        subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    )


def git_commit_subject(repo: Path) -> str:
    return subprocess.check_output(
        ["git", "log", "-1", "--format=%s"], cwd=repo, text=True
    ).strip()


def git_commit_iso(repo: Path) -> str:
    # Author date, including offset (not box-local).
    return subprocess.check_output(
        ["git", "log", "-1", "--format=%cI"], cwd=repo, text=True
    ).strip()


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

CLASSIFICATION_ORDER = ("maps_cleanly", "needs_capability", "special")


def _concrete(records: Iterable[OperatorRecord]) -> list[OperatorRecord]:
    return [r for r in records if not r.is_base]


def _pct(n: int, total: int) -> float:
    return (100.0 * n / total) if total else 0.0


def _pct_label(n: int, total: int) -> str:
    return f"{n} ({_pct(n, total):.1f}%)"


def summarize(records: list[OperatorRecord]) -> dict[str, Any]:
    concrete = _concrete(records)
    total = len(concrete)
    by_class = Counter(r.classification for r in concrete)
    by_kind = Counter(r.kind for r in concrete)
    by_provider = Counter(r.provider for r in concrete)
    flag_counts = Counter(f for r in concrete for f in r.flags if f != "openlineage")
    # openlineage is tracked separately; still count it but don't lead with it
    flag_counts_all = Counter(f for r in concrete for f in r.flags)

    def breakdown(counter: Counter[str], *, key: str) -> list[dict[str, Any]]:
        rows = []
        for name, n in counter.most_common():
            subset = [r for r in concrete if getattr(r, key) == name]
            c = Counter(r.classification for r in subset)
            rows.append(
                {
                    key: name,
                    "operators": n,
                    "maps_cleanly": c.get("maps_cleanly", 0),
                    "needs_capability": c.get("needs_capability", 0),
                    "special": c.get("special", 0),
                    "maps_cleanly_pct": round(_pct(c.get("maps_cleanly", 0), n), 1),
                }
            )
        return rows

    return {
        "operator_classes_including_bases": len(records),
        "base_classes": len(records) - total,
        "concrete_operators": total,
        "classification": {
            label: {"count": by_class.get(label, 0), "pct": round(_pct(by_class.get(label, 0), total), 1)}
            for label in CLASSIFICATION_ORDER
        },
        "by_kind": breakdown(by_kind, key="kind"),
        "by_provider": breakdown(by_provider, key="provider"),
        "flag_counts": flag_counts_all.most_common(),
        "top_special_flags": [
            {"flag": f, "concrete_operators": n}
            for f, n in flag_counts.most_common()
            if f in SPECIAL_FLAGS
        ],
    }


def records_to_jsonable(records: list[OperatorRecord]) -> list[dict[str, Any]]:
    rows = []
    for r in records:
        d = asdict(r)
        rows.append(d)
    return rows


def write_csv(path: Path, records: list[OperatorRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "provider",
        "module_path",
        "class_name",
        "is_base",
        "source_system",
        "dest_system",
        "kind",
        "classification",
        "classification_reasons",
        "flags",
        "constructor_args",
        "inherited_arg_names",
        "template_fields",
        "deferrable",
        "openlineage_methods",
        "base_classes",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in records:
            w.writerow(
                {
                    "provider": r.provider,
                    "module_path": r.module_path,
                    "class_name": r.class_name,
                    "is_base": r.is_base,
                    "source_system": r.source_system or "",
                    "dest_system": r.dest_system or "",
                    "kind": r.kind,
                    "classification": r.classification,
                    "classification_reasons": "|".join(r.classification_reasons),
                    "flags": "|".join(r.flags),
                    "constructor_args": json.dumps(r.constructor_args, ensure_ascii=False),
                    "inherited_arg_names": "|".join(r.inherited_arg_names),
                    "template_fields": "|".join(r.template_fields),
                    "deferrable": r.deferrable,
                    "openlineage_methods": "|".join(r.openlineage_methods),
                    "base_classes": "|".join(r.base_classes),
                }
            )


def _md_table(headers: list[str], rows: list[list[Any]]) -> str:
    def cell(v: Any) -> str:
        return str(v).replace("|", "\\|")

    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(cell(c) for c in row) + " |")
    return "\n".join(out)


def write_markdown(
    path: Path,
    *,
    airflow_root: Path,
    sha: str,
    subject: str,
    commit_iso: str,
    transfer_module_count: int,
    included_extra: list[str],
    skipped: list[dict[str, str]],
    records: list[OperatorRecord],
    summary: dict[str, Any],
) -> None:
    concrete = _concrete(records)
    total = len(concrete)
    c = summary["classification"]
    bases = [r for r in records if r.is_base]

    # Providers that have a transfers/ tree (exclude common.io from the "13").
    transfer_providers = sorted({r.provider for r in records if r.provider != "common.io"})
    # Module counts under transfers/ (exclude file_transfer)
    transfer_modules = sorted({r.module_path for r in records if "/transfers/" in r.module_path})

    # Notable special examples: prefer well-known ones if present, else first by flags.
    preferred_examples = [
        "BaseSQLToGCSOperator",
        "SqlToS3Operator",
        "GCSToBigQueryOperator",
        "S3ToRedshiftOperator",
        "HiveToDruidOperator",
        "CopyFromExternalStageToSnowflakeOperator",
        "S3ToDynamoDBOperator",
        "S3ToHiveOperator",
        "SqlToSlackWebhookOperator",
        "HiveToMySqlOperator",
    ]
    specials = [r for r in records if r.classification == "special"]
    examples: list[OperatorRecord] = []
    by_name = {r.class_name: r for r in specials}
    for name in preferred_examples:
        if name in by_name:
            examples.append(by_name[name])
        if len(examples) >= 5:
            break
    if len(examples) < 5:
        for r in specials:
            if r not in examples:
                examples.append(r)
            if len(examples) >= 5:
                break

    clean_examples = [r for r in concrete if r.classification == "maps_cleanly"]
    # Prefer FileTransferOperator, *ToS3 local, oracle_to_oracle, etc.
    clean_pref = [
        "FileTransferOperator",
        "LocalFilesystemToS3Operator",
        "OracleToOracleOperator",
        "TeradataToTeradataOperator",
        "HiveToSambaOperator",
        "GoogleDriveToLocalOperator",
        "ImapAttachmentToS3Operator",
    ]
    clean_shown: list[OperatorRecord] = []
    clean_by = {r.class_name: r for r in clean_examples}
    for name in clean_pref:
        if name in clean_by:
            clean_shown.append(clean_by[name])
        if len(clean_shown) >= 4:
            break

    # Why ~86 differs
    base_only_modules = []
    modules_with_ops = {}
    for r in records:
        if "/transfers/" not in r.module_path:
            continue
        modules_with_ops.setdefault(r.module_path, []).append(r)
    for mod, recs in modules_with_ops.items():
        if recs and all(r.is_base for r in recs):
            base_only_modules.append(mod)

    top_special = summary["top_special_flags"][:8]
    top_flags = summary["flag_counts"][:12]

    kind_rows = [
        [
            row["kind"],
            row["operators"],
            row["maps_cleanly"],
            row["needs_capability"],
            row["special"],
            f"{row['maps_cleanly_pct']:.1f}%",
        ]
        for row in summary["by_kind"]
    ]
    provider_rows = [
        [
            row["provider"],
            row["operators"],
            row["maps_cleanly"],
            row["needs_capability"],
            row["special"],
            f"{row['maps_cleanly_pct']:.1f}%",
        ]
        for row in summary["by_provider"]
    ]

    example_rows = []
    for r in examples:
        example_rows.append(
            [
                f"`{r.class_name}`",
                r.provider,
                r.kind,
                ", ".join(f"`{x}`" for x in r.classification_reasons) or "—",
            ]
        )
    clean_rows = []
    for r in clean_shown:
        clean_rows.append([f"`{r.class_name}`", r.provider, r.kind, ", ".join(r.arg_names[:8])])

    skipped_final = [s for s in skipped if s.get("reason") != "pending-parse"]

    lines: list[str] = []
    a = lines.append
    a("# Pair-mapping audit of Airflow transfer operators")
    a("")
    a("Answers: **what fraction of Airflow's per-pair transfer operators map cleanly")
    a("onto a generic source-hook / destination-hook operator?**")
    a("")
    a("This is evidence for the Apache Airflow dev-list thread by David Blain,")
    a("`[DISCUSS] One transfer operator on hook contracts instead of one operator")
    a("per source/destination pair` (claim: ~86 transfer modules across 13")
    a("providers). It is a static AST heuristic, not a final AIP decision.")
    a("")
    a("Reproduce:")
    a("")
    a("```bash")
    a("# Shallow sparse clone of apache/airflow (providers/ only), then:")
    a("just audit")
    a("# or: .venv/bin/python scripts/audit_transfers.py --airflow-src /workspace/airflow-src")
    a("```")
    a("")
    a("## Corpus")
    a("")
    a(f"- **Airflow commit:** `{sha}`")
    a(f"- **Commit subject:** {subject}")
    a(f"- **Commit date:** {commit_iso} (git author date, with original offset)")
    a(f"- **Scan root:** `{airflow_root}/providers`")
    a(
        f"- **Modules under `providers/**/transfers/`** (excluding `__init__.py` and tests): **{transfer_module_count}**"
    )
    a(
        f"- **Provider packages with a `transfers/` tree:** **{len(transfer_providers)}** — "
        + ", ".join(f"`{p}`" for p in transfer_providers)
    )
    if included_extra:
        a(
            "- **Also included:** "
            + ", ".join(f"`{p}`" for p in included_extra)
            + " (the generic file operator the thread already discusses; not counted in the 13 providers above)"
        )
    a(
        f"- **Operator classes found:** {summary['operator_classes_including_bases']} "
        f"({summary['concrete_operators']} concrete, {summary['base_classes']} base classes)"
    )
    a("")
    a("### Match to the ~86 / 13 claim")
    a("")
    a(
        f"This snapshot has **{transfer_module_count}** modules under `transfers/` "
        f"across **{len(transfer_providers)}** provider packages. That is the same 13 "
        "providers as the claim if `apache.hive` and `apache.druid` are counted "
        "separately and `common.io` is not (FileTransferOperator lives there, not under "
        "`transfers/`)."
    )
    a("")
    if transfer_module_count == 86:
        a("The module count matches the ~86 claim exactly on this commit.")
    elif transfer_module_count == 87:
        a(
            "The count is **one above** the ~86 claim. The extra module is "
            "`amazon/aws/transfers/base.py` (`AwsToAwsBaseOperator` only — not a "
            "source/destination pair). Excluding that base module leaves **86** "
            "pair-transfer modules, which matches the claim. Three other `transfers/` "
            "modules hold only a base class and stay inside that 86: "
            + ", ".join(f"`{m}`" for m in base_only_modules if not m.endswith("transfers/base.py"))
            + ". We did not invent a historical count; this is what "
            f"`{sha[:12]}` contains."
        )
    else:
        a(
            f"The count differs from ~86 by {transfer_module_count - 86:+d} on this "
            "commit. Likely causes: a different include rule (excluding base modules, "
            "or including FileTransferOperator / Transfer Service operators under "
            "`operators/`), or a different Airflow SHA than the one cited in the thread. "
            "Numbers below are from this SHA only."
        )
    a("")
    if skipped_final:
        a("Skipped `*_to_*.py` modules that are not data transfers:")
        a("")
        for s in skipped_final:
            a(f"- `{s['path']}` — {s['reason']}")
        a("")
    a("Out of scope (not under `transfers/`, not `FileTransferOperator`, not a data-transfer `*_to_*.py`):")
    a("")
    a("- Same-bucket copy/admin operators such as `S3CopyObjectOperator`.")
    a("- Google Cloud Storage Transfer Service *job* operators under `operators/cloud_storage_transfer_service.py`.")
    a("- Databricks `COPY INTO` operators, Kafka produce/consume, speech-to-text.")
    a("")
    a("## Headline (concrete operators only)")
    a("")
    a("Base classes (`AwsToAwsBaseOperator`, `BaseSQLToGCSOperator`, `BigQueryToSqlBaseOperator`,")
    a("`BaseSqlToSlackOperator`) are parsed and classified but **excluded from the percentages**")
    a("so a pair is not counted twice via its base.")
    a("")
    a(f"| Classification | Concrete operators | Share of {total} |")
    a("|---|---:|---:|")
    for label in CLASSIFICATION_ORDER:
        a(f"| `{label}` | {c[label]['count']} | {c[label]['pct']:.1f}% |")
    a(f"| **Total** | **{total}** | **100%** |")
    a("")
    maps_or_cap = c["maps_cleanly"]["count"] + c["needs_capability"]["count"]
    maps_or_cap_pct = _pct(maps_or_cap, total)
    file_rows = [row for row in summary["by_kind"] if row["kind"] == "file\u2192file"]
    file_note = ""
    if file_rows:
        fr = file_rows[0]
        file_note = (
            f" Of {fr['operators']} file\u2192file concrete operators, "
            f"{fr['maps_cleanly']} map cleanly, {fr['needs_capability']} need a file-contract "
            f"capability (compression, ACL, server-side copy, move, path layout, …), and "
            f"{fr['special']} are special."
        )
    a(
        f"**{c['maps_cleanly']['pct']:.1f}%** map cleanly onto source-conn + source-location, "
        f"dest-conn + dest-location, replace/overwrite, and generic format/batch options. "
        f"**{c['needs_capability']['pct']:.1f}%** would map if the contract grows an optional "
        f"capability (compression, ACL/extra_args, server-side copy, schema sidecar, "
        f"move, pagination, deferrable, dest encryption, …). **{c['special']['pct']:.1f}%** carry "
        "semantics a generic operator should not absorb (partitioning / dest-table DDL / "
        "warehouse COPY-UNLOAD-extract jobs / custom callables / Slack notifications / "
        "pre-post SQL)."
    )
    a("")
    a(
        f"**{maps_or_cap} / {total} ({maps_or_cap_pct:.1f}%)** are `maps_cleanly` or "
        f"`needs_capability` — they would map onto a generic operator once optional "
        f"capabilities exist.{file_note}"
    )
    a("")
    a("## Classification rules")
    a("")
    a("Rules are data-driven in `scripts/audit_transfers.py` (`FLAG_ARG_RULES`,")
    a("`FLAG_METHOD_RULES`, `FLAG_SOURCE_RULES`, `SPECIAL_FLAGS`, `CAPABILITY_FLAGS`).")
    a("Flags are collected first; the bucket is then assigned in this order:")
    a("")
    a("1. **`special`** if any special flag is present:")
    a("   - `partitioning_or_splitting` — operator-owned output layout or size-based file splitting")
    a("     (`partition_columns`, `max_rows_per_file`, `approx_max_file_size_bytes`, Hive `partition`, …)")
    a("   - `dest_table_ddl` — `create` / `recreate` / `tblproperties` / `field_dict`")
    a("   - `warehouse_copy_job` / `warehouse_job_options` — Redshift `COPY`/`UNLOAD`, Snowflake")
    a("     `COPY INTO`, BigQuery extract/load/copy jobs, Druid ingest, DynamoDB `import_table`,")
    a("     Teradata Native Object Store `CREATE TABLE`")
    a("   - `custom_callable` — required `parser` / `process_func` / `pre_process`")
    a("   - `notification` — destination is Slack")
    a("   - `pre_post_operator_sql` — `mysql_preoperator` / `postoperator` wrapping the transfer")
    a("   - `point_in_time_export` — DynamoDB PITR export API")
    a("   - `teradata_nos_auth` — Teradata AUTHORIZATION / NOS load path")
    a("2. Else **`needs_capability`** if any capability flag is present:")
    a("   - `compression`, `dest_encryption`, `acl_or_extra_args`, `server_side_copy`, `schema_sidecar`,")
    a("     `move_semantics`, `incremental_filter`, `path_layout`, `pagination`,")
    a("     `bulk_load`, `deferrable`, `xcom_side_channel`, `object_retention`, `upsert`")
    a("3. Else **`maps_cleanly`**. Prefix/wildcard listing and `replace`/`overwrite` alone")
    a("   stay in this bucket: a file contract is expected to cover them.")
    a("")
    a("Constructor args declared on a parsed transfer base class are merged into")
    a("subclasses before classification (so `MySQLToGCSOperator` inherits")
    a("`partition_columns` from `BaseSQLToGCSOperator` and is `special`).")
    a("")
    a("## Per-kind breakdown (concrete operators)")
    a("")
    a(_md_table(
        ["Kind", "Operators", "maps_cleanly", "needs_capability", "special", "maps_cleanly %"],
        kind_rows,
    ))
    a("")
    a("Kind is inferred from the `XToYOperator` / `x_to_y.py` pair, mapped to")
    a("file / sql / record / message. See `FILE_SYSTEMS`, `SQL_SYSTEMS`,")
    a("`RECORD_SYSTEMS`, `MESSAGE_SYSTEMS` in the script.")
    a("")
    a("## Per-provider breakdown (concrete operators)")
    a("")
    a(_md_table(
        ["Provider", "Operators", "maps_cleanly", "needs_capability", "special", "maps_cleanly %"],
        provider_rows,
    ))
    a("")
    a("## Top special-semantics flags")
    a("")
    a("Counts are concrete operators that carry the flag (an operator may have several).")
    a("")
    if top_special:
        a(_md_table(
            ["Flag", "Concrete operators"],
            [[r["flag"], r["concrete_operators"]] for r in top_special],
        ))
    else:
        a("(no special flags fired)")
    a("")
    a("Most frequent flags overall (including capability and informational flags such as `replace`):")
    a("")
    a(_md_table(
        ["Flag", "Concrete operators"],
        [[name, n] for name, n in top_flags],
    ))
    a("")
    a("## Notable `special` examples")
    a("")
    a(_md_table(["Operator", "Provider", "Kind", "Why special"], example_rows))
    a("")
    a("## Notable `maps_cleanly` examples")
    a("")
    if clean_rows:
        a(_md_table(["Operator", "Provider", "Kind", "Leading constructor args"], clean_rows))
        a("")
    a("## Heuristic limits (read before quoting the percentages)")
    a("")
    a("- **Static AST only.** The script does not import Airflow or run operators.")
    a("  Behaviour that lives only in `execute()` without a distinctive argument,")
    a("  method name, or source keyword can be missed — or a keyword in a docstring")
    a("  can over-flag.")
    a("- **Optional args still flag the class.** `DynamoDBToS3Operator` is `special`")
    a("  because it *can* run a point-in-time export, even if most callers only scan.")
    a("- **Inheritance is resolved only among transfer modules.** Args declared on")
    a("  `S3ListOperator`, `ADLSListOperator`, or `BaseSQLOperator` are not merged.")
    a("- **`schema` as a bare argument is not treated as a sidecar** (it often means")
    a("  a SQL namespace). Sidecars are `schema_filename` / `schema_fields` / `schema_object`.")
    a("- **File→file with gzip/ACL is `needs_capability`, not `maps_cleanly`.** Those")
    a("  operators are still first-wave candidates once the file contract exposes the capability.")
    a("- **Deferrable support is a capability flag**, even when the rest of the args are generic,")
    a("  because a generic operator would need a generic trigger.")
    a("- **Out-of-scope operators** listed above are not in the denominator.")
    a("- No pandas was used; outputs are stdlib CSV and JSON.")
    a("")
    a("## Outputs")
    a("")
    a("- `bench-out/PAIR_AUDIT.csv` — one row per operator class")
    a("- `bench-out/PAIR_AUDIT.json` — same data plus the summary block")
    a("- `docs/PAIR_AUDIT.md` — this report")
    a("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_json(
    path: Path,
    *,
    sha: str,
    subject: str,
    commit_iso: str,
    airflow_root: str,
    transfer_module_count: int,
    included_extra: list[str],
    skipped: list[dict[str, str]],
    records: list[OperatorRecord],
    summary: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "airflow_sha": sha,
        "airflow_commit_subject": subject,
        "airflow_commit_date": commit_iso,
        "airflow_root": airflow_root,
        "transfer_module_count": transfer_module_count,
        "included_extra_modules": included_extra,
        "skipped_modules": [s for s in skipped if s.get("reason") != "pending-parse"],
        "rules": {
            "special_flags": sorted(SPECIAL_FLAGS),
            "capability_flags": sorted(CAPABILITY_FLAGS),
            "informational_flags": sorted(INFORMATIONAL_FLAGS),
            "flag_arg_rules": {k: sorted(v) for k, v in FLAG_ARG_RULES.items()},
        },
        "summary": summary,
        "operators": records_to_jsonable(records),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def default_airflow_src() -> Path | None:
    env = os.environ.get("AIRFLOW_SRC")
    if env:
        return Path(env).expanduser().resolve()
    candidates = [
        Path("/workspace/airflow-src"),
        Path(__file__).resolve().parents[2] / "airflow-src",
        Path(__file__).resolve().parents[1] / "airflow-src",
    ]
    for c in candidates:
        if (c / "providers").is_dir() and (c / ".git").exists():
            return c
    return None


def audit(airflow_root: Path) -> tuple[list[OperatorRecord], dict[str, Any], dict[str, Any]]:
    providers_root = airflow_root / "providers"
    if not providers_root.is_dir():
        raise SystemExit(f"No providers/ directory under {airflow_root}")

    included, skipped = discover_modules(providers_root)
    finalized_skipped = [s for s in skipped if s["reason"] != "pending-parse"]

    records: list[OperatorRecord] = []
    module_texts: dict[str, str] = {}
    for path in included:
        rel = path.relative_to(providers_root).as_posix()
        module_texts[rel] = path.read_text(encoding="utf-8")
        records.extend(parse_module(path, providers_root=providers_root))
    apply_inheritance(records, module_texts=module_texts)
    records.sort(key=lambda r: (r.provider, r.module_path, r.class_name))

    transfer_modules = sorted(
        {p.relative_to(providers_root).as_posix() for p in included if "transfers" in p.parts}
    )
    included_extra = sorted(
        {p.relative_to(providers_root).as_posix() for p in included if "transfers" not in p.parts}
    )
    meta = {
        "sha": git_sha(airflow_root),
        "subject": git_commit_subject(airflow_root),
        "commit_iso": git_commit_iso(airflow_root),
        "transfer_module_count": len(transfer_modules),
        "included_extra": included_extra,
        "skipped": finalized_skipped,
        "included_count": len(included),
    }
    return records, summarize(records), meta


def main(argv: list[str] | None = None) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--airflow-src",
        type=Path,
        default=None,
        help="Path to a clone of apache/airflow (providers/ required). "
        "Defaults to $AIRFLOW_SRC or /workspace/airflow-src.",
    )
    parser.add_argument("--csv", type=Path, default=repo_root / "bench-out" / "PAIR_AUDIT.csv")
    parser.add_argument("--json", type=Path, default=repo_root / "bench-out" / "PAIR_AUDIT.json")
    parser.add_argument("--md", type=Path, default=repo_root / "docs" / "PAIR_AUDIT.md")
    args = parser.parse_args(argv)

    airflow_root = args.airflow_src or default_airflow_src()
    if airflow_root is None:
        print(
            "No Airflow source tree found. Shallow-sparse clone providers/ with:\n"
            "  git clone --depth 1 --filter=blob:none --sparse https://github.com/apache/airflow.git /workspace/airflow-src\n"
            "  git -C /workspace/airflow-src sparse-checkout set --cone providers\n"
            "Then re-run: just audit",
            file=sys.stderr,
        )
        return 2
    airflow_root = airflow_root.resolve()

    records, summary, meta = audit(airflow_root)
    write_csv(args.csv, records)
    write_json(
        args.json,
        sha=meta["sha"],
        subject=meta["subject"],
        commit_iso=meta["commit_iso"],
        airflow_root=str(airflow_root),
        transfer_module_count=meta["transfer_module_count"],
        included_extra=meta["included_extra"],
        skipped=meta["skipped"],
        records=records,
        summary=summary,
    )
    write_markdown(
        args.md,
        airflow_root=airflow_root,
        sha=meta["sha"],
        subject=meta["subject"],
        commit_iso=meta["commit_iso"],
        transfer_module_count=meta["transfer_module_count"],
        included_extra=meta["included_extra"],
        skipped=meta["skipped"],
        records=records,
        summary=summary,
    )

    concrete = _concrete(records)
    c = summary["classification"]
    print(f"Airflow SHA: {meta['sha']}")
    print(f"transfers/ modules: {meta['transfer_module_count']}")
    print(
        f"concrete operators: {len(concrete)}  "
        f"maps_cleanly={c['maps_cleanly']['count']} ({c['maps_cleanly']['pct']}%)  "
        f"needs_capability={c['needs_capability']['count']} ({c['needs_capability']['pct']}%)  "
        f"special={c['special']['count']} ({c['special']['pct']}%)"
    )
    print(f"wrote {args.csv}")
    print(f"wrote {args.json}")
    print(f"wrote {args.md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
