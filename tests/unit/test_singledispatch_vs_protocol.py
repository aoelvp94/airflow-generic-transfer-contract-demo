"""Observations from examples/singledispatch_vs_protocol.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "singledispatch_vs_protocol.py"


def _load():
    spec = importlib.util.spec_from_file_location("singledispatch_vs_protocol", EXAMPLE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # The example inserts src/ on import. A second import in this process must
    # not run it twice and register the singledispatch functions again.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


example = _load()


def test_protocol_registration_matches_an_unregistered_store() -> None:
    assert example.by_protocol(example.OnlyCopy()) == "file"
    assert example.by_protocol(example.GCSHook()) == "file"
    assert example.by_protocol(object()) == "default"


def test_two_protocols_on_one_hook_are_ambiguous() -> None:
    hook = example.S3Hook()
    with pytest.raises(RuntimeError, match="Ambiguous dispatch"):
        example.by_either_protocol(hook)
    # A store that implements only one of the two registrations still resolves.
    assert example.by_either_protocol(example.GCSHook()) == "file"
    assert example.by_either_protocol(example.OnlyCopy()) == "file"


def test_isinstance_checks_are_independent_and_the_operators_accept_both_roles() -> None:
    hook = example.S3Hook()
    assert example.protocols_of(hook) == {
        "file": True,
        "batch_read": True,
        "batch_write": True,
    }
    assert example.capability(hook, hook) == "file-and-record"
    assert example.capability(hook, example.GCSHook()) == "file"
    assert example.run_real_operators(hook) == ("s3", "s3-write")


def test_source_dispatch_ignores_the_destination_pair() -> None:
    same, other = example.pair_rows()
    assert same["marker_policy"] == "copy"
    assert same["path_layout"] == "preserve"
    assert other["marker_policy"] == "skip_overlapping"
    assert other["path_layout"] == "join_source_key"
    assert same != other
    assert example.dispatch_on_source(example.S3Hook()) == ("s3-source", "s3-source")
    assert example.dispatch_on_method(example.S3Hook()) == (
        "method-s3-source",
        "method-s3-source",
    )


def test_report_runs() -> None:
    text = example.report()
    assert "Ambiguous dispatch" in text
    assert "s3-source" in text
    assert "skip_overlapping" in text
