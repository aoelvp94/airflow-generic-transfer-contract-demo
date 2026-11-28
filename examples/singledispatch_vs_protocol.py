"""``functools.singledispatch`` next to the repository's Protocol checks.

Run from the repository root::

    python examples/singledispatch_vs_protocol.py

The functions are what ``tests/unit/test_singledispatch_vs_protocol.py`` asserts.
"""

from __future__ import annotations

import importlib.util
import sys
from functools import singledispatch, singledispatchmethod
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from generic_transfer.contract import BatchReadable, BatchWritable, FileCopyable  # noqa: E402
from generic_transfer.operator import GenericFileTransfer, GenericRecordTransfer  # noqa: E402


class S3Hook:
    """Implements the file protocol and both record protocols."""

    def copy_files(self, **params: object) -> str:
        return "s3"

    def read_batches(self, **params: object):
        yield {"system": "s3"}

    def write_batches(self, batches: object, **params: object) -> str:
        del batches, params
        return "s3-write"


class GCSHook:
    """Implements only the file protocol. Not registered with singledispatch by name."""

    def copy_files(self, **params: object) -> str:
        return "gcs"


class OnlyCopy:
    """A third store. Nothing registers this class."""

    def copy_files(self, **params: object) -> str:
        return "new"


@singledispatch
def by_protocol(hook: object) -> str:
    return "default"


@by_protocol.register
def _by_file_protocol(hook: FileCopyable) -> str:
    del hook
    return "file"


@singledispatch
def by_either_protocol(hook: object) -> str:
    return "default"


@by_either_protocol.register
def _either_file(hook: FileCopyable) -> str:
    del hook
    return "file"


@by_either_protocol.register
def _either_batch(hook: BatchReadable) -> str:
    del hook
    return "batch"


@singledispatch
def by_source(src: object, dst: object) -> str:
    del dst
    return f"unregistered:{type(src).__name__}"


@by_source.register(S3Hook)
def _by_s3_source(src: S3Hook, dst: object) -> str:
    del src, dst
    return "s3-source"


class Chooser:
    """``singledispatchmethod`` still keys off the first argument after ``self``."""

    @singledispatchmethod
    def strategy(self, src: object, dst: object) -> str:
        del dst
        return f"method-default:{type(src).__name__}"

    @strategy.register(S3Hook)
    def _s3(self, src: S3Hook, dst: object) -> str:
        del src, dst
        return "method-s3-source"


def load_pair_policy() -> dict[tuple[str, str], dict[str, str]]:
    """Load ``PAIR_POLICY`` without importing the shim package."""
    path = ROOT / "shims" / "generic_all_pairs" / "pair_policy.py"
    spec = importlib.util.spec_from_file_location("aip132_pair_policy", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.PAIR_POLICY


def protocols_of(hook: object) -> dict[str, bool]:
    """Independent ``isinstance`` checks. Several may be true at once."""
    return {
        "file": isinstance(hook, FileCopyable),
        "batch_read": isinstance(hook, BatchReadable),
        "batch_write": isinstance(hook, BatchWritable),
    }


def capability(source: object, dest: object) -> str:
    """Which transfers the two hooks can do, without picking one winner."""
    file_ok = isinstance(dest, FileCopyable)
    record_ok = isinstance(source, BatchReadable) and isinstance(dest, BatchWritable)
    if file_ok and record_ok:
        return "file-and-record"
    if file_ok:
        return "file"
    if record_ok:
        return "record"
    return "neither"


def run_real_operators(hook: S3Hook) -> tuple[str, str]:
    """The same object can be a file destination or a record endpoint.

    ``GenericFileTransfer`` checks ``FileCopyable``. ``GenericRecordTransfer``
    checks ``BatchReadable`` and ``BatchWritable``. Neither call goes through
    ``singledispatch``.
    """
    copied = GenericFileTransfer(destination_hook=hook, copy_params={}, collect_lineage=False).execute()
    written = GenericRecordTransfer(
        source_hook=hook,
        destination_hook=hook,
        collect_lineage=False,
    ).execute()
    return copied, written


def dispatch_on_source(src: S3Hook) -> tuple[str, str]:
    """One registration serves every destination type."""
    return by_source(src, GCSHook()), by_source(src, S3Hook())


def dispatch_on_method(src: S3Hook) -> tuple[str, str]:
    chooser = Chooser()
    return chooser.strategy(src, GCSHook()), chooser.strategy(src, S3Hook())


def pair_rows() -> tuple[dict[str, str], dict[str, str]]:
    policy = load_pair_policy()
    return policy[("s3", "s3")], policy[("s3", "gcs")]


def report() -> str:
    s3s3, s3gcs = pair_rows()
    source_gcs, source_s3 = dispatch_on_source(S3Hook())
    method_gcs, method_s3 = dispatch_on_method(S3Hook())
    try:
        both = by_either_protocol(S3Hook())
    except RuntimeError as exc:
        both = f"{type(exc).__name__}: {exc}"
    lines = [
        f"protocol OnlyCopy={by_protocol(OnlyCopy())} GCSHook={by_protocol(GCSHook())}",
        f"two protocols on S3Hook={both}",
        f"isinstance S3Hook={protocols_of(S3Hook())}",
        f"capability S3->S3={capability(S3Hook(), S3Hook())} S3->GCS={capability(S3Hook(), GCSHook())}",
        f"operators {run_real_operators(S3Hook())}",
        f"singledispatch source S3->GCS={source_gcs} S3->S3={source_s3}",
        f"singledispatchmethod S3->GCS={method_gcs} S3->S3={method_s3}",
        (
            "PAIR_POLICY s3->s3 "
            f"marker={s3s3['marker_policy']} layout={s3s3['path_layout']}; "
            "s3->gcs "
            f"marker={s3gcs['marker_policy']} layout={s3gcs['path_layout']}"
        ),
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    print(report())
