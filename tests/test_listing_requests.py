"""Regression tests for the 06 listing fix (no per-key info()/HEAD).

Unit tests use a fake filesystem that counts calls. The integration test counts
real HTTP requests on the wire against the emulators for every provider.
"""
from __future__ import annotations

from typing import Any

import pytest

from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook, copy_files_sequential
from generic_transfer.adapters.file_sink import DictObjectStore, list_transfer_entries, list_transfer_keys


class CountingFS:
    """DictObjectStore wrapper that counts info/open/cat_file/find calls."""

    def __init__(self, store: DictObjectStore) -> None:
        self.store = store
        self.calls: dict[str, int] = {}

    def _bump(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1

    def find(self, prefix: str = "", detail: bool = False) -> Any:
        self._bump("find")
        return self.store.find(prefix, detail=detail)

    def info(self, key: str) -> dict:
        self._bump("info")
        return self.store.info(key)

    def open(self, key: str, mode: str = "rb"):
        self._bump("open")
        return self.store.open(key, mode)

    def cat_file(self, key: str) -> bytes:
        self._bump("cat_file")
        return self.store.get(key)

    def ls(self, path: str, detail: bool = False):
        self._bump("ls")
        return self.store.ls(path, detail=detail)


def _tree() -> DictObjectStore:
    s = DictObjectStore()
    s.put("data/", b"")                 # s3/gcs-style marker with slash
    s.put("data/a.json", b"a")
    s.put("data/nested", b"")           # adlfs-style marker without slash
    s.put("data/nested/b.json", b"bb")
    s.put("data/empty.txt", b"")        # real 0-byte file: must be kept
    s.put("data/nested2/", b"")         # marker with no children: still a marker
    s.put("data/nested-sibling.json", b"c")  # shares the 'data/nested' string prefix
    return s


def test_single_listing_no_info_calls():
    fs = CountingFS(_tree())
    entries = list_transfer_entries(fs, "data")
    assert [e["name"] for e in entries] == [
        "data/a.json",
        "data/empty.txt",
        "data/nested-sibling.json",
        "data/nested/b.json",
    ]
    assert {e["name"]: e["size"] for e in entries}["data/nested/b.json"] == 2
    assert fs.calls == {"find": 1}


def test_list_transfer_keys_matches_entries():
    assert list_transfer_keys(_tree(), "data") == [
        "data/a.json",
        "data/empty.txt",
        "data/nested-sibling.json",
        "data/nested/b.json",
    ]


def test_copy_small_objects_use_cat_file_not_open():
    fs = CountingFS(_tree())
    written: dict[str, bytes] = {}
    res = copy_files_sequential(
        source_fs=fs, source_prefix="data", put=written.__setitem__, dest_prefix="out"
    )
    assert res["count"] == 4
    assert written["out/nested/b.json"] == b"bb"
    assert written["out/empty.txt"] == b""
    assert fs.calls.get("info", 0) == 0
    assert fs.calls.get("open", 0) == 0
    # 0-byte real file is known from the listing: no request at all.
    assert fs.calls["cat_file"] == 3


def test_large_object_streams_through_open(monkeypatch):
    import generic_transfer.adapters.file_copy as fc

    monkeypatch.setattr(fc, "SMALL_OBJECT_MAX_BYTES", 1)
    fs = CountingFS(_tree())
    written: dict[str, bytes] = {}
    copy_files_sequential(source_fs=fs, source_prefix="data", put=written.__setitem__,
                          chunk_size=1)
    # 'bb' (2 bytes) is above the patched threshold -> open(); others cat_file.
    assert fs.calls["open"] == 1
    assert written["nested/b.json"] == b"bb"


def test_per_key_listing_still_available_for_comparison():
    fs = CountingFS(_tree())
    hook = ObjectStoreFileCopyHook(put=lambda k, d: None)
    res = hook.copy_files(source_fs=fs, source_prefix="data", listing="per_key",
                          mode="sequential")
    # The per-key rule keeps the childless "data/nested2/" marker (5 objects);
    # the single-listing rule treats any zero-byte trailing-slash key as a
    # marker (4 objects). Documented behaviour change in docs/AZURE_SLOWDOWN.md.
    assert res["count"] == 5
    assert fs.calls["info"] >= 7  # one per listed key


def test_listing_param_is_in_schema():
    schema = ObjectStoreFileCopyHook(put=lambda k, d: None).describe_params()
    assert schema["properties"]["listing"]["enum"] == ["single", "per_key"]


@pytest.mark.integration
@pytest.mark.parametrize("flavour", ["s3", "gcs", "azure"])
def test_wire_requests_generic_vs_naive(require_emulators, flavour):
    """On every provider, generic sequential makes no source HEADs and no more
    HTTP requests than the naive pair copy (which also copies markers)."""
    from generic_transfer import wire
    from generic_transfer.adapters.emulators import (
        clear_prefix, ensure_bucket, make_fs, seed_many_small,
    )
    from generic_transfer.adapters.pair_baseline import naive_pair_copy

    src_bucket = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}[flavour]
    src = make_fs(flavour, bucket=src_bucket)
    ensure_bucket(src)
    seed_many_small(src, "wire-test", n=20, size=64, marker_every=10)
    written: dict[str, bytes] = {}

    wire.install()
    wire.reset()
    naive_pair_copy(src, src, source_prefix="wire-test", dest_prefix="wire-naive")
    naive_http = wire.snapshot()
    clear_prefix(src, "wire-naive")

    wire.reset()
    res = copy_files_sequential(
        source_fs=src.fs, source_bucket=src.bucket, source_prefix="wire-test",
        put=written.__setitem__, dest_prefix="out",
    )
    gen_http = wire.snapshot()
    clear_prefix(src, "wire-test")

    assert res["count"] == 20
    # No per-object HEADs. adlfs find() may HEAD the prefix itself once.
    assert gen_http.get(f"{flavour}:head", 0) <= 1, gen_http
    assert gen_http.get(f"{flavour}:get", 0) == 20, gen_http
    # naive: list + 23 GET + 23 PUT; generic: list + 20 GET (PUTs to a dict)
    naive_reads = sum(v for k, v in naive_http.items() if not k.endswith(":put"))
    assert sum(gen_http.values()) <= naive_reads, (gen_http, naive_http)
