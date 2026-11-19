"""Wire-level listing check against S3, GCS, and Azure emulators."""
from __future__ import annotations

import pytest

from generic_transfer.adapters.file_copy import copy_files_sequential

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("flavour", ["s3", "gcs", "azure"])
def test_wire_requests_generic_vs_naive(require_emulators, flavour):
    """On every provider, generic sequential makes no source HEADs and no more
    HTTP requests than the naive pair copy (which also copies markers)."""
    from generic_transfer import wire
    from generic_transfer.adapters.emulators import (
        clear_prefix,
        ensure_bucket,
        make_fs,
        seed_many_small,
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
        source_fs=src.fs,
        source_bucket=src.bucket,
        source_prefix="wire-test",
        put=written.__setitem__,
        dest_prefix="out",
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
