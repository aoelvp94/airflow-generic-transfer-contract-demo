"""Chunked uploads on the emulators: S3 multipart, GCS resumable, Azure blocks.

``open_upload`` is what ``ObjectStoreFileCopyHook`` writes through when it has
a ``dest_emu``. These tests check the bytes land intact, that an aborted
upload leaves no object, and that the hook streams a multi-MiB object across
providers.
"""
from __future__ import annotations

import hashlib

import pytest

from generic_transfer import GenericFileTransfer
from generic_transfer.adapters.emulators import (
    EmulatorFS,
    clear_prefix,
    ensure_bucket,
    get_bytes,
    list_keys_native,
    make_fs,
    open_upload,
    seed_one_large,
)
from generic_transfer.adapters.file_copy import ObjectStoreFileCopyHook

pytestmark = pytest.mark.integration

FLAVOURS = ("s3", "gcs", "azure")
SRC_BUCKET = {"s3": "src-bucket", "gcs": "src-bucket", "azure": "src-container"}
DST_BUCKET = {"s3": "dst-bucket", "gcs": "dst-bucket", "azure": "dst-container"}
MIB = 1024 * 1024


@pytest.fixture(scope="module", autouse=True)
def _emulators(require_emulators):
    for flavour in FLAVOURS:
        ensure_bucket(make_fs(flavour, bucket=SRC_BUCKET[flavour]))
        ensure_bucket(make_fs(flavour, bucket=DST_BUCKET[flavour]))


def _dst(flavour: str) -> EmulatorFS:
    return make_fs(flavour, bucket=DST_BUCKET[flavour])  # type: ignore[arg-type]


def _src(flavour: str) -> EmulatorFS:
    return make_fs(flavour, bucket=SRC_BUCKET[flavour])  # type: ignore[arg-type]


def _payload(size: int) -> bytes:
    block = bytes(range(256)) * 4096  # 1 MiB, not a repeated single byte
    return (block * (size // len(block) + 1))[:size]


@pytest.mark.parametrize("flavour", FLAVOURS)
def test_open_upload_streams_past_the_part_threshold(flavour):
    """12 MiB in 1 MiB writes: S3 sends 5+5+2 MiB parts, GCS 1 MiB resumable
    chunks, Azure one staged block per flush. The object reads back intact."""
    dst = _dst(flavour)
    clear_prefix(dst, "stream")
    data = _payload(12 * MIB)
    with open_upload(dst, "stream/big.bin", chunk_size=MIB) as writer:
        for offset in range(0, len(data), MIB):
            writer.write(data[offset : offset + MIB])
        assert writer._stream is not None  # promoted to a chunked upload
    assert get_bytes(dst, "stream/big.bin") == data


@pytest.mark.parametrize("flavour", FLAVOURS)
def test_open_upload_small_object_is_one_put(flavour):
    dst = _dst(flavour)
    clear_prefix(dst, "stream-small")
    with open_upload(dst, "stream-small/a.json", chunk_size=MIB) as writer:
        writer.write(b'{"id":')
        writer.write(b"1}")
        assert writer._stream is None  # never left the buffer
    assert get_bytes(dst, "stream-small/a.json") == b'{"id":1}'


@pytest.mark.parametrize("flavour", FLAVOURS)
def test_open_upload_abort_leaves_no_object(flavour):
    """An error mid-copy cancels the upload instead of committing a prefix."""
    dst = _dst(flavour)
    clear_prefix(dst, "stream-abort")
    data = _payload(7 * MIB)

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        with open_upload(dst, "stream-abort/partial.bin", chunk_size=MIB) as writer:
            for offset in range(0, len(data), MIB):
                writer.write(data[offset : offset + MIB])
            assert writer._stream is not None
            raise Boom()
    assert list_keys_native(dst, "stream-abort") == []


@pytest.mark.parametrize(
    ("src_flavour", "dst_flavour"),
    [("s3", "gcs"), ("gcs", "azure"), ("azure", "s3")],
)
def test_hook_streams_large_object_across_providers(src_flavour, dst_flavour):
    """The hook copies an 8 MiB object chunk by chunk; bytes match end to end."""
    src = _src(src_flavour)
    dst = _dst(dst_flavour)
    key = seed_one_large(src, "large-src", size_mb=8)
    clear_prefix(dst, "large-out")
    expected = hashlib.sha256(get_bytes(src, key)).hexdigest()
    result = GenericFileTransfer(
        destination_hook=ObjectStoreFileCopyHook(dst),
        copy_params={
            "source_fs": src.fs,
            "source_bucket": src.bucket,
            "source_prefix": "large-src",
            "dest_prefix": "large-out",
            "chunk_size": MIB,
        },
    ).execute()
    assert result["count"] == 1
    assert list_keys_native(dst, "large-out") == ["large-out/large.bin"]
    assert hashlib.sha256(get_bytes(dst, "large-out/large.bin")).hexdigest() == expected
