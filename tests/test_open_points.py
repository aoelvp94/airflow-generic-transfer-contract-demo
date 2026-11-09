"""Open points from the thread: lineage, discoverability, file contract, deferrable."""
from __future__ import annotations

import asyncio

import pytest

from generic_transfer import (
    FileCopyable,
    GenericAsyncRecordTransfer,
    GenericFileTransfer,
    GenericRecordTransfer,
    ParamValidationError,
)
from generic_transfer.adapters.file_sink import DictObjectStore, ObjectStoreCopySink
from generic_transfer.adapters.http_source import PaginatedHttpSourceHook
from generic_transfer.pagination import offset_pagination
from fake_api.server import start


def test_lineage_collected_from_hooks():
    server, base = start()
    try:
        source = PaginatedHttpSourceHook(
            base,
            pagination=offset_pagination(records_key="data"),
            records_key="data",
            lineage_dataset="http://acme.example/orders",
        )

        class Dest:
            def write_batches(self, batches, **params):
                return {"n": sum(len(b) for b in batches)}

            def get_lineage_datasets(self, **params):
                return [{"uri": f"file:///{params.get('key', 'out')}", "role": "outlet"}]

            def describe_params(self, role="destination"):
                return {
                    "type": "object",
                    "properties": {"key": {"type": "string"}},
                    "additionalProperties": False,
                }

        op = GenericRecordTransfer(
            source_hook=source,
            destination_hook=Dest(),
            source_hook_params={"path": "/orders", "params": {"limit": 10}},
            destination_hook_params={"key": "raw/orders.json"},
        )
        result = op.execute()
        assert result["n"] == 25
        assert op.inlets[0]["uri"] == "http://acme.example/orders"
        assert op.outlets[0]["uri"] == "file:///raw/orders.json"
    finally:
        server.shutdown()


def test_discoverability_rejects_unknown_and_bad_types():
    class Src:
        def read_batches(self, **params):
            yield [{"id": 1}]

        def describe_params(self, role="source"):
            return {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": False,
            }

    class Dest:
        def write_batches(self, batches, **params):
            return list(batches)

        def describe_params(self, role="destination"):
            return {
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "additionalProperties": False,
            }

    with pytest.raises(ParamValidationError, match="missing required"):
        GenericRecordTransfer(
            source_hook=Src(),
            destination_hook=Dest(),
            source_hook_params={},
        ).execute()

    with pytest.raises(ParamValidationError, match="unknown params"):
        GenericRecordTransfer(
            source_hook=Src(),
            destination_hook=Dest(),
            source_hook_params={"path": "/x", "nope": 1},
        ).execute()

    with pytest.raises(ParamValidationError, match="type errors"):
        GenericRecordTransfer(
            source_hook=Src(),
            destination_hook=Dest(),
            source_hook_params={"path": 123},
        ).execute()


def test_file_copyable_separate_from_write_batches():
    store = DictObjectStore()
    store.put("data/a.json", b"1")
    store.put("data/", b"")
    store.put("data/nested/", b"")
    store.put("data/nested/b.json", b"2")
    dest = DictObjectStore()
    sink = ObjectStoreCopySink(dest)
    assert isinstance(sink, FileCopyable)
    op = GenericFileTransfer(
        destination_hook=sink,
        copy_params={"source_fs": store, "source_prefix": "data"},
    )
    result = op.execute()
    assert result["count"] == 2
    assert sorted(dest.find("")) == ["a.json", "nested/b.json"]
    assert op.outlets


def test_async_deferrable_sketch():
    class AsyncSrc:
        async def aread_batches(self, **params):
            yield [{"id": 1}, {"id": 2}]
            yield [{"id": 3}]

    class AsyncDest:
        async def awrite_batches(self, batches, **params):
            rows = []
            async for batch in batches:
                rows.extend(batch)
            return {"records": len(rows), "rows": rows}

    async def run():
        return await GenericAsyncRecordTransfer(
            source_hook=AsyncSrc(),
            destination_hook=AsyncDest(),
        ).execute()

    result = asyncio.run(run())
    assert result["records"] == 3
    assert [r["id"] for r in result["rows"]] == [1, 2, 3]
