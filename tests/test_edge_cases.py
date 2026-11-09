"""Edge-case matrix for pagination, markers, keys, Iceberg, and resume."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests
import requests_mock

from generic_transfer import GenericFileTransfer, GenericRecordTransfer
from generic_transfer.adapters.file_sink import DictObjectStore, ObjectStoreCopySink, is_folder_marker, list_transfer_keys
from generic_transfer.adapters.http_source import PaginatedHttpSourceHook
from generic_transfer.adapters.iceberg_sink import IcebergAppendSink
from generic_transfer.pagination import cursor_pagination, offset_pagination, page_number_pagination
from fake_api.server import start


def test_empty_prefix_lists_nothing():
    store = DictObjectStore()
    assert list_transfer_keys(store, "missing") == []


def test_nested_markers_and_adlfs_style_without_trailing_slash():
    store = DictObjectStore()
    store.put("data/", b"")
    store.put("data/a.json", b"a")
    store.put("data/nested", b"")  # adlfs-style: marker without slash
    store.put("data/nested/b.json", b"b")
    # zero-byte "data/nested" with children under data/nested/ → marker
    assert is_folder_marker(store, "data/")
    assert is_folder_marker(store, "data/nested")
    keys = list_transfer_keys(store, "data")
    assert keys == ["data/a.json", "data/nested/b.json"]


def test_zero_byte_real_file_is_not_marker():
    store = DictObjectStore()
    store.put("data/empty.txt", b"")
    assert not is_folder_marker(store, "data/empty.txt")
    assert list_transfer_keys(store, "data") == ["data/empty.txt"]


def test_special_character_and_unicode_keys():
    store = DictObjectStore()
    store.put("data/hello world.json", b"1")
    store.put("data/café.json", "ñ".encode())
    store.put("data/path with spaces/x.json", b"2")
    dest = DictObjectStore()
    result = ObjectStoreCopySink(dest).copy_files(source_fs=store, source_prefix="data")
    assert result["count"] == 3
    assert dest.exists("hello world.json")
    assert dest.exists("café.json")
    assert dest.exists("path with spaces/x.json")


def test_overwrite_idempotent_rerun():
    store = DictObjectStore()
    store.put("data/a.json", b"v1")
    dest = DictObjectStore()
    sink = ObjectStoreCopySink(dest)
    sink.copy_files(source_fs=store, source_prefix="data")
    store.put("data/a.json", b"v2")
    sink.copy_files(source_fs=store, source_prefix="data")
    assert dest.get("a.json") == b"v2"


def test_partial_failure_resume_skips_already_copied():
    store = DictObjectStore()
    store.put("data/a.json", b"a")
    store.put("data/b.json", b"b")
    dest = DictObjectStore()
    dest.put("a.json", b"a")  # already done
    result = ObjectStoreCopySink(dest).copy_files(
        source_fs=store,
        source_prefix="data",
        resume_from=["a.json"],
    )
    assert result["skipped"] == ["a.json"]
    assert result["copied"] == ["b.json"]
    assert dest.get("b.json") == b"b"


def test_pagination_empty_last_page_and_single_page():
    with requests_mock.Mocker() as m:
        m.get("http://api.test/items", [{"json": {"data": [{"id": 1}], "has_more": False}}])
        hook = PaginatedHttpSourceHook(
            "http://api.test",
            pagination=offset_pagination(records_key="data"),
            records_key="data",
        )
        rows = [r for batch in hook.read_batches(path="/items") for r in batch]
        assert rows == [{"id": 1}]
        assert hook.request_count == 1


def test_pagination_max_pages_cap():
    with requests_mock.Mocker() as m:
        m.get(
            "http://api.test/items",
            [
                {"json": {"data": [{"id": 1}], "has_more": True}},
                {"json": {"data": [{"id": 2}], "has_more": True}},
                {"json": {"data": [{"id": 3}], "has_more": True}},
            ],
        )
        hook = PaginatedHttpSourceHook(
            "http://api.test",
            pagination=offset_pagination(records_key="data"),
            records_key="data",
            max_pages=2,
        )
        rows = [r for batch in hook.read_batches(path="/items", params={"limit": 1}) for r in batch]
        assert len(rows) == 2
        assert hook.request_count == 2


def test_pagination_cursor_loop_guard():
    with requests_mock.Mocker() as m:
        # Always returns the same next cursor → would loop without the guard.
        m.get(
            "http://api.test/events",
            json={"events": [{"id": "1"}, {"id": "2"}]},
        )
        hook = PaginatedHttpSourceHook(
            "http://api.test",
            pagination=cursor_pagination(records_key="events", id_key="id", inclusive=True),
            records_key="events",
            max_pages=20,
        )
        rows = [r for batch in hook.read_batches(path="/events", params={"limit": 2}) for r in batch]
        assert hook.request_count < 20
        assert rows


def test_http_429_then_success_retries():
    with requests_mock.Mocker() as m:
        m.get(
            "http://api.test/orders",
            [
                {"status_code": 429, "json": {"error": "slow down"}},
                {"json": {"data": [{"id": 1}], "has_more": False}},
            ],
        )
        hook = PaginatedHttpSourceHook(
            "http://api.test",
            pagination=offset_pagination(records_key="data"),
            records_key="data",
            max_retries=3,
        )
        rows = [r for batch in hook.read_batches(path="/orders") for r in batch]
        assert rows == [{"id": 1}]
        assert hook.request_count == 2


def test_http_5xx_exhausts_retries():
    with requests_mock.Mocker() as m:
        m.get("http://api.test/orders", [{"status_code": 503, "json": {"error": "down"}}] * 3)
        hook = PaginatedHttpSourceHook(
            "http://api.test",
            records_key="data",
            max_retries=3,
        )
        with pytest.raises(requests.HTTPError):
            list(hook.read_batches(path="/orders"))


@pytest.mark.iceberg
def test_iceberg_schema_drift_drops_extra_columns(tmp_path: Path):
    sink = IcebergAppendSink(tmp_path / "wh")
    # Create table with id, name
    sink.write_batches(iter([[{"id": 1, "name": "a"}]]), namespace="raw", table="t")
    # Drift: extra column "extra" dropped; missing "name" -> null
    result = sink.write_batches(
        iter([[{"id": 2, "extra": "x"}, {"id": 3, "name": "c"}]]),
        namespace="raw",
        table="t",
    )
    assert result["records"] == 2


@pytest.mark.iceberg
def test_iceberg_large_batch(tmp_path: Path):
    sink = IcebergAppendSink(tmp_path / "wh")
    batch = [{"id": i, "v": i * 2} for i in range(5000)]
    result = sink.write_batches(iter([batch]), namespace="raw", table="big")
    assert result["records"] == 5000


def test_live_fake_api_page_number_and_next_link():
    server, base = start()
    try:
        from generic_transfer.pagination import next_link_pagination, page_number_pagination

        customers = PaginatedHttpSourceHook(
            base,
            pagination=page_number_pagination(records_key="results", total_pages_key="total_pages"),
            records_key="results",
        )
        rows = [r for b in customers.read_batches(path="/customers", params={"page_size": 5}) for r in b]
        assert len(rows) == 11

        feeds = PaginatedHttpSourceHook(
            base,
            pagination=next_link_pagination(path="page.next"),
            records_key="items",
        )
        feed_rows = [r for b in feeds.read_batches(path="/feeds", params={"limit": 3}) for r in b]
        assert len(feed_rows) == 7
    finally:
        server.shutdown()
