"""Parity: generic operator vs a per-pair HttpToObject baseline."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests_mock as rm

from generic_transfer import GenericRecordTransfer
from generic_transfer.adapters import FileBatchSink, PaginatedHttpSourceHook
from generic_transfer.pagination import offset_pagination
from tests.baseline_httptos3 import HttpToObjectBaseline


def _orders_mocks(m: rm.Mocker) -> None:
    m.get(
        "http://api/orders?limit=2",
        json={"data": [{"id": 1}, {"id": 2}], "has_more": True},
    )
    m.get(
        "http://api/orders?limit=2&offset=2",
        json={"data": [{"id": 3}], "has_more": False},
    )


def test_stock_baseline_silently_keeps_page_one(tmp_path: Path):
    """Stock HttpToS3 shape: max_pages=1, no records_key → page 1 body only."""
    store: dict[str, bytes] = {}
    with rm.Mocker() as m:
        _orders_mocks(m)
        baseline = HttpToObjectBaseline(
            endpoint="http://api/orders",
            store_put=lambda k, b: store.__setitem__(k, b),
            key="raw/orders.json",
            params={"limit": 2},
            max_pages=1,
        )
        res = baseline.execute()
    assert res["pages"] == 1
    assert baseline.request_count == 1
    body = json.loads(store["raw/orders.json"])
    # Raw single-response body, not a flat record list.
    assert body["data"] == [{"id": 1}, {"id": 2}]
    assert body["has_more"] is True


def test_generic_matches_paginated_pair_baseline(tmp_path: Path):
    """When the pair operator is taught to paginate, both paths agree."""
    pager = offset_pagination(records_key="data")
    store: dict[str, bytes] = {}

    with rm.Mocker() as m:
        _orders_mocks(m)
        baseline = HttpToObjectBaseline(
            endpoint="http://api/orders",
            store_put=lambda k, b: store.__setitem__(k, b),
            key="raw/orders.json",
            params={"limit": 2},
            pagination=pager,
            records_key="data",
            max_pages=10,
        )
        base_res = baseline.execute()

    with rm.Mocker() as m:
        _orders_mocks(m)
        source = PaginatedHttpSourceHook(
            "http://api",
            pagination=pager,
            records_key="data",
            max_pages=10,
        )
        sink = FileBatchSink(str(tmp_path))
        generic_res = GenericRecordTransfer(
            source_hook=source,
            destination_hook=sink,
            source_hook_params={"path": "/orders", "params": {"limit": 2}},
            destination_hook_params={"key": "raw/orders.json"},
        ).execute()

    assert base_res["records"] == generic_res["records"] == 3
    assert baseline.request_count == source.request_count == 2
    assert json.loads(store["raw/orders.json"]) == json.loads(
        (tmp_path / "raw/orders.json").read_text()
    )
    assert [r["id"] for r in json.loads((tmp_path / "raw/orders.json").read_text())] == [1, 2, 3]


def test_generic_http_to_file_end_to_end_against_fake_api(tmp_path: Path):
    from fake_api.server import start

    server, base = start()
    try:
        source = PaginatedHttpSourceHook(
            base,
            pagination=offset_pagination(records_key="data"),
            records_key="data",
            max_pages=50,
        )
        sink = FileBatchSink(str(tmp_path))
        result = GenericRecordTransfer(
            source_hook=source,
            destination_hook=sink,
            source_hook_params={"path": "/orders", "params": {"limit": 10}},
            destination_hook_params={"key": "raw/orders.json"},
        ).execute()
        assert result["records"] == 25
        assert source.request_count == 3  # 10 + 10 + 5
        rows = json.loads((tmp_path / "raw/orders.json").read_text())
        assert len(rows) == 25 and rows[0]["product"] == "Acme Widget"
    finally:
        server.shutdown()


@pytest.mark.iceberg
def test_generic_http_to_iceberg(tmp_path: Path):
    pytest.importorskip("pyiceberg")
    from fake_api.server import start
    from generic_transfer.adapters import IcebergAppendSink

    server, base = start()
    try:
        source = PaginatedHttpSourceHook(
            base,
            pagination=offset_pagination(records_key="data"),
            records_key="data",
        )
        sink = IcebergAppendSink(tmp_path / "warehouse")
        result = GenericRecordTransfer(
            source_hook=source,
            destination_hook=sink,
            source_hook_params={"path": "/orders", "params": {"limit": 10}},
            destination_hook_params={"namespace": "raw", "table": "orders"},
        ).execute()
        assert result["records"] == 25
    finally:
        server.shutdown()
