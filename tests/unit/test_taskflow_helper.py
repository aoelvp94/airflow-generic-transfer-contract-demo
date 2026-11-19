"""The hook-only transfer runs without an Airflow scheduler."""
from __future__ import annotations

from pathlib import Path

from emulators.fake_api.server import start
from examples.taskflow_transfer import copy_orders, copy_orders_with_operator
from generic_transfer.adapters.file_sink import load_records


def test_taskflow_helper_matches_operator_without_scheduler(tmp_path: Path):
    server, base = start()
    try:
        helper_root = tmp_path / "helper"
        operator_root = tmp_path / "operator"
        helper_root.mkdir()
        operator_root.mkdir()
        helper = copy_orders(base, str(helper_root))
        via_operator = copy_orders_with_operator(base, str(operator_root))
        assert helper["records"] == via_operator["records"] == 25
        assert load_records(helper_root / "raw/orders.json") == load_records(
            operator_root / "raw/orders.json"
        )
    finally:
        server.shutdown()
