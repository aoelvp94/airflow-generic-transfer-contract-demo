"""Hooks inside a TaskFlow task, and the same transfer through the operator.

Airflow is not imported at module import. Call ``copy_orders`` directly to
run the hook path with no scheduler. In a Dag, decorate that function:

.. code-block:: python

    from airflow.sdk import task  # Airflow 3.x (3.4 uses this path)
    # Airflow 2.4–2.10: from airflow.decorators import task

    @task
    def copy_orders(base_url: str, sink_root: str) -> dict:
        return transfer_records(...)

``airflow_tasks`` applies ``@task`` when Airflow is installed. Airflow 3
imports ``task`` from ``airflow.sdk``. Airflow 2.4 through 2.10 import it
from ``airflow.decorators``.
"""
from __future__ import annotations

from typing import Any

from generic_transfer import GenericRecordTransfer
from generic_transfer.adapters import FileBatchSink, PaginatedHttpSourceHook
from generic_transfer.pagination import offset_pagination


def transfer_records(
    source: Any,
    destination: Any,
    *,
    source_params: dict[str, Any] | None = None,
    destination_params: dict[str, Any] | None = None,
) -> Any:
    """Hook-only transfer: the destination consumes the source iterator."""
    return destination.write_batches(
        source.read_batches(**(source_params or {})),
        **(destination_params or {}),
    )


def transfer_with_operator(
    source: Any,
    destination: Any,
    *,
    source_params: dict[str, Any] | None = None,
    destination_params: dict[str, Any] | None = None,
) -> Any:
    """Same transfer via ``GenericRecordTransfer``."""
    return GenericRecordTransfer(
        source_hook=source,
        destination_hook=destination,
        source_hook_params=source_params,
        destination_hook_params=destination_params,
    ).execute()


def _orders_hooks(base_url: str, sink_root: str) -> tuple[Any, Any, dict[str, Any], dict[str, Any]]:
    source = PaginatedHttpSourceHook(
        base_url,
        pagination=offset_pagination(records_key="data"),
        records_key="data",
        max_pages=50,
    )
    destination = FileBatchSink(sink_root)
    source_params = {"path": "/orders", "params": {"limit": 10}}
    destination_params = {"key": "raw/orders.json"}
    return source, destination, source_params, destination_params


def copy_orders(base_url: str, sink_root: str) -> dict[str, Any]:
    """Body of a TaskFlow task. Hooks only; no operator and no scheduler."""
    source, destination, source_params, destination_params = _orders_hooks(base_url, sink_root)
    return transfer_records(
        source,
        destination,
        source_params=source_params,
        destination_params=destination_params,
    )


def copy_orders_with_operator(base_url: str, sink_root: str) -> dict[str, Any]:
    """The same orders transfer through ``GenericRecordTransfer``."""
    source, destination, source_params, destination_params = _orders_hooks(base_url, sink_root)
    return transfer_with_operator(
        source,
        destination,
        source_params=source_params,
        destination_params=destination_params,
    )


def airflow_tasks() -> tuple[Any, Any]:
    """Return ``copy_orders`` and ``copy_orders_with_operator`` wrapped in ``@task``.

    Airflow 3: ``airflow.sdk.task``. Airflow 2.4–2.10: ``airflow.decorators.task``.
    """
    try:
        from airflow.sdk import task
    except ImportError:
        from airflow.decorators import task

    return task(copy_orders), task(copy_orders_with_operator)
