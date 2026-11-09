"""Paginated HTTP source hook that satisfies BatchReadable structurally.

Pagination is handled inside the hook. The operator only sees batches of
records. Uses ``requests``; works against a real server or requests-mock.
"""
from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, Callable

import requests

from generic_transfer.pagination import extract_records


class PaginatedHttpSourceHook:
    """Minimal stand-in for an HttpHook with a ``read_batches`` capability."""

    batch_format = "rows"

    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        pagination: Callable[[Any], dict | None] | None = None,
        records_key: str | None = None,
        max_pages: int = 100,
        request_timeout: float = 30.0,
        max_retries: int = 3,
        retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504),
        lineage_dataset: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.pagination = pagination
        self.records_key = records_key
        self.max_pages = max_pages
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.retry_statuses = retry_statuses
        self.lineage_dataset = lineage_dataset
        self.request_count = 0

    def describe_params(self, role: str = "source") -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "params": {"type": "object"},
            },
            "required": [],
            "additionalProperties": False,
        }

    def get_lineage_datasets(self, **params: Any) -> list[Any]:
        path = params.get("path", "/")
        uri = self.lineage_dataset or f"{self.base_url}{path}"
        return [{"uri": uri, "role": "inlet"}]

    def _get(self, url: str, params: dict[str, Any] | None) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            resp = self.session.get(url, params=params, timeout=self.request_timeout)
            self.request_count += 1
            if resp.status_code in self.retry_statuses:
                last_exc = requests.HTTPError(
                    f"{resp.status_code} for {url}", response=resp
                )
                time.sleep(0.01 * (2 ** attempt))
                continue
            resp.raise_for_status()
            return resp
        assert last_exc is not None
        raise last_exc

    def read_batches(
        self,
        *,
        path: str = "/",
        params: dict[str, Any] | None = None,
        **_: Any,
    ) -> Iterator[list[dict[str, Any]]]:
        url = self.base_url + (path if path.startswith("/") else f"/{path}")
        kwargs: dict[str, Any] | None = {
            "url": url,
            "params": dict(params or {}) or None,
        }
        pages = 0
        seen_cursors: set[str] = set()
        while kwargs is not None and pages < self.max_pages:
            resp = self._get(kwargs["url"], kwargs.get("params"))
            pages += 1
            body = resp.json()
            yield extract_records(body, self.records_key)
            if self.pagination is None:
                break
            nxt = self.pagination(resp)
            if nxt is None:
                break
            # Guard against cursor loops (same next params forever).
            cursor_key = str(nxt.get("url") or nxt.get("params"))
            if cursor_key in seen_cursors:
                break
            seen_cursors.add(cursor_key)
            kwargs = {
                "url": nxt.get("url", kwargs["url"]),
                "params": nxt.get("params"),
            }

    async def aread_batches(
        self,
        *,
        path: str = "/",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Async wrapper around the sync paginator (demo / deferrable sketch)."""
        for batch in self.read_batches(path=path, params=params, **kwargs):
            yield batch
