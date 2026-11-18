"""Paginated HTTP source hook that satisfies BatchReadable structurally.

Pagination is handled inside the hook. The operator only sees batches of
records. Uses ``requests``; works against a real server or requests-mock.
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Callable

import requests

from generic_transfer.pagination import extract_records


class PaginatedHttpSourceHook:
    """Minimal stand-in for an HttpHook with a ``read_batches`` capability."""

    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        pagination: Callable[[Any], dict | None] | None = None,
        records_key: str | None = None,
        max_pages: int = 100,
        request_timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.pagination = pagination
        self.records_key = records_key
        self.max_pages = max_pages
        self.request_timeout = request_timeout
        self.request_count = 0

    def read_batches(
        self,
        *,
        path: str = "/",
        params: dict[str, Any] | None = None,
        **_: Any,
    ) -> Iterator[list[dict[str, Any]]]:
        url = self.base_url + (path if path.startswith("/") else f"/{path}")
        kwargs: dict[str, Any] | None = {"url": url, "params": dict(params or {}) or None}
        pages = 0
        while kwargs is not None and pages < self.max_pages:
            resp = self.session.get(
                kwargs["url"],
                params=kwargs.get("params"),
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
            self.request_count += 1
            pages += 1
            body = resp.json()
            yield extract_records(body, self.records_key)
            if self.pagination is None:
                break
            nxt = self.pagination(resp)
            if nxt is None:
                break
            kwargs = {
                "url": nxt.get("url", kwargs["url"]),
                "params": nxt.get("params"),
            }
