"""Stateless pagination helpers for HTTP-style record sources.

Each factory returns ``fn(response) -> dict | None``: kwargs for the next
request (``params`` or ``url``), or ``None`` to stop. The page cap lives in
the source hook, not here.
"""
from __future__ import annotations

from typing import Any, Callable
from urllib.parse import parse_qsl, urlparse

PaginationFn = Callable[[Any], "dict | None"]


def _get(obj: Any, path: str | None) -> Any:
    if not path:
        return obj
    for part in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def _query(url: str) -> dict:
    return dict(parse_qsl(urlparse(url).query))


def offset_pagination(
    offset_param: str = "offset",
    limit_param: str = "limit",
    records_key: str | None = None,
    has_more_key: str = "has_more",
) -> PaginationFn:
    def fn(response):
        body = response.json()
        records = _get(body, records_key) or []
        if not records or (has_more_key and not body.get(has_more_key)):
            return None
        params = _query(response.request.url)
        offset = int(params.get(offset_param, 0))
        params[offset_param] = str(offset + len(records))
        return {"params": params}

    return fn


def page_number_pagination(
    page_param: str = "page",
    records_key: str | None = None,
    total_pages_key: str = "total_pages",
    start: int = 1,
) -> PaginationFn:
    def fn(response):
        body = response.json()
        records = _get(body, records_key) or []
        params = _query(response.request.url)
        page = int(params.get(page_param, start))
        total = body.get(total_pages_key) if total_pages_key else None
        if not records or (total is not None and page >= int(total)):
            return None
        params[page_param] = str(page + 1)
        return {"params": params}

    return fn


def cursor_pagination(
    cursor_param: str = "after",
    records_key: str | None = None,
    id_key: str = "id",
    numeric: bool = True,
    inclusive: bool = True,
) -> PaginationFn:
    def fn(response):
        records = _get(response.json(), records_key) or []
        params = _query(response.request.url)
        ids = [r[id_key] for r in records]
        current = params.get(cursor_param)
        if inclusive and current is not None:
            ids = [i for i in ids if str(i) != str(current)]
        if not ids:
            return None
        nxt = max(ids, key=int) if numeric else max(ids, key=str)
        params[cursor_param] = str(nxt)
        return {"params": params}

    return fn


def next_link_pagination(path: str = "next", allow_other_hosts: bool = False) -> PaginationFn:
    def fn(response):
        link = _get(response.json(), path)
        if not link:
            return None
        if not allow_other_hosts and urlparse(link).netloc != urlparse(response.request.url).netloc:
            return None
        return {"url": link, "params": None}

    return fn


def extract_records(body: Any, records_key: str | None) -> list[dict]:
    if records_key:
        chunk = _get(body, records_key) or []
        if not isinstance(chunk, list):
            raise TypeError(f"{records_key!r} is not a list")
        return list(chunk)
    if isinstance(body, list):
        return list(body)
    return [body]
