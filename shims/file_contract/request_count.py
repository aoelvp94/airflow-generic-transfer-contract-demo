"""Count emulator API calls during a transfer (boto3 and Azure)."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from typing import Any, Iterator


class RequestCounter:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()

    def bump(self, name: str) -> None:
        self.counts[name] += 1

    def as_dict(self) -> dict[str, int]:
        return dict(sorted(self.counts.items()))

    def transfer_dict(self) -> dict[str, int]:
        """Drop obvious setup-only names if present; keep transfer ops."""
        return self.as_dict()


@contextmanager
def count_s3_client(client: Any) -> Iterator[RequestCounter]:
    counter = RequestCounter()

    def _before_call(**kwargs: Any) -> None:
        model = kwargs.get("model")
        name = getattr(model, "name", None) or kwargs.get("event_name", "unknown")
        counter.bump(str(name))

    event = client.meta.events
    event.register("before-call.s3", _before_call)
    try:
        yield counter
    finally:
        event.unregister("before-call.s3", _before_call)
