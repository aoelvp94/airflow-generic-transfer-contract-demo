"""Contract + operator unit tests (no adapters yet)."""
from __future__ import annotations

import pytest

from generic_transfer import BatchReadable, BatchWritable, GenericRecordTransfer


class StubSource:
    def __init__(self, batches):
        self._batches = batches
        self.seen_params = None

    def read_batches(self, **params):
        self.seen_params = dict(params)
        yield from self._batches


class StubSink:
    def __init__(self):
        self.seen = []
        self.seen_params = None

    def write_batches(self, batches, **params):
        self.seen_params = dict(params)
        for batch in batches:
            self.seen.extend(batch)
        return {"records": len(self.seen)}


class NotASource:
    pass


def test_structural_typing_without_importing_protocol():
    """A plain class with the right methods satisfies the Protocol."""
    src = StubSource([[{"id": 1}]])
    sink = StubSink()
    assert isinstance(src, BatchReadable)
    assert isinstance(sink, BatchWritable)
    assert not isinstance(NotASource(), BatchReadable)


def test_operator_forwards_params_and_streams():
    src = StubSource([[{"id": 1}, {"id": 2}], [{"id": 3}]])
    sink = StubSink()
    op = GenericRecordTransfer(
        source_hook=src,
        destination_hook=sink,
        source_hook_params={"endpoint": "/orders", "limit": 2},
        destination_hook_params={"key": "raw/orders.json"},
    )
    result = op.execute()
    assert result == {"records": 3}
    assert src.seen_params == {"endpoint": "/orders", "limit": 2}
    assert sink.seen_params == {"key": "raw/orders.json"}
    assert [r["id"] for r in sink.seen] == [1, 2, 3]


def test_operator_rejects_non_contract_hooks():
    with pytest.raises(TypeError, match="read_batches"):
        GenericRecordTransfer(
            source_hook=NotASource(),
            destination_hook=StubSink(),
        ).execute()
    with pytest.raises(TypeError, match="write_batches"):
        GenericRecordTransfer(
            source_hook=StubSource([[]]),
            destination_hook=NotASource(),
        ).execute()


def test_template_fields_declared():
    assert "source_hook_params" in GenericRecordTransfer.template_fields
    assert "destination_hook_params" in GenericRecordTransfer.template_fields
