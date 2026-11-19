"""Hook contracts the generic operator depends on.

The Protocol classes live in :mod:`generic_transfer.contract.protocols`.
Import them from this package so the public names stay stable::

    from generic_transfer.contract import BatchReadable, FileCopyable
    from generic_transfer import GenericRecordTransfer, GenericFileTransfer
"""
from __future__ import annotations

from .protocols import (
    AsyncBatchReadable,
    AsyncBatchWritable,
    BatchReadable,
    BatchWritable,
    FileCopyable,
    LineageProvider,
    ParamsDescribable,
    batch_format,
)

__all__ = [
    "AsyncBatchReadable",
    "AsyncBatchWritable",
    "BatchReadable",
    "BatchWritable",
    "FileCopyable",
    "LineageProvider",
    "ParamsDescribable",
    "batch_format",
]
