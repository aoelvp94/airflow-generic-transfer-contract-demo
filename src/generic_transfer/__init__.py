"""One generic transfer operator on hook contracts.

Public names (re-exported here):

* Protocols — :mod:`generic_transfer.contract` (defined in ``contract/protocols.py``)
* Operators — :class:`GenericRecordTransfer`, :class:`GenericFileTransfer`,
  :class:`GenericAsyncRecordTransfer`

Capability adapters (HTTP, object store, Iceberg, ADBC) are
:mod:`generic_transfer.adapters`. Per-system emulator connections are
:mod:`generic_transfer.adapters.systems`. Airflow pair-operator shims live
outside this package, under ``shims/``.
"""
from .contract import (
    AsyncBatchReadable,
    AsyncBatchWritable,
    BatchReadable,
    BatchWritable,
    FileCopyable,
    LineageProvider,
    ParamsDescribable,
    batch_format,
)
from .operator import (
    GenericAsyncRecordTransfer,
    GenericFileTransfer,
    GenericRecordTransfer,
    ParamValidationError,
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
    "GenericAsyncRecordTransfer",
    "GenericFileTransfer",
    "GenericRecordTransfer",
    "ParamValidationError",
]
