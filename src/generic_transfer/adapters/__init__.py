"""Capability adapters. One module per kind of hook, not per cloud pair.

* ``http_source`` — paginated HTTP ``BatchReadable``
* ``file_sink`` — object-store record sink, folder-marker listing
* ``file_copy`` — ``FileCopyable`` (sequential, parallel, server-side)
* ``iceberg_sink`` — Iceberg append
* ``adbc_io`` — Arrow/ADBC sqlite source and sink
* ``emulators`` / ``systems`` — local S3, GCS, and Azure filesystems
* ``pair_baseline`` — naive per-pair native-SDK copy used as the control

Per-cloud Airflow operator shims are not in this package. See ``shims/``.
"""
from .file_sink import (
    DictObjectStore,
    FileBatchSink,
    ObjectStoragePath,
    ObjectStoreCopySink,
    is_folder_marker,
    list_transfer_keys,
)
from .file_copy import ObjectStoreFileCopyHook
from .http_source import PaginatedHttpSourceHook
from .iceberg_sink import IcebergAppendSink
from .adbc_io import AdbcSqliteSink, AdbcSqliteSource, DictRowsSink, DictRowsSource

__all__ = [
    "PaginatedHttpSourceHook",
    "FileBatchSink",
    "DictObjectStore",
    "ObjectStoragePath",
    "ObjectStoreCopySink",
    "ObjectStoreFileCopyHook",
    "is_folder_marker",
    "list_transfer_keys",
    "IcebergAppendSink",
    "AdbcSqliteSource",
    "AdbcSqliteSink",
    "DictRowsSource",
    "DictRowsSink",
]
