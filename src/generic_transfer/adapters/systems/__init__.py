"""Per-system emulator connections.

The generic operator does not import this package. It only sees
``BatchReadable`` / ``FileCopyable`` hooks. These modules are the place
that knows how S3 (S3Mock or MinIO), GCS (fake-gcs-server), and Azure
(Azurite) differ at connection time.

Shared seeding, marker trees, and the ``EmulatorFS`` wrapper stay in
:mod:`generic_transfer.adapters.emulators`, which calls
:func:`open_s3`, :func:`open_gcs`, and :func:`open_azure`.
"""
from __future__ import annotations

from .azure import open_filesystem as open_azure
from .azure import storage_options as azure_storage_options
from .gcs import open_filesystem as open_gcs
from .gcs import storage_options as gcs_storage_options
from .s3 import open_filesystem as open_s3
from .s3 import storage_options as s3_storage_options

__all__ = [
    "azure_storage_options",
    "gcs_storage_options",
    "open_azure",
    "open_gcs",
    "open_s3",
    "s3_storage_options",
]
