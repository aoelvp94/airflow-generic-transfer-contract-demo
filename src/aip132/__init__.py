"""AIP-132 phase 1 demo: prefix copy on ``FileTransferOperator``.

Importing this package imports Airflow. Install the ``aip132`` extra first.
"""

from aip132.file_transfer import (
    PrefixFileTransferOperator,
    classify_folder_markers,
    copy_files,
    copy_object,
    copy_single_file,
    list_prefix_once,
    plan_prefix_copy,
    stream_copy,
)

__all__ = [
    "PrefixFileTransferOperator",
    "classify_folder_markers",
    "copy_files",
    "copy_object",
    "copy_single_file",
    "list_prefix_once",
    "plan_prefix_copy",
    "stream_copy",
]
