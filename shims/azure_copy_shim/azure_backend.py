"""WasbHook / local / S3 adapters for the prototype file-transfer contract."""

from __future__ import annotations

import tempfile
from typing import Any


class LocalFileSource:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path


class WasbHookWriter:
    def __init__(self, hook: Any) -> None:
        self.hook = hook

    def load_file(
        self,
        file_path: str,
        container_name: str,
        blob_name: str,
        create_container: bool = False,
        **load_options: Any,
    ) -> None:
        # Same keyword call as LocalFilesystemToWasbOperator / S3ToWasb.move_file.
        self.hook.load_file(
            file_path=file_path,
            container_name=container_name,
            blob_name=blob_name,
            create_container=create_container,
            **load_options,
        )


class S3ToWasbCopier:
    """Stream one S3 object to a Wasb blob via a temp file (pair path)."""

    def __init__(self, s3_hook: Any, wasb_hook: Any, s3_bucket: str) -> None:
        self.s3_hook = s3_hook
        self.wasb_hook = wasb_hook
        self.s3_bucket = s3_bucket

    def copy_via_temp(
        self,
        source_key: str,
        container_name: str,
        blob_name: str,
        create_container: bool,
        **wasb_extra: Any,
    ) -> None:
        with tempfile.NamedTemporaryFile("w") as temp_file:
            s3_client = self.s3_hook.get_conn()
            s3_client.download_file(self.s3_bucket, source_key, temp_file.name)
            self.wasb_hook.load_file(
                file_path=temp_file.name,
                container_name=container_name,
                blob_name=blob_name,
                create_container=create_container,
                **wasb_extra,
            )
