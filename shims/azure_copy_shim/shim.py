"""Shims for LocalFilesystemToWasbOperator and S3ToAzureBlobStorageOperator."""

from __future__ import annotations

from typing import Any

from azure_backend import S3ToWasbCopier, WasbHookWriter


def make_local_to_wasb_shim(mod: Any) -> type:
    Upstream = mod.LocalFilesystemToWasbOperator

    class LocalFilesystemToWasbOperatorShim(Upstream):
        def execute(self, context) -> None:
            hook = _hook_ctor(mod, "WasbHook")(wasb_conn_id=self.wasb_conn_id)
            self.log.info(
                "Uploading %s to wasb://%s as %s",
                self.file_path,
                self.container_name,
                self.blob_name,
            )
            writer = WasbHookWriter(hook)
            writer.load_file(
                file_path=self.file_path,
                container_name=self.container_name,
                blob_name=self.blob_name,
                create_container=self.create_container,
                **self.load_options,
            )

    LocalFilesystemToWasbOperatorShim.__name__ = "LocalFilesystemToWasbOperator"
    LocalFilesystemToWasbOperatorShim.__qualname__ = "LocalFilesystemToWasbOperator"
    return LocalFilesystemToWasbOperatorShim


def _hook_ctor(mod: Any, name: str):
    # Look up on the module so @mock.patch("...local_to_wasb.WasbHook") works.
    return getattr(mod, name)


def make_s3_to_wasb_shim(mod: Any) -> type:
    Upstream = mod.S3ToAzureBlobStorageOperator

    class S3ToAzureBlobStorageOperatorShim(Upstream):
        def execute(self, context) -> list[str]:
            self.log.info(
                "Getting %s from %s" if self.s3_key else "Getting all files start with %s from %s",
                self.s3_key if self.s3_key else self.s3_prefix,
                self.s3_bucket,
            )
            files_to_move: list[str] = self.get_files_to_move()
            if files_to_move:
                copier = S3ToWasbCopier(self.s3_hook, self.wasb_hook, self.s3_bucket)
                for file_name in files_to_move:
                    # Route through the contract-style copier, using the same
                    # key construction as the pair's move_file.
                    source_s3_key = self._create_key(self.s3_key, self.s3_prefix, file_name)
                    dest_blob = self._create_key(self.blob_name, self.blob_prefix, file_name)
                    copier.copy_via_temp(
                        source_s3_key,
                        self.container_name,
                        dest_blob,
                        self.create_container,
                        **self.wasb_extra_args,
                    )
                self.log.info("All done, uploaded %s to Azure Blob.", len(files_to_move))
            else:
                self.log.info("There are no files to move!")
            return files_to_move

    S3ToAzureBlobStorageOperatorShim.__name__ = "S3ToAzureBlobStorageOperator"
    S3ToAzureBlobStorageOperatorShim.__qualname__ = "S3ToAzureBlobStorageOperator"
    return S3ToAzureBlobStorageOperatorShim
