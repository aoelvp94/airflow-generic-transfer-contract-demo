# AIR331 prototype

This is a Python `ast` checker for the per-pair file transfer operators this
repository already maps. It is not a fork of Ruff. A production rule would
be one of Ruff's Rust `AIR` rules. `AIR331` is not an assigned Ruff code:
the published set runs through `AIR321` (`airflow31-moved`). The code here
is a placeholder so a diagnostic has a stable id.

```bash
just air-transfer-check
python -m tools.air_transfer_check path/to/dags --diff
python -m tools.air_transfer_check path/to/dags --fix
```

Exit status is 1 when a diagnostic remains. `--fix` rewrites only the safe
shape and still exits 1 if any flag-only diagnostic is left.

## What is flagged

Imports and calls of these classes, when the import module is the provider
module this repository maps:

| Operator | Autofix |
|---|---|
| `S3CopyObjectOperator` | Only the single-object shape below |
| `S3CopyPrefixOperator` | No |
| `GCSToGCSOperator` | No |
| `S3ToGCSOperator` | No |
| `GCSToS3Operator` | No |
| `S3ToAzureBlobStorageOperator` | No |
| `AzureBlobStorageToS3Operator` | No |
| `GCSToAzureBlobStorageOperator` | No |
| `AzureBlobStorageToGCSOperator` | No |
| `LocalFilesystemToWasbOperator` | No |

`FileTransferOperator` and `PrefixFileTransferOperator` are not flagged.

## Why only one shape is rewritten

`replace`, prefix layout, flatten, and folder-marker policy are not the same
call across those operators. `copy_path` strips the source prefix
(`keep_structure=True`), skips every classified marker by default, raises
on a flattened basename collision before copying, and returns destination
keys for a prefix (`None` for one file). The pair operators do not share
those defaults:

- `S3ToGCSOperator` joins the full source key unless `apply_gcs_prefix=True`,
  and its marker policy keeps a trailing-slash key that does not overlap
  another object.
- `GCSToS3Operator` joins the full object name when
  `keep_directory_structure` is true, which is the default.
- `S3ToAzureBlobStorageOperator` with `replace=False` compares full
  destination blob names to relative source names.
- `GCSToGCSOperator` defaults to `replace=True`, copies markers, and returns
  URIs. `overwrite` defaults to false, and a single file returns `None`.
- `S3CopyPrefixOperator` always replaces, copies markers, and returns `None`.
- WASB operators target the blob service. `ObjectStoragePath` does not
  construct `wasb`.

Those calls are reported with a hint and the source is left unchanged.

## The safe rewrite

`S3CopyObjectOperator` copies one key, replaces the destination, and returns
`None`. There is no prefix, glob, flatten, or marker policy to get wrong.
The rewrite is applied only when every keyword is among `task_id`,
`source_bucket_name`, `source_bucket_key`, `dest_bucket_name`,
`dest_bucket_key`, and `aws_conn_id`; both keys are string literals; a key
does not end in `/` and contains no `*`; and a full `s3://` key is not
combined with a bucket name. `aws_conn_id` defaults to `aws_default` on the
AWS base operator, so the rewrite sets both connection ids to that value
when the call omits it. `overwrite=True` is set because CopyObject replaces
the object and the single-file operator raises when overwrite is false.

A version id, ACL, metadata directive, KMS argument, or any other keyword
blocks the rewrite. The S3 hook turns an omitted `acl_policy` into the
canned ACL `private`. The rewritten call does not take an ACL. The proof
is the object key, the body, the replacement, and the `None` return, not
the canned ACL.

```python
FileTransferOperator(
    task_id="copy_one",
    src="s3://src-bucket/incoming/a.txt",
    dst="s3://dst-bucket/archive/a.txt",
    source_conn_id="aws_default",
    dest_conn_id="aws_default",
    overwrite=True,
)
```
