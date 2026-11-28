---
name: aip132-transfer-migration
description: Migrate a Dag off a per-pair Airflow file transfer operator (S3ToGCSOperator, GCSToS3Operator, S3ToAzureBlobStorageOperator, GCSToGCSOperator, and the other file pairs this repository maps) onto FileTransferOperator or copy_path. Use when editing a task that constructs one of those operators, or when asked how an AIP-132 prefix copy replaces a pair operator. Do not rewrite a call that fails the "do not auto-migrate" checklist.
---

# AIP-132 transfer migration

AIP-132 phase 1 copies objects with one operator on storage paths, instead of one operator class per source and destination. In this repository the copy is `aip132.path_ops.copy_path`. `aip132.file_transfer.FileTransferOperator` checks arguments, calls `copy_path`, and emits OpenLineage. `PrefixFileTransferOperator` is that same class. A `@task` calls `copy_path` and does not import an operator.

Upstream, the same arguments are meant to land on `airflow.providers.common.io.operators.file_transfer.FileTransferOperator`. This demo subclasses the installed operator so it can run without editing site-packages. Behaviour of the prefix arguments is the demo, described in [docs/AIP-132-demo.md](../../docs/AIP-132-demo.md).

The pair mapping below is the one this repository already records. Policy rows are `shims/generic_all_pairs/pair_policy.py`. Constructor translation for the upstream unit-test wrappers is `shims/generic_all_pairs/wrappers.py`. The audit of those constructors is `audits/results/PAIR_AUDIT.csv` and `shims/generic_all_pairs/RESULTS.md`.

## What copy_path does

| Argument | Behaviour |
|---|---|
| `src`, `dst` | String or `ObjectStoragePath`. A source string that ends with `/` is a prefix even when the path does not exist and even after `ObjectStoragePath` drops the slash. A missing prefix returns `[]`. A source that is not a prefix is one object. |
| `source_conn_id`, `dest_conn_id` | Used when the path is a string. |
| `overwrite` | Default `False`. Prefix: keys already at the planned destination are left in place and omitted from the return value. The check is one destination listing, not a per-key `exists`. Single file: `False` raises `ValueError` if the destination file exists. `True` replaces. |
| `keep_structure` | Default `True`: the destination key is the source key relative to the source prefix. `False`: basename only. Two source keys that share a destination key raise `ValueError` before any object is copied. |
| `match_glob` | `fnmatch` against the relative key and its basename. `*` matches `/`. This is not a `**` glob and not a delimiter split. |
| `folder_markers` | `skip` (default) or `copy`. Markers come from one `find` (`classify_folder_markers`): directory rows, names ending in `/`, and empty or unknown-size names that have a listed child. A zero-byte object with no children is a file. `collapse_marker_aliases` drops a lone `type=directory` row that `find(withdirs=True)` invented, and copies a real marker once. `file://` and `memory://` directories are not object keys. |
| `chunk_size` | Bytes per read on a cross-store stream. Default 1 MiB. A failed write deletes the partial destination so a later `overwrite=False` retry does not treat the leftover as complete. |

Same-store copies (same protocol and connection id) use `ObjectStoragePath.copy`. Cross-store copies stream with `shutil.copyfileobj`. A folder marker is an empty write (`fs.pipe` when the key must keep its trailing slash). There is no move, gzip, delimiter, deferral, transfer-service job, ACL, KMS, or container creation.

Return value and XCom:

- A prefix returns the destination keys that were copied, sorted. A trailing slash the listing stored is put back, because `ObjectStoragePath.key` drops it. Skipped keys are absent.
- A single file returns `None`, matching the installed `FileTransferOperator`.

OpenLineage (`src/aip132/lineage.py`):

- Start names the configured source and destination and does not list.
- Complete names each copied object. Pairs are matched by destination key, not by position in the returned list. That matters when `keep_structure=False`, because the returned keys are sorted by destination while the copy walked sources in source-key order.
- The in-process cache key is `(dag_id, task_id, run_id, map_index)`, and it stores the returned output names. Mapped tasks and a second run in the same process do not reuse each other's pairs.
- A listener in another process rebuilds pairs from the XCom list only when `keep_structure` is true. Flattened copies need the in-process pairs to name the right source for each destination.
- Schemes reported to OpenLineage: `s3`/`s3a` as `s3`, `gs`/`gcs` as `gs`, `abfs`/`abfss` as `abfss`, `wasb`/`wasbs` as `wasbs`. `wasb` is not a filesystem `ObjectStoragePath` can construct; the lineage helper parses that URI as text.

## Operators this repository maps

`path_layout` is how the pair operator builds the destination key. `preserve` strips the source prefix (the same idea as `keep_structure=True`). `join_source_key` appends the full source key under the destination prefix. `flatten` keeps the basename.

`marker_policy` is not the same enum as `folder_markers`. `skip_overlapping` drops a trailing-slash key only when another listed key sits under it. `copy_path` has no overlapping-only mode: `skip` drops every classified marker, `copy` writes real marker objects.

| Operator | replace default | Path layout | Markers | `execute` returns |
|---|---|---|---|---|
| `S3CopyObjectOperator` | always replaces; no flag | one object | none | `None` |
| `S3CopyPrefixOperator` | always replaces; no flag | `preserve` | `copy` | `None` |
| `GCSToGCSOperator` | `True` | `preserve` | `copy` | destination URIs, directory URIs removed |
| `S3ToGCSOperator` | `False` | `join_source_key` unless `apply_gcs_prefix=True` | `skip_overlapping` | S3 keys, or GCS URIs when `return_gcs_uris=True` |
| `GCSToS3Operator` | `False` | `join_source_key` when `keep_directory_structure=True` (the default) | `skip_overlapping` | GCS object names |
| `S3ToAzureBlobStorageOperator` | `False` | `preserve` (strips `s3_prefix`) | `skip_overlapping` | files it decided to move |
| `AzureBlobStorageToS3Operator` | `False` | `join_source_key` (`dest_s3_key` plus the blob name) | `skip_overlapping` | blob names |
| `GCSToAzureBlobStorageOperator` | `False` | `join_source_key` | `skip` (every trailing-slash key) | uploaded blob names |
| `AzureBlobStorageToGCSOperator` | upload replaces; no flag | one blob | none | one GCS URI, or a one-item list |
| `LocalFilesystemToWasbOperator` | upload replaces; no flag | one local file | none | `None` |

`S3ToAzureBlobStorageOperator` with `replace=False` does not compare the planned destination key. It subtracts full destination blob names from relative source names, so an existing `out/a.txt` is not seen as the image of `pre/a.txt`. The repro is `experiments/s3_to_wasb_replace_repro/` and the report is [apache/airflow#76880](https://github.com/apache/airflow/issues/76880). `copy_path` skips the planned destination key. Those are not the same skip set.

## Rewrite a single S3 object

This is the one mapped call whose prefix, glob, flatten, and folder-marker behaviour cannot diverge, and whose return value is already `None`.

`S3CopyObjectOperator` copies one key and replaces the destination. It has no prefix and no marker policy. `execute` returns `None`. The single-file branch of `FileTransferOperator` also returns `None`, and it replaces only when `overwrite=True` (the default `False` raises if the destination file exists).

```python
from airflow.providers.common.io.operators.file_transfer import FileTransferOperator

copy = FileTransferOperator(
    task_id="copy_one",
    src="s3://src-bucket/incoming/a.txt",
    dst="s3://dst-bucket/archive/a.txt",
    source_conn_id="aws_default",
    dest_conn_id="aws_default",
    overwrite=True,
)
```

`aws_conn_id` defaults to `aws_default` on the AWS base operator and is the connection for both buckets. A full `s3://` key is used as the URI only when the separate bucket argument is omitted; the S3 hook treats "URI plus bucket name" as a different call, so do not invent a concatenation for that shape.

Leave the call alone when it passes `source_version_id`, `acl_policy`, `meta_data_directive`, `kms_key_id`, `kms_encryption_type`, `verify`, or a region. Those arguments have no `FileTransferOperator` equivalent. Also leave it alone when a bucket or key is not a literal: a dynamic key may be a prefix, and a prefix return value would become a list instead of `None`.

The hook's default canned ACL is `private` when `acl_policy` is omitted. A same-store `ObjectStoragePath.copy` does not take an ACL argument. For a call that sets no ACL, the object body and the key are the part that matches; do not claim ACL parity if a downstream check reads the canned ACL.

## Do not auto-migrate

Stop and rewrite by hand, or leave the pair operator, when any row below is true.

1. The class is not in the table above. SQL, warehouse `COPY`, Hive, Redshift, FTP, HTTP, and the operators in [docs/first-wave-exclusions.md](../../docs/first-wave-exclusions.md) are out of scope.
2. The call sets a flag this operator does not implement: `gzip`, `delimiter`, `apply_wildcard`, `deferrable`, `poll_interval`, `move_object=True`, `create_container=True`, `load_options`, ACL, KMS, metadata directive, `extra_args`, impersonation, a modified-time filter, retention, `source_objects` as a list, `exact_match=False` together with a prefix, or `source_version_id`.
3. `replace` and `overwrite` do not describe the same action. `GCSToGCSOperator.replace` defaults to `True`; `overwrite` defaults to `False`. On a single file, `overwrite=False` raises, while several pair operators skip. On a prefix, `overwrite=False` skips planned destination keys that already exist. `S3ToAzureBlobStorageOperator` with `replace=False` uses the comparison in the section above, not that planned key.
4. The destination key would change. `keep_structure=True` is `preserve`, not `join_source_key`. `S3ToGCSOperator` defaults to `apply_gcs_prefix=False`, so `sales/a.txt` is joined onto the destination prefix and the prefix appears twice unless the Dag set `apply_gcs_prefix=True`. `GCSToS3Operator` defaults to `keep_directory_structure=True`, which this repo records as `join_source_key`, not as "strip the GCS prefix". `flatten_structure=True` is `keep_structure=False` only after checking collisions: `copy_path` raises `ValueError` and copies nothing; a pair operator may overwrite or dedupe.
5. Folder markers would change. `skip_overlapping` is not `skip` and not `copy`. Same-store GCS and S3 prefix copies keep marker keys; `folder_markers` defaults to `skip`. A zero-byte file with no children must stay a file under both policies. An implicit directory row must not become a new object.
6. The XCom consumers would break. Prefix `copy_path` returns sorted destination keys. `S3ToGCSOperator` returns source S3 keys unless `return_gcs_uris=True`. `GCSToGCSOperator` returns destination URIs and drops names that end with `/`. `S3CopyPrefixOperator` returns `None`. A single file returns `None`, so a one-object `GCSToGCSOperator` (it returns a list of URIs) is not a drop-in.
7. An OpenLineage consumer depends on the pair operator's datasets. This operator's complete event is one dataset per copied key, paired by destination key, and it omits keys that were skipped. Marker keys appear only when they were copied. `wasb` URIs are not `ObjectStoragePath` values; `LocalFilesystemToWasbOperator` and the Azure blob operators talk to the blob service, not to an `abfs` path this helper can open.
8. The Dag uses a transfer service, server-side job, or move. `S3ToGCSOperator` can submit a Storage Transfer Service job. `GCSToGCSOperator(move_object=True)` deletes the source. Neither is a `copy_path` call.

## Manual prefix example

`S3ToGCSOperator` is flag-only. The shape below is the usual one, and it is not the same call as `copy_path` with the same strings.

```python
S3ToGCSOperator(
    task_id="sales",
    bucket="src",
    prefix="sales/",
    dest_gcs="gs://dst/sales/",
    replace=False,
)
```

`apply_gcs_prefix` is false by default, so the object key `sales/a.txt` is joined under `gs://dst/sales/` and lands at `gs://dst/sales/sales/a.txt`. `copy_path("s3://src/sales/", "gs://dst/sales/")` strips `sales/` and writes `gs://dst/sales/a.txt`. Matching the old layout means choosing a destination prefix that already accounts for the join, or migrating only Dags that set `apply_gcs_prefix=True` and then checking markers and the return value.

`replace=False` skips existing destination objects after that join. `overwrite=False` skips planned keys from a different layout, so the skip set moves with the layout. `skip_overlapping` can keep a trailing-slash key that has no children; `folder_markers="skip"` drops every classified marker. The default return value is the S3 keys, not the destination keys `copy_path` puts in XCom.

A Dag that set `apply_gcs_prefix=True`, does not use wildcard, gzip, delimiter, or deferral, and reads no XCom can be moved after those three checks. Write `folder_markers` and `overwrite` explicitly. Do not leave them at the new defaults and assume the old defaults came along.

```python
from airflow.sdk import task
from aip132.path_ops import copy_path

@task
def copy_sales() -> list[str]:
    # apply_gcs_prefix=True on the old task: strip "sales/" and keep the tail.
    return copy_path(
        "s3://src/sales/",
        "gs://dst/sales/",
        source_conn_id="aws_default",
        dest_conn_id="google_cloud_default",
        overwrite=False,
        keep_structure=True,
        folder_markers="skip",
    )
```

`GCSToS3Operator(..., keep_directory_structure=False, flatten_structure=True)` is `keep_structure=False` only when no two objects share a basename. If they do, `copy_path` raises before copying. Inspect the prefix or keep the directory structure.

`GCSToGCSOperator` defaults to `replace=True`, copies marker keys, and can move the source. A single object with `exact_match=True` and `replace=True` still returns a list of URIs. Point XCom at that change before swapping the class. A prefix without `exact_match=True` is a listing, including wildcard and delimiter, and stays on the pair operator until those are removed.

## Checklist before editing the Dag

- The class is in the table, and every keyword is either copied into the new call or intentionally dropped.
- The destination key of one real object was written out for both the old layout and `keep_structure`.
- `overwrite` is set from the old `replace` behaviour, including the single-file raise and the S3-to-Azure comparison.
- `folder_markers` is set from the old marker policy, or the Dag does not store marker keys.
- `keep_structure=False` was checked for basename collisions.
- Callers of the task return value accept the new XCom (`list[str]` of destination keys, or `None` for one file).
- OpenLineage consumers accept start-as-configured-path and complete-as-copied-keys.
- The schemes are ones `ObjectStoragePath` opens (`s3`, `gs`, `file`, `abfs`, and `memory` in tests). A `wasb://` blob upload is not that path.
