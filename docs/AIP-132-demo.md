# AIP-132 phase 1 demo

[AIP-132](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/467436039/AIP-132+One+transfer+operator+on+hook+contracts)
phase 1 extends `common.io` `FileTransferOperator` so one operator can copy a
prefix between object stores. The rollout plan's first demo is: copy an S3
prefix, handle folder markers, and return the keys that were copied.

This repository does that on Airflow 3.4 with
`PrefixFileTransferOperator` (`src/aip132/file_transfer.py`). The operator
subclasses `FileTransferOperator` and is a real `BaseOperator`. The copy
logic is plain functions, so a `@task` can call `copy_files` and get the same
behaviour. The example Dag is `examples/aip132_s3_prefix_to_gcs.py`.

Upstream `FileTransferOperator` still copies a single object via
`src.copy(dst)`. A single-file `src` in this operator does that same call:
`overwrite=False` raises when the destination file exists, and `execute`
returns `None`.

## Run

Python 3.11 or newer (Airflow 3.4). The extra pins Airflow 3.4.0,
`apache-airflow-providers-common-io` 1.11.0, amazon 9.39.0, google 22.8.0,
and microsoft-azure 15.3.0, plus the fsspec backends those providers open
(`s3fs` 2026.9.0, `gcsfs` 2026.10.1, `adlfs` 2026.8.0). `just aip132-test`
installs them into `.venv-aip132` with the Airflow 3.4.0 constraint file.

```bash
just aip132-test
# file:// and memory:// only. Prints pytest's own summary.

just up          # S3Mock, fake-gcs-server, Azurite
.venv-aip132/bin/python -m pytest -v -m integration tests/integration/test_aip132_prefixes.py
just aip132-demo # S3 prefix -> GCS, markers skipped, keys printed
just down
```

`gcsfs` 2026.10 and later defaults to a gRPC client. The emulator demo
attaches `gcsfs.core.GCSFileSystem`, the HTTP client fake-gcs-server answers,
through `airflow.sdk.io.attach`. That is the same hook `ObjectStoragePath`
uses for a connection id.

## What the operator accepts

| Argument | Role |
|---|---|
| `src`, `dst` | String or `airflow.sdk.ObjectStoragePath`. A prefix ends with `/`. |
| `source_conn_id`, `dest_conn_id` | Used when the path is a string. |
| `match_glob` | `fnmatch` against the relative key and its basename. `*` matches `/`. |
| `overwrite` | Prefix: `False` skips keys that already exist. Single file: upstream raise. |
| `keep_structure` | `True` keeps the relative path. `False` flattens to the basename. |
| `folder_markers` | `skip` (default) or `copy`. |
| `chunk_size` | Bytes per read on the cross-store stream. Default 1 MiB. |

`execute` returns the copied destination keys (`ObjectStoragePath.key`),
sorted. Airflow stores that list as the task XCom. Keys that were skipped
are not in the list.

## Phase-1 items

| Item | In this demo | Still open |
|---|---|---|
| Prefix and glob | A trailing slash or a directory is listed with one `find(detail=True, withdirs=True)`. `match_glob` filters that listing. | A prefix that is not a directory and has no trailing slash is still a single key. `fnmatch` is not a `**` glob. |
| Overwrite versus skip | Prefix `overwrite=False` compares planned destination paths to one destination listing and leaves existing keys unchanged. `True` replaces them. | Single-file `overwrite=False` still raises, matching upstream. Flattening two sources onto one basename keeps the first when overwrite is false. |
| Folder-marker rule | Markers are classified from that one listing: directory rows, names ending in `/`, and empty (or unknown-size) names that have a listed child. A zero-byte file with no children is copied. The classifier does not call `info`. | `folder_markers=copy` writes an empty object for a real marker key. `file://` and `memory://` directories are not objects, so they are skipped even then. Opening a cross-store object can still stat inside fsspec. |
| Report what was copied | Prefix `execute` returns the destination keys. The S3→GCS demo prints them and checks the destination listing. | Single-file `execute` returns `None`, matching upstream, so that XCom is null. |
| Native fast path | When `samestore` is true (same protocol and connection id), each object is `ObjectStoragePath.copy`, which calls `fs.copy`. | This is same-store `fs.copy`, not a transfer service such as Storage Transfer Service. There is no hook method yet for "this backend can copy natively" beyond `samestore`. Deferrable copies are not implemented. OpenLineage for a prefix is still the parent's start facet on the two paths, not one dataset per object. |

Phase 2 (paged records) and turning the existing pair operators into wrappers
are out of scope for this demo. The generic record path in
`src/generic_transfer/` is unchanged.

## Helpers

`copy_files` is the function the operator calls. `list_prefix_once`,
`classify_folder_markers`, `plan_prefix_copy`, `copy_object`, `stream_copy`,
and `copy_single_file` are the smaller pieces. `examples/aip132_s3_prefix_to_gcs.py`
shows `copy_s3_prefix_to_gcs` as a `@task` body with the same arguments.
