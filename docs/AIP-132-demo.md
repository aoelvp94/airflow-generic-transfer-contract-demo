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
# file:// and memory:// only, including OpenLineage facets.
# Prints pytest's own summary.

just up          # S3Mock, fake-gcs-server, Azurite
.venv-aip132/bin/python -m pytest -v -m integration tests/integration/test_aip132_prefixes.py
just aip132-demo # S3 prefix -> GCS, markers skipped, keys printed
just down

just aip132-e2e  # emulators + Airflow 3.4 scheduler, then tears both down
```

`just aip132-e2e` builds an Airflow 3.4.0 image (common.io, OpenLineage,
s3fs, gcsfs) and starts `airflow-apiserver`, `airflow-scheduler`, and
`airflow-dag-processor` with `LocalExecutor`. There is no triggerer: the
operator is not deferrable. The three emulators are the same compose
services as `just up`. The recipe copies `src/aip132` and `examples/aip132_s3_prefix_to_gcs.py` into
the image (a bind mount of those paths is not used). The scheduler runs that
Dag.
The recipe triggers that Dag, reads the task XCom from the Airflow 3 REST
API, and reads the OpenLineage console transport from the task log and the
scheduler log. It then stops the Airflow stack and the emulators.

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
| `folder_markers` | `skip` (default) or `copy`. `copy` writes the marker key as stored and returns it. |
| `chunk_size` | Bytes per read on the cross-store stream. Default 1 MiB. |

`execute` returns the copied destination keys, sorted. The key is
`ObjectStoragePath.key`, with a trailing slash restored when the listing
stored one (`ObjectStoragePath` drops it, and that slash is part of the
object key). Airflow stores that list as the task XCom. Keys that were
skipped are not in the list.

## Phase-1 items

| Item | In this demo | Still open |
|---|---|---|
| Prefix and glob | A trailing slash or a directory is listed with one `find(detail=True, withdirs=True)`. `match_glob` filters that listing. | A prefix that is not a directory and has no trailing slash is still a single key. `fnmatch` is not a `**` glob. |
| Overwrite versus skip | Prefix `overwrite=False` compares planned destination paths to one destination listing and leaves existing keys unchanged. `True` replaces them. | Single-file `overwrite=False` still raises, matching upstream. Flattening two sources onto one basename keeps the first when overwrite is false. |
| Folder-marker rule | Markers are classified from that one listing: directory rows, names ending in `/`, and empty (or unknown-size) names that have a listed child. A zero-byte file with no children is copied. The classifier does not call `info`. `find(withdirs=True)` also reports a synthetic directory next to the zero-byte marker object (`nested` and `nested/`). `folder_markers=copy` writes the object key once. adlfs omits the slash on that object; the slash is restored. The write uses `fs.pipe`, because `s3fs.open` drops a trailing slash. Unit tests cover `file://` / `memory://` (a listed marker key is copied; a directory entry is not). Integration tests cover every S3, GCS, and Azure emulator pair, next to `skip`, and the returned key matches the native destination key, slash included. | `file://` and `memory://` directories are not objects, so they are skipped even when the policy is `copy`. Opening a cross-store object can still stat inside fsspec. |
| Report what was copied | Prefix `execute` returns the destination keys. The S3→GCS demo prints them and checks the destination listing. `just aip132-e2e` triggers the example Dag on a real scheduler and reads the same list from the REST API. | Single-file `execute` returns `None`, matching upstream, so that XCom is null. |
| Native fast path | When `samestore` is true (same protocol and connection id), each object is `ObjectStoragePath.copy`, which calls `fs.copy`. A folder marker is an empty `PUT` rather than `fs.copy`, because there is no body to copy server-side. | This is same-store `fs.copy`, not a transfer service such as Storage Transfer Service. There is no hook method yet for "this backend can copy natively" beyond `samestore`. |
| OpenLineage | `get_openlineage_facets_on_start` names the source and destination paths (one dataset each: the prefix, or the single file) and does not list. `get_openlineage_facets_on_complete` names each object that was copied. Markers are included only when they were copied. Names use `s3://bucket`, `gs://bucket` (`gcs` is reported as `gs`), `abfss://container` (plus `@{account}.dfs.core.windows.net` when the account is known), and `wasbs://container` (plus `@{account}.blob.core.windows.net`). `file` matches `FileTransferOperator`. The provider is an optional import, pinned in the `aip132` extra. Unit tests assert the facets. The scheduler run emits them through the console transport. | The account host is omitted when the URI has no account and no filesystem is already attached. Deferrable copies do not emit a second complete event from a triggerer. |
| Scheduler run | `just aip132-e2e` runs Airflow 3.4.0 (`api-server`, `scheduler`, `dag-processor`, `LocalExecutor`) against S3Mock, fake-gcs-server, and Azurite, triggers `aip132_s3_prefix_to_gcs`, and checks the XCom and the console event. | Deferrable mode is not implemented, so the triggerer is not started. Real cloud accounts are not used. |

Phase 2 (paged records) and turning the existing pair operators into wrappers
are out of scope for this demo. The generic record path in
`src/generic_transfer/` is unchanged.

## OpenLineage

Start and complete answer different questions, so the operator emits both.

On start the file list is not known yet. Listing there would be a second pass
before the copy's single `find`. The start facets are the paths the task was
given: one input dataset and one output dataset. A prefix stays a prefix
(`sales/`, trailing slash kept). A single file is that file, and for `file`,
`s3`, and `gs` the namespace and name match `FileTransferOperator`.

On complete the return value is the destination keys that were copied. The
complete facets are one input and one output per key. A marker is in that
list only when `folder_markers=copy` wrote it. A key skipped because
`overwrite` is false is not. The listener sometimes runs on a different
operator instance than `execute`, so the complete hook also rebuilds those
pairs from the task return value when `keep_structure` is true.

`s3a` is reported as `s3`, and `gcs` as `gs`, which is the spec scheme.
`abfs` and `wasb` are reported as `abfss` and `wasbs` (the TLS names; the
insecure schemes are the same dataset). The account host is taken from the
URI authority `container@account....windows.net`. That text is parsed
directly: `ObjectStoragePath` would treat the container as an Airflow
connection id. `wasb` is not a filesystem `ObjectStoragePath` can construct,
so the start hook never builds one for it. A bare `abfs://container/key` has
no account in the URI, so the namespace is `abfss://container` unless an
already attached filesystem has `account_name`. ABFS uses the DFS endpoint
(`.dfs.core.windows.net`). WASB uses the blob endpoint
(`.blob.core.windows.net`). A prefix name keeps the trailing slash from the
URI the task was given. `ObjectStoragePath.key` drops that slash, so the
start hook does not read the name from the path object.

The OpenLineage provider is imported inside the hooks. It is pinned in the
`aip132` extra and is not required to import the operator.

## Helpers

`copy_files` is the function the operator calls. `list_prefix_once`,
`classify_folder_markers`, `plan_prefix_copy`, `copy_object`, `stream_copy`,
and `copy_single_file` are the smaller pieces. `examples/aip132_s3_prefix_to_gcs.py`
shows `copy_s3_prefix_to_gcs` as a `@task` body with the same arguments.
