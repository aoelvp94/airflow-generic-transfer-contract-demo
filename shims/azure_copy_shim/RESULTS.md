# Azure closest-transfer shim experiment results

Scored against the pre-registered criteria in `THRESHOLD.md`
(commit `4899d72`, written **before** any shim run or emulator start).

## Verdict

**Success** on both scored corpora and on the Azurite (plus S3Mock)
parity check. The finding that **no same-provider wasb→wasb or
ADLS→ADLS copy operator exists** at this SHA is unchanged and is
not scored as a test failure.

## What exists (verified)

Airflow SHA `2b59699b0fe9683c418806a334b47077513bde5a`. No
`WasbToWasb`, `ADLSToADLS`, or Azure blob copy operator in
`providers/microsoft/azure` transfers or operators. Closest operators
used, as pre-registered:

- Azure-only: `LocalFilesystemToWasbOperator` (2 test methods).
- Closest multi-object transfer writing Azure blob:
  `S3ToAzureBlobStorageOperator` (10 test methods).

## Run setup

- Tests unmodified, SHA operator modules overlaid, same venv as the
  GCS experiment plus `apache-airflow-providers-microsoft-azure` and
  `apache-airflow-providers-amazon`.
- Command: `just experiment-azure-copy`.
- Parity: Azurite `3.37.0` on port 10000 and S3Mock `3.12.0` on port
  19090 (needed only for S3→Wasb). Started via Docker
  (`DOCKER_HOST=tcp://127.0.0.1:2375`). Only `s3` and `azure` were
  started; both were stopped afterwards. Asia/Yerevan wall time about
  17:29 on 2026-11-10.

## Pass rate (criterion 1), per corpus

| Corpus | Methods passed | Bar | Items passed |
|---|---|---|---|
| LocalFilesystemToWasb | **2/2 (100%)** | 2/2 | 3/3 (`test_execute` is parametrized True/False for `create_container`) |
| S3ToAzureBlobStorage | **10/10 (100%)** | 8/10 | 25/25 |

No skips or collection errors. Both corpora clear the bar.

`test_execute` for Local→Wasb asserts `WasbHook.load_file` was called
with the file, container, blob, `create_container`, and
`timeout=2` from `load_options` — hook-call-shape coupling. It passed
because the writer delegates to that method with the same keywords.

Several S3→Wasb tests patch `S3Hook` and `WasbHook` on the transfer
module and also call `get_files_to_move` / `move_file` /
`_create_key` / `_strip_overlapping_folder_markers` directly. Those
methods stay on the upstream base class; `execute()` routes the copy
through the contract-style temp-file copier that still calls
`download_file` and `load_file`.

## Failure categories (criterion 2)

No unit-test failures in either corpus, so this criterion holds
vacuously.

## Constructor (criterion 3)

Both shims subclass the pair and do not define `__init__`. No extra
public arguments.

LocalFilesystemToWasb (6): `file_path`, `container_name`, `blob_name`,
`wasb_conn_id`, `create_container`, `load_options`.

S3ToAzureBlobStorage (13): `aws_conn_id`, `wasb_conn_id`, `s3_bucket`,
`container_name`, `s3_prefix`, `s3_key`, `blob_prefix`, `blob_name`,
`create_container`, `replace`, `s3_verify`, `s3_extra_args`,
`wasb_extra_args`.

## Azurite / S3Mock parity

All five pre-registered scenarios matched on destination keys and
bytes, and on request counts as measured. Two of the five also
matched on a **shared pair behaviour**: the hook raises
`ResourceExistsError` when overwrite is false or when
`replace=False` does not skip a dest blob before `load_file`
(which does not pass `overwrite=True` by itself).

| Scenario | Objects | Request counts | Errors (pair = shim) |
|---|---|---|---|
| local_upload_bytes | `one/blob.txt` | PUT 1, load_file 1 | none |
| local_overwrite_true | dest becomes `newer-azurite` | PUT 1, load_file 1 (second upload only) | none |
| local_overwrite_false | dest stays `hello-azurite` | `{}` (see caveat) | ResourceExistsError |
| s3_to_wasb_prefix_and_marker | `out/a.txt`, `out/nested/b.txt`, `out/dir/` | PUT 3, load_file 3, s3.GetObject 3, s3.HeadObject 3, s3.ListObjectsV2 1 | none |
| s3_to_wasb_replace_false | `out/a.txt`, `out/nested/b.txt` after a failed run | `{}` (see caveat) | ResourceExistsError |

The folder marker `pre/dir/` was copied to `out/dir/` when
`replace=True`, same as the pair. Request counts for the two
`ResourceExistsError` runs were reset to empty in the harness
`except` handler, so they match vacuously; dest state and exception
type still matched.

## Caveats

1. This is **not** a wasb→wasb experiment. Azure has no same-provider
   file→file copy operator at this SHA.
2. Local→Wasb is a single-file upload, not a listing+copy path.
   S3→Wasb is cross-cloud and uses download + `load_file`, not
   server-side copy.
3. Listing / replace / marker stripping for S3→Wasb stay on the
   inherited pair methods. The shim rewires `execute()` to a
   contract-style copier that still uses those hook calls.
4. Constructor identity is from subclassing.
5. `replace=False` on S3→Wasb still raised `ResourceExistsError` on
   **both** pair and shim against Azurite (existing dest blob
   `out/a.txt` was not skipped before upload). That is pair behaviour
   observed in this harness, not a shim-only defect.
6. Request-count instrumentation mixes Azure HTTP methods, hook
   methods, and boto3 events. The successful transfer scenarios
   matched at that granularity.

## Recommendation

Against `THRESHOLD.md` the experiment **succeeds**, so it does not
trigger the pre-registered "fail → option (c)" recommendation.

The missing same-provider Azure copy operator remains a design fact:
there is no GCSToGCS / S3CopyPrefix equivalent to genericise on the
Azure side. Option (c) (thin pair operators + shared helpers + a
conformance suite) is still the natural way to introduce a wasb→wasb
path later, rather than claiming one already exists.
