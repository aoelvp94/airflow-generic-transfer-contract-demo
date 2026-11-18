# Results: one generic file-transfer operator on all 9 pairs

Scored against `THRESHOLD.md` (commit `2aca97b9abf42cb0c6c4a9c5396bec356b597ea9`), written and committed before this implementation or any emulator run.

Airflow SHA for upstream references:
`2b59699b0fe9683c418806a334b47077513bde5a`.

Emulators: Adobe S3Mock 3.12.0 (`:19090`), fsouza/fake-gcs-server (`:14443`), Azurite 3.37.0 (`:10000`). The harness started all three, then stopped them; nothing was left running.

The operator under test is `GenericFileTransferOperator` in this directory. It is not a subclass of any pair operator and does not import pair listing helpers. Per-pair `marker_policy` and `path_layout` are selected from `pair_policy.py` so the generic planner can be compared with each reference's default dest-key and marker behaviour.

## Verdict: **fail → option (c)**

| Criterion | Result |
|---|---|
| (a) Parity on all 9 pairs for core scenarios | **Fail.** 34/35 scored cells passed. The failing cell is `s3→azure` `replace_false`: dest keys match, but `out/a.txt` bytes do not. |
| (b) Generic constructor ≤ 18 named args | **Pass.** The generic operator has **16** named arguments. |
| (c) Wrapper tests | Scored, not the bar. Hook-call-shape failures do not fail the experiment (pre-registered). The 10 items the classifier labelled `behavior_difference` are pair-only constructor/validation/STS behaviour (see below), not dest-byte disagreements. |

Because (a) failed, the recommendation is **option (c): keep thin pair operators, extract shared hook/helpers, and enforce a conformance suite**.

The one dest-byte miss is exactly the kind of pair-specific `replace=False` comparison the suite would have to pin down: `S3ToAzureBlobStorageOperator` subtracts the dest listing (full blob names under `blob_prefix`) from *relative* source names, so an existing `out/a.txt` is not recognized as the mapped dest of `pre/a.txt`. The generic operator calls `exists` on the planned dest key and skips it. After the run, the reference dest held the source bytes; the generic dest still held the pre-seeded `OLD-BYTES`.

## 9× scenario matrix

`PASS*` means objects, markers, and error classes matched, and a request-count difference was explained by a documented policy (see the request-count table). `SKIP` means the reference has no `replace` flag (pre-registered as not scored). `n/a` means the scenario is not scored for that pair (move only for `GCSToGCSOperator`; single-blob only for `AzureBlobStorageToGCSOperator`).

| Pair | Reference | prefix_copy | folder_marker | overwrite_true | replace_false | move | single_blob |
|---|---|---|---|---|---|---|---|
| s3→s3 | `S3CopyPrefixOperator` | PASS | PASS | PASS | SKIP | n/a | n/a |
| gcs→gcs | `GCSToGCSOperator` | PASS | PASS | PASS | PASS* | PASS | n/a |
| azure→azure | `naive_pair_copy` | PASS* | PASS* | PASS* | SKIP | n/a | n/a |
| s3→gcs | `S3ToGCSOperator` | PASS | PASS | PASS | PASS* | n/a | n/a |
| gcs→s3 | `GCSToS3Operator` | PASS | PASS | PASS | PASS* | n/a | n/a |
| s3→azure | `S3ToAzureBlobStorageOperator` | PASS* | PASS* | PASS* | **FAIL** | n/a | n/a |
| azure→s3 | `AzureBlobStorageToS3Operator` | PASS | PASS | PASS | PASS* | n/a | n/a |
| gcs→azure | `GCSToAzureBlobStorageOperator` | PASS | PASS | PASS | PASS* | n/a | n/a |
| azure→gcs | `naive_pair_copy` / `AzureBlobStorageToGCSOperator` | PASS | PASS | PASS | SKIP | n/a | PASS* |

Scored cells: **34/35 passed**, 1 failed, 3 skipped (not scored).

`gcs→azure` prefix/folder/overwrite correctly omit trailing-slash markers (`marker_policy=skip`, matching `_should_skip_gcs_object`). The other eight prefix copies keep the `pre/dir/` marker under the dest prefix, matching their references.

Path layout follows each reference's default: `preserve` (strip source prefix) for s3→s3, gcs→gcs, azure→azure, s3→azure, and azure→gcs prefix; `join_source_key` (dest prefix + full source key) for s3→gcs (`apply_gcs_prefix=False`), gcs→s3, azure→s3, and gcs→azure.

## Request-count comparison (prefix_copy, and notable diffs)

Exact match unless noted.

| Pair | Ref counts | Generic counts | Notes |
|---|---|---|---|
| s3→s3 | `CopyObject` 3, `ListObjectsV2` 1 | same | exact |
| gcs→gcs | `GET` 1, `POST` 3 | same | list + 3 rewrites |
| azure→azure | `GET` 5, `PUT` 3 | `GET` 1, `PUT` 3 | explained: generic `server_side_copy` (`start_copy_from_url`) vs naive get+put |
| s3→gcs | `ListObjectsV2` 1, `GetObject` 3, `gcs.POST` 3 | same | exact |
| gcs→s3 | `gcs.GET` 4, `PutObject` 3 | same | list + 3 downloads + 3 puts (GET total 4) |
| s3→azure | `ListObjectsV2` 1, `HeadObject` 3, `GetObject` 3, `azure.PUT` 3 | `ListObjectsV2` 1, `GetObject` 3, `azure.PUT` 3 | explained: pair `download_file` issues Head before Get; contract `read_bytes` does not |
| azure→s3 | `azure.GET` 5, `PutObject` 3 | same | exact |
| gcs→azure | `gcs.GET` 3, `azure.PUT` 2 | same | list + 2 files (markers skipped) |
| azure→gcs prefix | `azure.GET` 5, `gcs.POST` 3 | same | exact |
| azure→gcs single_blob | `azure.GET` 1, `gcs.POST` 1 | `azure.GET` 2, `gcs.POST` 1 | explained: generic always lists, even on `exact_match`; the single-blob operator gets one blob by name |

`replace=False` cells that passed on objects also differed in existence-check shape: the generic operator calls `ObjectExists.exists` per planned dest key; pair operators typically list the dest prefix once. Write/copy counts matched; those count diffs are tagged `replace=False dest check: per-key exists vs dest-prefix list`.

## Constructor-arg comparison

Generic named args (**16**, cap 18):

`source_backend`, `dest_backend`, `source_bucket`, `dest_bucket`, `source_prefix`, `dest_prefix`, `replace`, `move`, `marker_policy`, `path_layout`, `match_glob`, `exact_match`, `server_side_copy`, `last_modified_time`, `maximum_modified_time`, `is_older_than`.

`task_id` / `BaseOperator` / `**kwargs` are not counted, per the threshold.

| Pair operator | Named args | Pair-specific args with **no generic equivalent** |
|---|---:|---|
| `GCSToGCSOperator` (largest) | 18 | `source_objects`, `delimiter`, `source_object_required`, `retain_until_time`, `retention_mode`; plus conn/auth: `gcp_conn_id`, `impersonation_chain` |
| `S3CopyPrefixOperator` | 9 | `kms_key_id`, `kms_encryption_type`, `continue_on_failure`, `acl_policy`, `meta_data_directive` |
| `S3CopyObjectOperator` | 9 | `source_version_id`, `acl_policy`, `meta_data_directive`, `kms_key_id`, `kms_encryption_type` |
| `S3ToGCSOperator` | 14 | `delimiter`, `gzip`, `deferrable`, `poll_interval`, `return_gcs_uris`; plus conn/auth: `aws_conn_id`, `gcp_conn_id`, `verify`, `google_impersonation_chain` |
| `GCSToS3Operator` | 14 | conn/auth only: `gcp_conn_id`, `dest_aws_conn_id`, `dest_verify`, `google_impersonation_chain`, `dest_s3_extra_args`, `s3_acl_policy`, `gcp_user_project` (`keep_directory_structure` / `flatten_structure` map to `path_layout`) |
| `S3ToAzureBlobStorageOperator` | 13 | `create_container`; plus conn/auth: `aws_conn_id`, `wasb_conn_id`, `s3_verify`, `s3_extra_args`, `wasb_extra_args` |
| `AzureBlobStorageToS3Operator` | 12 | `delimiter`; plus conn/auth: `wasb_conn_id`, `aws_conn_id`, `dest_verify`, `dest_s3_extra_args`, `s3_acl_policy`, `wasb_extra_args`, `s3_extra_args` |
| `GCSToAzureBlobStorageOperator` | 13 | `create_container`; plus conn/auth: `gcp_conn_id`, `google_impersonation_chain`, `wasb_conn_id`, `gcp_user_project` |
| `AzureBlobStorageToGCSOperator` | 10 | `filename`, `gzip`, `unwrap_single`; plus conn/auth: `wasb_conn_id`, `gcp_conn_id`, `impersonation_chain` |

Backends carry credentials, so conn ids are intentionally absent from the generic constructor. That is a smaller shared API, not extra generic args.

## Upstream tests via the thin wrapper

The wrapper maps pair constructor args onto `GenericFileTransferOperator`. It does not subclass the pair class, does not call `hook.list` / `hook.rewrite` / `copy_object`, and therefore does not satisfy hook-shape assertions. Tests were not edited. Collection used the SHA-matched operator modules from `airflow-src`, with `providers/*/tests` on `sys.path` so `unit.*` imports resolve, and a stub for `time_machine` (not installed in the experiment venv).

JUnit item counts are parametrized tests; they can exceed the pre-registered method counts in `THRESHOLD.md`.

| Corpus | Pre-registered methods | Items collected | Passed | Pass rate (items) | Categories of non-passes |
|---|---:|---:|---:|---|---|
| `GCSToGCSOperator` | 45 | 81 | 1 | 1/81 | hook_call_shape 79, missing_capability 1 |
| `S3CopyObject` + `S3CopyPrefix` | 16 | 17 | 0 | 0/17 | hook_call_shape 9, missing_capability 8 |
| `S3ToGCSOperator` | 27 | 69 | 2 | 2/69 | hook_call_shape 11, missing_capability 49, behavior_difference 7 |
| `GCSToS3Operator` | 20 | 35 | 0 | 0/35 | hook_call_shape 15, missing_capability 18, behavior_difference 2 |
| `S3ToAzureBlobStorageOperator` | 10 | 25 | 0 | 0/25 | hook_call_shape 1, missing_capability 24 |
| `AzureBlobStorageToS3Operator` | 7 | 8 | 0 | 0/8 | hook_call_shape 8 |
| `GCSToAzureBlobStorageOperator` | 10 | 10 | 0 | 0/10 | hook_call_shape 7, missing_capability 2, behavior_difference 1 |
| `AzureBlobStorageToGCSOperator` | 3 | 4 | 1 | 1/4 | hook_call_shape 2, missing_capability 1 |
| **Total** | **138** | **249** | **4** | **4/249** | hook_call_shape 132, missing_capability 103, behavior_difference 10, env_issue 0 |

There is no azure→azure upstream corpus.

The 10 `behavior_difference` items, on inspection, are pair-only API checks the generic wrapper does not implement: `S3ToGCSOperator` `return_gcs_uris` deprecation warnings (3), `poll_interval` validation (2), and Cloud Storage Transfer Service `submit_transfer_jobs` (2); `GCSToS3Operator` / `GCSToAzureBlobStorageOperator` `match_glob` rejected on an older google provider (3). Those args have no generic equivalent (`deferrable`, `poll_interval`, `return_gcs_uris`, and a provider-version gate around `match_glob`). They are not dest-key or byte disagreements.

Passing items were constructor/`__init__` cases (and one GCS delete-path test) that never required hook-call shape.

Hook-call-shape failures were pre-registered as not failing the experiment.

## Caveats

- Emulators are not production S3 / GCS / Azure. Request shapes, listing, and copy semantics can differ on the real clouds. This was pre-registered, not a post-hoc excuse.
- Wire counts are SDK/HTTP calls observed on the instrumented clients (botocore `before-call.s3`, GCS session `request`, Azure pipeline policy). They are not CloudTrail / audit-log equivalents.
- For azure→azure and azure→gcs prefix, the naive reference's stock `list_keys_native` built a new Azure client for the list call, which evaded the wire counter. The harness patched listing to reuse the cached client so list+get counts are comparable. That is instrumentation, not a change to copy behaviour.
- `S3ToGCSOperator` was run with `deferrable=False` and its default `apply_gcs_prefix=False`. GCS `*` wildcard split and deprecated `delimiter` were not ported (pre-registered policy).
- The experiment venv has no `s3fs`/`gcsfs`; bucket setup uses native SDKs (`emu_native.py`).
- No real cloud accounts were used. No remotes were written.

## How to rerun

```bash
just experiment-all-pairs
```

Starts only emulators that are down, runs `parity.py` then `run_wrappers.py`, and stops only what it started.
