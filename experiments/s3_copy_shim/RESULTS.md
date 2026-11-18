# S3 copy shim experiment results

Scored against the pre-registered criteria in `THRESHOLD.md`
(commit `4899d72`, written **before** any shim run or emulator start).

## Verdict

**Success.** All three unit-test criteria hold, and the S3Mock
behavioural parity check holds.

## Operators in scope

There is no `S3ToS3` module under `providers/amazon/**/transfers/`.
Same-provider copy is `S3CopyObjectOperator` and `S3CopyPrefixOperator`
in `airflow.providers.amazon.aws.operators.s3`, at Airflow SHA
`2b59699b0fe9683c418806a334b47077513bde5a`.

## Run setup

- Tests (unmodified): `providers/amazon/tests/unit/amazon/aws/operators/test_s3.py`,
  classes `TestS3CopyObjectOperator` and `TestS3CopyPrefixOperator`.
- The test file also imports `tests_common`, which is not in the
  providers-only checkout. The runner stubs those imports so the file
  can be collected; the scored copy tests do not call the stubs.
  `validate_template_fields` is loaded from the SHA amazon test utils.
- Environment: same venv as the GCS experiment
  (`apache-airflow-core==3.4.0`, `pytest==9.2.0`) plus
  `apache-airflow-providers-amazon` and `moto`. Operator modules under
  test are SHA overlays.
- Command: `just experiment-s3-copy`. Elapsed pytest time: 2.57s.
- Parity: Adobe S3Mock `3.12.0` on port 19090, started via Docker
  (`DOCKER_HOST=tcp://127.0.0.1:2375`; the default unix socket was not
  available). Only the `s3` service was required; it was stopped after
  the check. Asia/Yerevan wall time about 17:29 on 2026-11-10.

## Pass rate (criterion 1)

| Scoring | Passed | Total | Rate |
|---|---:|---:|---:|
| Pre-registered (test methods) | 16 | 16 | 100% |
| Collected pytest items | 17 | 17 | 100% |

`test_get_openlineage_facets_on_start` on the prefix operator is
parametrized 2 ways, hence 17 items. Threshold was ≥ 80% (13/16).
**16/16 ≥ 13/16.** No skips or collection errors in the scored run.

### Per-method results (all passed)

**S3CopyObjectOperator (6):** `test_s3_copy_object_arg_combination_1`,
`test_s3_copy_object_arg_combination_2`,
`test_get_openlineage_facets_on_start_combination_1`,
`test_get_openlineage_facets_on_start_combination_2`,
`test_template_fields`, `test_s3_copy_object_with_kms`.

**S3CopyPrefixOperator (10):** `test_s3_copy_prefix_basic`,
`test_s3_copy_prefix_selective_copying`, `test_s3_copy_prefix_s3_urls`,
`test_invalid_combination_bucket_with_s3_url`,
`test_s3_copy_prefix_same_bucket`, `test_s3_copy_prefix_empty_result`,
`test_continue_on_failure_false`, `test_continue_on_failure_true`,
`test_get_openlineage_facets_on_start` (2 items), `test_template_fields`.

Unlike the GCS corpus, most of these tests use moto and assert on
destination keys (behaviour), not hook-call shape. The KMS test
monkeypatches `S3Hook.copy_object` and did pass.

## Failure categories (criterion 2)

No unit-test failures, so this criterion holds vacuously.

## Constructor (criterion 3)

Both shims subclass the SHA-matched pair operator and do not define
`__init__`. Public arguments are identical: 9 named args each, plus
`AwsBaseOperator` / `BaseOperator` kwargs. No extra public args.

`S3CopyObjectOperator`: `source_bucket_key`, `dest_bucket_key`,
`source_bucket_name`, `dest_bucket_name`, `source_version_id`,
`acl_policy`, `meta_data_directive`, `kms_key_id`,
`kms_encryption_type`.

`S3CopyPrefixOperator`: `source_bucket_prefix`, `dest_bucket_prefix`,
`source_bucket_name`, `dest_bucket_name`, `kms_key_id`,
`kms_encryption_type`, `continue_on_failure`, `acl_policy`,
`meta_data_directive`.

## S3Mock parity

All four pre-registered scenarios matched on destination keys and
bytes, folder-marker handling, overwrite, and request counts
(botocore `before-call.s3` on the hook client, excluding bucket-create
and the overwrite seed `PutObject`).

| Scenario | Objects | Request counts (pair = shim) |
|---|---|---|
| single_object_copy | `path2/data_copy.txt` | CopyObject 1 |
| prefix_copy_nested | `out/a.txt`, `out/nested/b.txt` | CopyObject 2, ListObjectsV2 1 |
| overwrite_existing_dest | dest bytes become the source | CopyObject 1 |
| folder_marker_under_prefix | `out/dir/` and `out/file.txt` both copied | CopyObject 2, ListObjectsV2 1 |

## Caveats

1. Listing and destination-prefix remapping for `S3CopyPrefixOperator`
   follow the pair (paginator + prefix string splice). The generic
   piece is the copy loop and `S3Hook.copy_object` as
   `server_side_copy`.
2. `S3CopyObjectOperator.execute` still issues one positional
   `copy_object` call so s3:// vs bucket+key combinations and the KMS
   monkeypatch keep the pair's hook-call shape.
3. Constructor identity is from subclassing.
4. Unit tests ran against moto, not S3Mock; parity ran against S3Mock.

## Recommendation

Against `THRESHOLD.md` the experiment **succeeds**, so it does not
trigger the pre-registered "fail → option (c)" recommendation.
