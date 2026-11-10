# S3ToAzureBlobStorageOperator replace=False does not skip existing blobs under blob_prefix

Paste into a new Apache Airflow issue using the "Airflow Issue report" form.
Do not file this automatically. Labels from the template: `kind:bug`, `needs-triage`.

## Under which category would you file this issue?

Providers

## Apache Airflow version

Reproduced on `main` at `057f009928112df519ed6fc80f54d675857c4b9d` (2026-11-10). The operator file `providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py` is unchanged since `2b59699b0fe9683c418806a334b47077513bde5a`. Please confirm on the latest `main` or the current `microsoft-azure` provider before treating this as stale.

## What happened and how to reproduce it?

**Issue Description**

`S3ToAzureBlobStorageOperator(replace=False, s3_prefix=..., blob_prefix=...)` is documented as skipping destination blobs that already exist. In `get_files_to_move()`, source keys are stripped to names *relative to `s3_prefix`* (for example `pre/a.txt` becomes `a.txt`), but the destination listing from `WasbHook.get_blobs_list_recursive(prefix=blob_prefix)` returns *full* blob names (`out/a.txt`). The operator then does a set subtraction of those two lists, which never matches, so the existing dest blob stays in `files_to_move` and `move_file()` still transfers it.

Relevant lines in `providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py` on that SHA:

```python
# line 232
files_to_move = [s3_key.replace(f"{self.s3_prefix}/", "", 1) for s3_key in s3_keys]
# lines 260-269
azure_blob_files += self.wasb_hook.get_blobs_list_recursive(
    container_name=self.container_name, prefix=self.blob_prefix
)
existing_files = azure_blob_files if azure_blob_files else []
files_to_move = list(set(files_to_move) - set(existing_files))
```

`WasbHook.get_blobs_list_recursive` appends `blob.name` (full name). Destination keys are later built as `f"{blob_prefix}/{file_name}"` in `_create_key` (lines 300-305).

Existing unit tests in `test_s3_to_wasb.py` mock `get_blobs_list_recursive` with relative names (`MOCK_FILES`), so they do not catch this.

**Steps to reproduce**

1. Place `s3://src-bucket/pre/a.txt` with body `NEW-FROM-S3`.
2. Pre-create Azure blob `out/a.txt` in the target container with different body `OLD-DEST-BYTES`.
3. Run:

```python
S3ToAzureBlobStorageOperator(
    task_id="repro",
    s3_bucket="src-bucket",
    s3_prefix="pre",
    container_name="dst-container",
    blob_prefix="out",
    replace=False,
).execute(context)
```

4. Observe `get_files_to_move()` returning `['a.txt']` instead of `[]`. The operator then attempts to write `out/a.txt`. Whether the dest content is silently overwritten or `upload_blob` raises (Azure SDK default `overwrite=False` unless `wasb_extra_args` sets `overwrite=True`) depends on the hook/SDK, but the skip filter has already failed.

A mocked-hook unit test and a local S3Mock + Azurite repro are in the companion notes; this issue is filed from a local investigation only.

Local emulator observation on this SHA (S3Mock + Azurite, real hooks, no `wasb_extra_args` overwrite): `get_files_to_move()` returned `['a.txt']`; `execute()` then raised `ResourceExistsError` (`BlobAlreadyExists`, RequestId `24cedd6b-3bc4-41d6-8700-97e2b35c141a`, Time `2026-11-10T15:17:28.351Z`). Destination `out/a.txt` stayed `OLD-DEST-BYTES` with etag `"0x1C90C581C3ADC70"` and last-modified `2026-11-10T15:17:27+00:00` (19:17:27 Asia/Yerevan) before and after. The skip filter failed; dest bytes were not overwritten on the default `upload_blob(overwrite=False)` path.


## What you think should happen instead?

When `replace=False`, a destination blob that already exists at the mapped path (`{blob_prefix}/{relative-source-name}`) should be skipped. `get_files_to_move()` should return an empty list in the scenario above, `execute()` should not write the dest blob, and dest bytes / ETag / last-modified should stay unchanged.

The comparison should use the same dest key `_create_key(blob_name, blob_prefix, file_name)` that `move_file()` writes, or strip `blob_prefix` from the listing before subtracting.

## Operating System

Linux (reproduced against local S3Mock and Azurite, and with mocked hooks). Not deployment-specific.

## Deployment

Virtualenv installation

## Apache Airflow Provider(s)

- amazon
- microsoft-azure

## Versions of Apache Airflow Providers

```
apache-airflow-providers-microsoft-azure==15.3.1
```

(the in-tree operator on `main` at the SHA above; `s3_to_wasb.py` matches `2b59699b`).

## Official Helm Chart version

Not Applicable

## Kubernetes Version

Not Applicable

## Helm Chart configuration

Not Applicable

## Docker Image customizations

Not Applicable

## Anything else?

Reproduced every time with `s3_prefix` + `blob_prefix` and a dest blob already at `{blob_prefix}/{relative}`. The `s3_key` + `blob_name` path compares basenames and is a separate case; this report is about the prefix listing path.

Read-only search of `apache/airflow` issues and PRs mentioning `S3ToAzureBlobStorageOperator` and `replace` did not find an existing report of this comparison bug. Closest related work (folder markers, TLS), not this filter:

- https://github.com/apache/airflow/pull/76408
- https://github.com/apache/airflow/pull/76222

**AI assistance:** This investigation and draft were prepared with assistance from a coding agent (Grok Bot). A follow-up pull request, if submitted, will include the project's Gen-AI disclosure in the PR description as required by [contributing-docs/05_pull_requests.rst](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions) (`Generated-by` in the PR template). The issue form itself has no AI-disclosure field.

## Are you willing to submit PR?

Yes I am willing to submit a PR!

## Code of Conduct

I agree to follow this project's [Code of Conduct](https://github.com/apache/airflow/blob/main/CODE_OF_CONDUCT.md)
