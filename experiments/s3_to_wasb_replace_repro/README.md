# Repro: `S3ToAzureBlobStorageOperator` `replace=False` does not skip existing dest blobs

Local-only investigation. This directory is not an upstream PR. The patch is
**not** applied to `airflow-src` or any remote.

Confirmed on `apache/airflow` main `057f009928112df519ed6fc80f54d675857c4b9d`
(operator file unchanged since `2b59699b0fe9683c418806a334b47077513bde5a`).

## What is wrong

`get_files_to_move()` builds `files_to_move` as names **relative to
`s3_prefix`** (e.g. `a.txt` from `pre/a.txt`). When `replace=False` and
`blob_prefix` is set, it lists dest blobs with
`WasbHook.get_blobs_list_recursive`, which returns **full blob names** (e.g.
`out/a.txt`). It then subtracts the two sets:

```python
# providers/microsoft/azure/src/airflow/providers/microsoft/azure/transfers/s3_to_wasb.py
# lines 232 and 260-269 on main (057f0099)
files_to_move = [s3_key.replace(f"{self.s3_prefix}/", "", 1) for s3_key in s3_keys]
# ...
azure_blob_files += self.wasb_hook.get_blobs_list_recursive(
    container_name=self.container_name, prefix=self.blob_prefix
)
existing_files = azure_blob_files if azure_blob_files else []
files_to_move = list(set(files_to_move) - set(existing_files))
```

`{"a.txt"} - {"out/a.txt"}` is still `{"a.txt"}`, so the existing dest blob
is not skipped. `move_file()` then writes `out/a.txt` via
`_create_key(None, "out", "a.txt")`.

Current upstream unit tests mock `get_blobs_list_recursive` with *relative*
names (`MOCK_FILES`), so they do not catch this.

## How to run

From the repo root, with `AIRFLOW_SRC` pointing at an Airflow checkout (default
`/workspace/airflow-src`) and the experiment venv that has the Azure and Amazon
providers:

```bash
# Mocked-hook unit check + fails-before / passes-after (no emulators)
experiments/gcs_to_gcs_shim/.venv/bin/python \
  experiments/s3_to_wasb_replace_repro/repro.py --unit --before-after

# Optional: S3Mock + Azurite (starts nothing by itself; start emulators first)
just up   # or: docker compose up -d s3 azure
experiments/gcs_to_gcs_shim/.venv/bin/python \
  experiments/s3_to_wasb_replace_repro/repro.py --emulator
just down # stop emulators when finished
```

The proposed unit test (same assertion as `--before-after`) lives in
`test_replace_false_filter.py`. The local fix is
`s3_to_wasb_replace_false.patch` (do not apply to this demo's tree). A
paste-ready GitHub issue draft is `ISSUE.md`. This issue was **not** filed.


## Observed results (this run)

Airflow SHA `057f009928112df519ed6fc80f54d675857c4b9d`. Bug **confirmed** in the filter.

Mocked-hook unit + in-memory before/after (`repro.py --unit --before-after`):

- Unpatched `get_files_to_move()` = `['a.txt']` (should have been `[]`).
- After applying `s3_to_wasb_replace_false.patch` in memory (airflow-src untouched): `[]`.

S3Mock + Azurite (`repro.py --emulator`), source `pre/a.txt` = `NEW-FROM-S3`, dest `out/a.txt` pre-created as `OLD-DEST-BYTES`, `replace=False`:

- Before: text=`OLD-DEST-BYTES`, etag=`"0x1C90C581C3ADC70"`, last-modified=`2026-11-10T15:17:27+00:00` (19:17:27 Asia/Yerevan).
- `get_files_to_move()` = `['a.txt']` (filter did not skip).
- `execute()` then called `wasb_hook.load_file` / `upload_blob` for `out/a.txt` and raised `azure.core.exceptions.ResourceExistsError: The specified blob already exists.` (RequestId `24cedd6b-3bc4-41d6-8700-97e2b35c141a`, Time `2026-11-10T15:17:28.351Z` = 19:17:28 Asia/Yerevan). Azure SDK default is `overwrite=False`; this run did not pass `overwrite=True` in `wasb_extra_args`.
- After: dest unchanged — same text, etag, and last-modified.

So the comparison bug is real (existing dest is still selected for transfer). Silent overwrite is *not* the default hook/SDK path; it depends on `wasb_extra_args` setting `overwrite=True`. An earlier all-pairs facade used `overwrite=True` and therefore changed dest bytes. This repro uses the real hooks.

## Search (read-only)

No open or closed apache/airflow issue described this `replace=False`
full-name vs relative-name comparison. Related closed work on the same
operator is about folder markers and TLS, not this filter:

- https://github.com/apache/airflow/pull/76408 — Fix S3ToAzureBlobStorageOperator failing on S3 folder markers (merged)
- https://github.com/apache/airflow/pull/76222 — Verify S3 TLS certificates by default (merged)
