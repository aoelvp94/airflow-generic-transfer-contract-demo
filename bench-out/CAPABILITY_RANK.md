# Capability ranking (primary `needs_capability` operators)

Source: `/workspace/airflow-generic-transfer-contract-demo/bench-out/PAIR_AUDIT.csv`. Stdlib only. 43 concrete operators in the
`needs_capability` bucket. An operator is *unlocked* when every capability it
lists is in the selected set.

**9** of these operators list **≥ 4** capabilities.

## Sole-blocker

Operators that need exactly one capability (adding that one would unlock them).

| Capability | Operators unlocked | Examples |
|---|---:|---|
| `compression` | 7 | `GlacierToGCSOperator`, `GoogleAdsToGcsOperator`, `AzureBlobStorageToGCSOperator`, `FacebookAdsReportToGcsOperator`, `HttpToGCSOperator`, … (+2) |
| `replace` | 4 | `ImapAttachmentToS3Operator`, `FileTransferOperator`, `BigQueryToMsSqlOperator`, `BigQueryToMySqlOperator` |
| `bulk_load` | 2 | `HiveToMySqlOperator`, `VerticaToMySqlOperator` |
| `move_semantics` | 2 | `GCSToGoogleDriveOperator`, `LocalFilesystemToGoogleDriveOperator` |
| `schema_sidecar` | 2 | `GCSToPrestoOperator`, `GCSToTrinoOperator` |
| `xcom_side_channel` | 1 | `GCSToLocalFilesystemOperator` |
| `acl_or_extra_args` | 1 | `LocalFilesystemToWasbOperator` |

Total sole-blocker operators: **19**.

## Total-any

How many `needs_capability` operators list each capability at all.

| Capability | Operators |
|---|---:|
| `replace` | 24 |
| `compression` | 17 |
| `acl_or_extra_args` | 12 |
| `wildcard_or_prefix` | 8 |
| `move_semantics` | 7 |
| `dest_encryption` | 6 |
| `path_layout` | 5 |
| `bulk_load` | 3 |
| `pagination` | 2 |
| `xcom_side_channel` | 2 |
| `upsert` | 2 |
| `schema_sidecar` | 2 |
| `incremental_filter` | 1 |
| `object_retention` | 1 |
| `deferrable` | 1 |
| `server_side_copy` | 1 |

## Greedy set-cover order

At each step pick the unused capability that unlocks the most remaining operators.
Ties break alphabetically.

| Step | Capability | Newly unlocked | Cumulative unlocked | Remaining |
|---:|---|---:|---:|---:|
| 1 | `compression` | 7 | 7 | 36 |
| 2 | `replace` | 6 | 13 | 30 |
| 3 | `move_semantics` | 3 | 16 | 27 |
| 4 | `acl_or_extra_args` | 2 | 18 | 25 |
| 5 | `dest_encryption` | 6 | 24 | 19 |
| 6 | `wildcard_or_prefix` | 4 | 28 | 15 |
| 7 | `path_layout` | 4 | 32 | 11 |
| 8 | `bulk_load` | 2 | 34 | 9 |
| 9 | `schema_sidecar` | 2 | 36 | 7 |
| 10 | `upsert` | 2 | 38 | 5 |
| 11 | `pagination` | 1 | 39 | 4 |
| 12 | `xcom_side_channel` | 2 | 41 | 2 |
| 13 | `deferrable` | 0 | 41 | 2 |
| 14 | `server_side_copy` | 1 | 42 | 1 |
| 15 | `incremental_filter` | 0 | 42 | 1 |
| 16 | `object_retention` | 1 | 43 | 0 |

