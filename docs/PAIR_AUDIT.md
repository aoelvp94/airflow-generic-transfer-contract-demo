# Pair-mapping audit of Airflow transfer operators

Answers: **what fraction of Airflow's per-pair transfer operators map cleanly
onto a generic source-hook / destination-hook operator?**

This is evidence for the Apache Airflow dev-list thread by David Blain,
`[DISCUSS] One transfer operator on hook contracts instead of one operator
per source/destination pair` (claim: ~86 transfer modules across 13
providers). It is a static AST heuristic, not a final AIP decision.

Reproduce:

```bash
# Shallow sparse clone of apache/airflow (providers/ only), then:
just audit
# or: .venv/bin/python scripts/audit_transfers.py --airflow-src /workspace/airflow-src
```

## Corpus

- **Airflow commit:** `2b59699b0fe9683c418806a334b47077513bde5a`
- **Commit subject:** Rework fork CI draft and label rules into one state table (#76681)
- **Commit date:** 2026-11-10T13:32:23+01:00 (git author date, with original offset)
- **Scan root:** `/workspace/airflow-src/providers`
- **Modules under `providers/**/transfers/`** (excluding `__init__.py` and tests): **87**
- **Provider packages with a `transfers/` tree:** **13** — `amazon`, `apache.druid`, `apache.hive`, `google`, `microsoft.azure`, `mysql`, `oracle`, `presto`, `samba`, `slack`, `snowflake`, `teradata`, `trino`
- **Also included:** `common/io/src/airflow/providers/common/io/operators/file_transfer.py` (the generic file operator the thread already discusses; not counted in the 13 providers above)
- **Operator classes found:** 88 (84 concrete, 4 base classes)

### Match to the ~86 / 13 claim

This snapshot has **87** modules under `transfers/` across **13** provider packages. That is the same 13 providers as the claim if `apache.hive` and `apache.druid` are counted separately and `common.io` is not (FileTransferOperator lives there, not under `transfers/`).

The count is **one above** the ~86 claim. The extra module is `amazon/aws/transfers/base.py` (`AwsToAwsBaseOperator` only — not a source/destination pair). Excluding that base module leaves **86** pair-transfer modules, which matches the claim. Three other `transfers/` modules hold only a base class and stay inside that 86: `google/src/airflow/providers/google/cloud/transfers/bigquery_to_sql.py`, `google/src/airflow/providers/google/cloud/transfers/sql_to_gcs.py`, `slack/src/airflow/providers/slack/transfers/base_sql_to_slack.py`. We did not invent a historical count; this is what `2b59699b0fe9` contains.

Skipped `*_to_*.py` modules that are not data transfers:

- `edge3/src/airflow/providers/edge3/migrations/versions/0002_3_2_0_add_concurrency_to_edge_worker.py` — name matches *_to_* but is not a data transfer
- `edge3/src/airflow/providers/edge3/migrations/versions/0006_5_0_0_add_task_instance_id_to_edge_job.py` — name matches *_to_* but is not a data transfer
- `edge3/src/airflow/providers/edge3/migrations/versions/0007_5_1_0_add_priority_weight_to_edge_job.py` — name matches *_to_* but is not a data transfer
- `google/src/airflow/providers/google/cloud/hooks/speech_to_text.py` — name matches *_to_* but is not a data transfer
- `google/src/airflow/providers/google/cloud/hooks/text_to_speech.py` — name matches *_to_* but is not a data transfer
- `google/src/airflow/providers/google/cloud/operators/speech_to_text.py` — name matches *_to_* but is not a data transfer
- `google/src/airflow/providers/google/cloud/operators/text_to_speech.py` — name matches *_to_* but is not a data transfer

Out of scope (not under `transfers/`, not `FileTransferOperator`, not a data-transfer `*_to_*.py`):

- Same-bucket copy/admin operators such as `S3CopyObjectOperator`.
- Google Cloud Storage Transfer Service *job* operators under `operators/cloud_storage_transfer_service.py`.
- Databricks `COPY INTO` operators, Kafka produce/consume, speech-to-text.

## Headline (concrete operators only)

Base classes (`AwsToAwsBaseOperator`, `BaseSQLToGCSOperator`, `BigQueryToSqlBaseOperator`,
`BaseSqlToSlackOperator`) are parsed and classified but **excluded from the percentages**
so a pair is not counted twice via its base.

| Classification | Concrete operators | Share of 84 |
|---|---:|---:|
| `maps_cleanly` | 15 | 17.9% |
| `needs_capability` | 43 | 51.2% |
| `special` | 26 | 31.0% |
| **Total** | **84** | **100%** |

**17.9%** map cleanly onto source-conn + source-location, dest-conn + dest-location, and generic format/batch options (no listing, overwrite, or other capabilities). **51.2%** would map if the contract grows optional capabilities (listing, overwrite, compression, ACL, server-side copy, schema sidecar, move, pagination, deferrable, …). **31.0%** carry *core* semantics a generic operator should not absorb (partitioning / dest-table DDL / warehouse COPY-UNLOAD-extract jobs / required custom callables / Slack notifications).

**58 / 84 (69.0%)** are `maps_cleanly` or `needs_capability` — they would map onto a generic operator once optional capabilities exist. Of 28 file→file concrete operators, 4 map cleanly, 24 need a file-contract capability (compression, ACL, server-side copy, move, path layout, …), and 0 are special.

### Conservative view (optional special flags still force `special`)

The primary table above ignores special flags that come *only* from off-default
optional arguments (`optional_special`). This conservative table is the older rule:
those flags still force `special`.

| Classification | Concrete operators | Share of 84 |
|---|---:|---:|
| `maps_cleanly` | 12 | 14.3% |
| `needs_capability` | 40 | 47.6% |
| `special` | 32 | 38.1% |

### Before / after vs `bdf2cbe`

Previous published snapshot (primary numbers at that commit: 16 / 36 / 32 of 84):

| View | maps_cleanly | needs_capability | special |
|---|---:|---:|---:|
| Previous (`bdf2cbe`) | 16 (19.0%) | 36 (42.9%) | 32 (38.1%) |
| This run, primary | 15 (17.9%) | 43 (51.2%) | 26 (31.0%) |
| This run, conservative | 12 (14.3%) | 40 (47.6%) | 32 (38.1%) |

Rule changes since that snapshot: docstrings are stripped before keyword matches; prefix/wildcard and replace/overwrite are capabilities (not a free pass to `maps_cleanly`); special flags that come only from off-default optional args are `optional_special` and do not force the primary bucket; inheritance now also merges base classes resolvable under `providers/` (`ADLSListOperator`, `AwsBaseOperator`, `BaseSQLOperator`, `S3ListOperator`).

**10** concrete operators changed primary bucket:

| Operator | Kind | From | To | Why |
| --- | --- | --- | --- | --- |
| `HiveToDynamoDBOperator` | sql→record | special | maps_cleanly | optional_special=custom_callable |
| `ImapAttachmentToS3Operator` | record→file | maps_cleanly | needs_capability | listing/overwrite (or other cap) now counted |
| `SqlToS3Operator` | sql→file | special | needs_capability | optional_special=partitioning_or_splitting |
| `HiveToMySqlOperator` | sql→sql | special | needs_capability | optional_special=pre_post_operator_sql |
| `FileTransferOperator` | file→file | maps_cleanly | needs_capability | listing/overwrite (or other cap) now counted |
| `BigQueryToMsSqlOperator` | sql→sql | maps_cleanly | needs_capability | listing/overwrite (or other cap) now counted |
| `BigQueryToMySqlOperator` | sql→sql | maps_cleanly | needs_capability | listing/overwrite (or other cap) now counted |
| `PrestoToMySqlOperator` | sql→sql | special | maps_cleanly | optional_special=pre_post_operator_sql |
| `TrinoToMySqlOperator` | sql→sql | special | maps_cleanly | optional_special=pre_post_operator_sql |
| `VerticaToMySqlOperator` | sql→sql | special | needs_capability | optional_special=pre_post_operator_sql |

## Split view

- **Group A** — file/record transfers with no SQL endpoint: `file→file`, `record→file`, `file→record`.
- **Group B** — anything that touches SQL (kinds whose source or dest category is `sql`).

| Group | Operators | maps_cleanly | needs_capability | special | maps_cleanly % |
| --- | --- | --- | --- | --- | --- |
| A — file/record | 44 | 7 | 33 | 4 | 15.9% |
| B — touches SQL | 40 | 8 | 10 | 22 | 20.0% |

## Capability-heavy operators (≥ 4 capabilities)

**10** concrete operators expose 4 or more capability flags (listing, overwrite, compression, …). These are still `needs_capability` or `special`; the count is about contract surface area, not a third bucket.

| Operator | Kind | Capability count | Capabilities |
| --- | --- | --- | --- |
| `S3ToGCSOperator` | file→file | 6 | `compression`, `deferrable`, `path_layout`, `replace`, `server_side_copy`, `wildcard_or_prefix` |
| `GCSToBigQueryOperator` | file→sql | 5 | `acl_or_extra_args`, `compression`, `deferrable`, `replace`, `schema_sidecar` |
| `GCSToGCSOperator` | file→file | 5 | `incremental_filter`, `move_semantics`, `object_retention`, `replace`, `wildcard_or_prefix` |
| `ExasolToS3Operator` | sql→file | 4 | `acl_or_extra_args`, `compression`, `dest_encryption`, `replace` |
| `FTPToS3Operator` | file→file | 4 | `acl_or_extra_args`, `compression`, `dest_encryption`, `replace` |
| `GCSToS3Operator` | file→file | 4 | `acl_or_extra_args`, `path_layout`, `replace`, `wildcard_or_prefix` |
| `LocalFilesystemToS3Operator` | file→file | 4 | `acl_or_extra_args`, `compression`, `dest_encryption`, `replace` |
| `SFTPToS3Operator` | file→file | 4 | `acl_or_extra_args`, `compression`, `dest_encryption`, `replace` |
| `SFTPToWasbOperator` | file→file | 4 | `acl_or_extra_args`, `move_semantics`, `replace`, `wildcard_or_prefix` |
| `SalesforceToS3Operator` | record→file | 4 | `acl_or_extra_args`, `compression`, `dest_encryption`, `replace` |

## Inheritance resolution

Constructor args and `template_fields` from transfer-module bases are merged as before. This run also loads base classes found under `providers/` (for example `BaseSQLOperator`, `S3ListOperator`, `ADLSListOperator`, and `AwsBaseOperator`).

Resolved non-transfer bases: `ADLSListOperator`, `AwsBaseOperator`, `BaseSQLOperator`, `S3ListOperator`.

Unresolved bases (not merged):

- `BaseOperator` — Airflow core; not present in a providers/-only checkout
- `AwsBaseHookMixin` — no class definition found under providers/

## Optional special flags

**17** concrete operators have `optional_special` flags (special semantics present only as off-default optional constructor args, with no core method/source cue). They stay out of the primary `special` bucket unless another core special flag is present. The conservative view still counts them as `special`.

| Operator | optional_special | Primary | Conservative |
| --- | --- | --- | --- |
| `DynamoDBToS3Operator` | `point_in_time_export` | special | special |
| `HiveToDynamoDBOperator` | `custom_callable` | maps_cleanly | special |
| `RedshiftToS3Operator` | `warehouse_job_options` | special | special |
| `S3ToRedshiftOperator` | `warehouse_job_options` | special | special |
| `SqlToS3Operator` | `partitioning_or_splitting` | needs_capability | special |
| `HiveToDruidOperator` | `dest_table_ddl` | special | special |
| `HiveToMySqlOperator` | `pre_post_operator_sql` | needs_capability | special |
| `MsSqlToHiveOperator` | `partitioning_or_splitting` | special | special |
| `MySqlToHiveOperator` | `partitioning_or_splitting` | special | special |
| `S3ToHiveOperator` | `partitioning_or_splitting` | special | special |
| `VerticaToHiveOperator` | `partitioning_or_splitting` | special | special |
| `GCSToBigQueryOperator` | `partitioning_or_splitting` | special | special |
| `PrestoToMySqlOperator` | `pre_post_operator_sql` | maps_cleanly | special |
| `TrinoToMySqlOperator` | `pre_post_operator_sql` | maps_cleanly | special |
| `VerticaToMySqlOperator` | `pre_post_operator_sql` | needs_capability | special |
| `AzureBlobStorageToTeradataOperator` | `teradata_nos_auth` | special | special |
| `S3ToTeradataOperator` | `teradata_nos_auth` | special | special |

## Classification rules

Rules are data-driven in `scripts/audit_transfers.py` (`FLAG_ARG_RULES`,
`FLAG_METHOD_RULES`, `FLAG_SOURCE_RULES`, `SPECIAL_FLAGS`, `CAPABILITY_FLAGS`).
Flags are collected first; the bucket is then assigned in this order:

1. **`special`** (primary) if any *core* special flag is present. A flag is core
   when it comes from a required or on-by-default constructor arg, a method name,
   an `execute()` source keyword (docstrings stripped first), or the destination
   system itself (Slack). Core special flags:
   - `partitioning_or_splitting` — operator-owned output layout or size-based file splitting
     (`partition_columns`, `max_rows_per_file`, `approx_max_file_size_bytes`, Hive `partition`, …)
   - `dest_table_ddl` — `create` / `recreate` / `tblproperties` / `field_dict`
   - `warehouse_copy_job` / `warehouse_job_options` — Redshift `COPY`/`UNLOAD`, Snowflake
     `COPY INTO`, BigQuery extract/load/copy jobs, Druid ingest, DynamoDB `import_table`,
     Teradata Native Object Store `CREATE TABLE`
   - `custom_callable` — required `parser` / `process_func` / `pre_process`
   - `notification` — destination is Slack
   - `pre_post_operator_sql` — `mysql_preoperator` / `postoperator` wrapping the transfer
   - `point_in_time_export` — DynamoDB PITR export API
   - `teradata_nos_auth` — Teradata AUTHORIZATION / NOS load path
   Special flags that appear only as off-default optional args are stored as
   `optional_special` and do **not** force the primary bucket.
2. Else **`needs_capability`** if any capability flag is present, including
   optional ones (the API exposes the capability even when the default is off):
   - `wildcard_or_prefix` (listing), `replace` (overwrite),
   - `compression`, `dest_encryption`, `acl_or_extra_args`, `server_side_copy`, `schema_sidecar`,
     `move_semantics`, `incremental_filter`, `path_layout`, `pagination`,
     `bulk_load`, `deferrable`, `xcom_side_channel`, `object_retention`, `upsert`
3. Else **`maps_cleanly`**.

Constructor args declared on a parsed transfer base class, and on other Operator
bases found under `providers/`, are merged into subclasses before classification
(so `MySQLToGCSOperator` inherits `partition_columns` from `BaseSQLToGCSOperator`
and is `special`).

## Per-kind breakdown (concrete operators)

| Kind | Operators | maps_cleanly | needs_capability | special | maps_cleanly % |
| --- | --- | --- | --- | --- | --- |
| file→file | 28 | 4 | 24 | 0 | 14.3% |
| record→file | 14 | 2 | 9 | 3 | 14.3% |
| sql→sql | 14 | 4 | 5 | 5 | 28.6% |
| sql→file | 12 | 2 | 2 | 8 | 16.7% |
| file→sql | 10 | 0 | 3 | 7 | 0.0% |
| sql→record | 2 | 2 | 0 | 0 | 100.0% |
| file→record | 2 | 1 | 0 | 1 | 50.0% |
| sql→message | 2 | 0 | 0 | 2 | 0.0% |

Kind is inferred from the `XToYOperator` / `x_to_y.py` pair, mapped to
file / sql / record / message. See `FILE_SYSTEMS`, `SQL_SYSTEMS`,
`RECORD_SYSTEMS`, `MESSAGE_SYSTEMS` in the script.

## Per-provider breakdown (concrete operators)

| Provider | Operators | maps_cleanly | needs_capability | special | maps_cleanly % |
| --- | --- | --- | --- | --- | --- |
| google | 35 | 6 | 18 | 11 | 17.1% |
| amazon | 21 | 3 | 13 | 5 | 14.3% |
| apache.hive | 6 | 1 | 1 | 4 | 16.7% |
| microsoft.azure | 6 | 1 | 5 | 0 | 16.7% |
| mysql | 4 | 2 | 2 | 0 | 50.0% |
| teradata | 3 | 1 | 0 | 2 | 33.3% |
| slack | 2 | 0 | 0 | 2 | 0.0% |
| apache.druid | 1 | 0 | 0 | 1 | 0.0% |
| common.io | 1 | 0 | 1 | 0 | 0.0% |
| oracle | 1 | 1 | 0 | 0 | 100.0% |
| presto | 1 | 0 | 1 | 0 | 0.0% |
| samba | 1 | 0 | 1 | 0 | 0.0% |
| snowflake | 1 | 0 | 0 | 1 | 0.0% |
| trino | 1 | 0 | 1 | 0 | 0.0% |

## Top special-semantics flags

Counts are concrete operators that carry the flag (an operator may have several).

| Flag | Concrete operators |
| --- | --- |
| partitioning_or_splitting | 15 |
| warehouse_copy_job | 9 |
| warehouse_job_options | 8 |
| dest_table_ddl | 5 |
| pre_post_operator_sql | 4 |
| custom_callable | 3 |
| notification | 2 |
| teradata_nos_auth | 2 |

Most frequent flags overall (including capability and informational flags such as `replace`):

| Flag | Concrete operators |
| --- | --- |
| compression | 28 |
| replace | 26 |
| openlineage | 21 |
| partitioning_or_splitting | 15 |
| acl_or_extra_args | 13 |
| wildcard_or_prefix | 11 |
| schema_sidecar | 11 |
| warehouse_copy_job | 9 |
| warehouse_job_options | 8 |
| move_semantics | 7 |
| dest_encryption | 6 |
| path_layout | 5 |

## Notable `special` examples

| Operator | Provider | Kind | Why special |
| --- | --- | --- | --- |
| `BaseSQLToGCSOperator` | google | sql→file | `partitioning_or_splitting` |
| `GCSToBigQueryOperator` | google | file→sql | `warehouse_copy_job`, `warehouse_job_options` |
| `S3ToRedshiftOperator` | amazon | file→sql | `warehouse_copy_job` |
| `HiveToDruidOperator` | apache.druid | sql→sql | `partitioning_or_splitting`, `warehouse_copy_job`, `warehouse_job_options` |
| `CopyFromExternalStageToSnowflakeOperator` | snowflake | file→sql | `warehouse_copy_job`, `warehouse_job_options` |

## Notable `maps_cleanly` examples

| Operator | Provider | Kind | Leading constructor args |
| --- | --- | --- | --- |
| `OracleToOracleOperator` | oracle | sql→sql | oracle_destination_conn_id, destination_table, oracle_source_conn_id, source_sql, source_sql_params, rows_chunk, **kwargs |
| `TeradataToTeradataOperator` | teradata | sql→sql | dest_teradata_conn_id, destination_table, source_teradata_conn_id, sql, sql_params, rows_chunk, **kwargs |
| `HiveToSambaOperator` | apache.hive | sql→file | hql, destination_filepath, samba_conn_id, hiveserver2_conn_id, **kwargs |
| `GoogleDriveToLocalOperator` | google | file→file | output_file, file_name, folder_id, drive_id, gcp_conn_id, impersonation_chain, **kwargs |

## Heuristic limits (read before quoting the percentages)

- **Static AST only.** The script does not import Airflow or run operators.
  Behaviour that lives only in `execute()` without a distinctive argument,
  method name, or source keyword can be missed. Module/class/function docstrings
  are stripped before keyword matching; comments are not.
- **Optional special args no longer force the primary `special` bucket.** They are
  listed as `optional_special`. A non-off default still counts as core (for example
  `approx_max_file_size_bytes=1900000000` keeps the SQL-to-GCS family special;
  `process_func=_convert_item_to_json_bytes` keeps `DynamoDBToS3Operator` special).
  The conservative table is the previous, harsher rule.
- **Inheritance is merged for Operator bases found under `providers/`.** `BaseOperator`
  (Airflow core) is unresolved in a providers-only checkout and is reported as such.
- **`schema` as a bare argument is not treated as a sidecar** (it often means
  a SQL namespace). Sidecars are `schema_filename` / `schema_fields` / `schema_object`.
- **Listing (`wildcard_or_prefix`) and overwrite (`replace`) are capabilities.**
  An operator whose only extra args are prefix/replace is `needs_capability`, not `maps_cleanly`.
- **File→file with gzip/ACL/move/listing is `needs_capability`, not `maps_cleanly`.** Those
  operators are still first-wave candidates once the file contract exposes the capability.
- **Deferrable support is a capability flag**, even when the rest of the args are generic,
  because a generic operator would need a generic trigger.
- **Out-of-scope operators** listed above are not in the denominator.
- No pandas was used; outputs are stdlib CSV and JSON.

## Outputs

- `bench-out/PAIR_AUDIT.csv` — one row per operator class
- `bench-out/PAIR_AUDIT.json` — same data plus the summary block
- `docs/PAIR_AUDIT.md` — this report
- `just rank-capabilities` — sole-blocker / total-any / greedy set-cover over the CSV

