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
| `maps_cleanly` | 16 | 19.0% |
| `needs_capability` | 36 | 42.9% |
| `special` | 32 | 38.1% |
| **Total** | **84** | **100%** |

**19.0%** map cleanly onto source-conn + source-location, dest-conn + dest-location, replace/overwrite, and generic format/batch options. **42.9%** would map if the contract grows an optional capability (compression, ACL/extra_args, server-side copy, schema sidecar, move, pagination, deferrable, dest encryption, …). **38.1%** carry semantics a generic operator should not absorb (partitioning / dest-table DDL / warehouse COPY-UNLOAD-extract jobs / custom callables / Slack notifications / pre-post SQL).

**52 / 84 (61.9%)** are `maps_cleanly` or `needs_capability` — they would map onto a generic operator once optional capabilities exist. Of 28 file→file concrete operators, 5 map cleanly, 23 need a file-contract capability (compression, ACL, server-side copy, move, path layout, …), and 0 are special.

## Classification rules

Rules are data-driven in `scripts/audit_transfers.py` (`FLAG_ARG_RULES`,
`FLAG_METHOD_RULES`, `FLAG_SOURCE_RULES`, `SPECIAL_FLAGS`, `CAPABILITY_FLAGS`).
Flags are collected first; the bucket is then assigned in this order:

1. **`special`** if any special flag is present:
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
2. Else **`needs_capability`** if any capability flag is present:
   - `compression`, `dest_encryption`, `acl_or_extra_args`, `server_side_copy`, `schema_sidecar`,
     `move_semantics`, `incremental_filter`, `path_layout`, `pagination`,
     `bulk_load`, `deferrable`, `xcom_side_channel`, `object_retention`, `upsert`
3. Else **`maps_cleanly`**. Prefix/wildcard listing and `replace`/`overwrite` alone
   stay in this bucket: a file contract is expected to cover them.

Constructor args declared on a parsed transfer base class are merged into
subclasses before classification (so `MySQLToGCSOperator` inherits
`partition_columns` from `BaseSQLToGCSOperator` and is `special`).

## Per-kind breakdown (concrete operators)

| Kind | Operators | maps_cleanly | needs_capability | special | maps_cleanly % |
| --- | --- | --- | --- | --- | --- |
| file→file | 28 | 5 | 23 | 0 | 17.9% |
| record→file | 14 | 3 | 8 | 3 | 21.4% |
| sql→sql | 14 | 4 | 1 | 9 | 28.6% |
| sql→file | 12 | 2 | 1 | 9 | 16.7% |
| file→sql | 10 | 0 | 3 | 7 | 0.0% |
| sql→record | 2 | 1 | 0 | 1 | 50.0% |
| file→record | 2 | 1 | 0 | 1 | 50.0% |
| sql→message | 2 | 0 | 0 | 2 | 0.0% |

Kind is inferred from the `XToYOperator` / `x_to_y.py` pair, mapped to
file / sql / record / message. See `FILE_SYSTEMS`, `SQL_SYSTEMS`,
`RECORD_SYSTEMS`, `MESSAGE_SYSTEMS` in the script.

## Per-provider breakdown (concrete operators)

| Provider | Operators | maps_cleanly | needs_capability | special | maps_cleanly % |
| --- | --- | --- | --- | --- | --- |
| google | 35 | 8 | 16 | 11 | 22.9% |
| amazon | 21 | 3 | 11 | 7 | 14.3% |
| apache.hive | 6 | 1 | 0 | 5 | 16.7% |
| microsoft.azure | 6 | 1 | 5 | 0 | 16.7% |
| mysql | 4 | 0 | 1 | 3 | 0.0% |
| teradata | 3 | 1 | 0 | 2 | 33.3% |
| slack | 2 | 0 | 0 | 2 | 0.0% |
| apache.druid | 1 | 0 | 0 | 1 | 0.0% |
| common.io | 1 | 1 | 0 | 0 | 100.0% |
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
| `SqlToS3Operator` | amazon | sql→file | `partitioning_or_splitting` |
| `GCSToBigQueryOperator` | google | file→sql | `partitioning_or_splitting`, `warehouse_copy_job`, `warehouse_job_options` |
| `S3ToRedshiftOperator` | amazon | file→sql | `warehouse_copy_job`, `warehouse_job_options` |
| `HiveToDruidOperator` | apache.druid | sql→sql | `dest_table_ddl`, `partitioning_or_splitting`, `warehouse_copy_job`, `warehouse_job_options` |

## Notable `maps_cleanly` examples

| Operator | Provider | Kind | Leading constructor args |
| --- | --- | --- | --- |
| `FileTransferOperator` | common.io | file→file | src, dst, source_conn_id, dest_conn_id, overwrite, **kwargs |
| `OracleToOracleOperator` | oracle | sql→sql | oracle_destination_conn_id, destination_table, oracle_source_conn_id, source_sql, source_sql_params, rows_chunk, **kwargs |
| `TeradataToTeradataOperator` | teradata | sql→sql | dest_teradata_conn_id, destination_table, source_teradata_conn_id, sql, sql_params, rows_chunk, **kwargs |
| `HiveToSambaOperator` | apache.hive | sql→file | hql, destination_filepath, samba_conn_id, hiveserver2_conn_id, **kwargs |

## Heuristic limits (read before quoting the percentages)

- **Static AST only.** The script does not import Airflow or run operators.
  Behaviour that lives only in `execute()` without a distinctive argument,
  method name, or source keyword can be missed — or a keyword in a docstring
  can over-flag.
- **Optional args still flag the class.** `DynamoDBToS3Operator` is `special`
  because it *can* run a point-in-time export, even if most callers only scan.
- **Inheritance is resolved only among transfer modules.** Args declared on
  `S3ListOperator`, `ADLSListOperator`, or `BaseSQLOperator` are not merged.
- **`schema` as a bare argument is not treated as a sidecar** (it often means
  a SQL namespace). Sidecars are `schema_filename` / `schema_fields` / `schema_object`.
- **File→file with gzip/ACL is `needs_capability`, not `maps_cleanly`.** Those
  operators are still first-wave candidates once the file contract exposes the capability.
- **Deferrable support is a capability flag**, even when the rest of the args are generic,
  because a generic operator would need a generic trigger.
- **Out-of-scope operators** listed above are not in the denominator.
- No pandas was used; outputs are stdlib CSV and JSON.

## Outputs

- `bench-out/PAIR_AUDIT.csv` — one row per operator class
- `bench-out/PAIR_AUDIT.json` — same data plus the summary block
- `docs/PAIR_AUDIT.md` — this report

