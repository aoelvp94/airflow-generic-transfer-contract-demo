# ADBC / Arrow fit bench

- Rows: **1000000** | batch_size: **50000** | rounds: **3**
- Captured (UTC): `2026-11-09T22:59:49.549476+00:00`

| Path | Median (s) | Peak MB (tracemalloc med) | Records (last) |
|---|---:|---:|---:|
| `arrow_adbc_e2e` | 0.575 | 0.2 | 1000000 |
| `arrow_to_rows_then_sink` | 2.912 | 244.9 | 1000000 |
| `pure_rows_200000` | 0.004 | 2.3 | 200000 |

## Does the contract fit?

Works:
- Protocols accept Any batches; isinstance checks are structural.
- AdbcSqliteSource.batch_format == 'record_batch'.
- GenericRecordTransfer streams RecordBatch end-to-end without knowing Arrow.

Needs a decision:

- batch type negotiation when source is Arrow and sink wants rows (or vice versa).
- schema handling / cast policy at the operator vs hook boundary.
- whether FileCopyable and BatchReadable share one operator or stay split.

Reproduce: `just bench-adbc`
