# ADBC / Arrow fit bench

- Rows: **1000000** | batch_size: **50000** | rounds: **3**
- Captured (UTC): `2026-11-19T10:20:44.974360+00:00`
- Host: `Intel(R) Xeon(R) Processor` x4, 15.6 GiB RAM, `Linux-6.12.113+-x86_64-with-glibc2.39`, Python `3.12.3`
- Peak RSS source: `VmHWM` (child process, `/proc/self/status` VmHWM; `ru_maxrss` where /proc is absent). Before is the mark after imports and before the transfer; after is the mark when the transfer returns. Medians are over the rounds.

| Path | Median (s) | RSS before MB | RSS peak MB | Records (last) |
|---|---:|---:|---:|---:|
| `arrow_adbc_e2e` | 0.543 | 79.7 | 94.5 | 1000000 |
| `arrow_to_rows_then_sink` | 0.549 | 79.8 | 344.1 | 1000000 |
| `pure_rows_200000` | 0.045 | 79.7 | 133.0 | 200000 |

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
