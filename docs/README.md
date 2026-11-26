# Docs

Narrative write-ups for the prototype. The root [README](../README.md) is the
maintainer entry point. Numbers in these files are the committed runs; the
commands that regenerate them are in the root README.

| Doc | What it answers |
|---|---|
| [AIP-132-demo.md](AIP-132-demo.md) | Phase-1 prefix copy on `FileTransferOperator`: prefix/glob, overwrite, folder markers (`skip` and `copy`), returned keys, the same-store fast path, OpenLineage names, and the Airflow 3.4 scheduler run. |
| [PAIR_AUDIT.md](PAIR_AUDIT.md) | What fraction of Airflow's pair transfer operators map onto one generic operator. Snapshot of the AST audit (15 / 43 / 26 of 84 concrete classes). |
| [../audits/results/PAIR_AUDIT.csv](../audits/results/PAIR_AUDIT.csv) | One row per operator class. |
| [../audits/results/CAPABILITY_RANK.md](../audits/results/CAPABILITY_RANK.md) | Which shared capabilities unlock the `needs_capability` bucket (sole-blocker and greedy set-cover). |
| [first-wave-exclusions.md](first-wave-exclusions.md) | Operators that should stay out of a first rebase (partitioning, schema sidecars, format codecs, SQL temp tables). |
| [AZURE_SLOWDOWN.md](AZURE_SLOWDOWN.md) | Why an early scale run showed the generic copier slower only on Azure sources (listing HEADs and a client-construction bias). |
| [../benchmarks/results/BENCH.md](../benchmarks/results/BENCH.md) | Emulator bench: folder markers, 6 pairs, naive 4 objects / 9 requests vs generic 2 / 5. |
| [../benchmarks/results/BENCH_SCALE.md](../benchmarks/results/BENCH_SCALE.md) | Full scale run (1000 × 4 KiB, 100 MiB, 20 ms/request). Machine header, per-round max, wire counts. |
| [../benchmarks/results/BENCH_ADBC.md](../benchmarks/results/BENCH_ADBC.md) | 1,000,000-row Arrow end-to-end vs Arrow coerced to rows. |
| [../benchmarks/results/PROFILE_LISTING.md](../benchmarks/results/PROFILE_LISTING.md) | Where the listing requests went, per pair. |
| [../benchmarks/results/CLIENT_COST.md](../benchmarks/results/CLIENT_COST.md) | Fresh native SDK client vs a cached client, per GET. |
| [../benchmarks/results/EMULATOR_DEGRADATION.md](../benchmarks/results/EMULATOR_DEGRADATION.md) | PUT/GET latency as an emulator fills up. |
| [../shims/gcs_to_gcs_shim/RESULTS.md](../shims/gcs_to_gcs_shim/RESULTS.md) | GCSToGCS shim vs unmodified upstream tests (81/81). |
| [../shims/s3_copy_shim/RESULTS.md](../shims/s3_copy_shim/RESULTS.md) | S3 copy shim (17/17) and S3Mock parity. |
| [../shims/azure_copy_shim/RESULTS.md](../shims/azure_copy_shim/RESULTS.md) | Azure closest-operator shim and Azurite parity. |
| [../shims/generic_all_pairs/RESULTS.md](../shims/generic_all_pairs/RESULTS.md) | One generic file operator on 9 pairs: 34/35 scored cells. |
| [../experiments/s3_to_wasb_replace_repro/README.md](../experiments/s3_to_wasb_replace_repro/README.md) | Local repro of `S3ToAzureBlobStorageOperator` `replace=False`. |

Each shim directory also has a `THRESHOLD.md` that was committed before that
shim was run. Read the threshold first if you are checking whether the result
moved the goalposts.
