# Scale bench (full run)

- Captured (UTC): `2026-11-10T07:30:59.132551+00:00`; wall clock 62.5 min
- Host: `Intel(R) Xeon(R) Processor` x8, 15.6 GiB RAM, `Linux-6.12.113+-x86_64-with-glibc2.41`, Python `3.12.15`, Docker `29.1.4`
- Emulators: `adobe/s3mock:3.12.0`, `fsouza/fake-gcs-server:1.56.1`, `mcr.microsoft.com/azure-storage/azurite:3.37.0`
- Packages: fsspec `2026.9.0`, s3fs `2026.9.0`, gcsfs `2026.10.1`, adlfs `2026.8.0`, boto3 `1.43.117`, aiohttp `3.14.5`, urllib3 `2.8.0`, azure.storage.blob `12.31.1`, google.cloud.storage `3.17.1`
- Rounds: **5** (round 0 = cold) | large object: **100 MiB** | small objects: **1000 x 4096 B** | workers: `4,8,16` | chunk_size: `1048576`
- wire_latency: **20.0 ms** before every HTTP request, **200** objects

> **Caveat:** Emulators on localhost do not represent real cloud latency or throughput. Times without injected latency are mostly client and emulator CPU. The wire_latency scenario adds a fixed delay before every HTTP request to model round-trip time.

server_side_same_provider was re-run at `2026-11-10T08:44:30.314269+00:00` after a demo bug fix (shared listing + cached native client). The other four scenarios are from the original full run.

Warm med = median of rounds 1..N-1 (with 3 rounds that is the mean of two samples, so one stall moves it; this run uses the rounds shown above). Max = slowest round. Columns: *HTTP reqs* = real requests counted at the transport (median over rounds); *Logical* = the copier's own list/get/put tally (last round). Peak MB = tracemalloc peak (Python allocations only, median over rounds).

## `mixed_tree`: Mixed tree (markers, unicode, spaces, 0-byte real file)

| Pair | Mode | Cold (s) | Warm med (s) | Max (s) | Objects | HTTP reqs | Logical | Peak MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | naive_sequential | 0.106 | 0.079 | 0.106 | 7 | 15 | 15 | 0.4 |
| `s3->gcs` | generic_sequential_per_key | 0.156 | 0.123 | 0.156 | 5 | 26 | 11 | 0.4 |
| `s3->gcs` | generic_sequential | 0.041 | 0.041 | 0.041 | 5 | 10 | 11 | 0.3 |
| `s3->gcs` | generic_parallel_4 | 0.036 | 0.033 | 0.036 | 5 | 10 | 11 | 0.4 |
| `s3->gcs` | generic_parallel_8 | 0.033 | 0.034 | 0.036 | 5 | 10 | 11 | 0.4 |
| `s3->gcs` | generic_parallel_16 | 0.032 | 0.033 | 0.035 | 5 | 10 | 11 | 0.4 |
| `gcs->s3` | naive_sequential | 0.072 | 0.061 | 0.072 | 7 | 16 | 15 | 0.1 |
| `gcs->s3` | generic_sequential_per_key | 0.066 | 0.066 | 0.073 | 5 | 37 | 11 | 0.3 |
| `gcs->s3` | generic_sequential | 0.033 | 0.033 | 0.034 | 5 | 10 | 11 | 0.3 |
| `gcs->s3` | generic_parallel_4 | 0.026 | 0.023 | 0.026 | 5 | 10 | 11 | 0.4 |
| `gcs->s3` | generic_parallel_8 | 0.024 | 0.025 | 0.046 | 5 | 10 | 11 | 0.4 |
| `gcs->s3` | generic_parallel_16 | 0.025 | 0.024 | 0.025 | 5 | 10 | 11 | 0.3 |
| `s3->azure` | naive_sequential | 0.117 | 0.095 | 0.117 | 7 | 15 | 15 | 0.4 |
| `s3->azure` | generic_sequential_per_key | 0.140 | 0.135 | 0.144 | 5 | 26 | 11 | 0.4 |
| `s3->azure` | generic_sequential | 0.060 | 0.060 | 0.082 | 5 | 10 | 11 | 0.3 |
| `s3->azure` | generic_parallel_4 | 0.047 | 0.046 | 0.048 | 5 | 10 | 11 | 0.5 |
| `s3->azure` | generic_parallel_8 | 0.044 | 0.045 | 0.049 | 5 | 10 | 11 | 0.5 |
| `s3->azure` | generic_parallel_16 | 0.049 | 0.044 | 0.051 | 5 | 10 | 11 | 0.5 |
| `azure->s3` | naive_sequential | 0.098 | 0.097 | 0.103 | 7 | 18 | 15 | 0.3 |
| `azure->s3` | generic_sequential_per_key | 0.132 | 0.129 | 0.132 | 5 | 24 | 11 | 0.5 |
| `azure->s3` | generic_sequential | 0.062 | 0.063 | 0.064 | 5 | 11 | 11 | 0.5 |
| `azure->s3` | generic_parallel_4 | 0.049 | 0.049 | 0.057 | 5 | 11 | 11 | 0.5 |
| `azure->s3` | generic_parallel_8 | 0.051 | 0.049 | 0.052 | 5 | 11 | 11 | 0.5 |
| `azure->s3` | generic_parallel_16 | 0.050 | 0.051 | 0.052 | 5 | 11 | 11 | 0.5 |
| `gcs->azure` | naive_sequential | 0.064 | 0.069 | 0.101 | 7 | 16 | 15 | 0.2 |
| `gcs->azure` | generic_sequential_per_key | 0.066 | 0.067 | 0.072 | 5 | 36 | 11 | 0.4 |
| `gcs->azure` | generic_sequential | 0.037 | 0.038 | 0.045 | 5 | 10 | 11 | 0.3 |
| `gcs->azure` | generic_parallel_4 | 0.031 | 0.029 | 0.031 | 5 | 10 | 11 | 0.4 |
| `gcs->azure` | generic_parallel_8 | 0.028 | 0.029 | 0.032 | 5 | 10 | 11 | 0.5 |
| `gcs->azure` | generic_parallel_16 | 0.036 | 0.029 | 0.036 | 5 | 10 | 11 | 0.5 |
| `azure->gcs` | naive_sequential | 0.093 | 0.076 | 0.093 | 7 | 18 | 15 | 0.3 |
| `azure->gcs` | generic_sequential_per_key | 0.120 | 0.114 | 0.120 | 5 | 24 | 11 | 0.5 |
| `azure->gcs` | generic_sequential | 0.048 | 0.049 | 0.051 | 5 | 11 | 11 | 0.5 |
| `azure->gcs` | generic_parallel_4 | 0.043 | 0.044 | 0.051 | 5 | 11 | 11 | 0.5 |
| `azure->gcs` | generic_parallel_8 | 0.046 | 0.043 | 0.049 | 5 | 11 | 11 | 0.6 |
| `azure->gcs` | generic_parallel_16 | 0.046 | 0.042 | 0.046 | 5 | 11 | 11 | 0.6 |

## `small_objects`: Many small objects

| Pair | Mode | Cold (s) | Warm med (s) | Max (s) | Objects | HTTP reqs | Logical | Peak MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | naive_sequential | 8.361 | 8.374 | 8.638 | 1011 | 2024 | 2023 | 2.6 |
| `s3->gcs` | generic_sequential_per_key | 20.863 | 18.888 | 20.863 | 1000 | 4024 | 2001 | 2.3 |
| `s3->gcs` | generic_sequential | 8.772 | 8.757 | 8.923 | 1000 | 2002 | 2001 | 2.3 |
| `s3->gcs` | generic_parallel_4 | 6.335 | 6.307 | 6.463 | 1000 | 2002 | 2001 | 2.9 |
| `s3->gcs` | generic_parallel_8 | 6.478 | 6.147 | 6.478 | 1000 | 2002 | 2001 | 3.2 |
| `s3->gcs` | generic_parallel_16 | 6.381 | 6.322 | 6.414 | 1000 | 2002 | 2001 | 3.4 |
| `gcs->s3` | naive_sequential | 8.486 | 8.705 | 8.952 | 1011 | 2024 | 2023 | 4.2 |
| `gcs->s3` | generic_sequential_per_key | 11.899 | 12.050 | 12.593 | 1000 | 6050 | 2001 | 4.1 |
| `gcs->s3` | generic_sequential | 7.255 | 7.354 | 7.637 | 1000 | 2002 | 2001 | 4.1 |
| `gcs->s3` | generic_parallel_4 | 5.027 | 4.930 | 5.049 | 1000 | 2002 | 2001 | 4.1 |
| `gcs->s3` | generic_parallel_8 | 4.568 | 6.450 | 14.510 | 1000 | 2002 | 2001 | 4.1 |
| `gcs->s3` | generic_parallel_16 | 7.277 | 5.133 | 7.277 | 1000 | 2002 | 2001 | 4.1 |
| `s3->azure` | naive_sequential | 13.693 | 13.200 | 14.220 | 1011 | 2024 | 2023 | 2.6 |
| `s3->azure` | generic_sequential_per_key | 22.889 | 22.321 | 22.889 | 1000 | 4024 | 2001 | 2.3 |
| `s3->azure` | generic_sequential | 12.408 | 13.530 | 14.164 | 1000 | 2002 | 2001 | 2.3 |
| `s3->azure` | generic_parallel_4 | 9.321 | 8.796 | 9.348 | 1000 | 2002 | 2001 | 3.6 |
| `s3->azure` | generic_parallel_8 | 8.487 | 8.289 | 9.001 | 1000 | 2002 | 2001 | 3.9 |
| `s3->azure` | generic_parallel_16 | 8.412 | 8.236 | 8.412 | 1000 | 2002 | 2001 | 4.4 |
| `azure->s3` | naive_sequential | 12.355 | 13.119 | 13.470 | 1011 | 2034 | 2023 | 8.6 |
| `azure->s3` | generic_sequential_per_key | 24.530 | 23.449 | 24.530 | 1000 | 4022 | 2001 | 7.9 |
| `azure->s3` | generic_sequential | 12.646 | 12.314 | 12.668 | 1000 | 2002 | 2001 | 7.9 |
| `azure->s3` | generic_parallel_4 | 8.751 | 9.461 | 10.150 | 1000 | 2002 | 2001 | 7.9 |
| `azure->s3` | generic_parallel_8 | 8.850 | 9.222 | 10.394 | 1000 | 2002 | 2001 | 8.2 |
| `azure->s3` | generic_parallel_16 | 9.125 | 8.586 | 9.125 | 1000 | 2002 | 2001 | 8.7 |
| `gcs->azure` | naive_sequential | 9.963 | 10.149 | 10.339 | 1011 | 2024 | 2023 | 4.2 |
| `gcs->azure` | generic_sequential_per_key | 13.417 | 14.428 | 15.232 | 1000 | 6055 | 2001 | 4.2 |
| `gcs->azure` | generic_sequential | 9.302 | 8.930 | 9.302 | 1000 | 2002 | 2001 | 4.2 |
| `gcs->azure` | generic_parallel_4 | 6.094 | 5.604 | 6.094 | 1000 | 2002 | 2001 | 4.2 |
| `gcs->azure` | generic_parallel_8 | 5.593 | 5.840 | 5.882 | 1000 | 2002 | 2001 | 4.2 |
| `gcs->azure` | generic_parallel_16 | 5.774 | 5.759 | 6.038 | 1000 | 2002 | 2001 | 4.6 |
| `azure->gcs` | naive_sequential | 10.220 | 10.252 | 11.280 | 1011 | 2034 | 2023 | 8.6 |
| `azure->gcs` | generic_sequential_per_key | 21.720 | 21.287 | 21.720 | 1000 | 4022 | 2001 | 7.9 |
| `azure->gcs` | generic_sequential | 9.940 | 10.581 | 10.769 | 1000 | 2002 | 2001 | 7.9 |
| `azure->gcs` | generic_parallel_4 | 8.314 | 8.502 | 9.007 | 1000 | 2002 | 2001 | 8.1 |
| `azure->gcs` | generic_parallel_8 | 8.304 | 7.670 | 8.304 | 1000 | 2002 | 2001 | 8.6 |
| `azure->gcs` | generic_parallel_16 | 7.410 | 7.464 | 7.525 | 1000 | 2002 | 2001 | 9.2 |

## `large_object`: One large object

| Pair | Mode | Cold (s) | Warm med (s) | Max (s) | Objects | HTTP reqs | Logical | Peak MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | naive_sequential | 1.187 | 0.769 | 1.187 | 1 | 4 | 3 | 136.2 |
| `s3->gcs` | generic_sequential | 1.000 | 0.840 | 1.001 | 1 | 6 | 3 | 255.9 |
| `s3->gcs` | generic_parallel_4 | 0.728 | 0.782 | 1.073 | 1 | 6 | 3 | 255.9 |
| `gcs->s3` | naive_sequential | 0.962 | 1.073 | 1.120 | 1 | 4 | 3 | 108.8 |
| `gcs->s3` | generic_sequential | 1.037 | 0.979 | 1.037 | 1 | 33 | 3 | 211.9 |
| `gcs->s3` | generic_parallel_4 | 0.983 | 0.954 | 0.986 | 1 | 32 | 3 | 211.9 |
| `s3->azure` | naive_sequential | 2.067 | 2.136 | 2.262 | 1 | 28 | 3 | 136.2 |
| `s3->azure` | generic_sequential | 2.473 | 2.070 | 2.473 | 1 | 30 | 3 | 255.9 |
| `s3->azure` | generic_parallel_4 | 2.192 | 2.139 | 2.192 | 1 | 30 | 3 | 255.9 |
| `azure->s3` | naive_sequential | 1.558 | 2.198 | 40.479 | 1 | 20 | 3 | 288.4 |
| `azure->s3` | generic_sequential | 1.352 | 1.458 | 1.708 | 1 | 16 | 3 | 256.0 |
| `azure->s3` | generic_parallel_4 | 1.067 | 1.267 | 1.356 | 1 | 16 | 3 | 256.0 |
| `gcs->azure` | naive_sequential | 2.815 | 2.281 | 2.815 | 1 | 29 | 3 | 108.8 |
| `gcs->azure` | generic_sequential | 2.262 | 2.290 | 2.441 | 1 | 58 | 3 | 211.9 |
| `gcs->azure` | generic_parallel_4 | 2.398 | 2.326 | 2.398 | 1 | 57 | 3 | 211.9 |
| `azure->gcs` | naive_sequential | 2.165 | 1.430 | 2.165 | 1 | 21 | 3 | 288.4 |
| `azure->gcs` | generic_sequential | 0.900 | 0.979 | 1.028 | 1 | 17 | 3 | 256.0 |
| `azure->gcs` | generic_parallel_4 | 1.202 | 1.054 | 1.212 | 1 | 17 | 3 | 256.0 |

## `server_side_same_provider`: Same-provider server-side copy vs read/write

Before the fix this path listed every native key and called `is_folder_marker` (an `info()` / HEAD, plus a child `find` for each zero-byte marker) **and** built a new boto3 / GCS / Azure client per object. The two skipped keys were the folder markers, reported because they were inspected one-by-one instead of being filtered by `list_transfer_entries`. That made server-side slower than read/write and inflated HTTP counts (9–14 vs 5–6). After the fix, server-side copies the same two files, `skipped` is empty, and it is faster than read/write with 3 HTTP requests (4 on Azure).

| Pair | Mode | Cold (s) | Warm med (s) | Max (s) | Objects | HTTP reqs | Logical | Peak MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `s3->s3` | server_side | 0.019 | 0.014 | 0.019 | 2 | 3 | 3 | 0.3 |
| `s3->s3` | read_write | 0.026 | 0.023 | 0.033 | 2 | 5 | 5 | 0.3 |
| `gcs->gcs` | server_side | 0.008 | 0.006 | 0.008 | 2 | 3 | 3 | 0.3 |
| `gcs->gcs` | read_write | 0.010 | 0.009 | 0.011 | 2 | 5 | 5 | 0.3 |
| `azure->azure` | server_side | 0.023 | 0.023 | 0.023 | 2 | 4 | 3 | 0.4 |
| `azure->azure` | read_write | 0.037 | 0.036 | 0.037 | 2 | 6 | 5 | 0.4 |

## `wire_latency`: Small objects with injected per-request latency

| Pair | Mode | Cold (s) | Warm med (s) | Max (s) | Objects | HTTP reqs | Logical | Peak MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | naive_sequential | 9.970 | 9.968 | 10.008 | 205 | 411 | 411 | 0.8 |
| `s3->gcs` | generic_sequential_per_key | 20.121 | 20.338 | 20.791 | 200 | 811 | 401 | 0.7 |
| `s3->gcs` | generic_sequential | 9.840 | 9.927 | 10.261 | 200 | 401 | 401 | 0.7 |
| `s3->gcs` | generic_parallel_4 | 2.558 | 2.560 | 2.645 | 200 | 401 | 401 | 1.2 |
| `s3->gcs` | generic_parallel_8 | 1.430 | 1.468 | 1.546 | 200 | 401 | 401 | 1.3 |
| `s3->gcs` | generic_parallel_16 | 1.442 | 1.431 | 1.606 | 200 | 401 | 401 | 1.6 |
| `gcs->s3` | naive_sequential | 10.251 | 10.380 | 10.536 | 205 | 412 | 411 | 0.9 |
| `gcs->s3` | generic_sequential_per_key | 19.595 | 19.229 | 19.651 | 200 | 1307 | 401 | 0.8 |
| `gcs->s3` | generic_sequential | 9.881 | 9.785 | 10.177 | 200 | 401 | 401 | 0.8 |
| `gcs->s3` | generic_parallel_4 | 2.490 | 2.630 | 2.787 | 200 | 401 | 401 | 1.0 |
| `gcs->s3` | generic_parallel_8 | 1.440 | 1.440 | 1.519 | 200 | 401 | 401 | 1.1 |
| `gcs->s3` | generic_parallel_16 | 1.043 | 1.098 | 1.136 | 200 | 401 | 401 | 1.3 |
| `s3->azure` | naive_sequential | 11.549 | 11.237 | 11.549 | 205 | 411 | 411 | 0.8 |
| `s3->azure` | generic_sequential_per_key | 21.931 | 22.447 | 22.771 | 200 | 811 | 401 | 0.9 |
| `s3->azure` | generic_sequential | 11.357 | 11.535 | 11.845 | 200 | 401 | 401 | 0.9 |
| `s3->azure` | generic_parallel_4 | 2.854 | 2.923 | 3.263 | 200 | 401 | 401 | 1.5 |
| `s3->azure` | generic_parallel_8 | 2.017 | 1.824 | 2.017 | 200 | 401 | 401 | 2.1 |
| `s3->azure` | generic_parallel_16 | 1.717 | 1.659 | 1.782 | 200 | 401 | 401 | 2.6 |
| `azure->s3` | naive_sequential | 10.989 | 11.104 | 11.506 | 205 | 416 | 411 | 1.7 |
| `azure->s3` | generic_sequential_per_key | 21.856 | 22.778 | 24.052 | 200 | 810 | 401 | 1.7 |
| `azure->s3` | generic_sequential | 11.447 | 11.067 | 11.447 | 200 | 402 | 401 | 1.7 |
| `azure->s3` | generic_parallel_4 | 2.977 | 3.026 | 3.167 | 200 | 402 | 401 | 2.3 |
| `azure->s3` | generic_parallel_8 | 1.936 | 1.947 | 2.140 | 200 | 402 | 401 | 2.6 |
| `azure->s3` | generic_parallel_16 | 1.798 | 1.733 | 1.798 | 200 | 402 | 401 | 2.1 |
| `gcs->azure` | naive_sequential | 10.548 | 11.093 | 11.404 | 205 | 412 | 411 | 0.9 |
| `gcs->azure` | generic_sequential_per_key | 19.917 | 19.950 | 20.234 | 200 | 1349 | 401 | 0.9 |
| `gcs->azure` | generic_sequential | 10.135 | 10.596 | 11.313 | 200 | 401 | 401 | 0.9 |
| `gcs->azure` | generic_parallel_4 | 2.729 | 2.731 | 3.104 | 200 | 401 | 401 | 1.6 |
| `gcs->azure` | generic_parallel_8 | 1.651 | 1.565 | 1.651 | 200 | 401 | 401 | 2.2 |
| `gcs->azure` | generic_parallel_16 | 1.588 | 1.246 | 1.588 | 200 | 401 | 401 | 2.8 |
| `azure->gcs` | naive_sequential | 10.864 | 10.976 | 11.049 | 205 | 416 | 411 | 1.7 |
| `azure->gcs` | generic_sequential_per_key | 21.074 | 20.999 | 21.324 | 200 | 810 | 401 | 1.7 |
| `azure->gcs` | generic_sequential | 10.182 | 10.280 | 10.325 | 200 | 402 | 401 | 1.7 |
| `azure->gcs` | generic_parallel_4 | 2.732 | 2.767 | 2.873 | 200 | 402 | 401 | 1.7 |
| `azure->gcs` | generic_parallel_8 | 1.671 | 1.687 | 1.797 | 200 | 402 | 401 | 2.5 |
| `azure->gcs` | generic_parallel_16 | 1.569 | 1.563 | 1.612 | 200 | 402 | 401 | 2.6 |

## Reproduce

```bash
just up && just bench-scale          # full run -> bench-out/BENCH_SCALE.*
just up && just bench-scale-quick    # quick run -> bench-out/BENCH_SCALE_QUICK.*
```
