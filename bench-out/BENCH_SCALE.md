# Large-scale emulator bench

- Captured (UTC): `2026-11-09T22:00:39.661416+00:00`
- Platform: `Linux-6.12.94+-x86_64-with-glibc2.41` Python `3.12.15`
- Rounds: **3** | large object: **8 MiB** | small N: **100** | workers: `4,8,16` | chunk_size: `1048576`
- S3 backend: `s3mock`

> **Caveat:** Emulators on localhost do not represent real cloud latency or throughput. Median times here are mostly client/emulator overhead. Use latency_demo to see how request count dominates under RTT.

## Scenario: `mixed_tree`

| Pair | Mode | Cold (s) | Warm med (s) | Median (s) | Objects | Reqs | Peak MB (tracemalloc) |
|---|---|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | naive_sequential | 0.6870 | 0.4921 | 0.5308 | 7 | 15 | 1.8 |
| `s3->gcs` | generic_sequential | 0.3680 | 0.3703 | 0.3693 | 5 | 11 | 1.2 |
| `s3->gcs` | generic_parallel_4 | 0.4102 | 0.3966 | 0.4102 | 5 | 11 | 1.3 |
| `s3->gcs` | generic_parallel_8 | 0.3453 | 0.3541 | 0.3536 | 5 | 11 | 1.4 |
| `s3->gcs` | generic_parallel_16 | 0.3453 | 0.3719 | 0.3574 | 5 | 11 | 1.3 |
| `gcs->s3` | naive_sequential | 0.3125 | 0.3629 | 0.3448 | 7 | 15 | 1.1 |
| `gcs->s3` | generic_sequential | 0.3639 | 0.2664 | 0.2712 | 5 | 11 | 1.0 |
| `gcs->s3` | generic_parallel_4 | 0.2453 | 0.2558 | 0.2557 | 5 | 11 | 1.8 |
| `gcs->s3` | generic_parallel_8 | 0.3368 | 0.2613 | 0.2665 | 5 | 11 | 2.0 |
| `gcs->s3` | generic_parallel_16 | 0.2375 | 0.3160 | 0.2659 | 5 | 11 | 1.8 |
| `s3->azure` | naive_sequential | 0.4855 | 0.5914 | 0.5891 | 7 | 15 | 1.6 |
| `s3->azure` | generic_sequential | 0.3965 | 0.4803 | 0.4742 | 5 | 11 | 1.2 |
| `s3->azure` | generic_parallel_4 | 0.3639 | 0.4532 | 0.4046 | 5 | 11 | 1.6 |
| `s3->azure` | generic_parallel_8 | 0.4209 | 0.4250 | 0.4209 | 5 | 11 | 1.5 |
| `s3->azure` | generic_parallel_16 | 0.3875 | 0.4264 | 0.4152 | 5 | 11 | 1.7 |
| `azure->s3` | naive_sequential | 0.4803 | 0.4441 | 0.4549 | 7 | 15 | 1.2 |
| `azure->s3` | generic_sequential | 0.4366 | 0.4265 | 0.4313 | 5 | 11 | 1.1 |
| `azure->s3` | generic_parallel_4 | 0.4368 | 0.4890 | 0.4818 | 5 | 11 | 1.9 |
| `azure->s3` | generic_parallel_8 | 0.4127 | 0.4355 | 0.4184 | 5 | 11 | 2.3 |
| `azure->s3` | generic_parallel_16 | 0.4749 | 0.4308 | 0.4353 | 5 | 11 | 2.2 |
| `gcs->azure` | naive_sequential | 0.1858 | 0.2627 | 0.2520 | 7 | 15 | 0.7 |
| `gcs->azure` | generic_sequential | 0.1602 | 0.2185 | 0.2163 | 5 | 11 | 0.7 |
| `gcs->azure` | generic_parallel_4 | 0.1503 | 0.2031 | 0.2003 | 5 | 11 | 0.7 |
| `gcs->azure` | generic_parallel_8 | 0.1504 | 0.2129 | 0.2071 | 5 | 11 | 0.8 |
| `gcs->azure` | generic_parallel_16 | 0.1450 | 0.2121 | 0.2079 | 5 | 11 | 0.8 |
| `azure->gcs` | naive_sequential | 0.2266 | 0.2783 | 0.2671 | 7 | 15 | 0.6 |
| `azure->gcs` | generic_sequential | 0.2935 | 0.3054 | 0.2944 | 5 | 11 | 0.7 |
| `azure->gcs` | generic_parallel_4 | 0.2822 | 0.3062 | 0.2976 | 5 | 11 | 0.8 |
| `azure->gcs` | generic_parallel_8 | 0.3051 | 0.3185 | 0.3140 | 5 | 11 | 0.9 |
| `azure->gcs` | generic_parallel_16 | 0.3070 | 0.2837 | 0.2854 | 5 | 11 | 0.8 |

## Scenario: `small_1000`

| Pair | Mode | Cold (s) | Warm med (s) | Median (s) | Objects | Reqs | Peak MB (tracemalloc) |
|---|---|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | naive_sequential | 5.9944 | 6.3994 | 6.3042 | 102 | 205 | 5.6 |
| `s3->gcs` | generic_sequential | 4.8228 | 4.8397 | 4.8228 | 100 | 201 | 5.2 |
| `s3->gcs` | generic_parallel_4 | 4.5301 | 4.4467 | 4.4522 | 100 | 201 | 5.3 |
| `s3->gcs` | generic_parallel_8 | 5.0346 | 4.8515 | 4.8969 | 100 | 201 | 5.2 |
| `s3->gcs` | generic_parallel_16 | 4.7482 | 4.6285 | 4.7482 | 100 | 201 | 6.3 |
| `gcs->s3` | naive_sequential | 4.0517 | 4.3825 | 4.2942 | 102 | 205 | 5.2 |
| `gcs->s3` | generic_sequential | 4.4061 | 4.2590 | 4.2882 | 100 | 201 | 5.4 |
| `gcs->s3` | generic_parallel_4 | 3.9546 | 3.9765 | 3.9546 | 100 | 201 | 7.6 |
| `gcs->s3` | generic_parallel_8 | 4.1155 | 4.1706 | 4.1538 | 100 | 201 | 9.4 |
| `gcs->s3` | generic_parallel_16 | 4.1796 | 4.7441 | 4.4227 | 100 | 201 | 11.3 |
| `s3->azure` | naive_sequential | 5.9338 | 6.5533 | 6.5069 | 102 | 205 | 7.2 |
| `s3->azure` | generic_sequential | 5.1219 | 5.8628 | 5.7781 | 100 | 201 | 7.7 |
| `s3->azure` | generic_parallel_4 | 4.4849 | 4.8804 | 4.7794 | 100 | 201 | 7.9 |
| `s3->azure` | generic_parallel_8 | 4.5013 | 5.0149 | 5.0051 | 100 | 201 | 6.4 |
| `s3->azure` | generic_parallel_16 | 4.4235 | 4.8152 | 4.8026 | 100 | 201 | 8.0 |
| `azure->s3` | naive_sequential | 4.2139 | 4.9596 | 4.7477 | 102 | 205 | 7.3 |
| `azure->s3` | generic_sequential | 5.9182 | 5.8574 | 5.9002 | 100 | 201 | 7.3 |
| `azure->s3` | generic_parallel_4 | 5.2970 | 5.9377 | 5.6887 | 100 | 201 | 8.5 |
| `azure->s3` | generic_parallel_8 | 6.2055 | 5.7342 | 5.7624 | 100 | 201 | 10.2 |
| `azure->s3` | generic_parallel_16 | 5.3771 | 5.4763 | 5.3771 | 100 | 201 | 12.7 |
| `gcs->azure` | naive_sequential | 2.2485 | 2.6322 | 2.6025 | 102 | 205 | 5.8 |
| `gcs->azure` | generic_sequential | 2.1324 | 2.5689 | 2.5626 | 100 | 201 | 5.7 |
| `gcs->azure` | generic_parallel_4 | 2.0261 | 2.3814 | 2.3348 | 100 | 201 | 5.8 |
| `gcs->azure` | generic_parallel_8 | 2.0768 | 2.3266 | 2.2706 | 100 | 201 | 6.2 |
| `gcs->azure` | generic_parallel_16 | 1.8330 | 2.4296 | 2.4237 | 100 | 201 | 5.7 |
| `azure->gcs` | naive_sequential | 2.5004 | 3.2627 | 2.8341 | 102 | 205 | 5.6 |
| `azure->gcs` | generic_sequential | 3.9264 | 4.0710 | 4.0607 | 100 | 201 | 5.6 |
| `azure->gcs` | generic_parallel_4 | 3.4636 | 3.8820 | 3.8291 | 100 | 201 | 5.6 |
| `azure->gcs` | generic_parallel_8 | 3.8184 | 4.0263 | 3.9794 | 100 | 201 | 5.7 |
| `azure->gcs` | generic_parallel_16 | 3.8952 | 3.8754 | 3.8952 | 100 | 201 | 5.8 |

## Scenario: `large_object`

| Pair | Mode | Cold (s) | Warm med (s) | Median (s) | Objects | Reqs | Peak MB (tracemalloc) |
|---|---|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | generic_sequential | 0.3453 | 0.2988 | 0.3037 | 1 | 3 | 32.5 |
| `s3->gcs` | generic_parallel_4 | 0.2638 | 0.3034 | 0.3020 | 1 | 3 | 32.5 |
| `gcs->s3` | generic_sequential | 0.3130 | 0.2724 | 0.2759 | 1 | 3 | 32.7 |
| `gcs->s3` | generic_parallel_4 | 0.2741 | 0.3007 | 0.2931 | 1 | 3 | 32.7 |
| `s3->azure` | generic_sequential | 0.3028 | 0.3198 | 0.3122 | 1 | 3 | 24.8 |
| `s3->azure` | generic_parallel_4 | 0.2477 | 0.2926 | 0.2879 | 1 | 3 | 24.8 |
| `azure->s3` | generic_sequential | 0.2915 | 9.3712 | 0.3533 | 1 | 3 | 24.7 |
| `azure->s3` | generic_parallel_4 | 0.3234 | 0.3357 | 0.3321 | 1 | 3 | 24.7 |
| `gcs->azure` | generic_sequential | 0.1995 | 0.2283 | 0.2279 | 1 | 3 | 32.7 |
| `gcs->azure` | generic_parallel_4 | 0.1751 | 0.2298 | 0.2255 | 1 | 3 | 32.7 |
| `azure->gcs` | generic_sequential | 0.2394 | 0.2722 | 0.2643 | 1 | 3 | 32.3 |
| `azure->gcs` | generic_parallel_4 | 0.2390 | 0.2579 | 0.2457 | 1 | 3 | 32.2 |

## Scenario: `server_side_same_provider`

| Pair | Mode | Cold (s) | Warm med (s) | Median (s) | Objects | Reqs | Peak MB (tracemalloc) |
|---|---|---:|---:|---:|---:|---:|---:|
| `s3->s3` | server_side | 0.3887 | 0.2946 | 0.2970 | 2 | 3 | 1.4 |
| `s3->s3` | read_write | 0.2848 | 0.3073 | 0.3015 | 2 | 5 | 1.3 |
| `gcs->gcs` | server_side | 0.0831 | 0.0904 | 0.0885 | 2 | 3 | 0.4 |
| `gcs->gcs` | read_write | 0.0873 | 0.0969 | 0.0894 | 2 | 5 | 0.4 |
| `azure->azure` | server_side | 0.1660 | 0.2574 | 0.2487 | 2 | 3 | 0.7 |
| `azure->azure` | read_write | 0.2073 | 0.2619 | 0.2557 | 2 | 5 | 0.7 |

## Scenario: `latency_demo`

Injected latency: **25.0 ms/request** on `s3->gcs`.

Naive copies 4 objects (9 req); generic copies 2 (5 req). With injected per-request latency, wall time tracks request_total.

| Path | Median (s) | Requests (last) | Objects | Injected latency (s) |
|---|---:|---:|---:|---:|
| naive | 0.546 | 9 | 4 | 0.225 |
| generic | 0.337 | 5 | 2 | 0.125 |

## Reproduce

```bash
just up && just bench-scale          # full (100 MiB + 1000 objs)
just up && just bench-scale-quick    # 8 MiB + 100 objs
```
