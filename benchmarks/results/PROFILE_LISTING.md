# Listing/read profile per pair (n=1000, rounds=3, injected latency 0.0 ms/request)

- Captured (UTC): `2026-11-10T06:43:05.030297+00:00`

| Pair | Path | Median (s) | Objects | HTTP total | HTTP breakdown (last round) |
|---|---|---:|---:|---:|---|
| `azure->gcs` | naive | 4.369 | 1011 | 2034 | azure:get=1022, azure:list=1, gcs:put=1011 |
| `azure->gcs` | generic_per_key | 9.187 | 1000 | 4022 | azure:get=1000, azure:head=2011, azure:list=11, gcs:put=1000 |
| `azure->gcs` | generic_single | 4.630 | 1000 | 2002 | azure:get=1000, azure:head=1, azure:list=1, gcs:put=1000 |
| `azure->gcs` | list_per_key | 2.244 | 1000 | 1022 | azure:head=1011, azure:list=11 |
| `azure->gcs` | list_single | 0.193 | 1000 | 2 | azure:head=1, azure:list=1 |
| `azure->s3` | naive | 6.421 | 1011 | 2034 | azure:get=1022, azure:list=1, s3:put=1011 |
| `azure->s3` | generic_per_key | 10.912 | 1000 | 4022 | azure:get=1000, azure:head=2011, azure:list=11, s3:put=1000 |
| `azure->s3` | generic_single | 6.308 | 1000 | 2002 | azure:get=1000, azure:head=1, azure:list=1, s3:put=1000 |
| `azure->s3` | list_per_key | 2.305 | 1000 | 1022 | azure:head=1011, azure:list=11 |
| `azure->s3` | list_single | 0.215 | 1000 | 2 | azure:head=1, azure:list=1 |
| `s3->gcs` | naive | 4.293 | 1011 | 2024 | gcs:put=1011, s3:get=1011, s3:list=2 |
| `s3->gcs` | generic_per_key | 9.242 | 1000 | 4024 | gcs:put=1000, s3:get=1000, s3:head=2011, s3:list=13 |
| `s3->gcs` | generic_single | 4.382 | 1000 | 2002 | gcs:put=1000, s3:get=1000, s3:list=2 |
| `s3->gcs` | list_per_key | 2.658 | 1000 | 1024 | s3:head=1011, s3:list=13 |
| `s3->gcs` | list_single | 0.092 | 1000 | 2 | s3:list=2 |
| `gcs->s3` | naive | 4.249 | 1011 | 2024 | gcs:get=1011, gcs:list=1, gcs:metadata=1, s3:put=1011 |
| `gcs->s3` | generic_per_key | 7.302 | 1000 | 6325 | gcs:get=1000, gcs:list=2013, gcs:metadata=2312, s3:put=1000 |
| `gcs->s3` | generic_single | 4.534 | 1000 | 2002 | gcs:get=1000, gcs:list=2, s3:put=1000 |
| `gcs->s3` | list_per_key | 1.042 | 1000 | 2169 | gcs:list=1013, gcs:metadata=1156 |
| `gcs->s3` | list_single | 0.017 | 1000 | 2 | gcs:list=2 |
| `s3->azure` | naive | 6.650 | 1011 | 2024 | azure:put=1011, s3:get=1011, s3:list=2 |
| `s3->azure` | generic_per_key | 11.834 | 1000 | 4024 | azure:put=1000, s3:get=1000, s3:head=2011, s3:list=13 |
| `s3->azure` | generic_single | 6.952 | 1000 | 2002 | azure:put=1000, s3:get=1000, s3:list=2 |
| `s3->azure` | list_per_key | 2.587 | 1000 | 1024 | s3:head=1011, s3:list=13 |
| `s3->azure` | list_single | 0.094 | 1000 | 2 | s3:list=2 |
| `gcs->azure` | naive | 5.063 | 1011 | 2024 | azure:put=1011, gcs:get=1011, gcs:list=1, gcs:metadata=1 |
| `gcs->azure` | generic_per_key | 6.758 | 1000 | 6340 | azure:put=1000, gcs:get=1000, gcs:list=2013, gcs:metadata=2327 |
| `gcs->azure` | generic_single | 4.717 | 1000 | 2002 | azure:put=1000, gcs:get=1000, gcs:list=2 |
| `gcs->azure` | list_per_key | 0.988 | 1000 | 2171 | gcs:list=1013, gcs:metadata=1158 |
| `gcs->azure` | list_single | 0.018 | 1000 | 2 | gcs:list=2 |
