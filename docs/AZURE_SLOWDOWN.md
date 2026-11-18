# Why the generic copier looked slower than naive for Azure sources

## Symptom (quick run at `6694c61`, 100 small objects, median of 3)

| Pair | naive_sequential (s) | generic_sequential (s) |
|---|---:|---:|
| `azure->gcs` | 2.834 | 4.061 |
| `azure->s3` | 4.748 | 5.900 |
| `s3->gcs` (for contrast) | 6.304 | 4.823 |
| `gcs->s3` (for contrast) | 4.294 | 4.288 |

Only the two Azure-source pairs had generic slower than naive. The "Reqs"
column said generic made *fewer* requests (201 vs 205), so it looked like an
Azure/adlfs problem.

## Root cause: two measurement bugs and one real cost, not Azure

Evidence: `scripts/profile_listing.py` (counts real HTTP requests at the
aiohttp/urllib3 transport via `generic_transfer.wire`), `scripts/probe_client_cost.py`,
and a cProfile of the old path. Snapshots: `bench-out/PROFILE_LISTING.md`,
`bench-out/PROFILE_LISTING_cprofile.txt`, `bench-out/CLIENT_COST.md`.

1. **The generic copier made ~2x the HTTP requests, on every provider (real cost).**
   `list_transfer_keys` called `is_folder_marker` -> `fs.info()` (one HEAD) for
   every listed key, and the copy loop then called `fs.open()`, whose file
   object issues another HEAD to learn the size before the GET. The old
   "Reqs" column was a *logical* tally (list/get/put), so those HEADs were
   invisible. Wire counts for 1000 objects (`PROFILE_LISTING.md`):

   | Pair | naive HTTP | generic per-key (old) HTTP | generic single listing (new) HTTP |
   |---|---:|---:|---:|
   | `azure->gcs` | 2034 | 4022 (2011 HEAD) | 2002 (1 HEAD) |
   | `azure->s3` | 2034 | 4022 (2011 HEAD) | 2002 (1 HEAD) |
   | `s3->gcs` | 2024 | 4024 (2011 HEAD) | 2002 (0 HEAD) |
   | `gcs->s3` | 2024 | 6325 (2013 list + 2312 metadata) | 2002 |
   | `s3->azure` | 2024 | 4024 (2011 HEAD) | 2002 (0 HEAD) |
   | `gcs->azure` | 2024 | 6340 (2013 list + 2327 metadata) | 2002 |

   On gcsfs, `info()` on a key is a list call plus a metadata GET, so the old
   path was ~3x. The cProfile of the old `azure->gcs` path shows
   `is_folder_marker` at 4.26 s and adlfs `AzureBlobFile.__init__` (HEAD via
   `details`) at 4.76 s cumulative for 1000 objects.

2. **The naive baseline paid client construction per object (measurement bug).**
   `get_bytes_native` built a new boto3 / GCS / BlobServiceClient on every
   GET. Median overhead per object (`CLIENT_COST.md`): **S3 4.0 ms, GCS 1.8 ms,
   Azure 0.4 ms**. That penalty made naive look slow for S3 and GCS sources and
   hid issue 1 there; for Azure sources the penalty was small, so issue 1
   showed. That is why only `azure->*` looked bad.

3. **S3Mock slowed down during long runs (emulator artifact).** Every round
   wrote to a new destination prefix and nothing was deleted. S3Mock PUT
   latency grows with stored objects: 4.6 ms at 0, 29.4 ms at 10 000,
   69.3 ms at 20 000 (`EMULATOR_DEGRADATION.md`). Pairs that run later and
   write to S3 (`azure->s3` is the 4th pair) were penalised by bench order.
   fake-gcs-server (memory backend) also grows: PUT 1.2 ms -> 20.5 ms at
   20 000. Azurite stays flat (3.3 to 4.3 ms).

## Fix (commit "perf: classify folder markers from one listing ...")

* `list_transfer_entries(fs, prefix)`: one `find(prefix, detail=True)`;
  markers are classified from that listing only (zero bytes and
  `type == "directory"`, trailing `/`, or another listed key under it).
  No per-key `info()`.
* `_read_object`: when the listing says the object is <= 8 MiB, use one
  `cat_file` GET. s3fs `cat_file` issues a size HEAD when
  `max_concurrency > 1`, so pass `max_concurrency=1` where supported. A
  0-byte real file costs no request. Larger objects still stream via
  `fs.open()` (one HEAD + ranged GETs).
* `listing="per_key"` keeps the old path for before/after comparison.
* Bench fixes: naive reuses cached clients; native HTTP pools widened to 32
  (default 10 is below 16 workers); destination prefixes are wiped after each
  round, outside the timed region.

Behaviour change: a zero-byte key ending in `/` with nothing under it (an
empty "folder") is now treated as a marker and skipped. The per-key rule kept
it. Covered by `tests/test_listing_requests.py`.

## Before / after (1000 objects, median of 3, fresh emulators, `PROFILE_LISTING.md`)

| Pair | naive (s) | generic per-key, old (s) | generic single listing, new (s) |
|---|---:|---:|---:|
| `azure->gcs` | 4.369 | 9.187 | 4.630 |
| `azure->s3` | 6.421 | 10.912 | 6.308 |
| `s3->gcs` | 4.293 | 9.242 | 4.382 |
| `gcs->s3` | 4.249 | 7.302 | 4.534 |
| `s3->azure` | 6.650 | 11.834 | 6.952 |
| `gcs->azure` | 5.063 | 6.758 | 4.717 |

Listing phase alone for 1000 objects: Azure 2.244 s -> 0.193 s,
S3 2.658 s -> 0.092 s, GCS 1.042 s -> 0.018 s.

After the fix the generic sequential path makes the same number of GET/PUT
requests as naive, minus the markers, and its time is within a few percent
of naive on localhost (both are dominated by one GET + one PUT per object).
The real-RTT effect is in `BENCH_SCALE.md` -> `wire_latency`.

Reproduce: `just up && just profile-listing`, `.venv/bin/python scripts/probe_client_cost.py`,
`just probe-emulators`.

## Full-scale confirmation (1000 objects, median of 5, 2026-11-10)

The full run in `bench-out/BENCH_SCALE.md` confirms the same picture at
1000 × 4 KiB objects. After the listing and baseline fixes, generic
sequential is within a few percent of naive on every pair, including
Azure sources. The old per-key path (`generic_sequential_per_key`) is
still ~2× slower and issues ~2–3× the HTTP requests, which is the
behaviour that made Azure look uniquely bad in the quick run.

| Pair | naive med (max) s | generic sequential med (max) s | generic parallel_16 med (max) s | naive HTTP | generic HTTP | per-key HTTP |
|---|---:|---:|---:|---:|---:|---:|
| `s3->gcs` | 8.361 (8.638) | 8.772 (8.923) | 6.381 (6.414) | 2024 | 2002 | 4024 |
| `gcs->s3` | 8.701 (8.952) | 7.255 (7.637) | 5.145 (7.277) | 2024 | 2002 | 6050 |
| `s3->azure` | 13.693 (14.220) | 13.311 (14.164) | 8.268 (8.412) | 2024 | 2002 | 4024 |
| `azure->s3` | 12.872 (13.470) | 12.456 (12.668) | 8.612 (9.125) | 2034 | 2002 | 4022 |
| `gcs->azure` | 10.063 (10.339) | 9.053 (9.302) | 5.769 (6.038) | 2024 | 2002 | 6055 |
| `azure->gcs` | 10.220 (11.280) | 10.576 (10.769) | 7.410 (7.525) | 2034 | 2002 | 4022 |

Naive copies 1011 keys (it keeps the 11 folder markers); generic copies
1000. `azure->gcs` generic sequential is ~3.5% slower than naive; every
other pair is even or faster, and `workers=16` is faster on all six.
Azure is not a special case.

Reproduce: `just up && just bench-scale`.
