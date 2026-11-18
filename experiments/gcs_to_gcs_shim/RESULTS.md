# GCSToGCS shim experiment results

Scored against the pre-registered criteria in `THRESHOLD.md`
(commit `d94c096`, written **before** any shim run).

## Verdict

**Success.** All three pre-registered criteria hold.

This is not a claim that a from-scratch generic listing policy is a
drop-in for `GCSToGCSOperator`. It is a claim that, with the pair's
listing and destination-key policy expressed against a small file
contract, and with GCS server-side copy implemented as
`GCSHook.rewrite`, the unmodified upstream unit tests pass at the
threshold. Caveats are at the end; they do not move the pre-registered
goalposts.

## Run setup

- Airflow SHA (operator + tests): `2b59699b0fe9683c418806a334b47077513bde5a`
- Test file (unmodified): `providers/google/tests/unit/google/cloud/transfers/test_gcs_to_gcs.py`
- How the class was swapped: `run_upstream.py` loads the SHA-matched
  `gcs_to_gcs.py` over the pip-installed provider module, then replaces
  `GCSToGCSOperator` with the shim **before** pytest collects the test
  module. The tests themselves were not edited.
- Environment: dedicated venv at `experiments/gcs_to_gcs_shim/.venv`
  (`apache-airflow-core==3.4.0`, `apache-airflow-providers-google==22.9.0`,
  `pytest==9.2.0`). The operator module under test is the SHA overlay,
  not the 22.9.0 wheel.
- The providers-only checkout has no `tests_common.pytest_plugin`.
  Pytest was run with `--noconftest` and a local `pytest.ini` so
  collection would not fail on that missing plugin. The google tests
  `conftest.py` only loaded that plugin and applied a Werkzeug version
  shim; these tests do not need other fixtures from it.
- Airflow 3 operator logs go through structlog to stdout, so pytest
  `caplog` did not see them. A first run of
  `test_executes_with_source_objects_not_found` failed for that reason
  on **both** the shim and the original operator (the expected warning
  was present on stdout). `run_upstream.py` then configured a
  structlog-to-stdlib bridge, equivalent in purpose to the missing
  plugin. After that, the test passed. This is harness setup, not a
  test edit and not a change to the operator.
- Command: `just experiment-gcs-to-gcs` (3-minute timeout). No
  emulators were started. Elapsed pytest time for the scored run: 0.77s.
- Scored run wall time (Asia/Yerevan): 2026-11-10, about 17:21.

## Pass rate (criterion 1)

Pre-registered corpus: **45** test methods on
`TestGoogleCloudStorageToCloudStorageOperator`. Parametrization
expands that to **81** collected pytest items
(notably `test_copy_files_into_a_folder` 24 ways,
`test_get_openlineage_facets_on_complete` 10 ways,
`test_execute_wildcard_prefix_and_suffix` 3 ways,
`test_copy_single_file_retention` 3 ways).

| Scoring | Passed | Total | Rate |
|---|---:|---:|---:|
| Pre-registered (test methods) | 45 | 45 | 100% |
| Collected pytest items | 81 | 81 | 100% |

Threshold was ≥ 80% (36/45 methods). **45/45 ≥ 36/45.** No skips,
collection errors, or environment errors in the scored run.

## Failure categories (criterion 2)

No failures in the scored run, so this criterion holds vacuously
(there is nothing to map). Categories from `THRESHOLD.md` were not
needed.

### Hook-call-shape coupling (observed, not a failure)

35 of the 45 test methods assert on specific `GCSHook` call shapes
(`list`, `rewrite`, `exists`, `delete`, `is_updated_*`, `is_older_than`
via `assert_called_once_with` / `assert_has_calls` / similar). Those
tests measure **implementation coupling**, not just observable copy
behaviour. They passed because `GcsHookBackend` delegates to those
same hook methods with the same arguments the pair operator uses
(including omitting `match_glob` on wildcard `list` calls, and passing
`retain_until_time` / `retention_mode` through to `rewrite`).

A backend that copied correctly via download/upload, or via a
different hook method, would have failed many of these tests even with
identical object outcomes. That risk was pre-registered in
`THRESHOLD.md`. It did not materialise **because the shim was written
to the hook surface the tests already assert on.**

## Constructor comparison (criterion 3)

The shim subclasses the SHA-matched `GCSToGCSOperator` and does not
define its own `__init__`. Public arguments are therefore identical:
the same 18 named parameters plus `**kwargs` for `BaseOperator`
fields such as `task_id`.

| Argument | Pair | Shim |
|---|---|---|
| `source_bucket` | yes | yes (inherited) |
| `source_object` | yes | yes |
| `source_objects` | yes | yes |
| `destination_bucket` | yes | yes |
| `destination_object` | yes | yes |
| `delimiter` | yes | yes |
| `move_object` | yes | yes |
| `replace` | yes | yes |
| `gcp_conn_id` | yes | yes |
| `last_modified_time` | yes | yes |
| `maximum_modified_time` | yes | yes |
| `is_older_than` | yes | yes |
| `impersonation_chain` | yes | yes |
| `source_object_required` | yes | yes |
| `exact_match` | yes | yes |
| `match_glob` | yes | yes |
| `retain_until_time` | yes | yes |
| `retention_mode` | yes | yes |
| extra public args beyond the pair | none | **none** |

Criterion 3 asks that the shim expose **no more** public arguments
than the pair. It does not ask for a *smaller* generic constructor.
This experiment reused the pair's 18 arguments; it does not demonstrate
that a slimmer generic API would suffice.

## What the shim actually does

- `contract.py`: `ObjectLister` (with marker policy on `ListQuery`),
  `ObjectExists`, `ObjectReader`, `ObjectWriter`, `ServerSideCopy`,
  `ObjectDeleter`, `MtimeFilter`.
- `gcs_backend.py`: `GcsHookBackend` implements those by calling
  `GCSHook.list` / `exists` / `rewrite` / `delete` / mtime helpers.
  `rewrite` is the `server_side_copy` capability. Reader/writer exist
  on the contract but are not used on this path.
- `listing.py`: pair-specific listing and destination-key policy,
  **ported** from `GCSToGCSOperator` at this SHA (wildcard split,
  `exact_match`, deprecated `delimiter`, `match_glob`, `replace=False`
  destination-list subtraction, empty-list-then-`exists` fallback,
  `source_object_required`).
- `transfer.py`: generic copy loop (optional mtime filter, server-side
  copy, optional delete for move, destination URIs).
- `shim.py`: `execute()` validates arguments the same way, plans
  `TransferItem`s through that policy, then calls `transfer_objects`.
  `template_fields`, OpenLineage, and deprecation warnings stay on the
  upstream base class.

## Per-method results (all passed)

| Test method | Items | Result |
|---|---:|---|
| `test_execute_wildcard_prefix_and_suffix` | 3 | pass |
| `test_execute_wildcard_with_replace_flag_false` | 1 | pass |
| `test_execute_no_wildcard_with_replace_flag_false` | 1 | pass |
| `test_copy_file_with_exact_match` | 1 | pass |
| `test_copy_file_with_exact_match_destination` | 1 | pass |
| `test_execute_wildcard_with_destination_object` | 1 | pass |
| `test_execute_wildcard_with_destination_object_retained_prefix` | 1 | pass |
| `test_execute_wildcard_without_destination_object` | 1 | pass |
| `test_execute_wildcard_empty_destination_object` | 1 | pass |
| `test_execute_wildcard_with_destination_object_rename` | 1 | pass |
| `test_wc_with_last_modified_time_with_all_true_cond` | 1 | pass |
| `test_wc_with_last_modified_time_with_one_true_cond` | 1 | pass |
| `test_wc_with_no_last_modified_time` | 1 | pass |
| `test_no_prefix_with_last_modified_time_with_true_cond` | 1 | pass |
| `test_no_prefix_with_maximum_modified_time_with_true_cond` | 1 | pass |
| `test_exe_last_modified_time_and_maximum_modified_time_with_true_cond` | 1 | pass |
| `test_execute_no_prefix_with_no_last_modified_time` | 1 | pass |
| `test_no_prefix_with_last_modified_time_with_false_cond` | 1 | pass |
| `test_executes_with_is_older_than_with_true_cond` | 1 | pass |
| `test_execute_more_than_1_wildcard` | 1 | pass |
| `test_execute_with_empty_destination_bucket` | 1 | pass |
| `test_executes_with_empty_source_objects` | 1 | pass |
| `test_raises_exception_with_two_empty_list_inside_source_objects` | 1 | pass |
| `test_executes_with_single_item_in_source_objects` | 1 | pass |
| `test_executes_with_multiple_items_in_source_objects` | 1 | pass |
| `test_executes_with_source_objects_not_found` | 1 | pass |
| `test_executes_with_source_objects_failed_delete` | 1 | pass |
| `test_executes_with_a_delimiter` | 1 | pass |
| `test_executes_with_delimiter_and_destination_object` | 1 | pass |
| `test_executes_with_different_delimiter_and_destination_object` | 1 | pass |
| `test_executes_with_no_destination_bucket_and_no_destination_object` | 1 | pass |
| `test_wc_with_last_modified_time_with_all_true_cond_no_file` | 1 | pass |
| `test_execute_wildcard_with_replace_flag_false_with_destination_object` | 1 | pass |
| `test_execute_source_object_required_flag_true` | 1 | pass |
| `test_execute_replace_false_skips_single_file_when_destination_exists` | 1 | pass |
| `test_execute_replace_false_copies_single_file_when_destination_missing` | 1 | pass |
| `test_execute_replace_true_copies_single_file_even_when_destination_exists` | 1 | pass |
| `test_copy_files_into_a_folder` | 24 | pass |
| `test_get_openlineage_facets_on_complete` | 10 | pass |
| `test_execute_returns_list_of_destination_uris_single_file` | 1 | pass |
| `test_execute_returns_list_of_destination_uris_multiple_files` | 1 | pass |
| `test_wildcard_deprecation_warning_uses_rendered_source_object` | 1 | pass |
| `test_execute_returns_empty_list_when_no_files_copied` | 1 | pass |
| `test_execute_excludes_directory_uri_from_return` | 1 | pass |
| `test_copy_single_file_retention` | 3 | pass |

## Caveats

1. **Listing policy was ported, not invented.** Wildcard splits,
   destination prefix rewriting, `replace=False` dest-list subtraction,
   and the empty-list-then-exists fallback live in `listing.py` as
   GCSToGCS-specific policy. The generic piece is the copy loop and the
   contract. A production generic operator would still need an
   equivalent listing policy per source kind. This result does not
   show that one shared lister API already replaces every file→file
   pair.
2. **Passing hook-shape tests is circular if the backend is written to
   those hooks.** 35/45 methods would have been `hook_call_shape`
   failures under a different (still correct) implementation. The high
   pass rate should not be read as "the tests are behavioural."
3. **Constructor identity is from subclassing**, not from discovering
   a smaller generic API.
4. **No live GCS and no emulator** in this run. Behavioural parity
   against a real bucket or `fake-gcs-server` was not part of the
   pre-registered criteria.
5. **Environment is a pip Airflow 3.4.0 + google provider 22.9.0 venv**
   with a SHA-matched operator/test overlay. Hook *implementations*
   come from 22.9.0; the tests mock the hook, so that should not
   matter for this corpus, but it is not a full providers-from-SHA
   install.
6. **`source_object_required` and URI-return rules** (called out in
   `THRESHOLD.md` as not named catalog capabilities) happened to pass;
   they were implemented in the ported policy, not added as new catalog
   flags.

## Recommendation

Against `THRESHOLD.md` the experiment **succeeds**, so it does not
trigger the pre-registered "fail → option (c)" recommendation.

The caveats still matter for the broader design discussion: the
evidence is that a generic copy path *plus a pair-shaped listing
policy* can stand in for `GCSToGCSOperator` under this test corpus.
That is consistent with keeping thin pair operators that supply
listing/dest-key policy and shared helpers for the copy loop — which
is the shape of option (c) — even though this particular run cleared
the numeric bar.
