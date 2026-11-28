# Final operator and the `*To*Operator` guard

Two static checks for the AIP-132 shape: one `FileTransferOperator`, not a new
class per source and destination.

## `@typing.final` on the class

`FileTransferOperator` in `src/aip132/file_transfer.py` is decorated with
`@typing.final`. A type checker then rejects every subclass. That includes a
subclass that overrides `execute` and a subclass that only adds a
backend-specific `__init__`.

`@final` on `execute` alone would not do that. An empty
`class S3ToGCSOperator(FileTransferOperator)` would still type-check, and so
would a subclass that overrides `__init__` and leaves `execute` alone. Those
are the per-backend operators this demo is replacing. The class decorator
rejects them. `typing.final` sets `__final__` on the class and returns it.
`execute` is unchanged.

`PrefixFileTransferOperator = FileTransferOperator` is an assignment. It is
not a subclass, so the alias stays the same class.

`just aip132-final-check` installs mypy into `.venv-aip132` and runs
`tests/unit/test_aip132_final.py`. The test runs mypy with
`--follow-imports=silent` and `MYPYPATH=src` on two files that pytest does not collect:

| File | Expected |
|---|---|
| `tests/typing/final_operator_rejected.py` | Exit non-zero. mypy reports `Cannot inherit from final class "FileTransferOperator"` on the `S3ToGCSOperator` line (that class overrides `execute`) and on the `GCSToS3Operator` line (that class only replaces `__init__`). |
| `tests/typing/final_operator_alias_ok.py` | Success. The alias is annotated as `type[FileTransferOperator]`. |

This repository has no mypy configuration. The recipe checks only those files.
Airflow already type-checks providers with the `mypy-providers` hook in
`providers/.pre-commit-config.yaml` (`entry: ../scripts/ci/prek/mypy.py`,
stage `pre-push`). `@final` on the upstream operator would be enforced there.
It does not need a new hook.

## Guard against a new `*To*Operator` class

`tools/pair_operator_guard` parses Python with the `ast` module. A class whose
name matches `*To*Operator` (`S3ToGCSOperator`, `SqlToS3Operator`, `ToOperator`)
fails the check unless the allowlist names it. The match is case-sensitive.

Ignored names:

| Name | Why |
|---|---|
| `FileTransferOperator` | No `To`. |
| `PrefixFileTransferOperator` | No `To`. |
| `S3ToGCS` | Does not end in `Operator`. |
| `GCSToGCSOperatorShim` | Does not end in `Operator`. |
| `AutoOperator` | The letters are `to`, not `To`. |

`tools/pair_operator_guard/allowlist.txt` is the list. A bare name allowlists
that class in every scanned file. `relative/path.py:ClassName` allowlists one
file. Comments and blank lines are ignored. `src/` in this repository defines
no matching class, so the committed list is empty.

`just pair-operator-guard` runs `tests/unit/test_pair_operator_guard.py`
(flagged, allowlisted, and non-matching names) and then:

```bash
python -m tools.pair_operator_guard src
```

The scanner does not import Airflow. A class built by assigning `__name__`
after the class statement is not a new class statement and is not reported.
The shims in this repository do that.

### Providers pre-commit

This repository has no `.pre-commit-config.yaml`, so the script is not
installed as a hook here. `just pair-operator-guard` is the runner. The CLI
is the hook entry Airflow's provider checks already use: paths on argv
(`pass_filenames: true`), one finding per line, exit 1 when a class is
outside the allowlist, exit 2 when a file does not parse.

Airflow's provider hooks live in `providers/.pre-commit-config.yaml` (local
hooks, `language: python`, scripts under `scripts/ci/prek/`).
`validate-operators-init` is the closest neighbour. It receives changed
files and limits itself to:

```text
^.*/src/airflow/providers/.*/(operators|transfers|sensors)/.*\.py$
```

A pair-operator guard would be another local hook beside that one, narrowed
to transfer modules, with the historical pair classes on the allowlist until
each becomes a wrapper:

```yaml
- id: check-no-new-pair-transfer-operator
  name: Reject a new *To*Operator transfer class
  language: python
  entry: ../scripts/ci/prek/check_no_new_pair_transfer_operator.py
  pass_filenames: true
  files: ^.*/src/airflow/providers/.*/transfers/.*\.py$
```

`mypy-providers` stays the check for `@final`. This script stays the check
that a new class statement matched the old pair name.
