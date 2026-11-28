# singledispatch and hook Protocols

`functools.singledispatch` is a poor fit for choosing a copy strategy in this
repository. The experiment in
[examples/singledispatch_vs_protocol.py](../examples/singledispatch_vs_protocol.py)
is what that sentence is based on. `tests/unit/test_singledispatch_vs_protocol.py`
locks the observations. Both were run on CPython 3.11.18 and 3.12.3.

## What singledispatch does here

Registering `generic_transfer.contract.FileCopyable` does match a structural
implementer. `OnlyCopy` and `GCSHook` are not named in the registry. On 3.11
and 3.12, `singledispatch` still selects the `FileCopyable` function, because
it falls through to `issubclass` and `@runtime_checkable` Protocols are
structural on those versions. A new hook with `copy_files` does not need its
own `@register`.

`singledispatchmethod` has the same rule. The dispatch key is the first
argument after `self`, not the pair of arguments.

## Where it stops matching the operator

Two facts in the same run go the other way.

A hook that implements more than one protocol raises
`RuntimeError: Ambiguous dispatch` when both `FileCopyable` and
`BatchReadable` are registered. `S3Hook` in the example implements
`copy_files`, `read_batches`, and `write_batches`. `isinstance` is true for
all three at once. `GenericFileTransfer` then checks `FileCopyable` and
`GenericRecordTransfer` checks the two batch protocols. The same object is
passed to both operators. `singledispatch` has to pick one function and
refuses when two registrations match.

Legacy pair behaviour is not a property of one hook. `PAIR_POLICY` in
`shims/generic_all_pairs/pair_policy.py` gives `("s3", "s3")` marker policy
`copy` and layout `preserve`, and gives `("s3", "gcs")` marker policy
`skip_overlapping` and layout `join_source_key`. A registration on `S3Hook`
returns `s3-source` for an `S3Hook` destination and for a `GCSHook`
destination. A registration on `FileCopyable` returns `file` for every store
that implements `copy_files`, so it cannot tell those two rows apart either.
Encoding the pair means a second dispatch, or the table that is already
there.

The operator in `src/generic_transfer/operator.py` does not consult that
table. It asks which protocols each hook implements for the role it is about
to use. That is a set of `isinstance` checks, not a function of one type.
The path copy in `src/aip132/path_ops.py` does not dispatch on hook classes
at all: `copy_path` takes two storage paths and branches on `samestore`.

## Result

`singledispatch` can see a Protocol. It still does not select a strategy
from a source and a destination together, and it cannot ask two protocol
questions about one object. Those are the decisions this repository actually
makes. Nested `singledispatch` can spell a nominal pair, at one registration
per concrete class per side, which is the per-pair surface AIP-132 is
replacing.
