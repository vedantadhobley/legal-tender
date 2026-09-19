# Receipt reference endpoint safety

Status: accepted Go consumer of a completed reference join. Fixture, replay,
failure, race, vet and the retained [2024 gate](../audit/receipt-reference-topology-2026-09-12.md)
pass, including full endpoint readback and earlier report-policy comparisons. This is a step in
[participant publication](./receipt-participant-publication.md), not a contributor
graph or a cycle-wide conduit qualification result.

## Purpose and unchanged policy

An exact pair can also receive an invalid reference from a third occurrence.
That evidence must reach the proposed pair before the
[reviewed association policy](./receipt-report-association.md) can qualify it.
Checking the number of exact neighbors alone is insufficient.

The bounded report reviewer now calls `earmarkassociation.Decide` for the same
role, memo, ID-conflict and amount-comparison rules. Its policy version and wire
output stay unchanged. The shared function accepts already-verified topology and
source-role evidence; it neither discovers relationships nor resolves identities.

The new topology consumer prepares that topology from the
[complete-cycle reference artifacts](./receipt-reference-join.md). It contains no
committee-name, donor-name, transaction-suffix or amount exception. It creates
no amount, updates no raw facts and sets both eligibility flags to false.

## Scope and evidence

Require the exact `manifest.json` and expected reference calculation ID. Validate
its version, policy, scope, source/count conservation and immutable identities.
Bind the exact manifest bytes as well as its logical ID: lookup backing includes
filter-only candidates that the reference logical ID intentionally omits.

Read and verify all four retained streams before publication:

1. Read every reference decision. Mark a non-exact reference's source occurrence.
   For a valid report scope and nonempty back-reference, request every matching
   target occurrence by exact recipient/report/transaction key.
2. Merge those requests with the full retained lookup stream. Mark all matching
   target occurrences, not a chosen first member. Missing schedules, duplicate
   sources and ambiguous targets still carry invalid-incident evidence.
3. Rebuild exact neighbor counts from both directions of every incidence. Compare
   every endpoint with the earlier neighbor artifact. Reciprocal references stay
   one distinct peer. Retain the sole peer's ordinal only when exactly one exists.
4. Merge unsafe incidents and exact endpoints into one ordinal-ordered sparse
   stream. Require conservation and verified output before creating a manifest.

The output represents the union of exact-reference endpoints and invalid-reference
incident endpoints. An omitted source occurrence has neither kind of incident.
**Omission does not prove transaction-ID uniqueness.** The reference join is complete
for requested keys, not every transaction key in the cycle. In particular, an
unreferenced duplicate outside requested keys cannot be assigned the bounded
reviewer's duplicate-specific reason from this index alone. A later all-receipt
consumer must preserve that unassessed state or obtain complete key membership;
it must not manufacture uniqueness or silently widen the policy's scope.

A locally safe one-peer endpoint is not a qualified conduit. Its peer can be
unsafe or shared, and neither endpoint's source role is present in this stream.
Both endpoints and the retained source-role fields must pass the shared policy.
Contributor appearance preservation is independent of positive qualification.

## Bounded representation

Two independent passes run concurrently: exact-incidence/neighbor verification,
and decision/invalid-target propagation. They share one physical workspace cap.
Each key group and adjacency list is streamed, not loaded into memory. Sort runs
remain at most 100,000 records and 32 MiB encoded; merge fan-in is at most 16.
This consumer does not rescan or copy source Parquet. It does not rerun the
bounded report inspector once per report.

The output uses the existing checked zstd sort-stream container. Each endpoint
has an eight-byte big-endian ordinal key, zero tag, and matching record ordinal.
Its 33-byte payload contains four big-endian uint64 values followed by one byte:

- Distinct exact peer count.
- Sole peer ordinal, or zero unless the count is one.
- Incoming exact reference count.
- Outgoing exact reference count.
- Unsafe reason bits: `1` for an invalid own reference; `2` for being a matching
  target of an invalid reference. Both bits may be present.

`DecodeEndpoint` validates the representation and topology bounds. Full source
fields stay in the original fact at its exact source ordinal. This internal
access stream is not a replacement source format or a public graph schema.

Logical identity binds executable, policy, exact reference manifest and source
identities, cycle, scope, populations and endpoint value digest. Run size,
fan-in, physical filenames and timings do not change it. Failed attempts keep
their remaining workspace without a success manifest. Only workspace-owned
temporary files are removed after their replacement passes readback.

## Command and remaining boundary

```bash
legal-tender pipeline fec build-receipt-reference-topology \
  --reference-manifest <reference-directory>/manifest.json \
  --expected-reference-id <exact-calculation-id> \
  --output-dir <new-directory> \
  --run-rows 100000 --merge-fan-in 8 --max-workspace-bytes 4294967296
```

Run with the input directory read-only, no network, and an explicit process
memory/CPU budget. The initial gate uses the existing temporary eight-CPU/4 GiB
container budget with `GOMEMLIMIT=2GiB`. No standing service budget changes.

The [participant index](./receipt-participant-index.md) now has an accepted full
2024 gate. The [conduit publisher](./receipt-conduit-publication.md) joins its role
evidence to this topology; its complete 2024 gate and replay pass and are retained.
Next, attach the
qualified associations to committee ancestry and candidate authorization.
Source membership, readback, graph import/storage and
candidate-path gates still apply. Neither Dagster activation nor terminal-dollar
allocation is part of this topology consumer.
