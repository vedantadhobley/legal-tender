# Committee-flow publication and readiness

Go implements immutable publication and exact-input readiness for the
[observation graph](./arango-committee-flow-evidence.md). The separate graph
command is now implemented; these publication commands do not mutate it.
The [thin Dagster chain](./committee-flow-orchestration.md) now invokes the
publication, readiness, and graph commands with exact upstream manifest paths.
Economic-flow resolution remains unimplemented.
The [complete 2024 gate](../audit/committee-flow-publication-2026-09-08.md)
passes with exact prior-result equivalence and 15.738-second publication reuse.

## Calculation publication

`publish-committee-flow-reconciliation` takes explicit Schedule A/B fact
manifests, a coordinated source release, and a cycle. It reuses the unchanged
[calculation and result contract](./committee-flow-reconciliation.md), including
the calculation ID, source-indexed evidence, exact signed amounts, and
`graph_eligible=false`. No publisher-specific wrapper or second calculation
schema is needed: the validated result itself is the immutable manifest.

Under the storage root, the publisher owns
`calculations/fec/committee-flow-reconciliation/v1/`:

- `manifests/<calculation-id>.json`: deterministic, immutable result JSON.
- `evidence/<calculation-id>/...`: the unchanged A/B and component artifacts.
- `current/<cycle>.json`: an atomic copy of the verified immutable result.

Evidence keys in the result resolve against this publication directory, not
against the source storage root. The manual calculation retains its explicit
output root and does not update these pointers. A retained audit result or
completion marker is not a published input.

Every invocation verifies exact A/B fact manifests, all Parquet shard hashes,
and original/coordinated release ancestry. With a new calculation identity,
Go decodes the policy columns, calculates candidate components, writes compact
evidence, and checks complete readback. It publishes the immutable result,
replays that result and its compact evidence, and then advances the pointer.

With an existing identity, it verifies and replays the compact evidence but
does not decode source rows or run source selection again. This is **no
source-row scan**, not zero I/O: shard hashing still reads stored Parquet bytes.
Worker count and runtime do not affect identity. A policy/input change does.
The existing calculation ID includes the coordinated release; reuse across a
different coordinated release is not implemented even if relevant source
bytes remain unchanged. Do not claim finer invalidation than this boundary.

Per-cycle advisory locks serialize publication. Immutable files use an atomic
create-only link and reject unequal bytes on collision. Pointers use fsync and
atomic rename. An interrupted immutable-before-pointer publication can recover
by validating the immutable result and advancing the pointer without rescanning.
Failed work cannot publish a ready pointer. Existing graph pointers are untouched.
Run diagnostics stay on stderr; stable versioned results stay on stdout.

## Observation readiness

`publish-committee-flow-evidence-bundle` requires a published calculation,
committee-master facts, and the expected cycle. Its
[bundle contract](../../contracts/bundles/fec/committee-flow-evidence/v1/)
pins exact calculation bytes, A/B inputs, coordinated source ancestry, and
committee-master fact identity. The bundle itself is deterministic and stored
under `bundles/fec/committee-flow-evidence/v1/`, using the same immutable/current
publication boundary.

The committee check walks from facts to their immutable occurrence manifest
and original releases. It compares the archive SHA/size and selected member's
compressed/uncompressed SHA/sizes against the coordinated release. An older
master is allowed only for identical selected bytes. A cycle label or common
ancestor alone is insufficient. It also verifies the complete committee fact
artifact, row lineage, cycle, state, and conservation.

V1 accepts only same-cycle masters. Historical identity automation remains
deferred; this bundle does not change the existing identity-aware v2 graph.
The observation graph labels absent masters as unresolved. No
identity in this boundary is eligible for terminal attribution.

`verify-committee-flow-evidence-bundle` performs read-only loading: validate
pointer/immutable equality, replay candidate evidence, verify all source
backing and master ancestry, then rebuild and compare the exact bundle. It
resolves immutable paths only, so advancing an upstream current pointer cannot
silently replace an already published bundle's inputs.

Readiness certifies compatible verified inputs for an observation-only
consumer. It does not certify complete endpoint coverage, actual payment
identity, source truth, or an already-built graph. Each ledger stays separate.
