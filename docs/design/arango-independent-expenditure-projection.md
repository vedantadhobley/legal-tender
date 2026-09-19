# ArangoDB independent-expenditure projection

> **Status:** The reported-ID v1 probe remains historical. The resolved-ID v2
> calculation, bundle, and graph pass unchanged for 2020, 2022, 2024, and
> 2026, and v2 is the active automation target. These remain isolated physical-
> model gates, not the unified production graph publication.

## Purpose

Project disclosed outside spending into a query-bearing graph without making
ArangoDB the evidence authority or treating the spending as money received or
controlled by a candidate.

The immutable Schedule E facts and effective calculation remain authoritative.
ArangoDB holds the smaller derived structure needed to find spender-to-candidate
paths, inspect candidate neighborhoods, and summarize support and opposition.

## Exact input boundary

The active resolved readiness bundle binds these immutable inputs from one
cycle and coordinated FEC release:

1. One published resolved independent-expenditure aggregate.
2. Its exact dense candidate-resolution calculation and Schedule E ancestry.
3. The exact candidate-master fact set used by candidate resolution.
4. One committee-master fact set from that cycle and release.

The bundle ID hashes the ordered input roles, every set identity, and every
manifest digest. It contains lineage and readiness evidence, not copied data.
The projection ID independently hashes the projection version, cycle, every
set identity, and every manifest digest, so adding the bundle does not change
an existing graph identity. A mixed-release or mixed-cycle input fails before
database creation. Dagster passes the bundle path and records the returned
identities; Go verifies artifacts, derives documents, performs arithmetic,
imports the graph, and runs the checks.

The publisher writes one immutable manifest plus an atomic cycle pointer. An
identical publication returns the original manifest. Dagster maps the same-
cycle resolved calculation and exactly the `candidate-master` and
`committee-master` fact partitions into this bundle, then eagerly targets the
v2 graph asset. The earlier effective-calculation bundle remains historical
and manually runnable.

## Physical model

The isolated named graph is `independent_expenditures`.

| Collection | Type | Grain |
|---|---|---|
| `entities` | Vertex | One referenced cycle-scoped candidate or spending committee. |
| `independent_expenditure_edges` | Edge | One resolved spender-candidate-stance calculation result. |
| `projection_metadata` | Document | One content-addressed completion record. |

Only entities referenced by an edge enter this projection. Master facts supply
display and classification attributes. If a referenced ID lacks a same-cycle
master fact, the projector writes an explicit `missing_master_fact`
placeholder and marks the result `partial`; it does not drop the edge or
invent identity data.

Each v2 edge points from spending committee to candidate and preserves:

- separate `independent_expenditure_support` or
  `independent_expenditure_oppose` semantics;
- exact signed decimal minor units;
- expenditure, positive, negative, and zero counts;
- confirmed, resolved, and unverified count and signed-amount components;
- result, resolved calculation, candidate-resolution, Schedule E fact-set, and
  source-release identity.

Projection metadata preserves the exact ambiguous and unresolved decision
count and signed amount left outside candidate edges. The dense upstream
decision artifact remains the complete identity evidence.

An edge is reported spending around a candidate. It is not a donation, a
candidate-controlled receipt, a literal payment to the candidate, or evidence
of coordination, benefit, corruption, or influence.

## Publication and replay

The v1 database name is `lt_ie_probe_<cycle>_<projection-id-prefix>`. V2 uses
`lt_ie_probe_resolved_<cycle>_<projection-id-prefix>`. Both satisfy the
restricted probe prefix; v2 always creates a new content-addressed database
and never relabels or mutates v1. The command creates collections and indexes
idempotently, imports deterministic documents in batches, reads all exact edge
amounts back, and writes `projection_metadata` last.

The metadata document is the completion marker. An identical retry verifies
the stored counts and exact amounts and reuses the database without importing
again. Immutable artifacts remain the rollback and rebuild authority.

## Query gate

The first probe repeatedly executes:

1. inbound named-graph paths for one representative candidate;
2. outbound named-graph paths for one representative spender; and
3. an exact candidate support/opposition summary.

These prove the outside-spending edge boundary. They do not prove PAC transfer
paths, terminal-source attribution, cycles, shortest paths, communities, or
centrality. Those require the receipt/disbursement flow and entity-resolution
projections.

## Measured results

### Active resolved-ID v2

The complete v2 projection contains 1,858 entities and 5,303 edges: 3,889
support and 1,414 opposition. It conserves $4,318,692,700.10 in exact signed
amounts: $1,809,660,053.46 support and $2,509,032,646.64 opposition. The 296
unresolved decisions and $18,549,639.21 remain explicit outside candidate
edges.

Expected and observed counts and amounts matched. Every referenced candidate
and spender has a same-release master fact, so the projection state is
`ready`. Ten-run query p95 latencies were below one millisecond, and an
identical replay reused the same database. Exact aggregate, bundle, graph,
storage, and query evidence is in the
[resolved 2024 audit](../audit/resolved-independent-expenditures-2026-08-31.md).

The unchanged v2 method also produced ready 2020, 2022, and 2026 projections
with zero missing masters, exact amount and count readback, sub-millisecond
representative query p95, and content-addressed replay. Across all four cycles,
201,250 source decisions conserve $10,358,126,288.01; 1,198 decisions and
$32,673,571.32 remain explicit outside candidate edges. See the
[cross-cycle audit](../audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md).

### Historical reported-ID v1

The complete 2024 projection contains 1,953 entities and 5,495 edges: 4,028
support and 1,467 opposition. It conserves $4,337,242,339.31 in exact signed
amounts: $1,819,924,212.53 support and $2,517,318,126.78 opposition.

All expected and observed counts and amounts matched. The projection uses
about 10.10 MiB across documents and indexes. It contains 92 explicit missing
candidate masters and no missing spender masters, so its state is `partial`.
All three query medians were below one millisecond, and an identical replay
reused the same database. Exact evidence is in the
[2024 projection audit](../audit/arango-independent-expenditure-projection-2026-08-31.md).
The subsequent readiness publication passed all six blocking checks and
reused this exact projection through a bundle-only invocation. See the
[2024 readiness-bundle audit](../audit/independent-expenditure-projection-bundle-2026-08-31.md).

The subsequent [candidate-reference audit](../audit/schedule-e-candidate-reference-integrity-2026-08-31.md)
found that the 92 placeholders cover 199 edges and $52,059,391.58. They are
unresolved source assertions, not simple missing metadata: 55 reported IDs
exist only outside the 2024 cycle, 37 have no official candidate record, and
some IDs contradict the reported name or office. The graph remains an
arithmetic and physical-model probe. Those 199 edge identities are not
production-safe until a per-fact candidate-reference calculation runs before
grouping.

The later candidate-resolution calculation classified all
58,288 attributed facts and conserved $4,337,242,339.31: 45,185 facts were
confirmed, 1,811 resolved by one exact context, 10,996 retained as unverified
reported IDs, none were ambiguous, and 296 remained unresolved. It provides a
projectable identity for 99.492177% of facts and 99.572317% of signed amount.
See the
[candidate-resolution audit](../audit/independent-expenditure-candidate-resolution-2026-08-31.md).

The existing 5,495 v1 graph edges still use the earlier reported-ID groups.
They remain valid historical physical and arithmetic evidence but are not
resolved graph output. The separate v2 projection above is the active result.

## Next gates

1. Audit candidate and committee history for the Schedule A receipt
   projection's cycle-scoped missing-master population.
2. Audit the 707 missing committee masters in the implemented receiver-
   reported flow graph, then add candidate-committee linkages at the unified
   graph boundary.
3. Audit Schedule B separately before sender-side flow reconciliation.
4. Merge accepted receipt, transfer, outside-spending, and entity projections
   behind one versioned publication boundary before claiming multi-hop source
   attribution.
