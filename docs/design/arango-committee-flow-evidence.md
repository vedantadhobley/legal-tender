# ArangoDB committee-flow evidence boundary

Status: implemented in Go and verified against the complete selected 2024
cohorts. See the [measured graph gate](../audit/arango-committee-flow-evidence-2026-09-08.md).
Existing receiver-flow and independent-expenditure graphs remain unchanged.
The preceding [publication and readiness boundary](./committee-flow-publication.md)
is implemented in Go.

## What the review establishes

The [candidate reconciliation](./committee-flow-reconciliation.md) preserves
related reports; it does not identify economic payments. Its connected
components can contain repeated annual contributions, one-to-many reporting,
in-kind descriptions under generic type codes, and signed corrections.
Equal component totals do not prove a single payment. A unique exact signature
does not establish cash movement or original funding ownership.

The next graph must therefore distinguish three things:

1. A receipt or disbursement observation selected by a named source policy.
2. A candidate reconciliation component linking possible related observations.
3. A later economic-flow assertion supported by an accepted resolution rule.

Only the first two exist. The third must not be manufactured by the projector.

## Isolated physical model

Use a new content-addressed projection and named graph,
`committee_flow_evidence`. Do not replace the current
[receiver-reported graph](./arango-receiver-reported-committee-flow-projection.md).
Start with the measured 2024 inputs; other cycles require their own fact and
calculation gates.

| Collection | Grain | Purpose |
|---|---|---|
| `entities` | One referenced committee in the selected cycle | Endpoints with explicit master/identity coverage |
| `receiver_reported_observations` | One selected A source occurrence | Directed reported source to receiving filer |
| `sender_reported_observations` | One selected B source occurrence | Directed filing sender to reported recipient/beneficiary |
| `reconciliation_components` | One candidate component, document only | Evidence membership, state, and separate ledger amounts |
| `projection_metadata` | One verified projection publication | Exact lineage, checks, query-gate results, completion state |

The two observation collections contain graph edges. A component is a
document, not another money-bearing edge or a traversal hop. Store its
directed endpoint IDs and index them for evidence lookup. Its fact references
resolve to the observation collections and immutable source artifacts.

No additional membership graph is needed for this boundary. A candidate
component already has complete A/B ordinal arrays. If later measured document
limits require partitioned membership, preserve the same logical identity and
count/readback contract; never silently truncate a component.

## Observation identity and fields

Derive edge identity from side, exact fact-set identity, and physical ordinal.
Never use a transaction label or cross-ledger `SUB_ID` equality as a payment
identifier. Preserve zero and negative observations separately.

Each edge carries:

- selected cycle, ledger side, exact source fact set, ordinal, and `SUB_ID`;
- source and recipient endpoint IDs, without name-based identity inference;
- exact signed minor units and nullable reported date;
- reported transaction code and policy-assigned role;
- calculation/policy identity and candidate-component ID; and
- explicit `economic_flow_status=not_established` and
  `terminal_attribution_eligible=false`.

Store the exact minor-unit amount as a canonical decimal integer string, as
the existing receiver-flow projection does, and require exact readback. Use
integer arithmetic in Go, not binary floating point.
Full source rows stay in Parquet. The Go source-review reader proves the
ordinal-based drilldown path. The [read-only API](./committee-flow-api.md) now
serves paginated observations, paths, component members, and full source rows.

A contribution or transfer category does not prove cash. The source review
found in-kind descriptions under generic codes. Do not derive
`cash=true`, beneficial ownership, or terminal status from the current role.
Candidate authorization, employer, corporation, and lobbying relationships
remain separate evidence domains.

## Reconciliation documents

Carry the unchanged assertion ID, matching method/version, exact member
identities, state, and separate A/B signed amounts. Store source references
even for one-sided evidence. The UI must distinguish:

- source codes or amounts disagreeing;
- dates differing under the candidate rule;
- several possible source counterparts; and
- no counterpart in these selected cohorts.

Do not label all these states as filing errors. Generic versus in-kind codes
can coexist with consistent narrative evidence. Two receipt items may sum to
one disbursement. The current matcher does not establish either interpretation
automatically.

## Query and counting boundary

Financially interpreted traversals must select one ledger explicitly. Keep
the existing receiver-reported view as the default until this new projection
passes its own gate. Do not run an unlabeled money path through both edge
collections. A diagnostic combined evidence view is permitted only when it
identifies each reported observation and offers no combined money total or
attribution result.

Do not sum edge amounts along a path: successive reports are not independent
funding dollars. Reconciliation membership cannot create a path, community
connection, shortest-path shortcut, or terminal source. A date filter uses
that observation's own reported date and must expose source-cycle coverage;
the review found a 2021 reported date inside a 2024 source partition.

An investigation panel may show A's reported subtotal and B's reported subtotal
side by side. It must not sum, average, minimize, maximize, or turn them into
an uncertainty interval for one alleged payment. Candidate groups do not yet
define a common economic quantity.

## Readiness and acceptance gates

Before creating a database, require a Go-owned immutable readiness bundle
that binds:

1. A persistently published reconciliation result and all evidence artifacts.
2. Exact A/B fact manifests and coordinated source-release ancestry.
3. Same-cycle committee-master facts selected by that coordinated release.
4. Any separately accepted historical identity evidence used by the projection.

The Go publisher now persists the unchanged result and its compact evidence;
the readiness bundle pins those bytes and same-cycle committee-master facts.
The retained manual audit's `complete` file is not a readiness pointer.
Allow unchanged source bytes from earlier fact publications only through the
existing explicit ancestry checks. Pin selected master bytes the same way;
do not accept a different master merely because its cycle matches.
Readiness v1 has `identity_scope=same_cycle_master_only` and accepts no
historical identity input. It does not imply complete master coverage; missing
endpoints stay unresolved in this graph. Historical identity
automation remains a separate accepted-refresh-policy prerequisite.

Require count and signed-amount readback for each ledger independently, every
selected occurrence exactly once, every occurrence linked to one component,
all component membership and amounts conserved, and endpoint coverage
explicit. Unknown endpoints stay unresolved; historical registration is not
current-cycle identity. No non-current identity becomes terminal eligible.

Measure import size, memory, idempotent replay, source drilldown, one-sided
evidence lookup, bounded single-ledger paths, and cycle queries. Include a
test proving that candidate components and the opposite ledger cannot enter
a single-ledger money path. Use explicit container/internal memory caps under
the project's infrastructure contract when that implementation is proposed.

The accepted gate now supports the [thin Dagster chain](./committee-flow-orchestration.md)
for publication, readiness, and projection. Economic-flow resolution, split-report reconciliation,
cash/in-kind classification, and terminal-source attribution remain versioned
calculation work, not graph-import heuristics.

## Implemented Go command

```text
legal-tender pipeline fec probe-arango-committee-flow-evidence \
  --storage-root /storage --cycle 2024 \
  --projection-bundle <published-observation-readiness-manifest> \
  --endpoint http://legal-tender-dev-arango:8529 \
  --password-env ARANGO_PASSWORD
```

Credentials come from the configured environment; they do not belong in the
URL or command arguments. The client rejects embedded credentials and
redirects, and excludes transport/server error text from diagnostics.

Projection version is `legal-tender.arango.committee-flow-evidence.v1`.
The ID hashes that version, the exact bundle ID, and the bundle SHA-256,
with NUL-delimited parts. The database is
`lt_flow_evidence_<cycle>_<first-32-projection-hash-characters>`. Arango's
[traditional database names](https://docs.arango.ai/arangodb/stable/concepts/data-structure/databases/)
have a 64-byte limit; completion metadata retains and checks the full identity.
The default batch size is 5,000 documents; changing it does not change identity.

The command revalidates readiness before database creation. It checks every
component member against the selected occurrence set, endpoint orientation,
and separate signed amount. A per-projection storage lock serializes callers
that share the storage root. The named graph contains exactly the two edge
collections. Component endpoint and observation membership indexes support
evidence lookup without extra edges.

Full readback streams each collection in key order and compares every JSON
field with its source-derived expected value. Missing explicit false/null
fields and unexpected extra fields fail, as do altered source locators or
amounts. It does not trust a stored document digest. Final completion metadata
is written only after data readback, query isolation, full source drilldown,
and storage checks. A replay verifies completed data without replacing it;
an incomplete import can resume with deterministic bulk replacement. This is
an isolated gate, not a cross-host publication/serving lease.

The query gate uses [explicit edge collections](https://docs.arango.ai/arangodb/stable/aql/graph-queries/traversals/),
not the combined named graph. Requests have a maximum of eight hops and 25
results. Representative gates use three-hop ANY neighborhoods, four-hop
directed paths, bounded BFS hop-shortest paths, and discovered cycle samples.
Self-loop observations remain queryable. Every returned edge is checked for
ledger membership and endpoint continuity; shortest hops are independently
checked in Go. These samples are not exhaustive path enumeration or a graph-
wide acyclicity test. Every query has an explicit memory/runtime cap, and
cursor pages are streamed and closed on failure. No amount is a path weight.

`flowreconciliation.LookupSources` accepts 1–64 explicit side/fact-set/ordinal
locators. It rejects foreign or unselected locators, revalidates the published
calculation and complete source backing, then seeks the original full Parquet
rows. The graph gate reads locators from Arango before invoking this boundary.
This library boundary remains the standalone full-validation lookup. The
serving reader pins source membership/indexes at startup and verifies only
requested shards during lookup; its HTTP API is implemented but not deployed.

Thin Dagster publication/readiness/projection assets now pass isolated real
2024 execution and replay. Historical identity inputs, other-cycle gates,
economic-flow resolution, and terminal-source attribution remain separate work.
