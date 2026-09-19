# ArangoDB receiver-reported committee-flow projection

> **Status:** The immutable v1 flow boundary and additive identity-aware v2
> bundle and graph pass the complete 2024 gate. Dagster still targets v1 while
> v2 remains an explicit manual gate. This is not yet the unified production
> graph or terminal-source result.

## Purpose

Project accepted Schedule A committee-flow results into a graph that can answer
multi-hop path, neighborhood, shortest-path, and cycle questions without making
ArangoDB the evidence authority.

The immutable Schedule A facts and receiver-flow calculation remain
authoritative. ArangoDB stores the smaller query-bearing relationship
projection. The graph does not copy all Schedule A rows.

## Exact input boundary

The shipped v1 readiness bundle binds these immutable inputs from one cycle and
coordinated FEC release:

1. One published receiver-reported committee-flow calculation.
2. Its exact Schedule A fact-set ancestry.
3. One committee-master fact set from the same cycle and release.

The v2 bundle then binds the entire immutable v1 bundle plus one published
committee identity-coverage calculation. That calculation pins the endpoint
population, selected master, explicit different-release comparison masters,
normalized historical masters, and official raw historical archives. Its raw
archive references contain storage-relative replay keys, archive digests,
members, row digests, and parse issues.

Candidate linkage and summary facts are useful diagnostic audit evidence, but
they cannot change an exact-ID registration decision. They therefore do not
enter the identity calculation ID.

Each bundle ID hashes the ordered input roles, set identities, and manifest
digests. The projection ID separately hashes the projection version and the
same immutable lineage. Mixed cycles, mixed releases, mutable-pointer drift,
missing backing artifacts, or digest changes block projection before database
creation.

The active Dagster v1 chain maps the same-cycle calculation and exactly the `committee-master`
classic-fact partition into the bundle. Its eager automation then supplies the
bundle artifact to the graph asset. Python does not parse, group, or import the
data. The v2 calculation, bundle, and graph commands are additive and manual
until their source-boundary policy is promoted. Go performs every parse,
decision, import, readback, and query gate in both versions.

## Physical model

The named graph is `receiver_reported_committee_flows`.

| Collection | Type | Grain |
|---|---|---|
| `entities` | Vertex | One referenced, cycle-scoped committee. |
| `receiver_reported_flows` | Edge | One source-committee, recipient-committee, receipt-role calculation result. |
| `projection_metadata` | Document | One content-addressed completion record. |

Each edge points from the identified source committee to the reporting
recipient committee. It preserves:

- the accepted receipt role;
- exact signed integer cents;
- total, positive, negative, and zero receipt counts;
- result and calculation-set identity;
- Schedule A fact-set and coordinated-release identity.

Only referenced committees enter the graph. Version 2 gives each vertex one
explicit identity state:

| State | Meaning | Terminal-identity eligible |
|---|---|---:|
| `current_cycle_master` | Exact selected-cycle master assertion supplies current canonical fields. | Yes |
| `historical_registration` | Exact ID occurs in official historical evidence; assertions remain nested and canonical fields stay empty. | No |
| `alternate_release_registration` | Exact ID occurs only in a different same-cycle release. | No |
| `unresolved_reported_id` | Exact reported ID is absent from every audited master. | No |

Terminal-identity eligibility is a prerequisite for a later classifier, not a
terminal classification. Historical names never populate the vertex's current
canonical name. Every non-current state binds its exact calculation decision.
The projector preserves all edges and money regardless of identity state.

These edges are receiver-reported evidence. They are not sender-side Schedule B
facts. A later reconciliation may connect the two assertions, but it must not
turn them into two independent payments or silently select one as truth.

## Topology and query boundary

The projector computes topology from the complete deduplicated adjacency before
import:

- weakly connected components;
- strongly connected components;
- cyclic strongly connected components;
- committees contained in cycles;
- one representative multi-hop path; and
- one representative directed cycle when a cycle exists.

The live query gate executes:

1. a direction-agnostic neighborhood with depth 1 through 4 and a 25-row cap;
2. up to 25 ranked shortest paths between representative committees;
3. one directed shortest path; and
4. one directed cycle traversal when cycles exist.

Unrestricted enumeration of every simple path in a cyclic graph is not a safe
interactive default. It can grow exponentially and the first 2024 attempt hit
the 60-second query deadline. The production API may expose exhaustive path
enumeration only with explicit endpoint, depth, row, time, and memory budgets.
The default investigative surface uses ranked paths and bounded neighborhoods.

## Publication and replay

V1 database names use `lt_flow_probe_<cycle>_<projection-id-prefix>`. V2 uses
`lt_flow_probe_v2_<cycle>_<projection-id-prefix>`. Each command creates one new
content-addressed database, collections, indexes, and named graph. It imports
deterministic batches, reads every edge amount and role back through paginated
cursors, and writes `projection_metadata` last.

The metadata document is the completion marker. An identical retry verifies
stored counts and exact amounts and reuses the completed database. Immutable
filesystem artifacts remain the rebuild and rollback authority.

## Measured 2024 result

The v2 graph contains 8,397 committee vertices and 180,283 flow edges. It
conserves $4,672,820,179.49. Expected and observed counts, role counts, total
amounts, and role amounts match exactly.

The topology contains 92 weak components, 6,128 strong components, 35 cyclic
strong components, and 2,304 committees in cycles. Representative bounded
queries completed in sub-millisecond to low-millisecond time. The graph uses
about 187.68 MiB across documents and indexes.

Identity state readback matches exactly: 7,690 current-cycle masters, 675
historical registrations, zero alternate-release registrations, and 32
unresolved reported IDs. All 707 non-current states are terminal-identity-
ineligible. The result remains `partial` only because the 32 reported IDs have
no official registration evidence. They cover 50 receipts and $173,821.08.

See the [2024 readiness and graph audit](../audit/arango-receiver-reported-committee-flows-2026-09-01.md)
and [committee-master gap audit](../audit/receiver-flow-master-gaps-2026-09-01.md).
The additive v2 result is in the
[identity-coverage graph audit](../audit/arango-receiver-flow-identity-coverage-2026-09-01.md).

## Next gates

1. Audit and publish the sender-reported Schedule B fact and calculation
   boundary.
2. Reconcile receiver- and sender-side assertions without double counting.
3. Add candidate-committee linkage and accepted entity-resolution projections.
4. Define a versioned terminal-source stopping rule over the unified graph,
   with explicit unresolved and cyclic attribution states.
5. Promote v2 into Dagster only after the historical-source acquisition and
   refresh policy is an accepted asset boundary.
