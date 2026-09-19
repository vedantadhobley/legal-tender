# ArangoDB candidate-receipt projection

> **Status:** Version 1 probe implemented and measured against the complete
> 2024 candidate-receipt result. This is a physical-model gate, not the final
> money-flow ontology or a production publication.

## Decision

Keep large immutable source and fact relations in filesystem artifacts. Put
only query-bearing entities, relationships, monetary components, calculation
results, and projection metadata in ArangoDB.

The first projection proves that this boundary supports direct result lookup
and graph-neighborhood retrieval without copying 264 million Schedule A rows
into the graph. It does not yet claim donor-to-candidate paths, PAC transfer
paths, terminal-source attribution, or independent spending.

## Source authority

The rewrite still depends on official FEC bulk products. It changed which
product is authoritative for detailed receipts.

| Role | Current authority | Use in this projection |
|---|---|---|
| Candidate identity | Cycle `cn` bulk file | Candidate vertices and display fields |
| Committee identity | Cycle `cm` bulk file | Committee vertices and display fields |
| Candidate-committee relationship | Cycle `ccl` bulk file | Relationship state and supporting fact IDs |
| Candidate summaries | Cycle `weball` and `webl` bulk files | Independent reconciliation inside the candidate result |
| Detailed receipts | Processed Schedule A bulk dump | Exact itemized receipt calculation in Parquet; not copied row by row into ArangoDB |
| Classic detailed subsets | `indiv`, `oth`, and `pas2` | Comparison evidence only; not a silent fallback |

The classic `indiv` file is a threshold-dependent subset. It cannot replace
the full processed Schedule A relation. `pas2` mixes committee contributions
and independent expenditures and cannot define either complete source family
on its own.

Processed Schedule B remains the planned authority for committee outflows,
transfers, refunds, and the second side of committee flows. Processed Schedule
E remains the planned authority for independent expenditures. Neither is
represented by version 1 of this graph.

## Input identity

One projection binds these exact immutable inputs:

1. Candidate-receipt fact bundle.
2. Compact candidate-receipt calculation set.
3. Candidate-master fact set.
4. Committee-master fact set.

All inputs must share one cycle and source release. The calculation must name
exactly the four fact sets frozen by the bundle. The projector verifies the
result artifact it consumes. Its projection ID hashes the model version and
every input ID plus manifest digest.

## Physical model

The named graph is `candidate_receipts`.

| Collection | Type | Grain |
|---|---|---|
| `entities` | Vertex | One cycle-scoped candidate or committee master entity |
| `candidate_committee_relationships` | Edge | One disclosed committee-to-candidate relationship, including authorized, unauthorized, or unresolved state |
| `receipt_components` | Edge | One calculated signed itemized-individual receipt subtotal from an authorized committee to its candidate |
| `candidate_results` | Document | One complete versioned candidate receipt result |
| `projection_metadata` | Document | One content-addressed projection identity, input set, and expected collection counts |

Both edge collections run from committee to candidate. This direction answers
the current inbound-candidate neighborhood query. A receipt-component edge is
a calculated candidate attribution through an authorized committee; it must
not be described as a literal committee payment to the candidate.

Every projected document has a deterministic `_key`, schema version, cycle,
lineage identity, and document digest. Money remains a signed decimal minor-
unit string. The graph does not coerce exact money into a binary floating-point
amount.

Source IDs used by a calculation can be absent from a cycle master file. The
projector emits an explicit `missing_master_fact` placeholder vertex and marks
the probe partial. It does not discard the calculation relationship or invent
master attributes.

## Filesystem and graph boundary

Parquet remains the canonical query layout for individual Schedule A facts.
It supports complete evidence replay, column pruning, and future donor-level
drilldown without making each wide fact a graph document.

ArangoDB holds the smaller structures that justify graph storage:

- traversable entity relationships;
- candidate-facing monetary components;
- complete calculation results used by the API;
- the exact lineage needed to resolve those components back to immutable
  filesystem evidence.

A later donor/entity projection may add coarse donor-to-committee graph edges
that reference underlying Parquet fact ranges or indexes. That design requires
measured investigative queries and identity-resolution rules first.

## Publication and recovery

The probe never writes the legacy `legal_tender` database. It creates an
isolated database named from the cycle and the first 16 hexadecimal characters
of the projection ID.

Collections and indexes are created idempotently. JSON Lines imports use
deterministic documents and replacement for the same keys inside this isolated
content-addressed database. Expected collection counts are checked before
`projection_metadata` is written. Metadata is the completion marker and is
published last.

An interrupted database has no completion marker. A retry resumes deterministic
imports. A completed database with different metadata or collection counts is
rejected. No automatic operation drops or truncates a database.

## Accepted probe queries

Version 1 measures:

1. Direct candidate-result lookup by document ID.
2. One-hop inbound receipt-component traversal from candidate to its
   authorized committees through the named graph.

These queries prove the current physical boundary only. All-path,
shortest-path, cycle detection, community, and centrality benchmarks require
the Schedule B committee-flow layer and donor/entity projection. Measuring
them against the current one-hop graph would produce meaningless confidence.

## 2024 result

The complete 2024 probe projected 31,035 entities, 8,175 candidate results,
8,584 candidate-committee relationships, and 2,116 receipt components. Every
collection count matched the deterministic build. It identified 143 candidate
and 156 committee placeholders caused by IDs absent from the respective cycle
master facts, so the result is explicitly partial.

The subsequent [master-gap audit](../audit/receipt-master-gaps-2026-08-31.md)
reproduced all 299 placeholders from immutable inputs. It found 298 zero-
dollar relationship or summary assertions and one two-record −$8.4 million
adjustment component. Neither a different 2024 master snapshot nor the other
active-cycle masters provide a valid general backfill.

ArangoDB reported 132,727,296 document bytes and 7,427,295 index bytes across
the five projection collections, for about 133.66 MiB combined.

Across 25 measured executions after one warm-up, direct lookup had a 92 µs
median and 142 µs p95. The inbound neighborhood had a 177 µs median and 299 µs
p95. A second run reused the completed projection and preserved every count.
The exact identities and measurements are in the
[2024 projection audit](../audit/arango-candidate-receipt-projection-2026-08-31.md).

## Next gates

1. ~~Publish an exact-input readiness bundle for the implemented receiver-
   reported committee-flow calculation, then project its grouped Schedule A
   results and benchmark real multi-hop paths and cycles.~~ See the
   [projection design](./arango-receiver-reported-committee-flow-projection.md).
2. Audit and contract processed Schedule B, official committee history, and committee
   summaries before sender-side flow reconciliation.
3. ~~Publish the accepted Schedule E source through release-inventory v2,
   implement its effective-record calculation, and measure the evidence-backed
   support/oppose edge projection without adding outside spending to candidate-
   controlled receipts.~~ See the
   [projection design](./arango-independent-expenditure-projection.md).
4. Pin the current ArangoDB 3.11 image to an exact patch or digest, then test
   3.12 storage compatibility in a separate change. Do not combine an engine
   upgrade with the projection-model decision or add the separate Graph
   Analytics product without a measured need.

## References

- [ArangoDB HTTP API](https://docs.arango.ai/arangodb/stable/develop/http-api/)
- [ArangoDB import API](https://docs.arango.ai/arangodb/stable/develop/http-api/import/)
- [ArangoDB named graph API](https://docs.arango.ai/arangodb/3.11/develop/http-api/graphs/named-graphs/)
- [ArangoDB AQL cursor API](https://docs.arango.ai/arangodb/stable/develop/http-api/queries/aql-queries/)
