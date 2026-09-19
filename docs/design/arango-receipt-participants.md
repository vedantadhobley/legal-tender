# Streaming receipt-participant graph

Status: expanded and compact bounded importers, live field equivalence, and
retained 100,000/million-row replay gates pass;
complete-cycle publication remains open.
The [dated gate](../audit/arango-receipt-participants-2026-09-12.md) owns real
acceptance, exact identities, performance and retention. This is an additive
implementation of the [connected-graph contract](./connected-funding-graph.md),
not a replacement for existing accepted graphs or a production cutover.

## Typed physical model

| Collection | Meaning and membership |
|---|---|
| `contributor_appearances` | One vertex per selected physical occurrence. Key is the existing fact-set/ordinal/role appearance identity. V1 embeds all 24 participant fields; v2 references the exact retained row. Both preserve source membership, conduit disposition and explicit unresolved recipient state. |
| `reported_receipts` | Appearance → reported recipient committee. Exact nullable signed cents, memo flag and source locator; financial eligibility remains false. Invalid recipient IDs retain the appearance without inventing an endpoint. |
| `reported_conduit_associations` | Appearance → qualified reported conduit committee. Preserves the original/related ordinals and decision; adds exactly zero money. |
| `candidate_authorization_context` | Committee → candidate, using the unchanged shared authorization policy and all its supporting linkage facts. Includes unauthorized/unresolved states; positive paths filter for authorized context. |
| `entities` | Exact reported committee/candidate IDs. Pins same-cycle master facts or preserves an explicit missing-master state. No name-based resolution. |
| `projection_metadata` | Exact input definitions and verified sample completion; never a current-generation pointer. |

The named graph is `receipt_evidence`. A reported receipt and a conduit association
are separate edge types, not a synthetic two-payment chain. A supporting memo
outside the selected range has a full source locator, not a fabricated graph
vertex. Source names, employer fields and the rest of each original fact remain
available through the [participant inspector](./receipt-participant-index.md).
Neither shared descriptions nor contribution thresholds merge appearances.

## Exact ancestry and scope

Require complete participant and conduit manifests with expected calculation
IDs. Validate and hash the same bytes. Pin their source fact identity, all
physical artifact descriptors and the original cycle. Require the immutable
Schedule A manifest and immutable committee, candidate and linkage manifests
from that exact source release. This first sample does not infer compatibility
across different releases; future integration must use the accepted explicit
reuse/readiness contract rather than silently taking newer masters.

Projection identity binds the schema, executable, all input identities/digests
and selected ordinal range. Worker count and batch size do not change it.
The isolated database uses `lt_receipt_sample_<cycle>_<identity-prefix>`; full
identity lives in completion metadata. Previous publications remain untouched.

The command accepts an explicit range of at most one million occurrences. Its
state is always `verified_bounded_sample_not_complete_cycle`. The original
complete-cycle denominator remains in the result. An isolated sample is not a
full contributor graph, an integrated A/B/E generation or terminal attribution.

## Bounded execution and verification

1. Verify source ancestry and complete source backing once per invocation.
   A scoped inspector reuses that verification for the source witnesses; every
   lookup still verifies its compact shard and complete source row.
2. Merge the source-ordered conduit stream with selected participant shards.
   Read intersecting participant shards to EOF. Read the entire conduit stream
   to EOF and recheck its physical/value hashes and outcome/amount censuses,
   including evidence outside the sample.
3. Preserve exact applicable decision membership. Encode JSON before borrowed
   Parquet values can be reused. Hash canonical document bytes in source order.
4. One producer feeds one queued batch and one to eight import/readback workers.
   Each batch has at most 5,000 documents and 8 MiB; each document at most 64 KiB.
   Smaller master/linkage context has explicit 100,000-row/identity caps.
   There is no receipt-population map or whole-cycle document slice.
5. Import with deterministic keys and complete batch checks. Read every field
   back by exact key, including null, false, precise cents and extra attributes.
   Check collection counts to reject extra documents outside submitted keys.
6. Check typed source-selected candidate and conduit paths when present. Compare
   graph source locators and participant fields with full retained source rows.
   Preserve explicit absence of a witness when the selected range has none.
7. Measure encoded bytes, process peak RSS, timing and collection figures.
   Publish completion last. An incomplete attempt may replace deterministic
   documents on retry; a completed replay only validates and cannot repair data.

A per-identity local lock prevents concurrent publishers on this host. Query
cursors have a 128 MiB memory cap, a 30-second runtime cap and bounded pages;
failure cancels workers and cleans up cursors. Endpoint credentials are separate,
redirects are rejected, and server/transport error text is not logged.

The source merge/JSON producer remains serial; imports and readback are parallel.
Four workers is not a claim of four-CPU or linear end-to-end speedup. Full-cycle
work must preserve the bounds and measure producer cost before adding concurrency.

## Storage boundary and next work

The expanded v1 physical model measures the cost of copying the complete
compact participant record into each appearance. The retained fact and participant
publications remain authoritative. The implemented `compact-v2` layout references
them without discarding source grain; its
[acceptance gate](../audit/arango-receipt-participants-compact-2026-09-12.md) records
complete selected membership, reconstruction and live v1 equivalence.

Only appearance payloads change. Replace `participant` with `source_row_ordinal`,
`inventory_component` and `source_route`. Keep the same source-grain key, fact-set
ID, conduit decision, connection states and false identity-resolution flag.
Receipt/conduit edges and all context documents retain identical projected fields.
The definition's schema version changes to `receipt-participant-sample.v2`,
producing a separate isolated database. `expanded-v1` remains available explicitly,
and is the existing benchmark command's compatibility default.

Every compact readback reconstructs the former appearance from the actual graph
document plus its exact participant row. Compare all fields, not only money or
successful links. Completion binds both physical and expanded-source document
digests. A missing graph field is not silently filled during reconstruction.

An optional `--compare-v1-result` plus `--expected-compare-sha256` adds full live
readback of the accepted expanded graph, requiring identical source ancestry and
range. It must pass before completion when requested; it never writes to that
graph. This extra comparison does not become an input dependency for future
full-cycle publication. Standalone compact replay still performs mandatory
source reconstruction and complete physical readback.

Physical bytes plus temporary reconstruction evidence share the same 8 MiB
batch cap. Source proof is held only in bounded worker buffers, not written
back into the compact graph. This leaves a source-backed field lookup boundary:
queries for fields removed from appearance documents must use the exact participant
or fact publication rather than a missing Arango `participant.*` attribute.

Encoded JSON byte totals are exact transfer payload measurements, not database
disk size. Retain collection figures separately; the vendor defines document
size there as an approximate on-disk measure, not a whole-database allocation
guarantee. See the [collection API](https://docs.arango.ai/arangodb/stable/develop/http-api/collections/).
Do not extrapolate this sample into a promised full-cycle disk footprint or runtime.

The compact acceptance gate passes with a 45.26% encoded-payload reduction on
the million-row sample, full live v1 equivalence and standalone replay.
The separate [full-cycle publisher](./arango-receipt-participant-cycle.md) now
implements storage admission, source-shard checkpoints and immutable publication.
Its full occurrence/edge conservation, replay and query gates remain the next
acceptance boundary. Attach the
existing committee ancestry and outside-spending families under exact generation
readiness. Do not enable weekly automation or call the connected graph complete
through this benchmark.

```text
legal-tender pipeline fec benchmark-arango-receipt-participants
  --storage-root ROOT --lock-dir SHARED_LOCK_DIRECTORY
  --participant-manifest PARTICIPANTS/manifest.json --expected-participant-id ID
  --conduit-manifest CONDUITS/manifest.json --expected-conduit-id ID
  --schedule-a-facts IMMUTABLE --committee-facts IMMUTABLE
  --candidate-facts IMMUTABLE --linkage-facts IMMUTABLE
  --endpoint ARANGO_ENDPOINT --password-env CONFIGURED_PASSWORD_VARIABLE
  --first-ordinal 1 --max-rows 100000 --workers 4 --batch-rows 1000
```

Python/Dagster wiring, identity resolution and financial selection are unchanged.

Select the lean layout with `--layout compact-v2`. For live v1 equivalence, also
supply `--compare-v1-result EXACT_RESULT --expected-compare-sha256 SHA256`.
