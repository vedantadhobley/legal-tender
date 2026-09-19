# Committee-flow observation graph gate — 2026-09-08

The isolated Go projection passes the complete selected 2024 cohorts from the
[published reconciliation and readiness bundle](./committee-flow-publication-2026-09-08.md).
It stores source observations and candidate-component evidence, not resolved
payments. Existing receiver-flow and independent-expenditure graphs remain
unchanged. No source download, raw-row rewrite, matching-rule change, or
record-specific exception was needed.

## Conserved graph contents

| Collection | Documents | Separate signed observation amount |
|---|---:|---:|
| `entities` | 9,545 | Not a money ledger |
| `receiver_reported_observations` | 320,731 | $4,672,820,179.49 |
| `sender_reported_observations` | 341,720 | $5,158,069,433.82 |
| `reconciliation_components` | 308,488 | Separate A/B evidence amounts only |

Those are the unchanged selected cohorts, not every Schedule A/B record.
Each physical selected source ordinal appears once in its ledger and belongs
to exactly one component. Parallel observations remain separate even when
reported labels, dates, or amounts repeat. A contains 687 negative and 68 zero
observations; B contains 9,698 negative and 43 zero observations. All signed
amounts remain decimal integer strings in Arango and exact integers in checks.
The two ledger totals must not be combined or presented as a payment interval.

The graph is `partial`: 8,431 referenced committees have a same-cycle master,
and 1,114 do not. Every absent master has explicit unresolved coverage and
null master fields. This is not a determination that the reported ID is
invalid. This bundle accepts no historical master input; the earlier receiver
identity-aware v2 graph remains separate. No entity or observation is eligible
for terminal attribution in this boundary.

The named graph has exactly two edge collections. Components are ordinary
documents with endpoint indexes and complete A/B ordinal arrays. No component
can become a traversal hop or shortest-path shortcut through the shipped
query boundary.

## Exact identity

```text
projection_id = 89f049d4604f20a204c0ef52eb7ee5c37bbb7b59a516a901be06742529ce3b3f
database      = lt_flow_evidence_2024_89f049d4604f20a204c0ef52eb7ee5c3
graph         = committee_flow_evidence
bundle_id     = 113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d
bundle_sha256 = d450dfdb2efca9b0ace973fcb3daf2218ad0775152183fd03cd93f34dbc9a62e
calculation   = 987626c6070c7e7f57db092ab108fb56d0ab97c6ac6a3f802dd277d9738d6ae8
```

The bundle pins exact A/B and same-cycle master ancestry. Projection identity
adds the graph-model version and exact bundle bytes. Changing batch size does
not change graph identity. The database suffix uses 32 hash characters to fit
Arango's traditional name limit; completion metadata retains the full hash.

An initial live attempt used an overlong database name. Arango rejected it
with error 1208 before collection import. The general naming function was
corrected and covered by a regression test. The failed attempt is retained;
no source or calculation check was relaxed.

## Runtime, memory, and storage

| Operation | Result | Seconds |
|---|---|---:|
| First import plus full readback/query/source gates | Complete, partial master coverage | 94.836 |
| Immediate replay, batch option changed from 5,000 to 1,000 | Same graph, no document replacement | 78.857 |
| Final binary verification, including self-loop-safe query handling | Same graph, all gates pass | 73.967 |
| Final binary replay | Same graph, all gates pass | 70.077 |
| Independent read-only graph validator | All observations and memberships conserved | 9.409 |

Go jobs used a 4 GiB container cap, four CPUs, `GOMEMLIMIT=2GiB`, and
`GOMAXPROCS=4`. Final process RSS peaks were 1,708,511,232 and 1,646,608,384
bytes, measured with Linux `getrusage`. A sampled container memory high-water
mark reached roughly the 4 GiB cap during backing reads; that includes charged
file cache and is not process RSS. All completed jobs exited zero without an
OOM. The one-off independent validator used a 1 GiB cap and two CPUs.
Arango retained its existing 32 GiB container cap and internal settings.

Arango collection figures for the four data collections reported
1,167,497,961 bytes of document-plus-index storage at the first final replay,
about 1.09 GiB. These are engine-reported figures that change with storage
maintenance, not an exclusive physical disk allocation or an export size.
Metadata and shared database overhead are not included. Full raw source facts
remain in Parquet and were not copied wholesale into Arango.

Every invocation still verifies source backing. The source-drilldown gate
rehashes the pinned source tree and seeks selected original rows; it does not
decode all 421 million source rows again. These timings are warm local gates,
not weekly refresh or production latency guarantees.

## Readback and query checks

Go reads every field of every entity, observation, and component back from
Arango. It compares against the source-derived model, including exact money,
source locators, policy identities, nulls, and explicit false flags. It does
not compare only counts or stored document digests. Completion metadata is
written after all acceptance checks; a completed replay verifies rather than
repairs existing documents.

Each ledger passed three-hop ANY neighborhoods, four-hop directed path
samples, bounded BFS shortest paths, directed cycle samples, and one-sided
component/member lookups. On the first final replay, path samples took
485.288 ms for A and 206.245 ms for B; shortest-path samples took 0.702 ms and
36.202 ms. These are single representative measurements, not percentiles.
Every returned edge passed ledger and endpoint-continuity checks. Go also
checked the shortest hop count independently.

Queries use explicit edge collections, at most eight hops and 25 results,
five-second runtimes, and 128 MiB query memory caps. Full readback uses
streamed cursor pages with a separate 600-second cap. Self-loop observations
remain visible. The gate does not enumerate all paths, prove graph-wide
acyclicity, or calculate centrality/community metrics.

The graph supplied one source locator from each ledger. The Go lookup checked
both against the selected publication and returned the full 99-column A and
98-column B physical Parquet rows after verifying all 423 backing shards.
The earlier [source review](./committee-flow-source-review-2026-09-08.md)
remains the broader semantic sample; this gate proves graph-to-source access.

The independent one-off Python validator is not pipeline behavior. It reads
every observation and independently checks key derivation, per-ledger ordinal
uniqueness, integer amounts/signs, endpoints, fact identity, and attribution
flags. It consumes all component members exactly once, checks each component's
separate amounts and unchanged state counts, validates master coverage and
graph collection types, and compares initial/final lineage and source proof.
No source record or graph document is mutated by that validator.

Code gates cover missing/repeated members, endpoint conflicts, signed amount
drift, values beyond JavaScript integer precision, omitted false/null fields,
unknown fields, opposite-ledger and component path rejection, query bounds,
self-loops, cursor continuation/cleanup, lock cancellation, source-locator
rejection, credential redaction, database-name limits, and CLI dispatch.
Final `go mod tidy -diff`, `go vet ./...`, and `go test ./...` passed, as did
targeted race tests for the graph, reconciliation, and CLI packages. All 43
existing calculation-contract and Go/Dagster-boundary Python tests passed;
their warnings concern existing Dagster beta APIs. The changed documentation
has no missing relative-link targets, and `git diff --check` passes.

## Retained evidence and next work

Results, replay results, progress logs, source proof, independent validation,
runner/validator scripts, binary digest, and explicit completion markers live
under `/storage/dumps/audits/fec/arango-committee-flow-evidence/2026-09-08/2024/`.
The retained audit tree measured 78,187 bytes via `du -sb` after publication.
The initial naming failure is retained separately from successful results.
The existing source/calculation/bundle publications remain their own canonical
evidence; the audit does not duplicate their large artifacts.

The [implemented contract](../design/arango-committee-flow-evidence.md) owns
the command and query boundaries. Thin Dagster publication/readiness/graph
assets are next. A serving API, pagination, historical identity refresh,
other-cycle gates, economic-flow resolution, and terminal attribution remain
explicit deferred work. This gate does not introduce those interpretations.
