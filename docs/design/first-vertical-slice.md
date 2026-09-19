# First vertical slice: evidence-backed candidate receipts

> **Status:** Draft implementation contract. Accepted architecture decisions
> apply. The coordinated source-release boundary and immutable Schedule A
> occurrence/change ledger are implemented. The five classic products and
> Schedule A now have lossless normalized-fact publishers. The 2024 columnar
> fact, compact occurrence/change, and direct calculation-probe corpus gates
> passed. The first Go candidate itemized-individual calculation and Dagster
> asset are fixture tested. The compact calculation publisher also passed
> exact complete-corpus equivalence. Coordinated fact-bundle readiness and
> exact Dagster partition automation then passed the real 2024 gate. ArangoDB
> domain state, graph projection, and the source-to-API path remain open.

## Outcome

The first slice answers a narrow, useful question:

> For one candidate and election cycle, which processed Schedule A receipts
> did the FEC classify as itemized individual receipts for the candidate's
> authorized committees, what did the FEC publish as the candidate summary,
> and which exact source records and coordinated FEC release support each
> answer?

The slice starts with immutable FEC source releases and ends with a Go API
response that can expand every displayed receipt and linkage to its source
evidence. It establishes ingestion, provenance, cycle isolation, incremental
processing, ArangoDB storage, Dagster orchestration, and the Go/Python boundary
before adding committee transfers or terminal-source attribution.

This is a vertical slice through the target system. It is not a port of the
legacy `indiv`, `donors`, `contributed_to`, `committee_receipts`, or
`candidate_funding` assets.

## No parity presumption

Legacy behavior has no default disposition. A behavior becomes `KEEP` only
when the product contract, source semantics, or a separately accepted domain
decision justifies it. Existing code and output supply cases to examine; they
do not supply the answer.

The slice does not inherit these Python behaviors:

- Replacing one active raw ZIP per source and cycle.
- Dropping malformed values or semantically unwanted rows during parsing.
- Replacing source records that share a document key.
- Merging contributors by normalized name plus employer.
- Applying a contribution threshold before creating investigative state.
- Combining direct and earmarked totals with a maximum heuristic.
- Converting per-record employer text into a timeless employment edge.
- Replacing transaction facts with cycle-level amount/count edges that have no
  source-record membership.
- Choosing one summary source through an implicit fallback.
- Overwriting historical candidate or committee attributes with the latest
  cycle's values.
- Storing top-N or five-channel output as the only surviving result.

Some of those behaviors may encode a useful intent. If so, that intent will be
reintroduced through an explicit fact, resolution, or calculation contract
and tested independently.

## Product contract covered

The slice implements the first increment of:

- `C-001`: authorized-committee receipts, limited initially to itemized
  individual receipts plus separately reported FEC candidate summaries.
- `C-005`: itemized contributor transactions and disclosed employer text,
  without claiming that matching disclosure names are one person.
- `D-001`: source-specific freshness and coverage-through dates.
- `D-002`: itemized coverage, excluded-record reasons, and missing partitions.
- `D-003`: source-record and calculation changes between published snapshots.
- `D-004`: reproducible answers with snapshot, source-record, and calculation
  identifiers.
- `G-002`: a direct disclosed path from a contributor occurrence through an
  authorized committee to a candidate.

It does not complete candidate-controlled receipts. Candidate loans, refunds,
transfers, and other receipt categories require calculation contracts of their
own. The initial production slice does not ingest OpenFEC API rows or raw
electronic filings. A future as-filed product requires its own accepted source,
calculation, freshness, and presentation contract. FEC-published summaries stay
separate from calculated receipt components.

## Source scope

The first slice ingests these FEC sources for one or more two-year election
cycles:

| Source | Role in the slice |
|---|---|
| `cn` | Candidate source assertions. |
| `cm` | Committee source assertions. |
| `ccl` | Candidate-to-committee linkage facts and designation codes. |
| Processed Schedule A dump | Canonical detailed processed receipts and FEC processing fields. |
| All-candidate summary (`weball`) | FEC-published summary facts for candidates with financial activity, including total receipts and total individual contributions. |
| Current House/Senate campaigns (`webl`) | A distinct FEC-published campaign summary with total receipts and total individual contributions. |

These sources publish only as one accepted
[`fec.release.v1`](./fec-release-strategy.md). Each fact family has one
authoritative official bulk path. A changed API view or overlapping classic
file never patches a missing or stale bulk product. Classic `indiv.zip` is a
disclosure-threshold subset and remains an independent comparison input.

The dump's PostgreSQL distribution format does not make PostgreSQL the domain
database. The first corpus probe streams selected partition `COPY` data from a
version-pinned `pg_restore` container. A temporary, memory-capped PostgreSQL
restore is only the benchmark fallback.

The two summary datasets remain distinct publisher-defined facts. The slice
does not silently prefer one, fill one from the other, or decompose either into
invented transaction facts. The accepted reconciliation contract shows each
source with its own coverage and meaning.

All source adapters follow the strict acquisition, parsing, drift, and
publication rules in the [source-contract design](./source-contracts.md). The
exact source inventory and implementation sequence live in the
[source catalog](./source-catalog.md).

Every displayed monetary component follows the shared
[money-measure contract](./money-measures.md). The first slice normally sums
reported Schedule A points with lossless signed-cent arithmetic. Partial signed
coverage is not automatically presented as a lower bound.

The initial real-data proof uses the closed 2024 cycle, then a coordinated
release containing the partial 2026 cycle. Together they prove closed- and
active-cycle coverage, release-level atomicity, targeted propagation, and
change behavior. This implementation order does not limit retention or the
eventual historical backfill.

## Evidence layers

### Immutable source snapshot

Every successful download produces a content-addressed archive and manifest.
The canonical identity is the SHA-256 digest of the received bytes, not a URL,
retrieval time, `Last-Modified` value, or filename.

The manifest records at least:

- Publisher and dataset.
- Election cycle requested.
- Source URL and retrieval time.
- HTTP validators and response metadata when supplied.
- Byte length and SHA-256 digest.
- Container members or relations, sizes, and source checksums when supplied.
- Header-schema identity and version used for parsing.
- Downloader version and result status.

Downloads write to a temporary file, validate the archive, fsync as required,
and atomically publish the content-addressed path. A failed refresh cannot
damage a prior snapshot. Identical bytes reuse the existing snapshot.

### Source-record occurrence

A source-record occurrence identifies an exact row in an exact snapshot:

```text
snapshot ID + source relation/member + partition + one-based row number
```

The parser preserves the raw row locator, raw field strings, extra or missing
field state, decode issues, and a row-content hash. A malformed row becomes a
record with parse issues; it does not disappear.

An immutable per-snapshot record index maps occurrence identities to their
archive locations and parsed record-version identities. The first Schedule A
implementation used immutable zstd JSONL occurrence and issue artifacts plus a
globally sorted natural-key index and change artifact. The 2024 corpus proved
its logical behavior but rejected its 113.45 GB physical footprint. The
accepted replacement preserves those identities through dense source-row
membership, a fixed-width partitioned key index, sparse exceptions, and actual
inter-release deltas. Its complete 2024 publication reduced the footprint by
90.94%. Neither layout creates one ArangoDB edge for every weekly occurrence
of an unchanged row.

### Source-record version

A record version represents the content of a publisher record. Its identity
combines dataset, cycle, the publisher's natural key when available, and the
raw row-content hash. Identical records across snapshots can reuse one record
version while the snapshot index preserves where each occurrence appeared.

When a natural key retains different content, both versions survive. When a
record disappears from a later snapshot, it becomes absent from that snapshot;
the prior version is not deleted.

### Normalized fact

A normalized fact is a typed interpretation of one record version. It keeps:

- Every source field and its original text.
- Parsed typed values beside, not instead of, the original text.
- Dataset, cycle, source-record version, and source snapshot lineage.
- Parser and schema version.
- Parse warnings and invalid-field state.
- The source's distinct temporal fields, including transaction date,
  candidate election year, FEC election year, and summary coverage end.

Normalization does not decide whether a receipt counts in a total. The
processed calculation projection applies the FEC `is_individual` and memo
subtotal rules. Refund and broader receipt netting remain separate contracts.
A later raw-filing product cannot enter this projection implicitly.

### Entity and relationship projections

FEC candidate and committee IDs anchor canonical candidate and committee
entities. Their names, party, office, address, designation, and other mutable
fields remain versioned source assertions by cycle and snapshot. A current
display label may be projected; it cannot overwrite assertion history.

A candidate-committee relationship references its `ccl` fact. Version 1 marks
`A` and `P` designations as authorized relationships while preserving the
original designation. Missing or conflicting evidence remains unresolved; the
system does not infer authorization from committee name.

The slice does not create canonical person entities. Each receipt supplies a
disclosed contributor occurrence containing the name, address, employer, and
occupation reported on that record. Later identity resolution may connect
occurrences to a person through versioned evidence without changing the
receipt fact.

The logical direct path is:

```text
disclosed contributor occurrence
        --[reported itemized receipt]-->
authorized committee
        --[FEC candidate-committee linkage]-->
candidate
```

Whether contributor occurrences and receipt relationships are stored as
physical graph vertices and edges or constructed from indexed fact documents
is deliberately unresolved until the ArangoDB corpus probe. The API contract
and evidence semantics must be identical either way.

### Calculation projection

Each calculation projection is keyed by candidate, cycle, source view,
calculation version, and exact input snapshot set. It records:

- Authorized committee IDs and linkage fact IDs.
- Included and excluded receipt fact-set manifests.
- Exact inclusion, publisher-processed amendment, and memo-subtotal policy
  versions.
- Computed itemized-individual amount and record count.
- Applicable FEC summary fact or facts and their coverage-through dates.
- Difference between the computed component and published summary fields,
  without assigning an unsupported cause.
- The coordinated release ID plus the independent source version and watermark
  for each input fact family.
- Unresolved and invalid records grouped by explicit reason.
- Calculation time, code version, and output data version.

Large lineage sets are stored as immutable manifests or reproducible indexed
predicates, not embedded arrays on a candidate document. Drilldown must still
return the exact constituent facts.

## API result

The Go API supplies at least these capabilities; paths are illustrative until
the API contract is finalized:

```text
GET /api/v1/candidates/{candidate_id}/cycles/{cycle}/receipts
GET /api/v1/candidates/{candidate_id}/cycles/{cycle}/receipts/items
GET /api/v1/evidence/facts/{fact_id}
GET /api/v1/evidence/snapshots/{snapshot_id}
```

The candidate receipt response separates:

- FEC-published summary amounts, separated by summary dataset.
- The processed itemized-individual component at its dump snapshot.
- Unitemized amounts only when explicitly present in a source summary.
- Other receipt categories not yet implemented.
- Difference and unresolved state.
- Coordinated release, per-source coverage-through date, and partial-cycle
  status.

Receipt rows are cursor-paginated and filterable by committee, date, amount,
transaction type, election designation, memo state, contributor disclosure
fields, and evidence status. Display ranking and page limits do not mutate or
truncate stored facts.

Every amount links to its calculation record and every row links to source
evidence. The response must remain reproducible after a newer snapshot is
published by supplying the prior published data version.

## Execution contract

The Dagster asset graph, partitions, Go commands, incremental transitions,
checks, and acceptance scenarios are specified in the
[`first-slice execution contract`](./first-vertical-slice-execution.md). The
logical grain and calculations are defined by the
[`evidence model`](./evidence-model.md) and
[`authorized-receipt calculation contracts`](./calculation-contracts.md).

## Real-corpus proof before physical lock-in

The 2024 data must measure:

- Immutable archive and record-index storage growth.
- Normalized fact storage with and without physical receipt edges.
- ArangoDB import throughput, index size, memory, and compaction behavior under
  the host memory cap.
- Candidate receipt page and evidence-drilldown latency.
- Change detection and affected-key cost for a realistic refresh.
- Rebuild cost after a parser or calculation-version change.

The probe compares at least:

1. Receipt facts as indexed documents with direct-path construction at query
   time.
2. Receipt facts as graph edges from disclosed contributor occurrences to
   committees.
3. Preserved receipt documents plus coarser graph projections that reference
   immutable fact-set manifests.

The chosen physical model must preserve the same logical evidence contract.
ArangoDB remains the working primary domain database, but this probe decides
which facts belong in its named graph rather than assuming the legacy edge
shape was correct.

## Deferred from this slice

- Canonical person identity and contributor deduplication.
- Employer-to-organization resolution and corporate families.
- Personal-contributor materiality rules or donor tiers.
- Committee transfers and pooled-money allocation.
- Earmark and conduit relationship resolution beyond preserving source fields
  and applying the source memo-subtotal rule.
- Terminal-source classification and traversal.
- Independent expenditures.
- Lobbying filings and contextual links.
- Raw electronic filings, effective report-family calculations, and a
  separately labeled as-filed view.
- Cross-cycle aggregates and rankings.
- Community, centrality, similarity, and ideology analysis.

The next implementation gate is the real-corpus ArangoDB projection probe. It
will measure a query-bearing entity, monetary, and path model before the graph
ontology is accepted for transfers, conduit relationships, terminal sources,
and evidence-backed path return.

## External references

- [FEC bulk-data catalog](https://www.fec.gov/data/browse-data/?tab=bulk-data)
- [FEC processed Schedule A weekly dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [FEC candidate-committee linkage fields](https://www.fec.gov/campaign-finance-data/candidate-committee-linkage-file-description/)
- [FEC individual-contribution fields](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/)
- [FEC candidate-summary semantics](https://www.fec.gov/campaign-finance-data/candidate-summary-file-description/)
- [FEC current House/Senate summary semantics](https://www.fec.gov/campaign-finance-data/current-campaigns-house-and-senate-file-description/)
