# Machine-readable contracts

This tree contains shared API value contracts and versioned contracts for
external source adapters.

The [committee-flow HTTP contract](./api/committee-flow/v1/) describes the
implemented read-only observation API, including exact signed money, bounded
pages, pinned projection lineage, and source evidence.

It also contains [`calculations/`](./calculations/), where versioned methods
select and compare preserved facts without changing source evidence.

[`releases/`](./releases/) defines coordinated source inventories,
metadata-only discovery, selection plans, and accepted release manifests.

[`evidence/`](./evidence/) defines immutable source-record occurrences,
structured issues, natural-key indexes, and semantic change sets created from
accepted releases before normalized facts.

[`facts/`](./facts/) defines lossless normalized source facts and immutable
fact-set manifests. The shipped FEC contracts cover the five classic
reference/summary products and processed Schedule A receipts. The new
[committee-summary fact contract](./facts/fec/committee-summary/v1/) preserves
whole-CSV occurrences and typed facts through opt-in release v4; live activation
remains separate from its completed artifact gate.

[`bundles/`](./bundles/) defines immutable, calculation-specific selections of
coherent fact publications. Bundles contain lineage and readiness evidence,
not copied facts or calculated values.

[`projections/`](./projections/) defines query-bearing database projections
derived from exact fact and calculation identities. Implemented contracts
cover the isolated ArangoDB candidate-receipt and independent-expenditure
probes.

[`audits/`](./audits/) defines strict diagnostic outputs that test a proposed
boundary without publishing facts or calculations. The first contract covers
same-publisher-batch Schedule A/B alignment candidates.
The [storage review v2](./audits/fec/storage-review/v2/result.schema.json) is a
read-only streaming acquisition/staging budget scenario, not a publication or
approval. [Historical v1](./audits/fec/storage-review/v1/result.schema.json)
preserves the former full-uncompressed-extract reserve interpretation.

Common contracts:

- [`common/money-measure/v1/`](./common/money-measure/v1/) — exact wire schema
  and FEC/LDA examples for points, bands, estimates, partial coverage, and
  attribution uncertainty.

External-source behavior is defined in the
[source-contract design](../docs/design/source-contracts.md).

The [relationship evidence contract](./sources/wikimedia/relationship-statements/v1/contract.json)
defines single-response entity queries using the existing role reader plus
parent/child properties. It preserves source direction, time precision and unknown
endpoints without identity joins or financial attribution.

The [company HTML evidence contract](./sources/company/html-evidence/v1/contract.json)
defines offline lexical extraction from pinned pages. Text, metadata and JSON-LD
retain byte-level evidence without interpreted roles or accepted donor identities.

The [Wikimedia organization-candidate contract](./sources/wikimedia/organization-candidates/v1/contract.json)
defines bounded raw capture and strict offline replay. Name matches are proposals,
not canonical identity or verified employment/ownership edges.

The [GLEIF name-search contract](./sources/gleif/name-search/v1/contract.json)
defines independent source-derived organization lookup, bounded pagination and
same-name ambiguity. A preexisting LEI or Wikidata item is not required; identity
and person-role approval remain separate.

The [report-metadata contract](./sources/fec/report-metadata/v1/) defines the
implemented Go local reader, bounded manual HTTP request/outcome contracts, and
raw endpoint assertions. Its acquisition contract remains draft pending history,
refresh reconciliation, and population coverage; financial selection is not shipped.

`source-contract.schema.json` validates contract metadata. Each dataset then
owns a version directory containing:

- `contract.json` — acquisition, identity, time, revision, quality, and use
  rules.
- `record.schema.json` — accepted physical JSON record shape when applicable.
- `page.schema.json` — optional API response envelope and pagination evidence.
- `fixtures/` — small canonical JSON examples or exact publisher-document
  bytes used to test the contract.
- `fixtures/manifest.json` — provenance, scenarios, and fixture digests.

`calculation-contract.schema.json` and
`calculation-fixture-manifest.schema.json` provide the corresponding boundary
for calculated family, selection, reconciliation, and attribution results.

Fixtures are source examples, not runtime raw storage. Runtime acquisition
preserves the complete publisher response and transport metadata under the
external Legal Tender data root. No credential may appear anywhere in this
tree.

Contract status has three states:

| State | Meaning |
|---|---|
| `draft` | Evidence is incomplete; implementation must not claim acceptance. |
| `accepted` | Schema and semantics have been reviewed and can gate publication. |
| `retired` | Kept for replay and provenance but not selected for new data. |

The initial LDA contracts remain `draft` until the remaining edge-case fixture
coverage and effective-amendment validation close the open items in the
[LDA schema audit](../docs/design/lda-source-schema.md). The first selected
[API/document comparison](../docs/audit/lda-api-printable-comparison-2026-08-27.md)
is complete and is part of the contract evidence.

The initial FEC bulk contracts also remain `draft`. The ZIP contracts use exact
local 2024 source rows. Processed Schedule A uses explicitly synthetic
relation-shaped API fixtures for row values. Its exact 2026-08-23 archive-range
observation now pins the whole artifact, catalog/DDL, and conserved 2025/2026
extract; paired exact COPY/canonical fixtures close that partition's physical-
row gap. All four target partitions now pass row/byte/digest and lossless zstd
storage conservation. A separate targeted API contract preserves exact-value
pages for
diagnostics, including processing-lag evidence, but does not claim a complete
partition. See the
[FEC source-contract audit](../docs/design/fec-source-contracts.md). The
processed API and raw electronic-filing contracts preserve deferred research;
they are not dependencies of the initial coordinated release. Processed
Schedule E now has an exact 80-column contract and complete current-corpus Go
parser proof. Active release-inventory v2 preserves the all-history relation
once, and selected-cycle occurrence, fact, and effective-calculation
publications now pass for all four target cycles.
Processed Schedule B also passes its same-publisher-batch Schedule A alignment
gate. It remains draft until a new release inventory and lossless selected-
cycle fact publication land.

Publisher index:

- [`sources/fec/`](./sources/fec/) — initial bulk candidate, committee,
  linkage, summary, processed Schedule A, and processed Schedule E contracts
  plus deferred API/raw-filing research contracts.
- [`sources/lda/`](./sources/lda/) — LDA.gov OpenAPI evidence, seven dataset
  contracts, record schemas, canonical JSON fixtures, exact printable document
  fixtures, and a draft period-aware monetary rule table.

Calculation index:

- [`bundles/fec/candidate-itemized-individual-receipts/v1/`](./bundles/fec/candidate-itemized-individual-receipts/v1/)
  — exact same-cycle and same-release selection of the four fact sets required
  by the compact candidate-receipt calculation.
- [`bundles/fec/independent-expenditure-projection/v1/`](./bundles/fec/independent-expenditure-projection/v1/)
  — exact same-cycle and same-release selection of one effective Schedule E
  calculation plus candidate and committee master facts for graph publication.
- [`bundles/fec/receiver-reported-committee-flow-projection/v1/`](./bundles/fec/receiver-reported-committee-flow-projection/v1/)
  — exact same-cycle and same-release selection of one receiver-flow
  calculation plus committee-master facts for graph publication.
- [`bundles/fec/receiver-reported-committee-flow-projection/v2/`](./bundles/fec/receiver-reported-committee-flow-projection/v2/)
  — additive identity-aware readiness boundary over the immutable v1 flow
  bundle.
- [`calculations/fec/candidate-itemized-individual-receipts/compact/v1/`](./calculations/fec/candidate-itemized-individual-receipts/compact/v1/)
  — accepted compact predicate, sparse-exception, candidate-result, and
  immutable calculation-set schemas.

- [`calculations/fec/candidate-itemized-individual-receipts/v1/`](./calculations/fec/candidate-itemized-individual-receipts/v1/)
  — Schedule A receipt decisions, same-cycle authorized-committee candidate
  components, independent `weball`/`webl` reconciliations, result and manifest
  schemas, and canonical fixtures.
- [`calculations/fec/effective-independent-expenditures/v1/`](./calculations/fec/effective-independent-expenditures/v1/)
  — accepted processed Schedule E membership predicate, sparse route and
  amount exceptions, spender-candidate-stance results, and immutable manifest.
- [`calculations/fec/receiver-flow-committee-identity-coverage/v1/`](./calculations/fec/receiver-flow-committee-identity-coverage/v1/)
  — exact-ID registration evidence, explicit unresolved state, immutable
  decision publication, and terminal-identity guard.

Projection index:

- [`projections/arango/candidate-receipts/v1/`](./projections/arango/candidate-receipts/v1/)
  — content-addressed candidate, committee, authorization, result, and receipt-
  component graph probe.
- [`projections/arango/independent-expenditures/v1/`](./projections/arango/independent-expenditures/v1/)
  — content-addressed spender-to-candidate support/opposition graph probe with
  exact amount readback and explicit master coverage.
- [`projections/arango/receiver-reported-committee-flows/v1/`](./projections/arango/receiver-reported-committee-flows/v1/)
  — content-addressed source-to-recipient committee graph with exact role and
  amount readback, topology, bounded paths, cycles, and master coverage.
- [`projections/arango/receiver-reported-committee-flows/v2/`](./projections/arango/receiver-reported-committee-flows/v2/)
  — additive identity-aware graph with distinct current, historical,
  alternate-release, and unresolved vertex states.
