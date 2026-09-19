# Source contracts and parser boundary

> **Status:** Target design. This contract governs all new Go source adapters.
> The [machine-readable registry](../../contracts/) contains source contracts
> at explicit draft or accepted maturity. Go implements the five classic FEC
> parsers plus occurrence and fact publication and the processed Schedule A
> and E parsers plus occurrence and fact publication. The separate
> [committee-summary CSV reader](./committee-summary-source.md) now passes
> complete four-cycle verification and lossless artifact readback. Its immutable
> publisher and real coordinated release v4 now pass publication and readback.
> The [report-metadata reader](./report-metadata-reader.md) validates local API
> captures and preserves raw assertions. Its [bounded Go fetcher](./report-metadata-capture.md)
> implements manual HTTP capture; history/refresh reconciliation remains draft.
> Remaining source families still require their own adapters.

The additive [Wikidata role-statement reader](./wikidata-role-extraction.md) now
extracts explicit relationships from retained item responses under a draft contract.
It preserves raw statements and time precision. The separate
[exploratory affiliation discovery adapter](./affiliation-discovery.md) now plans
source-qualified name/employer queries and retains Wikipedia-linked item responses.
Identity approval and publication remain unimplemented boundaries.

## Purpose

Correct raw parsing is the foundation of every later graph path, total, entity
resolution, and attribution. Legal Tender must know which exact artifact it
received, which publisher schema it accepted, which bytes produced each
record, and which semantic rules were applied. A plausible-looking document in
ArangoDB is not proof that parsing succeeded.

The source boundary has three separate contracts:

1. **Transport contract** — where an artifact comes from and how to capture it
   without damaging the prior snapshot.
2. **Physical schema contract** — how bytes become record occurrences and raw
   fields.
3. **Semantic normalization contract** — how raw fields become typed facts and
   source assertions.

The publisher's header, data dictionary, API response, or database schema is
evidence for these contracts. It is not executable authority that can change
production behavior without review.

## Contract registry

Each source has a versioned JSON contract under the
`contracts/sources/<publisher>/<dataset>/` tree. JSON keeps the Go runtime on
the standard library path and permits contracts to be embedded with
`go:embed`. Repository JSON Schemas validate contract metadata and fixture
manifests. Dataset record schemas validate the accepted physical shape.

The registry entry identifies at least:

| Area | Required contract data |
|---|---|
| Identity | Stable source ID, contract version, publisher, dataset, and semantic domain. |
| Authority | Artifact, documentation, schema, code-list, and terms URLs with retrieval dates. |
| Acquisition | URL template or endpoint, method, authentication class, rate limits, pagination, cadence, timeout, and retry policy. |
| Artifact | Expected media type, compression/container format, member or relation selection, encoding, delimiter, quoting, line ending, and empty-record policy. |
| Partition | Cycle, calendar period, filing period, page, or snapshot dimensions and how each is derived. |
| Physical schema | Ordered columns or JSON paths, source names, cardinality, raw-null forms, and compatibility policy. |
| Identity | Strong and weak publisher keys, duplicate policy, occurrence locator, and record-version identity. |
| Semantics | Typed fields, units, lossless decimal scale, source measurement and precision, date formats, time zones, enumerations, unknown-code handling, and required fields. |
| Revisions | Amendment, termination, memo, effective-record, and disappearance semantics known from the publisher. |
| Quality | Conservation, referential, plausibility, uniqueness, and source-specific checks with publication severity. |
| Use | Retention, citation, redistribution, privacy, and source-disclaimer requirements. |
| Outputs | Normalized fact types, assertion types, and emitted change-set dimensions. |

The registry is descriptive configuration, not a programming language. Rules
that require nontrivial calculation remain versioned Go code referenced by a
contract ID and covered by fixtures.

### Official schemas are pinned evidence

Downloaded header files, OpenAPI descriptions, data dictionaries, code lists,
and database metadata are stored or content-addressed as source-schema
artifacts. The contract records their digest and the accepted mapping.

For an ordered delimited file, the parser requires all three to agree:

```text
accepted contract columns
        == pinned publisher header columns
        == observed row field count
```

An upstream header can inform a proposed contract update. It cannot silently
replace the embedded contract during a scheduled run. This retains the useful
part of the legacy downloaded-header approach without making a mutable remote
file production code.

## Acquisition contract

Every source refresh creates an immutable snapshot manifest before parsing.
The manifest contract in the [evidence model](./evidence-model.md#source-snapshot)
applies, plus these acquisition outcomes:

| State | Meaning |
|---|---|
| `unchanged` | Received bytes match an accepted prior snapshot. |
| `captured` | A new artifact passed transport and container checks. |
| `not_modified` | Publisher returned a valid conditional-response state. |
| `unavailable` | The publisher did not supply an artifact after bounded retries. |
| `rejected_transport` | Status, media type, size, checksum, redirect, or container checks failed. |
| `unknown_schema` | Bytes were captured, but their physical schema is not accepted. |

Downloads use temporary files, streamed hashing, size limits, archive checks,
and atomic publication. A failed download or parse never replaces the current
published data version. Redirect targets and response headers are evidence and
belong in the manifest.

API sources preserve complete page response bodies or lossless
content-addressed chunks, request parameters excluding secrets, pagination
links or cursors, response time, and response headers relevant to versioning
or throttling. A reconstructed array of selected fields is not the raw source.

## Physical parsing contract

### Occurrences never disappear

Every record boundary discovered in a captured artifact yields one source
occurrence or one explicit container/record issue. Blank records follow the
source-specific contract; they are never discarded by a generic trim call.

For each occurrence, preserve:

- Snapshot, member/relation/page, partition, and one-based ordinal.
- Raw bytes or a lossless locator into an immutable artifact.
- Raw content digest and field boundaries.
- Detected encoding and decode issues.
- Observed field count and accepted schema fingerprint.
- Duplicate byte and duplicate publisher-key states.

The parser does not pad missing fields, truncate extra fields, ignore decoding
errors, or skip a record because a required identifier is absent. It emits the
occurrence with issues and blocks only the affected downstream publication.

### Record and field states

Occurrence shape and typed-field validity are separate.

Occurrence states include:

- `shape_valid`
- `missing_fields`
- `extra_fields`
- `decode_error`
- `record_boundary_error`
- `unknown_schema`

Typed fields retain the evidence-model states `valid`, `source_null`,
`invalid`, and `unsupported`. A missing physical field is not the same as a
present publisher null. An unknown code is preserved as valid raw text with an
`unknown_code` issue unless the contract says the value is structurally
invalid.

### Containers and relations are explicit

ZIP ingestion never selects the first member by accident. The contract names
an exact member, anchored pattern, or complete member set and defines how
multiple matches behave. PostgreSQL dumps name the accepted relations and
server/tool compatibility range. JSON APIs define both result-record and
response-envelope schemas plus the pagination evidence needed to prove a
window complete. Byte-oriented formats such as FEC electronic filings define
framing and version dispatch before text decoding.

Unrecognized members or relations are recorded. A new member is not parsed as
an old dataset because its filename is similar.

## Semantic normalization contract

Normalization retains raw values beside parsed values. It may:

- Parse signed money losslessly into integer cents with a declared scale while
  retaining the source's threshold, rounding, precision, and accounting
  semantics.
- Parse dates and timestamps under source-specific formats.
- Decode a publisher code while preserving the original code.
- Create source-scoped publisher identities.
- Emit typed facts and assertions.

Normalization does not:

- Decide whether a row counts in a total.
- Resolve a disclosure name to a canonical person or organization.
- Turn employer text into a corporate contribution.
- Collapse amendments or memo records without an accepted calculation.
- Infer candidate authorization, terminal status, or money flow from a name.
- Replace missing detail with a publisher summary.

Those are versioned linkage, resolution, classification, or calculation
contracts over preserved facts.

Money fields also follow the common
[money-measure contract](./money-measures.md). A losslessly parsed decimal is a
source representation; its measurement can still be a rounded estimate,
threshold band, summary value, or method-specific amount. Source adapters do
not manufacture a point estimate from those states.

## Schema fingerprints and drift

A physical-schema fingerprint is deterministic and source-specific. Examples:

- Delimited file: ordered source columns, delimiter, encoding, and contract
  format version.
- JSON API: sorted observed JSON paths with primitive/container kinds and
  cardinality expectations, plus the accepted contract version.
- PostgreSQL relation: ordered columns, database types, nullability, and
  relation identity.
- FEC electronic filing: HDR version, byte delimiter, observed terminator,
  ordered logical row layout, physical field count, and extra-field policy.

The raw artifact digest is not the schema fingerprint. Different releases can
share a schema, and identical-looking top-level JSON can contain changed nested
objects.

Drift is handled as follows:

| Change | Capture raw artifact | Normalize | Publish affected dataset |
|---|---:|---:|---:|
| Known schema, new rows | Yes | Yes | After checks |
| Accepted additive optional field | Yes | Yes, preserving field | After checks |
| Unreviewed new or reordered column | Yes | No | No |
| Missing required field | Yes | Partial evidence only | No |
| Type or meaning change | Yes | No under old contract | No |
| Unknown enum value | Yes | Yes with issue | Source policy decides |

The default is fail closed at publication, not fail closed at capture. Keeping
the bytes allows inspection and replay after the contract is updated.

An accepted source change requires:

1. A captured fixture showing the old and new shapes.
2. Updated publisher-schema evidence and digest.
3. A reviewed contract-version change.
4. Parser and normalization tests.
5. An explicit compatibility and reprocessing decision.

## API drift without a publisher schema

Some public APIs expose browsable responses but no durable versioned OpenAPI
contract. For those sources, Legal Tender maintains representative raw
fixtures and a reviewed structural contract. Every run records observed-path
fingerprints by endpoint.

Unknown fields are preserved in raw responses. They do not break capture.
They also do not become normalized facts until accepted. Missing required
paths, changed types, pagination loops, repeated page identities, or a
shrinking result set without an explained filter block publication.

## Validation gates

A source version moves through these states:

```text
captured -> physically_validated -> normalized -> semantically_checked
         -> staged -> published
```

Each transition is immutable. A failed transition records diagnostics and
leaves the prior published version intact.

Every dataset defines checks in five classes:

1. **Conservation** — every discovered occurrence becomes a fact or an
   explicit issue; counts reconcile by state.
2. **Shape** — member, relation, column, path, encoding, and schema fingerprint
   match accepted contracts.
3. **Identity** — required IDs, uniqueness expectations, and duplicate states
   are reported without destructive deduplication.
4. **Semantics** — money, date, enum, amendment, and time-partition rules pass
   source-specific checks.
5. **References** — candidate, committee, filing, client, registrant, and other
   endpoints resolve or remain explicit orphans.

Checks have `observe`, `partial`, or `block` severity. Severity is part of the
source contract; scheduled code does not choose it ad hoc.

## Change propagation

The source adapter emits a content-addressed change set between accepted
snapshot indexes:

- Added, changed, absent, duplicate, and invalid publisher record references.
- Affected cycles or filing periods.
- Source-scoped entity IDs and relationship endpoints when parseable.
- Fact types requiring normalization or removal from the new published view.

Dagster reasons about source and projection partitions. Go computes exact
record changes and affected domain keys. A schema-contract change invalidates
the relevant normalized fact version even when source bytes are unchanged.

## Source-specific contract status

This document defines the common boundary. Each source needs its own contract
before implementation. The initial order is maintained in the
[source catalog](./source-catalog.md):

1. Draft FEC contracts now cover candidate master, committee master,
   candidate-committee linkage, processed Schedule A, all-candidates summary,
   and current House and Senate campaign summary. The five classic products
   have Go occurrence and normalized-fact publishers; Schedule A has its
   occurrence publisher. Real-corpus and Schedule A fact gates remain in the
   [FEC source-contract audit](./fec-source-contracts.md).
2. Draft LDA contracts cover filings, contribution reports, master entities,
   constants, and printable documents. Their remaining gates are in the
   [LDA schema audit](./lda-source-schema.md).
3. FEC processed Schedule E has passed source selection, complete-corpus
   parsing, release publication, and effective-record calculation. Processed
   Schedule B has passed physical, alignment, release-v3, and lossless 2024
   columnar-publication gates. Its effective-record, outgoing-flow-role, and
   sender/receiver reconciliation calculations remain separate. Debts, loans,
   and raw filing versions follow.

The current Python implementation and local source artifacts were examined in
the [2026-08-27 source-boundary audit](../audit/source-boundary-2026-08-27.md).

## Primary references

- [FEC bulk data catalog](https://www.fec.gov/data/browse-data/?tab=bulk-data)
- [FEC processed schedule dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [LDA API root](https://lda.gov/api/v1/)
- [LDA API terms, rate limits, and citation rules](https://lda.gov/api/tos/)
