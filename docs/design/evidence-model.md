# Normalized evidence model

> **Status:** Draft target contract for the Go rewrite. Logical identities and
> preservation rules are binding for the first vertical slice. The first
> physical Schedule A occurrence/index/change and lossless receipt-fact JSON
> implementations exist, but the 2024 corpus probe rejected their storage
> layout. The replacement Parquet and compact occurrence/change contracts are
> implemented and passed complete 2024 corpus gates. Compact calculation
> membership is still required before ArangoDB collection design.

## Purpose

Legal Tender must preserve what a publisher disclosed before it decides what
the disclosure means. Source evidence, typed facts, identity assertions,
relationships, and calculations are different objects with different version
rules.

This model supports three requirements at once:

- Retain the finest practical source grain without forcing early aggregation.
- Reproduce any displayed amount, relationship, or graph path from exact input
  evidence and a versioned calculation.
- Reprocess changed evidence without rebuilding unrelated candidates, cycles,
  or graph neighborhoods.

It is not an ArangoDB collection design. The same logical model must survive a
different physical layout chosen by the real-corpus probe.

## Detailed-receipt source decision

The FEC processed Schedule A weekly database dump is the canonical detailed
receipt source for the first slice. The classic `indiv.zip` file is not.

The classic individual-contributions file is a thresholded subset of itemized
Schedule A records. It is useful for comparison with the legacy system, but it
cannot satisfy the target preservation contract. The processed Schedule A
dataset supplies substantially richer disclosure and filing context, including
structured contributor fields, FEC's `is_individual` classification, action
code, transaction and back-reference identifiers, line number, file number,
original sub-ID, form, and conduit fields.

The Schedule A source is a PostgreSQL custom-format dump. That distribution
format does not make PostgreSQL the Legal Tender domain database. The corpus
probe must compare a streaming `pg_restore` extraction with a temporary,
memory-capped PostgreSQL extraction container. Both routes end in the same Go
normalizer and ArangoDB domain model.

The dump is a processed publisher snapshot, not a complete archive of every
historical filing revision. Legal Tender preserves every accepted acquisition
snapshot it captures after ingestion starts. A monthly or weekly processed
snapshot history cannot reconstruct intermediate revisions. A later raw-filing
product may preserve separate as-filed evidence, but it cannot rewrite this
processed view.

### Local corpus evidence

A sequential sample of the first 1,000,000 rows in the local 2024 `itcont.txt`
found 759,085 `N`, 240,845 `A`, and 70 `T` amendment indicators; 1,379 memo-code
`X` rows; 4,695 negative amounts; and 41,406 zero amounts. It also found 33
reused `(CMTE_ID, TRAN_ID)` pairs but no reused
`(CMTE_ID, RPT_TP, TRAN_ID)` triples. Sample `X` rows described earmarks,
non-contribution accounts, and generic memorandum activity.

This sequential sample is not a population estimate. It supplies
counterexamples to destructive assumptions: amendment indicator is not an
event identity, transaction ID is not global, signed and zero values are real
source states, and memo code cannot justify deleting evidence.

## Invariants

1. No valid or invalid source row disappears during ingestion.
2. Original text survives beside every parsed value.
3. A source identifier is evidence, not automatically an economic-event ID.
4. `SUB_ID` identifies a processed FEC row. It does not prove that two rows are
   the same contribution.
5. Amendment, memo, refund, conduit, and inclusion semantics live in versioned
   projections. They do not mutate normalized facts.
6. A newer snapshot never deletes or overwrites an older snapshot, record
   version, fact, assertion, or calculation.
7. Candidate, committee, filing, transaction, election, report, and coverage
   time remain separate dimensions.
8. Names, employers, addresses, or transaction amounts never create a canonical
   person identity by themselves.
9. Money uses signed integer minor units after lossless decimal parsing. Raw
   amount text, source precision, thresholds, rounding, and accounting method
   also survive. No float participates in a monetary calculation, and exact
   parsing does not imply exact economic measurement.
10. Every material output identifies its input snapshot set, fact-set
    membership, calculation version, and publication version.

## Evidence layers

```text
source snapshot
    -> source-record occurrence
        -> source-record version
            -> normalized fact / source assertion
                -> entity and relationship projection
                    -> calculation run and fact-set manifest
                        -> published API projection
```

Each arrow means “derived from.” It never means “replaced by.”

### Source snapshot

A source snapshot is the exact artifact received from a publisher. Its ID is
the lowercase hexadecimal SHA-256 digest of the received bytes.

Required metadata includes:

- Publisher, dataset, requested source partition, and source URL. A partition
  can be an FEC cycle, filing year, posted-date acquisition window, or another
  source-native dimension.
- Retrieval start and completion times in UTC.
- HTTP status, validators, media type, byte length, and digest.
- Container format and member metadata when applicable.
- Source schema fingerprint and downloader version.
- Validation state, diagnostics, and prior equivalent snapshot when present.

The artifact is immutable and content-addressed. An identical download reuses
the same snapshot identity.

### Source-record occurrence

An occurrence answers: “Where did these bytes appear in this snapshot?”

Its logical locator is:

```text
snapshot ID + source relation/member + partition + one-based row ordinal
```

The occurrence retains the raw record bytes or a lossless locator to them, raw
field boundaries, decoding state, extra or missing fields, and a row-content
digest. Duplicate rows produce distinct occurrences even when their content is
identical.

The implemented Schedule A locator records the selected source artifact,
relation, cycle, one-based row ordinal, and exact uncompressed byte offset and
length. Its raw digest hashes the exact COPY row bytes. An independent
semantic digest hashes all 81 decoded values with null-aware length framing;
equivalent COPY escape representations therefore retain different raw
evidence without creating a false semantic change.

The occurrence ID is a hash of the full logical locator. Row order is evidence
within a snapshot; it is not a cross-snapshot record identity.

### Source-record version

A record version answers: “Which publisher record content did we observe?”

Its identity combines:

```text
publisher + dataset + source partition + publisher record reference
    + raw content digest
```

The publisher record reference uses the strongest source-native key available.
For processed Schedule A this is normally `SUB_ID`. For datasets without a
reliable natural key, the reference is an explicit composite or an
`unkeyed:<occurrence-id>` value. A duplicated natural key never causes data
loss: every occurrence remains, and the duplicate becomes a validation state.

Identical record content can reuse one version across snapshots. Different
content under the same publisher reference creates another immutable version.
Presence and absence belong to the snapshot index, not to a mutable flag on the
record version.

### Normalized fact

A normalized fact is a typed interpretation of one source-record version under
one parser and schema version. Its ID includes the record-version ID and the
normalization schema version.

Every fact has a common provenance envelope:

| Field | Meaning |
|---|---|
| `fact_id` | Stable derived-fact identity. |
| `fact_type` | Versioned logical type such as `fec.schedule_a_receipt.v1`. |
| `source_record_version_id` | Exact source content interpreted. |
| `origin_snapshot_id` | Snapshot occurrence used to create the immutable fact. |
| `publisher` / `dataset` | Source namespace. |
| `source_partition` | Source-native acquisition or reporting dimensions used by the contract. |
| `source_times` | Independently typed event, reporting, publication, effective, update, and observation times that the source supplies. |
| `cycle` | FEC two-year period on facts whose FEC source supplies or defines it; absent on unrelated domains such as LDA filings. |
| `schema_version` | Normalization contract. |
| `parser_version` | Go implementation identity. |
| `raw_fields` | Original source values without semantic cleanup. |
| `typed_fields` | Parsed values with explicit null and invalid states. |
| `issues` | Structured warnings or errors by field and code. |

Normalization may standardize field names, parse dates, parse money, and decode
source codes. It must not force every domain into an FEC cycle or decide
whether a record counts in an aggregate.
All snapshots containing the same record version are resolved through immutable
occurrence indexes; the fact is not mutated to append snapshot IDs.

Money fields use the source-observation portion of the shared
[money-measure contract](./money-measures.md). A typed fact preserves the raw
value and lossless parsed cents separately from measurement kind, bounds,
precision increment, accounting method, and observation state. The normalized
fact does not derive an aggregate midpoint or choose an effective filing.

### Source assertion

A source assertion records what a source said about an entity at a particular
cycle and snapshot. Candidate name, party, office, committee name, designation,
organization type, and address are assertions. They are not mutable properties
on a timeless entity.

An assertion records subject ID, predicate, raw and typed value, valid or
reported time when available, snapshot time, fact ID, and source authority. A
current display profile is a projection over assertions with an explicit
selection policy.

### Entity and relationship projection

Publisher IDs anchor source-scoped entities:

```text
fec:candidate:<CAND_ID>
fec:committee:<CMTE_ID>
```

A candidate-committee relationship is supported by a specific normalized `ccl`
fact. An `A` or `P` designation may create an authorized relationship for a
cycle, but the raw designation remains available and conflicts remain
unresolved.

Schedule A creates a disclosed contributor occurrence scoped to its receipt
fact. It can carry reported name, address, employer, and occupation without
claiming that it is a resolved person. Later resolution can connect several
occurrences to a person or organization through its own evidence and versioned
decision.

Relationships that matter to an investigation retain supporting fact IDs or an
immutable fact-set manifest. A relationship amount is never the only surviving
copy of constituent transactions.

### Calculation artifact

A calculation run is immutable and records:

- Contract and implementation versions.
- Parameter values and cycle.
- Exact input data versions and snapshot set.
- Included, excluded, and unresolved fact-set manifests.
- Output values and conservation checks.
- Code build identity, execution time, and run ID.
- Publication state and result digest.

A fact-set manifest is a content-addressed, sorted membership artifact. It can
contain fact IDs directly or a reproducible predicate plus the input index
version when that representation is proven equivalent. A changing query over
“latest” data is not a fact-set manifest.

Every monetary calculation artifact uses the result interval and orthogonal
uncertainty states from the [money-measure contract](./money-measures.md).
Alternative amendment, identity, or classification results retain separate
scenario manifests instead of collapsing to one range.

## First-slice fact contracts

### Candidate assertion fact

`fec.candidate_assertion.v1` preserves every `cn` field and at least parses:

- Candidate ID, name, party, election year, cycle, office, state, district,
  incumbent/challenger/open-seat status, status, street, city, state, and ZIP.
- Raw election-year and cycle fields separately.

The candidate entity key is a resolved FEC candidate ID, not an unchecked ID
string copied from another record type. A candidate-master fact directly
asserts its own ID. A Schedule E fact preserves its reported candidate ID and
requires a separate versioned candidate-reference decision before graph
projection. Name or office changes create more assertions; they never rewrite
the source fact.

### Committee assertion fact

`fec.committee_assertion.v1` preserves every `cm` field and at least parses:

- Committee ID, name, type, designation, organization type, party, filing
  frequency, connected organization, candidate ID, address, and cycle.

Committee profiles remain cycle- and snapshot-specific.

### Candidate-committee linkage fact

`fec.candidate_committee_linkage.v1` preserves every `ccl` field and at least
parses candidate ID, election year, cycle, committee ID, committee type,
designation, linkage ID, and linkage type.

The fact supports a relationship; it is not itself collapsed into a timeless
edge. Multiple or conflicting linkage rows survive.

### Processed Schedule A receipt fact

`fec.schedule_a_receipt.v1` preserves every column distributed in the processed
Schedule A snapshot. Its typed surface includes, when present:

- Recipient committee ID and published recipient metadata.
- `SUB_ID`, original `SUB_ID`, transaction ID, back-reference transaction ID,
  link ID, file number, image number, filing form, line number, report type,
  report year, action code, and FEC load date.
- Receipt date, signed amount in cents, aggregate year-to-date amount, receipt
  type, schedule type, memo code, and memo text.
- Entity type, FEC `is_individual`, contributor ID, all distributed contributor
  name parts, address parts, employer, and occupation.
- Election designation, FEC election year, two-year transaction period, and
  conduit fields.

`memoed_subtotal` is a normalized boolean defined exactly as the current
openFEC API model defines it: `memo_code == "X"`. It is derived beside the raw
memo fields. It does not remove the receipt fact.

The receipt amount may be negative or zero. Both are retained and can
participate in a calculation only under that calculation's declared policy.

### Candidate summary facts

`fec.candidate_summary_all.v1` and `fec.campaign_summary.v1` preserve `weball`
and `webl` independently. They keep every publisher field, including total
receipts, total individual contributions, refunds, coverage end date, candidate
status, and source-specific election fields.

Neither summary is decomposed into invented transactions. A summary amount is
a separate source fact, not a fallback replacement for missing detailed facts.

## Time model

These values must never be silently substituted for one another:

- FEC two-year transaction period.
- Candidate election year.
- FEC election year or primary/general designation on a transaction.
- Report year and report type.
- Transaction date.
- Summary coverage-through date.
- FEC processing/load timestamp.
- Legal Tender snapshot retrieval and publication timestamps.

Source dates without time zones remain dates. System events use UTC timestamps.
The API exposes partial-cycle and incompatible-coverage states explicitly.

## Amendment and snapshot history

The processed Schedule A snapshot is accepted as the FEC's then-current
processed view. Legal Tender does not reconstruct an amendment chain by sorting
`AMNDT_IND`, action code, file number, transaction ID, or `SUB_ID`. Those fields
remain evidence because they do not by themselves establish a complete
economic-event identity or report chain.

For each new snapshot, Go computes record-reference and content-digest changes:

- `added`: a publisher reference appears for the first time.
- `changed`: a publisher reference has different content.
- `absent`: a prior reference is not present in the new snapshot.
- `duplicate`: a reference appears more than once in one snapshot.
- `invalid`: the row is present but cannot satisfy part of the typed schema.

These are observation changes, not claims that a donation occurred, changed,
or was deleted on the same date. Complete historical revision analysis requires
raw filings and its own contract.

## Parsing and quality states

A field has one of four parse states: `valid`, `source_null`, `invalid`, or
`unsupported`. A fact has the worst state of its required fields plus all
field-level issues. Invalid facts remain queryable through evidence APIs and
count toward source conservation checks.

At minimum, checks report:

- Total occurrences and unique record versions.
- Duplicate publisher references and duplicate raw rows.
- Missing required identifiers.
- Invalid dates and money.
- Unknown source codes without coercing them to a known code.
- Snapshot rows represented by valid facts or explicit issues.
- Facts whose referenced candidate, committee, or linkage endpoint is absent.

## Physical-storage boundary

The target storage roles are:

- Content-addressed filesystem artifacts for source snapshots, large occurrence
  indexes, change sets, and fact-set manifests.
- ArangoDB documents for queryable facts, assertions, projections, calculation
  metadata, and published-version pointers.
- ArangoDB edges only where traversal or direct path return justifies their
  cost. Fine-grained facts may remain indexed documents referenced by coarser
  edges.

The corpus probe chooses exact collections, edge density, indexes, sharding,
and compression. It cannot weaken identities, provenance, or replay behavior
defined here.

The first Schedule A evidence implementation used immutable zstd JSONL
occurrences, issues, natural-key indexes, and change sets. Its bounded hash
shards and global uniqueness proof worked, but the physical output consumed
113.45 GB for one 14.77 GB staged cycle. The accepted compact replacement uses
dense source-row membership, 48-byte partitioned key-index records, sparse
exceptions, implicit bootstrap membership, and actual later deltas. It
preserved all 264,085,606 unique 2024 rows in 10.277 GB, a 90.94% reduction,
with full source and shard verification. See the
[compact publication audit](../audit/schedule-a-compact-occurrence-publication-2026-08-31.md).

The accepted Parquet physical contract preserves all 81 decoded source values
and adds 18 columns for exact source locators and policy-free typed values.
The immutable COPY remains the exact-byte authority. One-million-row shards,
128,000-row groups, content-addressed files, per-shard semantic digests, full
readback before checkpoint, resumable publication, and atomic cycle pointers
are implemented. Per-row occurrence and record-version hashes are derived
from the fact-set ancestry and stored locators instead of repeated as high-
cardinality columns.

Ten million real rows first passed semantic, decision, size, scan, write, and
memory gates. The complete 264,085,606-row publication then proved the same
contract over the full selected relation. ArangoDB projections must consume the
published manifest; they must not treat a mutable directory scan as fact-set
membership. See the
[complete publication audit](../audit/schedule-a-columnar-publication-2026-08-31.md).

Receipt-calculation membership uses that exact fact-set manifest and a
versioned ordered predicate instead of a dense decision document per fact.
Only unresolved or invalid membership is materialized as sparse exceptions;
candidate results remain materialized. The complete 2024 calculation matched
the direct probe exactly and stored two exceptions. See the
[compact calculation publication audit](../audit/compact-receipt-calculation-publication-2026-08-31.md).

## Schema evolution

Source schema fingerprints and Legal Tender fact schemas are separate. A new
publisher column can produce a new source schema without immediately changing
the normalized schema. A semantic or typed-field change increments the fact
schema version and creates new facts; it does not mutate old facts.

Readers declare supported schema versions. Migrations rebuild derived facts and
projections from immutable record versions. They never rewrite source evidence.

## External references

- [FEC processed Schedule A weekly dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [FEC individual-contributions subset description](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/)
- [FEC candidate-committee linkage fields](https://www.fec.gov/campaign-finance-data/candidate-committee-linkage-file-description/)
- [FEC methodology for individual-contribution classification](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
- [openFEC Schedule A model](https://github.com/fecgov/openFEC/blob/develop/webservices/common/models/itemized.py)
