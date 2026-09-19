# Federal lobbying source ingestion

> **Status:** Research-backed deferred proposal for review. The common
> [source contract](./source-contracts.md) and the observed
> [LDA source schema](./lda-source-schema.md) apply. A valid LDA credential is
> configured locally; no source archive or Go adapter exists yet. Production
> requires a separate decision accepting the LDA API as the sole authority for
> these fact families. This source belongs to the later
> [legislative-influence phase](./legislative-influence.md), not candidate
> funding totals.

## Recommendation

If lobbying is promoted into production, use the current LDA.gov REST API as
the canonical structured source. Seed the complete API by filing year, capture
overlapping posted-date increments each Monday, and periodically reconcile
partitions whose offset pagination or mutable master records can drift.

There is no current official all-report bulk dump. The retired Senate
quarterly XML archive covers LD-1/LD-2 from 1999 through 2022 Q1, omits LD-203,
and no longer updates. Using it as the primary seed would add a second parser
without eliminating the need to crawl the current API.

The target acquisition pattern is therefore:

```text
complete API seed
    + Monday overlapping increments
    + Monday active-year reconciliation
    + Monday mutable-master snapshots
    + annual all-year reconciliation
```

The source's reported amount remains attached to its client/registrant filing.
Bills, issues, agencies, legislative roles, and votes are separate activity or
context relationships and cannot receive or duplicate that amount.

API JSON and printable filing documents are distinct source representations.
Preserve both; never replace one with parsed fields from the other. The
selected [API/document audit](../audit/lda-api-printable-comparison-2026-08-27.md)
found historical names, amount-band controls, and form grouping available only
in the printable representation, while IDs, standardized constants, and some
amendment types were available only in the API.

## Official source status

The [LDA public site](https://lda.gov/system/public/) directs researchers who
want all LD-1, LD-2, and LD-203 reports to the REST API. The
[current API root](https://lda.gov/api/v1/) exposes:

- Filings.
- Contribution reports.
- Registrants, clients, and lobbyists.
- Filing types, lobbying-activity issue codes, government entities, countries,
  states, lobbyist name affixes, and contribution item types.

The API publishes a machine-readable
[OpenAPI document](https://lda.gov/api/openapi/v1/) and
[rendered documentation](https://lda.gov/api/redoc/v1/). The OpenAPI artifact
retrieved on 2026-08-27 reported version `1.0.0` and SHA-256 digest:

```text
6c5f13c470bf1b4b09071db32d10df72372210fd2c2be921214e3c94f47dde8a
```

That digest is audit evidence, not a permanent expected value. Each run that
checks the remote contract records a new source-schema artifact; a changed
digest follows the schema-drift review in the common source contract.

The former `lda.senate.gov` API reached its documented sunset on July 31,
2026. New code uses only `lda.gov`.

### Retired XML archive

The [old Senate download page](https://www.senate.gov/legislative/Public_Disclosure/database_download.htm)
offers quarterly compressed XML for LD-1/LD-2 from 1999 through 2022 Q1. It is
useful as an independent historical completeness fixture. It is not current
bulk data and does not contain LD-203.

The current Senate public-disclosure page also contains a “Download the
Database” compressed-XML link, but that link belongs to gift-rule travel
disclosures. Its LDA section routes to LDA.gov.

## Corpus and acquisition cost

Counts observed on 2026-08-27 were:

| Endpoint | Records | Pages at 25 |
|---|---:|---:|
| Filings | 1,976,414 | 79,057 |
| Contribution reports | 674,034 | 26,962 |
| Registrants | 17,457 | 699 |
| Clients | 136,388 | 5,456 |
| Lobbyists | 88,712 | 3,549 |
| **Total** | **2,893,005** | **115,723** |

The API caps `page_size` at 25. It permits 120 requests per minute with a
registered key and 15 per minute anonymously. A perfect registered full seed
therefore has a lower bound of about 16.1 hours before retries, convergence
passes, validation, and document downloads. It is a resumable background job,
not a synchronous deployment step.

A sampled page was about 86 KB for 25 filings and 42 KB for 25 contribution
reports. Nested activity counts vary, so a bounded corpus benchmark must
measure total API JSON and printable-document storage before setting the
container and persistent-storage budgets.

## Authentication and source terms

The full seed should use a registered API key. Send it as:

```text
Authorization: Token <secret>
```

The key belongs only in the service's gitignored `.env`. It must never appear
in request manifests, logs, Dagster metadata, fixtures, or source URLs.

The [API terms](https://lda.gov/api/tos/) require the retrieval date and this
disclaimer in products using the retrieved data:

> Senate Office of Public Records cannot vouch for the data or analyses
> derived from these data after the data have been retrieved from LDA.gov.

Printable HTML/PDF documents and constants do not count against the documented
API request limit. A `429` response supplies `Retry-After`; the downloader must
honor it and checkpoint before waiting.

## Source grain and identities

### LD-1 and LD-2 filings

One filings-endpoint object is one occurrence of a source filing identified by
`filing_uuid`. Preserve at least:

- Filing type, year, period, post timestamp, termination date, document URL,
  and document content type.
- Income, expenses, and expense calculation method as separate raw meanings.
- Filing-level registrant and client data.
- Each lobbying activity with issue code, specific-issue text, lobbyists,
  covered positions, and government entities.
- Affiliated organizations, foreign entities and interests, and conviction
  disclosures.

Do not add `income` and `expenses` as if they were two components of one
universal amount. Registrants report one or the other under different filing
circumstances; the normalized fact preserves which meaning the filer used.
An API null is not zero. Until the printable form is observed, normalize it as
`unobserved` with a printable-document reason; the form can establish
`threshold_band`, `source_blank`, or `not_applicable` without establishing an
exact amount.

A numeric API value is preserved losslessly but is not automatically exact
economic money. LDA Method A uses good-faith estimates and source-defined
rounding. Other expense methods have their own accounting meanings. Store the
reported point, method, effective precision rule, and derived interval
separately under the shared [money-measure contract](./money-measures.md).

Registration thresholds are a separate coverage rule. Lobbying below the
applicable income or expense threshold may produce no registration or LD-2
record at all. Version those thresholds by reporting period and expose the
resulting statutory coverage boundary; do not infer the missing amount from
the $5,000 reporting band. The draft
[`lda.money.v1` table](../../contracts/sources/lda/rules/v1/rules.json) records
the 2013–2020, 2021–2024, and 2025–2028 registration periods separately from
the LD-2 amount rules.

The current API exposes filing years from 1999 onward. Reports before quarterly
reporting began use mid-year and year-end periods. The time model must preserve
filing period, filing year, post time, termination date, source retrieval time,
and later Legal Tender publication time separately.

### Government-entity scope caveat

The OpenAPI documentation states that filings posted before February 14, 2021
were imported with one filing-wide government-entity list. The API may repeat
that list within activity objects even though the source cannot prove that each
entity belongs to each activity.

Normalize government-entity association scope as:

| State | Meaning |
|---|---|
| `activity_scope` | Post-2021 source structure associates the entity with this activity. |
| `filing_scope` | Legacy import only establishes that the entity appears somewhere on the filing. |

Never construct an issue-to-agency edge from a `filing_scope` association.

### LD-203 contribution reports

One contribution-endpoint object is one source report identified by its own
`filing_uuid`. Preserve:

- Filing type, year, period, post time, filer type, and source document.
- Registrant or lobbyist source identity.
- PAC names and `no_contributions` state.
- Ordered contribution items with contribution type, contributor, payee,
  honoree, amount, and date.

The endpoint covers reports first required in July 2008. Official item types
include FECA contributions, honorary expenses, meeting expenses, presidential
library expenses, and presidential inaugural committee payments. LD-203 is not
a campaign-contribution feed. Only FECA-shaped items are candidates for FEC
reconciliation, and a resolved match links disclosures without duplicating the
amount.

Contribution items have no stable row ID. Their occurrence identity is:

```text
filing_uuid + JSON array path/index
```

Retain a content hash so a changed ordering or in-place API correction remains
observable.

### Master identities

Treat every numeric identifier as an opaque source-scoped value:

| Source object | Primary API identity | Additional evidence |
|---|---|---|
| Registrant | `registrant.id` | Optional House registrant ID and current profile. |
| Client | `client.id` | `client_id`, effective date, and registrant relationship. |
| Lobbyist | `lobbyist.id` | Structured name and registrant relationship. |

Do not merge API IDs, House IDs, client IDs, or names without a separately
tested identity contract.

Nested registrant/client objects inside old filing responses can contain a
recently updated master profile. They are not necessarily the name and address
as originally filed. Preserve filing-level “as listed” fields and the
printable filing document independently from current master assertions.

## Printable-document evidence

Download the publisher URL carried by each filing or contribution-report API
record. Preserve the exact HTML or PDF bytes, transport metadata, content type,
and digest before parsing form controls. A changed digest under one filing UUID
creates a new document observation and blocks unchecked derived-document
publication.

The document parser produces source assertions, not replacement API objects.
It must preserve:

- As-filed names, addresses, contacts, and agency text.
- Checked amount bands, reporting methods, and no-activity controls.
- Form-level issue-code grouping and repeating-section order.
- Signature timestamps and certification text.
- Explicit blank, unchecked, absent, invalid, and unobserved states.

API-only facts may publish as partial evidence when a document is unavailable.
A complete as-filed identity, form-grain, or amount-band claim requires the
document. A null API amount remains `unobserved`, not zero. Historical
document backfill is therefore required before the product claims complete LDA
coverage for those dimensions.

## Amendment, no-activity, and termination model

The API preserves originals, amendments, termination reports, termination
amendments, and no-activity variants as separate filing UUIDs. It does not
publish an `amends_filing_uuid` link.

Every filing remains queryable. An effective-period view is a versioned Legal
Tender calculation with candidate business keys:

| Filing family | Candidate business key |
|---|---|
| LD-1 registration | Registrant + client relationship. |
| LD-2 activity | Registrant + client + filing year + filing period. |
| LD-203 contribution | Filer type + filer identity + filing year + filing period. |

Candidate versions are ordered by `dt_posted` and classified using the pinned
filing-type constants. Any supersession link is an inference under a named rule
version, not a disclosed publisher relationship.

A termination report can still contain money and activity for its reporting
period. Termination changes lifecycle state; it does not delete the report or
make its disclosed activity zero.

Before an effective view can publish totals, validate the rule against real
original/amendment/no-activity/termination chains and their printable
documents. Until then, the evidence API may publish all filings but the
effective aggregate remains blocked or explicitly provisional.

## Acquisition design

### Complete seed

Partition filings and contributions by `filing_year`. A seed worker:

1. Captures the pinned OpenAPI document and constants.
2. Requests a year-filtered first page and records its count.
3. Follows publisher `next` URLs, preserving every raw response page.
4. Indexes results by `filing_uuid` without discarding duplicate occurrences.
5. Repeats the partition until UUID membership and per-UUID content hashes
   converge.
6. Publishes the partition only after occurrence conservation, schema,
   duplicate, and document-reference checks pass.

Filings and contribution reports require at least one query parameter to
paginate beyond page one. Year partitioning satisfies that requirement and
limits offset-page movement. Closed historical years should be stable, but the
convergence check still applies.

Seed registrants, clients, and lobbyists as independent complete snapshots.
Capture every constants endpoint as one small immutable snapshot.

### Weekly increments

Both filing endpoints support `filing_dt_posted_after` and
`filing_dt_posted_before`. These are date filters with strict boundaries. To
capture calendar day `D`, request an exclusive window from `D-1` through
`D+1`, then filter retained facts to the intended day while preserving the raw
overlap responses.

Each Monday run should:

1. Re-fetch from seven days before the last accepted posted-date watermark
   through tomorrow's exclusive date boundary. A missed run therefore widens
   the interval instead of creating a gap.
2. Capture pages and response metadata before parsing.
3. Deduplicate the published view by `filing_uuid` while retaining every
   occurrence.
4. Create a new record version if a known UUID's JSON changes.
5. Download and hash the printable document for each new or changed UUID.
6. Refresh registrants through overlapping `dt_updated` filters.
7. Capture and compare every constants endpoint.

The API uses offset pages rather than cursors. A window is complete only after
a convergence pass produces the same UUID membership and content hashes.

### Periodic reconciliation

| Cadence | Work |
|---|---|
| Monday 04:00 Eastern | Overlapping filing/LD-203 refresh, changed documents, active/previous-year reconciliation, registrant updates, complete client and lobbyist snapshots, and constants. |
| Annually | Reconcile every filing-year partition and all master tables. |
| On schema change | Capture, block affected publication, review contract, then replay. |

New filing payloads embed current client and lobbyist references, while weekly
full master snapshots cover in-place changes that cannot be selected through
an update filter.

## Raw storage layout

The physical manifest format remains a first-slice implementation decision,
but source partitions should distinguish acquisition time from disclosure
time:

```text
lda/
  schema/retrieved_date=YYYY-MM-DD/...
  constants/retrieved_date=YYYY-MM-DD/...
  filings/seed/filing_year=YYYY/...
  filings/incremental/posted_date=YYYY-MM-DD/...
  contributions/seed/filing_year=YYYY/...
  contributions/incremental/posted_date=YYYY-MM-DD/...
  masters/registrants/snapshot_date=YYYY-MM-DD/...
  masters/clients/snapshot_date=YYYY-MM-DD/...
  masters/lobbyists/snapshot_date=YYYY-MM-DD/...
  documents/<filing_uuid>/<content-digest>...
```

Each API page manifest records the normalized request without credentials,
retrieval start/end, status, response headers, byte length, digest, reported
count, page number, next/previous URLs, schema fingerprint, and downloader
version. The API currently supplies neither ETag nor Last-Modified validators;
content hashes are authoritative for observations.

## Dagster and Go boundary

Dagster owns these logical assets and partitions; Go owns every request,
checkpoint, parse, comparison, fact, and calculation:

```text
lda_schema_snapshot
lda_constants_snapshot
lda_filings_raw[filing_year | posted_date]
lda_contribution_reports_raw[filing_year | posted_date]
lda_master_snapshots[entity_type, snapshot_date]
lda_printable_documents[filing_uuid]
lda_filing_facts[filing_year]
lda_contribution_facts[filing_year]
lda_effective_periods[filing_year]
lda_fec_reconciliation[filing_year]
```

Dagster schedules the Monday and annual work and exposes partition health. Go
returns structured materialization and check results. The weekly schedule does
not imply global reprocessing: UUID/content changes emit
the exact affected filing periods, identities, organization resolutions, and
FEC reconciliation candidates.

## Publication checks

At minimum, block an affected partition when:

- A page cannot be retrieved after bounded retries.
- Pagination loops, skips a page, or fails to converge.
- The OpenAPI shape changed without an accepted contract.
- An occurrence is absent from both normalized facts and explicit issues.
- A non-null filing UUID is duplicated with conflicting content inside the same
  converged snapshot.
- Money, date, filing type, or required identity fields are invalid under the
  accepted contract.
- A printable document for a new/changed filing changes bytes unexpectedly or
  fails checks required by a complete as-filed projection.

Publish partial API evidence only when the contract explicitly permits the
condition and labels the document state as unobserved. Never convert a failed
page into “no filings,” and never convert an unavailable document into a zero
or historical-name assertion.

## Decisions still required

- Benchmark full JSON and document storage before choosing the document
  backfill rate and container memory cap.
- Measure printable-document availability and backfill API-only facts behind
  an explicit `unobserved` state and reason. Require the document before complete
  historical identity, form-grain, or amount-band claims.
- Validate effective-filing rules against representative real amendment and
  termination chains.
- Choose the first user-facing lobbying period. The acquisition design can
  backfill 1999 onward even if the initial UI starts with the latest four FEC
  cycles.
- Decide when the retired XML archive comparison adds enough independent
  validation to justify its second parser.

## Adjacent official disclosures

The FEC publishes lobbyist/registrant PAC and Form 3L bundled-contribution
data. It belongs in campaign-finance ingestion, not inside LDA totals. Form 3L
regular and semiannual covered periods can overlap, so those rows require a
non-additive effective-period calculation.

DOJ FARA publishes daily bulk CSV/XML and an API for foreign-agent disclosures.
FARA is a separate legal and evidence domain. It can later share entity
resolution with LDA but must never be merged into domestic lobbying totals.

## Primary references

- [LDA public data home](https://lda.gov/system/public/)
- [LDA API root](https://lda.gov/api/v1/)
- [LDA OpenAPI contract](https://lda.gov/api/openapi/v1/)
- [LDA rendered API documentation](https://lda.gov/api/redoc/v1/)
- [LDA API terms](https://lda.gov/api/tos/)
- [Official LDA reporting guidance](https://www.senate.gov/legislative/resources/pdf/S1guidance.pdf)
- [Official LDA registration thresholds](https://www.senate.gov/legislative/Public_Disclosure/new_thresholds.htm)
- [Retired Senate quarterly XML archive](https://www.senate.gov/legislative/Public_Disclosure/database_download.htm)
- [FEC lobbyist bundled-contribution file description](https://www.fec.gov/campaign-finance-data/lobbyist-bundled-contributions-file-description/)
- [DOJ FARA bulk repository](https://efile.fara.gov/ords/fara/f?p=107:21)
