# Schedule A acquisition and freshness strategy

> **Status:** Accepted processed-source design under the
> [coordinated FEC bulk-release strategy](./fec-release-strategy.md). Exact
> catalog/schema, four-target extraction evidence, classic-overlap evidence,
> the Go reader/verifier, metadata-only planning, body acquisition, runtime
> selected-relation staging, atomic source-release publication, and immutable
> per-cycle occurrence/change publication have landed. Lossless normalized
> receipt-fact publication passed the complete 2024 corpus gate; compact
> dependent evidence and domain persistence remain open. Raw-filing and processed/raw reconciliation
> work is preserved as deferred research.

## Decision

Legal Tender uses the official processed Schedule A PostgreSQL dump as the one
canonical production source for itemized FEC receipts. A coordinated FEC
release checks and acquires changed versions every Monday, streams only
the rolling four selected two-year relations through `pg_restore`, and never
runs PostgreSQL as the domain database.

Initial production does not call OpenFEC APIs or raw electronic-filing
endpoints. The processed Schedule A row API, raw `.fec` contracts, classic
`indiv`, and classic `oth` remain targeted-fixture, diagnostic, research, or
comparison evidence. None patches a missing or failed processed source.

The production cadence observes metadata and plans a complete candidate
release every Monday at 04:00 `America/New_York`. An unchanged source version
is reused without downloading or recomputing it.

## Evaluated representations

The official sources optimize different properties:

| Source | Strength | Limitation | Selected role |
|---|---|---|---|
| Processed Schedule A dump | Complete then-current processed relation, FEC coding, paper and electronic coverage, two-year partitions. | One growing all-history artifact; no per-partition download or delta; not an immutable amendment history. | Canonical production receipt source. |
| Processed Schedule A API | Same processed model, load-date filters, targeted record access, keyset pagination. | At most 100 rows per response; rate limited; load-date discovery cannot prove physical deletions; large filing waves require too many requests. | Targeted lookup, tests, and samples. |
| Raw electronic filing `.fec` | Exact as-filed document, one request per filing, versioned format marker, report and amendment context, available as received. | Electronic filings only; preliminary; lacks later FEC coding and processed `sub_id`; requires API discovery, form-version parsers, and separate effective-report calculations. | Deferred possible as-filed product. |
| Classic `indiv` ZIP | Period-partitioned, materially smaller, processed individual-contribution subset, already captured locally. | Explicitly a threshold-dependent subset of itemized individual receipts; physical shape differs from full Schedule A. | Independent comparison and coverage check. |

The production view does not synthesize these representations. It states the
processed snapshot it uses and leaves unavailable or failed source coverage
stale or unresolved. A future as-filed product requires a separate accepted
source authority, release, calculation, and UI boundary.

## Point-in-time measurements

The source decision was measured on 2026-08-27:

- The official public dump prefix contained one Schedule A object, not one
  object per two-year partition. Its size was 89,883,424,294 bytes. The FEC
  states that the dump covers 1975 onward, uses two-year inherited partitions,
  and is replaced weekly on Saturday.
- A processed API query for `two_year_transaction_period=2026` and load dates
  2026-08-16 through 2026-08-27 reported approximately 2,681,949 rows. At the
  accepted 100-row page size, that is at least 26,820 requests before retries,
  page capture, or reconciliation.
- The configured API key reported a request-limit header of 60 for the probed
  window. The code must obey returned limit and retry headers rather than
  assume a fixed undocumented interval.
- The electronic-filings endpoint reported 2,388 F3/F3P/F3X filings over the
  same receipt-date range. That listing needs 24 API pages at 100 records per
  page, followed by content-addressed filing downloads. A filing can contain
  many schedule rows, so filing-grain transfer avoids one API response per 100
  receipts.
- A sampled `.fec` file was available directly from the official document
  host. It began with an `HDR` row, version `8.5`, field separator byte `0x1c`,
  and LF record endings. Parser selection must use the pinned header version;
  this sample does not establish every historical format.

The 2026-08-23 dump was acquired and probed further on 2026-08-28. Its exact
first MiB contains the complete custom-archive catalog plus pre-data DDL.
PostgreSQL 15 `pg_restore` reports 27 Schedule A relations and 27 data entries;
the parent exposes the contracted 81 fields, and
`fec_fitem_sched_a_2025_2026` inherits that parent with an exact 2025/2026
check. The complete artifact is content-addressed, and its full catalog
inventories successfully.

The four selected extractions measured:

| Relation | Rows | COPY bytes | Restore + validation + digest |
|---|---:|---:|---:|
| `fec_fitem_sched_a_2019_2020` | 293,862,761 | 206,363,392,958 | 885 s |
| `fec_fitem_sched_a_2021_2022` | 166,775,547 | 117,038,862,557 | 496 s |
| `fec_fitem_sched_a_2023_2024` | 264,085,601 | 182,881,290,413 | 749 s |
| `fec_fitem_sched_a_2025_2026` | 166,293,056 | 114,190,988,781 | 493 s |
| **Total** | **891,016,965** | **620,474,534,709** | **2,623 s** |

Each capped restore produced exactly one COPY section and every row had 81
fields. An independent Go replay of the complete 2025/2026 extract reproduced
its row count and SHA-256 while decoding every source value. The four
uncompressed extracts total 577.86 GiB, so they are a transient validation
form, not the accepted hot-storage representation.

Each extract was stored again as zstd level 3, frame-tested, and fully
decompressed through SHA-256 comparison. The four compressed artifacts total
58,987,591,718 bytes (54.94 GiB, 9.51% of COPY bytes). Runtime Go reads these
as streams; it does not retain an uncompressed second copy after acceptance.

The repository Go reader then completed the full 2025/2026 compressed stream
in 469.606 seconds with an 8,316 KiB peak resident set. It accepted all
166,293,056 rows, rejected none, and exactly reproduced both compressed and
uncompressed byte counts and SHA-256 digests. This proves the implemented
reader for one complete target relation. The occurrence publisher now uses
that same streaming reader, but this historical corpus pass predates the
publisher and did not create an occurrence set. No complete production
occurrence publication or normalized-fact publication has run.

### Classic-product overlap result

An exact 2024 audit compared all 264,085,601 processed Schedule A `SUB_ID`s
with the exact `itcont.txt` and `itoth.txt` members already held by the legacy
pipeline. The processed relation contains 189,514,597 rows present in neither
classic product. Under the accepted itemized-individual calculation rule,
168,856,391 of those rows contribute a signed $3.158 billion—75.99% of the
cohort's row grain and 19.88% of its amount.

A separate non-individual, non-memo diagnostic found 1,334,580 additional rows
and $1.774 billion outside both classic products. That cohort is not yet a
committee-flow calculation, but it proves that `indiv` plus `oth` is not a
complete receipt boundary. The full evidence and snapshot-skew limitations are
in the [classic-file overlap audit](../audit/schedule-a-classic-overlap-2026-08-28.md).

These are dated planning measurements, not permanent constants. Production
discovery records live bulk-object validators and sizes on every run. The API
counts and limits above remain research evidence; initial production does not
re-query them.

## Processed baseline lane

### Discovery

A lightweight observation runs every Monday at 04:00 `America/New_York`, reads
the official S3 object metadata, and records key, version ID when present,
last-modified value, ETag, checksum metadata, and content length. Discovery
does not download the object and does not claim a new accepted snapshot.

### Acquisition triggers

Download the complete dump only when one of these conditions applies:

- the initial target-period seed has no accepted processed baseline;
- Monday discovery finds a stable source version different from the accepted
  version;
- a target election period is being closed or labeled complete;
- a parser, source contract, or product release requires a new corpus proof;
  or
- an operator explicitly requests a refresh.

Acquisition selects the newest stable observed version. If a weekend
publication is late, the run waits and retries rather than publishing the prior
artifact as a new release. The schedule is an operational default, not fact
time. Every product result exposes the actual processed snapshot and coverage
dates.

### Streaming extraction

The first corpus probe uses a version-pinned `pg_restore` container to:

1. verify the custom-format archive and inventory its table-of-contents;
2. capture schema-only output for the accepted parent relation and selected
   inherited partitions;
3. stream data-only `COPY` output for the target partitions to the Go parser;
4. require exactly one `COPY` section for each selected catalog relation;
   `pg_restore` can exit successfully when a table pattern matches nothing, so
   process success alone cannot accept an empty extract;
5. preserve SQL null, COPY escaping, relation identity, extraction ordinal,
   source values, and whole-dump lineage; and
6. write content-addressed partition extracts and indexes without building
   PostgreSQL indexes or a permanent database.

A temporary PostgreSQL restore remains a benchmark fallback if streaming COPY
parsing cannot satisfy conservation or throughput gates. Any temporary
container needs an explicit memory cap and no host port.

The full custom dump and every accepted derived partition extract remain
immutable. Before accumulated dump snapshots threaten the project storage
budget, the project must add a content-addressed cold-storage tier; it must not
silently delete evidence. The initial run needs room for the prior accepted
dump, the candidate dump, and selected extracts concurrently.

### Hot-storage budget

The [bounded streaming-storage contract](./fec-streaming-storage.md) owns the
implemented budget. Keep the **600 GiB hot cap**, **500 GiB filesystem free
floor**, and **25 GiB margin**. Count every retained device/inode's logical
bytes once, including partials, completed extracts, and compressed temporary
files. The cap is a circuit breaker, not a retention target.

The extractor streams COPY directly into zstd. It never materializes a full
uncompressed relation, so the former 206,363,392,958-byte working reserve is
zero in new results. Acquisition checks remaining downloads and a prior-size
full-output scenario before GETs. Shared write guards enforce actual growth
during acquisition/staging; each completed output reduces the next allowance.

The [September 9 gate](../audit/fec-streaming-storage-2026-09-09.md) passes
failure/retry, exact-byte comparison, bounded performance, and read-only real
planning checks. A fitting scenario is not a guarantee of unseen output size
or permission to acquire. No source was downloaded, deleted, or activated in
that gate. Full decompressed digest verification still precedes publication.

Verified cold retention remains required before perpetual refresh automation.
Until that exists, an over-budget refresh blocks and leaves the prior release
published. Do not delete or overwrite publisher evidence to force a refresh.
Changing cap/floor/margin remains an explicit measured storage decision, not
an automatic response to free host space.

The default four-cycle view selects four physical relations; it does not turn
their suffixes into fact time:

| Product cycle | Processed source relation |
|---:|---|
| 2020 | `fec_fitem_sched_a_2019_2020` |
| 2022 | `fec_fitem_sched_a_2021_2022` |
| 2024 | `fec_fitem_sched_a_2023_2024` |
| 2026 | `fec_fitem_sched_a_2025_2026` |

Every row still retains receipt date, report year, FEC election year,
two-year transaction period, publisher load timestamp, and snapshot time as
independent values. A future default window can select different relations
without reparsing or changing preserved facts.

## Deferred raw electronic-filing design

This section preserves the evaluated as-filed design and its exact research
artifacts. It is not part of the initial production path and creates no daily
Dagster partitions or API dependency. If a future decision promotes it, it
becomes a separate source family and product view; it never patches processed
Schedule A.

### Discovery window

The evaluated design requests one daily `efile_received_date` partition. Go
would query a closed receipt-date interval with an overlap, capture every API
page, and run a convergence pass against the same closed interval.
The interval is complete only when the file-number set and page evidence are
stable. Offset pagination, late arrivals, API errors, or a count mismatch never
publish an empty or partial day as complete.

Do not restrict acquisition to a hand-maintained list of forms. Capture every
filing returned for the interval. The parser can normalize supported Schedule A
rows now while preserving every other physical row for later contracts.

### Filing capture

For every discovered file number:

- preserve listing metadata, `fec_url`, `csv_url`, receipt and load timestamps,
  form type, report coverage, amendment fields, and the complete observed
  amendment chain;
- acquire the exact `.fec` bytes from the publisher URL with bounded retries;
- record final URL, response validators, size, retrieval time, and SHA-256;
- select a parser from the `HDR` format/version, never from a filename or the
  latest schema;
- emit one occurrence or explicit issue for every physical row; and
- fetch newly referenced predecessor filings needed to close an amendment
  family when they are not already present.

The publisher CSV can be comparison evidence. It does not replace `.fec` as
the exact as-filed artifact unless a separate contract accepts that
representation.

### Raw publication state

Raw normalized Schedule A rows publish as `as_filed`, not `processed`. A
versioned report-family calculation can identify the latest complete
electronic filing in an amendment chain. It must preserve originals,
amendments, terminations, repeated transaction IDs, memo rows, and explicit
unresolved chains.

No raw row participates in a user-facing current total until the raw-filing
source contract and effective-report calculation pass their own gates. When
they do, the API exposes the raw view as provisional and states that paper
filings and later FEC processing can change it.

## Deferred processed/as-filed reconciliation model

Processed and as-filed facts are related through evidence such as committee
ID, file number, report family, transaction ID, form and line, date, amount,
and content. A processed `sub_id` is not invented for a raw row.

Every comparison produces one explicit state:

- `corroborated`
- `representation_only_difference`
- `raw_only_pending_processing`
- `processed_only`
- `processed_changed`
- `conflict`
- `unresolved`

The [reconciliation calculation](./schedule-a-reconciliation.md) applies these
states in two stages: direct same-filing source comparison first, then
effective-revision alignment. This prevents an older processed snapshot from
turning a newer raw amendment into a false `processed_changed` result.

At a new processed baseline, calculations rebuild only report families,
committees, candidates, and periods named by the reconciliation change set.
The prior processed and raw views remain reproducible.

## Production Dagster and Go propagation

Source acquisition uses native source partitions:

| Asset class | Dagster partition |
|---|---|
| Coordinated FEC release discovery and acquisition | Observation/release data version, not a cycle partition |
| Candidate, committee, linkage, and summary bulk products | `fec_cycle` selected from the release |
| Processed Schedule A dump discovery | observation/data version, not a cycle partition |
| Processed dump extraction, occurrence publication, and domain projections | `fec_cycle` selected from the dump |

Go now owns natural-key and semantic-digest change detection between accepted
Schedule A releases. The occurrence change set names source records by cycle;
the next normalization and relationship layer will map those records to
committee IDs, candidates reachable through accepted linkage facts, and
calculation versions. Dagster fans a published release into targeted cycle
runs. A complete publisher download does not cause an unconditional complete
graph rebuild.

The API exposes at least these separate watermarks:

- coordinated FEC release ID and publication time;
- processed dump snapshot and extracted-through state;
- candidate, committee, linkage, and summary source observations; and
- applicable summary coverage-through dates.

There is no single source-agnostic “updated at” timestamp.

## Rejected alternatives

### Download an unchanged processed artifact every week

The Monday schedule observes every source, but an artifact with the same
accepted content identity is reused. Re-downloading known bytes adds transfer
cost without freshness. Dagster can avoid downstream recomputation after
hashing and diffing, but it cannot remove the 89.9 GB transfer granularity when
the publisher actually changes the Schedule A object.

### Seed and increment entirely through the processed row API

Keyset pagination is correct for targeted queries, but 100-row pages and
publisher rate limits make multi-million-row load windows unsuitable for the
main path. A load-date window also cannot independently prove that a row
disappeared from a later processed snapshot.

### Use classic `indiv` as complete Schedule A

The FEC describes `indiv` as a threshold-dependent subset of itemized
individual contributions. Treating it as complete would recreate the legacy
coverage ambiguity and lose other Schedule A receipt categories.

### Build only from raw filings

Raw filings provide the best incremental grain but omit later FEC coding and
do not alone establish complete paper and historical coverage. They cannot
replace the canonical processed product. A future as-filed product must remain
separate rather than patching it.

## Required implementation contracts

1. ~~Define the machine-readable `fec.release.v1` manifest, source-selection
   plan, state transitions, checks, and fixtures.~~ The v1 release inventory,
   schemas, and four planner-result fixtures are checked in.
2. ~~Implement metadata-only Monday discovery and candidate-release selection
   in Go.~~ The shipped commands emit `no_change`, `source_not_ready`,
   `invalid`, or an exact `update_available` version-frozen plan without
   downloading source bodies.
3. ~~Implement resumable, storage-gated Schedule A acquisition and exact
   selected-relation extraction under that plan.~~ Acquisition, exact one-
   section extraction, zstd conservation, checkpoint reuse, and source-release
   publication are implemented. The first coordinated source release was
   published on 2026-08-30.
4. ~~Implement immutable Schedule A occurrence, issue, record-version, and
   snapshot-index publication before normalized facts.~~ The shipped
   per-cycle publisher conserves every physical row, records exact raw
   locators and issues, and atomically publishes immutable evidence artifacts.
5. ~~Implement snapshot-to-snapshot natural-key and semantic-digest change
   sets.~~ The shipped external-sort index reports added, changed, absent,
   unchanged, duplicate, and invalid states. Mapping those record changes to
   affected committees and candidates remains part of normalization.
6. ~~Benchmark `pg_restore` streaming extraction against the complete dump and
   selected four target partitions.~~ All four pass row, byte, digest, and
   compressed-storage conservation. Complete value-level Go replay has also
   passed for 2025/2026. The repository now contains the bounded-memory Go
   COPY/zstd reader, exact fixture replay, and verification command;
   selected-relation source publication and occurrence publication are
   implemented. The first full-row normalized-fact JSON layout failed its
   complete-cycle storage gate. A replacement 99-column Parquet contract and
   resumable publisher now use deterministic source-row shards, full semantic
   readback, and atomic publication; its complete 2024 gate passed.
7. ~~Set the dump-artifact disk budget and cold-storage trigger in the target
   storage design before scheduled reconciliation starts.~~ The initial 600
   GiB hot cap, 500 GiB filesystem-free floor, 25 GiB margin, and retained-
   evidence rule remain. Streaming runtime enforcement is implemented; verified
   cold retention is still required before perpetual scheduled refreshes.

The existing electronic-filing, raw Schedule A, effective-report, and
processed/raw reconciliation contracts remain deferred research artifacts.
They are not gates for the initial bulk-only release.

## Official references

- [FEC processed schedules dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [OpenFEC API documentation](https://api.open.fec.gov/developers/)
- [FEC receipts data description](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/about-receipts-data/)
- [FEC individual-contributions bulk-file description](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/)
- [FEC committee-to-committee bulk-file description](https://www.fec.gov/campaign-finance-data/any-transaction-one-committee-another-file-description/)
