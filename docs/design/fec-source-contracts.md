# FEC source contracts

> **Status:** Source maturity is explicit per contract. Versioned source
> contracts and their first fixtures exist. Four describe
> deferred API/raw-filing research and are not initial production dependencies.
> Go occurrence and normalized-fact publishers now implement the five classic
> initial products and the accepted Schedule A path. Schedule E now has a
> machine-readable source contract, complete-corpus parser proof, coordinated
> release v2, selected-cycle occurrence evidence, lossless facts, and four
> published effective calculation sets. Schedule B has a complete artifact,
> strict parser, classic-comparison proof, and passing same-publisher-batch
> Schedule A alignment. Release-inventory v3 and the lossless selected-cycle
> columnar publisher now implement its physical production boundary. Active v4
> adds accepted lossless committee-summary publication for all four cycles,
> with complete raw/fact readback and replay; financial grouping remains separate.

## Result

The first machine-readable FEC contract set is under
[`contracts/sources/fec/`](../../contracts/sources/fec/):

Candidate, committee, linkage, summaries, and processed Schedule A belong to
the initial coordinated bulk release. V2 adds processed Schedule E; v3
adds processed Schedule B as archive-direct selected relations. Active v4 adds
whole committee-summary CSVs and release-bound facts. `schedule-a-api`
and the three `efile-*`
contracts remain diagnostic or deferred as-filed research under the
[release strategy](./fec-release-strategy.md); their presence does not
authorize production API acquisition.

| Contract | Source grain | Independent time or partition fields |
|---|---|---|
| `fec/candidate-master@1.0.0` | One physical candidate-master occurrence. | Acquired two-year snapshot; `CAND_ELECTION_YR`. |
| `fec/committee-master@1.0.0` | One physical committee-master occurrence. | Acquired two-year snapshot. |
| `fec/candidate-committee-linkage@1.0.0` | One physical linkage occurrence identified by `LINKAGE_ID`. | Acquired two-year snapshot; `FEC_ELECTION_YR`; `CAND_ELECTION_YR`. |
| `fec/all-candidates-summary@1.0.0` | One physical candidate summary occurrence in the all-candidates population. | Acquired two-year snapshot; row-level `CVG_END_DT`. |
| `fec/current-campaigns-summary@1.0.0` | One physical summary occurrence in the current House and Senate population. | Acquired two-year snapshot; row-level `CVG_END_DT`. |
| `fec/schedule-a@1.0.0` | One row from the processed Schedule A relation or inherited period partition. | Dump snapshot; receipt date; report year; FEC election year; two-year transaction period; publisher load timestamp. |
| `fec/schedule-b@1.0.0` | One row from a processed Schedule B inherited period partition selected by coordinated release v3. | Dump snapshot; disbursement and communication dates; report year; two-year transaction period; publisher load timestamp. |
| `fec/schedule-e@1.0.0` | One row from the processed all-history Schedule E relation. | Dump snapshot; expense, dissemination, signature, report, election-cycle, and publisher processing times remain independent. |
| `fec/schedule-a-api@1.0.0` | One processed Schedule A result occurrence inside one exact targeted API response. | Receipt, report, election, load, and Legal Tender API-observation times remain independent. |
| `fec/efile-listings@1.0.0` | One listing occurrence inside one exact API page; `file_number` is the publisher record key. | Receipt, filed, load, report coverage, and Legal Tender observation times. |
| `fec/efile-documents@1.0.0` | One exact `.fec` artifact and every ordered physical record within it. | Listing receipt partition and independent document observation. |
| `fec/efile-schedule-a@1.0.0` | One physical as-filed Schedule A occurrence within one document version. | Contribution date, listing receipt, report coverage, document observation, election code, and later target-period projection remain independent. |

The source partition is never the only time field. A 2024 candidate or linkage
archive can contain `CAND_ELECTION_YR=2020`, and one 2024 summary archive can
contain rows with different coverage end dates. The target four-period view is
a calculation over preserved time dimensions, not a parser rule. For the
processed dump, product cycles 2020, 2022, 2024, and 2026 currently select the
`2019_2020`, `2021_2022`, `2023_2024`, and `2025_2026` inherited relations;
the row's independent dates and years remain unchanged.

## Local pipe-delimited evidence

The 2024 artifacts were captured by the legacy sync on 2026-06-21. Their
metadata points at the official FEC bulk URLs. The contract fixtures map exact
physical source lines to the ordered local header evidence without changing
values or empty fields.

| Dataset | Exact member | Rows | Fields per observed row | Archive SHA-256 | Member SHA-256 |
|---|---|---:|---:|---|---|
| Candidate master | `cn.txt` | 9,799 | 15 | `2e4135da30780f81bbea764ac2e28676f00211a897e104c68a1821b4fc680db9` | `ab2a7975a16b3a274a3152674eb9e94a3adc4f28dc9b63826366d7e36b9f7cf8` |
| Committee master | `cm.txt` | 20,938 | 15 | `ad94580c9ea72564289c1f4e3d5756f9cbbd41a2696279a66270339a299bb5da` | `958b7183e3d06f92c7b702b18eeb438ec52d304e49844bfdba40e25fc685131d` |
| Candidate-committee linkage | `ccl.txt` | 8,620 | 7 | `c02fab1519161352db84919b0bba0ffffdfa7aab32054292d7b260b9ab769618` | `ac7b98148b17ecba02b023a237aa546b6d670b35cf045b84722a22e2822dd0e8` |
| All-candidates summary | `weball24.txt` | 3,856 | 30 | `339ff7a6b97936b425a5da00e358d4d13a462b96c8b5a062fca133d7a0fa1e92` | `3fed72cec7f85738ca72252b02d0cdcd36726e5eb4d3caf94d20da716d4d43c6` |
| Current-campaign summary | `webl24.txt` | 2,377 | 30 | `14f58032ede9babfaac5842465d945f5d85f6b18294ab3a39a1819766c2b459e` | `755455069fa412d3e4d23e0bd0ddc7af53d041440b1a1067dfda1f58ad956eab` |

The exact member names matter. The new parser must not open the first ZIP
member it happens to encounter. It also must not reproduce the legacy parser's
`errors="ignore"`, short-row padding, extra-field truncation, or dictionary
overwrite of duplicate IDs.

All twenty locally captured combinations of these five datasets and the 2020,
2022, 2024, and 2026 periods were then checked directly. Each archive contains
the exact contracted member, decodes strictly as UTF-8, uses LF record endings,
ends with a final LF, and has one observed row width matching its contract. The
contracts include exact one-line byte fixtures as well as mapped JSON fixtures,
so physical parsing and semantic record shape can be tested independently.

The candidate, committee, and linkage contracts cite the official FEC header
files. The official all-candidates and current-campaign file-format tables each
enumerate the same 30 columns and positions preserved by the local headers and
record schema. This closes the summary-header provenance gate while retaining
the two sources as different publisher populations.

## Summary money is not transaction money

The two summary sources share a physical layout but not a population. They
remain separate datasets. Every monetary column stays an independent signed
decimal assertion with its raw lexeme. A normalizer can emit a common
`reported_point`, but it cannot:

- assume `TTL_RECEIPTS` equals a selected local sum of other columns;
- fill missing itemized facts with summary totals;
- manufacture contributor identities from summary categories;
- clamp a negative refund or correction to zero; or
- aggregate candidates or authorized committees without addressing the
  publisher's transfer double-counting warning.

`CVG_END_DT` is the row's financial boundary. The ZIP path is only the source
snapshot partition.

## Processed Schedule A evidence

The official weekly endpoint redirects to a PostgreSQL custom-format dump.
The point-in-time HEAD response on 2026-08-27 reported 89,883,424,294 bytes,
which makes accidental weekly full downloads an operational risk. Selective
`pg_restore` can reduce restored relations but cannot avoid transferring the
complete artifact.

The official dump README and OpenFEC model define the parent
`disclosure.fec_fitem_sched_a` relation and its inherited two-year partitions.
An exact first-MiB range from the 2026-08-23 artifact contains the complete
custom-archive catalog and pre-data DDL. PostgreSQL 15 `pg_restore` identifies
27 Schedule A relations through `fec_fitem_sched_a_2025_2026`, 27 data
entries, and the selected partition's exact inheritance and period check. The
parent DDL exposes the same 81 fields, in the same order, as the record schema,
including source types, declared nullability, and defaults. The observation is
content-addressed under the Schedule A contract.

The complete artifact was then acquired under its observed ETag and validated
at exactly 89,883,424,294 bytes with SHA-256
`caead0f0fc2b5cd6583a194671349830ff94e6c7f8d139acddf102cb6b3039ea`.
The full catalog inventories successfully. Selective restore of
`fec_fitem_sched_a_2025_2026` produced one COPY section with 166,293,056 rows,
114,190,988,781 bytes, and SHA-256
`caff37ccb61c5c1bf07ebef14837411bde29a6c46811c5d8ed4ee555aa4db1de`.
Restore, 81-field validation, and the digest pass took 493 seconds under the
pinned 1 GiB/1-CPU container cap.

The other three target relations passed the same extraction gates. Across
2019/2020 through 2025/2026, the baseline contains 891,016,965 rows and
620,474,534,709 COPY bytes (577.86 GiB). Lossless zstd-3 storage copies total
58,987,591,718 bytes (54.94 GiB, 9.51%); every frame test passed and a full
decompression reproduced its uncompressed extract SHA-256.

An independent standard-library Go replay conserved the same row count and
digest while decoding every COPY value. It also found a contract defect before
publication: both receipt and publisher load fields are PostgreSQL
`timestamp without time zone` values with a space separator in COPY output.
The earlier API-derived schema treated receipt time as date-only and load time
as an API-style `T` timestamp. The dump contract now preserves the exact
PostgreSQL lexical form; the separate API contract preserves the API form.

Eight paired exact-COPY and canonical fixtures now cover negative money, SQL
null receipt time, escaped COPY text, action codes C/F/N, and the targeted
individual/memo back-reference pair. Canonical extraction keeps PostgreSQL
`NUMERIC` and large identifiers as strings, SQL null as null, booleans as
booleans, and timezone-free timestamp lexemes unchanged.

The two fixtures came from one official Schedule A API image and were mapped
into relation-shaped JSON:

- an individual receipt with receipt type `15E`, earmark memo text, and
  back-reference fields; and
- a related committee conduit-total row with `memo_cd=X`.

This pair demonstrates why ingestion cannot collapse rows early. Both are
source evidence, while a later versioned counting rule decides how they affect
a total. The fixtures are marked `synthetic_json` because an API projection is
not an exact restored relation row. Dump-only search vectors are unobserved in
these fixtures.

The separate targeted API contract preserves complete response pages and all
observed result values without pretending they are dump rows. Its exact
2026-08-27 fixtures capture 23 rows for one original-filing image range and a
complete zero-result page for its same-day amendment range. Every returned row
identifies the original file. The API evidence is suitable for an exact,
time-bounded reconciliation test but not a processed-period baseline.

The API has no advertised `file_number` filter. Targeted filing observations
bind an official listing image range plus committee and transaction period,
then verify every returned file number. They use keyset pagination; an offset
`page` parameter cannot establish complete traversal for this endpoint.

## Deferred raw electronic-filing evidence

The rejected routine-incremental design produced three reusable source
contracts and a format dispatch. They remain research for a possible separate
as-filed product:

- the listing contract preserves complete offset-paginated API pages and treats
  amendment fields as independent mutable publisher assertions;
- the document contract preserves every referenced `.fec` response byte and
  frames every physical row without selecting an effective report;
- the raw Schedule A contract maps physical rows to the official ordered layout
  while retaining exact empties, omitted trailing fields, extra fields, byte
  ranges, and row digests; and
- the format dispatch selects parsers only from HDR field 3. Versions 8.3,
  8.4, and 8.5 currently map to the same pinned 45-field Schedule A layout.

The official v8.5 specification defines field separator byte `0x1c`, CRLF
records, HDR as the first record, and the filing cover as the second. The exact
fixtures also prove official documents with bare LF records. The parser accepts
that observed framing variance without normalizing the stored artifact.

The listing fixtures capture one original/amendment family. The predecessor
record says it is superseded but its `amendment_chain` contains only itself;
the successor contains both file numbers. Family closure must therefore follow
all preserved link assertions and document headers instead of trusting one
array.

The Schedule A fixtures include an organization receipt and a memo row. The
same memo row bytes and transaction ID occur in the original and amendment.
Both are retained with different parent digests and byte offsets. This is a
source-level proof that transaction ID or content equality cannot collapse
amendment occurrences during ingestion.

## Initial bulk-contract acceptance gates

The initial production contracts move from `draft` to `accepted` only after
these gates close:

1. ~~Pin official publisher evidence for the two 30-column summary headers.~~
   The two FEC file-format tables were verified against the local headers and
   record schema on 2026-08-28.
2. ~~Complete the Schedule A dump proof.~~ The whole artifact, catalog, all 81
   fields, four complete target data blocks, lossless compressed storage
   copies, and exact 2025/2026 fixtures pass. Runtime Go fixture replay now
   lives in the repository; complete operation integration through atomic
   source-release and occurrence publication has landed. The rejected JSON
   receipt-fact layout has been replaced by a 99-column Parquet contract and
   resumable publisher; complete 2024 publication passed.
3. Expand the exact Schedule A corpus. Negative amount, null receipt time,
   COPY escaping, action A/C/F/N, non-memo, memo-X, and back-reference rows now
   exist. Add ordinary memo rows without X, more conduit shapes, and historic-
   period examples.
4. ~~Define and validate the pre-download `fec.release.v1` boundary, including
   exact source membership, independent observation times, `no_change`,
   `source_not_ready`, `update_available`, and invalid outcomes.~~ The
   language-neutral schemas, Go inventory/discovery/planner, fixtures, and
   live metadata check, fail-closed acquisition, selected-data staging, and
   atomic source-release publication have landed.
5. ~~Connect the five classic member streams to immutable occurrence, issue,
   change-set, and normalized-fact publication.~~ The code and explicit
   Dagster multi-partitions are implemented. All five products are published
   for 2024; candidate and committee masters are also published for 2020,
   2022, and 2026. Remaining classic slices materialize when a downstream
   accepted boundary requires them.
   Deterministic Schedule A COPY/zstd replay and columnar
   `fec.schedule_a_receipt.v1` publication are implemented; the complete 2024
   run passed its acceptance gate.

Deferred as-filed acceptance gates remain recorded separately: pin electronic,
paper, and historical coverage boundaries; expand all target form/version and
framing cases; and complete the effective-filing and processed/raw
reconciliation corpus. They do not block or feed the initial bulk release.

Schedule B now has a complete 39.31 GB artifact identity, exact 81-field
contract, strict 157,544,163-row 2024 parser proof, and classic direction and
amount comparison. The physical and classic gates pass. The immutable
2026-08-30 Schedule A/B comparison also passes every source-integrity and
conservation gate; unique exact or same-amount/different-date candidates cover
82.02% of accepted Schedule A flow amount. The accepted v3 release owns
the Schedule B archive, and the lossless selected-cycle columnar publisher
implements its physical fact boundary. Effective-flow and reconciliation
policy remain separate. See the
[Schedule A/B alignment audit](../audit/schedule-ab-alignment-2026-09-04.md).
Debts, loans, and raw filing versions still need source selection and source
contracts. Schedule E is the accepted independent-expenditure
occurrence authority. Its exact 80-column source contract and strict parser
pass the complete current relation, and release-v2 facts are implemented. The
separate effective calculation now publishes exact spender-candidate-stance
results for all four target cycles. Candidate resolution, resolved grouping,
readiness, and graph projections also pass unchanged for all four cycles. See
the [cross-cycle independent-expenditure audit](../audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md).

## Verification

Repository validation currently proves:

- all source contracts conform to `source-contract.schema.json`;
- all fixture manifests conform to `fixture-manifest.schema.json`;
- every JSON FEC fixture conforms to its dataset record or page schema;
- each manifest and contract digest matches its fixture bytes;
- both official summary file-format tables, the local `weball.csv` and
  `webl.csv` headers, and the shared record schema expose the same 30 fields in
  the same order;
- the processed Schedule A archive observation conforms to its schema and
  conserves its whole artifact, catalog counts, relation identities, range
  digest, and selected-extract measurements;
- exact 2026-08-23 archive DDL and the processed Schedule A record schema
  expose the same 81 fields in the same order;
- the compiled Go Schedule A schema reproduces all 81 archive names, types,
  nullability flags, and ordinals, and all eight exact COPY fixtures replay to
  their canonical JSON records;
- the 2025/2026 COPY extract conserves 166,293,056 rows and 114,190,988,781
  bytes under its exact digest, and the repository Go reader accepts every row
  while reproducing the uncompressed and compressed byte counts and digests
  with an 8,316 KiB peak resident set;
- all four target extracts conserve 891,016,965 rows and 620,474,534,709
  bytes, while their verified zstd copies total 58,987,591,718 bytes and
  reproduce every uncompressed digest;
- eight exact COPY fixtures each contain one LF-terminated 81-field row and
  replay exactly to their canonical record-schema fixtures;
- the processed Schedule B fixture pins the complete 39,310,353,867-byte
  artifact, SHA-256, PostgreSQL catalog, 26 inherited partitions, and all 81
  ordered fields with exact types and nullability;
- the compiled Go Schedule B schema matches that catalog, and the complete
  2024 relation conserves 157,544,163 unique rows, 123,284,784,602 COPY bytes,
  and its exact digest with zero invalid rows or duplicate `SUB_ID`s;
- every shared classic `pas2`/`oth` ID confirms Schedule B sender orientation,
  and every comparable amount is either exact or explained by whole-dollar
  classic truncation, with zero other amount conflicts;
- the processed Schedule A schema and both API-derived fixtures expose the
  same 81 fields;
- both targeted Schedule A API pages conform to their exact 81-field result
  and keyset-envelope schemas, with 23 unique original-filing rows and one
  conserved zero-result page;
- exact HDR fragments reproduce their claimed 8.3, 8.4, and 8.5 source-byte
  digests and select their declared versions;
- the electronic Schedule A layout has 45 continuous ordered fields matching
  the canonical row schema;
- every raw Schedule A row round-trips to its canonical field mapping, while
  the complete document fixture contains its contracted row at the exact byte
  range;
- the report-family fixture selects its unique amendment leaf, while the
  effective Schedule A fixture reproduces 2 added, 5 modified, 0 removed, and
  18 carried-forward logical rows from the exact parent documents; and
- the reconciliation fixture accounts for all 23 direct source rows and all
  25 effective rows, yielding 18 corroborated current rows and seven
  `raw_only_pending_processing` rows while processed evidence lags the raw
  amendment.

These are contract-shape checks. They do not convert a draft publisher
contract into accepted runtime behavior.

## Official references

- [FEC candidate master description](https://www.fec.gov/campaign-finance-data/candidate-master-file-description/)
- [FEC committee master description](https://www.fec.gov/campaign-finance-data/committee-master-file-description/)
- [FEC candidate-committee linkage description](https://www.fec.gov/campaign-finance-data/candidate-committee-linkage-file-description/)
- [FEC all-candidates summary description](https://www.fec.gov/campaign-finance-data/all-candidates-file-description/)
- [FEC current House and Senate campaigns description](https://www.fec.gov/campaign-finance-data/current-campaigns-house-and-senate-file-description/)
- [FEC processed schedules dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [OpenFEC itemized-data model](https://github.com/fecgov/openFEC/blob/develop/webservices/common/models/itemized.py)
- [OpenFEC electronic-filings API](https://api.open.fec.gov/v1/efile/filings/)
- [FEC filing-software and vendor resources](https://efilingapps.fec.gov/registration/softwarelogs.htm)
