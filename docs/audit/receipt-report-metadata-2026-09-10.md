# Receipt report-metadata qualification — 2026-09-10

Status: bounded qualification review complete; no new recurring source, report
selector, or financial calculation is accepted. Bulk report-period summaries
exist, but the inspected headers lack filing IDs and amendment links. Processed
OpenFEC metadata supplies those assertions, including paper and zero-Schedule-A
reports, with endpoint disagreements and schema/type exceptions that must remain
explicit. It is a proposed metadata source, not a transaction fallback.

## Bulk options and limits

The [FEC catalog](https://www.fec.gov/data/browse-data/) links the following
products. The retained response bodies, headers, listings, and selected schema
members are bound by the [digest fixture](./fixtures/receipt-report-metadata-2026-09-10.sha256).

| Inspected source | Useful evidence | Missing or unqualified |
|---|---|---|
| 2024 House/Senate, PAC, and Party report-summary CSV prefixes | Form, committee, report type/year, receipt date, coverage dates, and period financial columns. | No file number, amendment indicator, or amendment chain. Only headers and first rows checked, not complete bodies. |
| Processed schedule-dump directory | A/B/E and committee-history products. | No report-history relation product in this complete directory listing. |
| Daily electronic and paper archive listings | Dated object availability; downloadable originals and format packages. | Not a ready-made processed report-history table; complete original acquisition and history closure are unqualified. |
| OpenFEC processed filings and report endpoints | File IDs, committee/form/period, paper/electronic origin, and publisher status/link assertions. | No reliable financial-replacement flag or complete reporting-scope proof; endpoints are not interchangeable. |

The three CSV captures are HTTP 206 ranges of exactly 8,192 bytes each. Their
98-column headers each repeat `CAND_ID` twice: a future parser must preserve
column ordinals. In the inspected first row, `LINK_IMAGE` points to a legacy
committee page, not a filing. Matching committee, dates, and amounts cannot
invent an exact file ID. The full object sizes were 15,480,355, 43,929,141, and
4,508,013 bytes respectively; the full files were not downloaded.

The [report-summary dictionary](https://www.fec.gov/campaign-finance-data/committee-report-summary-file-metadata/)
describes period summaries back to 2008, nightly updates through the previous
day's 7 pm processing cutoff, and incorporation of amendments. It does not name
the cutoff timezone or promise an atomic snapshot with the weekly schedules.
These are useful separate period observations, not preserved version history.

The complete inspected listings include `bulk-downloads/`, `2024/`,
`data-dump/`, `data-dump/schedules/`, `data_dictionaries/`, `historical-reports/`
and its two children, `data.fec.gov/`, `fecviewer/`, and `data_requests/`.
No suitable standalone processed report-chain bulk product was identified in
this catalog and these prefixes. This is not proof that no other FEC export
exists. The historical-report prefix holds narrow older party documents, not
the required general modern filing history.

General paper/electronic listings are truncated at 1,000 entries. The June and
September 2026 subprefix listings are complete. June's paper listing has no
June 8 object; September 1–9 has nine one-byte `.nofiles.zip` paper objects.
An absent object and an explicit publisher marker are different observations.
Neither establishes that no report was required or filed. No daily archive
payload was fetched. The [electronic RSS window](https://efilingapps.fec.gov/rss/RSSHelp.html)
is seven days and electronic-only, not historical or paper coverage.

## Bounded processed-metadata comparison

The [captured OpenFEC schema](https://api.open.fec.gov/swagger/) has SHA-256
`0cca7f2af73270b35d7276e53d99b56b2f8f9970b33cb5d36a85fdb8b773caed`.
Three public-demo requests captured seven exact `/v1/filings/` witnesses,
ten `/v1/reports/house-senate/` results for `C00843367` in cycle 2024, and
34 `/v1/reports/pac-party/` results for `C00075820` in cycle 2024. Each returned
one complete page with an exact count. This is bounded endpoint qualification,
not target-population completeness. No private token was read or used.

The tests bind metadata to the previously retained
[original-report witnesses](./summary-report-review-2026-09-10.md):

- Electronic `1766839` and amendment `1780310` have matching original cover
  dates and an original-file reference consistent with the reported chain.
- Termination `1780346` has no Schedule A rows but is present in both metadata
  endpoints. Its original itemized-individual cover value and API value are zero.
- Paper `1876290`, `1882886`, and `1813890` match declared cover dates and
  beginning-image references under the official P3.4 field layout.
- The prior visual review identifies `1882886` as an attachment, not a complete
  replacement financial report. Its transcribed financial cover slots are blank;
  its processed report endpoint returns zero in the checked cover fields.

Publisher assertions for the same files differ across endpoints:

| File | Endpoint | `is_amended` | `most_recent` | `previous_file_number` |
|---|---|---|---|---|
| 1833804 | filings | false | false | 1833804 |
| 1833804 | reports/pac-party | true | true | 1833804.0 |
| 1882886 | filings | null | true | -1147523 |
| 1882886 | reports/pac-party | false | null | null |

These are endpoint-scoped source assertions, not a reason to select whichever
flag makes a total agree. Their different semantics and attachment handling need
an explicit selection contract. The new PAC/Party response is byte-identical to
the earlier retained report response; this comparison introduces no source repair.

Other representation details are material:

- Filings uses integer amendment-chain entries; the checked report response
  uses string entries even though its schema advertises numeric items.
- The zero-itemization House/Senate value is the JSON string `"0.00"`, despite
  a numeric schema. The checked PAC/Party zeros are JSON numbers. Independent
  comparisons use exact decimals and keep the original JSON representation.
- Paper chains can be null. Negative predecessor values stay signed publisher
  references, not absolute-valued file URLs. Root self-references are not enough
  to assert an amendment cycle. A chain to one amendment need not list successors.
- Filings coverage values are dates; report coverage values include midnight
  time text. Matching calendar dates does not assign a timezone.

## Refresh and scope requirements

The pinned filings contract offers receipt-date, cycle, report-year, and exact
file filters. It exposes `update_date` but no update-date range filter. The
[FEC's endpoint development notes](https://github.com/fecgov/openFEC/wiki/Create-an-endpoint)
describe nightly materialized-view refreshes. Captured API headers also permit
one-hour caching. Neither provides a stable cross-endpoint snapshot guarantee.

Design consequence: polling only new receipt dates cannot prove that status
assertions on older reports remain current. A future metadata capture needs
bounded family/scope refresh, history reconciliation, exact response ancestry,
and explicit incomplete capture states. Amendments received after the campaign
cycle must remain reachable. Large filing-query counts can be approximate;
the schema instructs clients to continue until an empty page. Pagination alone
does not prove a consistent snapshot while source results change.

Reporting scope means the form and financial-field population under comparison,
including federal/nonfederal distinctions where applicable. It does **not** mean
requiring a private bank-account number. Neither inspected endpoint supplies a
`financial_cover_present` assertion; zero amounts do not establish that a cover
was present. Originals remain a bounded evidence source for unresolved cases.

## Official paper schema follow-up

The catalog's format links are schema bundles, not filing metadata tables.
Only the final 65,536 bytes of each ZIP and two selected compressed members were
captured. Checks bind HTTP 206 ranges, ETags, central-directory metadata, local
member names, uncompressed sizes, CRCs, and decompressor completion. Neither
complete format archive nor a daily filing archive was downloaded.

The extracted `Paper_v3x/PAPER_Format_V3.4.xlsx` is 385,492 bytes with SHA-256
`f9637d641d581d169cecc42cfb46c08e5a073f02803df8159e3ff40e3860f259`.
Its HDR original-ID field is marked unused. Its Schedule A field 22 documents
`X = True`, not `Y`. This now supplies the paper-specific schema missing from
the [memo review](./receipt-memo-review-2026-09-10.md). The observed `Y` values,
checked memo boxes, and reported cover amounts still disagree in meaning; the
schema does not authorize converting or counting them differently.

## Verification and disposition

The [independent test](../../tests/test_receipt_report_metadata.py) passes all
seven checks against pinned sources and retained originals. Ruff passes. The
combined audit run passes nine checks; 21 opt-in cases from the earlier memo and
report-line audits are skipped because those separate corpora are not requested.
Tests run with no network, a 1 GiB container
cap, and two CPUs. No complete Schedule A scan or new original-file download
was needed. There is no runtime Python, Go source change, service, Dagster
activation, graph update, or financial-policy change in this step.

Evidence is retained under
`dumps/audits/fec/receipt-report-metadata/2026-09-10/attempt-02/` in project
storage: captured bodies/headers, source-selection map, schema members, test and
documentation snapshots, logs, final success markers, and verified tree digests.
The first test run exposed the unhandled monetary string; its failure log is
retained beside the corrected exact-type check and passing final result.
Retention attempt 01 stopped on a nonexistent snapshot-fixture path and remains
marked failed. Attempt 02 validates snapshot paths first and is the verified copy.
The active source-pointer digest remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

Next decision: accept a **separate processed report-metadata source** alongside
bulk-only transaction ingestion, or retain bulk-only acquisition and defer the
comparisons that need unavailable history. If accepted, first define a small Go
capture/assertion contract using these fixtures; do not start full history
acquisition or automatic financial selection. Full population closure, costs,
freshness reconciliation, and financial replacement remain unproved.
