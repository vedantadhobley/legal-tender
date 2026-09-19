# Committee financial-summary source review — 2026-09-08

Result: select the official committee-summary CSV for the next committee-cycle
summary fact family. The [contract remains draft](../../contracts/sources/fec/committee-summary/v1/)
until the Go parser and publication gates pass. This review adds no financial
facts, release members, graph edges, or Dagster assets.

## Captured evidence

Read the [official bulk catalog](https://www.fec.gov/data/browse-data/?tab=bulk-data),
[committee-summary dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/),
and [classic PAC-summary dictionary](https://www.fec.gov/campaign-finance-data/pac-and-party-summary-file-description/).
After checking sizes, fetched only the four small committee-summary CSVs, each
with a 16 MiB ceiling and 60-second timeout. Total CSV bytes: 31,118,056.
No Schedule A/B/E archive or API data was fetched.

| Cycle | Complete CSV bytes | Source rows | Distinct committee IDs | Repeated committee groups | Extra rows beyond committee IDs |
|---|---:|---:|---:|---:|---:|
| 2020 | 7,550,294 | 13,554 | 13,535 | 18 | 19 |
| 2022 | 7,798,792 | 13,977 | 13,946 | 31 | 31 |
| 2024 | 7,861,711 | 14,065 | 13,994 | 65 | 71 |
| 2026 | 7,907,259 | 14,152 | 14,079 | 70 | 73 |

Every file has 92 columns, LF record endings, a final LF, no embedded newlines,
and ASCII-compatible bytes. All cycle labels match the requested partitions.
All committee IDs match `C` plus eight digits. These are observed corpus results,
not justification for ignoring future decoding, framing, or identity issues.

The shared exact header SHA-256, including LF, is
`f8e539448b8271b6fc975f0982b5426cf2d1dc08425d0685fdf8f8f7b31de0b9`.
Whole-file hashes, final HTTP object versions, modification times, field profiles,
and fixture provenance are in the [machine-readable review](../../contracts/sources/fec/committee-summary/v1/review.json).
The publisher's Last-Modified values are September 8, 2026, 10:13:31–35 UTC;
they are snapshot metadata, not the coverage dates of all rows.

The retained source directory is
`/storage/dumps/audits/fec/committee-summary-source/2026-09-08/`.
It contains the four exact CSVs, their redirect/final response headers, both
dictionary pages, and the public dump-prefix XML inventory. No prior artifact
was overwritten. The research copies are not members of the existing release.

The [audited public dump prefix](https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/?list-type=2&prefix=bulk-downloads/data-dump/&max-keys=1000)
returned an untruncated inventory containing the README, Schedule A/B/E, and
committee history. No report-summary dump appeared in that prefix. This is not
a claim that no other report-level product could exist elsewhere.

## Physical and identity findings

The [source design](../design/committee-summary-source.md#physical-boundary)
records three header/dictionary spelling differences and the actual `YYYYMMDD`
date encoding. Pin the source header rather than trusting the dictionary's
names or date display format.

All nonblank values across the 75 monetary columns parse as signed decimal cents,
including leading-decimal lexemes. Raw field strings remain authoritative
evidence. Blanks do not become zero, and negative amounts are not clamped.

Every repeated committee group in all four snapshots differs only in `CAND_ID`.
The other 91 columns are identical within each group. There are no duplicate
`(CMTE_ID, FEC_ELECTION_YR, CAND_ID)` composites in these snapshots. This supports
candidate-reference fan-out as an observed shape, not a globally guaranteed
key or permission to discard source records. Summing each occurrence would
repeat financial amounts for those committees.

## Date and completeness findings

| Cycle | Invalid start dates | Reversed valid intervals | Valid intervals starting before cycle | Valid intervals ending after cycle | Blank opening cash | Blank unitemized individual subtotal |
|---|---:|---:|---:|---:|---:|---:|
| 2020 | 9 | 1 | 111 | 35 | 417 | 474 |
| 2022 | 7 | 0 | 179 | 13 | 245 | 278 |
| 2024 | 3 | 1 | 100 | 10 | 291 | 327 |
| 2026 | 1 | 0 | 99 | 0 | 81 | 122 |

All invalid calendar values observed are `CVG_START_DT=99999999`. Its intended
publisher meaning remains unverified. The counts above retain source-row grain,
including candidate-reference repetitions; they are not unique statement counts.
Intervals with an invalid endpoint are excluded only from interval comparisons,
not from source preservation. Both date fields are nonblank in every reviewed row.

The exact fixture preserves a blank statement, leading-decimal receipt, negative
unitemized subtotal, invalid date, repeated committee pair, reversed interval,
and ordinary committee row. Every selected record matches its original artifact
ordinal and raw byte hash.

## Diagnostic arithmetic, not repaired financial statements

| Cycle | Cash equation equal / different / missing operand | Individual subtotal equation equal / different / missing operand |
|---|---|---|
| 2020 | 11,594 / 1,500 / 460 | 12,817 / 238 / 499 |
| 2022 | 12,497 / 1,209 / 271 | 13,542 / 146 / 289 |
| 2024 | 12,721 / 1,040 / 304 | 13,557 / 166 / 342 |
| 2026 | 13,185 / 870 / 97 | 13,957 / 64 / 131 |

Cash comparison: `COH_BOP + TTL_RECEIPTS - TTL_DISB = COH_COP`.
Individual comparison: `INDV_ITEM_CONTB + INDV_UNITEM_CONTB = INDV_CONTB`.
Each triple conserves that cycle's source rows. A difference does not establish
its cause or authorize a source correction. These comparisons do not prove a
cash denominator, valid account scope, or agreement with Schedule A/B/E.

## Verification and next gate

The contract/schema tests and independent complete-snapshot tests pass: 22 tests.
The corpus gate verifies whole-byte identity, header order, all row widths,
exact money, date issues, repeated-key differences, summary equations, and
all fixture record locators. Python is used only for independent evidence
tests; there is no Python runtime ingestion path.
The broader rewrite contract/orchestration regression also passes: 79 passed,
10 unrelated opt-in corpus checks skipped, and 60 existing Dagster warnings.
That run repeated all four new corpus checks against the durable read-only
copies and verified the retained HTTP metadata and dictionary/inventory hashes.

Run the complete gate in the existing test image with the retained directory
mounted read-only and `LT_COMMITTEE_SUMMARY_REVIEW` pointing at that mount:

```bash
python -m pytest -q -p no:cacheprovider \
  tests/test_source_contracts.py tests/test_committee_summary_source.py
```

Without that environment variable, the four complete-corpus tests skip and the
fixture/contract checks still run. The next implementation is a strict Go reader
and issue-preserving verifier, followed by explicit source-publication acceptance.
Committee-level grouping, source-qualified reconciliation, report-time coverage,
and terminal attribution remain separate work.
