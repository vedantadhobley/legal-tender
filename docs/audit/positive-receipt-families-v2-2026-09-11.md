# Positive remaining receipt-family witnesses — 2026-09-11

Scope: test the [v2 receipt-family comparator](../design/receipt-family-comparison.md)
against real originals, the complete saved profile and real report metadata.
This gate adds fixtures/tests and records a shared header-binding limitation;
it does not change production calculations or relax report scope.

## Acquisition and provenance

The [fixture](./fixtures/positive-receipt-families-v2-2026-09-11.json) pins six
filings covering all seven form/category additions. Existing originals supplied
F3 candidate/other receipts, F3 offsets and F3X other receipts. The saved profile
ranked missing examples by total report rows, file number and committee; selection
did not require a matching cover amount or favorable comparison outcome.

Three small originals filled missing F3X shapes:
[loan repayments](https://docquery.fec.gov/dcdev/posted/1796827.fec),
[offsets](https://docquery.fec.gov/dcdev/posted/1678288.fec), and
[contribution refunds](https://docquery.fec.gov/dcdev/posted/1688894.fec).
The retained [F3 candidate/other-receipt original](https://docquery.fec.gov/dcdev/posted/1753173.fec)
lacked HTTP headers, so a recapture obtained transport evidence and verified its
body was byte-identical. Four captured bodies total 29,506 bytes. Each capture
had a 4 MiB body ceiling, timeout, headers, timestamps and an explicit exit marker.

The existing Go metadata client captured five selected committee populations
with the public demo credential: ten requests and 271,611 accounted response
bytes. Each capture reached an observed empty page within its three-request,
2 MiB budget. NRCC metadata was reused. No `.env` or private credential was read.
These are current API observations, not members of the earlier bulk snapshot.
Completing these bounded queries does not prove complete amendment history.

The source identities are unchanged from the
[v2 comparison gate](./receipt-families-v2-2026-09-11.md): profile SHA-256
`acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647`,
summary fact set
`603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637`,
and active source-pointer SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No bulk transaction body was rescanned or downloaded.

## Results

Every selected original Schedule A population matches its saved-profile groups,
with the two previously documented unknown processed amounts in filing 1730162
retained as exceptions. All new target lines have known positive amounts.

| Form / family | Filing | Original cover | Nonmemo detail | Metadata-bound comparison |
|---|---|---:|---:|---|
| F3 candidate contributions | 1753173 | $2,100.00 | $2,100.00 | Thresholded component; no total difference |
| F3 other receipts | 1753173 | $678.18 | $617.00 | Thresholded component; no total difference |
| F3 operating offsets | 1730162 | $2,754.42 | $2,754.42 | Header rule blocks binding |
| F3X other receipts | 1690269 | $503,536.15 | $483,585.28 | Thresholded component; no total difference |
| F3X loan repayments received | 1796827 | $2,500.00 | $2,500.00 | Header rule blocks binding |
| F3X operating offsets | 1678288 | $211.14 | $211.14 | Thresholded component; no total difference |
| F3X contribution refunds received | 1688894 | $4,500.00 | $4,500.00 | Header rule blocks binding |

Cover/detail agreement does not change the source-defined detail relationship.
Both equal-valued thresholded examples remain `component_not_comparable`.
No cover-minus-detail residual becomes unitemized money, cash or a donor amount.

Filing 1730162 retains eleven unreviewed SD10 records and two unknown processed
amounts on another line. Filing 1796827 retains one unreviewed SD9 record. Both
layout-completeness guards stay false. Debts and loan balances are not receipts.
Memo groups, negative amounts, out-of-cycle dates and all original bytes remain.

## Shared header-rule finding

The three blocked reports share the same source shape across F3/F3X and two
filing-software names: new-report suffix `N`, blank original-report reference,
and literal `0` in header sequence 7. Their metadata says unamended and supplies
a singleton chain containing the same file. Cover and metadata amounts agree.

The current generic binder requires the amendment-number field to be blank
for an unamended report. It emits `header_chain_identity_mismatch` for these
zero-numbered originals. This is a limitation of our accepted binding rule,
not evidence that their receipt amounts or report chains are wrong.

The pinned official 8.4 workbook describes sequence 6 as the original report ID
for amendments and sequence 7 as the sequential amendment number. It does not
explicitly settle zero versus blank for an original report. The source fixture
and independent tests preserve both the raw headers and the current blocker.
No file-ID, committee or software-name exemption was introduced. Review this
source shape as a general rule before qualifying these reports or widening
window totals; do not silently normalize `0` to blank.

## Verification and retained record

Durable attempt:
`/storage/dumps/audits/fec/positive-receipt-families-v2/2026-09-11/attempt-01/`.
The local workspace was `/tmp/legal-tender-family-v2-witnesses.FBIo5c`.
Retained drivers use read-only `/src` and `/storage`, plus a fresh `/audit`
workspace. The old Go source replay mounts its own prior directory at `/audit`
and writes logs under `/gate` to preserve the original recorded paths.

- Full Go tests, vet and affected-package race checks pass.
- The six new original/source results replay identically; all five previous
  source results also replay byte-identically through the shared test helper.
- The unchanged v2 command ran each of six real metadata/original cases twice;
  exact replay passes, taking about 13 seconds per pair including repeated
  full saved-profile validation. The three blockers are expected observations,
  not failed command execution or successful financial binding.
- All 28 enabled independent Python checks and Ruff pass. Checks cover raw
  bytes/headers, workbook positions, metadata capture hashes and raw records,
  exact profile groups, dates/memo signs, partial detail and header blockers.
- The application binary is unchanged:
  `58582cdaec99f6386fc2574bffdf268643b9eef991636f8bff92ecb38ce96cd2`.

The first old-source replay used a different mount path and correctly failed
byte identity. It was rerun with the original path; the initial log is retained.
No source data or runtime code changed to pass it. The checksum index, source
snapshot, metadata captures and explicit success markers are the execution record.
No graph, Dagster, source pointer, financial eligibility or terminal allocation
changed. Header qualification is next; new-family window/summary use remains open.
