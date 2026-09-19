# Receipt report-line membership — 2026-09-10

The [bounded Go reviewer](../design/receipt-report-lines.md) and independent
original-file check pass for seven retained Form 3 reports. All 168 Schedule A
occurrences match complete original transaction membership, receipt dates,
amounts, memo codes, form/line labels, entity labels, and back references.
The 88 non-memo `SA11AI` rows reproduce each original report-period line subtotal.
This is not a full-cycle financial comparison or a proof of terminal funding.

## Results

All amounts below are reported subtotals, not additional cash inferred from
memo records. A zero difference means agreement within this exact report scope.

| Original file | Coverage interval | All A rows | Non-memo 11AI rows | Original and detail subtotal | Difference |
|---|---|---:|---:|---:|---:|
| 1714573 | 2023-04-01–2023-06-30 | 8 | 6 | $20,000.00 | $0.00 |
| 1766866 | 2023-07-01–2023-09-30 | 56 | 36 | $84,165.00 | $0.00 |
| 1743911 | 2023-10-01–2023-12-31 | 11 | 6 | $5,850.00 | $0.00 |
| 1780310 | 2024-01-01–2024-03-31 | 6 | 6 | $4,300.00 | $0.00 |
| 1780346 | 2024-04-01–2024-04-30 | 0 | 0 | $0.00 | $0.00 |
| 1730369 | 2023-08-01–2023-09-30 | 13 | 2 | $2,250.00 | $0.00 |
| 1753173 | 2023-10-01–2023-12-31 | 74 | 32 | $29,200.00 | $0.00 |

The first five files belong to reported committee `C00843367`; the last two
belong to `C00849901`. These IDs select audit witnesses only. Runtime policy
contains no committee, candidate, file-number, or cycle-specific exception.

All 88 reviewed non-memo line receipts fall within their own cover intervals.
Nine **memo** records do not: one in file 1730369 is dated 2023-10-11, and eight
in file 1753173 are dated 2024-01-04 or 2024-01-23. Their dates match the retained
originals exactly, and all nine have publisher `is_individual=false`. They stay
in physical report membership and are excluded from the non-memo subtotal by
their memo code, not by date clipping. No explanation of those dates is inferred.

The zero-row report agrees with a zero original cover subtotal. The Go result
alone still carries `no_rows_in_snapshot`; it cannot infer the original zero.
Every transaction ID is present and unique within its selected report. This
does not establish a universal amendment selector or whole-cycle completeness.

## Pinned evidence and ancestry

Retained result directory:
`/storage/dumps/audits/fec/receipt-report-lines/2026-09-10/attempt-01/`.
It contains seven full report outputs, seven exact line reviews, job/check logs,
exit markers, and source-code/checksum evidence.

Original files are reused from the
[summary report investigation](./summary-report-review-2026-09-10.md) and the
[receipt association audit](./receipt-report-association-2026-09-08.md).
The former pins its originals and the official workbook in the
[source checksum list](./fixtures/summary-report-review-2026-09-10.sha256).
The latter pins both complete originals and compares their transaction evidence.
This turn downloaded no source and did not repeat a corpus-wide scan.

All originals in this gate have electronic header version 8.4, 93 cover fields,
and 45 Schedule A fields. The pinned official workbook maps Form 3 one-based
coverage fields 16/17 and report-period itemized-individual field 33. Separate
workbook checks map Form 3X fields 14/15/30, but **no original Form 3X report is
accepted by this gate**. Paper layouts and source-wide cross-version parser
acceptance remain separate.

Exact receipt ancestry:

- Inventory: `e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`.
- Fact set: `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Fact manifest SHA-256: `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829`.
- Receipt release: `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`.
- Release manifest SHA-256: `b921fda742759747b7e581c8897e8022ea5152eb33b86538ec80dbb9f13b5cad`.

This ancestry is **older than active v4**. None of these reviews claims to use
v4 facts or compares its subtotal with a fresh v4 cycle-summary amount.

## Verification and limits

The offline Go corpus job ran from 05:03:55 to 05:05:44 UTC. The test took
107.83 seconds, including one complete inventory/backing verification, seven
bounded report reads, and seven public-method replays. Exact replay passed.
The container had a 4 GiB memory cap, four CPUs, and a 2 GiB Go memory target.
Source storage was read-only; outputs were small audit artifacts.

Independent Python checks validate both wire contracts, review content identity,
source SHA-256, original layouts, all memberships, every grouped measure,
independent memo/individual axes, source dates, and exact decimal cover comparisons.
Those tests are audit-only, not another runtime pipeline. Regression fixtures
also cover source nulls/blanks, false/null individual flags, unsupported forms,
unknown memo codes/amounts, negative and zero money, identity issues, overflow,
invalid source metadata, empty reports, and cancellation.

Verification passed: `go test ./...`, `go vet ./...`, focused funding-basis/CLI
race tests, 12 independent filing/schema checks, and focused Ruff checks. The
Go corpus, independent test, lint, full-suite, vet, and race exit markers are zero.

The earlier [complete v4 profile](./receipt-report-profile-2026-09-10.md) remains
valid for its stated occurrence scope. It does not preserve independent memo
status in every predicate group or report groups for excluded rows. A future
cycle-wide comparison must capture that grain explicitly, without repeatedly
invoking this small-report reviewer. Effective report/account coverage and
cycle-summary compatibility still block a full-cycle numeric result.
