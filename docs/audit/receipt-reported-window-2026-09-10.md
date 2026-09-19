# Receipt detail and reported-window gate — 2026-09-10

Status: bounded Go comparison and independent retained-source checks pass.
The [design](../design/receipt-reported-window.md) owns current behavior.

## Result

For the retained `C00843367` reported interval, April 1, 2023 through April 30,
2024, the non-memo itemized-individual occurrence subtotal equals both the bound
report-period sum and the source-aligned committee-summary assertion. This is a
comparison of reported evidence, not a finding about financial correctness.

| File | All Schedule A occurrences | Reviewed non-memo line occurrences | Exact subtotal (cents) |
|---|---:|---:|---:|
| 1714573 | 8 | 6 | 2,000,000 |
| 1766866 | 56 | 36 | 8,416,500 |
| 1743911 | 11 | 6 | 585,000 |
| 1780310 | 6 | 6 | 430,000 |
| 1780346 | 0 | 0 | 0, corroborated empty case |
| Total | 81 | 54 | 11,431,500 |

All five per-report differences and the window/summary differences are zero.
The other 27 occurrences remain explicit memo/other-line evidence; no raw row
is deleted. The empty report requires its complete original with no Schedule A
records and explicit bound zero cover/metadata amounts. Profile absence alone
does not provide that zero.

The full-cycle request, deliberately missing-cover request, and unresolved NRCC
cohort remain blocked. No new window total or summary-minus-detail difference
is emitted for those cases. Superseded evidence is preserved but not selected.
The original window and summary cash discrepancies, unknown prefix/suffix
activity, and all financial/terminal guards remain unchanged.

## Sources and ancestry

The source is the existing complete
[v2 profile](./receipt-report-profile-v2-2026-09-10.md), not a new transaction
scan or the older Parquet fact snapshot. Its 266,269,624-byte JSON artifact has
SHA-256 `acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647`
and profile ID
`a84e5ad04a315a2dfedc575ac44aecbba8c816b8265a76b2a9a69cc810b7dcc7`.
The command revalidates every retained group and its content identity, then
checks exact summary/release/selected-relation ancestry without rereading the
182.88 GB uncompressed transaction relation.

The [previous span gate](./cycle-prefix-2026-09-10.md) owns pinned report metadata,
document descriptors, and reported-span evidence. The
[original line gate](./receipt-report-lines-2026-09-10.md) owns complete small
per-report rows and original membership. Independent checks compare every old
row's grouped measures with the corresponding v4 groups; those older facts keep
their original release IDs and row ordinals. Matching groups do not prove v4
transaction-ID uniqueness.

Original sources are the pinned files linked in the document descriptors, such
as the [first report](https://docquery.fec.gov/dcdev/posted/1714573.fec) and
[termination report](https://docquery.fec.gov/dcdev/posted/1780346.fec).
The FEC [committee presentation](https://www.fec.gov/data/committee/C00843367/?cycle=2024)
and [summary dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/)
describe the reported span and separate itemized/unitemized fields; these are
not substituted for the immutable bulk and original-file observations.

## Verification and operational boundary

Full Go tests, vet, focused race checks, independent original-file/group/summary
checks, Ruff, and local documentation-link checks pass. Fixtures cover unknown
memo/amount, scope mismatch, duplicate occurrence preservation, source/version
and content tampering, missing/partial/nonzero empty-report evidence, exact
signed differences beyond int64, field conflicts, and unchanged prior evidence.
All four new outputs replay identically and all four prior v2 commands reproduce
their retained bytes. A changed profile pin or original-document pin fails
without emitting a result.

Each real comparison takes approximately six seconds in the capped offline
container. Timing and output sizes are retained in `timings.json`; this is a
manual bounded comparison, not a benchmark for recurring multi-committee use.

Evidence is retained at
`/storage/dumps/audits/fec/receipt-reported-window/2026-09-10/attempt-01/`, including
outputs, descriptors, explicit completion markers, drivers, test logs, focused
source/contract snapshots, and verified `SHA256SUMS`. Large retained inputs are
referenced by their existing identities, not copied again.

The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No source facts, transaction predicates, ArangoDB projections, or Dagster assets
changed. No API capture or bulk download ran. The only external reads were the
public FEC description and committee pages used to check documentation context.
