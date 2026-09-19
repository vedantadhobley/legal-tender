# Complete F3X receipt-family witness — 2026-09-11

Result: the unchanged [Go family comparator](../design/receipt-family-comparison.md)
passes a complete real F3X filing with positive other-committee contributions
and affiliated/party transfers. No runtime counting rule, memory limit,
publication, graph or financial eligibility changed.

## Selection and acquisition

The retained-original inventory found small complete F3 originals, a partial
NRCC F3X report, and complete F3X filing 1708331 at 4,826,663 bytes. The latter
exceeds the existing 4 MiB document-reader cap. The retained NRCC report metadata
already supplies other candidates. The saved profile's Schedule A counts rule
out the larger multi-family examples without fetching them: filings 1841335
and 1857946 alone have more Schedule A rows than the 4,096-record document cap.

Among the retained metadata candidates with positive contribution and transfer
families, filing 1690269 has the fewest profiled Schedule A rows. It was selected
as a bounded diagnostic witness, not a representative political sample or a
named runtime exception. The inventory and source selection remain audit evidence.

One unauthenticated HTTPS request captured the
[complete original filing](https://docquery.fec.gov/dcdev/posted/1690269.fec),
with a 4 MiB maximum, a 45-second timeout, no redirects and no retries.
The body is 824,322 bytes; headers add 1,127 bytes. The existing Go reader verifies
the HTTP completeness evidence, all 3,321 framed records, exact 8.4 header,
123-field F3X cover, committee, report type and period. No budget was increased.

The [source digest fixture](./fixtures/receipt-family-witnesses-2026-09-11.sha256)
pins both artifacts. The existing metadata capture remains a separate snapshot;
it is not relabeled as part of the coordinated bulk release. No new API call,
credential access, bulk download or transaction-source scan occurred.

## Qualified report comparisons

The exact report is NRCC, committee `C00075820`, file `1690269`, Form 3X,
monthly report M2, January 1–31, 2023. It belongs to the selected 2024 two-year
partition. Retained metadata supplies an explicit singleton electronic chain;
the complete original's identity and period agree. This is an observed source
chain candidate, not universal proof of legally effective financial membership.

| Family | Nonmemo rows | Original cover / metadata / processed occurrence subtotal |
|---|---:|---:|
| Other committee contributions, SA11C | 67 | $1,731,000.00 |
| Affiliated/party transfers, SA12 | 18 | $548,487.72 |

Both exact report-minus-detail differences are zero. Party contributions and
loan receipts bind explicit reported zeroes, but have no family detail; they
retain null detail and differences with `no_nonmemo_family_detail`. The new
witness does not generalize the itemized empty-report zero policy.

All seven existing cover/metadata fields also bind for this report. The existing
summary comparator accepts the opening boundary only; six other field comparisons
remain blocked by the summary's different ending date. A complete January report
does not establish a complete two-year financial window.

## Complete original and profile grain

Independent tests verify all 2,595 original Schedule A rows, not only the 85
nonmemo rows in the two successful family comparisons. Every row has the expected
45-field layout and filer. Schedule A field positions and F3X cover labels are
checked against the pinned official 8.4 workbook.

The original rows match the complete saved profile's exact ten report groups
after regrouping only the dimensions the original supports: form/line and raw
memo convention. Counts, known/unknown amounts, exact signed/positive/negative
measures, and all receipt-date states/extrema agree. The independently loaded
profile is SHA-256 pinned and exactly matches the groups returned by Go.

This source exercises important preservation boundaries:

- SA12 contains 61 additional `X` memo occurrences. Their $660,650.00 is retained
  as memo evidence, not added to the nonmemo transfer subtotal.
- Of those transfer memo occurrences, 56 predate the selected cycle. Across the
  report, 62 Schedule A occurrences predate it. Receipt dates reach December 2021;
  none is clipped out to reproduce a January 2023 cover.
- One negative occurrence, −$469.07, remains in the signed evidence.
- Other receipt families and independent individual-classification axes remain
  in the saved groups; they are not discarded by the initial runtime subset.

The complete original has unique nonempty transaction IDs. The grouped profile
does not expose per-occurrence transaction IDs, so this does **not** prove v4
transaction identity or one-to-one all-field equivalence. The original-only
identity check, grouped equivalence, and financial membership remain separate.

## Verification and retention

The new [opt-in Go regression](../../internal/calculation/fec/fundingbasis/receipt_family_witness_test.go)
revalidates all inputs and reproduces the retained output byte for byte. It
passes in 5.96 seconds. The initial unchanged-command run and replay together
took 12.81 seconds. Its diagnostic output is 4,777,930 bytes because it preserves
both verified field projections; this is not the recurring publication format.

Full Go tests, vet, and focused race checks pass. Fifteen independent tests pass,
including [complete-original checks](../../tests/test_receipt_family_witnesses.py)
and the existing field-binding audit. Ruff and changed-document link checks pass.
Python changes are test-only; the workbook helper's optional sheet selection
preserves its existing default behavior. Runtime Go and the family policy are
unchanged. There are no source-data repairs or named production exceptions.

Evidence lives under
`/storage/dumps/audits/fec/receipt-family-witnesses/2026-09-11/attempt-01/`:
the small new original/headers, capture metadata, inventory, descriptor, comparison,
verification logs/drivers, and focused source/test/docs snapshots. No profile,
bulk archive or executable is duplicated. `SHA256SUMS` and explicit exit markers
record verification and retention completion.

The active source pointer remains SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Financial, family-window, cash and terminal-allocation guards remain unchanged.

## Next boundary

Qualify family-specific absent-detail evidence and field/window coverage before
new window or summary differences. This witness supplies complete original
evidence for empty F3X party/loan lines without yet turning them into detail
zeroes. Positive F3 authorized transfers, party contributions, other loan shapes,
remaining receipt families and F3P still require their own source witnesses.
Do not expand this small audit into a universal financial acceptance claim.
