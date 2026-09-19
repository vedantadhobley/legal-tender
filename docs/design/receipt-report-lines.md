# Bounded receipt report-line review

Status: implemented manual Go diagnostic. The
[seven-report original-file gate](../audit/receipt-report-lines-2026-09-10.md)
passes for retained Form 3 filings. This is a report-membership test, not a
cycle-wide summary reconciliation or terminal-funding calculation.

## Membership contract

The initial reviewed scope is processed `F3` or `F3X`, schedule `SA`, line
`11AI`. The FEC's [Form 3 instructions](https://www.fec.gov/pdf/forms/fecfrm3i.pdf)
and [Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf) distinguish
this itemized contribution line from other receipt lines and from unitemized
amounts. The comparison target is the corresponding **report-period** cover
subtotal, not cumulative values, total receipts, or candidate-level summaries.

Each selected report row retains independent axes:

- Exact filing form, schedule, and line, including source null versus blank.
- Raw memo code: `X` is a memo subtotal; blank/null is non-memo; another
  nonblank value leaves reviewed-line membership unresolved.
- Publisher `is_individual`: true, false, or source null. This is diagnostic
  evidence, **not** a filter for this form-line population.
- Exact known/unknown amount state, signed subtotal, sign counts, and source
  row ordinals. Unsupported forms/lines remain outside the reviewed scope;
  they are not deleted or treated as zero.

`fec/reported-itemized-line-membership@1.0.0` groups these axes and conserves
every selected occurrence. A reviewed non-memo line subtotal includes all known
signed amounts on the selected form-line, regardless of the individual flag.
Unknown amounts and unrecognized memo codes remain separate blocking groups.
No source classification, accepted receipt predicate, or original value changes.

The [earlier occurrence profile](./receipt-report-profile.md) does not contain
this entire comparison grain. It records report groups only for the included
individual predicate. Its `excluded_non_individual` decision can also contain
memo rows because that predicate tests the individual flag first. It cannot
reconstruct independent memo status or complete per-report line membership.
Do not reinterpret its excluded groups as non-memo money.

## Verified, bounded reader

```bash
legal-tender pipeline fec review-funding-report-lines \
  --storage-root /storage \
  --basis-result <verified-receipt-inventory-json> \
  --committee <exact-committee-id> --file-number <positive-file-number>
```

The reader verifies the inventory and its complete immutable backing once,
then reuses the existing exhaustive bounded report reader. It reads all selected
committee/file rows, not a page or the included-individual subset. It fails
above 10,000 rows instead of returning truncated success. This manual command
is not a query plan to repeat across every report in recurring processing.

Rows must remain in source-ordinal order and match the verified cycle,
committee, file, typed metadata, and raw memo normalization. Missing source
columns, wrong physical types, overflow, backing corruption, and cancellation
fail before emitting JSON. Blank/null/repeated transaction IDs remain explicit
report-local identity issues; no duplicate amount is silently discarded.

The [wire schema](../../contracts/calculations/fec/committee-funding-basis/v1/report-lines.schema.json)
binds the exact inventory, fact manifest, and original receipt-source release.
The review ID hashes compact Go JSON with `review_id` empty. Exact grouped
source ordinals support full-row lookup in that pinned fact snapshot. Ordering
and replay are deterministic; source ancestry affects content identity.

## What this result does not establish

Runtime `comparison_ready` and `terminal_attribution_eligible` are always false.
Original-filing membership, report/account coverage, effective-report selection,
and cycle-summary compatibility remain explicit blockers. The command does not
ingest original filings, fetch an API, select amendments, or calculate a
summary-minus-detail difference. An empty snapshot report is not proof of a
zero report subtotal without separate original-source evidence.

The independent audit checks complete transaction membership against small,
hash-pinned original filings, then compares the original report's line subtotal.
Its cover offsets are test witnesses only, scoped to exact source hashes and
observed electronic-file layouts; they are not a general original-filing parser.
F3X has mapping/fixture checks but no electronic original-file gate here. The
later [memo review](../audit/receipt-memo-review-2026-09-10.md) checks complete
bounded F3/F3X paper-transcription membership separately and preserves conflicting
memo/cover evidence; it does not turn those reports into accepted line totals.

The real audit uses the older accepted Schedule A facts. It does not relabel
them as active v4 facts or establish compatibility with the fresh v4 cycle CSV.
Receipt dates outside a report period remain source evidence; dates do not
define physical report membership. Memo exclusion and date clipping are not
interchangeable operations.

## Next boundary

The [v2 cycle profile](./receipt-report-profile-v2.md) now preserves these
independent axes and all selected-cycle report-line populations in one bounded
pass; its complete 2024 gate and exact v1 regrouping pass. The separate
[receipt/window comparison](./receipt-reported-window.md) now qualifies a narrow
reported occurrence diagnostic, not unique/effective financial membership.
The [family source map](./receipt-families.md) defines remaining F3/F3X categories
for the next additive comparison. Financial report/account acceptance remains
required before funding use. Preserve source conflicts and unexplained
differences; do not infer unitemized receipts from a residual.

No source download, source/fact pointer, graph, API, Dagster asset, schedule, or
resident service changes. Python remains independent audit/schema test code;
all runtime membership and grouping stays in Go.
