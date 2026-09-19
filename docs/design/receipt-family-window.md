# Receipt-family date-window comparisons

Go's `compare-receipt-family-window` aggregates each accepted receipt family
over an exact requested date window. It uses the
[per-report comparisons](./receipt-family-comparison.md) and separate
[reported-zero qualifications](./receipt-family-absence.md), without replacing
either output. The [versioned policy](../../contracts/calculations/fec/receipt-family-window/v1/policy.json)
defines a reported-observation comparison, not a financial funding total.

## Inputs and membership

```sh
legal-tender pipeline fec compare-receipt-family-window \
  --storage-root /storage --summary-facts /path/to/exact-summary-manifest.json \
  --capture /path/to/retained-report-capture.json \
  --documents /path/to/pinned-documents.json \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --profile /path/to/complete-profile-v2.json --profile-sha256 EXPECTED_SHA256
```

The existing absence boundary re-verifies original bodies/headers, metadata,
source-chain scope, summary backing and the pinned complete profile. The command
accepts no saved readiness decisions or caller-selected report membership.
`reviewed` embeds the unchanged absence result, including all prior comparisons,
originals, groups and unresolved evidence. Existing input budgets are unchanged.

The exact metadata endpoint selects the reviewed F3 or F3X family set even when
no original documents were supplied. Each family has a member entry for every
intersecting observed chain candidate. Missing documents retain their observation
reference and blockers; they cannot disappear by iterating only supplied covers.
Superseded and unresolved alternatives remain in `reviewed`, never extra operands.
No candidate outside the requested window contributes to its sums.

Dates are inclusive report coverage dates, not receipt dates or hardcoded cycle
boundaries. A report crossing the requested boundary remains blocked; its amount
is neither clipped nor prorated. The shared report-period coverage sweep is reused.
No second date engine or new amendment selector is introduced.

## Two independently scoped value sets

For each exact form/family, the output distinguishes:

| Value set | Accepted operand | Missing evidence |
|---|---|---|
| Reported | Whole-period cover amount bound exactly to metadata within qualified report scope | Null value plus field/scope blocker |
| Comparison | Qualified nonempty nonmemo occurrence subtotal, or the separate qualified reported-zero observation | Null value plus comparison/absence blocker |

Each member identifies its report and family indexes in `reviewed`, operand
basis, exact signed values, and reported-minus-comparison difference. Equal and
unequal nonempty report comparisons are both eligible operands. A numeric
disagreement is not missing evidence, and it is not removed to force a match.

`qualified_reported_zero_without_occurrences` remains distinct from
`nonmemo_occurrence_subtotal`. Using a qualified zero in this new window does not
fill the old report's null detail or synthesize a receipt row. Thresholded fields,
cover-only categories, other schedules and unsupported forms stay outside the
accepted family subset. Loans retain their existing source roles; these values
do not establish who supplied principal or whether cash was available.

## Coverage and arithmetic

Each value set has its own calendar segments, covered/gap/overlap days, explicit
member values and blockers. A full-window value requires:

1. The existing observed-chain partition to qualify, including metadata traversal,
   unresolved cohorts, gaps, overlaps and cross-boundary checks.
2. Every intersecting candidate to have the relevant operand.
3. The operand periods to cover every requested day exactly once.

Reported totals can therefore qualify while comparison totals remain blocked.
One family's missing field does not invalidate a sibling family. Unresolved
report scope or partition failures still block every affected window.

Observed sums remain available when some operands qualify. They use arbitrary-
precision signed integer cents. No operands means null; explicit zero operands
can yield zero. These sums are not full-window values or lower bounds. Their
member sets can differ, so no difference is calculated between partial sums.

Only two qualified full-window values produce `delta_minor_units`, calculated as
reported minus comparison. Member discrepancies remain visible even if positive
and negative differences cancel at window level. Duplicate numerical amounts in
distinct report periods are not deduplicated. No family is added to a sibling or
nested cover total, and no cross-form combined total is produced.

## Limits and next gate

`reported_window_ready` and `comparison_window_ready` describe each new family
window only. Closed readiness flags in the unchanged nested v1 results retain
their original meaning. `financial_use_eligible` and
`terminal_attribution_eligible` remain false at the new top level.

The [retained gate](../audit/receipt-family-window-2026-09-11.md) verifies exact
membership, source operands, every calendar day, blocked alternatives, and old-
command replay. This is a bounded diagnostic over retained evidence, not a
scalable recurring storage format. It makes no network request, bulk scan,
publication, ArangoDB write or Dagster change.

The separate [family-summary comparator](./receipt-family-summary.md) now applies
qualified field meaning and date scope to matching committee-summary fields.
Existing seven-field summary comparisons remain unchanged; this window command
itself introduces no new summary difference. Remaining
receipt categories, broader formats, financial membership, cash
continuity and terminal attribution remain separate work.
