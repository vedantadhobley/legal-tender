# Receipt-family report comparisons

The additive Go `compare-receipt-families` command compares receipt categories
one report at a time. The default v1
[policy](../../contracts/calculations/fec/receipt-family-comparison/v1/policy.json)
uses the [reviewed family map](./receipt-families.md); it does not select
financial transactions or expand the existing itemized-window policy.
The opt-in [v2 policy](../../contracts/calculations/fec/receipt-family-comparison/v2/policy.json)
adds the remaining non-individual Schedule A leaves and distinguishes thresholded
detail components from fields that require itemization regardless of amount.

## Inputs and verification

Use the same arguments as [the itemized-window comparison](./receipt-reported-window.md):

```sh
legal-tender pipeline fec compare-receipt-families \
  --storage-root /storage --summary-facts /path/to/exact-summary-manifest.json \
  --capture /path/to/retained-report-capture.json \
  --documents /path/to/pinned-documents.json \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --profile /path/to/complete-profile-v2.json --profile-sha256 EXPECTED_SHA256
```

The command revalidates the original bodies, headers, metadata traversal,
observed source chains, document budgets, and summary backing. Its second field
projection must see the same metadata, descriptors and original identities.
The complete saved profile must pass its digest, every group/conservation check,
and exact summary/release ancestry checks. No source-body rescan or network
request occurs. This is a bounded diagnostic, not a per-committee production
execution strategy.

`reported` preserves the prior seven-field summary/window result. `family_reports`
contains the separately versioned field bindings; it does not assert family
window readiness. Every supplied original remains visible, including superseded
or incomplete originals. Missing documents remain visible in the existing
membership/window evidence; they cannot be bypassed to obtain a new family total.

## V1 field subset

| Form | Families | Exact Schedule A references |
|---|---|---|
| F3 | Party contributions; other committee contributions; authorized transfers; candidate-made or guaranteed loans; other loans | 11B, 11C, 12, 13A, 13B |
| F3X | Party contributions; other committee contributions; affiliated/party transfers; loans received | 11B, 11C, 12, 13 |

Source tests bind every position to the reviewed workbook map and every metadata
field to the pinned endpoint schema. Exact cover and numeric metadata values
must agree within the qualified filer/form/report/period/observed-chain scope.
Missing, blank, invalid and conflicting values retain separate blockers. A
field-level disagreement does not invalidate an unrelated field.

The metadata name `loans_made_by_candidate_period` is preserved, but the F3
family remains **candidate-made or guaranteed loans**. This field does not
identify who supplied loan principal. No candidate-specific runtime exceptions
or runtime remote-schema discovery exists.

## V2: remaining receipt categories

Add `--comparison-version v2` to the same command. Only this report-level
command accepts the flag. Existing absence, window and summary commands retain
their v1 policies and byte outputs; the v1 default also remains unchanged.
Both versions use the same profile verifier, source-scope binder, exact routing
and comparison engine. No committee, candidate, file number, year or observed
amount chooses a field's relationship.

V2 includes every v1 field plus these reviewed form-specific leaves:

| Form | Family | Schedule A line | Detail relationship |
|---|---|---|---|
| F3 | Candidate contributions | 11D | Thresholded component |
| F3 | Operating offsets / other receipts | 14 / 15 | Thresholded component |
| F3X | Loan repayments received | 14 | Required itemization |
| F3X | Operating offsets | 15 | Thresholded component |
| F3X | Contribution refunds received | 16 | Required itemization |
| F3X | Other federal receipts | 17 | Thresholded component |

Each output field carries its `detail_relation`. For `all_required_itemized`,
qualified nonempty detail can produce `equal` or `different`, as in v1. For
`thresholded_component_of_total`, qualified detail and the reported total remain
separate operands with state `component_not_comparable` and **null difference**.
Even equal numbers do not establish equal populations. Unknown amounts or scope
failures stay `blocked`; known occurrence measures and all raw groups remain.
Missing detail stays null even with a bound zero. No residual unitemized amount,
lower bound, donor, refund origin or cash amount is inferred.

The mappings follow the already pinned [F3 instructions](https://www.fec.gov/pdf/forms/fecfrm3i.pdf)
and [F3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf). The runtime
does not implement a new dollar-threshold filter. The upstream facts retain their
grain; this comparison declares what a source line can establish.

F3 operating offsets bind `total_offsets_to_operating_expenditures_period` to
both period-column cover positions 28 and 44. Both assertions survive; a blank
or disagreement blocks the field. F3X binds its single position 39 to
`offsets_to_operating_expenditures_period`. There is no metadata alias fallback.
The pinned schema permits an exact decimal string for the latter field; the
binder now uses exact accepted field names instead of an `individual` substring
test. Blank, null, malformed and numeric strings retain distinct outcomes.

Individual itemized/unitemized fields keep their existing separate policy.
Aggregate totals, F3X 11D, H3/H5, SL references and F3P are not additional
Schedule A leaves. Unassigned groups remain outside indexes, not deleted records.

## Occurrences, not inferred financial members

Each report preserves every exact committee/file profile group once. Family
group indexes use the exact filing form, `SA` schedule and line. The outside
indexes preserve the remaining groups. The earlier profile's `11AI` disposition
is never reused as a filter for these different families.

The raw memo convention remains narrow: `X` is retained outside the nonmemo
subtotal; blank/source-null contributes to the occurrence subtotal; another
code blocks its family. Raw memo values remain unchanged. Unknown nonmemo
amounts block a numeric difference. No individual-flag filter, receipt-date
clipping or transaction deduplication occurs. Ordinal extrema are not interpreted
as a contiguous membership range.

For required-itemized fields, the output records exact signed nonmemo measures and a report-minus-detail
difference only when scope, field and nonempty detail qualify. Known occurrence
subtotals with unknown amounts are not lower bounds. A numeric mismatch remains
an observation, never a repair, donor assignment or inferred unitemized amount.

An absent family always has null detail and difference, including when its cover
is explicitly zero and the complete original has no Schedule A records. The
existing itemized empty-report rule is unchanged; it has not been generalized.
The separate [absence reviewer](./receipt-family-absence.md) can now qualify a
reported-zero observation using stronger complete original/profile inventories.
It embeds this command's result unchanged and never fills its null detail.

## Limits and next gate

The [retained gate](../audit/receipt-family-comparison-2026-09-11.md) records the
initial witnesses and replay results. The subsequent
[complete F3X witness](../audit/receipt-family-witnesses-2026-09-11.md) now passes
positive other-committee contribution and affiliated/party transfer comparisons,
complete original/profile grouped checks and unchanged-command replay. Fixture
checks cover all accepted fields; this real witness does not cover every form
or positive family shape. Group equivalence is not
proof of unique transactions, effective financial reports, original membership
for the full processed population, or spendable cash.

This command adds no family window or summary differences. The separate
[family-window comparator](./receipt-family-window.md) now uses the accepted
nonempty comparisons and absent-population evidence with exact field/date coverage.
It leaves this command's output unchanged. The remaining initial shapes now have
[positive original/profile witnesses](../audit/positive-receipt-families-2026-09-11.md),
not universal source validation or new metadata binding. The policy's complete-original
requirement still applies to other reports. Use retained evidence first; a bulk
rescan is not a prerequisite.

The [v2 retained gate](../audit/receipt-families-v2-2026-09-11.md) documents the
remaining report-level fields and unchanged prior-command replay. New-family
absence/window/summary qualification, other schedules,
F3P and other unsupported forms remain separate work. Cash availability, prior-cycle origin, liability
roles and terminal attribution remain unqualified. No graph, source pointer,
published calculation, Dagster asset or financial eligibility changes here.

The [positive remaining-family gate](../audit/positive-receipt-families-v2-2026-09-11.md)
now verifies every new source shape against complete originals and the saved
profile, with real report metadata. Four target fields bind as thresholded
components; three report bindings stay blocked by literal zero amendment
numbering on unamended originals. Review the general header rule before extending
new-family windows. Source examples do not authorize file-specific exemptions.
