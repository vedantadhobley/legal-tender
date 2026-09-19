# Explicit report-period unitemized receipts

Status: implemented bounded Go review, not a funding-component selector. The
[retained-source gate](../audit/report-unitemized-2026-09-10.md) verifies raw
observations, scope, exact arithmetic, and replay. The
[versioned policy](../../contracts/calculations/fec/report-unitemized/v1/policy.json)
owns the accepted mapping and guards.

## Meaning and source mapping

This review reads Form 3X line 11(a)(ii), Column A: explicitly reported
unitemized individual contributions for the reporting period. The official
[Form 3X instructions, page 6](https://www.fec.gov/resources/cms-content/documents/fecfrm3xi.pdf)
distinguish it from itemized contributions, their total, and Column B
calendar-year-to-date amounts. The summary does not identify its donors or
establish a fixed single-donation size class.

| Representation | Itemized | Unitemized | Total individual contributions |
|---|---|---|---|
| Pinned paper P3.4 Form 3X, one-based positions | 29 | 30 | 31 |
| Pinned `/v1/reports/pac-party/` record | `individual_itemized_contributions_period` | `individual_unitemized_contributions_period` | `total_individual_contributions_period` |

Paper YTD positions 79–81 are not fallbacks. The `/v1/filings/` endpoint does
not supply this detailed unitemized field: `not_supplied_by_endpoint` is distinct
from a supplied null. Other report endpoints remain unsupported by this Form 3X
review; their original assertions remain in the nested evidence.

The [source reader](./report-scope-assessment.md) revalidates pinned local
body/header bytes and all metadata captures on every invocation. The review
does not accept a caller-supplied assessment or trusted eligibility flag.
Paper blanks, metadata nulls, empty strings, explicit zero, signed valid amounts,
and invalid amounts remain distinct. JSON number/string types and original
lexemes are retained. Only the pinned fields that allow strings use this
parsing rule; the older numeric-only total-receipts mapping is unchanged.

## Three separate results

1. **Reported observation:** retain the explicit unitemized field and its own
   reporting interval. Metadata values survive an unavailable paper/electronic
   cover; they do not borrow that cover's dates. Scope blockers remain visible.
2. **Intra-assertion subtotal diagnostic:** calculate `total - itemized -
   unitemized` only when all three operands are valid. Preserve a signed exact
   difference as `balanced` or `mismatch`; missing/invalid operands mean
   `unavailable`, not zero. Never solve for unitemized or fill it from detail.
3. **Reported pair:** compare each exact-file metadata observation to the paper
   unitemized field only when both values and their required scopes qualify.
   Emit metadata-minus-paper cents, or a null difference with blockers.

Pair scope reuses the [total-receipts review](./report-total-receipts-comparison.md)
checks: one complete qualified paper cover, filer, form, report code, amendment
indicator, paper origin, exact dates, and no conflicting document URL. Partial
responses and duplicate covers cannot qualify a pair. Unrelated invalid amounts
cannot conceal extra covers. Non-midnight timestamps are not truncated to dates.

A subtotal mismatch does not erase the explicit unitemized observation or
alone block a same-field reported comparison. It remains relevant to later
financial use. Matching scalars do not prove source accuracy or report selection.
Latest/amended-status assertions never choose a winner across metadata records.
All source issues and raw fields remain available in `evidence`.

## Command and boundaries

```bash
legal-tender pipeline fec review-report-unitemized \
  --source-url <original-report-url> \
  --body <retained-body> --body-sha256 <expected-sha256> \
  --headers <retained-headers> --headers-sha256 <expected-sha256> \
  --metadata-capture <retained-capture.json>
```

The command shares the bounded reader's input limits. It emits JSON on stdout;
exit zero means a valid review, not qualified financial use. Repeated metadata
captures produce separate observations. No matching capture means no metadata
comparison, not zero contributions. Exact differences use arbitrary-precision
integer strings over checked source cents.

`amount_method` is `explicit_reported_field_only`. `donor_composition` is
`not_identified_by_summary`. Financial-component, cycle-comparison, and terminal-
attribution eligibility remain false, as do the reader's financial-selection,
history, and original-image guards. No donor count, corporate identity, cash
denominator, or terminal origin is inferred from this field.

This is manual diagnostic output. It adds no acquisition, bulk scan, source
publication, graph dependency, Dagster asset, or Python runtime calculation.
Processed A/B/E ledgers keep their accepted source and counting rules.

The [membership review](./report-period-membership.md) now supplies observed
chain candidates and exact date coverage. The additive
[electronic field binding](./report-field-binding.md) now qualifies 8.4 covers and
binds explicit unitemized values with six other period fields to exact candidates.
This paper/metadata review stays unchanged. Window-level field coverage and
financial membership remain separate; bindings do not authorize pooled allocation.
