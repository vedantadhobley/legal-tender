# Receipt families and funding-basis boundaries

Status: reviewed source map with a passing
[retained-source audit](../audit/receipt-families-2026-09-11.md). This is the
field-contract step after the [itemized-window comparison](./receipt-reported-window.md),
not a new runtime counting policy or an accepted cash denominator.

The [machine-readable map](../../contracts/calculations/fec/receipt-families/v1/contract.json)
pins the official electronic 8.4 workbook and F3/F3X instructions. It maps period
cover fields, exact Schedule A line references, candidate summary counterparts,
itemization scope, and nested subtotal equations. It does not change any
published fact, existing predicate, graph edge, or previous comparison output.

## Different lines answer different questions

| Family | F3 line | F3X line | Detail relationship |
|---|---|---|---|
| Itemized contributions from individuals/other persons | 11(a)(i) | 11(a)(i) | Itemized component; not proof of natural-person identity or cash. |
| Explicit unitemized individual contributions | 11(a)(ii) | 11(a)(ii) | Cover observation, not fabricated detail. |
| Party / other committee contributions | 11(b)/(c) | 11(b)/(c) | Itemization required regardless of amount; party filers use 12 for party-to-party transfers. |
| Candidate contributions other than loans | 11(d) | — | Thresholded detail; F3X 11(d) is a subtotal, not this family. |
| Authorized / affiliated-or-party transfers | 12 | 12 | Itemization required; category may include loans and repayments. |
| Candidate-made or guaranteed / other loans | 13(a)/(b) | 13 | Itemization required; lender, borrower and guarantor are different roles. |
| Loan repayments received | — | 14 | Return of earlier lending, not a new contribution. |
| Operating offsets | 14 | 15 | Thresholded detail for recoveries such as refunds or deposits. |
| Contribution refunds received | — | 16 | Itemization required; an uncashed returned check has different reporting treatment. |
| Other receipts | 15 | 17 | Thresholded detail; not automatically donations. |
| Nonfederal / Levin transfers | — | 18(a)/(b) | H3/H5 observations with distinct account scope, not another Schedule A sum. |

These distinctions follow the FEC's form-specific instructions, not a shared
interpretation of a numeric line code. Exact periods and disclosure populations
must still qualify before comparison. [Form 3 instructions](https://www.fec.gov/pdf/forms/fecfrm3i.pdf),
[Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf).

The current names `individual_itemized` and `INDV_ITEM_CONTB` follow source
labels. They do not prove that each underlying contributor is a natural person.
Contributor/entity resolution remains independent of line membership.

## Detail coverage is not the cover total

For some categories, all receipts must be itemized. For others, only receipts
meeting the reporting threshold require detail. The all-required rule is a
reporting requirement, not proof that our processed population is complete,
unique, effective, correctly classified, or synchronized with the cover.

The existing complete original-file witnesses include positive other-receipt
cover amounts with no matching Schedule A rows. Keep those reported values and
the absence of detail separately. Do not label the difference unitemized money,
infer the payer or purpose, or turn an absent line into zero. An explicit
unitemized-individual field has stronger meaning than an unnamed residual.
The [FEC summary dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/)
also distinguishes itemized, unitemized, loan, transfer, offset and other fields.

The test audit preserves every selected occurrence and exact signed amount.
Known detail totals are not lower bounds when unknown signed values exist.
Unknown memo codes, source nulls, and unsupported keys remain visible. The map
does not reinterpret the v2 profile's old `outside_reviewed_form_line` decisions
as rejected transactions: those decisions belonged to its narrower `11AI` policy.

## Reported receipts are not necessarily spendable cash

In-kind support is reported under contribution categories and also as an
expenditure. Lack of a memo flag does not establish spendable cash. Loans need
their own origin and liability treatment; a candidate guarantee does not prove
that the candidate supplied the principal. Schedule C/C1 evidence can add terms,
balances and guarantees without adding a second loan-receipt amount.
[FEC contribution and loan instructions](https://www.fec.gov/pdf/forms/fecfrm3i.pdf).

Transfers can move previously counted funds; repayments, refunds and offsets can
return prior outflows. The map therefore records these as different families,
not different spellings of “donation.” Account transfers must not be added to
their underlying source receipts a second time. These are attribution design
constraints, not inferred links between particular payments.

Opening cash, prior-cycle origin, effective financial report membership,
non-cash classification and within-period availability remain separate
[funding-basis requirements](./funding-coverage-and-time.md). Matching subtotal
equations cannot establish them.

## Aggregates, scope and unsupported data

The map distinguishes leaf receipt categories from nested cover totals. Its
equations expand each total-receipts value to each leaf exactly once. F3X total
federal receipts removes nonfederal/Levin transfers once; it is not another
amount to add to total receipts. Preserve original totals and equation residuals
rather than repair a filed value.

Only exact F3/F3X field meanings are mapped here. F3P, F4 and F9 occurrences
remain preserved outside this map. F3P needs a separate presidential-form gate,
including its processed `17A` label and federal-fund fields. An unrecognized key
always remains unresolved, whether or not it appears in today's saved sample.

The retained F3X profile contains literal `11D`, `19A`, `SL1A` and `SL2` references.
Do not reinterpret `11D` as candidate contributions, strip `A` from `19A`, or
equate an `SL` reference with an H5 account transfer. The audit exposes those
populations without creating a receipt amount or changing a source label.

Workbook coverage alone is not an original-file gate. The initial source-map
audit's complete witnesses were F3; the later [complete F3X witness](../audit/receipt-family-witnesses-2026-09-11.md)
and [positive-family gate](../audit/positive-receipt-families-2026-09-11.md) now
provide bounded original coverage for both forms. Earlier partial/paper examples
keep their original limits.

## Bounded Go consumer and remaining work

The [additive Go comparator](./receipt-family-comparison.md) retains its initial
committee-contribution, transfer and loan subset as v1. Opt-in v2 includes all
remaining mapped non-individual Schedule A leaves at report scope. Required-
itemized fields can qualify a numeric difference; thresholded detail remains a
component with no total difference, even when values match. Cover-only fields
remain separate observations. Earlier window and summary policies are unchanged.

Do not silently broaden the existing empty-report zero rule to every absent
family. Bind new cover/metadata fields and verify their report/form/period
scope before emitting new window or summary differences. Source-aligned profile
evidence can be reused; another bulk scan is not a prerequisite for this step.

The separate [family-window](./receipt-family-window.md) and
[family-summary](./receipt-family-summary.md) consumers now add scoped comparisons
for the initial subset. Remaining receipt categories, wider source populations,
F3P support, complete cash/funding acceptance, recurring publication and terminal
allocation are separate work. Python remains only the independent source-map
and comparison test oracle; runtime processing is Go.
