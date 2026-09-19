# Summary values: reported evidence versus funding inputs

Status: accepted use policy. Existing Go calculations remain unchanged; targeted
regression tests verify their isolation and retained source cases. This policy
does not implement a report selector, authorize corrections, or enable terminal
allocation. It narrows the next work under the
[funding coverage contract](./funding-coverage-and-time.md).

The [regression gate](../audit/summary-value-use-2026-09-10.md) checks the existing
Go behavior and five retained cases without new data acquisition.

## The boundary

The processed bulk ledgers and their accepted projections remain usable without
reconstructing every original report or amendment. The
[Schedule A calculation](./calculation-contracts.md#amendment-handling) uses the
publisher's processed snapshot, not a locally rebuilt filing history. Other
sources retain their own counting contracts: in particular, Schedule E still
uses its accepted effective-record calculation. Bulk acquisition does not imply
one amendment rule for every dataset.

A summary conflict is not a new reason to delete a reported transaction, remove
its observation edge, or block an unrelated graph publication. Those outputs
still enforce their existing identity, membership, and integrity checks. They
show disclosed observations and possible paths, not proven cash provenance.

The [report-metadata source](./report-metadata-reader.md) supports investigation
of report selection, coverage, and contradictions. It is neither another
transaction ledger nor an automatically more authoritative monetary source.
Missing metadata blocks only a use that needs the missing evidence.

## Permitted uses

| Use | Acceptance rule | Available now |
|---|---|---|
| Show a reported summary field | Verified source identity and exact raw/typed value; show blanks, invalid values, variants, scope, and warnings. | Yes, as a publisher assertion. |
| Calculate arithmetic inside one summary assertion | Every operand is a valid exact number from that assertion. Retain signed differences, including nonzero results. | Yes, through the existing diagnostic equations. |
| Compare two representations of the same report-period field | Exact file/filer/form/period mapping, explicit valid operands, and applicable field-conflict checks. | Yes, through the [Form 3X total-receipts comparison](./report-total-receipts-comparison.md) and [unitemized review](./report-unitemized-receipts.md). Not a detail reconciliation or accuracy proof. |
| Compare summary and reported-window scalars | Qualified field/date/type scope with both source identities and snapshot limitations. | Yes, through the [v2 reported-span comparison](./summary-reported-span.md). Common report membership and full-cycle financial acceptance remain unproven. |
| Compare summary and detailed reported subtotals | Qualified field/population/period mapping and compatible source ancestry; preserve each side and all warnings. | Yes, for the narrow [receipt/window occurrence comparison](./receipt-reported-window.md). Unique/effective financial membership and other receipt fields remain unqualified. |
| Use a value as a funding component | Source-qualified meaning, reporting scope, period, cash role, and disposition of relevant conflicts. | No new component is qualified by this policy. |
| Allocate upstream dollars through pooled funds | Qualified component set plus complete funding coverage, cash timing, conserving allocation rules, and explicit origin uncertainty. | Not implemented. |

There is no global "this committee's data is good" switch. A valid scalar is not
a complete cash statement. An arithmetic discrepancy identifies incompatible
operands, not which operand is wrong. A balanced equation does not prove the
figures describe the right reports or all relevant funds.

For a future reported-subtotal comparison, compatible scopes—not a zero
difference—make subtraction meaningful. An intra-summary inconsistency stays
visible beside that comparison; it need not make every reported scalar unusable.
The existing readiness-v1 command is deliberately more restrictive and continues
to emit null comparison deltas. A later version must implement the qualified
comparison; do not flip its current guards or reinterpret old results.

## Field-level rules

The first review covers these nine preserved fields. Unassessed fields stay in
the facts; this table neither discards them nor approves their financial use.

| Fields | Usable reported observation | Additional evidence before funding use |
|---|---|---|
| `INDV_ITEM_CONTB` | The publisher's itemized-individual subtotal. | Matching form/line population and period; relevant memo, valuation, source-discrepancy, and selection issues. |
| `INDV_UNITEM_CONTB` | The explicit unitemized-individual subtotal, including an explicit zero. | Compatible summary scope and cash meaning; retain unknown donor composition. Never obtain this value by subtracting detail from a total. |
| `INDV_CONTB` | The reported total individual contributions. | Qualified constituent coverage and resolution of relevant subtotal contradictions. Do not add this total to its own components. |
| `TTL_RECEIPTS` | The reported total receipts for the declared scope. | Contributing receipt families and cash/non-cash treatment; not all receipts are donations or spendable cash. |
| `TTL_FED_RECEIPTS`, `TTL_FED_DISB` | Separate reported federal-scope amounts. | Accepted form/field mapping for that scope. No substitution merely because the federal-column equation balances. |
| `COH_BOP`, `COH_COP` | Reported opening/closing balances. | Qualified reporting interval and balance continuity where required. A closing stock is not a receipt; an opening stock has its own origin uncertainty. |
| `TTL_DISB` | Reported total disbursements. | Disbursement-family and cash-role coverage. Schedule A does not supply this population. |

Numbers, dates, source snapshots, and donor identities are independent evidence
dimensions. A qualified unitemized amount can have unknown donor identities.
Missing donor identity does not by itself make that amount unknown, and a known
amount does not identify any terminal donor. Likewise, qualifying an opening
balance must not invent its donors or reset it to zero at a cycle boundary.

All variants remain accessible. For a future field-level consumer, agreement on
the exact field and its applicable scope can survive an unrelated contact-name
conflict. That is not permission to choose a favorable complete variant. A
conflict in the field itself or its required scope must remain unresolved.

## Apply the retained discrepancy evidence

The [original-report investigation](../audit/summary-report-review-2026-09-10.md)
already provides the following bounded cases. They are audit witnesses, not
runtime committee-ID rules, and do not estimate corpus-wide error prevalence.

| Case | Keep usable | Do not promote |
|---|---|---|
| Loan attachment selected instead of the substantive financial report | Published summary values, original cover values, endpoint assertions, and the exact arithmetic discrepancy as separate evidence. | A complete cash basis from those selected period totals; neither a blank cover converted to zero nor a handpicked older financial report is an accepted repair. |
| Adjacent reports disagree on carried cash | Each reported period value and the measured discontinuity; retain the filer explanation separately. | Cash continuity, a missing transfer, a loan repayment, or forgiveness inferred from the gap or explanation. |
| Paper image and transcription disagree on itemized contributions | Both located source assertions and the subtotal inconsistency. | The disputed processed itemized value as a qualified funding component, or an automatic replacement from arithmetic or a named exception. |
| All inspected summary equations balance | The exact reported numbers and zero diagnostic residuals. | Complete funding, compatible detail coverage, or terminal allocation merely because arithmetic passes. |
| No summary or receipt rows in the selected inputs | The explicit absence in those snapshots. | Zero funding, a complete period, or terminal-source status. |

These issues attach to the affected assertion, field/operand set, reporting scope,
and intended use. A cash-equation issue does not erase the individual fields;
an individual-subtotal issue does not erase balances. Whether either set is
qualified for a particular calculation is a separate check.

## How blockers propagate

1. Preserve the source facts and accepted processed observation projections.
2. Attach a finding to exact evidence and affected fields/periods. Keep
   observation, arithmetic, comparison, and funding-use states separate.
3. Block a derived result only when its declared dependencies include an
   unresolved required input. Do not spread one report conflict to unrelated
   committees, periods, or graph edges.
4. A candidate allocation that depends on that input must expose its unresolved
   dependency. Do not remove the committee and renormalize the remaining donors
   to 100 percent. Partial reported amounts are not automatically lower bounds
   when unknown signed adjustments remain.

This does not add a new runtime dependency manager. Fine-grained invalidation
stays in Go under the [existing responsibility boundary](./go-dagster-boundary.md).
Current graph inputs and all no-terminal-allocation guards remain unchanged.

## Implemented scoped report evidence, not a second ledger

The [bounded Go assessment](./report-scope-assessment.md) now passes the retained
attachment gate with source-located financial-cover presence, supplemental
transcription shape, raw metadata assertions, and unresolved states. Original
image fidelity and financial replacement selection remain separate; no reported
amount is promoted to a funding input.

The assessment starts with the retained attachment case and records
financial-cover presence, declared period/form, reported amendment links,
relevant endpoint differences, and an explicit representation-level disposition.
It distinguishes cover presence from a complete financial report. Preserve the
evidence for each conclusion; caller-supplied "trusted" booleans are not verification.

An attachment without a financial cover must not become a zero-valued replacement
financial report. Identifying that attachment does not prove that an older report
is the effective financial version; unresolved family selection remains explicit.
A reconstructed report-level component, if later accepted, must be a separate
versioned calculation. It never rewrites the processed Schedule A snapshot or
silently patches the publisher's summary value.

The [total-receipts calculation](./report-total-receipts-comparison.md) now
qualifies one precise field and reporting interval for reported-pair arithmetic,
not financial use. The unitemized review now preserves explicit period amounts,
own-source scope, and separate subtotal diagnostics under the same boundary.
The [membership review](./report-period-membership.md) now qualifies observed
chains and date partitions, not financial membership or cash continuity.
The [field binding](./report-field-binding.md) and [window calculation](./report-window.md)
now qualify exact reported pairs and whole-period sums with field-local coverage.
Cash stocks are boundary values; subtotal/carry-forward mismatches remain separate.
These are not accepted financial cycle totals or funding components. This work used
existing captures and verified layouts without another bulk scan or API retry.
Broader history acquisition remains conditional on a demonstrated
coverage requirement. The pending metadata empty-page transport test is separate
from this financial-policy work.
