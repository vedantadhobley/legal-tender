# Funding coverage and time gate

Status: implemented read-only coverage audit with a passing
[2024 source gate](../audit/funding-coverage-2026-09-08.md). No pooled-fund allocation
method is accepted or implemented here. This narrows the existing
[money and uncertainty contract](./money-measures.md) and
[flow invariants](./fec-flow-fact-requirements.md) into the checks needed next.

## Three independent answers

Keep reported receipt totals, source identity, and available cash separate.
Exact arithmetic or an exact report reference does not establish the other two.

The [receipt-family map](./receipt-families.md) now pins F3/F3X category meanings,
including thresholded detail, borrowing, returned funds and nonfederal transfers.
It does not itself accept a cash component or change this audit's inputs.
The [Go family comparator](./receipt-family-comparison.md) now compares initial
contribution, transfer and loan categories per report. Matching reported amounts
does not change the cash/funding guards here; absent detail remains unknown.
The [receipt inventory](./committee-funding-basis.md) covers published Schedule A
occurrences, including explicit unknowns. It is not a complete cash statement.
The [earmark association](./receipt-report-association.md) adds source evidence,
not a new dollar amount or a verified cash-availability date.

An unresolved donor is not automatically an unresolved amount. Conversely,
identified contributors do not make a partial funding denominator complete.
Keep the current no-terminal-allocation guard until the relevant dimensions
are accepted for the selected monetary question.

The [summary-value use policy](./summary-value-use.md) separates reported values
and diagnostic arithmetic from qualified funding inputs. These allocation gates
do not require every processed transaction or graph edge to wait for complete
report history. Unknown donor identity and unknown cash amount remain distinct.

## Required evidence, without synthetic gap filling

The [bounded report investigation](../audit/summary-report-review-2026-09-10.md)
demonstrates why these checks are independent: internally balanced reports can
disagree on carried cash, a published amendment flag can select a nonfinancial
attachment, and a machine transcription can disagree with the original image.
Retain such evidence without replacing source values or treating a numerical
repair as an accepted funding basis.

| Component | Treatment required before pooled allocation |
|---|---|
| Itemized receipts | Preserve disjoint source-role membership and signed amounts; assess cash versus valuation separately. |
| Unitemized receipts | Require an explicit compatible summary assertion. A detail/summary difference is not an unitemized total or a fabricated donor. |
| Other receipt families | Inventory loans, repayments, refunds, transfers, offsets, and unresolved roles; do not label every receipt a contribution. |
| Opening balance | Bind a reported opening amount to the exact committee, account scope, period, and source. Missing opening evidence remains missing. |
| Prior-cycle money | Retain an explicit opening-funds origin gap unless supported ancestry exists. A rolling four-cycle view does not erase carry-in funds. |
| In-kind values | Keep non-cash support distinct from cash funding. Do not treat an offsetting receipt/expenditure pair as two spendable amounts. |
| Negative adjustments | Preserve signed source grain. Do not clamp to zero or subtract from an arbitrary donor/date. |
| Memo and conduit rows | Preserve them as evidence; no automatic additional cash leg. Amount mismatches remain unexplained unless evidence resolves them. |

The existing `weball`/`webl` schemas contain candidate-summary fields such as
`COH_BOP`, `COH_COP`, `TTL_RECEIPTS`, and `CVG_END_DT`. They do not expose an
explicit unitemized-individual field or provide a per-report, per-upstream-PAC
opening balance. See the [pinned source schema](../../contracts/sources/fec/all-candidates-summary/v1/record.schema.json)
and [summary comparison contract](./calculation-contracts.md#contract-d-candidate-summary-reconciliation).
Do not distribute candidate-level balances across committees or treat the
committee master as a financial statement.

The implemented audit reports requirements as `supported`,
`absent_from_supplied_sources`, `scope_incompatible`, or `unresolved`.
These states concern the supplied receipt/summary inputs, not a claim that the
FEC has no additional source. The completed
[committee-summary review](./committee-summary-source.md) selects the modern
cycle CSV for explicit summary assertions. Its Go reader now passes the complete
four-cycle gate. Same-release fact publication and manual Dagster handoff now
pass, and [exact assertion grouping](./committee-summary-assertions.md) preserves
members and arithmetic differences. The CSV still lacks report/amendment/account
identities. The existing audit above is unchanged; it does not consume these
new summaries or reinterpret its older release ancestry.

The separate [summary/receipt readiness review](./summary-receipt-compatibility.md)
now compares input ancestry and exposes nine reported fields beside exact
recipient inventory cohorts. It does not calculate numeric differences: current
release ancestry differs, and report/form-line coverage remains unverified.
Its comparison blockers remain distinct from cash-timing/allocation blockers.

## Implemented audit boundary

```bash
legal-tender pipeline fec audit-funding-coverage \
  --storage-root /storage --basis-result <verified-inventory-json> \
  --receipt-bundle <exact-receipt-fact-bundle>
```

Go loads the existing immutable candidate-receipt bundle and requires its
Schedule A manifest, count, cycle, and source release to match the inventory.
It independently verifies and scans the two candidate-summary artifacts named
by that bundle. The unused linkage member is not consumed or used to distribute
candidate balances. This audit does not claim a new full-bundle readiness check.
An exact older bundle remains a reproducible input; the audit does not silently
substitute a current release or claim that this is the latest available data.

The receipt inventory is regrouped by disjoint component and receipt role with
exact signed conservation. Existing memo, unknown, loan, in-kind, refund, and
unresolved roles remain visible. The audit does not reread Schedule A rows;
opening its verified inventory still hashes the backing Parquet artifacts.

Every selected summary fact must have the exact publication envelope, canonical
dataset/cycle/candidate key, reviewed source layout, unique identity, and a valid
state. Every reviewed money field must agree with the preserved raw lexeme in
exact cents; blank is distinct from zero. Currency and measurement kind are
checked. Coverage dates must agree with the strict raw date and remain grouped
as blank, before, within, or after the source cycle. No archive-wide date is
assigned to every fact.

Summary profiles return field-presence lists, blank/positive/negative/zero row
counts for each money field, and exact coverage-date counts. They do **not**
sum money across candidates or across the two overlapping source populations.
All original publication counts survive. When source occurrences were excluded
from the singleton fact projection, the profile state is
`valid_fact_subset_source_exclusions`, not complete raw-source coverage. The
excluded occurrences and their issues remain in source evidence; this audit
does not reinterpret or repair them.

Invalid fact states, duplicate identities, source/typed disagreement, changed
artifact bytes, incomplete reads, and cancellation fail without a successful
partial result. The summary identity index has a 100,000-row cap. It is not an
unbounded in-memory transaction index.

The [wire contract](../../contracts/calculations/fec/committee-funding-basis/v1/coverage.schema.json)
pins all inputs, findings, profile counts, and guards. Its audit ID hashes the
deterministic output with `audit_id` empty; result JSON has no execution-time
field. Both `complete_committee_funding_basis` and
`terminal_attribution_eligible` remain false. No source, current pointer,
Arango graph, Dagster asset, or resident service is changed.

Receipt event-date completeness, per-committee report coverage, detailed
disbursement cash roles, original-donor resolution, and recipient cash
availability are not profiled by this command. A preserved date column is not
promoted to a complete chronological funding model.

## Independent time meanings

Retain event dates, report coverage, source-cycle membership, filing identity,
and source retrieval/publication time independently. A published cycle is an
input partition, not proof that every date lies within that cycle or that the
complete original filing is present. Keep the default four-cycle view as a
view, not a retention or balance-reset boundary.

The FEC [receipt-date guidance](https://www.fec.gov/updates/date-of-receipt-for-campaigns-joint-fundraising-and-conduit-contributions/)
distinguishes the original donor's date from the conduit-to-campaign date.
Consequently, an original contribution date alone must not become the date the
recipient could spend those funds. Both dates survive a report association;
missing dates stay unknown. Do not require equal dates to establish the source
reference or infer that delayed reporting proves an earlier cash balance.

A future cash allocation must declare its period and within-day ordering rule.
It cannot fund an earlier expenditure with later receipts without an explicit
opening-balance or borrowing explanation. Same-day order without evidence
remains ambiguous. Report and amendment selection must be source-qualified;
never join two filings merely because their transaction IDs match.

## Allocation acceptance remains separate

No FIFO, proportional mixing, or graph-wide normalization is accepted by this
gate. Direct/earmarked evidence and modeled pooled allocation must remain
different result classes with versioned assumptions. Cyclic transfers require
an explicit conserving solution; revisiting a committee does not create funds.

Every later candidate result must conserve its selected boundary through
exclusive explained components and an explicit unresolved remainder. Never sum
every intermediate committee's receipts into candidate money. Never label the
sum of incomplete signed observations a lower bound; missing negative amounts
can reduce it. A numerical range needs defensible bounds, not invented error
bars. Until then, report the known subtotal and coverage state separately.
