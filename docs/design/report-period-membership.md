# Report membership and period coverage

Status: implemented bounded Go diagnostic. The
[retained-source gate](../audit/report-period-membership-2026-09-10.md) passes
source conservation, chain checks, interval coverage, and replay. The
[versioned policy](../../contracts/calculations/fec/report-period-membership/v1/policy.json)
defines the accepted structural checks. Financial membership is not yet accepted.

## Three different claims

| Output | Meaning | Does not establish |
|---|---|---|
| Publisher member | This exact endpoint record says `is_amended=false`. | Financial replacement or precedence over another endpoint. |
| Chain candidate | The observed electronic report cohort has one publisher member and a complete, consistent chain within the captured cohort. | Complete amendment history, verified cover values, or cash availability. |
| Observed partition ready | Chain candidates cover the explicit window exactly once, with no unresolved intersecting scope or boundary crossing. | A cycle monetary total, required filing coverage, active lifetime, or financial membership. |

This is a structural membership check, not a new monetary ledger. It never sums
the original plus its amendments. It also never replaces a problematic publisher
member with an older report merely because that report has usable numbers.

The [FEC amendment guidance](https://www.fec.gov/help-candidates-and-committees/filing-amendments/)
distinguishes complete electronic resubmissions from paper amendments that can
contain only corrected schedules. A uniform last-file-wins rule therefore cannot
establish financial replacement. The
[OpenFEC report endpoint implementation](https://raw.githubusercontent.com/fecgov/openFEC/develop/webservices/resources/reports.py)
also exposes `is_amended` and `most_recent` as separate filters; this diagnostic
does not treat them as interchangeable claims.

## Inputs and conservation

```bash
legal-tender pipeline fec review-report-period-membership \
  --capture <retained-report-endpoint-capture.json> \
  --start 2023-01-01 --end 2024-12-31
```

The [metadata reader](./report-metadata-reader.md) revalidates the descriptor,
schema, query, all response/header hashes, records, and pagination. Accept only
one unfiltered single-committee `/reports/pac-party/` or `/reports/house-senate/`
capture, with Form 3X or Form 3 respectively. Exact-file `/filings/` samples
cannot stand in for a committee report population. No HTTP call runs.

Every raw record remains in `evidence`, with exact body/object identity and
one-based page/ordinal ancestry. Each normalized observation belongs to one
cohort or the explicit ungrouped list. The command rejects a source capture
with blocking integrity, schema, scope, or duplicate-file issues; it does not
silently deduplicate. Invalid semantic scope remains an ungrouped observation.

The reader's existing limits bound this diagnostic to 16 pages, 100 records per
page, and 16 MiB of source artifacts. Requested windows must be explicit canonical
inclusive dates, ordered and at most 36,600 days. This is a work budget, not a
retention policy. Capture cycle, receipt date, report period, and review window
remain distinct. Midnight-without-zone metadata dates are accepted; non-midnight
times are not truncated.

## Source-reported chain checks

Cohorts use exact form, report type, report year, and start/end dates within the
one committee capture. They are scope buckets, **not inferred amendment families**.
Different dates, types, or forms are never silently reconciled into one scope.

To produce a chain candidate:

1. Every cohort member must explicitly report `means_filed=e-file`.
2. Exactly one member must explicitly say `is_amended=false`; every other
   member must explicitly say true. Null does not become false.
3. Every amendment chain must contain unique positive integral IDs and end in
   its own file ID. Preserve raw number/string representations; normalize only
   checked plain integral decimal references for this calculation.
4. The candidate's chain must contain exactly all observed cohort members.
   Each member's own chain must equal the corresponding prefix. Missing or
   out-of-scope references, disconnected same-period filings, duplicate links,
   or inconsistent ordering leave the cohort unresolved.

No largest-ID rule, latest flag, predecessor sentinel, timestamp, amount, or
committee-specific exception selects membership. Paper/mixed-origin cohorts
remain unresolved even when only one record is publisher-selected. A missing
paper chain is not repaired using equal dates or a nearby electronic report.

These are conservative **observed-chain** checks. They neither prove source
history closure nor determine an effective field overlay for partial amendments.
The retained attachment case motivates this boundary but is not a runtime ID rule.

## Calendar coverage

An event sweep creates separate publisher-member and chain-candidate timelines.
Each segment contains inclusive dates and the number of selected report intervals
covering it. Counts greater than one expose potential double counting; zero
exposes unrepresented days in this capture and requested window.

Intervals touching on consecutive days are adjacent. Intervals sharing a day
overlap. Nested reports, leap days, and cross-year reports follow the same rule.
Covered plus uncovered days must equal the requested window; overlap days are a
subset of covered days, not extra coverage.

Only dates are clipped for the coverage diagnostic. Cross-boundary report indexes
remain explicit and block partition readiness: a report-level amount cannot be
prorated across a cycle boundary from this metadata. Original intervals survive.

Unknown dates block the requested window because they cannot be located. A known
unresolved cohort entirely outside that window stays visible without blocking
the narrower partition. No report, zero receipts, or a termination code implies
zero funding or an inactive period before/after the reported interval. Gaps are
not automatically missing required filings or evidence of noncompliance.

`observed_partition_ready` requires all relevant scope and interval checks plus
at least exact-count satisfaction, not merely a partial traversal. It describes
the captured observations only. Neither exact counts nor an empty page prove an
atomic snapshot or complete history. The nested history/financial-selection
guards and top-level `financial_membership_ready`/`cycle_total_ready` stay false.

## Remaining financial boundary

The [reported unitemized fields](./report-unitemized-receipts.md) remain separate
observations. This command does not join them, reconcile cash, or publish a total.
A structural partition can coexist with a cash discontinuity or incorrect cover.
The [report coverage requirements](./receipt-report-coverage.md) still apply.

The additive [field binding](./report-field-binding.md) now connects chain
candidates to exact, source-qualified 8.4 electronic period fields. This command
remains unchanged. The [window calculation](./report-window.md) now adds field-level
membership and reported aggregation with separate subtotal/cash diagnostics.
Financial-use blockers remain explicit. Paper corrections, historical
refresh, full population, immutable publication, and recurring activation remain
separate; this diagnostic adds no Arango or Dagster dependency to bulk A/B/E.
