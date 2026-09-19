# Reported span versus full-cycle coverage

Status: implemented v2 comparison with a passing
[source and replay gate](../audit/cycle-prefix-2026-09-10.md). This corrects v1's
overly broad cycle-prefix prerequisite for **reported flow comparisons**. It does
not establish activity before a first report or promote full-cycle funding use.

## What the source investigation established

The FEC dictionary distinguishes the two-calendar-year summary population from
the first report's coverage start. The FEC's committee presentation also labels
raised/spent totals with their actual reported start/end dates, including late
starts. We use that declared span for a bounded reported-observation comparison;
we do not claim that it proves identical constituent reports or zero activity
outside the span. This is a reviewed interpretation of the two source descriptions,
not a finding that the dictionary's nominal cycle boundary changed.
[Summary definitions](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/),
[retained presentation witness](https://www.fec.gov/data/committee/C00843367/?cycle=2024).

Registration cannot establish the financial start date. FEC guidance requires
the first report to include pre-registration activity, including exploratory
activity, and to adjust its start accordingly. The retained case's April report
start precedes its June registration. Neither the earliest report in a captured
cycle nor an original-report indicator proves the first activity ever occurred
on that date. [First-report guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/quarterly-reports/).

Candidate Form 3 cumulative fields also cannot supply a calendar-cycle prefix:
Column B uses the office's election cycle, not necessarily our two-calendar-year
partition. House and Senate reporting cycles differ, and special elections need
their own dates. Do not reinterpret metadata names ending in `_ytd` as a common
time contract. No cumulative column is substituted by this calculation.
[Candidate aggregation guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/election-cycle-aggregation/).

## Implemented boundary

```bash
legal-tender pipeline fec compare-summary-report-window-v2 \
  --storage-root /storage --summary-facts <exact-summary-manifest.json> \
  --capture <report-endpoint-capture.json> --documents <document-set.json> \
  --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```

The [v2 policy](../../contracts/calculations/fec/summary-report-window/v2/policy.json)
uses the same verified inputs and field bindings as
[v1](./summary-report-window.md). Both commands reverify original bytes and
summary backing facts. V1 remains available with byte-identical retained outputs;
v2 does not rewrite previously published diagnostics or add a trusted-input flag.

| Use | V2 requirement |
|---|---|
| Compare five reported flow fields | The requested window exactly matches `CVG_START_DT` and `CVG_END_DT`, with every existing field/partition/type/conflict check passing. |
| Compare opening cash | Unchanged cycle-start stock rule. A late reported opening does not qualify January opening cash. |
| Compare closing cash | Unchanged exact ending-stock rule. |
| Claim full-cycle financial coverage | Not qualified by this command. |

Each assertion has a `cycle_span` with nominal cycle dates, its reported interval,
and any prefix/suffix outside that interval. Dates and inclusive day counts
conserve the calendar cycle, including leap days. No amount is assigned to those
outside intervals. Null prefix/suffix means no interval outside that boundary
**only when the reported span is valid**; an unqualified date state means unknown.
Conflicting date variants retain their own spans and remain blocking for comparison.

The span is only a boundary description. It does not prove that every day inside
it has reports. Internal gaps, incomplete documents, unresolved chains, overlaps,
and field nulls still block the corresponding window values. Selecting a wider
window does not add zero reports or silently change the requested scope.

`financial_coverage_established` and `outside_activity_known` stay false even
when dates touch both cycle boundaries. Existing `same_report_membership_proven`,
`financial_use_eligible`, and `terminal_attribution_eligible` guards also stay
false. Source snapshots remain independent. Exact equality is not an accuracy
proof, and different values remain valid diagnostics when their scopes qualify.

## Operations and next step

The only runtime changes are in Go's bounded summary comparison and CLI routing.
No source adapter, metadata API request, transaction scan, graph write, or Dagster
asset is added. Four small official pages are retained as audit evidence, not
runtime HTML inputs. Python remains an independent test oracle.

The [receipt/window comparison](./receipt-reported-window.md) now compares the
reviewed itemized-individual line against source-aligned occurrence evidence
over this same window. Other receipt families need their own field contracts.
Proof of inactive prefixes, full-cycle cash, and terminal allocation stays
separate; it is not a prerequisite for a narrower reported observation.
