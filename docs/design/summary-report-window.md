# Compare summary assertions with reported windows

Status: implemented v1 bounded Go diagnostic, preserved for replay. Use the
[v2 reported-span comparison](./summary-reported-span.md) for the corrected
reported-flow scope; v1's cycle-prefix check is too strict for that narrower use. The
[retained-source gate](../audit/summary-report-window-2026-09-10.md) passes. The
[versioned policy](../../contracts/calculations/fec/summary-report-window/v1/policy.json)
qualifies reported scalar comparisons, not financial reconciliation.

## Inputs and lineage

```bash
legal-tender pipeline fec compare-summary-report-window \
  --storage-root /storage --summary-facts <exact-summary-manifest.json> \
  --capture <report-endpoint-capture.json> --documents <document-set.json> \
  --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```

The command re-verifies the [reported window](./report-window.md) and the complete
published [summary assertions](./committee-summary-assertions.md). Committee and
cycle come from the verified single-committee capture, not caller overrides.
A summary from another cycle fails. Missing committee assertions remain absent,
not zero. Corrupt artifacts fail rather than becoming financial missingness.

The result retains the window once, all selected committee assertion variants and
occurrence members, source/fact/manifest identities, grouping identity and counts,
and each side's arithmetic diagnostics. It does not sum candidate fanout or choose
a favorable assertion. The complete summary source is verified once per invocation;
this bounded reviewer is not a proposed per-committee corpus pipeline.

Summary facts belong to an exact coordinated release. Metadata and original
documents have their own captures and hashes. `source_alignment` is explicitly
`independent_snapshots`; the summary does not identify its constituent file IDs.
Matching dates or values do not prove identical processing snapshots or report
membership. `same_report_membership_proven` therefore stays false. A numeric
comparison means two scoped reported observations can be subtracted, not that
one source validates, corrects, or supersedes the other.

## Date meaning is field-specific

The FEC defines summary flow totals from the start of the two-year cycle through
the most recent report. `CVG_START_DT` instead describes the first report's start.
Opening cash is a cycle-start balance; closing cash is from the most recent
filing. These are distinct dates, even if their amounts happen to agree.
[Official field definitions](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/).

| Summary field | Window counterpart | Required date scope |
|---|---|---|
| `INDV_ITEM_CONTB` | Itemized individual contributions | Cycle start through summary end |
| `INDV_UNITEM_CONTB` | Explicit unitemized individual contributions | Cycle start through summary end |
| `INDV_CONTB` | Total individual contributions | Cycle start through summary end |
| `TTL_RECEIPTS` | Total receipts | Cycle start through summary end |
| `TTL_DISB` | Total disbursements | Cycle start through summary end |
| `COH_BOP` | Opening boundary cash | Cycle-start stock |
| `COH_COP` | Closing boundary cash | Exact summary-end stock |

V1 conservatively requires both first-report start and requested start to equal
the cycle boundary for flows and opening cash. A later first report does not prove
that earlier days were inactive. Neither an explicit zero balance nor matching
flow totals fills that gap. The closing-stock comparison needs the exact ending
date, not equal starting dates. Opening-stock comparison does not require equal
ending dates. All fields still inherit the window reader's complete field-coverage
requirement; v1 does not extract a lone closing cover from an incomplete window.

Coverage dates must be valid, ordered, and inside the source cycle. Missing or
conflicting type, designation, or coverage scope remains blocking. Applicable
bound metadata observations must report the same committee type and compatible
form. The reviewed mapping covers H/S with Form 3 and N/Q/O/U/V/W/X/Y with Form 3X;
presidential, historical nonfederal, and other types require separate qualification.
[Official committee types](https://www.fec.gov/campaign-finance-data/committee-type-code-descriptions/).

## Scalar arithmetic, not financial acceptance

Each assertion gets seven comparisons with exact field references, both available
values, applicable blockers, and `blocked`, `equal`, or `different` state.
`delta_minor_units` is **summary minus window**, using arbitrary-precision signed
integer arithmetic. The source parser's existing individual-value limits stay
unchanged. A blocked comparison retains its operands but has a null delta.

Missing/invalid fields, partial observed sums, and absent populations never become
zero or a complete window value. A field conflict blocks that field in every
variant; an unrelated contact conflict need not block an otherwise compatible
field. All variants survive either way. No federal-column fallback is permitted.

Summary subtotal/cash mismatches and report/window/carry-forward mismatches stay
visible without erasing independent scalar comparisons. The existing summary cash
equation and window cash equation use opposite coefficient orientations; inspect
their named operands before comparing signed residuals.

`financial_use_eligible` and `terminal_attribution_eligible` remain false. This
does not qualify account completeness, available cash, donor composition, or
upstream allocation. Existing summary-versus-detail readiness-v1 stays unchanged.

## Operations and remaining scope

Exit zero means a valid diagnostic, including blocked and different fields.
Go owns the complete runtime path. No HTTP, bulk transaction scan, fact/source
publication, graph write, or Dagster change occurs. Python is an independent audit
oracle only. Source schemas do not change from a remote page during execution.

The [early-cycle investigation](../audit/cycle-prefix-2026-09-10.md) now separates
reported-span comparisons from proof of full-cycle coverage in v2. Earlier
inactivity is not established or required for that narrower comparison. Matching
source-aligned detail, financial membership, compact publication, and recurring
processing remain separate.
