# Report-window calculation gate — 2026-09-10

Status: Go implementation, retained-source checks, and replay pass. The
[design](../design/report-window.md) owns the current contract.

## Retained cases

The gate reuses the [verified electronic reports](./report-field-binding-2026-09-10.md)
and the existing complete House/Senate and PAC/Party captures. It performs no
new acquisition or bulk scan. Descriptor variants explicitly select retained
document witnesses, not financial membership; Go still verifies the whole capture.

| Case | Requested window | Result |
|---|---|---|
| SID narrower window | 2023-04-01 through 2024-04-30 | Five candidate reports cover all 396 days; seven reported window values qualify. The supplied superseded report stays unbound. |
| SID full 2024 cycle | 2023-01-01 through 2024-12-31 | The same observed sums survive, but 335 uncovered days prevent all full-window values. |
| SID missing-cover test | 2023-04-01 through 2024-04-30 | Deliberately omit one retained candidate cover from the descriptor. Its 92 days remain unbound; missing report equations and both adjacent cash checks are unavailable. No older report fills the gap. |
| NRCC unresolved cohort | 2024-09-01 through 2024-09-30 | The visible electronic prefix does not resolve the mixed-origin amendment cohort. All 30 days remain unbound and values remain null, not zero. |

For the qualifying narrower window, exact reported values are:

| Field | Reported USD |
|---|---:|
| Itemized individual contributions | 114,315.00 |
| Unitemized individual contributions | 6,252.00 |
| Total individual contributions | 120,567.00 |
| Total receipts | 2,120,568.37 |
| Total disbursements | 598,964.00 |
| Opening cash at window boundary | 0.00 |
| Closing cash at window boundary | 21,604.37 |

These are separately labeled reported values, not sums to combine into one
funding total. All five report-local individual and cash equations balance.
Three adjacent cash handoffs balance; the last retains a **−150,000,000-cent**
residual. The whole-window cash equation retains the same residual. This is the
[previously verified carry-forward discrepancy](./report-field-binding-2026-09-10.md#results),
not a new inferred transaction, source correction, or financial-use approval.

## Verification

- Full Go tests and vet; report-metadata, report-period, and CLI race checks.
- Fixtures cover exact sums, stock boundaries, document order, signed values,
  explicit zero versus empty populations, field-local blanks, subtotal/cash
  mismatches, missing covers, incomplete windows, overlapping/shared days,
  cross-boundary amounts, outside-window issue isolation, duplicate documents,
  invalid versions/JSON/Unicode, artifact tampering, cumulative budgets, and cancellation.
- Four retained CLI cases replay byte-identically. Seven prior field-binding
  results and twelve older unitemized, total-receipts, and period-membership
  results remain byte-identical after shared capture/JSON reuse.
- Twenty-five independent Python checks pass across the new window, prior
  binding, and membership gates. They verify raw bodies and metadata ancestry,
  each member's exact amount, every requested day, missing members, boundary
  balances, all equation operands/residuals, and unavailable states.
- Ruff, Go formatting, and documentation links pass. Runtime Python is unchanged.

Evidence is retained at
`/storage/dumps/audits/fec/report-window/2026-09-10/attempt-01/`, with results,
descriptors, replay/test logs, drivers, source/contract/test snapshots, and verified
`SHA256SUMS`. Existing original reports and metadata remain in their earlier roots.

The active FEC release pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Processed facts, graph projections, and Dagster are unchanged. The subsequent
[reported-summary comparison gate](./summary-report-window-2026-09-10.md) now
passes without terminal allocation or a new corpus fetch.
