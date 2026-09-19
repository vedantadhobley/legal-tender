# Summary versus reported-window gate — 2026-09-10

Status: bounded Go comparison, source checks, and replay pass. The
[design](../design/summary-report-window.md) owns the implemented contract.
The later [v2 source gate](./cycle-prefix-2026-09-10.md) corrects the overly strict
prefix prerequisite for reported flow comparison. This v1 evidence remains unchanged.

## Retained evidence and result

The command reuses the four [report-window cases](./report-window-2026-09-10.md)
and the verified 2024 [summary publication](./fec-v4-publication-2026-09-10.md).
No new source data, original filing, or transaction corpus was acquired or scanned.

| Case | Result |
|---|---|
| SID, 2023-04-01 through 2024-04-30 | Closing cash is a compatible reported-stock comparison: both sources report 2,160,437 cents; delta zero. |
| Same case, five flow totals | Both reported values are preserved and numerically equal. Comparisons remain blocked: first report and window start after the summary's nominal cycle boundary. |
| Same case, opening cash | Both values are explicit zero, but April opening cash does not establish January opening cash. Delta remains null. |
| SID full-cycle window | Existing coverage gaps and ending-date mismatch remain; no comparison qualifies. |
| SID deliberately omitted-cover fixture | Missing coverage remains; no partial sum replaces a window operand. |
| NRCC unresolved cohort | Existing unresolved membership and mismatched summary/window dates remain; no comparison qualifies. |

The SID summary retains its +150,000,000-cent cash diagnostic. The window retains
its −150,000,000-cent residual and the original adjacent-period discrepancy.
Those equations have opposite coefficient orientations; neither result is a
correction. The closing comparison does not establish cash continuity, accuracy,
common processed-report membership, financial funding, or terminal attribution.

This gate qualifies one real closing-stock pair. Fully compatible flow/opening
comparisons, including nonzero differences, are verified with synthetic cases;
this is not a successful real cycle-flow reconciliation or corpus-wide assessment.

## Verification

- Full Go tests and vet; summary-assertion, report-period, metadata, and CLI race checks.
- Fixtures cover exact signed differences beyond int64, partial cycle-to-date
  scope, late starts, stock-specific boundaries, invalid/reversed/out-of-cycle
  dates, blank versus explicit zero, field/scope conflicts, contact variants,
  duplicate/fanout membership, missing committees, types/forms, and cycle mismatch.
- All four CLI results replay byte-identically. Their embedded windows equal the
  previous outputs, and the prior standalone window command is byte-identical.
  Tampered summary/document identities and a wrong-cycle summary fail without
  emitting a result; stored readiness claims cannot bypass source verification.
- Independent checks read the complete small raw summary CSV, verify its manifest
  ancestry and hash, conserve every selected occurrence, and compare raw decimal
  values and source locators. Existing raw-report/metadata/day-coverage checks run
  alongside these tests. No runtime Python was added.

Evidence is retained at
`/storage/dumps/audits/fec/summary-report-window/2026-09-10/attempt-02/`, with
outputs, descriptors, drivers, test/replay logs, code/contract snapshots, explicit
exit markers, and verified `SHA256SUMS`. Original source artifacts remain in their
existing immutable locations.

Attempt 01 is retained as a partial audit copy with `retention.exit=2`: the
capability-restricted container could not restore archived file owners. Attempt
02 uses `--no-same-owner`, retains the same verified outputs, and requires no
additional capabilities. This copy failure did not affect source data or calculations.

The active source pointer is unchanged:
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Accepted facts, source publications, graph projections, and Dagster are unchanged.
