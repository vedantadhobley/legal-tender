# Receipt-family summary gate — 2026-09-11

Result: the additive [Go summary comparator](../design/receipt-family-summary.md)
preserves every prior result and compares only supported, date-compatible family
fields. The retained F3 span matches five summary fields on both comparison sides.
Missing coverage, different dates and blank summary operands remain blocked.

## Source qualification

The reviewed mappings use the official summary dictionary, the pinned electronic
8.4 workbook and F3/F3X instructions linked in the design. They preserve candidate-
guaranteed versus other loan roles and authorized/affiliated transfer meanings.
They do not attribute loan principal, prove cash availability or merge ledgers.

Existing source pins were reused:

- Summary dictionary HTML: `6fa2fa43035697db5d8de79590e8ecdc45f1c3325734d99991711f144eaa4141`.
- F3 instructions: `3dc5ec33fa35a403a890b082cbe9fe4134e6984c81069169f25a7cba2791ce84`.
- F3X instructions: `d7897e5aee7fd5fb65b4ee75b438d025a53b0f77160d55ed77f83c415526a8f0`.
- Electronic workbook: `9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6`.

Raw source paths and full provenance remain in the [family-map gate](./receipt-families-2026-09-11.md)
and [summary-source gate](./committee-summary-source-2026-09-08.md). Current official
form instructions/dictionary were also reviewed online. Small public OpenFEC model
files were inspected for terminology; they are not treated as proof of the bulk
summary ETL or as executable dependencies.

The calculation reuses the five [family-window cases](./receipt-family-window-2026-09-11.md),
the exact v2 profile and same-release summary fact set
`603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637`.
No new report capture, API call or processed Schedule A scan was needed.

## Observed results

| Case | Summary versus reported | Summary versus qualified detail | Reason when blocked |
|---|---|---|---|
| F3 April 2023–April 2024 | 5 equal | 5 equal | None |
| Same F3 documents, full cycle | 5 blocked | 5 blocked | Different summary dates and 335 uncovered days |
| F3 span, missing cover | 5 blocked | 5 blocked | 92 uncovered days |
| Unresolved partial F3X report | 4 blocked | 4 blocked | Scope/coverage gaps, date mismatch; loan summary blank |
| Complete F3X January report | 4 blocked | 4 blocked | Summary end differs; loan summary blank |

These requests overlap and are not disjoint financial populations.
The F3 `CAND_LOAN` comparison is exactly 200,000,000 cents on all three sides;
the four other accepted family values are explicit zeroes. Its pre-existing
150,000,000-cent cash-equation discrepancy remains unchanged. Matching these
scalars does not resolve that discrepancy or identify loan principal origin.

The F3X January report still has matching 173,100,000-cent other-committee
contributions and 54,848,772-cent affiliated/party transfers at report/detail
level. The broader summary values are retained without subtraction. Its
`OTH_LOANS` raw field is empty and typed value null, despite an explicit zero
January cover. No zero or `TTL_LOANS` fallback fills that field.

## Verification

Full Go tests, vet, focused race checks and the retained-summary identity gate
pass. Fixtures cover exact form mappings, field-local conflicts, preserved
variants/fan-out, missing/invalid operands, separate comparison-side readiness,
date/form/type blockers, missing summaries, cancellation and signed arithmetic
beyond int64. The compiled map is checked against both versioned contracts.

Five new outputs replay byte for byte. Five earlier family-window commands,
four itemized-window commands and four summary/window-v2 commands replay exactly;
each new nested family-window result equals its prior artifact. Changed original
and profile pins produce no result. Separate source-reader tests reject changed
manifest identity, cycle and representative-row evidence.

Independent Python tests read the pinned raw summary CSV, verify all selected
assertion member locators/hashes, compare raw/typed operands, and independently
check scope/readiness and exact differences. Earlier original/profile and
per-day window checks also run. Ruff and local documentation-link checks pass.
Python remains an offline test oracle, not runtime domain logic.

## Retention and remaining work

Evidence is retained under
`/storage/dumps/audits/fec/receipt-family-summary/2026-09-11/attempt-01/`.
Outputs, descriptors, logs, drivers and the source snapshot have `SHA256SUMS`
and explicit case/Go/Python/docs/retention exit markers. Existing originals,
large profile and binary are referenced rather than duplicated there.

The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
There was no source/calculation publication, graph write, Dagster activation,
credential access or operational service change.

Next: bounded positive witnesses for remaining family/form roles. Complete
financial membership, cash continuity and terminal-dollar allocation remain open.
