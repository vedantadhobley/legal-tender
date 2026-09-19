# Explicit unitemized receipts gate — 2026-09-10

Status: bounded Go review, full Go checks, retained-case replay, independent
checks, and previous-command equivalence pass. The
[design](../design/report-unitemized-receipts.md) owns current behavior.

## Retained observations

The gate reuses the same four report bodies and two metadata capture descriptors
as the [total-receipts gate](./report-total-receipts-2026-09-10.md). All seven
exact-file metadata matches remain separate. No report/API data was fetched.

| File | Paper unitemized observation | Matching metadata observation |
|---|---|---|
| `1876290` | Blank; amount unknown | PAC report says `"0.00"`; filings endpoint does not supply the field. |
| `1882886` | Blank; amount unknown | PAC report says `"0.00"`; filings endpoint does not supply the field. |
| `1813890` | Explicit `"0.00"`, 2024-04-01 through 2024-06-30 | Only the filings endpoint matches; it does not supply the field. |
| `1833804` | Electronic 8.4 prefix is outside the qualified cover layout | PAC report says `"2256923.61"`, 2024-09-01 through 2024-09-30; filings endpoint does not supply the field. |

For `1813890`, the exact paper individual-subtotal diagnostic is
`699.00 - 699033.00 - 0.00 = -698334.00`. That mismatch remains beside the
explicit zero. No corrected operand is inferred. The
[earlier source investigation](./summary-report-review-2026-09-10.md) owns the
original-image evidence; this command does not claim original-image verification.

For `1833804`, the metadata's three explicit individual-contribution fields
balance exactly. The positive unitemized amount remains an independent metadata
observation despite the unavailable qualified cover. It does not become a
paper-confirmed value, selected funding component, or identified donor amount.

None of these retained cases has both operands and scope for a numeric
unitemized pair. All seven pair outputs therefore have null deltas and explicit
blockers. Numeric positive, zero, negative, equal, and unequal pairs are tested
with schema-valid synthetic fixtures. This is a bounded source gate, not a
corpus-wide completeness or error-rate claim.

## Verification

The official [Form 3X instructions](https://www.fec.gov/resources/cms-content/documents/fecfrm3xi.pdf)
were checked for Column A and line 11(a)(ii). Independent tests verify exact
positions 29–31 and the separate YTD positions against the previously pinned
paper workbook. Both workbook and metadata Swagger hashes must match policy.
Existing [body pins](./fixtures/summary-report-review-2026-09-10.sha256) and
[header pins](./fixtures/report-scope-2026-09-10.sha256) remain the input anchors.

- Full `go test ./...`, `go vet ./...`, and targeted field-review, report-reader,
  and CLI race tests pass.
- Go fixtures cover raw JSON types, explicit zero, signed amounts, null, blank,
  invalid/sub-cent/exponent/whitespace values, subtotal discrepancies, unknown
  layouts, wrong/missing scope, conflicting metadata, tampering, cancellation,
  partial documents, and duplicate covers despite unrelated invalid money.
- Four unitemized command outputs replay byte-for-byte. The same binary also
  reproduces all four retained total-receipts outputs byte-for-byte after the
  shared scope-helper extraction.
- Twelve independent Python checks pass across both field reviews. They verify
  pinned mappings, raw body conservation, original JSON types, reporting periods,
  exact decimal arithmetic, endpoint absence, and all false eligibility guards.
  Ruff passes. Python is audit/test code only.

Go checks run offline with a 4 GiB/four-CPU container cap and 2 GiB Go memory
target. Retained-source checks run offline with 1 GiB/two CPUs and read-only
source mounts. No whole-cycle scan or large temporary data copy is needed.

## Retention and unchanged state

Verification logs, scripts, eight review outputs, code/policy/doc snapshots, and
a verified hash manifest are retained under
`dumps/audits/fec/report-unitemized/2026-09-10/attempt-01/` in project storage.
Original inputs stay in their prior audit directories. The separate review
does not modify them.

The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No processed ledger, fact publication, Arango graph, Dagster definition,
financial membership, or terminal-allocation guard changed.

Next: financial report membership and period/account coverage for the reported
components, before cycle totals or cash-based upstream use. Do not estimate
unitemized contributions from a Schedule A gap.
