# Electronic report-field binding gate — 2026-09-10

Status: bounded implementation and retained-source gate pass. The
[design](../design/report-field-binding.md) owns current behavior and limits.

## Results

Seven retained electronic representations were read offline. No original filing,
metadata response, or transaction dump was downloaded for this gate.

| Retained files | Result |
|---|---|
| `1714573`, `1766866`, `1743911`, `1780310`, `1780346` | Five observed-chain candidates bind all seven period fields: 35 exact reported-value bindings. |
| `1766839` | All seven reported pairs match, but the superseded observation is not a chain candidate. No field binds. |
| `1833804` | Its complete visible cover has seven exact reported pairs. The prefix capture and unresolved mixed-origin cohort block all bindings. |

The first six are complete F3 8.4 documents. The last is a 16,384-byte prefix
of a 19,020,241-byte F3X 8.4 document. The reader retains the complete header
and cover without claiming the rest of that document was inspected.

The five bound candidates cover the previously verified April 2023–April 2024
window, not the complete 2024 election cycle. Source amounts are unchanged.
In particular, the closing cash reported in
[filing 1780310](https://docquery.fec.gov/dcdev/posted/1780310.fec) and the opening
cash in [filing 1780346](https://docquery.fec.gov/dcdev/posted/1780346.fec) retain
the previously observed $1,500,000 downward discontinuity. The independent test
reproduces it from the bound fields. Matching metadata does not repair that
difference or promote cash eligibility.

## Schema evidence

The existing schema capture held version 8.5, while these reports declare 8.4.
The gate captured one bounded range from the official
[electronic-format archive](https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/electronic/eFilingFormats.zip).
It reused the retained central directory and verified the range against the
same publisher ETag and full-object length.

- Range: `10448130–10857261`, HTTP 206, 409,132 captured bytes.
- Member: `eFilingFormats/FEC_v8x/FEC_Format_v8.4.xlsx`.
- Compressed member: 408,620 bytes; extracted workbook: 583,496 bytes.
- Workbook SHA-256: `9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6`.
- ZIP member name, local-header flags/method, bounded decompression completion,
  extracted size, and CRC all pass. No full archive extraction was needed.

The [new pins](./fixtures/report-field-binding-2026-09-10.sha256) cover the
range, headers, workbook, and newly qualified original-response headers. Body
pins remain in the [original report audit](./fixtures/summary-report-review-2026-09-10.sha256).
Metadata remains the existing complete retained captures; no API key was used.

## Verification and retention

- Full Go tests and vet; affected reader/calculation/CLI race tests.
- Fixtures cover F3/F3X layouts, exact raw framing, optional comments, unknown
  versions/forms/bytes, short/extra rows, duplicate covers/headers, invalid dates,
  null/blank/zero amounts, fractional cents, wrong amendment roots, bad sequence
  shapes, superseded candidates, field-local conflicts, prefixes, tampering,
  cancellation, and window boundaries.
- Ten independent Python checks validate the actual workbook positions, every
  retained report byte/locator, every cover amount including column B, complete
  metadata artifact pins, all seven field mappings/pairs, binding exclusions,
  and the unchanged cash discrepancy. Python is audit-only.
- Seven CLI invocations replay byte-identically. Twelve previous unitemized,
  total-receipts, and period-membership results are byte-identical after the
  shared byte-framing refactor. Ruff and formatting checks pass.

Retained evidence belongs at
`/storage/dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/`.
It includes the bounded schema capture, extracted workbook, results, gate logs,
drivers, and source/contract/test snapshot with `SHA256SUMS`. Original report and
metadata bytes remain in their earlier audit roots; they are not duplicated.

The active FEC release pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No processed facts, transaction amounts, graph projections, or orchestration
state change. Next is window-level field membership/aggregation with separate
financial-use checks, not a new corpus scan or automatic filing downloader.
