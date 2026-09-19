# Remaining receipt-family comparison gate — 2026-09-11

Scope: implement the remaining mapped F3/F3X non-individual Schedule A receipt
categories without treating thresholded detail as a complete reported total.
The [shared Go comparator](../design/receipt-family-comparison.md) adds an opt-in
`--comparison-version v2`; v1 and its downstream consumers remain unchanged.

## Method and source meaning

Use the existing [reviewed source map](../design/receipt-families.md), pinned
electronic 8.4 workbook and retained report-metadata schema. The official
[F3 instructions](https://www.fec.gov/pdf/forms/fecfrm3i.pdf) distinguish candidate
contributions, offsets and other receipts with thresholded detail. The
[F3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf) distinguish loan
repayments and contribution refunds received from thresholded offsets/other
receipts. These source relationships, not observed amounts or entity IDs, drive
the [v2 policy](../../contracts/calculations/fec/receipt-family-comparison/v2/policy.json).
No new threshold filter or runtime remote-schema lookup was added.

V2 extends both form maps to all remaining reviewed non-individual SA leaves.
Required-itemized fields retain the existing scoped difference. Thresholded
detail has `component_not_comparable` with a null difference, even if its amount
equals the reported total. Missing/unknown detail, memo conflicts, unbound fields
and report-scope failures retain their blockers and underlying evidence.

F3 offsets use the exact detailed metadata field and both repeated period cover
positions 28/44. F3X uses its own field/position. There is no alias fallback.
Metadata string acceptance now uses exact pinned field names rather than an
`individual` substring. Tests verify permitted types against the source schema.

## Retained inputs and execution

Durable attempt:
`/storage/dumps/audits/fec/receipt-families-v2/2026-09-11/attempt-01/`.
The local verification workspace was `/tmp/legal-tender-family-v2.UyWPp7`.
Use the retained drivers with the same read-only `/storage`, `/metadata`,
`/reports` and `/profile` mounts and a fresh writable `/audit` directory.

- Summary fact set:
  `603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637`.
- Saved-profile SHA-256:
  `acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647`.
- Profile ID:
  `a84e5ad04a315a2dfedc575ac44aecbba8c816b8265a76b2a9a69cc810b7dcc7`.
- Active source-pointer SHA-256, unchanged:
  `b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

The complete saved profile was reverified, not the processed bulk body rescanned.
Existing original/metadata captures were reused. Comparison execution made no
network request and used no API token, new raw capture, publication, service
update or database write.
Verification containers were offline with explicit CPU, memory and PID caps.

## Observed results

Each case ran v2 twice and replayed the prior family-window and family-summary
commands byte-for-byte. Each four-run case took about 29 seconds; this includes
repeated full saved-profile verification, not only comparison arithmetic.

| Retained case | Equal required-itemized fields | Thresholded components | Blocked fields |
|---|---:|---:|---:|
| SID bounded window | 2 | 0 | 46 |
| SID requested full cycle | 2 | 0 | 46 |
| SID missing cover | 1 | 0 | 39 |
| NRCC unresolved original | 0 | 0 | 8 |
| NRCC January | 2 | 1 | 5 |

These are report-level diagnostics, not accepted full-window totals. The wider
SID request keeps its uncovered dates; report-level pairs cannot fill that gap.

In January filing 1690269, `other_federal_receipts` preserves a bound reported
total of $503,536.15 and nonmemo processed detail of $483,585.28. The relation is
thresholded, so the numeric difference stays null. This does not label the gap
unitemized, establish missing receipts, or infer a source of funds.

F3 filings 1743911 and 1780310 preserve positive other-receipt totals of $1.00
and $0.37 with no nonmemo detail. Both keep null detail/difference and the explicit
absence blocker. Explicit zero fields also do not manufacture detail. Existing
cash discrepancies, blanks, superseded reports and unresolved chains remain.

## Verification and limits

- Full Go tests, `go vet`, affected-package race checks and build passed.
- Table-driven tests cover every v2 field with matching/different/signed values,
  empty and unknown detail, unknown/memo codes, field/scope failures, exact line
  and schedule routing, and changed file/committee/year identifiers.
- Tests prevent F3X 11D from becoming F3 candidate contributions and reject
  unsupported form/relation fallback. All profile groups remain assigned once.
- Independent tests verify pinned workbook labels/positions, raw original-byte
  hashes, metadata/cover binding, saved-profile ancestry, exact grouped amounts,
  and preservation of earlier field results. They are audit-only Python, not
  production domain code.
- All 36 enabled independent checks and Ruff passed. The first audit run lacked
  the prior tests' `/reports` mount and used a pluralized test expectation for
  the workbook's singular `Offset` label. Both harness errors were corrected;
  the initial failure log is retained. Neither required a runtime or source edit.
- Ten prior window/summary command outputs replayed byte-identically. Five v2
  replays were identical. Changed original/profile pins failed without output.

These retained reports do not supply positive bound examples for every newly
added family. Synthetic counterexamples cover the general behavior; wider
positive original/profile/metadata witnesses remain a source gate. The January
thresholded component is a real bound example, not universal validation.

New-family absent-population, date-window and summary qualification are not
enabled by this report-level version. Full financial report membership, cash
availability, F3P and terminal allocation remain separate gates. All five v2
financial/publication readiness guards stay false. The retained checksum index,
test logs, source snapshot and explicit exit markers are the execution record.
