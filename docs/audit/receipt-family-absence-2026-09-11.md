# Receipt-family absence gate — 2026-09-11

Result: the additive [Go absence reviewer](../design/receipt-family-absence.md)
qualifies reported zero only when the bound zero, complete original census and
complete processed profile agree. Older detail/comparison outputs remain
unchanged. No financial or terminal eligibility is promoted.

## Inputs and source scope

This gate reuses the [F3 comparison evidence](./receipt-family-comparison-2026-09-11.md),
[complete F3X witness](./receipt-family-witnesses-2026-09-11.md), exact v2 profile,
verified summary publication, retained report metadata and original HTTP captures.
It does not fetch another filing or call an API. Official source URLs and original
digests remain in those linked gates; the unchanged workbook digest is
`9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6`.

Independent checks bind Schedule A/B/C/Text widths and exact filer positions to
that workbook. Only Schedule A leaf-line meanings are accepted here. Other
record layouts distinguish non-SA records; their amounts and financial line
validity are not assessed by the census.

## Observed results

| Case | Qualified reported-zero fields | Blocked family fields |
|---|---:|---:|
| F3 supplied reported span | 23 | 7 |
| Same F3 documents, requested full cycle | 23 | 7 |
| F3 span with one missing cover | 19 | 6 |
| Partial unresolved F3X original | 0 | 4 |
| Complete January F3X original | 2 | 2 |

Rows in this table are diagnostic cases, not disjoint financial populations.
The same F3 field can appear in multiple requested-window cases. No totals are
summed across them.

The five accepted F3 originals qualify 23 fields. Two positive loan comparisons
remain unchanged, not absence-qualified. All five fields in the supplied
superseded original remain blocked despite its complete matching census.
For the complete January F3X original, party contributions and loan receipts
qualify as reported zero. Positive other-committee contributions and affiliated
transfers retain their existing exact comparisons.

The complete originals' Schedule A counts match the profile on every observed
line, including memo populations. This includes the F3X witness's 2,595 Schedule
A records and the accepted F3 original with no Schedule A records at all.
The partial F3X capture remains incomplete; its final unframed record is explicit
issue evidence, not silently added to a known line count.

No date coverage expands. The full-cycle case keeps its unknown dates, and the
missing-cover case keeps its gap. New family-window totals are not emitted.
The prior detail field remains null wherever the old comparator found no detail.

## Verification and retention

Go fixtures cover exact form/line dispatch, unknown aliases, widths, filer
mismatches, incomplete/undecodable/control-byte records, duplicate headers,
positive/negative/blank/unbound covers, profile/original disagreements, memo-only
populations, zero-amount occurrences and positive/negative cancellation.
Changed source pins fail without result output. Cancellation and preservation
tests pass. None of these fixture identities or measured amounts enters runtime
policy.

The five new outputs replay byte for byte. The five older family outputs, four
itemized-window outputs and four summary/window-v2 outputs also replay exactly;
the embedded prior objects are unchanged. Independent checks reconstruct the
original row inventories, field values and profile populations. Full Go tests,
vet, focused race checks, independent Python source tests and Ruff pass. Python
remains an audit/test oracle, not runtime orchestration or calculation code.

Durable evidence lives under
`/storage/dumps/audits/fec/receipt-family-absence/2026-09-11/attempt-01/`.
It contains the bounded outputs, descriptors, timings, source-review log, test
logs, drivers and source snapshot with `SHA256SUMS`. The large profile, existing
original files and compiled binary are referenced by their retained source
identities, not copied into this audit. Explicit case, Go, Python, docs and
retention markers record completion.

The active FEC pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
There was no bulk scan, publication, graph/ArangoDB write, Dagster change,
credential access or operational memory-limit change.

Next: family-specific window coverage and aggregation. Remaining positive
form/role witnesses, wider format support and financial funding acceptance stay
separate gates; these bounded cases do not establish population-wide correctness.
