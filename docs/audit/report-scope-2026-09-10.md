# Retained report-scope gate — 2026-09-10

Status: bounded Go implementation and offline retained-case verification pass.
The [assessment contract](../design/report-scope-assessment.md) describes document
shape without selecting effective reports or correcting processed money.

## Result

| File | Captured bytes | Result | Important limit |
|---|---:|---|---|
| `1876290` | 601 | Supplemental-attachment shape; all 100 cover amount fields blank; one Schedule C-1. | Transcription evidence, not independent original-image confirmation. |
| `1882886` | 614 | Same supplemental-attachment shape. | The earlier manual image review supports this case; the runtime does not perform OCR. |
| `1813890` | 1,114 | Populated financial cover; all source values retained. | Known image/transcription disagreement remains; presence is not accuracy or financial eligibility. |
| `1833804` | 16,384-byte prefix | Explicitly unresolved, with exact complete/incomplete record locators. | Electronic 8.4 cover layout is not qualified by this paper reader; the full response is 19,020,241 bytes. |

The four bodies total 18,713 bytes and conserve 78 physical records. The
existing metadata reader revalidates two saved capture descriptors for each
case. Seven matching records across the assessments retain complete raw values
and capture/page/record ancestry. No endpoint status wins over another.
Repeated CLI executions produce byte-identical output at the same input paths.

The two attachment cases retain the literal paper cover blanks alongside the
processed report endpoint's zero-valued period cash/receipt/disbursement fields.
They are not converted into zero financial replacements. Negative/self previous-
file references and contradictory amended/latest assertions remain evidence,
not a repaired chain. Literal date/timestamp differences are distinguished from
the qualified date-level cover comparison.

## Independent evidence and checks

Source bodies and the electronic-prefix headers match the existing
[original-report digest fixture](./fixtures/summary-report-review-2026-09-10.sha256).
The three small complete-response header digests are newly pinned in this
[gate fixture](./fixtures/report-scope-2026-09-10.sha256); their retained bytes
were read, not fetched again. The paper workbook matches the prior
[metadata qualification fixture](./fixtures/receipt-report-metadata-2026-09-10.sha256).

The independent Python test reads that workbook directly and checks every
qualified amount position, identity/date field, header unused-ID assertion,
and Schedule C-1 field count. It then independently reconstructs every body
from the Go result's raw records, checks locators/digests, compares all cover
fields and exact monetary values, checks metadata membership/differences, and
requires all three eligibility guards to remain false. Six tests pass.

The Go suite covers blank versus zero, negative amounts, non-money columns,
bad/sub-cent money, unknown encodings/headers, short/extra fields, invalid loan
records, wrong filer, invalid/reversed dates, mixed schedules, multiple covers,
missing supplements, truncated prefixes, pin/framing/budget failures, metadata
conflicts, changed metadata bytes, and deterministic replay. The complete Go
suite and vet pass; focused reader/CLI race tests pass. Ruff passes. An initial
Python test-import error was fixed before the independent gate passed.

The saved [original attachment image](https://docquery.fec.gov/pdf/634/202410240300487634/202410240300487634.pdf)
was visually rechecked across all four retained rendered pages: cover memo,
Schedule C-1, and mailing evidence, without a replacement Form 3X summary.
This is manual source review for `1882886` only. It is not a named runtime rule
or an assertion that the other paper file's original image was verified.
The [prior report investigation](./summary-report-review-2026-09-10.md) owns the
monetary discrepancy analysis; this gate makes no new corrected-value claim.

## Retention and unchanged state

Audit outputs, commands, source/test snapshots, verification logs, and hash
manifest are retained under
`dumps/audits/fec/report-scope/2026-09-10/attempt-01/` in project storage.
Original bodies and schemas remain in the prior immutable audit directories;
this gate references their digests instead of duplicating those sources.

Go verification used a 4 GiB cap, 2 GiB Go memory target, and four CPUs. The
independent test container used 1 GiB and two CPUs. Both used read-only source
mounts and no network. No bulk scan, extraction, API capture, graph write,
Dagster registration, or source/fact pointer update occurred. The active v4
pointer remains `b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

The next step is one field/interval qualification, not a general amendment
selector or a new prerequisite for processed transaction queries.
