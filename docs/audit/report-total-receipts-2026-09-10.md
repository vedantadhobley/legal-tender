# Exact report-period receipts comparison — 2026-09-10

Status: bounded Go implementation, complete Go checks, retained-case replay,
and independent field/output verification pass. This qualifies one reported
comparison, not an effective funding component. The
[design](../design/report-total-receipts-comparison.md) owns the accepted boundary.

## Observed result

The command reuses four existing report captures and two validated metadata
capture descriptors. All seven exact-file metadata matches remain separate.

| File | Reporting interval | Result |
|---|---|---|
| `1813890` | 2024-04-01 through 2024-06-30 | Cover lines 6(c) and 19 both report $699.07; the exact-file filings assertion also reports $699.07. One qualified pair, delta zero. |
| `1876290` | 2024-09-01 through 2024-09-30 | Both cover totals are blank. Both metadata amounts are explicit zero; both pairs remain blocked with null deltas. |
| `1882886` | 2024-09-01 through 2024-09-30 | Same blank-versus-zero boundary; both pairs remain blocked. |
| `1833804` | Not assigned by the current cover reader | Both metadata amounts survive, but electronic 8.4 remains unqualified and the capture is only a prefix. Both pairs have null deltas. |

The positive case retains its known itemized-individual transcription discrepancy.
Its two year-to-date total-receipts positions also remain distinct: `69910.00`
and `699.10`. Neither is substituted into the report-period value. These raw
differences are visible in retained evidence; this gate does not repair them,
select an effective report, or assert that the entire report is accurate.

The [prior original-report audit](./summary-report-review-2026-09-10.md) owns
the image review and broader discrepancy explanations. Equality here establishes
agreement between two correlated FEC representations, not independent source
confirmation. Original-image, history, financial-component, cycle-comparison,
and terminal-allocation eligibility remain false.

## Source qualification and verification

The official [Form 3X instructions](https://www.fec.gov/resources/cms-content/documents/fecfrm3xi.pdf)
were consulted for Column A, line 19, line 6(c), and the distinction from federal-
only receipts. The previously pinned paper P3.4 workbook maps the two period
positions. The retained metadata schema and
[OpenFEC filing model](https://raw.githubusercontent.com/fecgov/openFEC/develop/webservices/common/models/filings.py)
identify the processed total-receipts field. No new report or API response was
captured; no bulk source was scanned or extracted.

Input body/header digests are checked against the existing
[original-report pins](./fixtures/summary-report-review-2026-09-10.sha256) and
[complete-header pins](./fixtures/report-scope-2026-09-10.sha256).
Each metadata capture is revalidated by the existing Go reader. Every original
record and its byte locator survives in the nested evidence. Repeated command
invocations are byte-identical at the same input paths.

The complete Go suite and vet pass, including unchanged existing calculation
tests. Focused report-field, source-reader, and CLI race tests pass. Fixtures
test explicit zero, negative amounts, null/blank/invalid/sub-cent values, missing
and contradictory cover totals, invalid dates, wrong metadata scope, missing
required assertions, another file with equal dates, unsupported endpoints,
partial bodies, tampered inputs, duplicate covers, and signed difference
overflow beyond int64. Valid pairs survive unrelated cover-value and amended/
latest-status conflicts without changing or discarding those assertions.

Six new independent Python checks validate the policy guards, exact workbook
field/line mappings, retained scope, every pair's raw and typed amount, raw-body
conservation, and the positive/blocked outcomes. Together with the existing
source-policy check, seven tests pass; five source-reader corpus tests are
intentionally skipped in this invocation because their separate audit path
was not supplied. They passed in the preceding report-scope gate. Ruff passes.
Python is audit/test code only; the runtime calculation is Go.

## Retention and unchanged state

Verification logs, command driver, outputs, code/policy/doc snapshots, and hash
manifest are retained under
`dumps/audits/fec/report-total-receipts/2026-09-10/attempt-01/` in project storage.
Original data stays in its prior audit directories. Go checks used a capped
4 GiB/four-CPU container with a 2 GiB Go memory target; independent checks used
1 GiB/two CPUs. Both data-check containers had no network and read-only sources.

The source release pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No source/fact publication, processed transaction change, Arango write, Dagster
asset, scheduled job, or financial eligibility change occurred. The existing
cycle-summary/receipt readiness-v1 deltas remain null.

Next: apply the field-local scope pattern to the required unitemized-receipt
component, preserving unknown donor composition and unresolved financial
membership. Do not infer it from a detail gap or a difference in totals.
