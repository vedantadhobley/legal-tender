# Same-report total-receipts comparison

Status: implemented manual Go calculation. The
[retained-case gate](../audit/report-total-receipts-2026-09-10.md) qualifies one
field and reporting interval for a numeric **reported-value comparison**.
It does not qualify a cash funding component, an effective cycle total, or
terminal-dollar allocation. This is the next implemented use under the
[summary-value policy](./summary-value-use.md), not a change to the existing
cycle-summary/receipt readiness command.

## Exact question

For one retained Form 3X filing, do its paper-transcribed report-period total
receipts agree with the corresponding captured processed-metadata assertion
for the **same file, filer, form, report code, and reporting interval**?

This compares correlated publisher representations. Agreement is not independent
confirmation of accuracy, source-image fidelity, complete disclosure, effective
report status, or available cash. Different monetary assertions remain separate;
none wins by being newer, larger, or more convenient for an equation.

## Qualified field mapping

The [versioned policy](../../contracts/calculations/fec/report-total-receipts/v1/policy.json)
defines `fec.form3x.total_receipts.column_a`. The FEC instructions distinguish
report-period Column A from calendar-year-to-date Column B, transfer line 19
to line 6(c), and distinguish total receipts from federal-only receipts on
line 20. Total receipts include several receipt families, not only donations.
See the [official instructions](https://www.fec.gov/resources/cms-content/documents/fecfrm3xi.pdf),
pages 4, 6, and 7.

| Representation | Exact field |
|---|---|
| Pinned paper P3.4 Form 3X, Column A line 6(c) | One-based cover position 22. |
| Same cover, Column A line 19 | Position 44; both explicit totals must be valid and equal. |
| Captured `/v1/filings/` record | `total_receipts`, with `form_type=F3X`. |
| Captured `/v1/reports/pac-party/` record | `total_receipts_period`, with `report_form=Form 3X`. |

The paper workbook pins field positions and types. The retained Swagger and
[OpenFEC filing model](https://raw.githubusercontent.com/fecgov/openFEC/develop/webservices/common/models/filings.py)
identify the processed assertion; exact-file witnesses test the mapping. No
runtime schema download changes this policy. House/Senate endpoint records and
unqualified electronic layouts cannot supply this Form 3X comparison.

Positions 75 and 94 are separate year-to-date totals. Position 45 is federal-only
receipts. They are neither alternatives nor missing-value fallbacks. The period
comes from valid full cover dates and must match metadata dates; no quarter,
cycle, or report-code inference supplies missing dates.

## Verification and field-local gates

The calculation invokes the [report-scope reader](./report-scope-assessment.md)
on pinned source inputs each time. It accepts no saved assessment JSON or
caller-provided eligibility flags. Exact bodies, headers, records, raw fields,
and metadata ancestry remain in the output.

Require a complete captured response, qualified paper cover layout, valid
filer/period, and one financial cover. Require both mapped cover amounts to be
explicit, valid, and equal. A blank or contradictory total blocks the pair;
the calculation does not choose one cover position. All other amounts remain
in the retained evidence.

Each metadata record is assessed separately. The exact file-number join is
performed by the source reader. Committee, form, report code, paper origin,
cover amendment indicator, and both dates must match. A missing required value
blocks comparison. An explicitly conflicting `fec_url` also blocks it; a null
URL does not override the exact file/filer/form/period join. Date-only and
midnight-without-zone representations are accepted. Non-midnight timestamps
are not clipped to dates.

The cover's new/amended/termination indicator identifies the represented form;
it is distinct from `is_amended`, `most_recent`, predecessor, and chain
assertions about financial selection. Those latter assertions remain visible
but do not select or disqualify a same-file reported-value pair. Matching dates
never joins an older or newer file.

Invalid or conflicting **other** fields do not manufacture a conflict in this
field. Cash continuity, individual subtotal discrepancies, and year-to-date
differences stay in the source evidence and prior diagnostics. They still
matter to financial uses that depend on them. This calculation does not compute
or clear every possible arithmetic warning in a report.

## Output and command

```bash
legal-tender pipeline fec compare-report-total-receipts \
  --source-url https://docquery.fec.gov/paper/posted/1813890.fec \
  --body <retained-body> --body-sha256 <expected-sha256> \
  --headers <retained-headers> --headers-sha256 <expected-sha256> \
  --metadata-capture <retained-capture.json>
```

The command uses the same bounded input flags as the source reviewer. JSON goes
to stdout. Exit zero means the review is valid, even if all pairs are blocked.
No matching metadata means no comparison, not zero receipts. Every matching
metadata occurrence remains a separate pair, including contradictory amounts
or scopes. There is no selected aggregate across pairs.

`delta_minor_units` is **metadata minus cover**, emitted only for a qualified
pair. It is an arbitrary-precision signed integer string over checked source
cents. A nonzero difference remains a diagnostic, not a repair. Blank, null,
invalid, and explicit zero states remain distinct; blocked pairs have null
deltas, never invented zeroes.

`financial_component_eligible`, `cycle_comparison_ready`, and
`terminal_attribution_eligible` always remain false. The nested source assessment
also retains its false history/image/financial-selection guards. `comparable`
applies only to this exact reported pair.

## Remaining boundary

The cycle-summary-versus-Schedule-A comparison is still unqualified. This command
does not establish which reports contribute to a cycle summary or which reports
are financially effective. Neither the accepted processed transaction rules nor
their graph projections change.

The [unitemized-receipts review](./report-unitemized-receipts.md) now applies
this pattern to explicit observations and separate subtotal diagnostics.
Do not qualify a complete cash basis merely by adding more matching scalars.
Effective report membership, component completeness, and cash timing require
their own evidence. Recurring publication and Dagster activation remain separate.
