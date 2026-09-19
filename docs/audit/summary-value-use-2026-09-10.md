# Summary-value use policy gate — 2026-09-10

Status: design and regression-test gate passes. The
[accepted use policy](../design/summary-value-use.md) distinguishes reported
observations, arithmetic, scoped comparisons, funding components, and allocation.
No runtime selector, API command, source correction, or eligible funding component
was added. Existing wire versions and financial guards remain unchanged.

## What changed

Current docs now state that missing report history and disputed summary values
are not additional dependencies of the accepted bulk observation graph. Source-
specific counting and integrity checks still apply. The pending financial work
starts with a source-located assessment of the retained attachment case, not
history-wide acquisition or a reconstruction of processed Schedule A amendments.

The policy distinguishes a conflict in one scalar or operand set from a problem
with every field for that committee. A reported value can remain visible with a
warning while its use in an allocation remains blocked. A balanced equation is
not financial qualification; an unknown donor is not necessarily an unknown
amount. No source amount was selected or corrected by these rules.

## Verification

New [Go regressions](../../internal/calculation/fec/fundingbasis/summary_use_test.go)
exercise the existing review with synthetic cash and individual discrepancies
and a scalar/contact conflict. They establish:

- A changed summary assertion changes review identity but not receipt measures,
  receipt input identity, or preserved source membership.
- Cash and individual arithmetic warnings stay in their respective field families.
- Scalar/contact conflicts do not become invented reporting-scope conflicts.
- Both conflicting variants remain available; no field is repaired or erased.
- Inputs are not mutated and no comparison, funding, or terminal guard is promoted.

The [independent retained-case tests](../../tests/test_summary_value_use.py) pin
the exact five earlier readiness-review artifacts by SHA-256 and check their
wire contract, reported fields, diagnostics, local blockers, and unchanged
eligibility. The cases cover the attachment discrepancy, inter-report cash gap,
paper-transcription mismatch, arithmetic-equal control, and absent-input state.

The earlier [original-report source gate](./summary-report-review-2026-09-10.md)
was rerun against retained files with networking disabled. All three source cases
pass. The test verifies pinned original bytes and exact numeric relationships;
the previous PDF interpretation remains a documented visual review, not new OCR.

Focused Go tests, the full Go suite, vet, and focused race tests pass. Independent
Python verification passes seven tests; one older full-inventory corpus opt-in
is skipped because that large backing input is not needed for this bounded gate.
All five new retained cases ran. Ruff and whitespace checks pass.

Go tests used a 4 GiB container cap, 2 GiB Go memory target, and four CPUs.
Independent checks used 1 GiB and two CPUs. No full Schedule A scan, API request,
new source download, database access, or service deployment occurred.

## Retention and limits

New test logs, explicit success markers, code/document snapshots, old witness
digest manifests, and verified tree hashes are retained under
`dumps/audits/fec/summary-value-use/2026-09-10/attempt-01/` in project storage.
Original report captures and earlier readiness results remain at their existing
audit locations; they were read-only inputs, not overwritten or recopied.
The active source-pointer SHA-256 remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

This gate does not prove graph-wide invalidation behavior or approve a funding
denominator. It verifies the current local review and records the intended
consumer boundary. Next implement the bounded attachment/report assessment
with exact evidence; identifying an attachment must not automatically select an
older report or change processed transactions.
