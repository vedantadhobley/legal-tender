# Receipt-family Go comparison gate — 2026-09-11

Result: the additive [report-family comparator](../design/receipt-family-comparison.md)
passes the bounded retained-data gate. It adds contribution, transfer and loan
field bindings and occurrence comparisons. It does not add family window totals,
cash eligibility, financial replacement selection or terminal allocation.

## Inputs and source identity

No source fetch, API request or bulk-body scan ran. The command reused the
[complete v2 profile](./receipt-report-profile-v2-2026-09-10.md), the
[retained originals and metadata](./report-field-binding-2026-09-10.md), and the
[prior itemized-window cases](./receipt-reported-window-2026-09-10.md).

Profile SHA-256:
`acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647`.
The 266,269,624-byte profile preserves all 264,085,633 physical occurrences in
its source-aligned selected relation. Each run revalidated all groups, content
identity, conserved measures and exact summary/release ancestry.

The [reviewed map](../../contracts/calculations/fec/receipt-families/v1/contract.json)
and [new comparison policy](../../contracts/calculations/fec/receipt-family-comparison/v1/policy.json)
bind the runtime subset to official workbook positions and pinned metadata field
types. Form-specific loan, transfer and contribution meanings remain those in
the [F3 instructions](https://www.fec.gov/pdf/forms/fecfrm3i.pdf) and
[F3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf). Source labels do
not establish cash availability or the identity of a loan's principal supplier.

## Retained results

Each case preserves the supplied originals, including unqualified ones. Counts
below are family/report comparisons, not distinct financial transactions.

| Case | Equal | Blocked | New command elapsed |
|---|---:|---:|---:|
| SID exact reported window | 2 | 28 | 6.35 s |
| SID full requested cycle | 2 | 28 | 6.39 s |
| SID missing one cover | 1 | 24 | 6.30 s |
| NRCC unresolved/prefix case | 0 | 4 | 6.30 s |

For SID, all five new fields bind in each of the five qualified originals.
Only two nonempty family populations qualify for numeric comparison: the
candidate-made-or-guaranteed loan observations in reports 1714573 and 1743911.
Each has 100,000,000 cents in its cover, report metadata and nonmemo processed
occurrence subtotal. The original-file witnesses were separately checked in the
[family source audit](./receipt-families-2026-09-11.md). This does not establish
candidate-funded principal or new terminal-source dollars.

The other qualified fields retain reported zeroes but null detail and null
differences when no nonmemo family population is observed. This includes the
complete empty termination report 1780346. Superseded report 1766839 remains
visible and unbound. Unknown or conflicting scope never gains eligibility.

The full-cycle case retains its existing coverage gaps. A missing cover still
removes that report's available comparison. Surviving per-report matches do not
establish a complete window. The F3X prefix/mixed-origin case remains blocked;
it is not a complete real F3X gate.

## Verification

- `go test ./...`, `go vet ./...`, and focused race checks passed.
- 33 independent Python checks passed, including the earlier original/profile
  checks. Python is only the test oracle, not runtime receipt logic.
- Compiled positions match the reviewed map; metadata fields have the exact
  pinned numeric/null endpoint types. Independent checks compare raw workbook
  labels, original byte hashes/cover values, metadata and saved profile groups.
- Every retained report group appears once, either in one family or outside
  the subset. The old profile's `11AI` dispositions remain unchanged.
- Unit tests cover all accepted F3/F3X families, exact form/line matching,
  field-local failures, schema rejection, unknown memo/amount states, signed
  differences, preserved duplicate occurrences and date/individual diagnostics.
- New outputs replay byte for byte. All four earlier itemized-window and all
  four v2 summary/window outputs also replay byte for byte.
- Tampered profile and original-document pins fail with no result output.
  Input-identity tests reject changes between the two field projections.
- Ruff and changed-document link checks passed.

Initial Go runs exposed two test-only mistakes: an incorrect relative contract
path and expecting a field-local result for metadata that the strict capture
reader already rejects. Both were corrected; failed logs remain in the audit.
No source bytes or runtime financial rule was changed to satisfy those tests.

## Retention and unchanged boundaries

Evidence is retained under
`/storage/dumps/audits/fec/receipt-family-comparison/2026-09-11/attempt-01/`.
It contains the four small comparison outputs, descriptors, timings, markers,
verification logs/drivers and focused source/contract snapshots. It does not
duplicate the profile, original archives or executable. `SHA256SUMS` verifies
the retained files; the explicit retention marker records completion.

The active FEC source pointer remains SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No graph, Dagster asset, source publication, recurring trigger or cash/terminal
eligibility changed.

Next: obtain positive complete-original committee-contribution and transfer
witnesses and a complete F3X witness, using retained evidence first. Qualify
family-specific absent-detail and field/window coverage before adding new
window or summary differences. This gate does not require another bulk scan.
