# Reported fields across a date window

Status: implemented bounded Go calculation. The
[retained-source gate](../audit/report-window-2026-09-10.md) passes. The
[versioned policy](../../contracts/calculations/fec/report-window/v1/policy.json)
defines reported arithmetic, not financial cash acceptance.

## Input and evidence boundary

```bash
legal-tender pipeline fec review-report-window \
  --capture <retained-report-endpoint-capture.json> \
  --documents <document-set.json> \
  --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```

The command verifies one single-committee metadata capture and applies the
existing [observed-chain membership](./report-period-membership.md) and
[electronic field-binding](./report-field-binding.md) rules. It does not accept
saved binding results, caller-selected candidates, or trusted readiness flags.
The metadata population is verified and emitted once, not once per report.
Every supplied document remains in `bindings`, including unbound alternatives.

The [document-set schema](../../contracts/calculations/fec/report-window/v1/documents.schema.json)
contains a version and a `documents` array. Each entry has `source_url`, `body`,
and `headers`; each artifact has `path`, `sha256`, and `bytes`. Relative paths
resolve against the descriptor directory; absolute paths are allowed for
explicitly supplied retained evidence. This is an operator input, not a source
publication or a filesystem confinement boundary. Its exact hash/size is retained.

Limits are 128 KiB for the descriptor, 64 documents, and 16 MiB for the combined
body/header bytes. Existing per-document and metadata limits still apply. An empty
array supports missing-evidence diagnostics; null is not an array. The shared
strict JSON decoder rejects unknown properties, duplicate keys, invalid Unicode,
and excessive nesting. Hash/type/size failures abort, rather than becoming missing
financial fields. Duplicate file IDs abort, including competing captures of the
same filing; this calculation does not choose a preferred representation.

## Membership and reported values

Each of the seven bound fields has its own exact members, missing-candidate
references, interval coverage, blockers, and reported-window readiness.
Missing documents, invalid layouts, source conflicts, and field nulls/blanks are
not zero. Unresolved cohorts and ungrouped observations remain in shared metadata
membership, even when there is no candidate to put in a field's missing list.

| Operation | Fields | Meaning |
|---|---|---|
| Sum whole report periods | Itemized, unitemized, and total individual contributions; total receipts; total disbursements | Independent signed sums. Do not add a total to its own components. |
| Opening boundary | Opening cash | Use the report whose start equals the requested start. Never sum opening balances. |
| Closing boundary | Closing cash | Use the report whose end equals the requested end. Never sum closing balances. |

`observed_sum_minor_units` is available for a sum field when at least one whole
candidate-period value binds. It is not a full-window total or a lower bound:
missing signed adjustments and overlapping periods can change the interpretation.
No operands yields null; explicit bound zero operands can yield zero. Cash fields
never have an observed sum.

`window_value_minor_units` requires all of the following:

1. The observed-chain partition passes for the requested window, including
   unresolved scope, source traversal, gaps, overlaps, and boundary checks.
2. Every intersecting chain candidate has this field bound.
3. Those field members cover every requested day exactly once.

For the first version, cash boundary values also require complete per-period
coverage of that cash field. Intermediate cash observations remain available
in the bindings. A value is still only a reported stock, not available funds.

Cross-boundary reports remain explicit and do not contribute prorated amounts.
Superseded and partial-capture reports cannot become candidate substitutes.
The requested window can cross calendar years or be narrower than an FEC cycle;
no fixed cycle dates, committee IDs, or report IDs select runtime behavior.

## Arithmetic is a separate result

Every equation names its exact observation/field operands and signed coefficients:

- Report and window individual subtotal: total minus itemized minus unitemized.
- Report and window cash: closing minus opening minus receipts plus disbursements.
- Adjacent-period carry-forward: next opening minus previous closing.

Equations return `balanced`, `mismatch`, or `unavailable`, with exact cents and
explicit blockers. Missing operands are never solved from other values. A
mismatch leaves independently bound fields and their reported totals intact.

Cash handoffs use the full chronologically ordered cohort list, not just usable
documents. Only calendar-adjacent candidate cohorts get a handoff check. Missing
documents/fields make that check unavailable; gaps and unresolved cohorts are
not bridged. Competing overlapping or unknown scope blocks the affected handoff,
not unrelated periods. One balanced handoff does not establish window continuity.

`financial_cycle_total_ready`, `cash_basis_ready`, and
`terminal_attribution_eligible` remain false in every result. The calculation
does not qualify account coverage, effective financial report completeness, cash
roles, or terminal origins. The nested source and membership guards are unchanged.

## Operations and next step

Exit zero means a valid diagnostic, including incomplete windows and arithmetic
mismatches. Runtime work stays in Go. The command makes no HTTP calls and does
not scan bulk data, rewrite source values, publish facts, change Arango, or add
Dagster assets. Python tests are an independent offline arithmetic/coverage oracle.

The [v2 reported-span comparison](./summary-reported-span.md) now checks source
ancestry, exact field meaning, and field-specific dates before subtraction.
Late cycle prefixes remain unqualified without blocking matching reported spans.
This is distinct from the pending summary-versus-detail comparison
and from financial funding acceptance. Compact immutable publication and broader
source coverage remain separate; the verbose bounded review is not a recurring
storage format.
