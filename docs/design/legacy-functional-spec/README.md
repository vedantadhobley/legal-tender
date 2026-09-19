# Legacy Python functional specification

> **Status:** In progress. The source boundary and grain excavation started
> 2026-08-27. This
> directory will describe the existing Python and Dagster system, not the Go
> target and not the desired product.

## Purpose

This specification freezes what the legacy system actually does so the rebuild
can distinguish hard-earned domain behavior from incidental implementation.
It answers "does the current system do this?" without requiring fresh code
archaeology.

For the bounded Python/Go capability comparison and current implementation
priority, see the [connected funding-graph completion plan](../connected-funding-graph.md).
That plan does not replace the unfinished behavioral excavation below.

The specification records behavior even when it appears incorrect. Corrections
belong in the target design and decision log, not in a rewritten description of
legacy reality.

## Evidence order

Use multiple sources and record disagreements:

1. Executed behavior against controlled fixtures or the persisted dataset.
2. Current code and queries.
3. Current generated output and validation scripts.
4. Decision records and dated audits.
5. Narrative implementation documents.

Live or persisted behavior can drift from documentation. Mark the disagreement
rather than selecting whichever source is convenient.

## Observation labels

- `OBSERVED` — verified by execution or direct persisted-data inspection.
- `CODE` — unambiguous from reachable code but not executed during excavation.
- `DOCUMENTED` — claimed by documentation but not yet verified.
- `UNCLEAR` — evidence is incomplete or contradictory.
- `BUG?` — behavior appears defective but is still part of the observed system.
- `DRIFT` — code, runtime state, and documentation disagree.

## Completed topics

- [`system-boundaries-and-sources.md`](./system-boundaries-and-sources.md) —
  source inventory, live persisted state, download/parser behavior, snapshots,
  scheduling, and failure boundaries.
- [`data-grain-and-lineage.md`](./data-grain-and-lineage.md) — stage-by-stage
  map of preserved dimensions, early aggregation, overwrite, filtering, and
  source-lineage loss.
- [`cycle-semantics.md`](./cycle-semantics.md) — source-cycle assignment,
  static active window, per-cycle and cross-cycle calculations, temporal leaks,
  and legacy recomputation behavior.

## Planned topics

The excavation will create focused documents for:

- Candidate, committee, donor, employer, and organization identity behavior.
- Graph vertices, edges, direction, aggregation, and named-graph behavior.
- Committee classification and terminal-source stopping rules.
- Candidate receipts and the five legacy funding channels.
- Independent-expenditure support and opposition behavior.
- Corporate-family and high-dollar-donor resolution.
- Scheduling, retries, partial failure, and recomputation.
- Validation, provenance, known false confidence, and failure cases.
- API, scripts, and current user-visible output.
- The unused lobbying client and unimplemented lobbying design.

Topic files will be linked here only when they exist.

## Required section shape

Each behavior section must contain:

1. Purpose as implemented.
2. Inputs and preconditions.
3. Observable behavior.
4. Output or state mutation.
5. Invariants actually enforced.
6. Failure and retry behavior.
7. Edge cases and ambiguity.
8. Code, data, test, and document evidence.
9. Representative scenario candidates or organizations.

Use exact collection, field, and output names where they are part of observable
behavior. Describe implementation only when it changes the behavior.

## Disposition pass

After the observed specification is complete, a separate disposition register
will classify each behavior:

- `KEEP` — preserve the behavior and add a parity scenario.
- `CORRECT` — preserve the intent but change the semantics or result.
- `REPLACE` — solve the requirement through a different product behavior.
- `REMOVE` — no longer required.
- `UNRESOLVED` — requires product or evidence review.

Every `CORRECT`, `REPLACE`, or `REMOVE` disposition needs an explicit target
contract and validation case. The legacy specification itself remains frozen.
