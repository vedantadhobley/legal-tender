# Receipt-family window gate — 2026-09-11

Result: the additive [Go window command](../design/receipt-family-window.md)
aggregates the accepted receipt families with separate reported and comparison
coverage. Complete retained spans qualify; missing days/covers remain blocked.
Old results are unchanged, including null absent detail and financial guards.

## Retained source evidence

The gate reuses the exact [absence evidence](./receipt-family-absence-2026-09-11.md),
including its original/metadata bindings, qualified reported zeroes and complete
v2 processed profile. Source digests and original capture links remain in that
audit and the linked F3/F3X gates. The accepted profile represents 264,085,633
physical rows; this run verifies the retained derived profile, not the bulk body.

The same five requests use the original path aliases so old command outputs can
be compared byte for byte. No source artifact is fetched, refreshed or rescanned.
Metadata captures keep their own identities and are not relabeled as part of a
new coordinated bulk release.

## Measured cases

| Requested case | Qualified family windows | Blocked family windows | Field coverage gap |
|---|---:|---:|---:|
| F3 April 2023–April 2024 reported span | 5 | 0 | 0 days |
| Same F3 documents, full 2024 cycle | 0 | 5 | 335 days |
| F3 span with one missing cover | 0 | 5 | 92 days |
| Unresolved partial F3X report | 0 | 4 | 30 days |
| Complete F3X January 2023 report | 4 | 0 | 0 days |

These cases overlap and must not be summed as disjoint financial populations.
All qualified window differences are zero in this retained sample; fixtures also
exercise nonzero and cancelling differences. Equality is not a financial accuracy
certificate or proof of unique/effective transactions.

The F3 span combines two separately reported $1,000,000 candidate-made or
guaranteed loan amounts into a $2,000,000 reported/comparison window. The other
four reviewed F3 families have qualified reported-zero observations throughout.
This is not an attribution of loan principal to the candidate. The five fields
from the supplied superseded filing remain outside window membership.

The complete F3X January window retains $1,731,000.00 in other-committee
contributions and $548,487.72 in affiliated/party transfers, with qualified
reported zero for party contributions and loan receipts. Memo populations and
receipt dates outside the period remain in the nested evidence; they are not
clipped to manufacture the subtotals.

The full-cycle and missing-cover cases retain observed sums, but their full-
window values and differences are null. The missing candidate-cover entry remains
explicit in every family. The unresolved F3X case produces no inferred zero.
No family summary comparison, cash basis or terminal-dollar attribution is added.

## Verification

Go fixtures cover both accepted forms, exact family dispatch, missing and empty
document sets, absent candidates, gaps, overlaps, cross-boundary periods,
unresolved cohorts, partial metadata, field-local failures, unqualified detail,
outside/superseded exclusion, cancellation, signed adjustments and sums beyond
int64. Per-report mismatches remain visible even when they cancel in a window.
No fixture IDs, dates or amounts select runtime behavior.

Each new output replays byte for byte. Five absence results, five family results,
four itemized-window results and four summary/window-v2 results also replay
exactly. The nested absence object equals its prior retained output. Tampered
original/profile pins fail without result output.

Independent Python checks verify original hashes/records, cover and metadata
operands, exact occurrence-group sums, qualified-zero bases, candidate membership,
every requested day's multiplicity, partial sums, full-window readiness and exact
differences. The preceding absence and complete F3X source tests also run. Python
is an offline test oracle; runtime behavior remains Go.

Full Go tests, vet, focused race checks, independent source tests, Ruff and local
documentation-link checks pass. All test containers are offline and capped.
Operational services and their memory limits are unchanged.

## Retention and next step

Evidence lives under
`/storage/dumps/audits/fec/receipt-family-window/2026-09-11/attempt-01/`.
The bounded outputs, descriptors, timings, drivers, logs and source snapshot are
covered by `SHA256SUMS` and explicit case/Go/Python/docs/retention exit markers.
Existing originals, the large profile and the compiled binary are referenced,
not duplicated into the durable audit.

The active FEC pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
There was no bulk scan/download, source or calculation publication, ArangoDB
write, Dagster activation or credential access.

Next: accept exact family-to-summary field/date mappings before adding summary
differences. Remaining positive form/role witnesses and financial scope remain
separate; this bounded gate does not establish full-population correctness.
