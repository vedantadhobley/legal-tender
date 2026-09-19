# Go report-metadata reader gate — 2026-09-10

Status: local Go reader and CLI gate pass. The separate OpenFEC metadata-source
direction is accepted; HTTP capture and recurring activation are not implemented.
No money, financial selector, bulk source/fact pointer, graph, or Dagster asset
changes. The [reader contract](../design/report-metadata-reader.md) defines the
implemented scope and the next bounded acquisition step.

## Exact evidence

This gate reuses the [qualified source captures](./receipt-report-metadata-2026-09-10.md),
not new API requests or original filings. The fixture digests bind both response
bodies and HTTP headers. New credential-free descriptors identify each query and
use the retained HTTP Date as `time_basis=http_date`; they do not invent client
capture timestamps or relabel the data as a new FEC snapshot.

| Endpoint | Retained query | Preserved occurrences |
|---|---|---:|
| `/v1/filings/` | Seven exact audited file IDs | 7 |
| `/v1/reports/house-senate/` | C00843367, cycle 2024 | 10 |
| `/v1/reports/pac-party/` | C00075820, cycle 2024 | 34 |

All 51 observations pass the reader. These are overlapping endpoint populations,
not 51 distinct filings. Each response remains its own capture. All three report
`exact_count_satisfied`, not an observed terminating empty page or a complete
amendment history. Both financial/history readiness guards remain false.

The output preserves the known conflicts for files 1833804 and 1882886, the
negative paper predecessor, number-versus-string chain members, and the
zero-itemization report's `"0.00"` string. The strict shape map accepts nine
specific observed numeric-string report fields and explicit report-chain element
types; it does not accept arbitrary new fields or normalize money.

## Gates

The Go tests check source-schema/compiled-map agreement, exact source identities,
all-row retention, duplicate file numbers, missing fields, unreviewed types,
query mismatches, page gaps, changed counters, approximate/empty-page semantics,
path/symlink confinement, size limits, invalid JSON/Unicode, duplicate object
keys, cancellation, and credential-field rejection. These are synthetic boundary
tests, not named production exceptions.

The complete Go suite and vet pass. Focused reader and CLI race tests pass.
All three actual CLI executions and byte-identical replays return exit 0. The
source/result-schema and [independent readback tests](../../tests/test_report_metadata_reader.py)
pass ten checks, including all original field values, JSON representations,
per-object byte hashes, row ordinals, lookup IDs, descriptor identities, header
digests, and unchanged readiness guards. Ruff and whitespace checks pass.

No new Go dependency is required. Build/test tools ran in a network-disabled
container capped at 4 GiB, with a 2 GiB Go memory target and four CPUs. Independent
tests used 1 GiB and two CPUs. Python remains test-only here; runtime reading is Go.
The full transaction corpus was not opened or scanned.

## Retention and limits

Evidence is retained in project storage under
`dumps/audits/fec/report-metadata-reader/2026-09-10/attempt-01/`: the three
source bodies/headers, capture descriptors, CLI results and replays, source and
contract snapshots, test logs, explicit success markers, and verified tree
digests. The build binary is regeneratable and is not retained with the audit.
The post-review source-pointer digest remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

This establishes local parsing and assertion preservation, not transport capture,
snapshot consistency, reporting-scope coverage, effective amendment selection,
or full-history cost. Next implement bounded Go HTTP capture with credential-safe
request metadata and explicit failure/retry records, then validate one small
scope. Old-report refresh and family closure precede history-wide activation.
