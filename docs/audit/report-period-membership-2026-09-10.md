# Report membership and interval gate — 2026-09-10

Status: bounded Go implementation, full Go checks, retained-case replay, and
independent source/day verification pass. The
[design](../design/report-period-membership.md) defines current behavior.

## Observed results

The gate reuses the previously retained 34-record PAC/Party capture and
10-record House/Senate capture. No new API/report data was fetched. Four runs
use explicit windows to distinguish cycle coverage from narrower period coverage.

| Capture/window | Scope cohorts | Chain candidates in capture | Uncovered days in requested window | Observed partition ready |
|---|---:|---:|---:|---|
| NRCC, 2023-01-01 through 2024-12-31 | 24 | 23 | 30 | No |
| NRCC, 2023-01-01 through 2023-12-31 | 24 | 23 | 0 | Yes |
| SID, 2023-01-01 through 2024-12-31 | 5 | 5 | 335 | No |
| SID, 2023-04-01 through 2024-04-30 | 5 | 5 | 0 | Yes |

All four chain timelines have zero overlapping days. Cohort/candidate counts
refer to the complete retained capture, not just the requested window. Every
source row remains a cohort member; no raw amendment occurrence is discarded.

The NRCC publisher-selected list has 24 records and no calendar gap. Its
September 2024 cohort contains electronic `1833804` and paper `1876290` and
`1882886`. Only the last record has `is_amended=false`, but missing paper chains
and mixed origin prevent a chain candidate. The diagnostic does not substitute
the older electronic filing or promote the attachment's processed zero. That
30-day unresolved interval blocks the full-cycle partition, not the independent
2023 window. The [original-report audit](./summary-report-review-2026-09-10.md)
owns the cover/image findings; no original image is parsed by this new command.

SID's five candidates cover April 2023 through April 2024. January–March 2023
and May–December 2024 remain unrepresented in the requested full-cycle window.
The termination report is included as an observed report, including its zero-
Schedule-A scope, but does not imply future zero reports. These gaps are not
a finding that reports were legally required or missing.

The narrower SID partition passes while the earlier audit's cash carry-forward
discrepancy still exists. This is intentional: date coverage and source-chain
consistency do not prove financial continuity. Financial membership and cycle
total readiness remain false in **every** output; no money is aggregated.

## Verification

The [FEC amendment guidance](https://www.fec.gov/help-candidates-and-committees/filing-amendments/)
and [OpenFEC report endpoint](https://raw.githubusercontent.com/fecgov/openFEC/develop/webservices/resources/reports.py)
were consulted to keep paper/electronic amendment semantics and distinct status
filters explicit. The chain-prefix predicate is our conservative structural
check, not a claim that the FEC guarantees complete financial replacement.

The Go command revalidates complete captures through the existing reader. The
independent gate checks every body/header against the existing
[source pins](./fixtures/receipt-report-metadata-2026-09-10.sha256), every raw
object hash and value, every observation/cohort membership, and each candidate's
source chain. A separate per-day Python enumeration reproduces the Go event
sweep exactly, including all requested days, coverage counts, gaps, and overlaps.

- Full `go test ./...` and `go vet ./...` pass.
- Report-period, metadata-reader, and CLI race gates pass.
- Fixtures test lower-ID successors, ignored latest flags, null status,
  competing/missing members, disconnected and contradictory chains, changed
  dates, malformed references, paper amendments, duplicate source files,
  invalid scope, partial traversal, empty captures, tampering, and cancellation.
- Interval fixtures cover shared days, nested overlaps, gaps, leap days,
  crossing windows, termination, and conflicts outside a narrower window.
- Four real command outputs replay byte-for-byte. Six independent Python
  checks and Ruff pass. Python is audit/test code only.

Go checks run offline with a 4 GiB/four-CPU container cap and 2 GiB Go memory
target. Source checks run offline with 1 GiB/two CPUs and read-only metadata.
No whole-cycle transaction scan or extraction runs.

## Retention and boundary

Outputs, drivers, verification logs, code/policy/doc snapshots, and a verified
hash manifest are retained under
`dumps/audits/fec/report-period-membership/2026-09-10/attempt-01/` in project
storage. Original captures remain in their prior audit directory.

The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Source/fact publication, A/B/E transaction membership, monetary calculations,
Arango, Dagster, and weekly activation are unchanged. This bounded diagnostic
is not a recurring whole-population storage design or financial selector.

Next: qualify and bind an electronic report-period field to an observed chain
candidate, with explicit financial-use blockers. Do not turn structural readiness
into a cash denominator by changing a boolean.
