# Early-cycle coverage and reported-span gate — 2026-09-10

Status: bounded source investigation and additive Go v2 comparison pass. The
[design](../design/summary-reported-span.md) owns current behavior.

## Finding and correction

The retained evidence does **not** prove that January–March activity was zero.
It does show that v1 required that proof for a use that did not need it:
comparing totals over the exact reported April-to-April window.

The FEC page labels that narrower reporting period explicitly. The pinned filing
list contains an original Form 1 received June 20, 2023 and a Form 3 beginning
April 1, 2023. Registration and first financial coverage are not interchangeable.
The first-report guidance requires pre-registration activity to be disclosed;
candidate cumulative reporting uses a different election-cycle time basis.
These findings justify separating reported comparison scope from full-cycle
coverage, not manufacturing an inactive prefix.

The [digest fixture](./fixtures/cycle-prefix-2026-09-10.sha256) pins four official
HTML pages and their response headers. Together the bodies are 181,713 bytes.
URLs and hashes are also retained in `source-evidence.json`. Existing filing,
report, metadata, and summary sources were reused; no transaction corpus or new
metadata response was fetched.

## Real results

| Case | V2 result |
|---|---|
| SID reported window, 2023-04-01 through 2024-04-30 | All five flow-field comparisons and closing cash qualify and have zero differences. Opening cash remains blocked. |
| SID full-cycle window | All fields remain blocked; the requested window was not narrowed implicitly. |
| SID deliberately missing-cover test | All window comparisons remain blocked; a reported envelope does not fill the internal gap. |
| NRCC unresolved cohort | All comparisons remain blocked by the existing evidence/scope checks. |

The SID envelope separates 90 prefix days, 396 reported-span days, and 245 suffix
days within the 731-day calendar cycle. Prefix/suffix intervals contain no money
or inactivity assertion. This does not declare missing required filings or
noncompliance. The exact reported amounts are unchanged from the
[window gate](./report-window-2026-09-10.md).

The source summary's +150,000,000-cent cash residual and the report window's
oppositely oriented −150,000,000-cent residual both survive. Source snapshots and
report membership remain distinct; no funding or terminal eligibility is promoted.
No correction, named runtime exception, or cumulative-column fallback was added.

## Verification and retention

- Full Go tests, vet, and focused summary/report-period/CLI race checks pass.
- Fixtures cover exact boundary matching, unchanged stock rules, unknown outside
  activity, leap days and different source cycles, invalid/reversed/out-of-cycle
  dates, conflicting variants, internal missing values, and signed differences
  beyond int64 without rounding or arithmetic-agreement prerequisites.
- Four new CLI outputs replay byte-identically. The four prior v1 outputs remain
  byte-identical; embedded source windows and summary assertions are unchanged.
  The v2 entrypoint rejects a tampered original document without emitting a result.
- Independent checks verify the pinned official pages and retained filing dates,
  each calendar day exactly once, every numeric difference, unchanged operands,
  all guards, and the prior raw-summary/report-window gates. Ruff and local
  documentation-link checks pass.

Evidence is retained at
`/storage/dumps/audits/fec/cycle-prefix/2026-09-10/attempt-01/`, with explicit
completion markers, results, source pages/headers, replay/test logs, drivers,
source/contract/test snapshots, and verified `SHA256SUMS`.

The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Source facts, transaction data, Arango projections, and Dagster are unchanged.
