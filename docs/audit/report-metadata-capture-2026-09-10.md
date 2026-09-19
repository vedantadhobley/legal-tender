# Bounded report-metadata capture — 2026-09-10

Status: Go implementation, offline failure/completion gates, and live seven-record
readback pass. **Live empty-page completion remains open:** the terminating request
hit the public demo quota. The [capture design](../design/report-metadata-capture.md)
defines the implemented boundary. No private credential was read or used.

## Scope and observed results

Only `/v1/filings/` for the seven already-qualified file IDs was requested:
1766839, 1780310, 1780346, 1813890, 1833804, 1876290, and 1882886. These are test
witnesses, not named runtime exceptions. Existing endpoint captures remain
development/replay inputs. No transaction, original filing, Swagger, or bulk
archive was downloaded in this gate.

The first run received one HTTP 200 body with seven records. It stopped because
the existing reader accepted curl's `HTTP/2` but not Go's `HTTP/2.0` protocol
representation. The reader now accepts both; an offline regression exercises
Go's format. This was our transport compatibility bug, not source schema drift.
The original failed attempt remains retained unchanged.

The second run used a fresh directory, two pages/two requests maximum, one attempt
per page, and a 2 MiB source budget. Page one returned the same 13,500-byte body:
`32b04ad101d926d7d5a6c8baff54a2dade3ec110b488e0e209232e2f2b9b7946`.
Its seven records passed every source check. Page two returned HTTP 429. The
credential-echo guard suppressed that error response because it contained the
public test credential; status, byte accounting, and suppression reason remain.
No private credential was substituted and no further request was made.

There were three HTTP request attempts across both runs. The final run accounted
for 15,772 bytes, including serialized non-sensitive headers and the suppressed
error response. It ended `blocked / credential_echo_suppressed`, exit 1. Its
page-one review is `validated_observations / exact_count_satisfied`, with seven
records, no issues, and no missing requested IDs. There is no final `capture.json`,
`review.json`, or live-success marker. Both readiness guards remain false.

The local replay verifies the exact accepted checkpoint without another request.
The rate-limited run is not relabeled as a successful or empty-page capture.

## Verification

The full Go suite, vet, and focused reader/CLI race tests pass after the protocol
fix. Fixture HTTP tests cover actual empty-page success, exact-count nontermination,
budgets, bounded retries, `Retry-After`, redirects, authentication failures,
truncated/oversized responses, schema drift, repeated/changed pagination,
cancellation, credential echoes, and overwrite refusal. No Go dependency was added.

Twelve independent Python checks pass against the retained old and new captures:
closed source/request/result schemas, exact body/header digests, every raw record
value, original observation ancestry, CLI replay, request scope, attempts, byte
budgets, failed-run markers, and unchanged financial/history guards. Ruff passes.
Python is test-only here; the fetcher and reader are Go.

Build/test containers used 4 GiB, a 2 GiB Go memory target, and four CPUs; the live
and independent checks used 1 GiB and two CPUs. Nothing was installed globally.
The active source-pointer SHA-256 remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No graph, source/fact pointer, Dagster asset, or schedule changed.

## Retention and next gate

The audit is retained under
`dumps/audits/fec/report-metadata-capture/2026-09-10/attempt-01/` in project storage.
It contains both run outcomes, accepted/rejected source evidence, checkpoints,
replay, request/contract/code snapshots, test logs, explicit exit markers, and
verified tree digests. Regeneratable build binaries are excluded.

Finish one small live traversal when API quota is available. This is the only
remaining transport smoke-test gate; do not start historical acquisition to test
it. Report-family/attachment selection design can proceed from existing originals
and endpoint observations. The
[source qualification](./receipt-report-metadata-2026-09-10.md) explains why bulk
transaction rows and amendment-incorporating period summaries do not provide the
complete report-version evidence needed for that selection.
