# Publication-independent committee path gate — 2026-09-13

Status: accepted for the existing real 2024 generation and synthetic two-cycle
composition. Full Go regression, targeted race checks and `go vet ./...` pass.
The real backend gate and fresh-process replay pass with identical bytes, zero
exit markers and no out-of-memory kill. The
[reader contract](../design/funding-window-reader.md) owns behavior.

This is not acceptance of two real cycles, every relationship family, a new
physical graph, terminal attribution or low-latency serving. Existing source and
single-cycle graph checks remain enabled.

## Retained scope

Attempt: `/storage/dumps/audits/fec/funding-window/2026-09-13/attempt-01/`.
It retains both executables, the Go source snapshot, exact input specification,
runner, results, timings, memory measurement, logs and explicit exit markers.
`SETUP_SHA256SUMS`, `FINAL_SHA256SUMS` and `REGRESSION_SHA256SUMS` all pass
independent readback. The source snapshot matches the working Go source tree.

The input is the [accepted 2024 typed generation](./funding-evidence-generation-2026-09-13.md):

- Generation ID: `35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
- Generation file SHA256: `1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.
- Input specification SHA256: `c5f9610e4f23112b6f3dcb3a2719a877300e9e1d4c8836da1c0025e80432299a`.

The live runner invokes the opt-in Go test, which calls the production reader
directly. Its result binds the test executable, not the separately retained CLI
executable. CLI argument validation has fixture coverage; this audit does not
claim a separate live CLI invocation.

The source root is read-only. Only this new audit directory is writable.
No download, source publication, graph import, pointer advance, Dagster change
or terminal policy is involved. Credentials pass privately through the existing
project service environment and are absent from arguments and retained artifacts.

## Complete date counts and query witnesses

The gate independently decodes every selected compact A/B observation to build
per-day and unknown-date counts. It compares that census with each production
query's included/before/after/unknown-excluded counts. This validates all selected
ledger observations, not all raw Schedule A/B rows or all possible paths.

Witness selection is automatic: the first dated non-self link in deterministic
link order for each ledger. No politician, committee or source ordinal is coded
as a special case. The source-selected dates below are outputs, not policy.

| Ledger and query | Included | Before | After | Returned paths |
|---|---:|---:|---:|---:|
| A, all supplied dates | 320,731 | 0 | 0 | 1 |
| A, 2024-08-06 | 648 | 238,807 | 81,276 | 1 |
| A, 2024-08-07 | 392 | 239,455 | 80,884 | 0 |
| B, all supplied dates | 341,720 | 0 | 0 | 2 |
| B, 2024-01-18 | 417 | 143,861 | 197,442 | 1 |
| B, 2024-01-19 | 259 | 144,278 | 197,183 | 0 |

These selected real populations contain no undated observations. Synthetic
two-cycle fixtures cover missing dates, their explicit bounded-window exclusion
and their inclusion when no date filter is supplied. The ledger populations stay
separate; these counts are not additive amounts or distinct payments.

The real queries use one committee hop and retain source-backed links. Every
returned date matches its source-verified graph observation. Synthetic fixtures
exercise multi-hop cross-cycle routes, parallel occurrences, historical facets,
input-order replay, overlap/cap rejection, cancellation and changed completion
boundaries. Existing single-cycle guards pass unchanged.

## Replay and resources

| Evidence | Result |
|---|---|
| Complete opening, verification and six-case gate | 155.165 seconds |
| Fresh-process opening, verification and replay | 148.195 seconds |
| Each result | 291,242 bytes; byte-identical |
| Cgroup peak, including charged filesystem cache | 4,296,081,408 bytes |
| Retained attempt | About 58 MiB, including both executables and source snapshot |

The transient live runner uses eight CPUs, a 4 GiB container cap and a 2 GiB Go
heap target. The regression runner uses four CPUs with the same memory limits.
Both use `golang:1.26.5-bookworm`; the host build toolchain is Go 1.26.5.
The peak is near the container circuit breaker. It is not process RSS or evidence
of memory headroom for a second cycle. Opening/readback and per-query topology
construction need separate profiling before larger or low-latency workloads.

- Gate ID: `b8d6c78cec5cfdafea113993a726ce08c8a1aa33b19f34ddf11100e866424089`.
- Result/replay SHA256: `913d816359e34f959ea93b9e3df36ed8cb9442b70638580cd576633de428efea`.
- Gate executable SHA256: `b3b1ba29c9e3f99047b66cf99910e9b395e7e5eb2a9c0dafaae6f14a86f7907a`.
- CLI executable SHA256: `8d081b14f03ecc9889f1d8b8165ea542939482936ba8e651db090822cc0841d1`.
- Source snapshot SHA256: `d6d2a0a1693e63cdc57e7bf3e425c8d1cb1bc32801c3f99eee29e2100a0c7043`.

Next: extend the explicit source/window boundary to receipt and candidate
connections. Preserve unknown validity and source reporting intervals. Validate
a second real cycle before claiming real cross-cycle integration; a full
four-cycle acquisition is not a prerequisite. The
[user interpretation review](../design/pre-attribution-review.md) remains open.

## Follow-up: full-chain synthetic loader integration

The review identified a test gap: the original two-cycle fixtures constructed
stub readers. `make test-window-integration` now closes that gap through the
[public-loader/CLI integration fixture](../../internal/integration/fundingwindow/window_test.go).
It creates tiny synthetic 2022/2024 publications with the actual occurrence,
fact, calculation, bundle and graph publishers, then calls `OpenWindowReader`.
Each cycle supplies one selected A and one selected B observation. Together they
form a two-step route that neither partition supplies alone.

Both ledgers pass exact source/date routing, historical facets, inclusive date
selection and reversed-input reopening. The CLI returns the same canonical result
as the public Go reader, replays the expected identity byte-for-byte, and emits no
successful result for a wrong expected identity. Wrong receipt locators, corrupted
second-source ZIP bytes, a changed second-graph observation and invalid second
completion metadata are rejected. Restored backing passes a final clean reopen.

The final isolated run passed with the race detector in 10.897 seconds, excluding
image startup and compilation. Full `go test ./...` and `go vet ./...` also pass.
The run's logs and zero `tests.exit`/`run.exit` markers were retained at
`/tmp/legal-tender-window-test.y6slyKP8/`; the container reported no OOM kill.
These temporary logs are not a durable real-source publication. The reproducible
acceptance mechanism is the checked-in fixture and Make target.

The runner was also exercised on a failing CLI comparison: it retained a nonzero
marker and removed its disposable stack. That failure came from comparing
indented JSON `RawMessage` bytes with compact bytes in the test; canonical JSON
comparison fixed the assertion without changing runtime code.

Acquisition metadata and the external Schedule B extraction process remain
synthetic boundaries. This does not qualify FEC downloads or `pg_restore`, two
real cycle corpora, or multi-cycle memory/latency capacity. The test uses real
isolated ArangoDB with no source/reader verification bypasses, development volumes,
real credentials or source downloads. All graph writes and corruption probes
target disposable fixture databases. Only generated test databases/volumes were
removed. The preceding real 2024 audit remains unchanged.

## Follow-up: receipt and candidate window connections

The additive [connection interface](../design/funding-window-reader.md#receipt-and-candidate-connections)
passes the same full-chain synthetic publisher/loader fixture in both ledgers.
The fixture covers receipt-to-committee, committee-to-candidate and
receipt-to-candidate routes; source-qualified identical ordinals; inclusive entry
dates; missing conduit links; preserved authorization assertions with shared graph
keys; original source routing; reversed-input reopening; and CLI identity/replay.
An already opened connection reader also rejects changed completion metadata.

The final isolated race run passed in 13.022 seconds, excluding startup and compilation.
Logs and zero `tests.exit`/`run.exit` markers remain in
`/tmp/legal-tender-window-test.pMuAHemI/`; no OOM kill occurred. All temporary
fixture containers and generated volumes were removed. Full Go regression,
targeted receipt/generation/CLI race tests and vet also pass.
Shared committee-chain evidence is verified once per returned source occurrence,
not repeatedly for each authorization variant; a regression test enforces this.

These are reproducible synthetic checks, not real-source acceptance of the new
interface. Its own retained real witness/replay gate remains next. No existing
publication, raw source, amount ledger, generation-bound reader or original
committee-window result contract was changed. Date-filtered Schedule E endings
remain explicitly unsupported until their source-member date boundary is defined.
