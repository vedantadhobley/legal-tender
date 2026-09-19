# Shared-conduit date-window gate — 2026-09-14

Status: accepted. Full Go regression, targeted race/static checks, isolated
two-cycle integration, the real read-only gate and byte-identical fresh-process
replay pass. Explicit success markers and retained setup/final checksums pass.

## Scope

The [window consumer](../design/funding-window-reader.md#shared-conduit-generation-inputs)
now binds the additive shared-conduit generation through an explicit v2 input
specification. Base-only v1 inputs retain their meaning. The unchanged A/B/E
generation remains separately identifiable inside the outer generation.

Dates come from the original receipt, never its related memo, source cycle or
publication time. Known dates use inclusive bounds. Unknown dates remain visible
in unbounded queries and explicitly excluded in bounded queries. Excluded entries
still receive source-backed checks. Original/new decisions, both source rows and
complete-group evidence remain available. No graph, money selection, identity
rule, terminal definition or current pointer changes.

Admission checks exact base/outer identities and rejects overlapping base and
extended inputs before opening graphs. Generation-qualified entries, links,
coverage and facets use the outer identity; the base ID is not an alias. Resource
guards count each base A/B population once, without claiming capacity for several
real extended generations.

## Verification

The [full-chain fixture](../../internal/integration/fundingwindow/shared_window_test.go)
publishes two synthetic source partitions through the normal publishers and real
Arango loader. It covers cross-partition committee/candidate paths, both ledgers,
original versus memo dates, undated entries, date exclusion, source-qualified
ordinals, extension facets, unchanged base family, support/opposition source-member
endings, wrong ancestry, overlapping inputs, reversed-input replay and CLI output.
Corrupting a date-excluded shared edge fails the query without repair.

`go test ./...` passes. Targeted `-race` tests and `go vet` pass for
`fundinggeneration`, `receiptgraph` and `cli`. Final disposable integration evidence
is `/tmp/legal-tender-window-test.9EsacVr1`, with `tests.exit=0` and `run.exit=0`.
Formatting, shell syntax and focused documentation-link checks also pass.

The [real gate](../../internal/projection/arango/fundinggeneration/window_shared_live_test.go)
independently decodes the complete selected A/B observation artifacts to check
date coverage. It selects shared witnesses from verified membership and ordered
graph keys, not named entities or fixed source ordinals. It checks source routing,
original/new decisions, exact source evidence and zero additional money.

All seven real cases pass: unbounded A and B entries, inclusive receipt-date
selection, before/after exclusion, unchanged original-family unavailability and
a dated committee continuation. The continuation returns a shared association
followed by one selected committee observation. This is a typed evidence path,
not an allocated payment.

Unknown-date and real cross-partition semantics are not inferred from those seven
cases: unknown dates and cross-partition joins are covered by the synthetic test.
The retained real gate uses the existing 2024 input only. It does not scan every
receipt date or enumerate every possible path.

## Retained evidence

Directory: `/storage/dumps/audits/fec/shared-conduit-windows/2026-09-14/attempt-01`.

- Extended generation: `f1c9a89e46ae1bdc291cadf8299230adfb30ca52ddc81932aa6f14afee5a6ec1`.
- Extended file SHA-256: `b6e65ec8d23b53dd9316131d67980b7a7272f51e69134fd276539a3e5f03c935`.
- Consumer executable: `45fafc9b4de12e8bac897dc059d9880aaf4256e0f19ab2c2aa5fed19eb98e5c6`.
- Retained source archive: `717af76a7b4636130c9559f375c2051782af216a5b2ff32aef7c32d5290ef5da`.
- Input specification: `b8e62c3467ffa72df9252bc99bab6de5cae1d2d80824d10861384f44633484ce`.
- Accepted gate ID: `578cdcf992be7d64a4a6a54412dad72690e8bbca31f368b8224102a1110cbc84`.

The [runner](../../scripts/run-shared-window-gate.sh) retains the executable,
source snapshot, input specification, results, logs, measurements and success
markers. Source storage mounts read-only; only the new audit directory is writable.
The configured credential passes privately through the environment, not an
argument or retained file. No source fetch or graph import occurs.

The first gate took 235.145 seconds and the fresh-process replay took 233.558
seconds, from 23:12:30 through 23:20:18 UTC. Results match byte-for-byte.
`gate.exit=0`, `replay.exit=0`, `exit-status.txt` reports `exit_code=0`, and the
container exited zero with `OOMKilled=false`. Fresh checksum verification passes
for every setup and final artifact listed by the runner.

The container had a 4 GiB memory/no-swap cap, 2 GiB Go heap limit and eight CPUs.
Recorded cgroup `memory.peak` was 4,295,966,720 bytes, about 4 GiB. This is not a
Go heap measurement or evidence of spare memory headroom. Opening includes full
base-graph checks and complete compact shared-membership/expected-payload
verification; selected shared live fields are checked on query. It is not a new
full shared-graph field scan or an interactive-serving benchmark.

The final integration rerun tightens the overlap test to require the admission
error, not merely any error. That test-only assertion postdates the retained
source snapshot; production and live-gate code are unchanged. Build-environment
pinning, real multi-cycle capacity/acceptance, identity resolution and the
raw-to-graph recovery checkpoint remain separate work.
