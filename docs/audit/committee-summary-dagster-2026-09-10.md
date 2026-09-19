# Manual committee-summary Dagster gate — 2026-09-10

Status: complete. The registered manual job passes all four published cycles
and byte-identical replay with source/fact storage mounted read-only. This
accepts the [thin handoff](../design/committee-summary-source.md#manual-dagster-handoff),
not weekly operation, financial grouping, or terminal-source attribution.

## Scope and exact inputs

`fec_committee_summary_facts` receives the actual `fec_release_publication`
output as an `AssetIn`, uses the shared dynamic cycle partitions, and invokes
the existing Go `publish-committee-summary` command through `GoPipelineResource`.
The registered `fec_committee_summary_fact_job` selects only that asset.

Input release:
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`.

Release-manifest SHA-256:
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

These are the existing [v4 publication](./fec-v4-publication-2026-09-10.md)
and its existing summary facts. The gate seeds only the immutable release path
into a private filesystem IO manager. It does not rerun source publication or
seed the resident Dagster instance. The configured current-pointer path is
deliberately absent; the input must come from the upstream asset value.

## Results

Each cycle ran twice through the registered job. Go verified the raw CSV,
every stored fact, hashes, and immutable backing on each call. The Dagster
output and stored control artifact match the already-published fact manifest.
The data version remains the fact-set ID, independent of the Dagster run ID.

| Cycle | Preserved records | Replay | Partition-scoped check |
|---|---:|---|---|
| 2020 | 13,554 | Identical | Passed |
| 2022 | 13,977 | Identical | Passed |
| 2024 | 14,065 | Identical | Passed |
| 2026 | 14,154 | Identical | Passed |

These are 55,750 source occurrences, not financial assertions to sum. Existing
invalid dates, identifiers, and intervals remain source evidence. A preservation
check can pass while those typed issues remain. Neither arithmetic differences
nor repeated candidate references are repaired or collapsed by orchestration.

The initial gate ran from 01:39:15 to 01:39:46 UTC. The combined boundary and
contract suite passed 59 tests, including the live replay; ten historical
corpus tests were skipped because their separate opt-in inputs were not set.
Those historical gates remain recorded in their own audits.

The final pinned suite, including the retry regression and another complete
four-cycle job replay, passed 60 tests with the same ten opt-in skips in
30.19 seconds (01:44:26–01:44:57 UTC). This run enabled the global scheduling
default but launched no daemon; the summary still had no automatic trigger.
Touched asset/test Ruff checks and formatting passed. No Go domain code changed.

## Failure, retry, and automation boundary

The [orchestration tests](../../tests/test_committee_summary_orchestration.py)
reject command errors, invalid result schemas, and wrong result cycles without
materializing the asset or running its consumer. The blocking check explicitly
uses the same partitions as the asset. A passed check follows output and targets
the materialization from that run, rather than becoming stale before output.

A retry regression loses the first completion acknowledgement after the shared
adapter stores a validated fixture result. Dagster retries with identical
command arguments, emits one successful materialization, and reuses the exact
control artifact. This tests retry orchestration; the real four-cycle gate
separately tests the Go publisher's existing-publication readback and reuse.

No schedule, sensor, or automation condition targets the summary asset. Tests
also enable the global scheduling default and prove that a release
materialization still requests no automatic summary work. The global switch
does not grant an opt-in path for this asset. Existing sensors are unchanged.

No discovery-default migration ran: the CLI still defaults to inventory v3.
Weekly acquisition, retention acceptance, financial assertion grouping,
same-release A/B/E refresh, and terminal allocation remain separate gates.

## Isolation and retained evidence

The offline one-shot container used a 2 GiB memory cap, a 1 GiB Go memory target,
four CPUs, and no credentials. Project data was read-only; only the gate's own
audit directory was writable. There was no daemon, external network, download,
extraction, source/fact mutation, graph access, service change, or deletion.
The active source pointer and immutable release bytes remained unchanged.

Evidence is retained under
`dumps/audits/fec/committee-summary-dagster/2026-09-10/attempt-01/`
in project storage: pinned code/contracts/fixtures and Go binary, runner,
test logs, explicit success markers, isolated instance events and IO handoff,
immutable command results, and `verification.json` with every run ID and
fact-manifest identity. `tests.exit` and `run.exit` are both zero.

`final-code/`, `final.sh`, `final-tests.log`, `final-pytest/`, and
`final-verification.json` retain the final suite; `final.exit` is also zero.
Independent post-exit checks matched its eight distinct run IDs and manifest
hashes against the first gate and immutable backing, and rechecked the unchanged
active release. Final verification SHA-256:
`0127550e540d12a940e5d8c7cea92456eb259c4378e1a8ef35654f1a46dd775d`.

Next: accept exact-evidence financial assertion grouping and investigate
summary arithmetic differences before scope-qualified receipt reconciliation.
The [active queue](../todo.md) keeps discovery migration and weekly activation
separate from that domain work.
