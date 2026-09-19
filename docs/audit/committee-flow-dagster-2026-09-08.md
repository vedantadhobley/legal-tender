# Committee-flow Dagster gate — 2026-09-08

Status: passed for isolated 2024 execution and replay. This accepts the
[thin orchestration boundary](../design/committee-flow-orchestration.md), not
live weekly daemon operation or other-cycle A/B coverage.

## Scope and environment

The existing Go publication, readiness, and graph commands ran through the
three new Dagster assets, twice, against the already-published 2024 inputs.
The gate seeded four immutable manifest paths into a separate filesystem IO
manager. It did not run source ingestion, discover new releases, decode the
raw corpus, replace existing graphs, or change the live Dagster instance.

The existing runtime image supplied Dagster 1.13.20 and Python 3.11.16. The
one-off container had a 4 GiB cap, four CPUs, `GOMEMLIMIT=2GiB`, and
`GOMAXPROCS=4`. It used the configured Arango password through the environment,
not arguments or logged configuration. No service cap or topology changed.

## Exact inputs and replay

The retained runner derives the immutable A/B/master/release paths from the
accepted [observation bundle and graph gate](./arango-committee-flow-evidence-2026-09-08.md).
It sets the resource's current-release pointer to a deliberately nonexistent
path. Both passes succeed without reading it.

| Stage | First pass | Replay | Check partition |
|---|---:|---:|---|
| Reconciliation publication | 17.351 s | 17.040 s | `2024` |
| Evidence readiness | 17.192 s | 17.319 s | `committee-flow-evidence\|2024` |
| Isolated graph verification/reuse | 69.219 s | 69.554 s | `2024` |

These timings cover each Dagster materialization call. Reuse still hashes
source shards and verifies graph contents; it is not zero I/O.

Both passes preserve the exact earlier calculation and bundle result bytes:

- Calculation SHA-256:
  `070e2c67057ab4671d48f139924760c6e5c8bf5fae60ecf2e6e2a779b4da1882`.
- Bundle SHA-256:
  `d450dfdb2efca9b0ace973fcb3daf2218ad0775152183fd03cd93f34dbc9a62e`.
- Projection ID:
  `89f049d4604f20a204c0ef52eb7ee5c37bbb7b59a516a901be06742529ce3b3f`.

Every blocking check passes for its actual partition. Calculation, bundle,
and projection data versions remain unchanged on replay. The graph returns
`reused=true`, preserves the earlier entity/component counts and separate
ledger measures, and remains `partial` for absent same-cycle masters.
The graph control-artifact digest changes with runtime measurements; its
logical projection identity does not. Full source drilldown stays in that
artifact and is absent from routine Dagster metadata.

## Failure and automation regressions

The tests verify exact forward/reverse partition mappings, including exclusion
of candidate-master facts and other cycles. They execute real Dagster
materializations around fixture Go results and reject command failure, invalid
schema output, and the wrong cycle without emitting an asset materialization
or executing its downstream consumer.

Automation tests establish that missing or failed upstream checks do not
authorize the graph. A 2024 pass cannot authorize 2026; a subsequent failure
blocks new work, and a passing recovery requests only its own cycle.

Two orchestration details were corrected before acceptance:

1. A check on a partitioned asset is unpartitioned unless its specification
   declares the partition definition explicitly. The new checks use the
   exact asset partitions. This is a preview API in the pinned Dagster release.
2. A passed check emitted before its asset materialization becomes stale for
   later automation ticks. Successful checks now follow output, and tests
   verify their target materialization belongs to the current run. Go/schema
   validation still happens before any output; failed results never publish
   a Dagster asset value.

The shared schema registry resolves pinned references locally. Tests reject
foreign hosts, filesystem URLs, path escapes, missing references, and invalid
graph states/measures/check lists. They also preserve exact large integer
strings and keep credentials out of subprocess arguments.

Verification: `make check` passed; the rewrite Python boundary/contract suite
passed all 56 tests; touched-file Ruff checks and formatting passed. No Go
selection, matching, or amount policy changed for this wiring gate.

## Retained evidence

Under the configured storage root:
`dumps/audits/fec/committee-flow-dagster/2026-09-08/2024/`.

The directory retains `gate.py`, `run.sh`, `result.json`, `progress.json`,
`run.log`, immutable `control/` artifacts, isolated `instance/` event state,
and `io/` manifest handoffs. `complete` was copied only after the successful
result was retained and compared with the runner output.

Result SHA-256:
`2790e0d02704dd6b6cad71e758a5c42bd23398e0a837bb7423a8bbe63f5971cc`.
The six run IDs and exact input paths are in that result.

`startup-failure/` preserves an initial runner setup failure: Dagster required
its temporary instance directory to exist. That attempt ran no Go command;
the runner now creates the directory before starting the isolated instance.

The next product boundary is a bounded, paginated investigative API over the
accepted graph and source lookup. Historical identity automation, cross-cycle
A/B gates, cross-release reuse, and economic-flow/terminal attribution remain
separate entries in the [rewrite queue](../todo.md).
