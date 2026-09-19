# Typed path gate — 2026-09-13

Status: accepted. Full Go regression, targeted race and static checks pass, including
independent small-graph enumeration, parallel occurrences, cycles, direct endings,
invalid/mixed routes, cancellation and explicit limit/unknown states.
The [contract](../design/funding-paths.md) owns behavior; this audit owns live status.
The final build passes all ten real cases, expected gate identity, byte-identical
fresh-process replay and independent retained checksum checks. All exit markers
are zero; the runner exited successfully without an out-of-memory kill.

## Retained scope

Final attempt: `/storage/dumps/audits/fec/funding-paths/2026-09-13/attempt-02/`.
It preserves the consumer executable, complete Go source snapshot, exact-input
runner, setup checksums, logs, timing, result/replay files and explicit exit markers.
No source download, new graph, schema mutation, pointer advance or Dagster change
is involved. The source root is read-only; only this new audit directory is writable.

The input is the [accepted typed generation](./funding-evidence-generation-2026-09-13.md).
Generation ID:
`35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
Generation file SHA256:
`1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.
Its prior [neighborhood acceptance](./funding-neighborhoods-2026-09-13.md)
remains unchanged.

The transient runner uses the existing project network, eight CPUs, a 4 GiB
container cap and `GOMEMLIMIT=2GiB`. Credentials pass privately from the existing
project container to the runner environment; they are not source, command-line
arguments or retained audit artifacts. Root inside the container accommodates
existing root-owned source directories without changing source permissions.

The earlier `attempt-01` completed all ten cases and byte-identical replay.
Final review then added a preflight rejection for committee self-queries that
could return an empty identity path without source evidence. Attempt 02 repeats
the full gate with that guarded build; attempt 01 remains intact as development
evidence, not the final reader's executable identity.

## Acceptance boundary

The code selects multi-hop witnesses from the verified selected graph topology,
not a list of named politicians or operator-chosen source rows. It keeps the two
committee ledgers and three candidate ending families separate. Selected receipt
and conduit entries must retain exact source and relationship evidence.
Known hop-bound and expansion-budget cases must remain explicit.

Fresh-process replay matches the expected gate identity and every result byte.
Each run reopens and verifies its source/graph backing independently.

| Case | Verified result |
|---|---|
| Selected A/B × authorization/support/opposition | All six combinations return source-backed three-step paths: two committee observations and one candidate ending |
| Receipt to candidate | Two four-step paths with exact Schedule A entry |
| Conduit to committee | Two two-step paths; the association adds no money |
| Zero committee-hop bound | No path returned; the existing frontier remains explicit |
| One-link work budget | Search stops with `more_paths_within_hop_bound=unknown`, not a false absence claim |

The result contains 13 paths across those ten cases. Parallel observations,
skipped cycle-closing edges and confirmed path-limit truncation remain visible.
These counts describe selected query cases, not distinct global routes or donors.

## Replay and resources

| Evidence | Final result |
|---|---|
| Complete verification and query gate | 157.607 seconds |
| Fresh-process verification and query replay | 158.519 seconds |
| Each result | 346,658 bytes; byte-identical |
| Cgroup peak, including charged filesystem cache | 4,296,007,680 bytes |
| Final retained attempt | About 34 MiB, including executable/source snapshot |

The cgroup peak was near the 4 GiB circuit breaker. It is not process RSS,
Go heap size or evidence of memory headroom. These measurements include generation
opening and source verification; they do not establish low-latency serving or
CPU-scaling performance.

Gate ID:
`d7f62fe506490524061fc5c92b5d670700fec461376f95be16d95efe0ee19a1c`.
Result/replay SHA256:
`55748ee8d27e7e3f0a7b65a03c787de4cb374da0a268addbe6fd34c9667ef3f1`.
Consumer executable SHA256:
`5d09484e58a59677952b2cc8ffecd3ab4a0fa2ad711ef9024db89560d09cae14`.
Source snapshot SHA256:
`de49ec987f8f2e4d5e59d0a490183c6fed8de4560afeab58436dd3801418b822`.

Setup/final checksum readback, complete result comparison and comparison of the
retained source snapshot to the working Go tree pass. Both audit attempts remain
retained; no existing graph or source publication was replaced.

This is selected typed-path acceptance, not every candidate or path, complete
financial graph coverage, a terminal-source definition, dollar allocation,
person/corporation resolution or serving readiness.
