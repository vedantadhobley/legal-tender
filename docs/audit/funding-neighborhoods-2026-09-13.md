# Funding-neighborhood gate — 2026-09-13

Status: accepted. Full Go regression, targeted race and static checks pass.
The real gate, fresh-process replay, expected gate identity, complete byte comparison
and retained checksum checks all pass. All three exit markers are zero; the
container exited successfully without an out-of-memory kill.

## Scope and retained inputs

The [reader contract](../design/funding-neighborhoods.md) queries one-hop typed
relationships through the [accepted generation](./funding-evidence-generation-2026-09-13.md).
The prior generation, source publications and all three graphs are unchanged.
Only a new audit directory is writable; the source root is mounted read-only.

Attempt: `/storage/dumps/audits/fec/funding-neighborhoods/2026-09-13/attempt-01/`.
It retains the consumer executable, source snapshot, exact-input runner, setup
checksums, result/replay logs and explicit exit markers. The configured Arango
password is piped privately from the existing project container, not written
into the runner, source snapshot, arguments or audit files.

Generation ID:
`35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
Generation file SHA256:
`1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.

The gate runs in a transient container capped at 4 GiB, eight CPUs and
`GOMEMLIMIT=2GiB`. It reuses the existing project network without host ports,
downloads, schema changes or graph writes. Container-root access accommodates
existing root-owned source directories; it does not change source permissions.

## Acceptance boundary

Witnesses are chosen from verified publications in Go, not from a list of
politicians or manually selected source records. The result labels each
relationship family and source-evidence grain. Continuations stay bound to the
generation, entity and family; missing masters remain distinct from absent facets.

All eleven selected cases passed: one witness for each of the eight declared
families, a cross-family committee, a cross-family candidate and a missing-master
committee. Six single-family cases had another page; each continuation advanced
and returned verified evidence. The remaining two ended after their first page.
The cross-family cases also retained available-empty, inapplicable and absent
facets without promoting any of them to a resolved identity or a funding total.

The result distinguishes five evidence grains: complete Schedule A source
occurrence, complete reported A/B source occurrence, linkage fact membership,
reconciliation component summary and resolved outside-spending calculation group.
No Schedule E individual-member enumeration is claimed.

## Replay and resource evidence

| Evidence | Result |
|---|---|
| First complete verification/query gate | 140.994 seconds |
| Fresh-process verification/query replay | 138.057 seconds |
| Each JSON result | 335,053 bytes; byte-identical |
| Cgroup peak, including charged filesystem cache | 4,294,971,392 bytes |
| Retained audit directory | About 34 MiB, including executable/source snapshot |

The cgroup peak was near the 4 GiB circuit breaker. It is not process RSS,
Go heap size or evidence of unused memory headroom. No low-latency serving or
CPU-scaling claim follows from these end-to-end verification measurements.

Gate ID:
`4fa819b3a8a98231228900ed07255cbb5f55ffb0f96cb7cf1d92b97495154411`.
Result and replay SHA256:
`24cd00df01e15688718351f8443b524d3bf9e662612f13edb66d45d9617e09a2`.
Consumer executable SHA256:
`7a4a6b7d0c4a014e475b58dba13886080b55136893d9a2dfece9915af1ff4128`.
Source snapshot SHA256:
`eac65177dc29ba8a32f10b0aad1b16c438a9a5b2694105ff0dc350f248ee25c3`.

Independent setup/final checksum readback and comparison of the retained source
snapshot to the working source also pass. Original graph and generation
publications remain unchanged.

This is not a new whole-receipt field scan, all-candidate/path coverage,
person/corporation resolution, terminal allocation, serving readiness or weekly
publication. Typed multi-hop generation access is next.
