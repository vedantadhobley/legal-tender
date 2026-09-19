# Terminal-source assessment gate — 2026-09-13

Status: accepted. Full Go regression, targeted race tests and static checks pass.
The complete selected-population assessment, expected-identity replay and
byte-identical output gate pass. All exit markers are zero; the runner exited
successfully without an out-of-memory kill. The
[assessment contract](../design/terminal-source-assessment.md) owns behavior.

## Retained inputs and scope

Attempt: `/storage/dumps/audits/fec/terminal-source-assessment/2026-09-13/attempt-01/`.
The directory preserves the exact Go executable/source snapshot, pinned runner,
checksums, result/replay files, logs, separate timings and explicit exit markers.
Only that new audit directory is writable. Existing source storage is read-only;
there is no source fetch, graph import, schema change or pointer advance.

Input: the [accepted typed generation](./funding-evidence-generation-2026-09-13.md).
Generation ID:
`35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
Generation file SHA256:
`1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.
Consumer executable SHA256:
`c1db8867c93260a76d2b80473af6b013a91be6afb1b5fa48dda8843589d817c8`.
Go source snapshot SHA256:
`147c000984cbd7fa9008320e9eab4be286104c103a97f95730a339e9f3362c15`.

The transient runner uses eight CPUs, a 4 GiB container cap and
`GOMEMLIMIT=2GiB`. Credentials pass privately from the existing project container
into its environment, never command-line values or retained artifacts. Root
inside the runner reads existing root-owned sources without changing permissions.

## Acceptance boundary

Assess the complete selected A and B committee endpoint topologies independently.
Require exact observation/endpoint conservation and deterministic full SCCs.
Retain every node's hypothesis outcomes and distinguish ledger absence, missing
same-cycle master facts, cyclic components and incoming-frontier differences.
Select source-backed witnesses automatically; missing case populations are not
invented. Recheck generation completion before returning, then open all backing
anew for expected-identity and byte-identical replay.

This accepts neither a terminal-source policy nor financial allocation. Reported
receipt roles, person/corporation resolution and full funding denominators remain
outside this assessment. Existing graphs and calculations remain unchanged.

## Observed selected-topology results

The endpoint union contains 9,545 committee identifiers. These counts describe
selected observation graphs, not distinct donors or allocated financial origins.

| Measure | Selected Schedule A | Selected Schedule B |
|---|---:|---:|
| Observations | 320,731 | 341,720 |
| Present endpoints | 8,397 | 8,278 |
| Present endpoints without same-cycle master | 707 | 502 |
| Endpoints with no selected incoming observation | 4,483 | 2,715 |
| Such endpoints with a same-cycle master | 3,776 | 2,715 |
| Root SCCs | 4,501 | 2,730 |
| Members of root SCCs | 4,524 | 2,746 |
| Cyclic SCCs | 35 | 36 |
| Cyclic root SCCs | 18 | 15 |
| Union endpoints absent from this ledger | 1,148 | 1,267 |

Among endpoints present in both ledgers, 1,077 have no selected incoming A
observation but do have incoming B observations; 161 show the reverse.
Another 2,326 have no selected incoming observations in either ledger. These
comparisons do not establish that either ledger is wrong or that any of those
committees is a terminal source. The result preserves the full state comparison
and per-node membership rather than discarding the differing cases.

All nine available source-witness cases passed. The B missing-master frontier
case has no matching selection population and records that state explicitly.
Missing B masters exist elsewhere in that selected topology; an absent witness
case is not a claim that the whole B graph has complete identity coverage.

## Replay and verification

Assessment ID:
`b167625e1525f356147dc7ccebcf07433907d74b86b176e5712c87720ff3dde6`.
Result and replay SHA256:
`fe39f6a785e88543b194a563b78bfadd30df8a364f24239812ee3bbce40014f1`.

| Measurement | Result |
|---|---|
| Complete assessment, including generation verification | 122.868 seconds |
| Fresh-process verification and assessment replay | 120.421 seconds |
| Each JSON result | 17,713,662 bytes; byte-identical |
| Cgroup peak, including charged filesystem cache | 4,295,979,008 bytes |
| Retained attempt | About 67 MiB, including executable, source and both results |

The cgroup peak is near the 4 GiB circuit breaker; it is not process RSS, Go
heap size or evidence of spare memory. These timings do not establish serving
latency or CPU scaling. The full population result is an audit artifact, not a
proposed HTTP response payload.

Setup and final checksum manifests independently revalidate. A separate JSON
check recomputed node in/out totals, component internal/external conservation,
hypothesis populations, absent-node null outcomes and comparison conservation.
No financial or terminal eligibility is promoted. The retained test logs and
runner have their own checksum manifest tied to the same Go source snapshot.
