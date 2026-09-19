# Terminal receipt-role profile gate — 2026-09-13

Status: accepted. Full Go regression, targeted race tests and static checks pass.
The complete-corpus profile, expected-ID replay and byte-identical worker-varied
output gate pass. All explicit exit markers are zero; both test and corpus runners
exited successfully without an out-of-memory kill. The
[role-profile contract](../design/terminal-receipt-roles.md) owns behavior.

## Retained scope

Attempt: `/storage/dumps/audits/fec/terminal-receipt-roles/2026-09-13/attempt-01/`.
It retains the executable, complete Go snapshot, exact-input runner, hashes,
logs, result/replay, timings and explicit exit markers. Sources are read-only;
only this new audit directory is writable. No source fetch, schema change,
graph import, pointer advance or schedule change is involved.

Input: the [accepted typed generation](./funding-evidence-generation-2026-09-13.md).
Generation ID:
`35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
Generation file SHA256:
`1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.
Consumer executable SHA256:
`5171cebecddc29603448229607ef0d4675528f5cf259befccb499001507fa11e`.
Go source snapshot SHA256:
`0d1a465a04bc38ae378e02305a357c6223d94b55325de6be6ac75a64580f315c`.

The first invocation uses eight compact shard readers; fresh replay uses four
with the expected profile ID. Both use an eight-CPU quota, 4 GiB cap and
`GOMEMLIMIT=2GiB`. Credentials pass privately into the runner environment, not
arguments or retained artifacts. Source permissions remain unchanged.

## Acceptance boundary

Each invocation independently verifies the generation, full compact participant
publication and complete census. Profiles conserve scope, role groups, amount
signs and annotation marginals. Reported IDs join exact same-cycle master facts
without becoming resolved people/corporations. Every selected source witness
must match its profile and receipt edge. Completion recheck, expected identity
and byte-identical worker-varied replay are required.

This does not accept a terminal policy, allocate dollars, check historical
registration or resolve employer/organization names. Occurrence counts are not
donor counts or effective-payment totals.

## Complete selected-scope results

Both invocations verify all 264,085,606 participant occurrences. The profile
scope is the selected committee endpoint union, not every source recipient.

| Population | Observed result |
|---|---:|
| Scoped recipient occurrences | 263,540,585 |
| Occurrences at recipients outside the selected endpoint union | 545,021 |
| Unresolved reported-recipient syntax | 0 |
| Committee profiles | 9,545 |
| Profiles with reported occurrences | 7,567 |
| Explicit zero-occurrence profiles | 1,978 |
| Joint role groups | 43,608 |
| Routed source committee IDs at scoped recipients | 7,556 |
| Those IDs with exact same-cycle master facts | 6,849 |
| Those IDs absent from the pinned same-cycle master | 707 |

The 707 absent-master source IDs cover 2,162 selected-scope occurrences; the
matched IDs cover 318,569. These are source-identity observations, not resolved
people/corporations, terminal donors or historical-registration conclusions.
The exact-ID join does not change the boundary graph's identity states.

Nine automatically selected source occurrences cover every observed source-route
and source-ID-state category plus conflict and individual-overlap cases. Each
matches its complete source record, profile key, recipient and stored receipt
edge. Reused witnesses remain one source occurrence with multiple case labels.

An independent retained `check-result.jq` passes both artifacts. It recomputes
whole-corpus and per-recipient amount-sign conservation, role-group populations,
annotation marginals, source-ID occurrence totals, profile/master/topology ID
bijections, zero-profile states and non-promoted eligibility. These checks do
not substitute for the Go scanner's full physical/canonical shard verification.

## Replay and resource evidence

Profile ID:
`3e7de06b6c1cdc3ee434100fd974d8e38f8601c3e1fd3e5ebece1565c55af089`.
Result and replay SHA256:
`5c11d12145a2ecdbd799590221194b1c769b8ab5546308f54cf6d0230008c729`.

| Measurement | Result |
|---|---|
| Eight-reader complete invocation | 263.338 seconds |
| Four-reader fresh invocation | 261.551 seconds |
| Each JSON result | 80,354,435 bytes; byte-identical |
| Cgroup peak, including charged file cache | 4,295,876,608 bytes |
| Retained attempt | About 186 MiB, including executable/source and both outputs |

The timings include generation verification, boundary assessment, profile scan,
full source witnesses and output. Both runs had the same eight-CPU quota; this
is not a four-CPU/eight-CPU or isolated scan-scaling experiment. It establishes
worker-independent results, not an eight-reader speed advantage. Four remains
the command default.

The cgroup peak is near the 4 GiB circuit breaker, not process RSS or Go heap,
and does not establish spare memory. The large JSON is a detailed audit artifact,
not a serving payload. No temporary source copy or new persistent graph was created.

Setup, final-output and test checksum manifests pass independently. Test logs,
their runner and the independent JSON checker are retained against the same Go
source snapshot. No raw publication, graph or terminal-policy boundary changed.
