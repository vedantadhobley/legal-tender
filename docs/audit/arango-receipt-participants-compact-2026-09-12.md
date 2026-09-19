# Compact Arango participant gate — 2026-09-12

Status: implementation, real comparison, varied-layout and standalone replay,
and checksum-verified durable retention pass. This extends the
[streaming graph contract](../design/arango-receipt-participants.md) and compares
against the [accepted expanded graph](./arango-receipt-participants-2026-09-12.md).
No full-cycle publication or financial/identity rule changes are included.

## Change and proof

The v2 appearance replaces its embedded 24-field participant row with the exact
ordinal, inventory component and source route. Fact-set identity, appearance key,
full conduit decision, connection dispositions and unresolved identity flag stay
in the graph. Receipt/conduit edges, their exact amounts and source locators,
candidate authorization and master records are unchanged.

The complete participant publication still retains all 24 fields. The original
fact still retains all source fields. This removes a duplicated graph copy;
it does not aggregate contributors, drop occurrences, merge identities or remove
raw data. Amount thresholds and names do not affect membership.

Each worker reads the compact document back and reconstructs its complete v1
appearance with the exact verified participant row. It compares every field,
including null/empty, signed/unknown money, memo and false flags. Source-evidence
document digests reproduce the old layout and are separate from physical v2
digests. Combined physical/proof batches remain capped at 8 MiB.

The optional v1 comparison binds a result-file SHA-256, requires exact source
inputs and ordinal range, and validates that old graph's completion and counts.
It then reads every corresponding old graph field too. Missing, altered or extra
documents fail the comparison. Neither the old database nor its metadata is
written. Candidate/conduit witnesses and source inspection must match unchanged.
Comparisons exclude only Arango-managed `_id` and `_rev`, as in the prior gate.

V2 completion does not depend on the existence of a full expanded graph.
Its own full reconstruction/readback and source digests are mandatory; the live
v1 comparison is additional acceptance evidence. A separate replay without the
comparison checks that boundary before accepting the layout for full-cycle work.

## Inputs and execution

The exact inputs and original source release are those in the
[expanded gate](./arango-receipt-participants-2026-09-12.md#inputs-and-reproducibility).
The comparison reads the accepted `100000-import.json` and `1000000-import.json`
from its retained audit root, not a mutable graph pointer.

Executable SHA-256:
`04d67d021cb58125034ab3751b4fc648dfe57de00ce4d2a0cea53b267268753e`.
Source archive SHA-256:
`4359ba1b4f9b6bb63dac03d8cf42121511f4c5c4e4ec9131579f343bffc0693e`.
All real invocations use that same executable. Source storage is read-only;
the new content-addressed v2 databases remain isolated from accepted v1 graphs.

Runner: temporary 4 GiB container, eight-CPU quota, `GOMAXPROCS=8`,
`GOMEMLIMIT=2GiB`; no standing service or memory setting changes. Imports use
four workers/1,000-row batches; varied-layout replays use one worker/257-row
batches. The final standalone replay uses four workers without the v1 read.
Each invocation verifies source backing and the complete conduit stream.

## Real results

| Measurement | 100,000 occurrences | 1,000,000 occurrences |
|---|---:|---:|
| Expanded encoded document bytes | 182,494,233 | 1,760,113,025 |
| Compact encoded document bytes | 103,943,308 | 963,570,458 |
| Total encoded reduction | 43.04% | 45.26% |
| Appearance-only encoded reduction | 60.38% | 61.66% |
| Documents verified in each graph | 232,694 | 2,129,879 |
| Import plus live comparison | 35.419 s | 78.334 s |
| One-worker replay plus live comparison | 45.333 s | 180.529 s |
| Import process peak RSS | 471,334,912 bytes | 504,160,256 bytes |

Projection IDs:

- 100,000: `f1221c41b5375e2971306777e17b483471e21b9dce5e6c453ac07e687ab7b309`.
- 1,000,000: `e5f901c77a05aac6c0fc4a00eaba18166bd403f6e701dc76fdb2d32c2d012f0f`.

The million-row graph keeps 1,000,000 appearances, 1,000,000 receipt edges,
103,051 conduit associations, 8,584 authorization edges and 18,244 entities.
Both samples preserve the 299 missing-master states from their exact context.
All edge/context fields, counts, conduit dispositions, candidate/conduit paths
and full source checks match v1. Every collection's reconstructed source digest
matches the corresponding v1 physical digest; only the appearance physical
digest and bytes change.

Worker/batch-varied replay preserves projection identity, both digest sets,
membership, payload bytes, live comparison and source/query witnesses. The
standalone million-row replay also reproduces all of these except the intentionally
omitted optional comparison. It finishes in 53.676 s, including 29.812 s of
stream/readback, with 525,922,304-byte process peak RSS. The runner's whole-sequence
cgroup memory peak is 1,084,735,488 bytes under its 4 GiB cap.

Import timing includes mandatory compact reconstruction **and** reading the entire
expanded comparison graph. It is not a like-for-like speed benchmark against the
old import. The one-worker replay intentionally uses smaller batches; it checks
layout-independent results, not a controlled CPU-scaling comparison.

Encoded JSON size is an exact payload measurement, not an exact database-disk
measurement. Collection figures remain engine estimates; do not sum their
document and index figures into a claimed exact disk allocation or extrapolate
a sample into a guaranteed full-cycle budget.

## Verification and retention

Full Go tests, focused race checks, vet, module tidiness and formatting pass.
Fixtures cover reconstruction of all participant fields, nullable and large signed
cents, unresolved evidence, altered ordinals/keys/fact IDs/dispositions, false or
extra fields, wrong source rows, batch-layout equivalence and baseline scope
rejection. Existing import, cancellation, corruption, membership and cursor tests
also pass.

All explicit code, fixture, import, replay, independent cross-result comparison
and retention markers are zero. Both validation containers exited zero without
an OOM kill. Independent result checks conserve receipt/disposition membership,
compare old/new fields through their verified digests, and compare full query and
source witnesses across all replays. The archived source matches the checked code.

Durable root:
`/storage/dumps/audits/fec/arango-receipt-participants-compact/2026-09-12/attempt-01/`.
It retains the executable, source snapshot, race-test binary, invocation scripts,
all result JSON, independent checks, logs, memory measurement, `SHA256SUMS` and
success markers. Every copied file passes checksum readback. The accepted v1
result files and both v1 databases remain unchanged.

## Next boundary

After this gate, use the compact layout for full-cycle publication work. Add the
full-cycle storage admission, progress/retry and publication boundary, then verify
all 264,085,606 occurrences and integrate committee ancestry. These bounded
samples do not complete the connected graph, four-cycle rollout, entity resolution
or terminal allocation. No source artifact or database is deleted by this work.
