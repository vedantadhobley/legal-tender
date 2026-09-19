# Receipt participant index gate — 2026-09-12

Status: accepted and retained. Bounded replay, both full 2024 publications,
independent full readback, source inspection and copied-file checks pass.
This audit covers the [source-grain participant index](../design/receipt-participant-index.md),
not Arango vertices or role-qualified memo-conduit associations.

## Pinned inputs and build

- Fact set: `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Fact manifest SHA-256:
  `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829`.
- Source population: 264,085,606 dense, valid 2024 occurrences in 265 shards.
- Initial executable SHA-256:
  `ff0bc5168060ad92c2bb85c98f353ac84fc2b184ee60e595004d2b7ef8232dfb`.
- Initial corpus-test executable SHA-256:
  `9538a8b23905e3c19b1eeda5456c9fe44d83719b028aa870d215bb666795c6c7`.
- Initial source/test archive SHA-256:
  `9d1affd4b84ef853c515a14efb42ea0e0c8890aca3f0ea3c4423a78e815721eb`.
- Final executable SHA-256:
  `319c10f621768f19e7b7610dae84aa0d68ae4956b9d1e912ff818a76dc76d86c`.
- Final corpus-test executable SHA-256:
  `b0aa89a1ea9e75e6f528f0594007abb8f0466f95456b032d956b700017bf0f63`.
- Final source/test archive SHA-256:
  `f0122efc4a1d5610bdb80bfce3d768299033dec4628e5d622c97bcaed301f919`.

No download, extraction, source rewrite, graph write, Dagster activation or
current-pointer change is part of this run. It reads the same pinned facts as
the accepted receipt inventory and reference topology.

## Bounded performance and equivalence

Eight whole source shards were selected explicitly: 0, 33, 66, 99, 132, 165, 198,
231. They contain eight million rows. Both runs used the same eight-CPU allowance,
4 GiB container cap, 2 GiB Go memory limit and 512 MiB shared output cap.
The worker setting changes concurrent shard jobs, not the container CPU limit.

| Shard workers | Complete seconds | Peak process RSS bytes | Output bytes |
|---|---:|---:|---:|
| 8 | 14.265 | 1,860,767,744 | 26,171,770 |
| 1 | 31.163 | 164,229,120 | 26,171,770 |

The runs produced identical file descriptors, physical hashes, canonical value
hashes, censuses and calculation identity:
`84daebaf9078a525140f94ad411ca7e52af55ed332aeb6a9147dfd4a5866fa6a`.
The measured 2.18× gain is an end-to-end shard-worker comparison, not a claim of
linear CPU scaling. The faster setting spends more memory on concurrent readers
and writers; both fit the same circuit breaker.

The separate read-only corpus test took 18.94 seconds. It reread every output
row, compared all 24 projected values against the older generic source-reader
path over all eight million rows, and opened the complete 99-field source record
at both boundaries of each sampled shard. It checks values through a canonical
hash, not only population totals. Both benchmark scopes remain explicitly
incomplete-cycle; no sample result is promoted to a publication.

## Full-cycle gate

Both complete publications retain all 264,085,606 occurrences in 265 shards and
868,576,646 bytes. Every shard passed physical, logical-value and population
readback before either success manifest appeared. The entire file-descriptor
list and all censuses are identical between the runs.

| Run | Workers | Publication seconds | Peak process RSS bytes |
|---|---:|---:|---:|
| Initial | 8 | 138.902 | 2,017,976,320 |
| Final guard/replay | 4 | 225.916 | 919,785,472 |

The final runtime adds an explicit 1 MiB manifest-size check before any manifest
write. The initial version reserved that space but did not enforce the serialized
size. The added failure fixture passes. No classification, source mapping or
index encoding changed. Both runs used the same eight-CPU/4 GiB/2 GiB budget and
8 GiB output cap; they are not a controlled same-build CPU scaling experiment.

Initial calculation:
`cfb785665450d74ac9a99fbbda367599ee22fdf0089f1f5e9fef48b773abd0e5`.
Final calculation:
`5cf4f803465c19f8abc0f4cd3eab87b132184bc47536b6aa3c0c5753dbbe0a1e`.
The build digest changes the calculation identity, while all appearance identities
and index artifacts remain unchanged. Same-build worker-varied calculation-ID
replay is established by the eight-million-row benchmark above.

The initial separate full-output gate passed in 74.89 seconds. It checked every
indexed row and compared all values against the older source reader over the
eight declared sample shards, with complete source inspection at both boundaries.
The final build's equivalent readback passes in 74.64 seconds, as does its
final-ordinal CLI inspection. That appearance's identity and all 99 physical
source fields match the initial inspection exactly. The final container's
memory peak is 1,560,088,576 bytes, including charged cache and the separate
readback process; it is not the publisher's process RSS.

The complete new component and amount-sign populations match the
[prior inventory](./committee-funding-basis-2026-09-08.md) exactly. This comparison
uses the retained inventory artifact and exact source identity, not copied totals
as production rules. All 36,584,727 memo rows, 1,356,364 negative rows, 7,011 zero
rows, two unknown amounts and 738 overlap rows survive. These are source-record
populations, not distinct donors or distinct payments.

## Code gates and retained scope

Complete Go tests, module consistency and vet pass. Focused race tests cover
the participant publisher, reference consumer and shared source-policy package.
Fixtures cover all monetary signs and unknowns, memo preservation, overlapping
membership, conflicting IDs, missing recipients, invalid conduit evidence,
null versus empty strings, Unicode, extreme signed cents, field binding,
worker-varied replay, source corruption, cancellation, output exhaustion,
malformed ordinals/states, changed hashes/counts and full-source inspection.

The runtime reuses existing inventory/source-role policy. It adds no named
committee, candidate, employer, amount, memo substring or ID exception. The
sample shard positions are explicit audit inputs only.

Durable evidence is in
`/storage/dumps/audits/fec/receipt-participant-index/2026-09-12/attempt-01/`.
The accepted manifest is `cycle-final/manifest.json`; `cycle/` preserves the
initial full run, and `benchmark-1/`/`benchmark-8/` preserve the bounded replay.
The directory retains both source snapshots, executables, job scripts, JSON
results, full-source inspection witnesses, code/corpus logs and explicit zero
exit markers. `SHA256SUMS` verifies every copied artifact; `retention.exit`
records the successful copy/check. The retained tree is about 1.90 GB, including
both full publications and the benchmarks, not another copy of the raw facts.
Working evidence also remains in `/tmp/legal-tender-participants.dWlKX4/`.
No source, previous audit, graph publication or temporary evidence was deleted.

Next: join this participant role evidence to the accepted exact reference
topology, then benchmark and publish typed Arango connections into committee and
candidate ancestry. Reference absence must not imply unassessed key uniqueness;
conduit associations must not add a second monetary contribution.
