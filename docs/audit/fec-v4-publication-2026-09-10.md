# Coordinated v4 release and committee-summary publication — 2026-09-10

Status: complete. The approved job ran from 01:08:52 to 01:11:14 UTC (2m 22s).
The source release published at 01:09:44 UTC. All command, replay, independent
readback, verification, and wrapper exit markers are zero. V4 is now the active
source release; discovery defaults and running services were not changed.

## Exact source publication

This run consumed the [completed stage](./fec-v4-staging-2026-09-09.md) and its
exact acquisition/plan bytes. It did not discover a different source selection,
download more bodies, or repeat extraction. The source snapshots retain their
original acquisition watermarks; publication time is not a new publisher update.

- Release ID: `fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`.
- Manifest SHA-256: `b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
- Plan SHA-256: `237aab2148035802292ef4e56165e719d4596a9ebce7116ab3a574a9f2d3ffe2`.
- Acquisition SHA-256: `faf9cfc98f44c554a70f325166f833770384aec709f1588f143987a4d00dcf0f`.
- Stage SHA-256: `16c8d1227532d5e19278f1ee27aab0186b21b0db81696b67990038b467028d85`.

All 27 source artifacts and 25 selected outputs form the same evidence chain.
The publisher rechecked immutable source-file metadata and all selected outputs'
compressed hashes. New-output decompression evidence comes from completed
staging, not another extraction. All eight blocking release checks passed.
Publication preserved exact control inputs by digest, verified the prior active
release under its lock, wrote the immutable manifest, and atomically advanced
`releases/fec/current.json`. The prior v3 manifest and source evidence remain.

A second publication with another run ID returned byte-identical manifest
bytes, including the original publication time. Independent checks confirm
the active pointer equals the immutable backing and stdout artifact, every
source selection matches the plan/acquisition, all stage outputs match, and
the old v3 immutable manifest is unchanged.

## Complete same-release summary result

Every selected CSV was consumed through the published v4 manifest. Each fact
set preserves all raw columns, typed values, issues, identities, and byte
locators without financial grouping or row deletion.

| Cycle | Preserved records | Compressed bytes | Retained typed issues |
|---|---:|---:|---|
| 2020 | 13,554 | 8,772,521 | 9 invalid dates; 1 reversed interval |
| 2022 | 13,977 | 8,952,277 | 7 invalid dates; 2 invalid identities |
| 2024 | 14,065 | 8,976,593 | 3 invalid dates; 1 reversed interval |
| 2026 | 14,154 | 8,879,185 | 1 invalid date; 7 invalid identities |

Total: 55,750 preserved records in 35,580,576 compressed artifact bytes.
Issue counts describe field/interval observations, not necessarily distinct
records. They were retained, not repaired. These are summary occurrences, not
committee funding totals or additional Schedule A/B/E transactions.

Go compared every stored record against a fresh source scan before publication
and again on replay. All four replays returned byte-identical fact manifests.
The independent Python test decoded the stored zstd bodies, checked both hashes
and counts, and reconstructed raw/typed values, CSV byte spans, occurrence IDs,
unkeyed record versions, fact IDs, and issue counts from each release-owned CSV.
All four cycles passed. It did not reinterpret research captures as release inputs.

## Verification and scope

The [independent publication tests](../../tests/test_committee_summary_publication.py)
now share the stored-value comparison across historical artifact gates and real
release-bound publications. Logical CSV spans use the CSV reader's consumed-line
positions, including quoted multiline fields. Cycles and raw artifact paths come
from the release; no candidate, committee, amount, or cycle-specific repair was added.

Pre-publication regressions passed 25 tests, including all four retained research
artifact comparisons; the not-yet-published live gate was skipped. After publication,
the live every-record comparison passed in 7.22 seconds. Independent post-exit
control verification also passed using read-only mounts.

After source acceptance was recorded, the combined source-contract, historical
artifact, and live-release regression run passed all 31 tests in 19.22 seconds.
The test log and exit marker are retained with the job evidence.

The job used an offline one-shot container with a 2 GiB memory cap, 1 GiB Go
memory target, four CPUs, and no credentials. Only FEC release/control metadata,
committee-summary facts, and the job audit directory were writable. Raw sources
and other facts/calculations were read-only. No graph write, resident service
change, weekly schedule activation, or deletion ran.

Evidence lives under `dumps/audits/fec/v4-publication/2026-09-10/attempt-01/`
in project storage. It includes the exact binary/inputs, pinned verifier/test
code, logs, source publication and replay, per-cycle summary manifests/replays,
independent readback, verification JSON, pointer hashes, and explicit exit markers.
The completed container was `legal-tender-v4-publication-20260910`.

## Acceptance and next gate

The committee-summary source contract is now accepted for lossless,
release-bound occurrence/fact preservation. This does not accept a singleton
committee financial assertion, complete cash denominator, report/amendment/account
history, or terminal allocation. Existing graphs and A/B/E fact/calculation
publications retain their original release ancestry; they were not relabeled v4.

Next switch default discovery to v4 with tested thin Dagster summary handoff.
Weekly acquisition remains disabled until the separate retention gate passes.
Exact-evidence assertion grouping and receipt comparison follow in the
[active queue](../todo.md); neither belongs in normalization.

Follow-up: the [manual summary Dagster gate](./committee-summary-dagster-2026-09-10.md)
now passes. Discovery migration and weekly activation were explicitly separated
from that wiring; financial assertion grouping is the next domain gate.
