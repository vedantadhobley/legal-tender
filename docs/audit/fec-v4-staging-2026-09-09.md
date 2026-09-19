# Approved v4 staging — 2026-09-09

Status: completed successfully at 23:14:13 UTC after starting at 22:41:58 UTC
(32m 15s). All three durable exit markers are zero, the Go result is
`status=staged`, and independent verification passed. V3 remains active.
Do not repeat the completed acquisition or stage.

The user approved staging/extraction and validation of the exact inputs from
the [completed acquisition](./fec-v4-acquisition-2026-09-09.md). This run does
not download source bodies, publish a release, change discovery defaults,
publish downstream facts, mutate graphs, or delete historical evidence.

## Inputs and storage review

The candidate remains
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`.
The exact plan SHA-256 is
`237aab2148035802292ef4e56165e719d4596a9ebce7116ab3a574a9f2d3ffe2`;
the acquisition-result SHA-256 is
`faf9cfc98f44c554a70f325166f833770384aec709f1588f143987a4d00dcf0f`.
The pre-launch verifier confirms all 27 acquired/reused artifacts, input
lineage, durable acquisition state, and unchanged active/immutable v3 manifests.

A fresh Go storage review passes the unchanged 600 GiB hot cap, 500 GiB free
floor, and 25 GiB margin. At pre-launch, unique Schedule A hot storage was
387,188,829,778 bytes and filesystem available space was 1,636,120,334,336 bytes.
Prior compressed sizes estimate 58,005,781,337 bytes of new staged output,
including 57,968,298,375 Schedule A bytes. These are estimates, not size bounds;
write-time guards enforce actual growth.

The existing review command is pre-acquisition-shaped: it does not credit
completed CAS captures, so it conservatively reserves the already downloaded
129,566,279,592 bytes again when given the original plan. Even this larger
scenario fits, at 562,206,773,832 hot bytes including margin and temporary peak,
and 751,286,518,529 required free bytes including the floor. This does **not**
mean another download will run. The stage command consumes the completed
acquisition and budgets only actual staging writes. A stage-aware read-only
review is a deferred diagnostic improvement, not a reason to alter the plan or
raise a limit.

## Execution boundary

- Container: `legal-tender-v4-staging-20260909`; stable Go run ID:
  `v4-stage-20260909-01`.
- Existing cached development runtime and the exact CLI binary from acquisition.
  No product code or standing service was changed.
- Offline container, 2 GiB memory cap, 1 GiB Go memory target, four CPUs, and
  128-PID limit. No credentials or `.env` are passed.
- Project storage is read-only except `raw/fec/`; this job's audit directory is
  separately writable. Release and downstream pointers remain read-only.
- The shared source-writer lock and cumulative streaming storage guards remain
  enabled. No full uncompressed temporary relation is created.
- The inventory selects 20 classic-file members, four Schedule A relations, and
  one all-history Schedule E relation for 25 staged outputs. Schedule B remains
  `archive_direct`; the four committee-summary CSVs remain whole source artifacts.
- New outputs pass full compressed/uncompressed digest readback before their
  checkpoint. Unchanged and checkpointed outputs require exact compressed hashes
  before reuse. Staging preserves physical COPY rows without field-level filtering;
  occurrence/fact validation remains separate.

At 22:42:13 UTC, all 20 small classic outputs were checkpointed and the first
Schedule A relation was streaming. Its temporary compressed output was
788,608,521 bytes. The container used about 79 MiB of its 2 GiB cap; v3 was
unchanged. Completed output count is not a work-percentage estimate: the large
relations dominate the remaining work.

At 22:52:50 UTC, the 2020 Schedule A output had passed readback and checkpointed
all 293,862,761 physical rows. The 2022 relation was streaming, with about
1.32 GB of temporary compressed output. No terminal marker existed yet; v3
remained unchanged. During the transition, resource checks showed active CPU
and I/O work and about 101 MiB memory use, not a stalled process.

## Verified result

All 25 inventory-selected outputs are complete: 22 marked `staged` and three
marked `reused`. Their logical compressed size totals 58,000,276,998 bytes;
this is not a claim of that much additional disk allocation. The result has no
issues, and all five blocking checks pass: input identity, acquired-source
membership, selected-output membership, output integrity, and storage budget.

| Selected relation | Physical rows | Compressed bytes |
|---|---:|---:|
| Schedule A, 2020 | 293,862,761 | 22,472,380,505 |
| Schedule A, 2022 | 166,775,547 | 10,222,594,425 |
| Schedule A, 2024 | 264,085,633 | 14,773,841,175 |
| Schedule A, 2026 | 172,729,583 | 10,492,874,888 |
| Schedule E, all history | 548,353 | 32,519,563 |

These are preserved physical source-row counts, not effective-record counts or
validated financial totals. Full field-level occurrence/fact processing and
downstream calculations remain separate. Schedule B and committee-summary
artifacts were already acquired; this stage did not materialize extra copies.

The independent verifier passed exact schemas, ordered inventory selection,
source/input lineage, immutable output metadata, durable stage-state equality,
storage limits, and unchanged active/immutable v3 manifests. Go supplied the
full compressed/uncompressed readback; the independent verifier did not repeat
that large body scan.

A read-only post-exit check at 2026-09-10 00:05:46 UTC confirmed all success
markers and checkpoints, zero private temporary output bytes, no issues, and
unchanged v3. Unique Schedule A hot storage was 434,927,926,346 bytes, with
1,587,666,006,016 filesystem bytes available. The one-shot container exited and
removed itself; all completed source outputs and audit evidence remain.

## Durable evidence and completion

Evidence lives under
`dumps/audits/fec/v4-staging/2026-09-09/attempt-01/` in project storage. It
contains exact inputs and hashes, the CLI binary, pinned schemas/inventory,
runner/verifier/monitor scripts, pre-launch and start-time storage reviews,
logs, results, and terminal markers.

- `run.exit`: wrapper outcome; absence is not success.
- `stage.exit` / `stage.json`: Go outcome and typed result.
- `verification.exit` / `verification.json`: independent schema, exact ordered
  selection, input lineage, output metadata, storage-limit, durable-state, and
  unchanged-v3 checks. The verifier does not repeat Go's full body readback.
- `stage.log`: current extraction; `runner.log` / `verification.log`: failures.
- `current-before.sha256` / `current-after.sha256`: active-manifest identity.

Success requires all three exit markers to be zero, `status=staged`, every
required blocking check passing, and independent verification passing. The
one-shot container removes itself on exit; disappearance is not success.
`monitor.py` is read-only and can run inside the live container or a capped,
offline replacement with `/job` and `/storage` mounted read-only.

The Go checkpoint and completed state are under
`raw/fec/stages/<candidate-release-id>/v4-stage-20260909-01{.checkpoint,}.json`.
On failure, inspect the result first; preserve exact inputs and completed
outputs. A retry must retain their identity rather than replan or redownload.

Follow-up: the [v4 publication gate](./fec-v4-publication-2026-09-10.md) now
passes source activation and all same-release summary publications using these
exact inputs. That separately approved operation, not staging, advanced the
source pointer. Discovery-default migration and thin summary orchestration
remain separate work.

The later [manual summary Dagster gate](./committee-summary-dagster-2026-09-10.md)
also passes; discovery migration and weekly activation remain separate.
