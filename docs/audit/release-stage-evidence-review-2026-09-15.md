# Historical stage evidence review — 2026-09-15 UTC

Status: cause identified; full Go tests, clean builds and real descriptor
comparison/replay pass. The exact original stage bytes remain
missing; no release pin was changed and no replacement was installed.

## Cause and retained evidence

The [September 4 publication audit](./schedule-b-columnar-publication-2026-09-04.md#manual-control-artifact-caveat)
already recorded this incident. Two manual staging containers shared a
convenience stdout file. A canceled older process overwrote the successful
retry's output after publication had consumed it. A separate stage result was
then regenerated and retained. This explains the two different byte identities;
it is not evidence of changed source transactions.

The original pinned digest is
`84156d7c692c564e3274e59ee7f47e5125733df60d26f04746ff47a869c19a67`.
The retained reconstruction has digest
`3aca399d465cfdb0f7afb70733e05f11228693d43c3de009164108e0e376cd6c`.
Its content-addressed control record and run-named copy are present. The expected
original control path is absent. The retained Schedule B columnar audit and
the other project audit directory did not supply another named stage record.

This review does not claim that every possible external backup was searched.
No source download, re-extraction, graph import or broad filesystem scan occurred.

## Repeatable comparison

The new Go command, `review-release-stage-evidence`, reads four bounded,
SHA-256-pinned metadata files: published release, its plan, its acquisition and
the explicitly selected candidate stage record. It reuses the known release
inventory and existing metadata validators. Plan/acquisition paths follow the
release-control layout; the candidate always retains its own digest.

The check verifies:

- Candidate/prior release, inventory, plan and acquisition identities.
- Complete selected-source membership and every `SelectedSource` field.
- Complete acquired-source membership, version identity, byte count, digest,
  storage key and acquisition time against the published descriptors.
- Complete staged-output membership and every `StagedOutput` field, including
  source reference, representation, row/column counts, compressed/uncompressed
  byte counts and hashes, encoding, path, disposition and staging time.
- Every selected output's link to its acquired source artifact.

Only array order is ignored. Missing, extra, duplicate or changed descriptors
fail. No source, cycle, relation, publication ID or expected count is hardcoded.

The real retained candidate matches all **23 source descriptors and 25 staged
output descriptors**. The review reports
`published_source_and_output_descriptors_match=true` and
`candidate_is_exact_referenced_stage=false` separately. It does not know the
lost original record's run-level timestamps, storage observations or checks,
and does not infer them from the later run.

This is a fresh comparison of metadata, not fresh hashing/decompression of the
source bodies, nor another graph or calculation check. `source_bytes_verified`
and `recovery_ready` remain false. The strict
[funding inventory](./funding-recovery-inventory-2026-09-15.md) still reports the
original stage file as missing; this review is not an alias or waiver.

## Prevention and tests

The existing direct publication boundary already preserves exact plan,
acquisition and stage input bytes under content-addressed paths before the
release pointer can advance. That fix and its collision/idempotence tests were
present before this investigation. This turn does not republish an old release.

New tests mutate every staged-output field, including nullable row/column
counts; reject wrong input chains, sources, sizes, paths, duplicate/missing
outputs and unknown JSON fields; and check bounded file reading, exact byte
pins, cancellation and CLI argument admission. Targeted tests, race checks,
static analysis and the existing preservation tests pass.

## Run and retained acceptance

Use an executable accepted by the [offline build gate](../go-build.md):

```bash
bash scripts/run-stage-evidence-review.sh \
  /absolute/storage/root /absolute/accepted-build/first/legal-tender \
  releases/fec/manifests/RELEASE_ID.json RELEASE_SHA256 \
  control/fec/release/stages/CANDIDATE_SHA256.json CANDIDATE_SHA256 \
  /absolute/new-review-directory
```

The runner uses `docker-compose.stage-review.yml`: network disabled, read-only
storage/executable, 512 MiB memory/no swap, 384 MiB Go heap, two CPUs and 64 PIDs.
Only the new audit directory is writable. It runs two fresh processes, compares
complete results, and retains recipe, runtime/build identity, checksums, timings,
memory and explicit status markers. A zero review exit proves descriptor
agreement, not recovery of missing original bytes.

The release gate passed the complete Go suite, negative build-contract checks
and two clean offline builds with identical executable bytes. Targeted race and
static checks passed separately.

| Evidence | SHA-256 |
|---|---|
| Accepted executable | `1def91db696da87d7a86d9070900bcdb7084048f39185c51a119a6b297f97dad` |
| Source archive | `512edb4a76b7e7d2ede350ae8801e51e76036e963025c0ac514702f6e21047b8` |
| Module archive | `71fb2573f01965e16a0939901120736c24433681188af4bb1f436ed0ea535d77` |
| Review ID | `344c69797184c0f23430603f87e9c02b9e90b5541ef3790abea18481a6f8712e` |
| Result and replay bytes | `baf118239fd2e7690311696268d024eec5c291c625ae7c00aba47e8d43f672c6` |

Both review executions and replay comparison exited zero. They took 0.021 and
0.020 seconds, with a container memory peak of 11,911,168 bytes. The proof's two
readiness booleans remain false; these success markers concern metadata comparison.

Retained locations under storage:

```text
builds/go/2026-09-15/1def91db696da87d7a86d9070900bcdb7084048f39185c51a119a6b297f97dad/build/
dumps/audits/fec/release-stage-evidence-review/2026-09-15/attempt-01/
dumps/audits/fec/funding-recovery-inventory/2026-09-15/attempt-02/
```

The review retains separate exact copies of all four input records in `inputs/`,
with their hashes checked against the published proof, plus runner/Compose,
runtime/build identities, checksums and result/replay markers. Copy checks pass.
The source archive includes the new runner and Compose recipe.

The original inventory was also rerun with its original accepted executable and
input specification. Its complete result is byte-identical to attempt 01
(`2f6bfa5d7aa55bff29b6f58a729dba46a3b3847746b9789c31ca3cc78fed6a9a`).
Both executions still exit `1` for the missing original stage; replay comparison
exits `0`. No source or graph artifact changed to make the review pass.

## Remaining decision

Do not hold out the reconstructed metadata as an exact historical replay.
An exact-byte checkpoint of the old generation remains incomplete. A separate
recovery gate can instead start from retained source facts or from raw source
bytes using a newly recorded execution, while preserving this operational-history
gap. Selecting and accepting that recovery boundary remains separate work;
this review neither changes it nor implements that rebuild.
