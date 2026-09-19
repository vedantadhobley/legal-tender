# Complete-cycle Arango receipt participants — 2026-09-12

Status: the complete 2024 import, independent publication verification and full
read-only replay pass. Their explicit success markers and retained evidence
checksums were verified. Storage/checkpoint controls and real interruption/resume
also pass. This accepts the receipt-observation publication, not an integrated
A/B/E generation or terminal attribution.
The [contract](../design/arango-receipt-participant-cycle.md) owns behavior.

## Exact scope

Use the same retained 2024 inputs as the
[compact gate](./arango-receipt-participants-compact-2026-09-12.md), now selecting
all 264,085,606 occurrences. The complete conduit publication expects 14,143,626
qualified non-monetary associations. Reported receipts, memo observations and
unknown identities remain separate from effective money and terminal allocation.

Executable SHA-256:
`9c9fc20ebd7a2420924df729a49b0dee40fbe40cf10d41f56d8e7420737dc8ef`.
Source archive SHA-256:
`8a152eaa5c4959522efa96b33bf4c5f8d0cbaeb03f00cc5d77801fbb4ca95090`.
Cycle projection ID:
`c0536042b7af9875f2e0de2d013f6ffeab31d8cc53755a8f660082753a0518e6`.
Database: `lt_receipt_cycle_2024_c0536042b7af9875f2e0de2d013f6ffe`.

The database is new and isolated. Earlier samples and accepted graph families
are neither overwritten nor dropped. No source pointer changes, source downloads,
Python code or new financial/identity rules are included.

## Resource envelope

Live preflight found about 1.38 TiB available on Arango's backing filesystem and
about 82 GiB host memory available. The existing `legal-tender-dev-arango` is
ArangoDB 3.11 with a 32 GiB container cap; no server setting is changed here.
Its actual Docker data volume is exposed read-only at `/arango-data` in the
temporary runner, not inferred from an unrelated source-storage path.

Admission reserves 256 GiB free space and allows 640 GiB of net filesystem
growth. The complete encoded-document ceiling is 384 GiB. These are explicit
operational bounds, not sample-based promises of physical disk size. The guard
observes shared filesystem pressure before writes and preserves its initial
baseline across retries.

Each runner has a 4 GiB memory cap, eight-CPU quota, `GOMEMLIMIT=2GiB` and
`GOMAXPROCS=8`. The deliberate interruption uses four workers/1,000-row batches;
the resumed full run uses eight workers/2,000-row batches. This validates
layout-independent resume, not a controlled CPU-scaling benchmark.

## Gates

Fixtures pass complete-shard checkpointing, partial-suffix interruption,
worker/batch-varied resume, no writes to checkpointed rows, completed replay,
corrupt-prefix rejection, immutable manifest reuse/rejection, checkpoint
tampering and ancestry rejection, sample-scope rejection, synchronous import,
read-only schema checks, encoded limits, admission failure, filesystem identity,
free-space reserve and shared-growth limits. Existing full-field, source
reconstruction, source-membership, cancellation and cursor tests also pass.

The real control test starts the actual full-cycle publication, requests a
controlled termination after two verified source shards, then resumes the same
database and executable. It does not publish a new bounded sample or discard
the completed prefix. A checkpoint is not a completion manifest.

The first run verified 1,000,000 rows at 54 seconds and 2,000,000 at 90 seconds,
then exited one on the requested cancellation. Its interruption-test wrapper
exited zero. The exact checkpoint accounts for 2,000,000 occurrences and
1,955,254,778 encoded document bytes. Its SHA-256 seal is
`af1000d7119b18488d4ec6a438d7d45a1ea26c62c723337becd938178e4d10e5`.
Independent checkpoint checks confirm complete prefix membership, source scope,
ancestry, digest shapes and the seal. No local completion manifest exists at
this stop. The code-test and interruption containers exited without OOM kills.
The interrupted runner's cgroup peak was 476,246,016 bytes.

The resumed eight-worker run reverified the first 1,000,000 and 2,000,000 rows
with `read_only=true` at 38 and 54 seconds. Its exact checkpoint comparison passed;
it then checkpointed 3,000,000 and 4,000,000 rows at 74 and 93 seconds with
`read_only=false`. The same projection identity and original storage baseline
remain in use. These are observed progress points, not full-cycle results or a
linear throughput promise.

## Retained evidence and completed job

Preflight audit root:
`/storage/dumps/audits/fec/arango-receipt-participant-cycle/2026-09-12/attempt-01/`.
The tested executable, source archive, scripts, logs, interrupted checkpoint,
independent checker and success markers are retained and checksum-verified there.
`SHA256SUMS` covers this frozen preflight evidence; it does not claim to cover
the later files still being written by the full run.

The resumed container was `lt-cycle-graph-full-20260912`. It ran the retained
`full-run.sh` and wrote only a new `full-run/` directory within that audit root.
Read `full-run/run.log` for progress and `full-run/run.exit` for the explicit
terminal status. A zero marker plus the verified immutable publication manifest
is required for publication acceptance; absence of the marker means unfinished.
The result and final memory measurement are `full-run/result.json` and
`full-run/memory-peak.txt`. Do not modify the active launcher or infer success
from process disappearance.

Durable publication root:
`/storage/projections/arango/receipt-participants/cycle-v1/`.
The runner mounts this at `/publication`; the projection's `progress.json` and
eventual `manifest.json` live in its full-identity subdirectory. The initial
growth baseline and limits stay in the checkpoint; rerunning does not reset them.

Full membership, completion, read-only replay and final retention checks now pass.
This establishes complete-cycle receipt-observation graph coverage, not integrated
committee ancestry or terminal amounts.

## Publication verification and replay

The one-shot `lt-cycle-graph-verification-20260912` waited for the import's explicit
`full-run/run.exit` marker. Its launcher starts no graph work while waiting, rejects a
nonzero import status, and fails explicitly if no marker arrives within twelve
hours. The active importer and its retained executable/scripts are unchanged.
At the queue check the importer had verified 53,000,000 rows at 18m 1s;
the waiting verifier used about 1 MiB and no measured CPU. This is a progress
observation, not completion or a runtime estimate.

The verifier is retained in `verification-01/` within the same audit root.
Its `SETUP_SHA256SUMS` covers the frozen checker, fixtures, logs and launch scripts.
The standalone Go checker imports no application code and compares JSON numbers
without floating-point conversion. Its executable SHA-256 is
`ac847a2d58986fe7dd6e26abad552093b5366948bfb30ab7f81761aa7866495a`;
its source SHA-256 is
`f4fa2291a82212dbf1f10cb9d5ca73cb738b0363a3d170f0eb65ddaa0b1e3e0c`.
Fixture/race/vet checks pass, including changed completion evidence, partial or
altered checkpoints, changed scope, all replay evidence fields and integers above
the exact floating-point range. Offline failed-import and missing-marker tests
also pass. An initial fixture-mount harness failure is retained separately under
`negative-initial/`; it is not the real verification result.

After successful import, the runner completed these checks:

1. Recheck the retained import setup checksums and executable, complete source
   scope, participant/conduit manifest identities, qualified association count,
   immutable manifest and full checkpoint. Preserve exact pre-replay snapshots.
2. Run the unchanged executable over the entire population with eight workers
   and 1,000-row batches. Source, prior audit, publication and Arango data mounts
   are read-only; only new audit output and the shared lock directory are writable.
   The existing completed-graph path performs database readback without imports.
3. Require read-only reuse and exact equality of definition, counts, dispositions,
   physical/source digests, encoded bytes, query/source witnesses and storage
   envelope. Require the manifest, checkpoint and original result bytes to remain
   unchanged. Timing and approximate engine storage figures are not equivalence
   operands.
4. Checksum the resulting evidence and publish its explicit terminal marker.

The runner uses the same temporary 4 GiB/eight-CPU cap and 2 GiB Go memory limit.
This is a manual acceptance chain, not Dagster activation or a new standing service.
Do not edit its active scripts.

Read `verification-01/phase.txt` for the stage and `launcher.log` for failures.
Stages are waiting, publication checks, full replay, exact comparison and complete.
Only `verification-01/verification.exit = 0`, passing `publication-check.exit`,
`replay.exit`, `equivalence.exit` and checksum readback establish the full gate.
Those real results now pass; they are not inferred from the checker fixtures.
Expected nonzero markers inside `negative-*` are test evidence, not real-run status.
`FINAL_SHA256SUMS` is produced only after successful full replay/comparison.

## Accepted complete result

Both import and full replay conserve 264,085,606 contributor appearances and
reported-receipt edges, 14,143,626 qualified conduit associations, 23,569 context
entities and 8,584 authorization edges. Unrouted receipts are zero. The 312
missing master references remain explicit; they were not repaired by inference.

The import took 6,016.007 seconds with 676,466,688 bytes peak process RSS.
Read-only replay took 8,023.387 seconds with 608,579,584 bytes peak process RSS.
Both containers exited zero without OOM. The complete encoded-document payload
is 240,083,228,231 bytes; this is not an exact physical database size.

The immutable manifest SHA-256 is
`5440395861205732971d0cd673c1eaf3bc60f213e3bd113a21ee3eace57d053a`.
The final checkpoint SHA-256 is
`a4378477c62ae1c03dd7f8ad3ef9e75d3322e5a017cde48e89088b67e9c7478e`.
`publication-check.json` and `equivalence.json` both report `passed`;
`full-run/run.exit` and `verification-01/verification.exit` are zero.
Every entry in `verification-01/FINAL_SHA256SUMS` passed fresh readback.
The original executable, inputs, manifests and graph remain unchanged.

Next is the reusable [cross-graph connection gate](../design/receipt-candidate-connection.md),
not another full import or a claim that identity/terminal policies are complete.
