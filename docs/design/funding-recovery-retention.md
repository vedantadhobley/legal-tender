# Removed funding-recovery input copying

Status: removed with user approval on 2026-09-15 after the
[scope correction](./pre-attribution-review.md). The command, capture/seal/cleanup
implementation, dedicated unit/container tests, Compose fixture, runner and
CLI/Make/build bindings are no longer shipped. Copy-only helper indirection was
removed from the retained read-only input reader and file verifier.

No source data, graph, published result or historical audit was removed. The feature
had no domain/Dagster consumers and had never captured a real generation. Existing
read-only inventory, planning, file verification and reproducible builds remain.
Further [recovery work](./funding-recovery-checkpoint.md) is deferred.

## Retained source and verification

Before removal, every removed file matched the bytes in the existing build archive:

```text
storage root: ~/workspace/data/legal-tender
archive: builds/go/2026-09-15/93e4d0afadfa5cf0cfda292f4c6b639cf1d74cbd9d7bbf99169188df78dcf6b8/build/source.tar.gz
archive SHA256: 2e6c38c3cd92edf60e2400e493e21e076d429dc43cb30e992b39e7d7441ed99d
```

Removed members: `internal/recovery/funding/retention.go`,
`retention_capture.go`, `retention_storage.go`, `retention_test.go`,
`retention_container_test.go` in that same package;
`internal/app/cli/funding_recovery_capture.go` and its `_test.go`;
`docker-compose.retention-test.yml`; `scripts/test-retention-integration.sh`.
These are recoverable source files, not deleted production state.

Full Go regression tests and `go vet ./...` passed after removal. Targeted race
tests passed for the surviving recovery tools and candidate-resolution package.
Shell syntax, release-source allowlist paths and local documentation links pass.
The historical
[fixture audit](../audit/funding-recovery-retention-2026-09-15.md) remains intact.

## Historical design — not current commands or next steps

The remainder records the removed implementation. Commands below require its
archived source; do not run them against the current tree or restore them by default.

## Boundary

The existing pipelines clean temporary/workspace files, not published generations.
An unused pin list would not protect anything. The recovery capture therefore
creates independent copies in a separate store, exposes completed input copies
read-only to consumers, and provides no sealed-checkpoint deletion operation.

Protection covers these supported tools and the read-only consumer mount. It does
not protect against a host administrator, a writable mount supplied outside this
contract, hardware loss, or an external command that removes the store. The fixture
teardown deliberately removes its generated volumes; it is not a production policy.
There is no retention expiry, scheduler, production mount change or garbage collector.

```text
retention-root/
  checkpoints/<new-name>/
    execution_input/<original-storage-relative-path>
    comparison_evidence/<original-storage-relative-path>
    evidence/inputs.json
    evidence/plan.json
    seal.json
    .capture.lock
  scratch/<new-name>/
    owner.json
    payload.partial
```

Original facts and historical outputs keep their existing identities and paths
inside their separate role trees. A file needed in both roles gets two independent
copies. Comparison evidence must remain unavailable to future rebuild producers;
the full retention-store verification probe is not itself a producer.

The exact input specification and generated plan are retained with their own byte
pins. The plan preserves the full provenance inventory, including its missing
historical record. Historical-only bodies are not copied. This store cannot replay
the original full-history inventory's file-presence observations from its own files.

## Capture contract

`capture-funding-recovery-inputs` reads the exact generation/locator specification
and derives a fresh typed plan. An arbitrary saved recipe is not capture authority.
It rejects incomplete dependency planning, missing pins, unsafe names, overlapping
declared source/store roots and existing checkpoint directories. It never resumes,
adopts, overwrites or removes an existing checkpoint, including a failed attempt.

The caller supplies a new checkpoint label, maximum logical copy bytes and a
positive free-space floor. Admission includes selected role copies, retained
evidence, the seal and a 1 MiB operational allowance. This is not a physical-block
quota or an exclusive disk reservation; real-run admission must separately budget
directory/allocation overhead and other writers. Free space is checked before
copying and on each bounded write. A later breach aborts without success.

Capture uses a single 128 KiB copy buffer. It hashes the original bytes during
copying, rejects observed source changes, syncs the destination, and performs full
readback against the original digest and size before placing each retained file.
No hard links to originals, implicit reflink savings, compression, normalization
or source-file edits are used. New directory entries and output files are synced.

The closed `legal-tender.funding-recovery-retention.v1` seal pins every retained
file, the input/plan/build identities and the retention policy. It is written only
after every copy passes. Readers require an exact seal digest, validate the closed
schema and self-identity, and reject unknown roles/fields or inconsistent pins.
`ReadRetention` validates metadata only; consumers must verify body hashes again
before a rebuild. A seal file's mere presence does not establish completeness.

The CLI reports zero only after sealing and scratch cleanup succeed. Cancellation
or failure returns nonzero. A failure before sealing leaves diagnostic files and
cannot be retried under that label. A failure after sealing can leave a valid
checkpoint plus scratch; the result reports those states separately. Removing
failed checkpoints is a future explicit, reviewed operation, not an automatic path.

## Cleanup contract

Capture invokes the same `CleanupCapture` boundary that fixtures exercise. It
acquires the capture lock, validates the exact seal and the closed scratch-owner
record, and removes only `scratch/<checkpoint-name>`. During capture the caller
already owns that lock. Cleanup refuses an active writer, missing/corrupt inventory,
bad pin, unknown field, wrong/missing owner, unsafe path or cancellation.

Removal uses a directory-confined scratch handle. There is no arbitrary path
parameter, sealed-checkpoint removal mode or fallback that treats unreadable
retention metadata as an orphan. A failed cleanup leaves both scratch and retained
inputs in place. Retained files and their directory trees are never cleanup targets.

## Commands and verification

The direct Go command requires all operational bounds explicitly:

```bash
legal-tender pipeline fec capture-funding-recovery-inputs \
  --storage-root /source \
  --inputs /inputs.json --expected-inputs-sha256 INPUT_SHA256 \
  --retention-root /retention --checkpoint NEW_NAME \
  --max-copy-bytes COPY_CEILING --free-floor-bytes FREE_FLOOR
```

No production capture runner or real-data copy is accepted yet. Before that run,
bind the original storage and executable/specification read-only, expose only the
new retention target writable, and admit memory, disk and mount costs explicitly.
Keep durable data under the project's configured storage layout. The path labels
above are container interfaces, not a new host-directory convention.

Run the fixture-only kernel-boundary proof with:

```bash
make test-retention-integration
```

Its archived `docker-compose.retention-test.yml` uses the pinned local
compiler image, no network, no capabilities, read-only roots, a 4 GiB/no-swap cap,
2 GiB Go heap target, four CPUs and a 1 GiB executable temporary filesystem.
Preparation and probe run sequentially. Only generated volumes are mounted; no
source corpus, credentials, database or active graph is accessible.

Preparation captures a synthetic storage-contract fixture. A separate process
then verifies all copies without the originals. Its store is read-only and its
scratch mount alone is writable. Direct unlink, overwrite, chmod, rename and file
injection must fail with `EROFS`; valid scratch cleanup must still succeed, and
bad-pin cleanup must preserve scratch. This is not the isolated publisher/graph
rebuild or a complete source-schema fixture.

The runner retains capture metadata, logs, runtime identity, timing, container
exit/OOM states, checksums and explicit final status. It rejects existing project
resources and verifies that its fixture containers/volumes are gone after teardown.
Unit tests additionally cover two synthetic cycles, independent inodes, changed
originals, deterministic replay, corrupt copies, disk pressure, cancellation,
closed schemas and fail-closed cleanup. The
[acceptance audit](../audit/funding-recovery-retention-2026-09-15.md) owns results.

## Former proposed next boundary — deferred

Bind and retain the exact producer build inputs and offline compiler/Arango runtime
images, define the real store's enforced consumer mounts, then admit real capture.
The v1 seal explicitly reports `offline_runtime_included=false` and
`recovery_ready=false`; source/module build evidence elsewhere is not runtime closure.
Next prove the normal publishers and complete comparator in an empty restricted
workspace and isolated database. These storage fixtures do not pass that gate.
