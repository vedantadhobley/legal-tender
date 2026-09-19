# Recovery input retention gate — 2026-09-15

## Result and scope

Historical result: the [implementation and fixture harness were later removed](../design/funding-recovery-retention.md)
with user approval on 2026-09-15. The verified source archive and all audit evidence
remain; commands and source filenames below refer to that archived implementation.

The [input retention implementation](../design/funding-recovery-retention.md)
passes its unit, container-boundary and build gates. It creates independent
verified copies, separates execution inputs from comparison evidence, rejects
existing targets and restricts cleanup to owned capture scratch.

The real funding generation is **not copied into this store yet**. These fixtures
do not execute the domain publishers, reconstruct graphs, archive runtime images,
or accept the full recovery checkpoint. No financial or identity rule changed.

## Tests and enforced boundary

Unit tests cover synthetic 2022/2024 storage plans, full copy/readback, separate
inodes, unchanged copies after original-file changes, deterministic seals across
new destinations, existing-target rejection, corruption, copy-budget rejection,
disk-floor rejection before and during a copy, cancellation and path aliases.
Closed metadata rejects unsupported fields, versions, roles, duplicates and
inconsistent totals. Missing/corrupt seals, wrong pins, missing/wrong owners,
active writers, unsafe names and cancellation all block scratch cleanup.

The archived `docker-compose.retention-test.yml` container fixture runs preparation
and probe in separate, sequential network-disabled containers. Both use the pinned
local compiler, no capabilities, read-only container roots, a 4 GiB/no-swap cap,
2 GiB Go heap target, four CPUs and a 1 GiB executable temporary filesystem.
No original source corpus, credentials, database or live graph is mounted.

Preparation captured 14 fixture files containing 20,936 payload bytes. Its fixture
seal is `7cd756fb7a15b899787fd989f41926880a9f5e8249bb1ff3b666ab6af5d01640`.
These are synthetic storage-contract inputs, not complete source/publication
schemas or an accepted real generation. The capture report SHA-256 is
`804c98a25b27629479d064b913f919f5e6514925c85e37af4481699b244ae051`.

The fresh probe read all retained bytes with the original files absent. Its store
was read-only and only scratch was writable. Direct unlink, overwrite, chmod,
rename and file injection each failed with `EROFS`. A wrong seal pin preserved
scratch; valid guarded cleanup removed scratch and left every retained byte intact.
Both container exit states were zero with `oom=false`.

Preparation, including test compilation, took 16.219 seconds and peaked at
878,141,440 cgroup bytes. Probe took 0.653 seconds and peaked at 118,378,496 bytes.
Final teardown verified zero remaining fixture containers and volumes. Test data
was deliberately removed and is reproducible from the code; logs and capture
metadata remain. This teardown is not a production retention policy.

## Build and real-file regression

Full Go source tests, targeted race tests, vet, shell syntax and documentation
checks pass. The [offline build boundary](../go-build.md) produced two identical
executables and passed its negative build tests.

| Artifact | SHA-256 |
|---|---|
| Accepted executable | `93e4d0afadfa5cf0cfda292f4c6b639cf1d74cbd9d7bbf99169188df78dcf6b8` |
| Source archive | `2e6c38c3cd92edf60e2400e493e21e076d429dc43cb30e992b39e7d7441ed99d` |
| Module archive | `71fb2573f01965e16a0939901120736c24433681188af4bb1f436ed0ea535d77` |

Build preparation took 3.191 seconds; first build plus tests took 65.517 seconds;
the second build took 17.080 seconds. First-build peak was 2,101,325,824 cgroup
bytes, below the existing 4 GiB cap. The fixture gate runs separately from the
default source suite; default tests do not silently imply container acceptance.

Because capture now shares the original verifier's confined read/hash logic,
the accepted executable reran the real-file verification gate. All 756 files and
29,228,221,182 bytes passed in two fresh processes, taking 11.249 and 11.385 seconds.
Their complete per-file results also match the preceding accepted verifier build.
Only build/plan identities change; historical publications remain unchanged.

- Verification ID: `60fc85076104ed0cc01e89c312e6e985951994cd61c82260d8e222c174611f06`.
- Result/replay SHA-256: `7e50c15ff269c724de14907e6e911bed1385b22149883be4c8b2c308aae49ac4`.
- `selected_file_bytes_verified=true`; `recovery_ready=false`.

## Earlier fixture failures

The first attempt failed before testing because an unquoted comma split a tmpfs
option. The next could not execute its Go test binary on a non-executable tmpfs.
Both configuration issues were corrected without changing checkpoint permissions.

An intermediate run passed the storage probe, but its original teardown left
stopped one-off containers and their volumes. Compose returned zero while reporting
volumes still in use, so that run's old final marker is not teardown acceptance.
Its project-scoped cleanup was completed separately. The final runner removes
owned one-off containers and explicitly checks both remaining-container and
remaining-volume lists before reporting success. Earlier logs and correction
records are preserved; historical markers were not rewritten.

## Retained evidence and next boundary

Under the configured Legal Tender storage root:

- `builds/go/2026-09-15/93e4d0afadfa5cf0cfda292f4c6b639cf1d74cbd9d7bbf99169188df78dcf6b8/build/`
  retains the accepted build inputs, equal executables, tests and checksums.
- `dumps/audits/fec/funding-recovery-retention/2026-09-15/attempt-01/`
  retains `fixture/`, `verifier/` and earlier diagnostic attempts. The fixture's
  `SHA256SUMS` and `TEARDOWN.sha256` bind test and cleanup evidence; the audit's
  `ARTIFACTS.sha256` binds all retained files. Copied checksums and exit markers pass.

No real source files were copied or removed, no checkpoint was deployed, and no
graph or current pointer changed. Retained build/audit files are not themselves
a deployed checkpoint-retention boundary.

Next bind and retain offline runtimes and production capture/consumer mounts,
admit real input capture, and prove the normal publisher/comparator chain in an
empty restricted workspace and database. User interpretation review and complete
raw-to-fact recovery remain separate requirements.
