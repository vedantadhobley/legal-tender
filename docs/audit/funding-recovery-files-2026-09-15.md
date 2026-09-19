# Scoped recovery file verification — 2026-09-15

## Result

The [Go file verifier](../design/funding-recovery-checkpoint.md#implemented-scoped-file-verification)
verified every selected execution and comparison file for the pinned shared-conduit
generation. Both fresh processes returned zero and produced byte-identical JSON.
This is a file-integrity gate, **not a completed recovery checkpoint**.

| Check | Result |
|---|---|
| Selected files | 756, all SHA-256 verified |
| Selected and verified logical bytes | 29,228,221,182 |
| Execution inputs | 460 files; 24,664,376,748 bytes |
| Comparison evidence | 296 files; 4,563,844,434 bytes |
| Historical inventory | Unchanged states: 43 metadata hashes, 1,339 size-only checks, one missing record |
| Dependency plan | Complete; execution remains blocked |
| `recovery_ready` | False |

Selection comes from the typed planner and existing producer dependencies, not
hardcoded file lists, cycles or entities. Historical-only bodies were not hashed.
The missing original stage record remains missing. The independent plan run's ID
and canonical JSON digest exactly match the successful verification's plan pins.

## Exact pins

Input specification:
[retained generation and locators](./fixtures/funding-recovery-2026-09-15/inputs.json).

| Artifact | SHA-256 or content identity |
|---|---|
| Input specification SHA-256 | `7735ec48a129bc9336c3eaf1962666fedab48f6c88bc6220cedab22a184f5d5e` |
| Accepted executable SHA-256 | `b447a35a26664c8c4948f70746d82fc5e326c339cbee74bf60598a854a8419bc` |
| Source archive SHA-256 | `a25440ffaa6a20e036c3c02504153deec46d994d11101510fcbb83a9fc9c6ba1` |
| Module archive SHA-256 | `71fb2573f01965e16a0939901120736c24433681188af4bb1f436ed0ea535d77` |
| Verification ID | `113fb82017a9f35dfb0df35d46dad6624b3427bb8d86eca9b9460695e5643e92` |
| Result/replay JSON SHA-256 | `2a37d16ccbc3035534540efad3bfefca882910fe1c1a59b777107aa51d94943c` |
| Fresh plan ID | `4cfc51ab45b476f6da6ebe0890586fb919388d21ca1c6e67f82645a3f108b16a` |
| Plan canonical JSON SHA-256 | `59e1126b54aeb5c3e83b9f326a608e757df85e7562ce48bdcb435dc27efa0cf4` |
| Plan result/replay file SHA-256 | `90eae86e0dbb0f406c4723e0d7f0556ca4bd1f6c06c0c5e3bc195c2ad51ad7bf` |
| Historical inventory ID under this build | `c2df532cfb6abefe01aca90bf06413bc23065f09d258a3132a0a779824249732` |

New build identity changes the inventory/plan IDs. It does not rewrite historical
publications or change the original inventory's incomplete state.

## Resource admission and repeatable command

Before launch, the source/output filesystem reported 1,337,443,594,240 available
bytes and the host reported 77,224,472 KiB available memory, with no swap.
Only small new build/audit outputs were written. No source bodies were copied.

```bash
bash scripts/run-funding-recovery-inventory.sh \
  /absolute/storage/root \
  /absolute/accepted-build/first/legal-tender \
  docs/audit/fixtures/funding-recovery-2026-09-15/inputs.json \
  /absolute/new-audit-directory verify 30000000000 2
```

The existing inspection container retained its 512 MiB/no-swap memory cap,
384 MiB Go heap target and two CPUs. Networking was disabled; storage, executable
and specification mounts were read-only. The explicit ceiling was 30 billion
selected logical bytes, with two hashing workers.

The two full checks took 11.464 and 11.608 seconds. Cgroup peak was 536,875,008
bytes, including charged file cache; this is not a measured Go heap peak or a
cold-storage throughput benchmark. Both processes completed successfully.

The reproducible build gate passed complete Go source tests, negative build
checks and exact executable comparison. Prepare took 3.289 seconds; first build
plus tests took 66.377 seconds; second build took 17.037 seconds. First-build
cgroup peak was 2,074,025,984 bytes, under the existing 4 GiB cap.

## Failure and replay tests

Fixture tests cover synthetic 2022/2024 plans, selected execution/comparison
roles, absent historical-only bodies, path deduplication, distinct copies with
equal digests, one/eight-worker equality, bad pins and incomplete dependencies.
Same-size corruption, truncation, missing files, symlinks, FIFOs, replaced inodes,
concurrent growth and cancellation cannot pass. Growth reads stop at expected
size plus one byte. These are verifier tests, not isolated graph-rebuild fixtures.

CLI tests cover required input/resource pins, unknown and unsafe flags, partial
JSON, exact replay identity checking and no storage writes. Full source tests,
targeted race tests, vet, shell syntax and touched-document checks pass.

The real one-byte-ceiling test returned `byte_ceiling_exceeded` before any
selected-file hash. All 756 entries remained `not_checked`, with zero verified
bytes. Its two processes returned one, and replay comparison returned zero.
Result SHA-256:
`c6b3dc8aea74bf6a39f4c11387b1cff36a66af1999d68f606f21d6f7f35117ac`.
The real metadata planner also retained its expected blocked/nonzero status.

## Retained artifacts and remaining boundary

Under the configured storage root:

- `builds/go/2026-09-15/b447a35a26664c8c4948f70746d82fc5e326c339cbee74bf60598a854a8419bc/build/`
  retains the accepted source/module archives, equal executables and build evidence.
- `dumps/audits/fec/funding-recovery-files/2026-09-15/attempt-01/`
  retains inputs, command/admission, results, replay, timings, memory and checksums.
  Its `plan/` and `budget-rejection/` subdirectories retain the companion gates.

New durable copies passed every original checksum and explicit exit marker.
`RETENTION.sha256` additionally binds runner/runtime/configuration files and
companion checksum inventories; **it does not enforce retention**. Temporary
copies were not removed. The compiler image remains locally available by digest,
not newly archived as an offline runtime package.

Current Go cleanup paths remove temporary/workspace files, not published-data
generations. Permanent checkpoint protection needs an enforced storage/cleanup
boundary and failure tests; no unused guard is claimed to protect existing data.
Next are runtime/input protection and the restricted fixture rebuild/comparator.
Normal source decoding, empty-target graph reconstruction, complete comparisons,
raw-to-fact recovery and user interpretation acceptance remain separate gates.
No source refresh, graph write, current-pointer update, financial rule, entity
resolution rule or terminal-attribution policy changed.
