# Fact-start recovery planning — 2026-09-15

Status: metadata planner, tests, offline build and retained-generation replay
pass. **The recovery rebuild has not run.** No source body was rehashed or
downloaded, no graph was accessed, and no current publication was changed.
This implements the first step of the
[fact-start checkpoint](../design/funding-recovery-checkpoint.md).

## Exact subject and build

The [input specification](./fixtures/funding-recovery-2026-09-15/inputs.json)
is unchanged, with SHA-256
`7735ec48a129bc9336c3eaf1962666fedab48f6c88bc6220cedab22a184f5d5e`.
It selects the same 2024 shared-conduit generation and exact base-generation
locators as the [original inventory](./funding-recovery-inventory-2026-09-15.md).
Neither cycle nor these historical identities are defaults in the planner.

Accepted executable:
`5235e7891773e9fc0f2438b5df8363987c7263f45bc6c09b2bee08a9f3a9ce04`.
Source archive:
`233ba68ac9f6cd601b04e3bf4e7b949b6dce7c1c37a396a9cc88f423a7b2274b`.
Module archive:
`71fb2573f01965e16a0939901120736c24433681188af4bb1f436ed0ea535d77`.

The [existing Go build gate](../go-build.md) passed full CGO-disabled source
tests, negative build tests, checksums and byte comparison of two fresh offline
builds. Preparation took 3.243 seconds; the first build plus tests took 63.976
seconds; the second build took 17.098 seconds. The largest recorded build cgroup
peak was 2,040,553,472 bytes, within the unchanged 4 GiB limit.
Targeted planner/CLI race tests, package vet and runner shell syntax checks also
passed. This is not an offline rebuild of every historical producer.

## Planner result

Plan ID:
`e36e6d8e501decbb12f2f5f3947a21945b6600b995b1e46f8e34271b1dc1bb71`.
Result and replay byte SHA-256:
`af5e23945792a69a6d03d8c4038b84ec82495710a553fa08dab42e6fe3d61c22`.

| Observation | Result |
|---|---:|
| Dependency-first producer/graph/generation steps | 17 |
| Execution-input files, including metadata | 460 |
| Execution-input logical bytes | 24,664,376,748 |
| Historical comparison files, including metadata | 296 |
| Comparison logical bytes | 4,563,844,434 |
| Remaining historical-only inventory nodes | 627 |
| Unknown required file sizes | 0 |
| Metadata dependency plan complete | true |
| Execution ready | false |

The execution files include A/B/E facts, six classic reference fact sets, their
required metadata, three release manifests and selected reference-proof source
archives/members. The planner does not promote all artifacts in a coordinated
release into execution inputs. The large historical A/B raw dumps remain
provenance for this fact-start boundary. This does not permit deleting them.

These figures are path-deduplicated logical sizes, not fresh body checksums,
physical disk usage, input-copy requirements or peak reconstruction capacity.
Metadata-only planning cannot establish that 24.7 GB of free space would suffice.

The recipe explicitly includes the receipt, A/B and outside-spending graph
constructors, generation verification, shared-group calculation and extension.
Every rebuilt-output binding names an earlier step. Old calculation/graph
metadata and files remain comparison evidence, not reusable execution seeds.
Normal producer/policy validation and the recovery-specific comparators are still
required before accepting reconstructed results.

## Blockers and preserved history

The result records:

- 722 required/comparison data files awaiting full body verification.
- 55 selected-input/output runtime or contract requirements that the provenance
  metadata declares but does not locate and verify for reconstruction.
- Unbound replay build and Arango runtime, unenforced retention, unadmitted
  workspace, unimplemented isolated executor and unimplemented recovery comparators.

There were no unsupported-producer, ambiguous-dependency, missing-required-file
or unclassified-role errors for this subject. That is dependency-plan coverage,
not proof that the future executor will work with only those mounts.

The embedded strict inventory still reports 43 freshly hashed metadata files,
1,339 size-checked data files and the same missing original staging record.
Its ID under this new inspector executable is
`fd67d041ef958bc497312cfbfc93ef4cc6b721f77167284125cf54957585228d`;
canonical JSON SHA-256 is
`cf1e905598f150f2981513d8ec445a8cab8c71d7a2e8eaa2e2d717a834134666`.
An independent invocation of the ordinary inventory command produced the same
complete inventory object. No prior inventory output was rewritten.

The [lost stage record](./release-stage-evidence-review-2026-09-15.md) is
historical-only for the selected normal-reader paths. It stays missing under its
original digest. A later executor must prove that undeclared reads fail; it cannot
introduce a bypass if a required reader instead needs that record.

## Replay, tests and retention

Both real planner processes used the unchanged 512 MiB/no-swap inspection
container, 384 MiB Go heap cap, two CPUs and no network. Storage and executable
were read-only; only new audit output was writable. Runs took 0.105 and 0.098
seconds, with a 28,184,576-byte cgroup memory peak.

Both commands exited 1 because execution remains blocked. Complete JSON and exit
statuses matched; `replay-check.exit` is 0. The independent inventory pair also
kept its incomplete/nonzero state and passed replay comparison.

Tests cover two-cycle metadata dependency ordering, required versus historical
inputs, unavailable files, unknown producer/schema versions, ambiguous/missing
roles, unsafe/current paths, cycles, changed metadata and cancellation. Reference
fixtures check exact archive/member selection, wrong pins, duplicate members and
exclusion of unrelated-cycle archives. CLI tests reject database, hashing and
missing-stage-override flags, preserve partial JSON and reject changed replay IDs.
These are planner tests, not a completed isolated graph reconstruction.

Retained paths, relative to storage:

- `builds/go/2026-09-15/5235e7891773e9fc0f2438b5df8363987c7263f45bc6c09b2bee08a9f3a9ce04/build/`
- `dumps/audits/fec/funding-recovery-plan/2026-09-15/attempt-01/`
- The audit's `inventory-check/` contains the independent inventory pair.

Build/audit checksums and explicit exit markers were verified after retention.
`RETENTION.sha256` additionally pins the audit recipes, runtime identity and
status files. The first copy could not preserve host ownership under the capped
container; its copied build bytes passed checksums unchanged. Audit copying then
preserved modes/times without requesting ownership changes. No old files were
removed or replaced. This retains this tool's evidence; it does not enforce
retention of the future recovery dependency set or preserve compiler/server images.

## Next implementation

Bind the replay runtime and isolated input/output workspace, implement exact
execution-input verification and retention protection, then exercise the normal
publishers and per-schema comparison in disposable fixtures. Only after those
gates and real capacity admission should the full fact-start rebuild run.
Raw-to-fact recovery, historical byte recovery and user interpretation acceptance
remain separate from this planner result.
