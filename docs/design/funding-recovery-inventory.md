# Funding recovery dependency inventory

Status: implemented as a read-only Go command. The
[retained-generation audit](../audit/funding-recovery-inventory-2026-09-15.md)
records the live result and open gaps. This implements dependency inspection,
not the complete [recovery checkpoint](../audit/pre-attribution-checkpoint-2026-09-14.md).
The [fact-start recovery planner](./funding-recovery-checkpoint.md#implemented-metadata-planner)
now derives a separate dependency recipe; it does not weaken this full-ancestry inventory or
clear its missing historical record.

## Input and authority

`inspect-funding-recovery-inventory` accepts an exact base or shared-conduit
generation and a closed, SHA-256-pinned input specification. The checked-in
[historical input](../audit/fixtures/funding-recovery-2026-09-15/inputs.json)
is one example, not a default generation or cycle.

Each reference has a kind, logical publication ID, manifest-byte digest and an
optional storage-root-relative path. Classic references also carry a dataset.
Explicit locators supply locations for audit publications and polymorphic
Schedule A occurrence representations. They cannot replace a parent's pinned
digest or an already declared path. Unknown fields, unsupported versions,
duplicate locators and conflicting identities fail.

The walker uses existing Go manifest types. Each adapter explicitly enumerates
its dependencies. It does not recursively search JSON strings for paths, consult
current pointers, scan directories for candidates or fetch missing files.
Publisher-owned immutable layouts resolve ordinary fact, occurrence, calculation,
bundle, release and release-control manifests.

An ID-only prior-publication reference requires a matching explicit locator or
an exact digest supplied by another dependency in the same walk. A file found at
an expected path cannot supply its own trusted digest. Missing pins stay explicit.

## Coverage

The supported chain includes:

- Base/shared generation and receipt projection completion manifests.
- Participants, conduit decisions, additive group decisions, reference joins
  and endpoint topology, including every declared output file.
- A/B flow reconciliation and readiness inputs; effective Schedule E,
  candidate-reference resolution, resolved aggregation and readiness inputs.
- A/B Parquet shards, E facts/occurrences, classic facts/occurrences, and compact
  or original Schedule A occurrence/index/change artifacts.
- Source releases, raw archives, staged selections and pinned release-control
  plan/acquisition/stage records, including referenced prior releases.

Artifact roots follow the producing code, not one universal assumption:

| Descriptor | Physical root |
|---|---|
| Participant and sorted reference/conduit filenames | Manifest directory's `data/` child |
| A/B reconciliation evidence keys | Reconciliation publication root |
| Source, occurrence and other calculation storage keys | Storage root |
| Release-control byte digests | `control/fec/release/{plans,acquisitions,stages}/` |

Full source-release ancestry may include artifacts for other acquisition cycles
or all-history archives. Their presence in this inventory is provenance, not a
claim that the selected graph covers those periods. No additional cycle is pulled.

## Verification levels

Default mode hashes every opened manifest and only checks blob presence, regular
file type and declared size. `--hash-blobs` also hashes all declared data files;
it is an explicit potentially large scan, not the default audit.

Each node records its verification state, expected identity, observed size/hash,
schema, dependency-enumeration status and any prior publisher-reported checks.
Missing, unreadable, unlocated, wrong-size, wrong-hash, changed-file and invalid
manifest states remain distinct. No missing input is replaced by a newer one.

`file_dependency_inventory_complete` means that the declared typed file chain
was expanded and all files passed the selected verification mode. It does not
mean full checksum coverage in default mode. `all_file_bytes_verified` is separate.
Checks copied from publisher manifests are prior attestations, never fresh checks.
The walker does not rerun domain calculations, decode every data file or independently
prove every manifest's domain invariants.

Executables, producer/schema contracts, release inventories, unpinned discovery
captures, extraction runtimes and live graph identities remain explicit unverified
requirements. The inventory does not find or validate their build environments,
container images, live database contents or retention guarantees.
`recovery_ready` therefore remains false even when the file inventory is complete.

## Safety and replay

The library uses a contained filesystem root, rejects absolute/traversal/current
paths and final symlinks/nonregular files, and detects dependency cycles. Limits
are 8 MiB per manifest, 128 MiB total metadata, 50,000 unique nodes and 800,000
dependency edges. Metadata inspection does not open large data bodies. Explicit
blob hashing is sequential and cancellable; it has no unbounded read buffer.

Nodes, edges and requirements are deduplicated and sorted. Results bind the
input bytes, inspector executable and verification mode. Equal observations
produce identical JSON and inventory IDs. Inspection is not an atomic filesystem
snapshot; concurrent changes can invalidate a later replay.

The command emits a partial JSON inventory and exits nonzero when dependencies
are missing or invalid. Unsafe input, conflicts and resource-limit failures can
stop before a result is emitted. Exit zero is not recovery acceptance.

## Run

Build an accepted executable using the [offline build gate](../go-build.md), then:

```bash
bash scripts/run-funding-recovery-inventory.sh \
  /absolute/storage/root \
  /absolute/accepted-build/first/legal-tender \
  /absolute/inputs.json \
  /absolute/new-audit-directory
```

The runner uses `docker-compose.recovery.yml`: no network or credentials,
read-only storage and executable, 512 MiB memory/no swap, 384 MiB Go heap limit,
two CPUs and 64 PIDs. Its sole writable mount is the new result directory.
It runs two fresh processes, compares their complete JSON and exit codes, and
retains timing, memory, checksums and explicit status files. A successful replay
of an incomplete inventory still returns nonzero.

No graph import, export, snapshot, source refresh, cleanup, pointer update or
retention-policy change is authorized by running this inspection.

## Reviewing a nonidentical historical control record

`review-release-stage-evidence` provides a separate, bounded metadata comparison
when another stage record is retained under its own digest. It verifies the
release/plan/acquisition chain and every published source/output descriptor.
The [historical review](../audit/release-stage-evidence-review-2026-09-15.md)
documents the command, tests, exact pins and real result.

Descriptor agreement is not byte identity. The review never substitutes the
candidate record for the original release reference or changes the funding
inventory's missing-node state. Lost run-level observations remain unknown,
and source-body integrity, graph recovery and user acceptance remain separate.
