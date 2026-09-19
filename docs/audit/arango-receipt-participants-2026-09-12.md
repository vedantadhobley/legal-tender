# Arango receipt-participant gate — 2026-09-12

The [streaming importer](../design/arango-receipt-participants.md) now connects
real source-grain appearances, reported receipts, qualified conduit associations
and exact candidate-authorization context in isolated Arango databases.
This audit does not accept a complete-cycle or integrated production graph.

## Inputs and reproducibility

All input data is retained, read-only 2024 evidence. No new source download,
extraction, counting policy or identity resolution was introduced.

| Input | Exact identity |
|---|---|
| Participant publication | `5cf4f803465c19f8abc0f4cd3eab87b132184bc47536b6aa3c0c5753dbbe0a1e` |
| Conduit publication | `1c597367db6598b0e8705e45b7b2d1cb83e6e0664f7425aa6e5677913a90ff05` |
| Schedule A facts | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Committee master | `e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d` |
| Candidate master | `eb251d4982a5f82f4172cf3d25eda4a908b96af258bc67478689b37c393aa3ec` |
| Candidate linkage | `4327fff8f584be8670174977b8fd5b93da4b2700c98c81915b5acce40cd8b718` |

The master/linkage manifests share the original Schedule A release
`fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`.
They are not the newest, differently released context. Each result pins all
manifest SHA-256 values as well as these logical identities.

Accepted executable SHA-256:
`328f7a1431f2aaa542bacf1635ca2a74955e807ba0d3a6ff30a599a473bb2be8`.
Worker-varied runs use the same executable, not rebuilt binaries. Separate
development probes preserve earlier builds and are not the final gate.

The intermediate `final/` development launcher exited 127 after it was edited
while still running; its log records a shell command-not-found error. Its
successful import payloads remain comparison evidence, but that launcher is
not accepted as a complete gate. The later `accepted/` runs used the stable
launcher and final executable; all four invocation markers, the outer gate,
and the independent checks are zero. Do not edit an active launcher or infer
completion from process disappearance.

## Gate status

Final-source fixture tests include source-grain identity, signed/zero/unknown
amounts, unresolved recipients, non-monetary conduit links, batch-layout
equivalence, borrowed-buffer ownership, worker cancellation, duplicate/missing
decision membership, corrupted trailing artifact bytes, publisher locks, cursor
cleanup and all-field readback failures. Full Go tests, focused race checks,
vet, module tidiness and formatting pass.

Both final real imports, their read-only replays and durable retention pass.
Every selected occurrence has one appearance and one receipt edge in these
samples. Unresolved conduit dispositions remain on the appearances.

| Selected ordinal range | Receipt edges | Qualified conduit edges | Import + readback stream | Whole invocation | Peak process RSS |
|---|---:|---:|---:|---:|---:|
| 1–100,000 | 100,000 | 6,855 | 8.954 s | 32.981 s | 440,205,312 B |
| 1–1,000,000 | 1,000,000 | 103,051 | 29.586 s | 53.017 s | 459,010,048 B |

These are overlapping samples, not 1.1 million distinct source occurrences.
Both include all 8,584 same-cycle authorization-context relationships. The
smaller graph has 17,255 entities; the larger has 18,244. Both preserve 299
missing-master entities, including entities referenced by that full authorization
context. Their absence does not delete receipt or authorization evidence.

The 100,000-row projection ID is
`cb168d27f033a7966aa9e5bec626a26992881d1aa071bd3379cc86b6ed01f997`.
The million-row projection ID is
`f22c2c9e0f4f57f45b2cbfced779d329898b76c8b0fc69b4b234e889aa436678`.
Exact database names and manifest digests are in `accepted/summary.json` and
the corresponding result files in the retained audit tree.

Initial import uses four workers and 1,000-document batches. Read-only replay
uses one worker and 257-document batches. It conserves the exact projection ID,
all per-collection document-value digests, counts, source checks, path witnesses
and encoded payload bytes. The smaller replay takes 36.380 seconds; the larger
takes 94.396 seconds. These are different operations and batch sizes, not a
controlled CPU-scaling benchmark. All four invocations verify the full conduit
artifact and source backing; only the selected participant ranges are imported.

The source-selected candidate and conduit paths pass. Graph/source checks open
original ordinals 1, 109 and 110 and preserve all source fields. Cross-build
checks also reproduce the initial sample payload exactly after removing repeated
backing verification and adding stricter same-byte manifest loading.

Exact encoded JSON payload is 182,494,233 bytes for the smaller sample and
1,760,113,025 bytes for the million-row sample. Most belongs to appearances
that duplicate the compact participant record; this is concrete evidence for
testing leaner graph documents before full-cycle publication. No retained source
field needs to be discarded to remove that duplicated graph storage.

The final runner's cgroup peak is 923,455,488 bytes. Process RSS above excludes
other processes and kernel-charged cache. No OOM or failed final gate occurred.

## Durable retention

The new retained audit root, relative to `/storage`, is
`dumps/audits/fec/arango-receipt-participants/2026-09-12/attempt-01/`.
It contains exact executables, source snapshots, import/replay results, complete
test logs, comparisons and `SHA256SUMS`. All copied files pass checksum readback;
`retention.exit`, `accepted/code.exit`, `accepted/gate.exit` and
`accepted/equivalence.exit` are zero. The working copy is
`/tmp/legal-tender-receipt-graph.Ta4DZm/`.

Final Go source archive SHA-256:
`929f8420715e49d564e6aadf88a6d740b98cf6b2f0efaa635b6b1d97f709d933`.
The source archive matches the working Go files after acceptance. Earlier
development sample databases remain preserved; only the two final identities
above are accepted by this gate.

## Resource and operational observations

Temporary workers use an eight-CPU quota, a 4 GiB container cap,
`GOMAXPROCS=8` and `GOMEMLIMIT=2GiB`. The Arango service is the existing
`legal-tender-dev-arango`, image `arangodb:3.11`; no service upgrade, restart,
host port or standing memory change occurred.

Live inspection found a 32 GiB Arango container cap and 28 GiB detected-memory
override. The global budget document's older 24 GiB entry does not describe
that live state. Reconcile its ownership separately; this sample does not
establish a new cross-project budget convention.

The initial 100,000-row probe imported/read back in 9.115 seconds but took
57.707 seconds overall. Repeated inspector calls rehashed the complete fact
backing for each witness. The final scoped inspector hashes it once per
invocation, retaining the per-shard and full-source comparisons. Different
executables identify the initial and final probes; their graph payloads must
match for the same range.

Collection figures are retained as engine estimates. Their sample values are
not a reliable per-cycle storage extrapolation. Exact encoded payload sizes
are recorded separately; database figures include different storage/index costs.

## Remaining acceptance

Complete-cycle layout/storage acceptance, all 264,085,606 source occurrences,
existing committee ancestry integration, four-cycle A/B rollout, employer and
corporate resolution, and unattended refresh remain separate open work.
The complete conduit calculation is an input; importing a sample of its positive
associations does not publish all 14,143,626 associations to Arango.

No terminal policy, pooled-money allocation, new memo money or production
generation pointer is introduced. Existing accepted graphs and source files
remain unchanged. No cleanup or database deletion was performed.
