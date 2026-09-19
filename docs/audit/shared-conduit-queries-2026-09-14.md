# Shared-conduit path and neighborhood gate — 2026-09-14

Status: accepted. Implementation, isolated full-chain integration, full Go
regression, targeted race/static checks, the real read-only query gate and
byte-identical fresh-process replay pass. All explicit exit markers and input,
setup and final artifact checksum checks pass.

## Scope

The [consumer contract](../design/shared-conduit-generation.md#consumer-boundary)
adds an explicit shared family without changing base graphs, source facts,
financial selection or terminal rules. It exposes prior/new decisions, complete
group evidence and both original source occurrences. No graph is reimported.

The full-chain synthetic integration test publishes fixture source contracts,
calculations, base graphs and an extension. It checks disjoint family routing,
pagination, source evidence, unchanged old entry/receipt, paths through both
committee ledgers, fresh reader and CLI equivalence, automatic gate replay,
foreign scope rejection, corrupted lookahead and corrupted base appearances.
The isolated stack uses no production storage and has explicit container caps.

Passing integration evidence: `/tmp/legal-tender-window-test.Cs4RH7LT`;
`tests.exit=0`, `run.exit=0`. Targeted `-race` tests pass for `receiptgraph`,
`fundinggeneration`, `cli` and `receiptconduits`. The complete `go test ./...`
regression suite also passes. `go vet` passes for changed projection and CLI
packages; formatting, shell syntax and focused documentation-link checks pass.

## Live inputs and retained runner

Retained run directory:
`/storage/dumps/audits/fec/shared-conduit-queries/2026-09-14/attempt-02`.

- Extended generation: `f1c9a89e46ae1bdc291cadf8299230adfb30ca52ddc81932aa6f14afee5a6ec1`.
- Extended file SHA-256: `b6e65ec8d23b53dd9316131d67980b7a7272f51e69134fd276539a3e5f03c935`.
- Extension projection: `f704616339eb95c36c5d47ed66956aeba9ede707bb81ac2e3bfe866a2b7c4465`.
- Consumer executable: `4cd34507948e28a16b339aa8cdb165d28f029d6ddea9dc6941e8b15ee396ec8a`.
- Retained source archive: `c7a8f0ab9ecb13d316f1e58674e8fa48526d4c4d2bad6170eb181714f2679b01`.

The earlier `attempt-01` also passed its query and fresh replay. Code review then
aligned the shared descriptor's ledger, grain, amount and overlap labels with
the existing conduit family, and added the consumer build to the gate envelope.
The final source revision adds regression assertions for those labels and all
three candidate endings. `attempt-02` verifies that exact final executable;
the first attempt is retained as development evidence, not final acceptance.

Rebuilding the final source archive with the original Linux/amd64 Go 1.26.5
toolchain, `CGO_ENABLED=1`, `-trimpath` and `-buildvcs=false` produces the identical
consumer executable. This proves same-toolchain replay, not cross-environment
bit reproducibility; the production build-environment task remains open.

The [runner](../../scripts/run-shared-conduit-query-gate.sh) pins all explicit input
manifest bytes, arguments, executable and source. Source storage mounts read-only;
only the new audit directory is writable. The one-shot container has a 4 GiB
memory/no-swap cap, 2 GiB Go heap limit and eight CPUs. No standing service cap
changes. The configured Arango credential passes privately through the environment
and is neither an argument nor a retained artifact.

## Verification boundary

The accepted gate ID is
`9ee761871e58227d0aa7689909802bba8fa7ae3aaf0c2dbf93f0f23c236a282e`.
The selected neighborhood and continuation each return one distinct source-backed
shared edge. The shared entry produces its one-edge association path; appending
a selected committee observation produces a two-edge path. The same occurrence
remains unavailable through the original conduit family. Both results keep the
common `schedule_a_occurrences` overlap group and unchanged evidence-only semantics.

The first final-binary gate took 188.125 seconds; the independent expected-ID
replay took 184.696 seconds. It ran from 22:47:35 through 22:53:48 UTC.
`gate.exit=0`, `replay.exit=0`, `exit-status.txt` reports `exit_code=0`, and the
container exited zero with `OOMKilled=false`. The recorded cgroup `memory.peak`
was 4,295,995,392 bytes (about 4.0 GiB); this is not a Go heap measurement or
evidence of spare memory headroom. The configured limits remain unchanged.

Opening verifies complete old/new compact calculation membership, complete
group evidence, reconstructed expected graph hashes, live schema/counts and
completion. Selected page, lookahead and path documents receive source-backed
field checks. This is not a fresh full live-graph field scan or an interactive
serving benchmark. The existing full graph replay covers that separate boundary.

Witnesses come from verified membership and deterministic graph-key ordering,
not a named entity list. A committee continuation is required only when a matching
selection population exists. No result proves complete financial flow, person
identity, terminal-source eligibility or dollar allocation. Date-window extension
support remains next work.
