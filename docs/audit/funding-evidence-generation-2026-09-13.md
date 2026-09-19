# Typed generation gate — 2026-09-13 UTC

Status: accepted for the scope below. Go regression, targeted race/static checks,
real read-only verification, byte-identical fresh replay and retained checksums pass.
The [generation contract](../design/funding-evidence-generation.md) owns behavior.

## Scope and retained execution

Bind the previously completed 2024 receipt graph, selected A/B graph and resolved
Schedule E graph without importing records, changing graph metadata, downloading
sources or updating a current pointer. Receipt source flags are copied from the
[accepted connection gate](./reference-content-equivalence-2026-09-13.md), not
reconstructed by hand. The additional outside bundle is its immutable manifest.

Audit root: `/storage/dumps/audits/fec/funding-generation/2026-09-13/`.
Executable SHA-256:
`d97edf0dce9ad59b0c5fa72f8d1b4d1875bf40ac2f4cd15bf2f20d221b227006`.
Go source snapshot SHA-256:
`5631908be7db9cc35ada818ef49cc1bb78cab02dc0ad65efcba900e8bc4deeac`.

Receipt projection:
`c0536042b7af9875f2e0de2d013f6ffeab31d8cc53755a8f660082753a0518e6`;
manifest SHA-256:
`5440395861205732971d0cd673c1eaf3bc60f213e3bd113a21ee3eace57d053a`.
A/B bundle:
`113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d`;
manifest SHA-256:
`d450dfdb2efca9b0ace973fcb3daf2218ad0775152183fd03cd93f34dbc9a62e`.
Outside bundle:
`67cfb78d87076b2897ed8e7fd1eb5999e6fca477307190b419f7f7e83451333f`;
manifest SHA-256:
`27fc89e114958b9313720e2425e620d0195dcc8c1283e6828cbf6c29fb912fef`.

Attempt 01 exited nonzero before source validation: UID 65534 could not read the
root-owned bundle directory. Its empty result, diagnostic, timing and `run.exit`
remain retained. No graph was opened or modified.

Attempt 02 uses the identical executable, source snapshot, scripts and input
hashes as container root, with source storage still mounted read-only. No host
permission changed. Only the new attempt directory is writable. The detached
container is `lt-funding-generation-20260913-02`, with 4 GiB memory, eight-CPU quota,
`GOMEMLIMIT=2GiB` and `GOMAXPROCS=8`. Existing database caps are unchanged.
Credentials are passed privately from the configured service and never retained.

`SETUP_SHA256SUMS` pins executable, source and scripts. The runner requires a
successful first result, a fresh invocation with `--expected-generation-id`,
byte-identical result/replay JSON, and checksum readback. Completion requires
`gate.exit`, `replay.exit` and `run.exit` all zero. The exit trap separately records
cgroup memory peak, which includes charged file cache and is not process RSS.

## Result

Generation ID:
`35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
Both result files are 28,663 bytes with SHA-256
`1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.
All three exit markers equal zero; the container exited zero without an OOM kill.
Both setup/final checksum lists pass fresh readback. The exit-trap markers and
memory sample are checked separately because they are written after final checksums.

The first verification took 105.746 seconds (132.539 user CPU seconds, 8.275 system).
Fresh replay took 104.333 seconds (134.096 user CPU seconds, 8.243 system). This is
full generation verification cost, not query latency. The cgroup peak was
4,295,913,472 bytes, approximately the configured 4 GiB cap, including file cache.
Do not interpret that as process RSS or claim memory headroom from this run.

| Bound family | Declared membership |
|---|---:|
| Reported receipts | 264,085,606 occurrences |
| Qualified conduit associations | 14,143,626 associations |
| Candidate authorization context | 8,584 context relationships |
| Receiver committee observations | 320,731 selected occurrences |
| Sender committee observations | 341,720 selected occurrences |
| Reconciliation candidates | 308,488 components |
| Independent support | 3,889 resolved groups |
| Independent opposition | 1,414 resolved groups |

These memberships have different grains and overlap. They are not additive money
totals. The receipt publication has zero unrouted receipts. The selected A/B graph
retains 1,114 unresolved same-cycle masters. The resolved E graph has no missing
masters but retains 296 unprojectable decisions outside its 5,303 candidate groups.

Six complete reference proofs bind three committee contexts, two candidate
contexts and the receipt linkage context. The new candidate policy leaves the
earlier CM/CCL proof IDs unchanged. Exact Schedule E membership proof
`5bf4f3daa003186aedfbc3fe80c084eace79c7ddcdf5afadbdc1bab6084e7ca2`
binds the earlier E source release to the A/B target without replacing provenance.
All three live completion boundaries were rechecked on each invocation.

## Acceptance boundary

This is a required-family generation binding with explicit source coverage and
typed physical routing. It does not scan all receipt graph fields again, verify
all candidate paths, sum overlapping ledgers, infer terminal dollars, resolve
corporations or enable weekly scheduling. A later generation-bound consumer and
its cross-family query gate remain required.
