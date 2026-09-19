# Reference equivalence and connection gate — 2026-09-13 UTC

Status: accepted for the scope below. The Go implementation, full regression suite,
targeted race/static checks, complete connection gate and fresh whole-command
replay pass. All explicit success markers and retained checksums pass.
The [contract](../design/reference-content-equivalence.md) owns behavior.

## Exact scope

Use the same receipt graph, participant/conduit publications, Schedule A facts,
masters/linkages and committee-flow bundle as the
[rejected first attempt](./receipt-candidate-connection-2026-09-12.md). No source
download, graph import, metadata replacement, current-pointer update, manual ID
substitution or financial-policy change is included.

The new Go code verified all 20,938 receipt-context committee facts, 20,938
flow-context committee facts and 8,619 linkage facts against the exact archive
members and complete normalization replay. The two committee contexts have equal
content fingerprints and retain their different occurrence/fact-set provenance.
The stored receipt graph is not rebuilt.

## Retained execution

Audit directory:
`/storage/dumps/audits/fec/receipt-candidate-connection/2026-09-13/attempt-01/`.

Executable SHA-256:
`99ed5ccf146701d67aa8d25060ad31db225aaa829c71607236d4c2803d5a8e90`.
Source snapshot SHA-256:
`f92387e2d8b6d9abbdc4e4bb853d8bce53b0a0425605a7092a6b842c148f7085`.

`SETUP_SHA256SUMS` verifies the frozen executable, Go source snapshot, scripts
and exact publisher-derived input arguments. The detached runner is
`lt-reference-connection-20260913-01`. Source storage is read-only; only the new
audit directory is writable. It has a 4 GiB memory cap, eight-CPU quota,
`GOMEMLIMIT=2GiB` and `GOMAXPROCS=8`. Existing Arango caps are unchanged.
Configured credentials are passed privately and are absent from snapshots/logs.

The command emits a success result only if the automatic gate passes. The runner
redirects output to `result.json` and then starts
a new invocation with `--expected-gate-id`, requires the same proof identity and
byte-identical JSON, and records `replay.json`. Each invocation also independently
reads every selected connection twice. The full participant/conduit census is
not a reimport or another full-width Schedule A decode.

`gate.exit`, `replay.exit` and `run.exit` all equal zero. Both `SETUP_SHA256SUMS`
and `FINAL_SHA256SUMS` pass fresh readback. The container exited zero without an
OOM kill. `result.json` and `replay.json` are byte-identical, each 254,115 bytes,
with SHA-256 `110a4e60e590b31959c5b451772f1902ec8dc8530bcb129b98f48a4f56aaf149`.
The original receipt projection manifest still has SHA-256
`5440395861205732971d0cd673c1eaf3bc60f213e3bd113a21ee3eace57d053a`.

`timing.txt` and `replay-timing.txt` record wall/user/system time. The cgroup peak
was 3,936,210,944 bytes (3.67 GiB), below the 4 GiB cap. This includes charged file
cache and is not process RSS. `memory-peak.txt` and `run.exit` are written by the
exit trap, after `FINAL_SHA256SUMS`; their values were checked separately.

## Complete result and fresh replay

The gate conserved all 264,085,606 source occurrences. Its twelve case categories
produced ten distinct source-backed witnesses: two categories share a witness,
and unresolved recipient syntax is absent in the complete census. Direct,
upstream, no-route, qualified/unqualified conduit, missing-master, cyclic, memo,
negative, zero and unknown-amount cases are represented. Every selected connection
passed full source/document readback and an internal second read.

Gate identity:
`e36b3af1cb54269ac63d75fd8f7c0eea10c57c5b1973d55240477976f0191ed4`.
First-run wall time was 444.137 seconds (565.805 user CPU seconds, 10.101 system
seconds). This includes reference verification, backing/graph verification,
complete narrow-index census and selected witness replay; it is not serving latency.
The fresh invocation took 439.305 seconds (556.130 user CPU seconds, 9.683 system
seconds). It repeated the complete census, reference proofs and selected witness
readback, required the expected gate identity and reproduced the exact JSON.

The receipt- and flow-context committee proofs preserve different fact/occurrence
ancestries while sharing ordered content fingerprint
`23bbf71ffd9e039827395ffada7aadc99ddcee3fa753c9c90a71d1240d7896ff`.
Their proof IDs are respectively
`a5c975d5ab25f6ca6ac4292557dbfb19633555fce091975b9c27950af21ee394` and
`5fb4f125fb3c521ef7bfac5b5c1d14805cd333bee4ae71d3758ee980dd25a299`.
The linkage proof is
`ab30420dc5cf2120ca8c84e8a1cd38dd62c1bb28af44dbcdd344790680253a39`.

This acceptance remains scoped to a full source-index census and automatically
selected cross-graph witnesses, not every candidate/path or terminal allocation.
Reachability through the broad committee network is not a claim that every
reachable receipt funded the selected candidate. The gate performs no chronological
allocation or path-dollar sum; financial and terminal eligibility remain false.
