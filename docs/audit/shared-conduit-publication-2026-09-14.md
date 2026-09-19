# Complete shared-conduit publication — 2026-09-14

Status: full 2024 calculation, varied-layout calculation replay, isolated graph
import and byte-identical fresh graph replay pass. Explicit exit markers and
all retained setup/final checksums were verified.

The [group contract](../design/shared-reference-group-rule.md) and
[graph-extension contract](../design/shared-conduit-generation.md) own behavior.

## Full-cycle calculation

All 264,085,606 retained participant occurrences and the complete endpoint
topology were verified. The reproduced v1 stream matches all 33,262,189 accepted
non-memo earmark decisions before applying any group upgrade.

| Population | Result |
|---|---:|
| Old qualified one-to-one associations, unchanged | 14,143,626 |
| New shared-group associations | 2,637,285 |
| Combined qualified associations | 16,780,911 |
| Other decision records copied unchanged | 30,624,904 |
| Old shared-degree exclusions remaining | 11,858,468 |
| Qualified complete groups | 22,334 |
| All roots with old shared-degree exclusions | 38,273 |

Every upgraded record keeps its source ordinal, related memo ordinal and prior
amount comparison. Neither amount values nor financial selection change. The
combined count is an association count, not donors, payments or dollars.

The group publication retains 15,657 incomplete groups, 148 conflicting-ID
groups, 133 unsupported-role groups and one unsafe group. It exposes 106,075
exact peers outside the compact sole-peer request population. Those missing
peers are not assigned guessed roles. This population/label scope differs from
the full-original-source review, which retrieved every immediate neighbor of
its selected roots.

No candidate, committee name, source ordinal, transaction suffix or amount
threshold selects an association. The positive population agrees with the
earlier complete/all-compatible profile without using those counts as a gate.

## Retained calculation evidence

Storage-relative root:
`dumps/audits/fec/shared-conduit-publication/2026-09-14/`.

`attempt-01` uses eight workers, 100,000-row sort runs and fan-in eight.
Its runtime was 331.392 seconds, peak process RSS 1,664,229,376 bytes and retained
calculation data 126,271,617 bytes. The 8 GiB shared workspace cap was unchanged.

| Artifact | ID / SHA-256 |
|---|---|
| Executable | `274c80b2568a61625672d59a2963e895a07f41b37657270dbe2d164ff0ed8588` |
| V2 calculation | `11a165f5a286e4345c65906c54813c7bba7f5d6263e62aacc18a40ddbe948673` |
| Original baseline | `1c597367db6598b0e8705e45b7b2d1cb83e6e0664f7425aa6e5677913a90ff05` |
| Baseline manifest | `de5fed44189aadb0f41617942550a589a39d404819b3e834ae2305f8a78e9e9f` |
| Unchanged baseline values | `8ff2967130c9da69a59f50f8ee09f9404ef1f27fb93926aca2335731e3ff2403` |
| Group decision values | `f9e6bfdc741d9b11484604fe1eb20ba1a80366639f21aa073a05774f2cb2ccc5` |
| Updated decision values | `56b5202f6f6818a532b01337170cd87733141fb8530777703e2406a0f24f17e2` |

`attempt-02` uses the same executable with four workers, 50,000-row runs and
fan-in four. It passes identical calculation identity, both complete canonical
value streams and every population count. Both attempts have zero explicit exit
markers and passing retained checksums. Replay took 518.573 seconds with
645,730,304 bytes peak process RSS and the same retained data size. These were
concurrent development runs, not a controlled CPU-scaling benchmark.

## Tests and graph boundary

Fixture tests cover complete/partial groups, unsafe original/root evidence,
conflicting IDs, unsupported roles, non-leaf peers, wrong/missing/duplicate
changes, null/negative amounts, byte-preserved prior decisions, cancellation,
manifest policy/version/census checks and varied sort/shard layouts. Targeted
race tests and affected-package static checks pass.

The disposable synthetic integration builds raw source, occurrence, fact,
reference, participant, v1/v2 calculation, base A/B/E graph and extension through
the public implementations. Import, complete readback, fresh varied-worker replay
and CLI identity checks pass. A deliberately modified completed edge is rejected
without repair; the restored fixture passes again. Test volumes are disposable;
these failure probes never target development data.

The live graph runner retains its own executable/source snapshot, exact base
generation and calculation inputs, immutable extension manifest, complete
readback and fresh replay. No old graph or current pointer is replaced. Existing
v1 query consumers remain on their old generation until extension-aware reading
is implemented and verified.

Graph audit root: `dumps/audits/fec/shared-conduit-generation/2026-09-14/attempt-01/`.
The detached runner is `legal-tender-shared-conduit-generation-20260914-01`.
`publication.exit=0`, `replay.exit=0` and `exit-status.txt` with `exit_code=0`
all pass, as do setup/final checksum lists. The container exited zero without
OOM. `replay.log` explicitly reports `read_only_replay=true`.

Graph executable SHA-256:
`a22eea4f8bf4fbd01ee17ce3b390fca78a6b5e25c7ddc8669faca5aa7c5dd674`.
Go source snapshot SHA-256:
`0466d735670ee2b79cf61c5271952fee7986798f8213b75d8ca2d67db04b61f7`.

## Accepted graph result

The isolated database is
`lt_receipt_shared_2024_f704616339eb95c36c5d47ed66956aeb`.
It contains 2,637,285 source-occurrence references, 2,637,285 new conduit edges
and 17 source-backed committee context vertices. Its receipt and authorization
collections are empty; no money ledger was duplicated. Every new edge was
checked against the existing base appearance and fully read back, then checked
again during fresh replay. The base A/B/E generation is structurally identical
inside the new generation result.

| Identity | SHA-256 / ID |
|---|---|
| Extension projection | `f704616339eb95c36c5d47ed66956aeba9ede707bb81ac2e3bfe866a2b7c4465` |
| Immutable extension manifest | `ab5ef6adad415e7bb9cd16308429abc4fa8c9f4e6c354734c47f8ea12246809e` |
| Extended generation | `f1c9a89e46ae1bdc291cadf8299230adfb30ca52ddc81932aa6f14afee5a6ec1` |
| Base generation | `35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd` |

The immutable manifest and storage envelope live under
`projections/arango/shared-conduits/v1/<extension-projection-id>/`.
Full first publication ran 21:45:26–21:48:32 UTC, 186 seconds. Fresh replay ran
21:48:32–21:52:19 UTC, 227 seconds. Both include reopening and verifying the base
generation; these are not interactive query latencies or controlled scaling tests.
The graph uses the documented 4 GiB encoded, 16 GiB net-growth and 256 GiB
free-reserve caps. These ceilings are not measured physical database size.

The final source archive rebuilt byte-identically on the host with Go 1.26.5,
`go build -trimpath -buildvcs=false`. The same archive built successfully in the
Debian Go image but produced a different executable hash; both builds enable
CGO. Cross-environment binary reproducibility is therefore not established.
The graph's retained executable and exact same-environment source rebuild are
verified. Pin one build environment before standardizing production executable
identities; do not confuse different build hashes with changed association data.

Next: make path, neighborhood and window readers consume the extension while
retaining base and new disposition evidence. Existing v1 consumers continue to
use the old graph coverage. No source refresh, terminal classification, donor
identity resolution or financial-attribution rule was added.
