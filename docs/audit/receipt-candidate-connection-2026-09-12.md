# Automatic receipt-to-candidate graph gate — 2026-09-12

Status: Go implementation, full regression suite, targeted race and static checks
pass. The real read-only gate rejected incompatible reference provenance. A final
metadata-only preflight reproduces that rejection in 0.020 seconds without network
access. No complete census/witness proof was produced by these attempts. The
[2026-09-13 reference-equivalence gate](./reference-content-equivalence-2026-09-13.md)
subsequently passed the complete index census, selected witnesses and exact replay
without replacing either source history. This audit preserves the earlier rejection.
The [contract](../design/receipt-candidate-connection.md) owns behavior.

## Scope and inputs

The [complete receipt publication and full replay](./arango-receipt-participant-cycle-2026-09-12.md)
are accepted. This gate uses that exact completed receipt graph and the existing
[committee-flow graph](./arango-committee-flow-evidence-2026-09-08.md), without
imports, source downloads, changed financial policies or a new database.

The Go command derives cycle, candidate test scope and source witnesses from
pinned inputs. It checks the complete participant/conduit index and selected
cross-graph witnesses, not every candidate's paths or terminal allocations.
No manually selected candidate, donor, row ordinal, embedding or LLM is involved.

Receipt projection: `c0536042b7af9875f2e0de2d013f6ffeab31d8cc53755a8f660082753a0518e6`.
Its required manifest SHA-256 is
`5440395861205732971d0cd673c1eaf3bc60f213e3bd113a21ee3eace57d053a`.
The exact A/B bundle is
`113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d`;
its fresh byte checksum matches
`d450dfdb2efca9b0ace973fcb3daf2218ad0775152183fd03cd93f34dbc9a62e`.

## Retention and execution

Audit root: `/storage/dumps/audits/fec/receipt-candidate-connection/2026-09-12/`.
Attempts 01–02 executable SHA-256:
`79412654e412c782c0c7e10b3c77d385a9e2cb6f12e560116d8eec727991c07c`.
Attempts 01–02 Go source snapshot SHA-256:
`c52cae4a624a9d34d551b24333be69f8a6098d92cbedf104583fa3fb749d5703`.

Attempt 01 exited one before opening either graph: a conduit identity had been
mis-transcribed in launch arguments. Its logs and empty result remain retained.
No source or validation rule changed. Attempt 02 instead takes the exact source
arguments from the frozen publisher configuration. All setup checksums pass.

`lt-receipt-connection-gate-20260912-02` ran attempt 02 with source storage
read-only and only its new audit directory writable. The runner has a 4 GiB cap,
eight-CPU quota, `GOMEMLIMIT=2GiB` and `GOMAXPROCS=8`. These are limits, not a
claim that every stage uses eight cores. Existing Arango settings are unchanged.
Credentials pass privately through the configured environment, not command flags
or source snapshots.

Attempt 02 exited one without OOM at the linkage source-archive check, before
the participant census. Its diagnostic mentions committee-master occurrence bytes
because the existing classic-reference helper shares that error wording for CCL.
The same attempt's verified flow bundle also pins a different committee-master
fact set from the receipt graph. Neither difference was bypassed.

## Provenance mismatch, not established content drift

Both graphs select the same exact Schedule A fact set and population. Their
committee context instead comes from two source releases:

| Reference | Receipt graph | Committee-flow graph |
|---|---|---|
| Committee fact set | `e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d` | `a9f3235c81ce1487c9322a899a586ec00992adf26b81bf4c76444c0c5f75d254` |
| Source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` | `fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf` |

The retained release manifests report different ZIP hashes for both `cm` and
`ccl`, but matching compressed and uncompressed selected-member hashes and sizes.
The shared uncompressed hashes are:

- `cm.txt`: `7f7abd31beaece86617a315b4908b9f7dfe9e839dc1fd8124032c224cea16be3`.
- `ccl.txt`: `899bfbd4f3de02d11a105dbe4760771aaea0f16057084c5e169c3fa74981e2b4`.

This indicates archive repackaging, not evidence that the selected reference rows
changed. It does not make the occurrence/fact provenance identities interchangeable.
The exact-reference contract still rejects them; source-byte equivalence needs an
explicit, independently verified bridge preserving both ancestries. No such bridge
has been implemented or accepted, and no graph was rebuilt to conceal the mismatch.

## Final inexpensive rejection

The Go consumer now validates immutable bundle metadata and exact shared Schedule A
and committee references before backing scans or database access. A matching
preflight still requires every existing full source/graph check; it is not readiness
acceptance by itself.

Attempt 03 executable SHA-256:
`02642da4af20f21f3e66de31342c11a6d0a90da491ee896a5c3a7efd842391ab`.
Source snapshot SHA-256:
`3c8e166f3f40869bb849f26a1c08a7202325506bf94cc2e4d4f6712b844d2f12`.
The exact compared releases and flow bundle are retained alongside the executable.
`lt-receipt-connection-gate-20260912-03` had networking disabled and no real database
credential. It exited one with the expected explicit committee-reference mismatch
in 0.020 seconds. `result.json` is empty. Setup and `REJECTION_SHA256SUMS` readback
pass. This is a passing rejection test, **not** a passing connection gate.

No import, metadata replacement, current-pointer change or database mutation occurred.
All attempts are stopped. The original source/graph import executable and its full
replay evidence remain untouched.

Next: implement a reusable reference-content equivalence proof for unchanged
selected bytes in different archives, retaining both provenance identities and
rejecting real content/schema drift. Then rerun the automatic connection gate.

This is a machine-run acceptance command, not weekly Dagster activation.
Identity/employer assertions and broader population integration remain separate
work in the [connected-graph queue](../todo.md).
