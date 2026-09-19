# Candidate upstream evidence gate — 2026-09-08

Status: passed for two data-selected 2024 candidates. The
[implemented contract](../design/candidate-upstream.md) owns the scope. This
gate proves candidate-specific committee ancestry and accounting, not terminal
donor allocation, complete candidate receipts, or chronological money tracing.

## Inputs and preparation

The command consumes the same pinned evidence as the current Arango observation
graph, without reading or writing Arango. Source bundle:
`113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d`.
The exact A/B and committee-master identities remain those of the
[observation graph gate](./arango-committee-flow-evidence-2026-09-08.md).

The old 2024 candidate-linkage publication referred to a different archive.
Its archive and the current release's archive are both 94,262 bytes but have
different SHA-256 identities. The existing classic occurrence/fact publishers
materialized release-matched linkage evidence from the already-staged member.
All 8,619 linkage facts were valid and conserved. No download or source-policy
change was needed. Existing canonical current pointers stayed unchanged; new
preparation pointers were written only in the isolated audit directory.

New linkage fact set:
`562246920ef6d5ce508db2c98bf8bc972e2e97041254fde1ce39703ab795dedf`.
New occurrence set:
`dfce55d8835b6c70321e2990b555f10b6b917a0540e3a10cacf48c4caae45ca1`.

An initial new-consumer attempt confused the normalized flow-role vocabulary
with the more specific receipt reporting role. The gate failed before producing
a result. Both consumers now use the same role mapping; reporting-role/code
agreement is checked separately. No selection rule or source record changed.
The failure log is retained.

## Measured candidates

The test selected the two candidates with the most selected observations to
their A/P-linked committees, with ID tie-breaking. This is test sample selection,
not a candidate-specific calculation rule. Shared or conflicting authorization
still goes through the normal unresolved route.

| Measurement | S6OH00163 | S6PA00217 |
|---|---:|---:|
| External selected receipt observations | 1,389 | 1,332 |
| Signed external-cohort observation amount | $14,976,486.23 | $11,003,822.09 |
| Negative observations retained | 2 | 1 |
| Reachable committees | 7,096 | 7,086 |
| Missing same-cycle masters | 623 | 620 |
| Upstream committees with no selected incoming observations | 4,166 | 4,157 |
| Cyclic strongly connected components | 18 | 18 |
| Upstream observation references | 296,781 | 295,449 |
| Calculation seconds after verified loading | 0.181 | 0.164 |
| Replay seconds | 0.164 | 0.197 |
| Compact result bytes, before final newline | 6,511,578 | 6,473,659 |

Neither sample has an internal or unresolved-authorization root observation;
synthetic gates cover those cases. All external-cohort amounts remain explicitly
unresolved for terminal attribution. No missing master or empty adjacency was
classified as a terminal donor. These are not candidate total-funding figures.

Full immutable input/evidence verification and loading took 17.391 seconds.
The entire two-candidate calculation/replay gate took 18.800 seconds. Process
peak RSS was 686,374,912 bytes. The disposable container used four CPUs, a
4 GiB cap, `GOMEMLIMIT=2GiB`, and `GOMAXPROCS=4`, with source storage mounted
read-only and networking disabled. These are warm local measurements, not
weekly-refresh or production latency guarantees.

## Compact representation

The first passing representation copied upstream observation fields into each
candidate result, producing 92,595,292 and 92,171,083 bytes. The accepted
representation replaces that repetition with sorted exact source-row ordinals
bound to the immutable fact set and shared observation artifact. It retains
all membership and source witnesses while reducing each result by about 93%.
No source field or observation was discarded. This remains a diagnostic; it
does not publish a separate production graph or duplicate source ledger for
each candidate.

## Verification

- Read and verify exact published source, calculation, bundle, and linkage ancestry.
- Independently compare every retained candidate observation and upstream
  ordinal against the selected source cohort, with no omitted/repeated member.
- Verify reverse-reachability closure and every shortest-hop witness's source
  edge, endpoint continuity, and decreasing distance to authorized scope.
- Preserve exact candidate-boundary accounting and byte-equivalent replay.
- Run the shipped CLI against the first real candidate and compare its parsed
  JSON with the accepted calculation result; every field matched.
- Validate both real results against the versioned JSON Schema, using only
  local schema resources, and independently recompute result identities and
  signed/positive/negative/count conservation.
- Pass full Go checks, targeted race tests, and all 59 rewrite Python boundary
  and schema tests. Python adds no pipeline behavior.

Unit/property tests also cover shared/conflicting authorization, internal
transfers, negative/zero and in-kind/refund observations, parallel edges,
self-loops, random graphs checked against an independent transitive closure,
10,000-hop chains, cancellation, changed inputs, and sums beyond int64 totals.

## Retained evidence and next step

The audit lives under the configured storage root at
`dumps/audits/fec/candidate-upstream/2026-09-08/2024/`. It retains isolated
linkage preparation pointers and the accepted result files, gate summaries,
logs, runner/validator, source snapshot, schemas, and completion markers.

Accepted calculation IDs:

- S6OH00163: `6eaafbb2bdc5a7695ef663e798d96184d1e7c90297dab01339870bb7f4a3128c`.
- S6PA00217: `6e3a272103987478cb808089d2d1050899e042341f5266bb0860291b3834182c`.

Result-file SHA-256 values, including the trailing newline:

- S6OH00163: `596b569394820a37dd0b92868d3e14636d30ef704445ea9101120898fe60ba35`.
- S6PA00217: `63c7aac3c8e3de163f9fcc41161a2c263c8311ebf487dca5e233426d18ecaadf`.

Next is a source-grain donor-bearing receipt/funding basis for reached
committees, with explicit coverage and time assumptions. Terminal classification
and pooled-dollar allocation remain unimplemented. This gate adds no automated
Dagster consumer, UI, API endpoint, graph mutation, or production cutover.
