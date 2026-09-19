# Receipt reference topology gate — 2026-09-12

Status: accepted and retained. Implementation, fixture gates, full 2024 calculation,
separate corpus readback, report-policy replay and full layout-varied replay pass.
The [design contract](../design/receipt-reference-topology.md) owns behavior and
limits. This does not publish contributor vertices or qualify conduit roles.

## Exact inputs and execution

The input is the accepted [narrow-reader reference join](./receipt-narrow-reader-2026-09-12.md),
not another Schedule A scan or download:

- Reference calculation: `1a66232f2d0afb11a0b4e38af2d7caa23c9ea34db9c909f0698ac00dd7514fab`.
- Reference manifest SHA-256: `75721e7c59fcd74321fa8b9a0e7e22397021c538dc08466cdad8b34f457ee2e4`.
- Fact set: `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Source fact manifest SHA-256: `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829`.
- Application SHA-256: `35ed731c3a59df7f1a1118139744ac9cef2477fa64563de41a1e75e2a96913a2`.
- Final source/test archive SHA-256: `46cbb903651fc7c6954b8ee38f401300311164c7774645999c7096baf63c9068`.
- Reference test executable SHA-256: `16a2644158de7c0c9ccbbc3bd8290e4e2b30eb94c02287cd270d2924ffd30446`.
- Funding test executable SHA-256: `2e785d59e43e02d4c37d661da309988cf3816f69c11bdfda26afe60516871e06`.
- Go 1.26.5, `go build -buildvcs=false ./cmd/legal-tender`.

Working audit: `/tmp/legal-tender-reference-topology.YxBljd/`.
The `cycle.sh` command runs in a network-disabled container with read-only input,
eight CPUs, 4 GiB RAM, `GOMEMLIMIT=2GiB`, and `GOMAXPROCS=8`. The shared output
workspace cap is 4 GiB. Two independent input passes run concurrently; eight CPUs
is a quota, not a claim of eight busy workers or measured scaling.

## Full calculation result

Calculation ID: `3192971a9cbd28696cbf911ece53b7362a5a77c7c84be4e262236dd8b1c2acca`.
The command completed successfully in **244.574 seconds** (4m 4.574s):

| Measure | Result |
|---|---:|
| Parent source occurrences | 264,085,606 |
| Exact-reference endpoints | 68,864,623 |
| Invalid reference sources | 1,146,097 |
| Members targeted by invalid references | 310,476 |
| Distinct unsafe endpoints | 1,455,130 |
| Retained endpoint union | 70,319,355 |
| Endpoint artifact bytes | 293,660,300 |
| Peak workspace bytes | 586,380,345 |
| Peak process RSS bytes | 260,030,464 |

The exact/unsafe populations overlap at 398 endpoints. These are reference
incidents, not counts of donors, qualified conduits or payments. The output
retains all unsafe endpoints, including those without an exact neighbor.

Endpoint artifact SHA-256:
`4132087fe45ba601485e6024dfd420d8546cebc2d109bb39fb2b5e1ff6351a71`.
Decoded-stream SHA-256:
`f3d9ed8a866c0ed869a3c3208054ceaf7e0206093ff93fc7149a4f7d4cbbbcd9`.

## Verification completed

- Complete Go test suite, affected-package race tests and repository-wide vet.
- Existing bounded report-association tests unchanged in expected behavior.
- Direct nullable-field incident oracle across run sizes and forced Bloom
  false positives; exact endpoint value digest stays unchanged.
- Forward, reverse and reciprocal references feed the shared role policy.
- Invalid incoming references at either endpoint block otherwise valid pairs;
  missing schedules, missing own transaction IDs, shared memos and fan-out remain
  explicit. Other report scopes do not contaminate a pair.
- Zero, negative, unequal and unknown amounts remain separate observations;
  the association decision adds no money and stays terminal-ineligible.
- Fixture publication and layout-varied replay have identical calculation IDs.
- Corruption in each of the four backing streams, wrong expected identity,
  wrong scope, cancellation and exhausted disk cap produce no success manifest.
- Empty-reference populations and malformed compact endpoint records fail or
  conserve explicitly as appropriate.
- Full endpoint/decision/neighbor cross-check: all 70,319,355 endpoint records
  and every source-invalid membership agree, with exact unsafe and neighbor counts.
  The separate checker completed in 244.66s, concurrent with the replay below;
  this cost is not included in the first calculation's runtime.
- Full same-build replay with 32,768-row runs and fan-in 3, replacing
  100,000/fan-in 8: 249.261s, identical calculation ID, counts, bytes, physical
  artifact SHA-256 and decoded-stream SHA-256. Physical filenames differ.
- Both previously accepted [report examples](./receipt-report-association-2026-09-08.md)
  reproduce the old reference/association decisions from their pinned full-source
  rows. The new cycle topology plus shared role policy also reproduces all 31
  association decisions over those 87 retained rows. The two checks take 9.96s
  and 10.12s, including full endpoint-stream digest verification.
- Final source rebuild is byte-identical to the application used for both full
  runs. Dependency consistency, formatting and relative documentation links pass.

## Retention

Verified directory:
`/storage/dumps/audits/fec/receipt-reference-topology/2026-09-12/attempt-01/`.
It retains both outputs, executable/test binaries, source snapshots, commands,
logs, comparisons and `SHA256SUMS`. The complete tree occupies 695,209,381 bytes.
The earlier working directory remains; no source or prior audit was removed.

`cycle.exit`, `replay.exit`, `corpus.exit`, both report exits, `check.exit`,
`comparison.exit`, final tests/race/vet, rebuild/test-build/snapshot and
`input-recheck.exit` are all zero. The retained `retention-check.exit` is zero
and every copied file passes checksum readback. The input reference manifest
still has its original digest after the checks.

## Boundaries

The full consumer checks retained reference backing, not source Parquet anew.
Raw-source equivalence belongs to the accepted parent gate. The separate corpus
checker compares every endpoint with the prior decision and neighbor streams;
the fixture oracle independently tests invalid-target propagation.

An omitted endpoint has no exact peer or invalid reference incident, not proven
unique transaction identity. This matters for later disposition of unreferenced
duplicates outside the requested-key population. Source-role qualification,
all-receipt participant publication, Arango connections and production activation
remain unimplemented by this command. Existing source, calculation, graph and
Dagster pointers are unchanged.
