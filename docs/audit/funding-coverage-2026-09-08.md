# Preserved funding coverage — 2026-09-08

Status: complete valid-fact summary scans, receipt-inventory conservation,
independent artifact comparisons, and deterministic replay pass. The
[coverage audit](../design/funding-coverage-and-time.md) does not establish a
complete committee cash denominator or terminal-dollar attribution.

## Exact inputs

The audit consumes the existing [2024 receipt inventory](./committee-funding-basis-2026-09-08.md)
and [candidate-receipt bundle](./candidate-receipt-fact-bundle-2026-08-31.md).
No source was fetched, re-extracted, rewritten, or reclassified.

- Inventory: `e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`.
- Receipt fact set: `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Receipt bundle: `b00ce42a65696310b8c1f8c3f8f3bc28077f5bc774e4955657cc16b210d629a3`.
- Source release: `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`.

This older coherent bundle is the exact input for these findings. It is not
presented as active release v3 or as a latest-source freshness check. The bundle
binds both summary manifest digests; each compressed and uncompressed artifact
digest, row count, source envelope, and inspected typed value is checked again.

## Measured coverage

The audit regroups the existing Schedule A inventory into 31 disjoint
component/receipt-role groups and conserves all 264,085,606 source facts,
including unknown, signed, and memo observations. It does not rescan the receipt
rows or calculate a network-wide money total.

| Independent summary population | Valid facts scanned | Earlier source exclusions | Coverage-through dates after 2024 | Negative beginning-cash observations |
|---|---:|---:|---:|---:|
| `weball` / all-candidates | 3,826 | 30 | 3 | 80 |
| `webl` / current-campaigns | 2,368 | 9 | 3 | 31 |

These populations overlap. Do not add the columns into unique-candidate,
unique-exception, or financial totals. Both exclusion counts were already
recorded by the original source publication as source-invalid occurrences;
neither is a new exclusion introduced by this audit. Their exact source issue
dispositions still need separate review. Source evidence remains preserved.

All 17 reviewed monetary fields have a nonblank value in these valid-fact
populations. That does not imply financial completeness. Zero and negative
values are preserved and counted, not treated as absent or changed to positive
cash. The audit does not explain the economic cause of a negative balance.

Both datasets contain one coverage-through observation for each of
2025-01-04, 2025-01-30, and 2025-01-31. The remaining valid facts have dates
within 2023–2024. The audit retains those exact source dates instead of stamping
every observation with the end of the 2024 cycle. This is a coverage finding,
not a claim that the publisher's partition is erroneous.

## What the inputs cannot establish

The [FEC all-candidates description](https://www.fec.gov/campaign-finance-data/all-candidates-file-description/)
defines a candidate-level financial summary and explains that activity may
combine authorized committees. Its published layout includes beginning/ending
cash and a coverage-through date, but no per-committee report identifier,
coverage start, or separate unitemized-individual amount. The preserved source
schemas and complete valid-fact scans agree with that layout.

Accordingly:

- Candidate summary observations are supported at their declared scope.
- Committee/report opening balance is scope-incompatible, even when `COH_BOP`
  is populated. No candidate balance is distributed across upstream PACs.
- Separate committee unitemized receipts and committee report intervals are
  absent from these supplied inputs. A detail/summary residual cannot fill them.
- Prior-cycle origins, cash versus valuation roles, recipient availability,
  negative-adjustment allocation, and a complete cash denominator stay unresolved.

The [committee master](https://www.fec.gov/campaign-finance-data/committee-master-file-description/)
is an identity/reference product, not a financial statement. This audit does
not load it as a substitute source for opening balances. It also does not
profile receipt event-date completeness or reinterpret Schedule B/E ledgers.

## Artifact and verification

Retained directory:
`/storage/dumps/audits/fec/funding-coverage/2026-09-08/2024/`.

- `coverage.json`: 110,117 bytes; SHA-256
  `82c397e66d7e242d6dd644ba5d45e40be34a4b339f07330083addcd80bfaf481`.
- Audit ID: `549bc8222859f01ce8d2c6dc6bd3a7c0eb757de5f32e4f26ddb80f465e7d44b3`.
- `coverage-replay.json`: byte-identical; both command `.exit` markers are zero.

The real audit completed in 8.871 seconds; replay took 10.265 seconds. It used
four CPUs, a 4 GiB container cap, `GOMEMLIMIT=2GiB`, read-only source storage,
and no network. Backing Schedule A hashes are verified, but receipt rows are
not decoded again. No additional receipt corpus or graph was created.

The independent `tests/test_funding_coverage_corpus.py` gate reads each pinned
summary artifact using the existing system zstd library and Python exact
decimal arithmetic. It verifies every field profile and date bucket, both
artifact hashes, source exclusions, receipt-role conservation, result identity,
and replay. Python is audit-only; runtime coverage policy remains in Go.
Formatting, dependency consistency, `go vet`, the full Go suite, and focused
funding-basis race tests pass.
The rewrite Python boundary suite passes 62 tests with ten opt-in corpus skips;
the separately enabled coverage/schema gate passes all six tests against the
retained source artifacts. Existing Dagster beta warnings remain unchanged.

Next review official committee-level financial-summary bulk options against
the missing committee/account/report, opening-balance, and unitemized fields.
Do not choose a new source or allocation algorithm based on its name alone.
