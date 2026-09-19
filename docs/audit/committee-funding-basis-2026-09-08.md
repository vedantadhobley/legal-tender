# Committee receipt inventory gate — 2026-09-08

Status: passed for the complete published 2024 Schedule A fact set. The
[implemented contract](../design/committee-funding-basis.md) defines a reported
receipt inventory, not a full cash funding basis or terminal allocation.

## Exact inputs and output

- Schedule A fact set:
  `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Manifest SHA-256:
  `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829`.
- Inventory calculation:
  `e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`.
- Source rows: 264,085,606 across 265 verified shards; no new source download,
  extraction, source fact, graph, or canonical current-pointer change.
- Inventory: 37,404 disjoint recipient/decision/role buckets; 32,081,354 JSON
  bytes, including source membership bitmaps. Full receipt fields stay in the
  existing Parquet corpus. No donor-name aggregation or identity merge occurred.

The full command, including manifest/backing verification and output, took
289.741 seconds. The disposable container used four CPUs, a 4 GiB cap,
`GOMEMLIMIT=2GiB`, and `GOMAXPROCS=4`; source storage was read-only and networking
was disabled. Its observed cgroup peak was 491,708,416 bytes, including the
preceding binary build and charged cache, not only process RSS. This is a local
measurement, not a weekly processing guarantee.

## Complete conservation and overlap

| Disjoint inventory component | Rows |
|---|---:|
| Individual predicate only | 222,204,713 |
| Committee-flow predicate only | 319,993 |
| Both predicates | 738 |
| Memo subtotal | 36,584,727 |
| Other reported receipt | 4,975,433 |
| Unknown amount | 2 |
| **Total** | **264,085,606** |

All rows have an exact-format recipient ID in this snapshot. There are
264,085,604 known amounts, 262,722,229 positive rows, 1,356,364 negative rows,
and 7,011 zero rows. These are reporting populations, not distinct economic
payments or candidate total-funding figures. Known signed amounts include
memo and other excluded populations and must not be presented as spendable cash.

Regrouping the independent decision axes exactly reproduces the earlier
[individual-receipt gate](./compact-receipt-calculation-publication-2026-08-31.md):
222,205,451 included rows and 1,588,756,934,113 signed cents, with identical
non-individual, memo, and unresolved counts. It also reproduces the accepted
[receiver-flow cohort](./receiver-reported-committee-flow-publication-2026-09-01.md):
320,731 included rows and 467,282,017,949 signed cents. The overlapping rows
are in both independent historical cohorts but appear only once in this
inventory. Neither accepted predicate was silently changed.

The overlap contains 720 affiliated-transfer and 18 registered-filer-contribution
rows: 619 positive, 66 negative, and 53 zero. Bounded full-row witnesses at
ordinals 34,844,745, 34,848,391, and 34,976,096 preserve `is_individual=true`
alongside `entity_tp=PAC`, matching raw/clean committee IDs, and accepted
committee receipt codes. That conflict is source evidence, not a reason to
rewrite the source fields or assign a resolved person identity. Its treatment
in a combined monetary consumer remains an explicit acceptance gate.

## Structured conduit coverage

The complete scan found zero nonempty `conduit_cmte_id` values. This measurement
does not establish that conduit activity was absent. Full source witnesses at
ordinals 535–537 have receipt type `15E`, null structured conduit ID/name, and
memo text identifying an earmarked contribution through a named conduit.
The row lookup preserves those fields unchanged; it does not extract a name
and promote it into an exact committee link.

The official [Schedule A column documentation](https://github.com/fecgov/openFEC/wiki/Schedule-A-column-documentation)
describes separate contributor, employer, conduit, and memo columns, but is
dated 2016 and does not guarantee their coverage in this current snapshot.
The [FEC reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/)
also distinguishes original contributions from conduit reporting. Neither
source justifies fabricating a structured ID from an unreviewed name match.

## Candidate connection and source lookup

The gate reuses the two data-selected candidate traces and exact bundle/linkages
from the [upstream gate](./candidate-upstream-2026-09-08.md). Recomputed trace IDs
match the earlier accepted IDs; no candidate-specific policy branch was added.

| Coverage measurement | S6OH00163 | S6PA00217 |
|---|---:|---:|
| Reached committees | 7,096 | 7,086 |
| Reached committees with Schedule A inventory rows | 5,916 | 5,910 |
| With individual-only component rows | 5,561 | 5,556 |
| Assessment output bytes | 8,903,621 | 8,895,350 |
| Full CLI assessment seconds | 25.994 | 25.759 |

These counts measure source coverage, not candidate quality, ideology, or the
share of money explained. Each committee's complete component measures match
the cycle inventory. There is no network-wide sum or terminal allocation.
The assessment commands independently verify their inventory and trace backing;
their timings include that startup work.

Committee `C00000935`, selected by sorted inventory ID among earmark-bearing
individual components, is reached by both traces. Two three-row source pages
used exclusive cursor 537 and retained ascending, unique source ordinals;
the first page replay was identical. Full CLI times were 8.421, 8.395, and
8.397 seconds, including backing verification. The null-amount lookup returned
both preserved unknown rows for `C00845032`, with no continuation, in 10.857
seconds. A three-row overlap lookup completed in 10.849 seconds.

## Verification and retained evidence

- Full Go formatting, module, vet, and test gates; targeted race tests.
- Unit fixtures: both predicates, overlap, memo precedence, null/empty/invalid
  recipients, signs, unknowns, arithmetic overflow, four-worker versus serial
  identity, shard-crossing pagination, null-preserving 99-field lookup,
  cancellation, changed backing, and exact-source candidate rejection.
- Independent Python audit: local JSON Schema resolution; full result identity;
  all count/sign/amount conservation; exact accepted-cohort equivalence; every
  candidate committee/component measure; source pages, cursor, replay, unknown
  values, and overlap witnesses. Python adds tests, not runtime calculation.
- Rewrite schema and thin-Dagster tests pass. No live Dagster materialization
  or Arango write was required for this source-only consumer.

Retained outputs live below `/storage/dumps/audits/fec/committee-funding-basis/2026-09-08/2024/`:
`inventory.json`, both candidate-ID JSON files, `page-1.json`, `page-2.json`,
`page-1-replay.json`, `unknown-page.json`, `overlap-page.json`, and exit markers.
The opt-in [corpus test](../../tests/test_funding_basis_corpus.py) uses
`LT_FUNDING_BASIS_AUDIT` to locate those artifacts. The executable remains
rebuildable from source rather than archived as another production binary.

Next: review overlap and earmark linkage, then accept the incomplete-funding,
opening-balance, time, and economic-role treatment before allocating terminal
dollars. The current inventory is useful source evidence, not that completed
attribution model.
