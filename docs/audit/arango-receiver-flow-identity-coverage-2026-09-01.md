# Receiver-flow identity coverage and ArangoDB v2 gate — 2026-09-01

## Result

The 2024 receiver-reported committee-flow graph now exposes distinct identity
states for every referenced committee. The new calculation, v2 readiness
bundle, isolated ArangoDB projection, exact readback, and representative query
gate passed every blocking check.

The v1 readiness bundle and database remain unchanged. Version 2 composes the
v1 money-flow boundary with an immutable committee identity-coverage
calculation. It does not backfill the selected committee master.

## Exact lineage

| Field | Value |
|---|---|
| Cycle | `2024` |
| Source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` |
| Base v1 flow bundle | `63a87bdcb239a0da6ac4b4a3c69f9eaf42fca6be690fb2a2f6e3872cef70f827` |
| Base bundle manifest SHA-256 | `5568aba0cb987eeaf42bd7cab3f2ad34f249cb47f4400f277f3254498d4908d4` |
| Identity calculation | `5a56dd384b9304e07b62786cf7b80e3ca489386b83d1e8e4dae9c15c480e61bc` |
| Identity manifest SHA-256 | `3edd8efd8abaaede271dc04b1becbd92a1a12c576ed3c0421169a30a8f8c16b5` |
| Identity decisions SHA-256 | `1c2daf7d2c3d736fef7e2b436c1bc7055b21a7333bba8419d6635a26d2c69d18` |
| V2 readiness bundle | `ab51d785aa58435e30295b2bb8051bbfbe2280c385bd810d1fd1cba6d1f0ffcc` |
| V2 bundle manifest SHA-256 | `1eb1ad7b3cc0c097bdc9dff8b8584e657d0954be04be902b79fac680ab679936` |
| Projection | `0c82fa480c9d9825ed1d3b40272a4a7e24a3546a92f8a99aa1f15b67b5bd99b0` |
| Database | `lt_flow_probe_v2_2024_0c82fa480c9d9825` |
| Named graph | `receiver_reported_committee_flows` |

The calculation binds the exact v1 flow bundle, calculation, selected 2024
committee master, different-release 2024 comparison, normalized 2020, 2022,
and 2026 masters, and every official raw cycle archive from 1980 through 2018.
Each raw input now carries a storage-relative replay key in addition to archive
and row digests.

Linkage and candidate summaries remain inputs to the broader diagnostic audit.
They do not affect identity state and therefore do not enter the identity
calculation ID.

## Identity calculation

The fast publication completed in 8.9 seconds and wrote a 191,878-byte zstd
artifact containing 707 decisions and 3,481 historical registration
assertions.

| Graph identity state | Committees | Terminal-identity eligible |
|---|---:|---:|
| `current_cycle_master` | 7,690 | 7,690 |
| `historical_registration` | 675 | 0 |
| `alternate_release_registration` | 0 | 0 |
| `unresolved_reported_id` | 32 | 0 |
| **Total** | **8,397** | **7,690** |

All 707 decisions are source-endpoint decisions. The publisher joins only by
exact reported committee ID. It never uses names, linkage, summary facts, or a
correction table to manufacture identity.

`terminal_identity_eligible` is only a prerequisite for a later terminal-
source classifier. It does not declare any current-cycle committee to be a
terminal source.

## Vertex evidence shape

Direct ArangoDB readback confirmed each state:

- `current_cycle_master` has the selected-cycle fact ID and canonical display
  fields, no identity decision, and eligibility `true`;
- `historical_registration` has an identity decision, eligibility `false`, an
  empty canonical name, and the complete historical assertion list; and
- `unresolved_reported_id` has an identity decision, eligibility `false`, an
  empty canonical name, and no registration assertions.

For example, historical vertex `C00000000` retains seven assertions beginning
with exact 1980 archive and row evidence. Unresolved vertex `C00035907`
retains its reported ID and decision lineage without adopting the differently
identified Smithfield committee found by name during the forensic audit.

## Graph conservation

| Measure | Expected | Observed |
|---|---:|---:|
| Committee vertices | 8,397 | 8,397 |
| Current-cycle masters | 7,690 | 7,690 |
| Historical registrations | 675 | 675 |
| Unresolved reported IDs | 32 | 32 |
| Terminal-identity eligible | 7,690 | 7,690 |
| Terminal-identity ineligible | 707 | 707 |
| Flow edges | 180,283 | 180,283 |
| Registered-filer contribution edges | 174,344 | 174,344 |
| In-kind contribution edges | 520 | 520 |
| Affiliated-transfer-in edges | 5,187 | 5,187 |
| Refund-or-repayment-received edges | 232 | 232 |

| Signed amount | Expected | Observed |
|---|---:|---:|
| All projected flows | $4,672,820,179.49 | $4,672,820,179.49 |
| Registered-filer contributions | $1,249,804,428.98 | $1,249,804,428.98 |
| In-kind contributions | $2,441,507.80 | $2,441,507.80 |
| Affiliated transfers in | $3,419,866,954.76 | $3,419,866,954.76 |
| Refunds or repayments received | $707,287.95 | $707,287.95 |

The topology is unchanged: 92 weak components, 6,128 strong components, 35
cyclic strong components, and 2,304 committees in cycles. Five-run query
medians were 710 µs for a bounded direction-agnostic neighborhood, 1,127 µs
for ranked paths, 283 µs for directed shortest path, and 119 µs for a directed
cycle.

The v2 collections and indexes use 154,851,054 bytes, about 147.68 MiB. The
smaller figure relative to the v1 gate is an ArangoDB physical measurement,
not an evidence deletion; the historical vertices retain all 3,481 assertions.

A hardened replay rehashed every normalized comparison artifact and all 20 raw
official ZIPs, recomputed calculation, decision, and bundle identities, then
verified and reused the completed database without imports. Exact state,
amount, topology, and query gates passed again.

## State and next gate

The projection remains `partial` because 32 reported IDs have no official
registration evidence. That state is honest and expected. The 675 historical
registrations are no longer generic missing-master placeholders, but neither
they nor the unresolved IDs can stop terminal-source traversal.

The active Dagster v1 chain remains unchanged while this additive v2 boundary
is evaluated. The next data boundary is independent Schedule B sender-reported
disbursements, followed by an explicit Schedule A/B reconciliation that does
not count two reports as two payments.

See the preceding [master-gap audit](./receiver-flow-master-gaps-2026-09-01.md)
for the source-row forensic evidence and the original
[v1 graph gate](./arango-receiver-reported-committee-flows-2026-09-01.md) for
the preserved base projection.
