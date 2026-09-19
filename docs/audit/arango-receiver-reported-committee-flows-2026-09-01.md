# Receiver-reported committee-flow graph gate — 2026-09-01

## Result

The complete 2024 receiver-reported committee-flow calculation now has an
immutable same-release readiness bundle and an isolated content-addressed
ArangoDB projection. All bundle and graph blocking checks passed. ArangoDB
readback conserved every projected result and signed cent.

The graph proves the receiver-side flow, multi-hop, and cycle boundary. It does
not yet reconcile Schedule B, connect committees to candidates, classify
terminal sources, or publish a production graph.

## Exact lineage

| Field | Value |
|---|---|
| Cycle | `2024` |
| Source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` |
| Schedule A fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Schedule A manifest SHA-256 | `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` |
| Receiver-flow calculation set | `023ecf57e2baf00f93a8fdea3014fdb35b2c7305af91424111a12de74869eda6` |
| Calculation manifest SHA-256 | `f7fd6e888593555b17947a49a9f7d881e78997cb7cc363606b55f78be6287453` |
| Committee-master fact set | `e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d` |
| Committee-master manifest SHA-256 | `e64a74911d279e2afec9adef864ec9218f37c091f85aac6488fec1f72b0a4f30` |
| Readiness bundle | `63a87bdcb239a0da6ac4b4a3c69f9eaf42fca6be690fb2a2f6e3872cef70f827` |
| Bundle manifest SHA-256 | `5568aba0cb987eeaf42bd7cab3f2ad34f249cb47f4400f277f3254498d4908d4` |
| Projection | `853ee7e99d62288fdf90855717a24a4a2332beb0302cbf700dff3a49e6dee6fe` |
| Database | `lt_flow_probe_2024_853ee7e99d62288f` |
| Named graph | `receiver_reported_committee_flows` |

The bundle contains 180,283 calculation results and 20,938 committee facts.
It passed exact-role, cycle, release, immutable-pointer, backing-integrity, and
projection-readiness checks.

## Exact graph conservation

| Measure | Expected | Observed |
|---|---:|---:|
| Referenced committee vertices | 8,397 | 8,397 |
| Present committee masters | 7,690 | 7,690 |
| Missing committee masters | 707 | 707 |
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

The readback uses all pages of each Arango cursor. Unit tests cover cursor
continuation so the amount check cannot silently stop at the first response
batch.

## Topology

| Metric | Result |
|---|---:|
| Weak components | 92 |
| Strong components | 6,128 |
| Cyclic strong components | 35 |
| Committees in cycles | 2,304 |
| Representative path length | 8 hops |
| Representative directed cycle | 2 hops |

The representative source, target, and cycle committees were `C00109017`,
`C00837625`, and `C00808840`.

## Query gate

Each query ran ten measured times after warm-up.

| Query | Rows | Minimum | Median | p95 / maximum |
|---|---:|---:|---:|---:|
| Direction-agnostic neighborhood, depth 1–4 | 25 | 560 µs | 611 µs | 693 µs |
| Ranked paths, capped at 25 | 25 | 998 µs | 1,020 µs | 1,167 µs |
| Directed shortest path | 9 | 254 µs | 282 µs | 377 µs |
| Directed cycle traversal | 1 | 128 µs | 146 µs | 195 µs |

The first path-query design enumerated unrestricted bounded-depth paths and
hit the 60-second deadline on this cyclic graph. It did not fail import or
readback. The corrected gate uses `K_SHORTEST_PATHS` with a 25-path cap. The
successful retry verified and reused the completed content projection.

## Storage and state

| Collection | Documents | Document bytes | Index bytes |
|---|---:|---:|---:|
| `entities` | 8,397 | 18,217,312 | 2,138,640 |
| `receiver_reported_flows` | 180,283 | 122,364,112 | 54,074,508 |
| `projection_metadata` | 1 | 944 | 112 |
| **Total** | **188,681** | **140,582,368** | **56,213,260** |

Documents and indexes use 196,795,628 bytes, about 187.68 MiB combined.

The result is `partial`, not `ready`, because 707 referenced committee IDs lack
a same-release master fact. Explicit placeholder vertices preserve their
edges and amounts. No terminal-source or identity inference should treat those
placeholders as resolved entities.

The follow-up [committee-master gap audit](./receiver-flow-master-gaps-2026-09-01.md)
found 675 IDs in official historical cycle masters and left 32 reported IDs
unmatched across 1980–2026. All 707 are source-only vertices. The unresolved
32 cover 50 receipts and $173,821.08. This explains the population but does
not change the projection state or backfill the selected master.

## Next gate

Add explicit historical-registration and unresolved-reported-ID assertion
states before terminal classification. Then publish the independent Schedule
B sender-side boundary and define a reconciliation calculation that preserves
both assertions without double counting.
