# ArangoDB independent-expenditure projection audit — 2026-08-31

## Scope

This audit projects the complete accepted 2024 effective Schedule E
calculation into an isolated ArangoDB named graph, reads exact values back,
executes representative queries, and repeats the command to prove idempotent
reuse. It does not modify the legacy graph or claim production publication.

## Release-coherence correction

The first invocation failed before database creation. The effective
calculation belonged to active release
`fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`,
while the current candidate and committee master pointers still named a prior
release with prefix `fec-a6e6f777`. The projector did not relax the same-
release invariant.

The 2024 candidate and committee occurrence/fact layers were republished from
the active v2 release. Every source row was unchanged from the previous
release and no invalid rows appeared:

| Dataset | Rows | New fact-set prefix |
|---|---:|---|
| Candidate master | 9,798 | `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6` |
| Committee master | 20,938 | `26c2ae5089155cf134257f381060050c5bd4ef64b400bfc37b92d4f3cc16d19c` |

## Projection identity

- Projection ID:
  `c198c1c0957db3f99f139c3717c7d88ee684121229ed667fd81eb7cd51b9b1ad`
- Database: `lt_ie_probe_2024_c198c1c0957db3f9`
- Graph: `independent_expenditures`
- Effective calculation set:
  `315227127d5707ff3246508d487e1d0be358e715d48b9829cee83f697cac8ce4`
- Schedule E fact set:
  `f38758f7f151505b892a217c856a3fdc81a93ca0ab0670906baed677951921ee`
- Candidate fact set:
  `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6`
- Committee fact set:
  `26c2ae5089155cf134257f381060050c5bd4ef64b400bfc37b92d4f3cc16d19c`
- State: `partial`

## Count and amount conservation

Every expected value matched the database readback.

| Measure | Expected | Observed |
|---|---:|---:|
| Entities | 1,953 | 1,953 |
| Candidates | 1,002 | 1,002 |
| Spender committees | 951 | 951 |
| Edges | 5,495 | 5,495 |
| Support edges | 4,028 | 4,028 |
| Opposition edges | 1,467 | 1,467 |
| Attributed amount | $4,337,242,339.31 | $4,337,242,339.31 |
| Support amount | $1,819,924,212.53 | $1,819,924,212.53 |
| Opposition amount | $2,517,318,126.78 | $2,517,318,126.78 |

Amount verification read every edge's decimal minor-unit value and summed it
in Go with arbitrary-precision integers. ArangoDB did not perform financial
arithmetic.

The graph contains 92 candidate placeholders and zero spender placeholders.
This missing-master coverage is why the result is `partial`; it does not affect
edge or amount conservation.

## Storage

| Collection | Document bytes | Index bytes |
|---|---:|---:|
| `entities` | 1,898,608 | 310,056 |
| `independent_expenditure_edges` | 4,017,208 | 4,365,090 |
| `projection_metadata` | 2,568 | 231 |
| **Total** | **5,918,384** | **4,675,377** |

The combined physical figure was 10,593,761 bytes, about 10.10 MiB. No raw
Schedule E row was copied into ArangoDB.

## Query measurements

The first run used representative candidate `P00009423`, representative
spender `C00483693`, one warm-up, and 25 measured repetitions.

| Query | Rows | Median | p95 |
|---|---:|---:|---:|
| Candidate inbound named-graph paths | 25 | 310 µs | 397 µs |
| Spender outbound named-graph paths | 25 | 299 µs | 310 µs |
| Candidate stance summary | 2 | 335 µs | 357 µs |

These timings validate the one-edge outside-spending model only. They are not
evidence for future multi-hop transfer or graph-analysis workloads.

## Replay

An identical second invocation returned the same projection ID and database
with `reused_projection: true`. It did not reimport documents. Counts, exact
amounts, missing-master counts, and storage figures remained unchanged. Replay
query medians were 308 µs, 297 µs, and 488 µs respectively.

## Verdict

The physical model passes the 2024 gate. It preserves source/calculation
lineage, support/opposition separation, exact cents, explicit incomplete
master coverage, bounded storage, graph queryability, and immutable replay.

This database is not production-ready. The readiness bundle now drives
Dagster automation, but the candidate-reference audit below proves that
history enrichment alone cannot make the affected edge identities reliable.
Per-fact resolution and the remaining cycles are still required.

## Readiness follow-up

The exact-input bundle and eager Dagster chain subsequently landed. A real
2024 bundle-only invocation passed all bundle checks and reused this exact
projection without imports. The subsequent
[candidate-reference audit](./schedule-e-candidate-reference-integrity-2026-08-31.md)
found that the 92 placeholders cover 199 edges and $52,059,391.58, and that
they cannot be repaired by copying history metadata onto the reported IDs.
This database remains a physical and arithmetic probe; a per-fact reference
calculation now precedes the other three cycles and unified publication. See
also the
[projection-readiness bundle audit](./independent-expenditure-projection-bundle-2026-08-31.md).
