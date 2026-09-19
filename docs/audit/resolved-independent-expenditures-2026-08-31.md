# Resolved independent expenditures and graph gate — 2026-08-31

## Outcome

The complete 2024 candidate-resolution publication now feeds a separate
resolved spender-candidate-stance calculation, exact readiness bundle, and v2
ArangoDB projection. All blocking lineage, count, amount, master-coverage,
readback, and replay checks passed.

The v2 projection is a new content-addressed database. The earlier v1
reported-ID probe remains unchanged and is not relabeled as resolved output.

## Exact immutable inputs

| Input | Identity |
|---|---|
| FEC source release | `fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a` |
| Schedule E fact set | `f38758f7f151505b892a217c856a3fdc81a93ca0ab0670906baed677951921ee` |
| Candidate-resolution calculation | `e947b7ff3e9231864d582526972e07a7acf3a8301c2ffb43e76de8e340ee43ba` |
| Candidate-resolution manifest SHA-256 | `5c2b9681b7e5fa658abaa46a99a75bd8e20e7ba489e5dc0d202422eba9770553` |
| Candidate-resolution decisions SHA-256 | `7df1629a2bd64720c8cea92f8873809be3783401dc4a9c7d09a47ee2289fdd97` |
| Candidate-master fact set | `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6` |
| Committee-master fact set | `26c2ae5089155cf134257f381060050c5bd4ef64b400bfc37b92d4f3cc16d19c` |

The readiness bundle requires the candidate master to equal the exact fact set
used by candidate resolution. Same cycle and release alone are insufficient.

## Resolved aggregate

The accepted calculation is
`484d2ddc86fe0e82e0e2e7d5a470f67b4d89cfddd0c3784a43b9d57b2263a830`.
It reads only the dense candidate-resolution decisions. It does not reread or
reinterpret Schedule E.

| Route or state | Facts | Signed amount |
|---|---:|---:|
| Source decisions | 58,288 | $4,337,242,339.31 |
| Projectable | 57,992 | $4,318,692,700.10 |
| Confirmed | 45,185 | $3,475,818,717.05 |
| Resolved | 1,811 | $299,813,805.04 |
| Unverified | 10,996 | $543,060,178.01 |
| Ambiguous | 0 | $0.00 |
| Unresolved | 296 | $18,549,639.21 |

Confirmed, resolved, and unverified decisions produced 5,303 groups. The
earlier reported-ID calculation had 5,495 groups. Regrouping by resolved ID
merged or redirected 192 net group keys; this is not a row deletion count.

Every result retains positive, negative, and zero counts plus separate
confirmed, resolved, and unverified counts and signed amounts. Every ambiguous
or unresolved decision becomes one sparse exception outside candidate edges.

| Artifact | Records | Uncompressed | Compressed | Compressed SHA-256 |
|---|---:|---:|---:|---|
| Results | 5,303 | 3,432,751 bytes | 323,572 bytes | `55b2925b776d0bdc1ee0c0b0a9e2ca1b99eed9fcdf19b236926f162292f10fed` |
| Exceptions | 296 | 200,470 bytes | 36,809 bytes | `c5c15c87319b15d2da45c0c8a3da6de4255c0230e650388b86c3ecf1c2abe5ce` |

An identical calculation invocation reused the immutable manifest and both
artifacts.

## Projection readiness

Bundle
`67cfb78d87076b2897ed8e7fd1eb5999e6fca477307190b419f7f7e83451333f`
pins the aggregate, candidate-resolution ancestry, both aggregate artifacts,
and both master fact sets. Seven blocking checks passed, including complete
backing verification and exact candidate-master identity.

Dagster now automates this resolved bundle and v2 graph. The v1 bundle and
reported-ID graph assets remain registered as historical/manual evidence but
are no longer the outside-spending automation target.

## ArangoDB v2 gate

Projection
`1817e94e6ef06e700bd8368f256c2ad88b8592906e3a8a6b760191eee39c44f0`
created database
`lt_ie_probe_resolved_2024_1817e94e6ef06e70`. The named graph remains
`independent_expenditures` inside its isolated database.

| Measure | Expected | Observed |
|---|---:|---:|
| Entities | 1,858 | 1,858 |
| Candidates | 911 | 911 |
| Spenders | 947 | 947 |
| Edges | 5,303 | 5,303 |
| Support edges | 3,889 | 3,889 |
| Opposition edges | 1,414 | 1,414 |
| Projected signed amount | $4,318,692,700.10 | $4,318,692,700.10 |
| Support signed amount | $1,809,660,053.46 | $1,809,660,053.46 |
| Opposition signed amount | $2,509,032,646.64 | $2,509,032,646.64 |

No referenced candidate or spender lacked a same-release master fact. This
removes the v1 projection's 92 missing-candidate placeholders and yields state
`ready`.

The first post-import storage observation was 19,650,630 combined document and
index bytes. RocksDB figures changed during background compaction; the replay
observation was 16,551,087 bytes. Storage figures are operational observations,
not projection identity.

All three representative queries returned rows on every run. Ten-run p95
latencies were 617 microseconds for candidate inbound paths, 398 microseconds
for spender outbound paths, and 465 microseconds for candidate stance summary.

An identical replay reused the completed database, read all counts and exact
amounts back, and reran the queries without imports.

## Boundary and later cross-cycle result

This proves a resolved one-hop outside-spending projection. It does not prove
PAC transfer paths, terminal-source attribution, coordination, influence,
corruption, donations, or personal payments to candidates.

The same unchanged method later passed the 2020, 2022, and 2026 graph gates.
See the [cross-cycle audit](./resolved-independent-expenditures-cross-cycle-2026-08-31.md).
A later production publication must combine accepted receipt, transfer,
outside-spending, and entity projections without weakening their separate
evidence and money semantics.
