# Independent-expenditure projection readiness bundle audit — 2026-08-31

## Scope

This audit publishes one immutable readiness selection for the real 2024
independent-expenditure graph projection, replays the publication, and invokes
the existing ArangoDB projection through that bundle rather than mutable
calculation and master pointers.

## Bundle identity

- Bundle ID:
  `eef89f0dbb5ee321aa0db72bc1456c4a80014d1223795d03a4b335e17f7e08ea`
- Cycle: `2024`
- State: `ready`
- Source release:
  `fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`
- Effective calculation set:
  `315227127d5707ff3246508d487e1d0be358e715d48b9829cee83f697cac8ce4`
- Schedule E fact set:
  `f38758f7f151505b892a217c856a3fdc81a93ca0ab0670906baed677951921ee`
- Candidate fact set:
  `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6`
- Committee fact set:
  `26c2ae5089155cf134257f381060050c5bd4ef64b400bfc37b92d4f3cc16d19c`

The identity also binds all four immutable manifest SHA-256 values, including
the Schedule E fact manifest named by the calculation.

## Readiness counts

| Input | Records |
|---|---:|
| Effective spender-candidate-stance results | 5,495 |
| Candidate-master facts | 9,798 |
| Committee-master facts | 20,938 |

The publisher passed six blocking checks:

1. exact role set;
2. one cycle;
3. one coordinated source release;
4. mutable pointer equality with immutable manifests;
5. complete backing-artifact digest verification; and
6. projection readiness.

The bundle contains no copied facts, calculation results, or graph documents.

## Replay

A second publication with a different run ID returned the original bundle,
including its original run ID and publication time. It did not write a new
immutable manifest or change the current pointer.

The graph command then accepted only the bundle path, resolved its three
immutable input manifests, and reused projection
`c198c1c0957db3f99f139c3717c7d88ee684121229ed667fd81eb7cd51b9b1ad`.
All 1,953 entity and 5,495 edge counts and all exact amount totals remained
unchanged. The graph did not reimport documents.

## Dagster boundary

The new multi-partitioned readiness asset maps one `cycle` calculation
partition and exactly two classic `dataset`/`cycle` partitions:

- `candidate-master`;
- `committee-master`; and
- the effective independent-expenditure calculation for the same cycle.

An eager automation sensor targets the readiness bundle and downstream graph
asset. The graph asset receives only the stored bundle artifact plus ArangoDB
connection settings. Python does not choose manifests, inspect records, or
perform graph or money logic.

## Verdict

The 2024 readiness and automation boundary passes. A changed Schedule E
calculation or either changed master fact set produces a new bundle identity
and targets only that cycle's graph projection. An unchanged input set reuses
both the bundle and the content-addressed graph.

The remaining operational gate is to publish coherent master facts and run
the same bundle/projection path for 2020, 2022, and 2026.
