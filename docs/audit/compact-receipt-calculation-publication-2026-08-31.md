# Compact 2024 candidate-receipt calculation publication

> **Observation date:** 2026-08-31 America/New_York  
> **Source release:** `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`  
> **Schedule A fact set:** `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`  
> **Calculation set:** `f8f2eefacff34b7b420246e2e665db525a1b13320673ee8b30620450dc5c39d4`  
> **Cycle:** 2024  
> **Status:** Accepted; complete-corpus equivalence and replay gates passed

## Question

Can Legal Tender preserve exact inclusion, exclusion, and unresolved receipt
membership without writing one decision document for every Schedule A fact?

The answer is **yes**. The production publisher binds one exact columnar fact
set to a versioned ordered predicate. It materializes only exceptional row
membership and candidate results. Every ordinary row decision is reproducible
from the fact-set identity, one-based row ordinal, and predicate version.

## Inputs

The calculation pinned four fact sets from one coordinated release:

| Role | Fact-set ID | Records |
|---|---|---:|
| Schedule A receipts | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` | 264,085,606 |
| Candidate-committee linkage | `4327fff8f584be8670174977b8fd5b93da4b2700c98c81915b5acce40cd8b718` | 8,619 |
| All-candidates summary | `87cfc643cf2dc090f98503bbc8b766d2d5bf320e46f5a059996c6464c6ef75bc` | 3,826 |
| Current-campaigns summary | `96bb26eda7e1f9849f47e5b97a182ec0e0e605f171a3cc1a9004b85a9e5873fe` | 2,368 |

The publisher rehashed all 265 Parquet shards before evaluation. Its reader
projected only the nine columns declared in
`legal-tender.fec.itemized-individual-receipt-membership-predicate.v1`.

## Runtime and storage

The complete publication ran from `13:56:42.035996681Z` through
`14:11:18.943473689Z`: **14m 36.907s**. A live sample used 149.4 MiB under the
16 GiB container cap and `GOMEMLIMIT=2GiB`. The run exited zero and was not
OOM-killed.

| Artifact | Records | Raw bytes | Compressed bytes |
|---|---:|---:|---:|
| Sparse exceptions | 2 | 722 | 314 |
| Candidate results | 8,175 | 17,481,529 | 1,016,233 |

The complete compact calculation tree uses **1,077,859 bytes**, including the
immutable and current manifest copies. No staged file remained after
publication. A same-input replay rehashed immutable backing and returned the
same manifest in **8.032s** without rewriting either data artifact.

## Exact equivalence

The compact publisher reproduced the retained direct probe exactly:

| Decision | Rows |
|---|---:|
| Included itemized-individual receipt | 222,205,451 |
| Excluded non-individual | 23,960,090 |
| Excluded memo subtotal | 17,920,063 |
| Unresolved individual classification | 0 |
| Unresolved amount | 2 |
| **Total** | **264,085,606** |

Included signed source amounts sum to **$15,887,569,341.13**. Candidate
routing also matched exactly: 32,154,142 routed rows, 231,931,464 rows without
a candidate route, and zero routed invalid receipt dates.

The publisher emitted 8,175 candidate results: 7,440 complete, 30 partial, and
705 not comparable. The result artifact is byte-identical to the direct probe:

- Uncompressed SHA-256:
  `fc63dbdec85d0f3693ed35641255c426190669c9441f8bd72d514d9a6430f23f`
- Compressed SHA-256:
  `6a4497efaf35cae1f3223d1553ff02296aaa57099cd3672d236bc2981ebb4ade`

All overall and per-summary reconciliation counts also match the direct probe.

## Sparse exceptions

The only exceptions are source row ordinals 141,770,828 and 141,770,829.
Both route to committee `C00845032`, have `amount_state=source_null`, and are
explicit `unresolved_amount` memberships. Their identities and reasons are
content-addressed evidence. No ordinary included or excluded membership is
duplicated as JSON.

## Decision consequence

1. Accept the compact calculation contract and production publisher for this
   component.
2. Keep the versioned predicate, exact input fact-set identity, and source row
   ordinal as ordinary membership evidence.
3. Materialize unresolved or invalid membership as sparse exception records
   and keep candidate results as the query-facing calculation artifact.
4. Do not run or automate the rejected dense per-row decision publisher for a
   complete cycle.
5. Add coordinated fact-bundle readiness before Dagster automation. Then use
   this accepted result to design and measure the real ArangoDB projection.
