# Accepted pre-attribution interpretation publications — 2026-09-20

Status: complete 2024 calculation publications. The user accepted the four
boundaries supported by the focused validation. Existing source facts,
candidate-resolution decisions, reconciliation components, graph generations and
financial totals remain unchanged.

## Accepted boundaries

1. Preserve filer-reported candidate assertions separately from exact-context
   alternatives. Only exact reported-ID and context agreement supplies a safe
   default endpoint.
2. Retain strict raw/clean counterparty-ID agreement for money-carrying committee
   flows. One-sided IDs remain self-reference evidence outside that ledger.
3. Retain exact-reference conduit pair/star associations with zero added money and
   no amount/date-equality requirement.
4. Replace transitive A/B components for presentation with direct pair candidates,
   explicit date gaps and per-observation candidate degree. Comparison still
   carries no money and performs no deduplication.

The accepted decision is recorded in the [decision log](../decisions.md). The
[validation report](./pre-attribution-interpretation-validation-2026-09-20.md)
contains the source evidence and population analysis.

## Candidate interpretations

The new command consumes one exact immutable candidate-resolution publication:

```text
legal-tender pipeline fec publish-independent-expenditure-candidate-interpretations \
  --storage-root /storage \
  --cycle 2024 \
  --candidate-resolution <exact-or-current-resolution-manifest> \
  --run-id <stable-run-id>
```

Calculation-set ID:
`f9b92025f6390a961149397da86cfe7c111177fb7d06824b0849107290c1205f`.

The publication conserves all 58,288 source decisions and
$4,337,242,339.31:

| Interpretation | Rows | Signed amount |
|---|---:|---:|
| Confirmed and safe default | 45,185 | $3,475,818,717.05 |
| Inferred alternative; reported ID absent from master | 757 | $33,509,752.37 |
| Conflicting alternative; reported ID present | 1,054 | $266,304,052.67 |
| Reported ID present but unverified | 10,996 | $543,060,178.01 |
| Ambiguous | 0 | $0.00 |
| Unresolved | 296 | $18,549,639.21 |

The 58,288-row artifact is 7,704,013 compressed bytes. Its compressed SHA-256 is
`009e7c28e44cedd43d4ef233d82380d1b2953cd8d69bc817af50613d0be47fb8`;
its uncompressed SHA-256 is
`ae977d50fa3d85add537bc042bc84db4deaaac058c10d6ae4f4f61d5544cbe52`.
All seven blocking checks pass. A fresh invocation semantically decoded and
validated every row, recomputed all counts and signed amounts, and reused the same
immutable calculation and artifact.

## Direct Schedule A/B comparison candidates

The new command consumes the exact published reconciliation and its retained
observation artifacts:

```text
legal-tender pipeline fec publish-committee-flow-comparison-candidates \
  --storage-root /storage \
  --cycle 2024 \
  --reconciliation <exact-or-current-reconciliation-manifest> \
  --run-id <stable-run-id> \
  --max-candidate-pairs 10000000
```

The capacity argument is an operational fail-closed guard. It does not filter or
rank candidates; exceeding it writes no partial publication.

Calculation-set ID:
`87599850f9292cadb6a2ae3bf80ac8a9726a82d9615c709ff6cf163c20438221`.

The complete direct comparison contains:

| Measure | Count |
|---|---:|
| Schedule A observations | 320,731 |
| Schedule B observations | 341,720 |
| Direct candidate pairs | 471,229 |
| Mutual one-to-one pairs | 155,364 |
| Pairs with competing candidates | 315,865 |
| Exact role/amount/date signatures | 40,877 |
| Same role/amount with different dates | 420,525 |
| Same role/date with conflicting amounts | 8,319 |
| Same amount/date with conflicting roles | 1,508 |

The candidate artifact is 37,383,002 compressed bytes and 497,251,583 bytes
before compression. Its compressed SHA-256 is
`ba453a9f0ba5b1e98477e0dcd56295583bb2805ea378d30fcc322988d7a34e97`;
its uncompressed SHA-256 is
`29acc0f09510470418d993dc8076a4ae8546c3202b04e677258455d24f4ab612`.
All seven blocking checks pass. A fresh invocation decoded and validated every
candidate, recomputed source coverage, state and ambiguity counts, and reused the
same immutable publication.

## Verification and remaining boundary

The complete Go suite passes. The repository JSON-contract suite passes all 13
tests. Both live replays verify exact source ancestry, immutable backing, semantic
record validity, identity uniqueness, ordering and complete count conservation.

These calculations are not yet graph or product consumers. The next implementation
is one product-shaped 2024 candidate slice that reads the accepted candidate
interpretations while keeping direct receipts, support, opposition, committee
paths, unresolved states and source drilldown separate. It may expose A/B
comparison evidence, but it must not promote comparison candidates into financial
edges or terminal attribution.
