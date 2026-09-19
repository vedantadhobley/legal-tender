# 2024 candidate-receipt fact-bundle publication

> **Observation date:** 2026-08-31 America/New_York  
> **Source release:** `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`  
> **Bundle ID:** `b00ce42a65696310b8c1f8c3f8f3bc28077f5bc774e4955657cc16b210d629a3`  
> **Cycle:** 2024  
> **Status:** Accepted; readiness, integrity, idempotence, and calculation-replay gates passed

## Question

Can Dagster start the compact candidate-receipt calculation only after the
exact Schedule A, linkage, and two summary fact sets for one cycle and source
release are ready, without moving source or calculation policy into Python?

The answer is **yes**. Dagster maps the required fact partitions to one bundle
partition. Go resolves and verifies the four current fact pointers, freezes
their immutable manifest identities, and publishes one content-addressed
bundle. The calculation consumes that bundle instead of independently reading
four mutable pointers.

## Inputs

| Role | Fact-set ID | Records |
|---|---|---:|
| Schedule A receipts | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` | 264,085,606 |
| Candidate-committee linkage | `4327fff8f584be8670174977b8fd5b93da4b2700c98c81915b5acce40cd8b718` | 8,619 |
| All-candidates summary | `87cfc643cf2dc090f98503bbc8b766d2d5bf320e46f5a059996c6464c6ef75bc` | 3,826 |
| Current-campaigns summary | `96bb26eda7e1f9849f47e5b97a182ec0e0e605f171a3cc1a9004b85a9e5873fe` | 2,368 |

Every fact set belongs to cycle 2024 and the same coordinated source release.
The publisher matched each selected pointer to its immutable manifest, rehashed
all 265 Parquet shards, and verified all three classic zstd artifacts.

## Publication and replay

The first publication ran from `18:35:09.518711939Z` through
`18:35:17.603138740Z`: **8.084s**. It passed all six blocking checks:

- exact role set;
- cycle coherence;
- source-release coherence;
- manifest immutability;
- backing integrity; and
- calculation readiness.

The immutable manifest and active 2024 pointer are each 3,270 bytes and have
the same SHA-256:
`66108c97917ea4cc18b9f22c4481862d888f5844e0d8c7cd3e73938bfd596de0`.

A same-input publication replay completed in **8.039s**. It returned the
original bundle byte-for-byte, including its original run ID and publication
time. It did not create a second logical bundle.

## Calculation replay

The compact calculation was then invoked with only the active bundle path and
the storage root. It resolved the four immutable manifests named by the bundle,
revalidated the bundle and its backing, and returned the existing calculation
set in **8.027s**:

`f8f2eefacff34b7b420246e2e665db525a1b13320673ee8b30620450dc5c39d4`

The returned manifest retained every previously accepted count and artifact
identity: 264,085,606 decisions, $15,887,569,341.13 in included signed source
amounts, two sparse exceptions, and 8,175 candidate results. The coordination
layer changed no domain result.

## Decision consequence

1. Accept the immutable four-input fact bundle as the calculation-readiness
   boundary.
2. Use Dagster multi-dimensional partition mappings to join the Schedule A
   cycle and exactly three required classic datasets. Do not encode
   `dataset:cycle` into an opaque string.
3. Let Dagster's eager automation launch the bundle only when all mapped facts
   are available, then launch the compact calculation from the bundle.
4. Keep same-release, immutable-pointer, and complete backing verification in
   Go. Dagster carries paths, dependencies, partition state, and metadata only.
5. Use the accepted 2024 result as the input to the real-corpus ArangoDB
   projection probe.
