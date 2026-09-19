# ArangoDB candidate-receipt projection audit — 2026-08-31

## Result

The Go-only version 1 candidate-receipt projection passed its complete 2024
import, count-conservation, query, and idempotent-replay gates. It is marked
`partial` because 143 candidate IDs and 156 committee IDs used by the
calculation do not have same-cycle master facts.

No Schedule A row was loaded as a graph document. The probe read the compact
candidate result artifact plus candidate and committee master artifacts.

## Identity

| Field | Value |
|---|---|
| Cycle | `2024` |
| Projection version | `legal-tender.arango.candidate-receipts-projection.v1` |
| Projection ID | `e3530a3f82e329eeb708ff1d06639c0f283469b8d7726ee6db8efac3b9c14c66` |
| Isolated database | `lt_probe_2024_e3530a3f82e329ee` |
| Named graph | `candidate_receipts` |
| Fact bundle | `b00ce42a65696310b8c1f8c3f8f3bc28077f5bc774e4955657cc16b210d629a3` |
| Compact calculation | `f8f2eefacff34b7b420246e2e665db525a1b13320673ee8b30620450dc5c39d4` |

The projection also binds the exact candidate-master and committee-master fact
set IDs and all four manifest byte digests in its metadata document.

## Counts

| Collection or condition | Expected | Observed |
|---|---:|---:|
| All entity vertices | 31,035 | 31,035 |
| Candidate vertices | 9,941 | 9,941 |
| Committee vertices | 21,094 | 21,094 |
| Candidate result documents | 8,175 | 8,175 |
| Candidate-committee relationship edges | 8,584 | 8,584 |
| Receipt-component edges | 2,116 | 2,116 |
| Candidate placeholders | 143 | 143 |
| Committee placeholders | 156 | 156 |

The placeholder counts are not import failures. They preserve calculation IDs
that are absent from the cycle master products without inventing names or
other attributes. Their population needs a source-history audit before a
production projection can be `ready`.

## Storage figures

ArangoDB 3.11 reported these collection figures after the replay. They are
storage-engine figures, not portable serialized sizes.

| Collection | Document bytes | Index bytes |
|---|---:|---:|
| `entities` | 45,346,041 | 3,215,817 |
| `candidate_results` | 45,346,041 | 845,625 |
| `candidate_committee_relationships` | 38,318,046 | 3,009,642 |
| `receipt_components` | 3,716,271 | 356,160 |
| `projection_metadata` | 897 | 51 |
| **Total** | **132,727,296** | **7,427,295** |

Combined reported document and index storage was 140,154,591 bytes, or about
133.66 MiB. This reinforces the coarse-projection boundary; it does not
estimate the footprint of future donor and committee-flow edges.

## Query measurements

The first execution warmed each query once, then measured 25 HTTP round trips.
The representative candidate was selected deterministically as the candidate
with the most receipt-component edges.

| Query | Rows | Minimum | Median | p95 | Maximum |
|---|---:|---:|---:|---:|---:|
| Candidate result by document ID | 1 | 80 µs | 92 µs | 142 µs | 180 µs |
| Inbound receipt-component neighborhood | 3 | 131 µs | 177 µs | 299 µs | 311 µs |

These are local warm-cache probe measurements. They are not capacity or
production latency claims.

## Replay

A second invocation rebuilt the deterministic in-memory document model,
found the matching completion metadata, skipped every import, and reused the
same database. It preserved all expected and observed counts. Ten repeated
queries completed again; medians were 132 µs for point lookup and 155 µs for
the inbound neighborhood.

## Accepted conclusions

1. Keep complete Schedule A facts in immutable Parquet rather than storing 264
   million wide graph documents.
2. Keep query-bearing candidate, committee, relationship, monetary component,
   and calculation result projections in ArangoDB.
3. Use content-derived projection identity and publish metadata last.
4. Preserve absent master facts as explicit placeholders and a partial state.
5. Do not claim PAC transfer, terminal-source, independent-expenditure, or
   multi-hop path behavior from this one-hop graph.

The physical model and remaining gates are in the
[projection design](../design/arango-candidate-receipt-projection.md).
