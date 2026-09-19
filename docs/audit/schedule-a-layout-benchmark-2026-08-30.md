# Schedule A physical-layout benchmark

> **Observation date:** 2026-08-30 America/New_York  
> **Source release:** `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`  
> **Cycle:** 2024  
> **Status:** The bounded gate passed; the subsequent complete-corpus
> publication also passed

## Question

Can a lossless columnar Schedule A artifact preserve the source grain and
lineage without the storage multiplication of the rejected JSON fact layout,
while letting calculations read only the columns they need?

The bounded answer was **yes**. A ten-million-row Parquet candidate retained all
81 decoded source values, round-tripped to the same semantic digest, made the
same receipt decisions, stayed within 7.54% of an equal-row zstd COPY baseline,
and scanned the five calculation columns 4.18 times faster. This accepted
Parquet for a complete-corpus probe. The subsequent
[complete publication gate](./schedule-a-columnar-publication-2026-08-31.md)
accepted the production physical contract; Dagster automation remains gated
on compact dependent evidence.

## Inputs and candidate

The benchmark read the first 10,000,000 rows of the retained 2024 selected
relation. The complete immutable relation contains 264,085,606 rows and is
14,765,199,882 compressed bytes. Its compressed SHA-256 is
`566c5509e5e31c0d8cbf5f74bb78a1f716411d8e5f858440ec166ba1b7da8909`.

The comparison wrote the same ten million rows twice:

- zstd-compressed PostgreSQL COPY with one encoder worker;
- ten one-million-row Parquet files with 128,000-row groups, zstd compression,
  and `parquet-go` v0.32.0.

The candidate stores all source lexemes as nullable strings except the FEC
`is_individual` value, which is a nullable Boolean. Ordered shard ranges map
each row to its exact one-based source ordinal. The immutable COPY relation
remains the byte authority. This candidate is lossless and columnar, but it is
not yet the final typed normalized-fact surface.

The projected calculation read only `cmte_id`, `contb_receipt_dt`,
`contb_receipt_amt`, `memo_cd`, and `is_individual`. It used Parquet column-page
readers rather than reconstructing full generic rows.

## Ten-million-row result

| Measure | Equal-row zstd COPY | Parquet candidate |
|---|---:|---:|
| Rows | 10,000,000 | 10,000,000 |
| Compressed bytes | 446,067,333 | 479,698,112 |
| Compressed bytes per row | 44.607 | 47.970 |
| Write time | 41.713s | 84.854s |
| Write rate | 239,729 rows/s | 117,848 rows/s |
| Five-column scan time | 13.996s | 3.350s |
| Five-column scan rate | 714,458 rows/s | 2,984,372 rows/s |

The Parquet-to-equal-row-zstd size ratio was 1.0754. The projected scan
speedup was 4.1771. Peak process RSS was 266,911,744 bytes and Go reported
336,132,424 bytes of system memory, both well below the 2 GiB process gate and
the 16 GiB container cap.

At the measured write rate, a complete 2024 Parquet pass projects to about
37m21s. Applying the measured 1.0754 ratio to the complete compressed source
projects 15,878,406,650 bytes, or 14.79 GiB. Applying the scan rates to the
complete row count projects about 88.5s for the five-column Parquet scan and
369.6s for the source scan. These are capacity estimates, not complete-corpus
measurements.

## Equivalence

The complete 81-column semantic digest matched before and after Parquet
round-trip. The five-column projection digest also matched. Both readers made
exactly the same decisions:

| Decision | Rows |
|---|---:|
| Included itemized-individual receipt | 8,179,136 |
| Excluded non-individual | 955,659 |
| Excluded memo subtotal | 865,205 |
| Unresolved individual classification | 0 |
| Unresolved amount | 0 |

The included signed amount was 142,148,485,684 minor units through both
readers. The shared full semantic digest was
`749a9c9cd14bfba5e9da83e9b5a2ed1f470c3e98fe399c0e06cd73319dfaa100`;
the shared projection digest was
`b9ddf9d31d6b546506c032bbe77a322642acc231b62b4d673c6ad316aea5b290`.

## Reader correction and artifact version

The first one-million-row pass used a generic projected-row reconstruction
and scanned at only 0.886 times the source rate. A direct column-page reader
then produced a 4.52-times speedup at the same row count. The ten-million-row
gate used that corrected reader. This is part of the physical contract: a
columnar file alone does not produce column pruning if the reader rebuilds
wide rows.

The executed ten-million-row artifact uses benchmark schema v1. Its measured
bytes, rates, digests, decisions, and verdict are valid. Its
`projected_parquet_bytes` field incorrectly scaled the unusually compressible
sequential prefix's bytes per row, yielding 12,668,136,660 bytes. The source
prefix used 44.607 bytes per row while the complete compressed source uses
about 55.91. Benchmark schema v2 corrects this reporting-only field by applying
the measured layout ratio to the known complete source size; the corrected
estimate is 15,878,406,650 bytes. The size gate itself used the measured
equal-row ratio and is unchanged.

## Gates and decision

The ten-million-row run passed every bounded gate:

- at least 10,000,000 decision rows;
- Parquet no larger than 1.5 times equal-row zstd;
- at least 100,000 Parquet rows written per second;
- projected complete write no longer than 45 minutes;
- at least 1.5-times projected-scan speedup;
- process RSS no higher than 2 GiB; and
- exact full-row, projection, and decision equivalence.

The verdict was `accepted_for_full_corpus_probe`. The next run had to process the
complete relation, publish immutable partition metadata, verify every shard
and ordinal range, exercise retry behavior, measure representative and
complete scans, and prove source-to-candidate drilldown. Only that result can
choose the production Schedule A physical fact layout and replace the paused
JSON publisher. That complete run passed on 2026-08-31; its actual
measurements and decision are in the
[columnar publication audit](./schedule-a-columnar-publication-2026-08-31.md).

The retained benchmark directories under `/storage/probes` are evidence only:
`fec-schedule-a-layout-2024-1m-v1`,
`fec-schedule-a-layout-2024-1m-v2`, and
`fec-schedule-a-layout-2024-10m-v1`. They have no active pointer and are not
Dagster assets.
