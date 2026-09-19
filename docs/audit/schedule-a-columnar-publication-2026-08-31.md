# Schedule A columnar publication gate

> **Observation date:** 2026-08-31 America/New_York  
> **Source release:** `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`  
> **Cycle:** 2024  
> **Status:** Passed; Parquet is the accepted physical layout for complete
> processed Schedule A fact sets

## Result

The production publisher converted the complete selected 2024 Schedule A
relation into one immutable `fec.schedule_a_receipt.v1` fact set. It preserved
all 81 source values, exact source-row locators, and 18 policy-free typed or
lineage columns. It published only after the source replay, every newly written
Parquet shard, and all conservation checks passed.

| Measure | Result |
|---|---:|
| Source rows | 264,085,606 |
| Published facts | 264,085,606 |
| Invalid facts | 0 |
| Excluded source rows | 0 |
| Parquet columns | 99 |
| Parquet shards | 265 |
| Parquet row groups | 2,113 |
| Parquet bytes | 16,759,429,988 (15.61 GiB) |
| Selected zstd COPY bytes | 14,765,199,882 (13.75 GiB) |
| Parquet size overhead | 13.506% |
| Selected uncompressed bytes | 182,881,299,512 |
| Publication wall time | 6,833.490 seconds (1h 53m 53s) |
| End-to-end publication rate | 38,646 rows/s |

The fact-set ID is
`6750b7656e14da33ba6c6324c4d575579562f2f5d0fd1816ab26a20b90de780d`.
The active pointer was written at `2026-08-31T08:33:26.738169508Z`. The
publisher process exited 0, was not OOM-killed, and removed its resumable
checkpoint after publication.

## Integrity evidence

The publisher replayed the complete selected source and matched these pinned
identities:

- compressed SHA-256:
  `566c5509e5e31c0d8cbf5f74bb78a1f716411d8e5f858440ec166ba1b7da8909`;
- uncompressed SHA-256:
  `3ef0fdcdbf246981e5724e810c89d11b79b702369b7467633e783d3936f2b9a5`;
- complete fact semantic SHA-256:
  `7f6d9e95cfa4a3fffad4566ccea1851ea619d1e4de1cd7b1e86dc48846151865`.

Each new shard was closed, checked against its write-stream byte count and
SHA-256, fully reread through `parquet-go` v0.32.0, checked against the exact
99-column schema, and compared with its source-value semantic digest before it
entered the checkpoint. All seven blocking manifest checks passed.

After rebuilding the image with the final reuse hardening, an idempotent rerun
rehash-verified all 265 published files in 9.169 seconds. It returned the same
fact-set ID and publication timestamp without replaying or rewriting the
source.

DuckDB 1.4.1 then independently scanned all 265 files. The projected scan took
2.029 seconds and proved:

- 99 columns and 264,085,606 rows;
- source ordinals from 1 through 264,085,606 with the exact expected ordinal
  sum of 34,870,603,780,236,421;
- raw byte coverage from 0 through 182,881,299,512;
- raw row-length sum of 182,881,299,512; and
- zero rows with `lt_normalization_state = 'invalid'`.

## Performance finding

The complete run was materially slower than the bounded write-only benchmark.
The ten-million-row benchmark wrote 117,848 rows/s, while production
publication completed at 38,646 rows/s. The production path synchronously
closes and fully rereads each one-million-row shard before it begins the next
shard. That verification is required, but its serial scheduling is not.

Before weekly automation, benchmark bounded overlap or parallel readback of
completed shards while preserving all current checks, deterministic
checkpoints, memory caps, and failure behavior. Do not weaken or sample the
semantic readback to recover throughput.

## Decision consequence

The complete gate accepts Parquet as the physical Schedule A fact layout. The
logical fact remains `fec.schedule_a_receipt.v1`; immutable zstd COPY remains
the exact-byte source authority. Filesystem manifests and Parquet hold the
fine-grained facts. ArangoDB will consume selected entity, monetary, and path
projections from an exact published manifest rather than store every wide raw
receipt as a graph document.

Dagster automation remains paused until compact occurrence/change evidence,
compact calculation membership, and coordinated fact-bundle readiness are
implemented and proven against this fact set.
