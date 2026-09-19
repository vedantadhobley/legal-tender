# Schedule A compact occurrence publication — 2026-08-31

## Result

The compact Schedule A occurrence and change representation passed its bounded
and complete 2024 corpus gates. It preserves the logical occurrence, unique-key,
exception, and inter-release-change boundary while removing per-row occurrence
and bootstrap-change JSON.

The complete publication used 10,276,745,806 physical bytes, including the
immutable manifest and active pointer. The rejected legacy occurrence,
natural-index, and bootstrap-change artifacts used 113,447,910,338 compressed
bytes. The compact representation reduced the footprint by 90.9414%.

## Exact input

- Cycle: `2024`
- Rows: 264,085,606
- Selected relation compressed bytes: 14,765,199,882
- Selected relation compressed SHA-256:
  `566c5509e5e31c0d8cbf5f74bb78a1f716411d8e5f858440ec166ba1b7da8909`
- Selected relation uncompressed bytes: 182,881,299,512
- Selected relation uncompressed SHA-256:
  `3ef0fdcdbf246981e5724e810c89d11b79b702369b7467633e783d3936f2b9a5`
- Compact occurrence-set ID:
  `0d4fb929d384d91fd766ab112383a8c2b4b333b2f05e4a60c230e64435cd11fb`
- Immutable manifest SHA-256:
  `d5474423021a51a5fd4c676e1b84ecc3ba9a2b3ee40520b5bd101029dbab3a19`

## Physical contract

The source artifact plus its one-based row range is the dense physical
occurrence membership. Source row ordinal and raw bytes derive occurrence ID,
raw digest, and record-version identity on demand. They are not repeated in a
JSON object for every row.

Valid unique `SUB_ID` values use 512 deterministic FNV-1a partitions. Each
partition is sorted numerically and stores fixed 48-byte records:

```text
uint64 big-endian SUB_ID
uint64 big-endian source row ordinal
32-byte semantic SHA-256
```

Invalid rows, invalid keys, and duplicate-key states remain sparse JSONL
evidence. A bootstrap manifest declares all current unique keys as added
without writing one delta record per key. Later publications materialize only
actual added, changed, absent, and invalid transitions.

## Bounded gate

The ten-million-row retained 2024 sample published in 39.708 seconds:

- 10,000,000 valid unique rows
- 480,000,000 uncompressed index bytes
- 381,005,759 compressed index bytes
- 251,837 rows/s
- zero row exceptions, key exceptions, or materialized bootstrap deltas
- about 91.1% less compressed storage than the equal-row share of the legacy
  three-artifact representation

This gate established the full-corpus capacity and throughput bound before the
production publication.

## Complete-corpus measurements

The complete container ran from `2026-08-31T13:21:04.328772782Z` through
`2026-08-31T13:38:07.990457602Z`:

- Wall time: 1,023.662 seconds (17m 3.66s)
- End-to-end throughput: 257,981 rows/s
- Exit code: 0
- OOM killed: false
- Partitions: 512
- Index records: 264,085,606
- Uncompressed fixed-width index: 12,676,109,088 bytes
- Compressed fixed-width index: 10,275,424,335 bytes
- Average compressed index bytes per key: 38.909
- Row exceptions: 0
- Key exceptions: 0
- Materialized deltas: 0
- Implicit bootstrap additions: 264,085,606

Content addressing collapsed the 512 references to the same empty
key-exception artifact into one physical file. The unique physical backing was:

| Backing | Bytes |
|---|---:|
| 512 fixed-width index shards | 10,275,424,335 |
| Unique empty exception and delta artifacts | 39 |
| Immutable manifest | 660,716 |
| Active cycle pointer | 660,716 |
| **Total** | **10,276,745,806** |

The compact total is 11.04 times smaller than the 113,447,910,338-byte legacy
representation.

## Integrity evidence

The publisher:

- replayed and hashed every compressed and uncompressed source byte;
- conserved all 264,085,606 source rows as valid unique keys;
- fully decompressed every fixed-width shard after publication;
- checked record width, sort order, partition membership, row ordinals,
  compressed and uncompressed byte counts, and both SHA-256 identities;
- verified sparse JSONL artifacts and all blocking manifest checks;
- atomically wrote the immutable manifest and active cycle pointer only after
  those checks passed; and
- removed its temporary staging records after success.

A same-input hardened replay rehashed every referenced artifact and returned
the original immutable manifest in 30.375 seconds. It exited 0 without an OOM.

Fixture tests also prove compact-to-legacy logical equivalence for `SUB_ID`,
row ordinal, and semantic digest; dirty input preserves invalid and duplicate
evidence; later snapshots emit only actual changes; corrupt index backing is
rejected; and the Parquet publisher consumes compact row membership directly.

## Real columnar-ancestry migration

After the compact gate, the rebuilt Parquet publisher consumed the real compact
manifest and adopted the existing verified 2024 shards under the new ancestry.
The migration exited 0 without an OOM in 39.952 seconds. It published columnar
fact set
`8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`
with the compact occurrence-set and manifest identities above.

All 265 shard storage keys, SHA-256 values, and byte counts exactly matched
predecessor fact set
`6750b7656e14da33ba6c6324c4d575579562f2f5d0fd1816ab26a20b90de780d`.
The adopted set still contains 264,085,606 facts and 16,759,429,988 shard
bytes. All blocking columnar checks passed. This proves the migration changed
lineage only and did not rewrite or duplicate the fact files.

## Disposition

Accept the compact manifest, fixed-width partitioned key index, sparse
exceptions, and actual inter-release delta layout for future Schedule A
occurrence publications. Retain the legacy 2024 artifacts until an explicit
cleanup decision; this gate does not authorize deleting them.

The remaining real-corpus boundary is compact calculation membership. Dagster
automation and ArangoDB projection remain paused until that boundary and
coordinated fact-bundle readiness pass.
