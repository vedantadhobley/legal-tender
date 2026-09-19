# Processed Schedule A columnar fact set v1

This contract is the production-shaped physical representation of the logical
`fec.schedule_a_receipt.v1` fact. It replaces the rejected full-row JSONL
artifact without changing fact grain or calculation policy.

Each Parquet row represents one unique, source-valid Schedule A occurrence.
It retains all 81 decoded publisher values. SQL null, empty text, numeric and
timestamp lexemes, Boolean values, negative money, and extra decimal scale
remain distinct. The file also carries a narrow typed projection for dates,
local timestamps, signed minor units, source decimal scale, election/report
years, transaction period, memo-subtotal state, and normalization state.

The source artifact, relation, cycle, one-based row ordinal, raw byte offset,
and raw byte length resolve the fact to exact immutable COPY bytes. Per-row
occurrence, record-version, and fact hashes are deterministic views of that
lineage and are not repeated as unique 64-byte columns.

Files use deterministic source-row ranges, zstd compression, bounded row
groups, content-addressed names, and complete readback before checkpointing.
An interrupted run replays the source but does not rewrite verified completed
shards. No active pointer moves until complete source identity, row, fact,
normalization-state, shard-range, and semantic conservation checks pass.

The shard semantic digest covers the exact source locator and all 81 decoded
source values. Full readback also requires the exact 99-column physical schema
and decodes every physical value. Typed projection values are deterministic,
rebuildable views tested against the normalization contract; they are not
included in the lossless source-semantic digest.

The physical schema is in [`physical-schema.json`](./physical-schema.json).
The published set envelope is governed by
[`manifest.schema.json`](./manifest.schema.json).

This contract applies no receipt-counting, memo exclusion, amendment
selection, entity resolution, candidate routing, aggregation, or graph policy.
