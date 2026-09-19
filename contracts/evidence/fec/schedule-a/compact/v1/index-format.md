# Compact key-index binary format v1

Each manifest partition uses zstd framing over a sequence of fixed 48-byte
records. There is no per-file header; the immutable manifest supplies the
schema version, partition number, record count, byte count, digests, and
partition configuration.

| Offset | Bytes | Encoding | Meaning |
|---:|---:|---|---|
| 0 | 8 | unsigned big-endian integer | Exact decimal `SUB_ID` value. Leading-zero source forms are invalid. |
| 8 | 8 | unsigned big-endian integer | One-based row ordinal in the selected COPY relation. |
| 16 | 32 | raw bytes | SHA-256 under `legal-tender.fec.schedule-a-semantic-digest.v1`. |

Records are strictly increasing by `SUB_ID` within their partition. Partition
membership is `FNV-1a-32(decimal SUB_ID bytes) mod partition_count`. Only
valid unique keys enter this index. Invalid and duplicate key states live in
the matching sparse key-exception artifact.
