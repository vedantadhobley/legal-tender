# Compact processed Schedule A occurrence contract v1

This physical contract replaces the rejected per-row occurrence,
natural-index, and bootstrap-change JSON layout. It preserves the logical
`source-record occurrence`, natural-key state, and semantic-change boundaries.

- Every physical source row is an occurrence. Its ID derives from the exact
  source artifact, relation, cycle, and one-based row ordinal. The manifest
  therefore represents clean occurrence membership as one dense ordinal
  range instead of one JSON object per row.
- Each valid unique `SUB_ID` appears in exactly one hash partition as one
  48-byte fixed-width record. The index stores unsigned big-endian `SUB_ID`,
  unsigned big-endian row ordinal, and the 32-byte semantic SHA-256.
- Invalid rows and duplicate keys remain sparse, explicit exception records.
- A bootstrap manifest declares every unique index record `added`; its delta
  artifact contains only invalid key states. Later manifests materialize only
  actual added, changed, absent, or invalid transitions.
- The immutable zstd COPY relation remains the exact-byte authority. Raw row
  hashes, occurrence IDs, and record-version IDs are derived on demand from
  the source locator. No evidence identity is discarded.

`manifest.schema.json` defines publication. `row-exception.schema.json`,
`key-exception.schema.json`, and `delta.schema.json` define the sparse JSONL
artifacts. [`index-format.md`](./index-format.md) defines the binary key index.

The contract contains no normalized facts, calculation decisions, entities,
or graph edges.
