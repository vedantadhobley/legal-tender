# Committee-summary occurrence/fact publication v1

The Go publisher consumes the exact CSV selected by a published coordinated
[v4 release](../../../../releases/fec/v4/). It preserves every source record;
it does not produce a committee aggregate or terminal-dollar attribution.

- [Fact schema](./fact.schema.json) — occurrence and unkeyed record-version
  identities, exact raw byte locator, all 92 raw fields, typed values, and issues.
- [Manifest schema](./manifest.schema.json) — exact release/source ancestry,
  deterministic verification report, immutable artifact identities, and readback.
- [Source contract](../../../../sources/fec/committee-summary/v1/) — reviewed
  accepted CSV framing and normalization for lossless preservation, not financial grouping.

Physical rows use the existing zstd JSONL artifact contract. This small summary
family does not change the Parquet choice for large detailed schedules. Before
manifest publication and on replay, Go compares every stored record against a
fresh raw read and verifies both compressed and uncompressed hashes at EOF.

IDs use SHA-256 over eight-byte big-endian length-prefixed UTF-8 components:

- Occurrence: `fec.committee-summary.occurrence.v1`, snapshot hash, `whole_csv`,
  cycle, decimal one-based ordinal.
- Record version: `fec.committee-summary.record-version.v1`, cycle,
  `unkeyed:<occurrence-id>`, raw-record SHA-256.
- Fact: `fec.committee_summary.v1`, record-version ID, parser version.
- Fact set: `legal-tender.fec.committee-summary-fact-set.v1`, exact release
  manifest SHA-256, source SHA-256, cycle, fact type, source contract, parser version.

Different releases receive different manifests even when identical source bytes
reuse the row artifact. Atomic create-if-absent publication preserves the winning
run/time on concurrent writes and replay. There is no mutable summary pointer.
The [design](../../../../../docs/design/committee-summary-source.md) owns commands,
storage limits, acceptance gates, and remaining consumer boundaries.
