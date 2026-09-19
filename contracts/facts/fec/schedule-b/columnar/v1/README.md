# Processed Schedule B columnar fact set v1

This contract publishes one fact for every valid physical row in one selected
two-year Schedule B relation. It preserves all 81 decoded publisher lexemes,
adds exact source locators and policy-free typed projections, and applies no
amendment, memo, recipient, or money-flow counting rule.

- [`physical-schema.json`](./physical-schema.json) fixes the 98-column Parquet
  representation.
- [`manifest.schema.json`](./manifest.schema.json) fixes release ancestry,
  archive and COPY replay evidence, global `SUB_ID` uniqueness, deterministic
  shard ranges, and content-addressed output identity.

The publisher reads `archive_direct` relations from the coordinated release's
immutable PostgreSQL dump. It does not retain a second full COPY extract.
Fact-set identity binds the Schedule B artifact and relation, not unrelated
members of the coordinated release. A descendant release that reuses the same
Schedule B bytes digest-verifies and reuses the existing fact set without
streaming the relation again.
