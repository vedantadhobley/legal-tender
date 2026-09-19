# FEC release contract v3

This contract extends the coordinated FEC release boundary with processed
Schedule B while preserving the replayable v1 and v2 contracts.

- [`inventory.json`](./inventory.json) is the exact required 23-source set.
- [`inventory.schema.json`](./inventory.schema.json) validates that inventory.
- [`discovery.schema.json`](./discovery.schema.json) validates metadata-only
  `HEAD` observations.
- [`release-plan.schema.json`](./release-plan.schema.json) validates the pure
  planner result.
- [`acquisition-result.schema.json`](./acquisition-result.schema.json)
  validates storage preflight, exact acquired or reused artifacts, the
  post-capture version observation, and blocking issues.
- [`staged-release.schema.json`](./staged-release.schema.json) validates the 20
  selected ZIP members, four cycle-scoped Schedule A relation extracts, one
  all-history Schedule E relation extract, storage recheck,
  zstd round trips, and blocking release checks. Schedule A extraction
  preserves every physical COPY data row; field-level issues belong to the
  downstream occurrence contract. `contracted_field_count` names the selected
  relation schema width; it is not a claim that every physical row has that
  width.
- [`release-manifest.schema.json`](./release-manifest.schema.json) validates the
  exact plan/acquisition/stage evidence chain, 23 source artifacts, 25 selected
  outputs, blocking checks, and published planner baseline.

Schedule B's four selected relations use `archive_direct`: acquisition validates
that the immutable PostgreSQL dump contains them, then each cycle publisher
streams its relation directly into Parquet. The release does not retain a
roughly 115 GiB intermediate COPY stream for 2024. The 22 v2 artifacts and all
25 staged outputs remain reusable when their publisher versions are unchanged;
only Schedule B is newly required by the inventory.
No result authorizes a body request except `update_available`.
