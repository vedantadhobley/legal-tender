# FEC release contract v1

This contract implements the coordinated FEC discovery, planning, acquisition,
selected-data staging, and source-release publication boundary.

- [`inventory.json`](./inventory.json) is the exact required 21-source set.
- [`inventory.schema.json`](./inventory.schema.json) validates that inventory.
- [`discovery.schema.json`](./discovery.schema.json) validates metadata-only
  `HEAD` observations.
- [`release-plan.schema.json`](./release-plan.schema.json) validates the pure
  planner result.
- [`acquisition-result.schema.json`](./acquisition-result.schema.json)
  validates storage preflight, exact acquired or reused artifacts, the
  post-capture version observation, and blocking issues.
- [`staged-release.schema.json`](./staged-release.schema.json) validates the 20
  selected ZIP members, four Schedule A relation extracts, storage recheck,
  zstd round trips, and blocking release checks. Schedule A extraction
  preserves every physical COPY data row; field-level issues belong to the
  downstream occurrence contract. `contracted_field_count` names the selected
  relation schema width; it is not a claim that every physical row has that
  width.
- [`release-manifest.schema.json`](./release-manifest.schema.json) validates the
  exact plan/acquisition/stage evidence chain, 21 source artifacts, 24 selected
  outputs, blocking checks, and published planner baseline.
- [`fixtures/`](./fixtures/) contains complete planner scenarios plus a
  canonical storage-blocked acquisition result.

`update_available`, `no_change`, and `source_not_ready` are normal planner
results. `invalid` means the supplied observations or prior manifest violate
the contract. No result authorizes a body request except
`update_available`.
