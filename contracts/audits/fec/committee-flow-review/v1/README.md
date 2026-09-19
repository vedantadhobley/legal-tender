# Committee-flow review v1

Read-only Go diagnostic over a saved reconciliation result. The
[result schema](./result.schema.json) preserves complete component/type and
signed date-difference profiles, targeted examples, and their full source rows.
See the [source review](../../../../../docs/audit/committee-flow-source-review-2026-09-08.md)
and [graph boundary](../../../../../docs/design/arango-committee-flow-evidence.md).

The command verifies all three saved evidence artifacts, recomputes every
candidate component, and checks the exact summary and separate ledger totals.
It reuses existing fact loaders to verify immutable release ancestry and all
backing shard hashes, then seeks directly to selected Parquet rows and reruns
the original membership predicate. Each saved observation must match its
source exactly. This is not another full source-row calculation scan.

Profiles cover every component. Source examples are deterministic and targeted,
not random: first component per state/cardinality/exact-signature stratum,
distinct type combinations for amount and role conflicts, largest membership
per state, and largest absolute one-to-one date gap. Ties use the first
component in calculation order. At most four rows per side are decoded for
each example; explicit flags expose truncation, while the full assertion
ordinal arrays remain available. More than 128 examples or 10,000 profile
shapes fails explicitly.

Full source rows preserve all 99 A or 98 B physical fields, including NULL and
empty text. Int64 physical values are decimal strings to avoid JSON precision
loss; int32 fields remain integers. Dates in observations are epoch-day values,
and profiles use signed A-date minus B-date. Shared exact signatures inside an
ambiguous component do not become accepted matches.

No membership changes, corrected values, economic assertions, graph writes,
publication pointer, or Dagster asset result from this command. The result
binds the saved reconciliation SHA-256 and calculation ID; execution time is
not part of its bytes. Run logs hold timing and explicit completion markers.
