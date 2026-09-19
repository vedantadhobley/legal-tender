# Receiver-reported committee flows v1

This accepted calculation defines a conservative Schedule A boundary for
committee-to-committee flow. Both contributor ID fields must contain the same
exact FEC committee ID, and the exact receipt type must describe an inbound
committee role.

The result is receiver-reported evidence. Schedule B remains reconciliation
evidence and never contributes another amount. Earmarked records remain source
attribution evidence. In-kind receipts remain a separate result role.

`manifest.schema.json` defines the immutable calculation set and compact
predicate. Ordinary included and excluded membership is reconstructed from the
exact fact set and predicate. `exception.schema.json` materializes only
unresolved rows, while `result.schema.json` groups included rows by source,
recipient, role, and cycle.

The [2024 cohort audit](../../../../../docs/audit/receiver-reported-committee-flow-cohort-2026-08-31.md)
records the corpus evidence used to accept the boundary. The
[publication audit](../../../../../docs/audit/receiver-reported-committee-flow-publication-2026-09-01.md)
records the complete immutable publication, exact conservation, minimal
Dagster asset, and replay gate.
