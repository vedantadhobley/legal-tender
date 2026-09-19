# Processed Schedule A occurrence contract v1

This contract is the immutable boundary between a published coordinated FEC
source release and normalized receipt facts.

- `occurrence.schema.json` defines one exact physical-row locator. It keeps
  row identity separate from publisher record identity.
- `issue.schema.json` defines a structured parse, partition, or duplicate-key
  problem without deleting the affected occurrence.
- `natural-index.schema.json` defines the sorted state of each publisher
  natural key in one snapshot. Duplicate keys remain an explicit state.
- `change.schema.json` defines a semantic transition between two occurrence
  sets. Raw serialization differences alone do not create semantic changes.
- `manifest.schema.json` defines the immutable, checked occurrence-set
  publication and all content-addressed artifacts it owns.

The occurrence set does not contain normalized facts, entities, graph edges,
receipt totals, or amendment-policy decisions.
