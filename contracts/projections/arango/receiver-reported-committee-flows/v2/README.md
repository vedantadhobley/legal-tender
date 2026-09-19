# ArangoDB receiver-reported committee flows v2

Version 2 keeps the v1 money edges and topology, then replaces the generic
`missing_master_fact` placeholder with explicit identity states:

- `current_cycle_master`
- `historical_registration`
- `alternate_release_registration`
- `unresolved_reported_id`

Historical and alternate-release assertions remain nested source evidence.
They do not populate the vertex's current canonical name or classification.
Only `current_cycle_master` is terminal-identity-eligible. This eligibility is
a prerequisite for later terminal-source classification, not a terminal-source
classification by itself.

The projector writes a new content-addressed `lt_flow_probe_v2_*` database and
does not alter the v1 database.
