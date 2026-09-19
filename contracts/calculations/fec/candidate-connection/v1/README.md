# Candidate connection source drilldown v1

The [result schema](./result.schema.json) binds one concrete observation from a
[v2 candidate report](../../candidate-evidence/v2/README.md) to its complete
immutable Schedule A source row. It is not a new financial calculation.

The CLI requires an expected parent report ID and source-row ordinal. It checks
both parent content hashes and refuses missing, conflicting or duplicate concrete
connections. Source lookup verifies exact calculation/input identities, selected
membership, every observation field and the full physical Parquet row. It preserves
all 99 fields and their source null/empty/type distinctions.

`parent_verification` deliberately says content identity, not full recalculation
or authenticated authorship. Names and candidate context remain parent assertions.
`source_verification` applies to the independently checked observation and row.
No terminal, amendment, memo, donor identity or cash policy is selected.

`connection_id` is SHA-256 of compact Go JSON with that field empty. It binds
the lookup executable, exact parent-document digest, parent content IDs, selected
source ancestry and complete output. Allocated amount is always null, and terminal
eligibility is always false. Filesystem paths and execution times are not hashed.

See the [design](../../../../../docs/design/candidate-evidence-view.md#source-row-drilldown)
for command usage and the distinction between startup verification and row seeking.
