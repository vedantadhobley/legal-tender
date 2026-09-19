# Committee-flow evidence projection result v1

[Result schema](./result.schema.json) for the Go
`probe-arango-committee-flow-evidence` command. It describes the isolated
observation graph, not resolved economic payments.

The result pins projection and bundle identities, separate A/B signed measures,
component membership counts, unresolved same-cycle masters, source drilldown,
bounded query outcomes, storage measurements, and the complete required check
list. Exact minor units are decimal integer strings. A `partial` result is a
verified graph with explicitly incomplete master coverage.

Shared measures reference the pinned
[reconciliation result](../../../../calculations/fec/committee-flow-reconciliation/v1/result.schema.json).
The Dagster adapter resolves schema IDs locally and never fetches them.
Fixture and failure coverage lives in
[the orchestration tests](../../../../../tests/test_flow_evidence_orchestration.py).
See the [graph design](../../../../../docs/design/arango-committee-flow-evidence.md)
and [orchestration boundary](../../../../../docs/design/committee-flow-orchestration.md).
