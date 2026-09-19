# Committee-flow reconciliation v1

Accepted Go evidence calculation, with manual and immutable publication commands. See the
[design and limitations](../../../../../docs/design/committee-flow-reconciliation.md)
and [calculation metadata](./contract.json).

The [publisher](../../../../../docs/design/committee-flow-publication.md) uses
the unchanged result as its immutable manifest. It adds a verified current
pointer and no-source-row-scan reuse, not a new matching policy or result type.

- [Result](./result.schema.json) binds exact source facts and policies to
  disjoint decision totals and content-addressed evidence.
- [Observation](./observation.schema.json) identifies a selected physical
  source occurrence, reported endpoints, role, date, and exact signed amount.
- [Assertion](./assertion.schema.json) retains candidate-component membership
  and separate amounts for both ledgers. It never merges the reports.
- [Fixtures](./fixtures/components.json) test exact corroboration, competing
  alternatives, in-kind conflict, and signed-amount conflict in Go.

Only the enclosing result supplies an observation's exact fact-set identity.
Dates are nullable days since Unix epoch. Unmatched means no candidate within
these selected cohorts, not no report. All results remain economic-flow
graph-ineligible; the separate observation-only consumer requires an exact
readiness bundle.
