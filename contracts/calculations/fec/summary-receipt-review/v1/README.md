# Summary/receipt readiness contract

[Policy](./policy.json) and [result schema](./result.schema.json) for the
[manual Go review](../../../../../docs/design/summary-receipt-compatibility.md).

This contract preserves reported observations and describes comparison blockers.
It is not a reconciled financial result. Comparison deltas are null; comparison,
funding-basis, and terminal-allocation eligibility remain false.

The schema enforces wire shape and guards. Go input verification and conservation
tests establish backing integrity and cohort membership; schema validation alone
does not prove source or financial correctness.
