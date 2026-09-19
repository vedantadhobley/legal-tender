# Complete-cycle report-line profile v2

The [design](../../../../../docs/design/receipt-report-profile-v2.md) owns this
manual diagnostic. V1 remains available; v2 is selected explicitly with
`--profile-version 2` on `profile-receipt-report-scope`.

Every selected-cycle physical occurrence appears in both a form-line group and
a reported-file/line group. Memo code, individual flag, individual-predicate
decision, and reviewed line disposition are independent axes. Nulls/blanks,
unknown amounts, signs, unsupported scope, and out-of-cycle dates remain evidence.
These tables and their two overlapping diagnostic subsets must not be added.

The [schema](./result.schema.json) binds the exact summary calculation and source
release, selected relation, complete physical verification, and disjoint group
conservation. Go enforces cross-group arithmetic and deterministic ordering; JSON
Schema alone cannot establish those invariants. First/last ordinals are extrema,
not exhaustive member lists. No publisher-key or transaction-ID uniqueness claim
is made. Original-filing completeness and report coverage remain unverified.

The ID hashes compact Go JSON with `profile_id` empty. Both readiness guards
remain false, and no financial comparison field is permitted. This is an audit
format, not accepted recurring storage or a Dagster publication contract.
