# Candidate committee upstream evidence, v1

This Go calculation traces the complete selected receiver-reported committee
cohort into one candidate's accepted same-cycle authorized committee scope.
It is an attribution-readiness result, **not terminal-dollar attribution**.

- [Design and acceptance boundary](../../../../../docs/design/candidate-upstream.md).
- [Result schema](./result.schema.json). Its referenced schemas resolve through
  the local contracts registry; no remote schema is needed.
- [Executable synthetic cases](../../../../../internal/calculation/fec/candidateupstream/analyze_test.go).

The policy is `fec/candidate-committee-upstream-evidence@1.0.0`.
Candidate scope reuses the accepted A/P, conflicting-designation, and shared-
authorization rules. Receiver selection reuses the accepted Schedule A cohort.
No candidate-specific mapping, new financial classification, matching rule,
depth limit, proportional denominator, or terminal classifier is introduced.

Every root observation appears once in external, internal, or unresolved-scope
accounting. Every upstream observation retains its original ordinal and
reported fields. The input pins the complete source fact set for row lookup.
SCCs and shortest-hop witnesses describe disclosed connectivity. Their amounts
are not summed along paths or used to allocate pooled funds.

`terminal_allocated_minor_units = "0"` means this method allocates no terminal
dollars. It does **not** mean that actual terminal donor funding is zero.
The entire external-cohort measure is `unresolved_attribution`. Unknown
authorization amounts remain outside that denominator in their own bucket.
