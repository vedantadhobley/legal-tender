# Candidate evidence result v1

The [integrated view](../../../../../docs/design/candidate-evidence-view.md)
joins existing evidence without accepting a terminal or allocation policy.
The result schema reuses the unchanged upstream, inventory, and summary-review
contracts. No remote schema retrieval is needed.

The SHA-256 of compact Go JSON with `result_id` empty is the result identity.
It includes `executable_sha256`, selected parameters, and all input/component
identities. Runtime paths and timestamps are excluded. Monetary values remain
decimal strings in cents; no new candidate-total field is introduced.

`committee_trace.not_covered` describes that nested trace's committee-only
scope, not the integrated receipt inventory. Its historical allocated zero is
not a terminal-donor total. The top-level terminal amount and both policies are
null. Summary comparison blockers never suppress the underlying observations.
