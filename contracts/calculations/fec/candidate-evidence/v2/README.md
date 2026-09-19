# Candidate evidence presentation v2

The [result schema](./result.schema.json) wraps the unchanged
[v1 evidence](../v1/README.md) with pinned source-name assertions and deterministic
shortest-hop witness examples. It does not introduce a new funding calculation.

Run `build-candidate-evidence --view-version v2`; optionally provide an exact
`--candidate-master-facts` manifest. Committee names must use the trace's exact
master source and per-node fact identities. Candidate names are same-cycle display
context only. No name is used for matching or authorization.

The report ID is SHA-256 of compact Go JSON with `report_id` empty. The nested v1
result includes the executable hash; timestamps and local paths do not enter the
identity. Retain all named inputs and executable/build evidence for reproduction.

Each example is one complete existing witness chain, selected by ascending hop
distance and committee ID, at most three distinct distances. Dates and signed
amounts stay on individual observations. `allocated_amount_minor_units` is always
null; no sum, minimum, residual, terminal definition, or pooled attribution is
selected. Known-date reversal and same-day flags compare consecutive known dates;
missing dates remain separately flagged.

See the [presentation design](../../../../../docs/design/candidate-evidence-view.md)
for readable-report behavior and scope limits.
