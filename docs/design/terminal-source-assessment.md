# Terminal-source boundary assessment

Status: implemented in Go. Full regression, targeted race and static checks pass.
The [retained 2024 assessment](../audit/terminal-source-assessment-2026-09-13.md)
and byte-identical fresh-process replay pass over both complete selected ledgers.

This is the first comparison of proposed terminal boundaries, not an accepted
terminal classifier or dollar-allocation method. It extends the
[typed generation](./funding-evidence-generation.md) and
[path reader](./funding-paths.md) without changing stored graphs.
The [product definition](./product-contract.md#terminal-source) remains authoritative:
terminal status requires a versioned domain policy, not merely missing adjacency.

## What is assessed

`assess-terminal-sources` loads the exact generation and verifies its backing.
It then assesses every endpoint and observation in each selected committee ledger
independently: receiver-reported Schedule A and sender-reported Schedule B.
It does not add the two ledgers or treat their observations as reconciled payments.

Unlike a bounded path query or candidate-scoped trace, this calculation has no
committee-hop cutoff, path limit or candidate filter. It computes full incoming,
outgoing and self-loop observation counts and strongly connected components (SCCs)
for the declared selected populations. Parallel observations remain separate;
the assessment adds no sign, amount, date or role filter. The generation's
existing selection predicates and source identities stay attached to the result.

The population is the union of endpoints in those two committee ledgers, **not**
every registered committee, receipt appearance, candidate, or possible source of
funds. An endpoint absent from one ledger has `not_in_selected_ledger` state and
null hypothesis matches there. It is not counted as a zero-inbound frontier.

## Provisional hypotheses

| Hypothesis | Matches within the selected ledger |
|---|---|
| `selected_inbound_frontier@1` | Present endpoint with no incoming observation |
| `same_cycle_master_frontier@1` | That frontier plus an exact same-cycle master fact |
| `selected_condensation_root@1` | Member of an SCC with no incoming observation from outside the component |

These are topology predicates under assessment policy
`fec/selected-committee-boundary-assessment@1.0.0`. None is an adopted terminal
policy. Matching a predicate never sets `terminal_attribution_eligible`.
The result retains all per-node outcomes, component membership/counts, matched,
not-matched and inapplicable totals, and a complete A/B frontier-state comparison.
These are observation and topology counts, not attributed amounts or donor counts.

A self-loop is a cyclic singleton. A root SCC can have many internal incoming
observations while having none from outside. Every cyclic member retains the
`cyclic_component_not_individual_origin` blocker. This prevents a closed cyclic
group from silently becoming a set of terminal donors.

Master presence means that a same-cycle master fact is bound to that identifier.
It does not establish current activity, a financial origin, a resolved person,
or a corporate association. A missing same-cycle master remains explicit; this
assessment does not test historical registration evidence or label that ID invalid.

The two ledgers' frontier states can differ. The comparison records that
difference without selecting a more authoritative reporter or treating it as an
error. Their existing source populations and selection contracts remain separate.

## Evidence and reproducibility

The pure calculation is in `internal/calculation/fec/terminalassessment`.
It uses the extracted iterative SCC engine also used by candidate-upstream
tracing. Traversal is linear in nodes plus observations, followed by canonical
sorting. It does not enumerate graph walks or introduce a new persistent index.
Incoming/outgoing and internal/external component counts must conserve every
selected observation. Unknown identity states, unbound endpoints, duplicate
observation keys within a ledger and extra non-endpoint identities fail closed.

The generation adapter checks these counts against the exact generation family
memberships. It selects reproducible witnesses from the data for identified and
missing-master frontiers, root and non-root cycles, and A/B frontier disagreement.
For each available case, it rechecks the identity document and one incident
observation against the complete backing source occurrence. An unavailable case
stays `no_matching_selection_population`; no curated committee list is used.
Witnesses illustrate the result. Full selected-model verification and complete
topology traversal, not one example edge, establish the absence of an incoming
selected observation or the membership of an SCC.

The command uses the [shared generation read flags](./funding-neighborhoods.md)
and optional `--expected-assessment-id`. Cycle and all domain input identities
come from the generation; there is no cycle override or manually selected scope.
The output binds the generation file checksum, generation ID, actual consumer
executable checksum, hypotheses, counts, components and source witnesses.
SCC IDs identify member sets within a ledger/policy; the enclosing assessment
binds the generation and observation evidence. An SCC ID alone is not a publication.

All three graph completion boundaries are rechecked before return. The immutable
publication contract is still required; this is not a cross-database transaction.
Replay opens and verifies the evidence again and must reproduce the expected
assessment ID and every output byte. No elapsed time or physical output path is
part of the semantic result. The retained runner records timings separately.

Tests cover independent all-pairs reachability versus SCC/root results, parallel
edges, self-loops, disconnected components, missing identity, absent-ledger
states, a 30,000-node chain, ordering, cancellation, invalid input and stable
witness selection. Existing candidate-upstream tests protect the shared-engine
extraction. The live gate verifies the pinned real generation and exact replay.

## What remains

The result lists unassessed generation families separately. It does not assess
the source roles of all receipt appearances or merge appearances into people.
No complete financial denominator, opening balance, chronological fund
availability, cross-cycle origin or terminal policy is established by this
calculation. All financial origins remain `not_established`; it emits no amount
allocation. It does not change graph records, Dagster wiring or weekly schedules.

The separate [receipt-role profiler](./terminal-receipt-roles.md) now joins reported
participant roles and exact source-ID master evidence to these boundaries. This
v1 topology assessment remains unchanged and does not gain financial eligibility.
The [reported identity view](./reported-identity-assertions.md) now exposes
source-grain employer/occupation and committee-organization fields separately.
The [person-affiliation tests](./person-affiliation-testing.md) now exercise a
separate synthetic role-screening evaluator. Live person/organization resolution
remains unimplemented, and affiliations do not become monetary edges. Complete the
[user-requested interpretation review](./pre-attribution-review.md) and pin the
evidence baseline before terminal definitions or allocation. Keep appearances
and resolved identities distinct when
refining terminal-policy candidates; no old name list, arbitrary threshold or
graph-depth stopping rule receives a presumption of validity. Financial allocation
remains a subsequent contract.
