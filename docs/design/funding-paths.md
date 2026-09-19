# Generation-bound typed paths

Status: implemented in Go. Full regression, targeted race/static checks and the
[retained 2024 path gate](../audit/funding-paths-2026-09-13.md) pass, including
byte-identical fresh-process replay. This extends the
[neighborhood reader](./funding-neighborhoods.md); it adds no graph import,
HTTP service, identity resolution or terminal-dollar calculation.

## Route contract

`inspect-funding-paths` consumes the same exact generation file/checksum,
retained receipt locators and connection flags as the neighborhood reader.
It revalidates the generation before querying. Cycle and source identities come
from that generation; no current pointer, cycle override or replacement bundle
is accepted. Producer and consumer executable identities remain separate.

A route consists of these independently labeled parts:

1. Optional entry from one exact Schedule A occurrence, through its reported
   receipt or qualified conduit association, to a committee.
2. Zero or more directed committee observations from exactly one selected ledger:
   receiver-reported Schedule A or sender-reported Schedule B.
3. Optional ending at a candidate through an `authorized` linkage, independent
   support group or independent opposition group.

Alternatively, start or end directly at a committee. Candidate targets require
one explicit ending family. There is no arbitrary mixed-edge graph traversal:
reconciliation components cannot be route steps, receiver and sender observations
cannot mix inside the committee chain, and support cannot silently become opposition. Non-authorized
linkage context remains inspectable through neighborhoods but cannot end an
authorized-candidate route.

These are evidence paths, not claims that the same dollars traversed every
step. Memo, negative, zero and unresolved source states remain unchanged.
The path search adds no temporal-order, effective-payment or amount-allocation
rule. There is no path-money sum.

## Query controls

| Flag | Contract |
|---|---|
| `--from-committee` | Exact FEC committee ID; excludes a receipt start |
| `--receipt-ordinal`, `--entry-family` | Exact pinned Schedule A row plus `reported_receipt`, `conduit_association`, or extension-only `shared_conduit_association` |
| `--ledger` | Required `schedule_a` or `schedule_b` selected observation ledger |
| `--target` | Exact committee or candidate ID |
| `--ending-family` | Required only for candidate targets: `candidate_authorization_context`, `independent_support` or `independent_opposition` |
| `--max-committee-hops` | Zero through eight, default four; excludes entry and ending |
| `--max-paths` | One through ten, default three |
| `--max-expansions` | One through 100,000 examined topology links, default 10,000 |

The search enumerates directed simple committee paths in deterministic depth-first
order. It is not a shortest-path query and does not enumerate indefinitely
repeated cyclic walks. Parallel source observations remain separate links and
separate paths. A same-committee start/target without a receipt entry is rejected:
it would otherwise manufacture an empty identity path from an unverified ID.
Cycle queries require a separate mode. A receipt may end at its reported
committee with zero committee hops because the entry itself supplies evidence.

The search compares against one lookahead path before declaring the path limit
truncated. `more_paths_within_hop_bound` is `yes`, `no`, or `unknown` when the
expansion budget prevents deciding. `complete_within_hop_bound` never means all
paths in an unbounded or fully covered financial graph.

Results count encountered cycle-closing edges, hop-bound frontiers and stops
without selected outgoing links. These are bounded search counters, not distinct
global cycles or terminal-source classifications. A receipt without the requested
entry returns `start_relationship_not_available` and its source evidence, not an
invented endpoint. Backend/source failures produce errors, not a partial success
artifact. There is no continuation token for path results in this version;
rerun with explicitly changed bounds when needed.

## Implementation and evidence

The generation opener already verifies every selected A/B and resolved E graph
document against its exact source-derived model. The path reader visits that
verified in-process topology and builds a lightweight adjacency index for its
chosen ledger. It does not rescan Arango for each expansion or load all receipt
occurrences into memory. This is not a new persistent graph or a new AQL serving
index. Generation opening, index construction and source readback are outside the
search expansion budget and remain operational costs to measure separately.

Every returned unique link is rechecked against the stored graph and exact
backing evidence. Receipt/A/B steps return full occurrence drilldown. Authorization
returns exact linkage membership. Outside steps identify their exact resolved
calculation group, not an enumerated list of all Schedule E source members.
The result deduplicates evidence across its paths without collapsing source edges.
Every FEC vertex retains receipt/A/B/E facets, including absent and missing-master
states. A contributor appearance remains a source occurrence, not a resolved person.

All selected completion boundaries are checked before return. The immutable-publication
contract remains required; no cross-database transaction protects against a
privileged concurrent writer preserving completion metadata. Result identity
includes the generation, consumer build, query bounds, search state and evidence.

`validate-funding-paths` chooses multi-hop witnesses from verified topology in Go.
It exercises both ledgers and all three candidate ending families, plus receipt
and conduit entries when their selection populations exist. It also requires
explicit hop-bound and expansion-budget states on selected real topology.
No operator-supplied politician list or source ordinal drives this gate.
The retained runner checks an expected gate ID and byte-identical fresh replay.
Cycle, participant and source identities come from verified inputs. The fixed
family names are versioned query/schema contracts, not entity-specific rules.

## Shared-conduit extension

The [extended-generation reader](./shared-conduit-generation.md#consumer-boundary)
adds `shared_conduit_association` as an explicit entry family. It requires exact
base and extension locators and reports the outer generation identity. The old
family does not acquire new links. Shared entries expose original/new decisions,
group evidence and both full source occurrences; their physical descriptor points
to the extension, not the base receipt database. The shared committee facet remains
separate from base facets. No chain, candidate-ending or money-selection rule changes.

The [shared-query gate](../audit/shared-conduit-queries-2026-09-14.md) verifies this
boundary separately from the original path gate. The
[date-window consumer](./funding-window-reader.md#shared-conduit-generation-inputs)
now binds the same extension while preserving original receipt dates.

## Terminal-source work remains a separate decision

The [terminal-source assessment](./terminal-source-assessment.md) now compares
provisional frontier, identity-aware frontier and root-SCC hypotheses against the
complete selected A/B topology. It has no path-query depth cutoff and does not
adopt a terminal rule. Its receipt-role and financial boundaries remain explicit.

Connected evidence will help us evaluate what a terminal source should mean.
It does not make an observed endpoint a genuine origin. Evaluate reported source
roles, identity evidence, missing upstream coverage, time scope and cyclic
components before adopting a terminal-classification rule. Keep competing rules
versioned and compare their consequences against the same pinned evidence.

Dollar allocation remains separate even after a source qualifies as terminal:
reachability alone does not establish how much of one receipt funded a later
outgoing observation. This reader intentionally leaves those choices open and
does not bake its query limits into a terminal definition.

Next core work remains in the [connected-graph plan](./connected-funding-graph.md)
and [active queue](../todo.md): unresolved identity/coverage, source-member
drilldown, other-cycle rollout and serving performance are not completed by a
bounded path gate.
