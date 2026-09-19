# Complete the connected funding-evidence graph

Status: accepted implementation priority, 2026-09-11. The milestones below are
not yet complete. The [full-cycle participant graph](./arango-receipt-participant-cycle.md)
passes 2024 import and independent full replay. Its read-only connection consumer
also passes the full index census and selected cross-graph witness/replay gate;
the typed A/B/E generation binding also passes. Its
[one-hop consumer](./funding-neighborhoods.md) passes and its
[typed multi-hop extension](./funding-paths.md) also passes its selected real
path and fresh-replay gate. Person/organization resolution remains.
Reported identity fields are now available through an exact
[source-backed view](./reported-identity-assertions.md); it does not add
employment/ownership edges. The [pre-attribution interpretation review](./pre-attribution-review.md)
is required before accepting terminal definitions or allocation.
These milestones extend the existing Go pipeline; they do not require
another rewrite or a database change. This document owns the completion plan;
the [active queue](../todo.md) tracks delivery.

Python is a reference for questions, counterexamples and prior learning, not a
guide to implementation or the target scope. The comparison below explains the
gap; restoring its outputs or collection shapes is not the acceptance criterion.
Every target behavior needs independent product and source justification.

## What is missing

The existing candidate report proves reproducible committee ancestry and source
drilldown for its selected population. It does **not** establish complete graph
coverage for the original product. Reported contributor/conduit publication and
its selected cross-graph witnesses now pass for 2024. Integrating all typed
relationship families, employers and organizations remains unfinished core work,
not optional UI enrichment. Complete that backend before building a GUI.

“Complete selected committee cohort” means every selected observation survived
that projection. It does not mean every Schedule A/B record is a graph edge,
every donor has a resolved identity, or all four cycles form one accepted
generation. Missing committee masters retain unresolved endpoints and edges;
that identity gap is different from an unimplemented relationship family.

## Comparison with Python

The Python implementation reached more of the end-to-end product. The Go
implementation has stronger verified evidence boundaries but has not yet
replaced that functional breadth. Neither statement establishes complete or
correct attribution.

| Capability | Python implementation | Current Go implementation |
|---|---|---|
| Contributor connections | Threshold-qualified donor vertices and donor-to-committee edges. | Complete 2024 participant/conduit publication and full graph readback pass, as do the full index census and selected receipt/committee/candidate witnesses with exact replay; this is not every candidate/path. |
| Employers and corporations | Donor-to-employer edges, employer mappings, corporate families and high-dollar-donor corporate links. | Reported employer fields retained; general person/organization resolution and these graph connections remain unimplemented. |
| Committee chains and candidates | Named graph connected transfers, candidate affiliations and support/opposition spending. | Selected A/B observation graph and candidate ancestry pass for 2024; resolved outside-spending graphs pass for four cycles, but these are not one integrated funding graph. |
| Terminal amounts | Committee classifications and depth-bounded proportional upstream attribution produced funding channels. | Selected-cohort reachability, cycles and unresolved attribution are explicit; terminal and allocation policies remain unselected. |
| Source evidence | Cycle-level aggregate edges omitted source-row membership and source-release identity. | Published facts, exact memberships, versioned calculations and source drilldown support replay and readback for the implemented slices. |

This comparison is supported by the legacy [named graph](../../src/assets/graph/political_money_graph.py),
[contribution projection](../../src/assets/graph/contributed_to.py),
[employer edges](../../src/assets/graph/employed_by.py),
[corporate enrichment](../../src/assets/enrichment/wikidata_resolution.py), and
[upstream calculation](../../src/assets/aggregation/candidate_upstream.py).
The [legacy grain excavation](./legacy-functional-spec/data-grain-and-lineage.md)
records the source loss and aggregation behavior in detail.

Code capability is not a claim that the retained Python database is a healthy
reference generation. The [2026-08-31 database audit](../audit/python-fec-arango-state-2026-08-31.md)
found stored candidate results and donor vertices, but an empty `contributed_to`
collection and mixed materialization dates. That is a dated observation, not a
new live database inspection. Preserve the legacy system as evidence; do not
accept its totals as a parity oracle or discard its useful domain lessons.

The current Go comparison is grounded in the [A/B graph gate](../audit/arango-committee-flow-evidence-2026-09-08.md),
[candidate ancestry gate](../audit/candidate-upstream-2026-09-08.md),
[four-cycle outside-spending gate](../audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md),
and [source drilldown gate](../audit/candidate-connection-2026-09-11.md).
Outside-spending exceptions remain outside candidate edges; a ready projection
does not mean every source decision resolved.

## Quality requirements to keep

- Preserve raw fields and exact source membership. A compact graph can reference
  shared fact sets; it need not copy the receipt corpus or collapse away lineage.
- Separate observations, identity assertions, effective financial records and
  allocation. An employee association is not a corporate payment. A conduit memo
  association does not create another receipt or another amount to count.
- Keep receiver and sender ledgers separate. A reconciliation candidate is not
  an additional payment edge. Candidate authorization is a relationship, not a
  transfer; outside support and opposition are not candidate-controlled receipts.
- Preserve signed, zero, unknown and memo states. Report unresolved or unsupported
  cases explicitly. Never convert missing adjacency into terminal status.
- Pin inputs, methods and build identity. Require conserved membership, exact
  source/graph readback and reproducible output before accepting a generation.

These are concrete improvements over the legacy projections. They do not prove
that every source interpretation or future identity decision is correct. A
holistic result needs both these guarantees and the missing product connections.

## Delivered slice: connected 2024 contributor evidence

Publish reported contributor connections into the existing committee ancestry
and candidate authorization view, with supported conduit associations and exact
source drilldown. This must work over the declared cycle population, not only
named candidates or hand-reviewed reports. It remains an observation graph,
not a terminal-dollar calculation.

The first implementation step is a cycle-wide contributor/reference publication
contract and a bounded storage/index benchmark over retained Schedule A facts.
The [participant contract and sort-run benchmark](./receipt-participant-publication.md)
now define that boundary. The complete-cycle reference join and its parallel
equivalence/source gate pass. The [participant index](./receipt-participant-index.md)
now implements source-grain appearances and dispositions, with complete 2024
readback, artifact replay and source inspection accepted and retained. Qualified
memo-conduit publication now has a [Go implementation](./receipt-conduit-publication.md)
with full 2024 membership/readback, independent corpus checks, same-build replay
and retention accepted. The bounded Arango importer now passes full live readback
and replay. Its [compact-layout gate](../audit/arango-receipt-participants-compact-2026-09-12.md)
preserves source grain and every edge/context field while reducing duplicated
appearance payload. The [full-cycle publisher](./arango-receipt-participant-cycle.md)
now implements storage guards, source-shard checkpoints and immutable completion.
Complete-corpus/replay acceptance passes.
The [read-only connection consumer](./receipt-candidate-connection.md) now implements
an exact receipt-to-committee-chain-to-authorization witness across existing
databases. Cross-cycle and readback fixtures pass. Its automatic real gate found
reference provenance differences despite matching selected CM/CCL member hashes.
The [explicit equivalence proof](./reference-content-equivalence.md) now passes
its real reference checks and the [complete 2024 connection gate](../audit/reference-content-equivalence-2026-09-13.md):
full index census, automatically selected source-backed witnesses and byte-identical
fresh replay. This accepts the receipt-observation slice and its selected chain
connections, not all-candidate path coverage, population-wide A/B/E integration
or terminal amounts.
Reuse the [receipt inventory](./committee-funding-basis.md),
[source-role policy](./receipt-source-evidence.md), and
[same-report association rules](./receipt-report-association.md).
Do not repeat the bounded report reviewer for every report or raise its in-memory
limit to approximate a cycle-wide job.

The contract must specify source occurrence identity, report/reference scope,
relationship types, exact memberships and explicit unsupported states. Select
physical graph grain and indexing from measured costs; this plan does not
require copying every raw receipt into Arango or adding a new indexing service.
Retain reported contributor appearances even when their real-world identity is
unknown. Do not merge people solely by normalized name/employer or require
perfect person resolution before connecting source evidence. A later display or
enrichment threshold must not delete underlying contributor evidence.

Acceptance requires:

1. Every input occurrence accounted for by documented role, evidence-only state
   or exclusion; every published relationship expandable to its exact facts.
2. Duplicate, missing, conflicting and out-of-scope report references kept
   explicit. Unsupported earmark forms remain visible, not forced into the
   currently accepted association pattern.
3. Contributor-to-committee connections join candidate-authorized roots and
   upstream committee chains without adding money for supporting memo links.
4. Real-corpus membership/readback and replay gates, plus fixtures for ambiguity,
   signed adjustments, overlaps and missing masters. Sample path checks alone
   cannot establish population coverage.
5. Measured peak memory, retained/temporary storage, scan/import time and unchanged
   replay cost. Keep existing accepted publications intact while validating the
   additive projection.

## Remaining core milestones

These are capability milestones, not a requirement to finish every cycle before
the next analytical layer. Continue the 2024 evidence/identity groundwork first;
the [cycle/window contract](./cycle-calculation-windows.md) permits later consumers
to request explicit cross-cycle evidence without collapsing the source grain.
Use multi-cycle fixtures where needed and accept real multi-cycle coverage
separately. Keep the assumption inventory current for the pre-attribution review.

1. **Integrate the relationship families.** Add the contributor publication to
   committee observations, candidate authorization and outside-spending context
   under an exact generation boundary. Queries must choose typed paths rather
   than treating every edge as a money transfer. Carry historical committee
   identity evidence only through its accepted ancestry/refresh contract; missing
   identities remain visible. One logical generation does not require one
   physical collection or one undifferentiated named graph.
   The [typed generation verifier](./funding-evidence-generation.md) now implements
   the read-only binding and complete selected A/B/E field checks; real verification
   and byte-identical replay pass. The generation-bound neighborhood reader now
   implements typed one-hop queries and source drilldown; its
   [real gate](../audit/funding-neighborhoods-2026-09-13.md) records acceptance.
   The [typed path reader](./funding-paths.md) adds explicit one-ledger committee
   chains with source entry and candidate context. Its
   [real gate](../audit/funding-paths-2026-09-13.md) owns acceptance; serving
   performance and all-candidate coverage remain distinct gates. The additive
   shared-conduit family now works through those readers and the
   [date-window consumer](./funding-window-reader.md#shared-conduit-generation-inputs),
   with original receipt dates and exact outer-generation qualification. Its
   [2024 gate](../audit/shared-conduit-windows-2026-09-14.md) passes; this does not
   resolve identities or accept real multi-cycle capacity.
2. **Complete the four-cycle rollout.** Detailed A/B publications used by current
   views cover 2024; the other target cycles still need those fact/calculation/
   graph gates. Schedule E's four-cycle gate does not complete A/B. Reuse the
   same methods with cycle parameters, pin compatible ancestry, and expose
   partial active-cycle coverage. The latest-four-cycle view is a default query
   window, not a retention or transaction-date boundary. This rollout is required
   for default-window production coverage, not before completing 2024 groundwork
   or designing a calculation; a calculation's required evidence still gates its
   own full acceptance.
3. **Add person and organization resolution.** The source-grain
   [reported assertion view](./reported-identity-assertions.md) now passes its
   complete 2024 gate; publish evidence-backed identity and corporate associations
   separately, including ambiguity and time scope. Reconsider legacy resolution
   rules independently. This work can proceed alongside graph rollout and must
   not block unresolved source appearances. Corporate association never implies
   that the corporation supplied an individual's contribution.
4. **Verify coordinated operation.** Exercise changed and unchanged generations,
   exact dependency invalidation, safe retry and retention/storage limits before
   enabling weekly publication. Dagster remains a thin control plane over Go;
   existing wiring is not proof of an unattended four-cycle production run.

Terminal classification, the necessary financial bases and dollar allocation
remain separate decisions and implementation gates. Connectivity alone cannot
choose how pooled money is attributed. The
[terminal-source evaluation boundary](./funding-paths.md#terminal-source-work-remains-a-separate-decision)
records the approach: compare explicit definitions against connected evidence,
keeping missing coverage and cyclic components distinct from genuine origins.
The implemented [boundary assessment](./terminal-source-assessment.md) compares
full selected-ledger frontiers, same-cycle identity evidence and root SCCs without
a path-query cutoff. The [receipt-role profiles](./terminal-receipt-roles.md) now
join reported participant roles and exact source-ID master evidence to that
scope. Source-grain employer/organization assertions are available; separate
identity resolution remains unimplemented, and no topology hypothesis becomes
a terminal policy.
Financial-source investigations should
gate the specific dollar use they affect, not all observation-level graph work.
Lobbying and legislative-influence analysis remain the separate later domain
defined by the [product contract](./product-contract.md#product-phases).

## Reporting completion

Report coverage by source release, cycle and relationship family. Distinguish
retained facts, connected observations, resolved identities, effective financial
membership and allocated amounts. Include explicit exclusions and unresolved
states; do not reduce those different measures to one “graph complete” label.

The 2024 milestone closes a concrete connection gap, not the entire product.
Finish its corpus and resource gates before selecting the next implementation
step. A GUI is not the next milestone. No calendar estimate or completion
percentage is established by this plan.
