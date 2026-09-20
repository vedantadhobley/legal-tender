# Review before terminal attribution

Status: accepted 2026-09-20 and implemented as separate 2024 interpretation
publications. Scope corrected
2026-09-15: the user asked which assumptions determine nodes, edges and counted
amounts. The assistant expanded that into recovery infrastructure. A full isolated
rebuild is **not a prerequisite for this review**. Do not resume recovery engineering
as the automatic next step or adopt terminal/allocation rules before this review.

The [first code-backed packet](../audit/pre-attribution-review-2026-09-14.md)
records the earlier implementation. The current edge map below also covers the
later shared-conduit extension. It describes code behavior, not a fresh source
audit, live graph census, or acceptance of the financial interpretations.

Verification on 2026-09-15: the nine targeted calculation/projection package suites
passed. The receipt-graph suite required permission for its temporary localhost
test server; the sandbox attempt could not bind a socket. No live database or
full source corpus was read for this review.

## What actually creates a connection

| Connection | Current implemented rule | What it does not establish |
|---|---|---|
| Reported contributor appearance → recipient committee | One appearance per source occurrence, keyed by fact set and ordinal. A syntactically valid reported recipient ID creates a receipt edge, including memo and unknown-amount records. Financial eligibility is false in this projection. [Projection](../../internal/projection/arango/receiptgraph/types.go), [tests](../../internal/projection/arango/receiptgraph/model_test.go). | A resolved person, a deduplicated donor, or an amount that should count in a total. |
| Committee → committee, Schedule A | Require valid normalization, recipient ID, agreeing raw/clean contributor committee IDs, non-memo status, known signed amount, and an included receipt-type code. One-sided/conflicting IDs and unsupported roles stay outside this selected ledger. [Policy](../../internal/calculation/fec/committeeflows/policy.go). | Every reported receipt is eligible; an excluded record has been deleted. |
| Committee → committee, Schedule B | Require the reviewed reporting scope/form/line and transaction-code combination, agreeing recipient committee IDs and known non-memo amount. Self-recipient and intermediary/earmark evidence are held out. A and B become separate source-occurrence edges. [Selection](../../internal/calculation/fec/flowreconciliation/policy.go), [projection](../../internal/projection/arango/flowevidence/model.go). | Two observations are two payments, or that their sum is a money-flow total. |
| Appearance → reported conduit | Exact same-report reference topology plus compatible original/memo roles and nonconflicting committee IDs. The original rule requires a safe one-to-one relationship. The additive rule requires a complete safe star: a shared memo with at least two peers, each linked only to that memo, all with compatible roles. Amount equality is not an eligibility condition. Both add zero money. [Pair policy](../../internal/calculation/fec/earmarkassociation/policy.go), [group policy](../../internal/calculation/fec/earmarkassociation/group.go), [group tests](../../internal/calculation/fec/earmarkassociation/group_test.go). | A separate receipt, equal amounts, resolved donor identity, or a traced dollar allocation. |
| Committee → candidate authorization context | Group CCL assertions by candidate/committee; A/P designations qualify unless conflicting/invalid evidence or authorization shared across candidates makes the relationship unresolved. Preserve unauthorized and unresolved relationship states too. The projection stores context edges; traversal must check their states. [Relationship rule](../../internal/calculation/fec/receipts/calculation.go), [projection](../../internal/projection/arango/receiptgraph/inputs.go). | Personal payment to the candidate or a known authorization date interval. |
| Spender → candidate, support or opposition | Effective Schedule E membership plus candidate-reference resolution. A unique normalized name/office context can replace a reported ID. An ID present in the master without corroborating context can remain unverified and still be projected. Aggregates preserve separate resolution counts/amounts and support/opposition. [Resolver](../../internal/calculation/fec/candidateresolution/resolve.go), [projection](../../internal/projection/arango/independentexpenditures/resolved_model.go), [precedence tests](../../internal/calculation/fec/candidateresolution/policy_fixture_test.go). | Candidate-controlled receipts or equally verified candidate assignments. |

A/B reconciliation adds **comparison components**, not another payment edge. For
the same directed committee pair, opposite-ledger observations become candidates
through role+amount, role+known date, or amount+known date. Connected components
retain competing matches. Role+amount has no maximum date gap. Even an exact
signature does not merge the two amounts. See the
[matcher](../../internal/calculation/fec/flowreconciliation/match.go).

Entity vertices use reported FEC IDs with explicit missing-master states, not
name-based corporate ownership. Employer/occupation text is currently a
[reported assertion view](./reported-identity-assertions.md), not person-to-employer
or corporation-to-candidate edges. Reachability and cyclic components do not
identify terminal donors or allocate dollars.

## Decisions to review first

1. Candidate resolution: retain the current ID replacement and unverified-ID
   routing rules, tighten them, or offer explicitly separate reported/resolved views?
2. Committee flow membership: is requiring agreement of both ID fields the right
   selection boundary, while preserving one-sided and conflicting evidence?
3. Conduits: are the pair/star rules sufficient for an association with zero new
   money even when amounts differ? Keep that question separate from allocation.
4. A/B comparison: is the broad candidate grouping useful as evidence, including
   distant same-amount records, without presenting it as deduplicated payments?

The user accepted all four boundaries on 2026-09-20. Strict counterparty agreement
and the zero-money conduit association retain their existing rules. Separate
candidate-interpretation and direct A/B pair publications implement the two
required replacement views without mutating v1 evidence or graphs. Fixed publisher
code mappings are explicit domain rules; they are not inherently brittle exceptions.
The reviewed predicates do not use named candidates/committees to select outcomes.
That is a scoped observation, not a repository-wide hardcoding audit.

## Focused 2024 validation — 2026-09-20

The [complete diagnostic](../audit/pre-attribution-interpretation-validation-2026-09-20.md)
now verifies the exact retained candidate-resolution and receiver-flow artifacts,
binds the complete Schedule B profile to the reconciliation inputs, and replays all
308,488 A/B components. It changes no rule or graph.

The evidence sharpens the four choices:

1. **Candidate resolution needs separate reported and inferred endpoints.** Of
   1,811 current replacements, 1,054 covering $266,304,052.67 conflict with a
   reported ID that exists in the pinned candidate master. A replacement cannot
   silently serve as a confirmed or source-reported default.
2. **Strict committee counterparty agreement is supported.** All 2,101 Schedule A
   and 97,850 Schedule B one-sided IDs are raw-only self references. Neither
   ledger contains a clean-only or conflicting raw/clean cohort. Raw fallback
   would manufacture self-flow edges.
3. **The conduit pair/star rule remains a zero-money association.** FEC guidance
   explicitly allows the supporting conduit date and displayed total to differ
   from an original contribution. Amount/date equality is not a valid gate.
4. **A/B comparison needs evidence tiers rather than unbounded components.** The
   replay finds 130,650 different-date one-to-one candidates. Most are proximate,
   but 32 exceed one year and the maximum is 1,099 days. Preserve every candidate,
   carry no money, and publish the gap before any default filter.

The user accepted these recommendations. The
[2024 publication gate](../audit/pre-attribution-interpretation-publications-2026-09-20.md)
now passes for the versioned candidate-interpretation and direct A/B pair contracts.
The existing immutable facts, decisions, reconciliation components, graphs and
current evidence pins remain unchanged.

## Candidate-ID resolution review — 2026-09-15

Scope: current Schedule E method
`legal-tender.fec.independent-expenditure-candidate-resolution-method.v2`, its
aggregate, and the resolved spending projection. No rule changed during this review.
This is candidate-reference resolution, not person/employer/corporation resolution.

The [resolver](../../internal/calculation/fec/candidateresolution/resolve.go) uses
normalized name tokens and office context within its pinned candidate-master input.
Names are uppercased, punctuation-separated and token-sorted; token multiplicity,
suffixes and accents remain significant. Office context adds state for Senate and
state/district for House; president uses name/office. It is exact equality of these
derived keys, not literal name equality or proof of real-world identity. Election
year is not part of this key; the calculation still pins its source cycle.

| Input condition, in evaluation order | Decision | Graph consequence |
|---|---|---|
| Context unusable, reported ID exists in the pinned master | `unverified` | Route to reported ID. |
| Context unusable, reported ID absent | `unresolved` | No candidate edge for this observation. |
| Reported ID matches normalized context | `confirmed` | Route to reported ID even if another ID shares the context. |
| Reported ID does not match context; exactly one other candidate does | `resolved` | Route to that other ID, even if the reported ID exists in the master. |
| Reported ID does not match context; multiple candidates do | `ambiguous` | No candidate edge for this observation. |
| No matching context; reported ID exists | `unverified` | Route to reported ID. |
| No matching context; reported ID absent | `unresolved` | No candidate edge for this observation. |

Concrete consequence: a row reports candidate A's ID but has candidate B's unique
normalized name/office context. The selected graph endpoint is B. The decision
still preserves A and its original fields; this is a derived reassignment, not a
rewrite of the source. Existing [precedence tests](../../internal/calculation/fec/candidateresolution/resolve_test.go)
exercise this case, normalized-context collisions and unchanged original assertions.

The [aggregate](../../internal/calculation/fec/candidateresolution/aggregate.go)
groups confirmed, resolved and unverified rows by spender, selected candidate and
stance. Its total includes all three categories. The
[graph edge](../../internal/projection/arango/independentexpenditures/resolved_model.go)
also preserves each category's amount and count. Ambiguous/unresolved amounts stay
in explicit exceptions. Therefore an edge's total is not a confirmed-only total.

Assessment: this is a general, deterministic resolution method with preserved
counterevidence, not a named-candidate patch. The substantive assumption is that
unique normalized name/office context may outweigh a conflicting reported ID;
the master itself does not prove which field is correct. `confirmed` also means
corroborated under this rule, not independently verified identity.

Accepted boundary: keep these distinctions and source assertions
explicit. The current inferred links can support investigation, but totals must
expose their confirmed/resolved/unverified composition or state an explicit selection.
Do not label the combined amount confirmed, treat reassignment as a source correction,
or add an LLM/fuzzy resolver to hide remaining uncertainty. The existing fields
already support the distinction; no new resolution framework is needed for this review.

## What the checkpoint must distinguish

ArangoDB is a derived investigative projection, not the raw database. The
immutable acquired files and occurrence records preserve source evidence;
normalized facts, selections, assertions and graph publications add successive
contracts. A graph backup alone cannot checkpoint all of those layers.

The inventory below routes the review. It is not yet an exhaustive code audit
or an endorsement of every existing rule.

| Boundary | Interpretation or selection to review | Existing contract |
|---|---|---|
| Source population | Processed FEC A/B/E dumps versus original filings; cycle selection and publisher processing are not a claim of complete real-world money. | [Source catalog](./source-catalog.md), [release strategy](./fec-release-strategy.md) |
| Calculation windows | Separate analysis over published inputs, not cross-cycle re-ingestion. Review inclusive known-date selection, unknown exclusion, one-version-per-cycle safeguards and historical facets. Schedule E explicitly selects expenditure or dissemination date at source-member grain while retaining the unchanged aggregate parent; authorization remains undated publication context. No chronology or allocation rule is implied. | [Cycle/window contract](./cycle-calculation-windows.md), [window readers](./funding-window-reader.md) |
| Physical evidence | COPY decoding, null representation, row boundaries, schema acceptance, duplicate occurrences and source locators. | [Source contracts](./source-contracts.md), [evidence model](./evidence-model.md) |
| Typed facts | Exact money/date parsing and code interpretation; raw fields remain. Classic facts select unique valid occurrences and exclude invalid/duplicate occurrences without deleting their evidence. | [Evidence model](./evidence-model.md), [classic normalizer](../../internal/source/fec/occurrence/classic_fact.go) |
| Financial membership | Memo, amendment, effective-record, receipt/disbursement family and candidate-authorization rules determine which facts count. | [Calculation contracts](./calculation-contracts.md), [flow requirements](./fec-flow-fact-requirements.md) |
| Reported participant roles | Contributor routing, entity conflicts, earmark/conduit/reference qualification and exact-ID master joins are decisions over source fields. | [Receipt source evidence](./receipt-source-evidence.md), [receipt-role profiles](./terminal-receipt-roles.md) |
| Cross-record/candidate links | Report references, A/B reconciliation, candidate-reference resolution and reference-content equivalence are not literal source-row copies. Review ambiguous and excluded outcomes too. | [Reference join](./receipt-reference-join.md), [reference equivalence](./reference-content-equivalence.md), [typed generation](./funding-evidence-generation.md) |
| Graph and queries | Edge direction, aggregation, identity placeholders, selected ledgers, SCCs and traversal bounds; reachability is not dollar origin. | [Connected graph](./connected-funding-graph.md), [typed paths](./funding-paths.md) |
| New reported text view | Exact field projection over all published A facts and all pinned CM facts; no trimming, name merging, employer filtering or corporate attribution. CM fact selection still inherits the classic fact boundary above. | [Reported identity assertions](./reported-identity-assertions.md) |
| Unchosen inference | Person/corporation resolution, employer-family membership, terminal definitions and allocation. Provisional frontier hypotheses are not accepted terminal rules. | [Terminal assessment](./terminal-source-assessment.md) |

## Review deliverables

For each non-literal transformation, record the versioned rule, code and tests,
source justification, complete input/output population, exclusions/ambiguities,
and the ability to replay or replace it. Distinguish publisher assertions from
our own rules. Flag heuristics, unsupported populations and rules with incomplete
real-data validation; do not summarize them all as “1:1.”

Use the [existing evidence pins](../audit/pre-attribution-checkpoint-2026-09-14.md)
and policy versions to identify the reviewed implementation. Record user acceptance
or requested changes in the decision log. Verification proves only its stated
scope; it does not prove that source assertions are true.

Full [fact-start recovery](./funding-recovery-checkpoint.md), runtime retention and
raw-to-fact recovery are separate deferred operational work. Existing read-only
inventory/verifier tools and reproducible builds remain useful. The missing exact
historical staging record remains an explicit evidence limitation; neither it nor
an unimplemented recovery executor prevents explaining the current graph rules.
Any physical backup, data copy or live rebuild requires its own scope and resource
plan. Do not represent the current evidence pins as a complete recovery baseline.

The checkpoint can cover the completed 2024 implementation without pretending
that it proves all-cycle behavior or prior-cycle origins. General cross-cycle
consumers require separate tests and coverage evidence. Completing all four A/B
cycles is not a blanket prerequisite for this review or for attribution design;
missing evidence still limits any calculation that depends on it.
