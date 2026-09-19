# Pre-attribution interpretation review — 2026-09-14

Historical packet. The [current review](../design/pre-attribution-review.md) also
covers the later shared-conduit group rule and supersedes this packet's framing
of recovery work as a prerequisite. Findings below describe the inspected version;
they are not a new verification of the current corpus or user acceptance.

Status: first code-backed review packet prepared. The user has not accepted
this checkpoint or any new interpretation. No financial rule, source fact,
graph, current pointer or schedule changed during this review.

Scope: the connected 2024 receipt/A/B/E generation and its source-grain window
reader. This is not an audit of every earlier prototype, an original-filing
verification, or a claim of complete economic money-flow coverage. The
[review requirement](../design/pre-attribution-review.md) remains open until
the user review and reproducibility work below are complete.

## What is literal, and what is not

The A/B/E fact layers preserve publisher fields and occurrence identity.
Parsing adds typed representations. Calculations add selections and assertions.
Arango projects those selected relationships. A graph edge is therefore not
automatically a literal source claim, a unique payment or a resolved donor.

The code inspected for this review contains general field/code predicates,
exact-key joins and explicit unresolved states. The inspected rules do not
branch on real candidate names, committee IDs, employers or particular amounts.
This is a scoped finding about the linked code, not a repository-wide proof.
Versioned source-code tables are still substantive interpretation choices.

### R1 — Source and normalized facts

The detailed sources are processed Schedule A/B/E snapshots. The selected
release is not a reconstruction of every submitted amendment. Existing
effective calculations rely on that publisher-processed population; they do
not independently choose a latest filing chain for every transaction.

Exact signed cents, parsed dates, memo flags and code fields are typed
interpretations alongside raw fields. Missing money stays unknown, not zero;
negative values stay signed. COPY decoding and schema acceptance are part of
the evidence boundary, not political or donor classification.

Classic CN/CM/CCL normalization differs from lossless detailed occurrence
publication: it selects unique valid publisher keys. Duplicate-key and invalid
source occurrences remain in evidence, but do not automatically become master
facts. An absent master therefore does not establish that an identity never
existed.

Code and tests: [classic selection](../../internal/source/fec/occurrence/classic_fact.go),
[occurrence tests](../../internal/source/fec/occurrence/), and the
[typed/effective contracts](../design/calculation-contracts.md).
The pinned A/B/E manifests declare respectively 264,085,606, 157,544,163 and
67,292 facts. These are three source populations, not counts of unique payments.
All three manifests declare zero invalid facts/source rows in these publications;
that is structural validation, not a finding that every reported value is true.

### R2 — Monetary membership and receipt routing

The narrow individual-receipt calculation uses the publisher individual flag,
memo exclusion and a known signed amount. That flag is not a resolved-person
identity. Candidate context comes from accepted authorization/linkage facts;
it does not turn a committee receipt into personal income for a candidate.

The A committee-flow predicate requires a valid receiving committee ID and
agreement between raw and publisher-cleaned source committee IDs. One-sided
or conflicting IDs remain unresolved. Exact transaction-code tables select
roles after memo/amount checks. Unknown or unsupported roles do not acquire
committee-flow membership by matching a name or amount.

B reporting first classifies reviewed form/line/schedule combinations. Sender
flow selection additionally requires agreeing raw/clean recipient IDs and
reviewed role/type combinations. Self recipients, intermediary evidence and
earmark-forwarding shapes remain held or unresolved. This excludes potentially
useful evidence from the selected flow graph without deleting its source facts.
The A selector does not include a loan-receipt cohort; selected B loans can
therefore be one-sided by construction.

In the combined receipt appearance view, accepted committee routing takes
precedence over publisher-individual overlap. Both underlying decisions survive.
The [participant gate](./receipt-participant-index-2026-09-12.md) preserves all
738 overlap rows, 36,584,727 memo rows, 1,356,364 negative rows, 7,011 zero rows
and two unknown amounts. Those overlapping populations must not be summed.

Code/tests: [A predicate](../../internal/calculation/fec/committeeflows/policy.go),
[B reporting](../../internal/calculation/fec/disbursements/policy.go),
[sender selection](../../internal/calculation/fec/flowreconciliation/policy.go),
[receipt routing](../../internal/calculation/fec/fundingbasis/evidence.go),
and [participant publication](../../internal/calculation/fec/receiptparticipants/).
Versions are recorded in those constants and the pinned manifests: A and B
reporting/flow predicates are v1; receipt routing is
`fec/reported-receipt-source-evidence@1.0.0`.

Source justification: the FEC's [transaction dictionary](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/)
distinguishes contribution, transfer, refund, loan, earmark and in-kind codes.
Our membership restrictions and conflict precedence are additional rules, not
instructions supplied by that dictionary. The [methodology page](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
documents the publisher's aggregate classifications. Both pages were reopened
for this review; no remote change was imported into a running parser.

### R3 — Report references and conduit associations

Reference policy `fec/same-report-reference@1.0.0` joins within the exact source
fact set, cycle, receiving committee and report file. Transaction labels are
not global identities. Duplicate source/target keys, self references, missing
targets and mismatched schedules have distinct states.

An exact reference alone is not a conduit. Association policy
`fec/same-report-earmark-memo-association@1.0.0` requires a reviewed non-memo
earmark, safe reciprocal one-peer topology, compatible original/related entity
roles, a related memo and matching committee-ID evidence without conflicting
fields. Amount equality is recorded but is not an eligibility condition.
Name-only or memo-substring matches do not manufacture IDs.

The [complete conduit gate](./receipt-conduit-publication-2026-09-12.md)
classifies all 33,262,189 applicable occurrences: 14,143,626 qualify and the
remaining dispositions stay explicit. The other 230,823,417 appearances are
outside this association rule, not discarded receipts. In particular, the
one-peer restriction leaves shared-reporting shapes unresolved; it does not
establish that they lack an intermediary.

Code/tests: [reference rule](../../internal/calculation/fec/reportreference/),
[association rule](../../internal/calculation/fec/earmarkassociation/),
[cycle consumer](../../internal/calculation/fec/receiptconduits/).
The FEC's [earmark reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-pac-reports/earmarked-contributions/)
distinguishes contributor and intermediary reporting. It does not prescribe
our one-peer algorithm. Added conduit money remains zero because the link
adds no second contribution, not because fees or other activity are known zero.

### R4 — A/B comparisons are candidate associations, not merged payments

`legal-tender.fec.committee-flow-candidate-components.v1` compares exact directed
committee pairs within a source-cycle calculation. Opposite-ledger candidates
share role/amount, role/known date, or amount/known date. Components preserve
all alternatives. An amount-only connection has no maximum date gap.

Thus repeated transactions can share a component. One-to-one exact agreement
also does not establish unique economic identity. Neither a component's sum nor
the difference between A and B totals defines a payment or an uncertainty range.

The retained generation contains 320,731 selected A observations, 341,720 B
observations and 308,488 comparison components. These memberships overlap the
receipt graph and are not additive. The [complete source review](./committee-flow-source-review-2026-09-08.md)
records split-report candidates, repeated-year ambiguity, amount/sign differences
and in-kind narratives under generic codes. None triggers a record-specific fix.

Code/tests: [selection and matcher](../../internal/calculation/fec/flowreconciliation/)
and [separate-ledger graph](../../internal/projection/arango/flowevidence/).
Changing matching rules should publish a new comparison, not rewrite A/B facts.

### R5 — Schedule E has a real identity-matching interpretation

Effective membership v1 excludes `memo_code=X` and unresolved amounts. Candidate
resolution then operates on the routeable effective population. Method
`legal-tender.fec.independent-expenditure-candidate-resolution-method.v2` is
not a literal ID join:

- Name comparison uppercases Unicode alphanumeric tokens, discards punctuation
  as separators, sorts tokens and retains token multiplicity. It does not remove
  suffixes, fold accents, infer nicknames or run fuzzy/LLM matching.
- Context uses office; Senate adds state and House adds state/district. House
  district formatting is normalized, including reviewed at-large spellings.
  Candidate election year is not an identity selector.
- A present reported ID with matching normalized context is `confirmed`, even
  if a second ID shares that normalized context.
- A unique alternative context can replace an ID that already exists in the
  master, producing `resolved`; the original reported ID survives separately.
- Without a matching context, a present reported ID can still route as
  `unverified`. Absent IDs without a unique match remain unresolved; multiple
  alternatives remain ambiguous.

The [complete resolution audit](./independent-expenditure-candidate-resolution-2026-08-31.md)
records 45,185 confirmed, 1,811 resolved, 10,996 unverified, zero ambiguous and
296 unresolved decisions. The 57,992 projectable decisions are not all confirmed
identities. The latest window gate preserves their state at source-member grain.
The broader 67,292-fact census also includes records outside that decision set.

Code/tests: [effective predicate](../../internal/calculation/fec/independentexpenditures/predicate.go),
[resolver](../../internal/calculation/fec/candidateresolution/resolve.go),
[characterization tests](../../internal/calculation/fec/candidateresolution/resolve_test.go),
[published-decision replay](../../internal/calculation/fec/candidateresolution/read_members.go).
The new tests freeze the precedence/collision boundaries for review, not as proof
that this method establishes real-world identity. This matching algorithm is
our inference over publisher assertions, not a publisher correction feed.

### R6 — Graph shape, time and text assertions

The [typed generation](../design/funding-evidence-generation.md) keeps source
appearances separate from people, exact committee/candidate IDs separate from
names, and receipt/A/B/support/opposition families separate from each other.
The A/B graph has 1,114 unresolved same-cycle masters; it does not inherit the
older receiver-graph historical identity classification automatically.

Window rules select each observation's own inclusive reported-date interval.
Unknown dates are excluded by bounded windows and remain visible in coverage;
an absent window includes the supplied unknown-date population. E requires an
explicit expenditure or dissemination basis. Authorization is undated context,
not proof of validity on every selected day. E member selection retains the
unchanged group parent; it does not allocate a group's total over dates.

Code/tests: [window reader](../../internal/projection/arango/fundinggeneration/window_reader.go),
[connection reader](../../internal/projection/arango/fundinggeneration/window_connections.go),
[spending reader](../../internal/projection/arango/fundinggeneration/window_spending.go),
and [full-chain synthetic tests](../../internal/integration/fundingwindow/).
Their policy/version constants distinguish committee windows, receipt/context
connections and source-grain E connections. The [retained window gate](./funding-window-spending-2026-09-14.md)
checks all E facts and 30 automatically selected query cases, not every possible path.

Cycle is still acquisition/publication identity; it is not a universal analysis
window. The current calculation contracts retain one accepted version per
source-cycle input. Real second-cycle A/B/receipt acceptance remains unproved;
synthetic two-cycle success is not a substitute. Traversal bounds and selected
ledger absence must not become terminal-donor evidence or a chronology claim.

The [reported-text view](../design/reported-identity-assertions.md), policy
`fec/reported-identity-field-projection@1.0.0`, preserves employer/occupation
and committee organization fields without name merging or identity resolution.
It is a separate source-backed view, not new employer/payment graph edges.

## Review questions before terminal work

1. Keep `confirmed`, context-`resolved`, `unverified` and unresolved identity
   states visible as distinct evidence; decide separately which may support a
   later attribution calculation. No such eligibility rule is accepted here.
2. Keep strict selected-flow and one-peer-conduit rules as explicitly scoped
   assertions, not a claim that excluded shapes lack money or relationships.
   Broader coverage requires additional source-backed rules and new versions.
3. Keep source observations, reconciliation candidates and reported text distinct
   from economic-payment and person/corporation assertions. Connected paths alone
   cannot choose terminal sources or assign dollars.

The earlier summary/report-family calculations are not required inputs to this
generation. Before using them for opening balances or residual funding, review
their own [source/interval and eligibility boundaries](../design/committee-funding-basis.md).
Neither summary differences nor unresolved graph amounts become inferred donors.

## Proposed checkpoint and verification

The [checkpoint evidence inventory](./pre-attribution-checkpoint-2026-09-14.md)
pins the exact generation, window results, relevant manifests and retained build
snapshots. It distinguishes fresh file verification from earlier execution gates
and lists missing restore work. The checksum list is review evidence, not a
complete dependency inventory or an enforced retention mechanism.

This turn added resolver characterization tests and reran the candidate resolver,
A selector, B reporting, reference and association package tests successfully.
Candidate-resolution race tests and vet also pass. No production Go behavior
changed. Existing corpus executions remain the linked
point-in-time evidence; they were not all rerun by this review.
