# Product contract

> **Status:** Draft for product review. This document defines desired behavior,
> not the current Python implementation. Open decisions are routed through
> [`investigative-questions.md`](./investigative-questions.md#open-product-decisions).

## Product promise

Legal Tender is an evidence-first investigative system for federal political
money and lobbying. It traces disclosed financial activity through people,
political committees, organizations, and related entities as far upstream as
the available evidence supports. It returns the path, source records,
calculation method, coverage, and uncertainty behind every material claim.

The product must reveal both what the public record supports and where the
record stops. It must not turn missing data, proportional attribution, entity
resolution, employment, or political context into a stronger claim than the
evidence warrants.

Every monetary answer follows the shared
[money-measure contract](./money-measures.md). A threshold band, rounded
estimate, incomplete source, or unresolved filing version must remain visible
as an interval, unknown state, or scenario rather than a precise total.

The old mission, "trace every dollar to its origin," remains an aspiration, not
a literal guarantee. The enforceable promise is:

> Trace every disclosed dollar as far as the evidence and an explicit
> attribution model permit; preserve unresolved amounts and explain why they
> remain unresolved.

## Primary user and use

The primary user is an investigator examining a candidate, organization,
person, committee, race, or policy area. The product should support both quick
orientation and evidence-level inspection without requiring direct database
access.

The system must support:

- Interactive investigation in a web UI.
- Stable API responses for other local services and reproducible analysis.
- Direct access from every aggregate to the paths and source records behind it.
- Dated snapshots so an investigation can be reproduced after source data or
  entity resolution changes.

Whether the initial UI is private, shared, or public is an open deployment and
presentation decision. Evidence and claim discipline apply in every mode.

## Product principles

### Evidence before narrative

The system presents records, relationships, calculations, and uncertainty. It
may help an investigator discover a pattern, but it does not declare motive,
coordination, corruption, illegality, or policy influence unless a source
explicitly establishes that fact.

### Different financial phenomena stay different

The product must not collapse these into one number:

- Receipts reported by a candidate's authorized committees.
- Independent expenditures supporting the candidate.
- Independent expenditures opposing the candidate.
- Donations by people affiliated with an employer or organization.
- An organization's own PAC or independent-expenditure activity.
- Lobbying income or expenses.

They may appear together in a dossier or graph. Each retains its own label,
direction, amount semantics, and source.

The product recognizes three semantic domains that can be connected but cannot
be summed into one universal money total:

1. **Campaign finances** — receipts and disbursements reported by the
   candidate's authorized committees.
2. **Outside election spending** — independent expenditures supporting or
   opposing the candidate, plus any separately modeled coordinated or
   electioneering activity.
3. **Lobbying activity** — reported lobbying income or expenses, clients,
   registrants, lobbyists, issues, and government entities.

These domains are not storage buckets and do not replace the underlying facts.
They prevent invalid comparisons and sums while allowing an investigator to
filter and group the preserved facts in other useful ways.

### Product phases

The initial Go product is the federal campaign-money system: candidate and
committee receipts, transfers, outside spending, terminal-source attribution,
and evidence-backed paths. Lobbying is not required for that funding product
and does not enter its totals.

A later [legislative-influence phase](./legislative-influence.md) combines LDA
lobbying evidence with official bills, sponsorship, committee jurisdiction,
time-bounded membership, actions, votes, campaign money, and versioned
beneficiary inference. The common evidence and graph boundaries must permit
that phase without treating its relationships as campaign-money edges.
Before that phase lands, lobbying and legislative-influence fields are
`not_implemented`, never zero.

### Preserve grain; aggregate late

The source and normalized layers preserve the finest reliable grain supplied
by each disclosure. No headline taxonomy, threshold, entity rollup, or UI total
may be the only surviving representation of data.

In particular:

- Preserve each source record and revision with its original identifiers and
  fields.
- Normalize records into typed facts without discarding dates, amounts,
  election designations, transaction types, memos, support or opposition,
  filing periods, source entities, or amendment relationships.
- Keep identity resolution, corporate-family membership, terminal
  classification, donor tiers, political alignment, and channel membership as
  versioned derived relationships or projections.
- Build totals, rankings, cohorts, and headline views from those facts with
  explicit filters and calculation versions.
- Permit materialized aggregates for interactive performance only when they
  are reproducible, invalidated by relevant changes, and expandable to their
  constituent facts.

Preserving grain does not require every raw row to be a graph edge. Raw and
normalized facts may be documents while graph edges provide indexed
relationships or projections. An aggregate edge must retain lineage to the
facts it summarizes; physical optimization cannot become information loss.

### Facts, resolution, attribution, and context stay distinguishable

Every returned claim identifies all applicable evidence states. These states
are composable, not a confidence ladder: an attributed amount can traverse
disclosed transactions between resolved identities and still include an
unresolved remainder.

| Status | Meaning |
|---|---|
| `disclosed` | Directly represented in an official source record. |
| `resolved` | Two source identities were joined using an explicit identity-resolution method. |
| `attributed` | An amount was allocated by a documented calculation over disclosed records. |
| `associated` | A non-monetary relationship such as employment, corporate control, or committee service. |
| `contextual` | A useful correlation that does not establish contact, payment, coordination, or causation. |
| `unresolved` | The system cannot support a more specific identity, path, or allocation. |

The UI must communicate these states without relying on color alone.

### Improvement is required

Legacy behavior is evidence, not authority. The rewrite must preserve valuable
domain knowledge while correcting misleading terminology, conflated amounts,
weak provenance, fragile classifiers, full-graph application processing, and
unnecessary recomputation.

No legacy behavior receives a presumption of parity. A parser rule, threshold,
identity heuristic, collection shape, graph edge, classification, aggregate,
or presentation survives only after the target product and evidence contracts
justify it independently. Legacy differential tests apply only to behaviors
explicitly accepted as `KEEP`.

## Core concepts

### Source record

An immutable representation of an official filing, transaction, bulk-file row,
API response, or published summary. Corrections and amendments create new
versions; they do not silently rewrite the evidence used by an older snapshot.

### Normalized fact

A typed representation of one disclosed event or state at the finest reliable
source grain, such as a contribution, transfer, expenditure, filing activity,
reported association, or published summary. It retains the source record and
revision lineage used to create it.

### Derived projection

A reproducible view over normalized facts and versioned relationships. Examples
include terminal-source attribution, the legacy five channels, the three
semantic domains, organization rollups, donor tiers, communities, centrality,
and alignment measures. A projection is disposable application state, not the
only copy of its inputs.

### Entity

A canonical investigative subject such as a person, organization, corporate
family, political committee, candidate, office, lobbying registrant, lobbying
client, government entity, issue, or bill. A canonical entity retains links to
all source identities and the evidence used to merge them.

### Monetary edge

A directed relationship supported by a financial record, such as a
contribution, committee transfer, or independent expenditure. It references
the normalized fact or facts that support it. Only monetary edges carry dollars
through an attribution calculation.

### Association edge

A relationship such as employment, corporate ownership, committee affiliation,
identity equivalence, lobbying work, or legislative service. Association edges
can enrich an investigation but cannot carry money unless a separate source
record establishes a monetary transaction.

### Path

An ordered sequence of entities and relationships returned with direction,
time scope, source evidence, and any calculated amount. A path may show
disclosed connectivity without proving that a specific fungible dollar was
earmarked across every hop.

### Terminal source

A terminal source is an attribution boundary selected by a versioned domain
policy. It is not necessarily a node with no incoming edges, the ultimate
economic origin of every dollar, or a permanent property of the entity.

The product must report:

- Why traversal stopped at the entity.
- Which classifier and version made the decision.
- The records and entity attributes used by the classifier.
- Whether the amount was direct, explicitly earmarked, or proportionally
  attributed through pooled committee funds.

### Unresolved attribution

Money for which the system knows a total but cannot support a more specific
source, path, or category. Unresolved attribution is a first-class result, not
an error bucket to hide or automatically relabel as grassroots money.

## Required investigative surfaces

The detailed question catalog lives in
[`investigative-questions.md`](./investigative-questions.md). The product must
provide the following connected surfaces.

### Candidate dossier

The candidate view must separate authorized-committee receipts, supportive
outside spending, opposing outside spending, lobbying context, and unresolved
attribution. It must show terminal sources and organization or person
associations without representing employee donations as company-controlled
money.

Every aggregate must be filterable by election cycle and expandable to paths
and source records. Lobbying context can connect financially related
organizations to their disclosed lobbying or connect the candidate's office to
committees, bills, and issues. It does not become money received by or spent for
the candidate.

### Organization dossier

The organization view must combine, without conflating:

- Its own political committees and disclosed transfers.
- Independent expenditures it made or funded.
- Donations by people who reported the organization as their employer.
- Donations by resolved executives, founders, or owners.
- Related subsidiaries, aliases, and corporate-family identities.
- Lobbying filings, registrants, lobbyists, issues, government entities, and
  reported lobbying amounts.
- Candidate and committee paths supported by the graph.

### Person dossier

The person view must show public contribution and lobbying identities,
employer or leadership associations, resolution confidence, recipient
committees and candidates, and any organization rollups that reuse those
amounts. It must not infer that an employer directed a personal contribution.

### Race and cohort view

The race view must compare candidates within the same contest without assuming
their source coverage is equivalent. It must show shared and distinctive
terminal sources, contributor overlap, supportive and opposing outside
spending, unresolved attribution, freshness, and coverage differences.

### Path explorer and neighborhoods

The user must be able to select two entities and request relevant paths, or
start from one entity and expand its neighborhood in either direction. The
result must preserve edge semantics and permit filters for time, election
cycle, relationship family, amount, evidence status, and confidence.

Literal unbounded enumeration of all paths is not a product requirement. In a
cyclic graph it can be infinite or unusably large. The product instead returns
cycle-simple, bounded, ranked paths with continuation controls and an explicit
notice when results were limited.

### Search and identity selection

The user must be able to find entities by official identifier, current name,
historical name, or resolved alias. Ambiguous results must present candidates
and supporting identifiers; search must not silently merge identities.

### Lobbying view

Lobbying is a first-class domain of the later legislative-influence product,
not a candidate-funding channel.

The product must show who reported lobbying for whom, reported income or
expenses, filing periods, lobbyists, issues, specific issue text, affiliated
organizations, and government entities contacted. It must preserve the filing
as the evidence-bearing fact.

The system must not create a direct client-to-politician influence edge when a
filing only names a chamber, agency, committee, issue, or bill. Committee
membership, bill sponsorship, and votes may be shown as separately labeled
context. Official sponsorship means introduction, not proven authorship. A
report-level lobbying amount remains attached to its filing and cannot be
allocated to every activity, bill, committee, or member it touches.

Lobbying contribution reports can contain political-contribution items. When
an item resolves to an FEC transaction, the graph links the two source records
as evidence for the same activity; it does not count the amount a second time.

The complete boundary for money, legislative facts, and inferred beneficiaries
is the [legislative-influence contract](./legislative-influence.md).

### Comparative and graph analysis

The product should support candidate, organization, race, and time-period
comparisons. Community, centrality, similarity, and ideological-alignment
results are derived analytics. Each must identify its input snapshot,
algorithm, parameters, and limitations.

No single left-right score is assumed by this contract. "Ideological" in the
legacy committee classifier is not an ideology measurement and must not be
presented as one.

## Answer contract

Every material API or UI answer must make the following available:

1. The answer or monetary point, interval, unknown state, or scenario set.
2. Its financial and temporal meaning.
3. Evidence status.
4. Source coverage and freshness.
5. Supporting records.
6. Supporting graph path or calculation components when applicable.
7. Resolution and attribution methods with versions.
8. Confidence or unresolved remainder.
9. Snapshot identifier and computation time.

A compact UI may hide detail initially, but it cannot omit the ability to
inspect it.

## Money and attribution rules

### Authorized-committee receipts and outside activity

Authorized-committee receipts are money or reportable value received by
committees authorized to act on the candidate's behalf. They can include
individual contributions, contributions or transfers from other committees,
candidate contributions or loans, and other receipt categories reported by the
committee. The exact categories and treatment of refunds, amendments, loans,
and in-kind contributions belong in the calculation contract.

Independent expenditures are different transactions made by outside spenders.
The expenditure total is the amount spent supporting or opposing the
candidate. Donations or transfers into the spender may explain its upstream
funding, but those source amounts are an attribution of the expenditure total,
not additional spending to add on top.

Opposition spending is not candidate funding. Lobbying is neither a campaign
receipt nor an independent expenditure. The product may calculate a broader
"political activity around this candidate" dashboard, but it must not label
the combined contexts as money received, controlled, or spent by the candidate.

### Pooled committee money

Most committee-to-committee money is fungible. Unless a filing explicitly
earmarks a transaction, an upstream amount assigned through pooled accounts is
a proportional attribution, not a literal claim that the same dollar crossed
every edge. The calculation and allocation basis must be visible.

### Individual and organization attribution

A person's self-reported employer is an association. Amounts may be aggregated
as "donations from people reporting employment at X." They must remain
separate from X's PAC, treasury, or independent-expenditure money.

Executive, founder, owner, and corporate-family relationships require their
own evidence and confidence. No association alone proves organizational
direction or a person's ideology.

### Conservation and double counting

Each calculation contract must define its input total and demonstrate how
exclusive output categories plus an unresolved remainder reconcile to it.
Cross-cut views may intentionally reuse the same amount, but must identify the
underlying transaction so consumers cannot sum overlapping views as though
they were exclusive.

Grouping and filtering occur over preserved facts or reproducible projections.
A precomputed total cannot prevent the same data from being regrouped by time,
cycle, transaction type, source, recipient, direction, relationship class, or
another retained dimension.

Exact decimal parsing does not imply exact economic measurement. Source money
observations retain threshold, rounding, accounting-method, and precision
semantics. Calculations carry compatible bounds and the separate coverage,
attribution, revision, identity, and evidence states defined by the
[money-measure contract](./money-measures.md). They never substitute a
midpoint or zero for uncertainty.

## Time, versions, and change

All records and relationships must carry their valid reporting period or
election cycle where available. Entity resolution, classification, and
attribution are versioned independently from source data.

An FEC election cycle is a reusable source-processing, reporting and query
partition, not a mandatory analytical boundary. The initial default view covers
the latest four available two-year cycles, including the active cycle.
Calculations declare their output scope and required evidence window separately;
they may consume detailed facts or compatible projections across cycles.
Only additive results with compatible, disjoint membership can be combined by
summing cycle subtotals. See [cycle partitions and calculation windows](./cycle-calculation-windows.md)
for identity, graph, non-additive and prior-period evidence boundaries.

The four-cycle default is a view, not a retention boundary:

- Preserve every ingested historical source record and normalized fact.
- Permit an investigator to select one cycle, the default four-cycle window,
  or another available range.
- Bind each projection to the exact cycle set and source snapshots it used.
- Distinguish output cycles from evidence cycles, including earlier context;
  expose missing coverage rather than treating a window boundary as an origin.
- Keep transaction dates, filing periods, candidate election years, linkage
  years, and source-cycle labels distinct.
- Mark an active or otherwise incomplete cycle as partial and expose coverage
  through dates by source.
- Do not let entity resolution, classification, materiality, or donor-tier
  decisions from one cycle silently change another cycle's calculation;
  declared cross-cycle dependencies produce new, reproducible result versions.
- State whether a cross-cycle monetary comparison uses nominal or adjusted
  dollars and the adjustment basis.

The product must support:

- A current published snapshot.
- Reproduction of a prior published answer.
- A change view explaining which source records, identities, relationships, or
  calculations changed.
- Partial freshness and coverage reporting by source domain.

New source data should update affected local projections. Global analytics may
be recomputed as versioned snapshot jobs when an incremental result would not
be correct.

## Source scope and sequencing

In scope for the initial campaign-money product:

- Federal Election Commission candidate, committee, contribution, transfer,
  independent-expenditure, and published-summary data.
- External entity sources used to resolve organization and corporate-family
  identities, with explicit provenance and caching.

In scope for the later legislative-influence product:

- Federal Lobbying Disclosure Act filings and contribution reports.
- Official congressional bill, sponsorship, committee, membership, action,
  and vote data.
- Versioned bill-reference, policy-effect, and beneficiary inference.

Deferred unless promoted by a product decision:

- State and local campaign finance.
- State lobbying regimes.
- Foreign Agents Registration Act data.
- Non-public or purchased data.
- Prediction of future donations, votes, or policy outcomes.

## Non-goals

Legal Tender does not promise to:

- Discover money absent from the available public record.
- Prove corruption, bribery, coordination, motive, or causation from network
  proximity.
- Treat employment as political agreement or corporate control.
- Treat lobbying on an issue as contact with every official who has related
  jurisdiction.
- Provide a compliance determination or legal conclusion.
- Return every possible path through a cyclic graph without limits.
- Preserve every legacy classification, threshold, channel, schema, or module.
- Run two permanent orchestration systems for the same pipeline.

## Product acceptance conditions

These conditions apply to the product phase being released. The initial Go
cutover is not blocked by the later legislative-influence questions.

The product is ready to replace the legacy system when:

1. Every required question has a defined response and evidence contract.
2. Approved legacy `KEEP` behaviors pass differential scenarios.
3. Approved `CORRECT` behaviors pass target cases demonstrating the intended
   semantic change.
4. Authorized-committee receipt totals reconcile to authoritative source
   totals within documented source and amendment limits.
5. Terminal-source results expose their paths, allocation method, classifier,
   and unresolved remainder.
6. Employee-affiliated, organization-controlled, independent-expenditure, and
   lobbying amounts remain mechanically distinguishable.
7. Every published claim is reproducible from a source snapshot and versioned
   calculations.
8. Incremental ingestion recomputes the affected local projections without a
   routine full rebuild.
9. Critical interactive queries meet latency and resource budgets established
   by real-corpus probes.
10. The new and legacy systems can run against separate state during shadow
    comparison, and cutover has a tested rollback path.
11. Every aggregate can drill down to preserved facts and can be recomputed
    under different supported filters without re-ingesting source data.

## Accepted direction

The following direction is accepted for design work but still requires
empirical validation before it becomes an implementation decision:

- A fresh Go application rather than an in-place Python refactor.
- ArangoDB as the primary investigative domain database.
- Dagster as the data control plane, with Python restricted to the Dagster API
  and a thin Go-process adapter. All data and domain behavior belongs in Go, as
  defined by the [`Go and Dagster boundary`](./go-dagster-boundary.md).
- Immutable raw source archives outside the graph database.
- Normalized facts preserved independently from disposable aggregates and
  graph projections.
- A graph model that separates monetary, identity, affiliation, lobbying, and
  contextual relationships.
- Vertical source-to-UI slices, shadow comparison, and reversible cutover.
- No blanket legacy-parity goal; accepted behaviors are selected individually
  and all other target behavior follows the new evidence and product contracts.

## Change control

Product decisions update this contract and the question catalog together.
Architecture decisions go in [`../decisions.md`](../decisions.md). Current
behavior belongs in the legacy functional specification. No implementation
detail becomes a product requirement merely because the legacy code contains
it.
