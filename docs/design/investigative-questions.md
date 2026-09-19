# Investigative question catalog

> **Status:** Draft for product review. These questions define the behavior the
> product must support. They are not claims that the current system answers
> them correctly.

## How to use this catalog

Each question is a product contract unit. Before implementation, it will gain:

- An accepted response schema.
- Applicable source domains.
- Evidence and calculation rules.
- Representative real and synthetic scenarios.
- Freshness and coverage requirements.
- Query and resource budgets measured against the real corpus.

Unless a question says otherwise, every answer follows the common answer
contract in [`product-contract.md`](./product-contract.md#answer-contract) and
the shared [money-measure contract](./money-measures.md). Monetary questions
return points, intervals, unknown states, or alternative scenarios with their
coverage and attribution states; they never manufacture a midpoint.

## Candidate questions

### C-001 — What receipts did the candidate's authorized committees report?

Return receipts to authorized candidate committees by cycle, committee, and
source category. Reconcile the result to the applicable FEC summaries and show
amendment and coverage state.

Do not include independent expenditures or lobbying in this amount.

### C-002 — What outside money supported or opposed the candidate?

Return independent expenditures supporting and opposing the candidate as
separate directional totals. Show spender, dates, purpose, source records, and
upstream attribution when available.

These totals contain expenditures, not contributions received by the spender.
Contributions to an authorized candidate committee belong in C-001.
Contributions or transfers to an outside spender may explain who funded its
activity, but are not added to the expenditure total.

Opposition spending must never be described as funding received by the
candidate.

### C-003 — Which terminal sources are attributed to the candidate's receipts?

Return terminal entities, exclusive attributed amounts, unresolved remainder,
and the disclosed or calculated paths from those entities to candidate
committees. State why each entity was terminal and whether the allocation was
direct, earmarked, or proportional.

### C-004 — Which terminal sources funded outside spending around the candidate?

Apply a distinct calculation to each independent-expenditure spender. Keep
support and opposition separate. Do not reuse the candidate-receipts
denominator or imply candidate control.

The attributed source amounts decompose the spender's reported expenditure;
they are not added on top of it. Preserve any portion that cannot be attributed
as an unresolved remainder.

### C-005 — Which people contributed?

Return itemized contributors with amount, transaction history, recipient
committee, amendments or refunds, public source records, entity-resolution
state, and any disclosed employer. Preserve summary-only individual money as
an unresolved-detail amount rather than inventing donor composition.

### C-006 — Which organizations are connected to the candidate's money?

Return organization-controlled political money separately from
employee-affiliated, executive-affiliated, founder-affiliated, and
owner-affiliated personal contributions. Identify reused transactions so the
cross-cut cannot be summed as another exclusive funding channel.

### C-007 — How did the candidate's financial network change?

Compare cycles or graph snapshots. Identify new or removed top sources,
material amount changes, changed entity resolutions, changed classifications,
and changes caused only by source freshness or amendments.

### C-008 — What cannot be determined for this candidate?

Return unresolved dollars and identities by reason: unitemized summary,
missing source partition, unmatched organization, unresolved passthrough,
classification uncertainty, depth or result limit, or inconsistent source
records.

## Organization questions

### O-001 — What political committees and source identities belong to this organization?

Return resolved PACs, subsidiaries, aliases, lobbying-client identities,
registrant identities, and corporate-family relationships. Every merge must
show its method, evidence, confidence, and alternatives where relevant.

### O-002 — Which candidates and committees received organization-controlled money?

Return direct PAC contributions, committee transfers, and attributed upstream
funding. Distinguish disclosed transactions from proportional attribution.

### O-003 — Which candidates did the organization support or oppose independently?

Return independent expenditures by direction, spender, date, amount, purpose,
and upstream path. Do not describe opposition spending as support.

### O-004 — What did organization-affiliated people contribute?

Return separate views for self-reported employees and resolved executives,
founders, or owners. Preserve the person and original transaction as the donor;
do not relabel the transaction as a corporate donation.

### O-005 — What did the organization report about lobbying?

Return client and registrant filings, lobbying income or expenses with their
reported meaning, periods, lobbyists, issues, specific issue text, government
entities contacted, affiliated organizations, and source filings.

### O-006 — How does political spending relate to lobbying activity?

Place campaign-finance and lobbying timelines next to each other. Show shared
entities, issues, bills, committees, or periods as evidence or context. Do not
claim that a donation purchased a lobbying outcome or that lobbying caused a
vote. Keep each lobbying amount at its report grain and each campaign
contribution attached to its disclosed political recipient.

### O-007 — What other organizations share this political network?

Return similarity or community results based on an explicit edge set, time
window, weighting method, and algorithm version. Allow inspection of the
relationships that produced the similarity.

## Person questions

### P-001 — What disclosed political contributions did this person make?

Return transactions, committees, candidate associations, cycle totals,
amendments, refunds, and source records. Do not merge people solely because
their normalized names match.

### P-002 — What organizations is this person associated with?

Return each employment, executive, founder, owner, lobbying, or political-role
relationship separately with its source, time scope, and resolution
confidence.

### P-003 — Which contribution patterns are visible?

Return recipient overlap, repeated committee routes, cycle changes, and graph
neighbors. Pattern language must remain descriptive unless an accepted model
supports a stronger derived claim.

### P-004 — Is this contributor also represented in lobbying disclosures?

Return resolved lobbyist identities and filings. A name match alone is not
sufficient; the answer must expose the identity-resolution basis.

## Race and cohort questions

### R-001 — Who is running in the same contest?

Return candidates, offices, parties as disclosed, election stage, cycle, and
the source records used to define the cohort. Do not merge races solely by a
display-name match.

### R-002 — Which sources are shared or distinctive across the race?

Compare terminal sources, contributors, organization associations, and
committee paths as separate overlap measures. Report coverage differences so a
thin candidate record does not appear artificially distinct or grassroots.

### R-003 — What outside spending surrounds the race?

Return supporting and opposing independent expenditures by candidate,
spender, terminal attribution, and time. Preserve direction and avoid summing
hostile spending into a candidate's receipts.

### R-004 — How did the financial shape of the race change?

Compare published snapshots or reporting periods. Separate real source changes
from amendments, coverage arrivals, identity-resolution changes, and
calculation-version changes.

## Path and neighborhood questions

### G-001 — What paths connect an organization and candidate?

Return ranked, cycle-simple paths with complete vertices and edges. Permit
filters for monetary only, associations included, cycle, date, direction,
amount, evidence status, and confidence.

The result must distinguish:

- A disclosed chain of transactions.
- A proportional allocation over disclosed pooled transactions.
- An association path that does not represent money flow.

### G-002 — What paths explain a reported amount?

Starting from an aggregate, return the path contributions that reconcile to
that amount, plus any unresolved remainder. A path contribution must identify
the calculation version and allocation basis.

### G-003 — What is near this entity?

Expand a neighborhood in incoming, outgoing, or both directions. The caller
selects relationship families. Results are paginated or continued, not
silently truncated.

### G-004 — What is the shortest or strongest connection between two entities?

Support shortest-hop and ranked weighted paths. "Strongest" requires a named
weighting policy; money, confidence, recency, and edge count cannot be mixed
without an explicit formula.

### G-005 — Where are the cycles and repeated passthrough structures?

Return committee-transfer cycles and repeated routing motifs without treating
their existence as wrongdoing. Show dates, amounts, and source records so an
investigator can evaluate the structure.

### G-006 — Why did traversal stop?

For each stopped branch, return terminal classification, unresolved reason,
query limit, missing denominator, cycle guard, or absent upstream evidence.

## Lobbying questions

### L-001 — Who reported lobbying for whom?

Return filing, client, registrant, lobbyists, reporting period, and reported
income or expenses. Preserve amendments and terminations.

### L-002 — What issues and government entities were reported?

Return general issue codes, specific issue text, government entities, and the
filing section that connects them. Preserve known historical source
limitations in how government entities were attached to activities.

### L-003 — Which bills or legislative subjects are referenced?

Return explicit text references and any resolved bill identity with extraction
method and confidence. A likely text match is not an official structured link.

### L-004 — Which officials are contextually related?

Return committee membership, sponsorship, or vote context only when supported
by separate congressional records. Label the relationship contextual unless a
lobbying filing explicitly identifies the official.

### L-005 — What political contributions appear in lobbying contribution reports?

Return disclosed contribution-report items with contributor, payee, honoree,
type, amount, and date. Reconcile overlaps with FEC transactions without
assuming that similarly shaped records are identical.

### L-006 — How did lobbying activity change over time?

Compare reported amounts, registrants, lobbyists, issues, and contacted
government entities by reporting period and source snapshot.

## Legislative-influence questions

These belong to the later
[legislative-influence phase](./legislative-influence.md), not the initial
candidate-funding implementation.

### LI-001 — What official legislative roles surround a referenced bill?

Return bill identity, official sponsor and cosponsors, committee referrals,
time-bounded committee membership, actions, amendments, and votes from
official congressional records. Use `sponsor`, not `author`, unless a source
explicitly establishes authorship. These are contextual, not monetary, edges.

### LI-002 — Who may benefit or be burdened by the legislation?

Return versioned beneficiary or burden hypotheses derived primarily from bill
text, official summaries, affected programs, eligibility rules, industries,
geographies, named entities, and other independent policy evidence. Include
direction, confidence, alternatives, supporting text, and method version.

Political donations and lobbying relationships may nominate or corroborate a
hypothesis. They cannot be its sole evidence; using a donation to infer benefit
and then presenting that donation as money from a beneficiary is circular.

### LI-003 — How do campaign money and lobbying intersect with the bill?

Return separate paths for campaign contributions, independent spending,
lobbying filings and activities, bill references, legislative roles, and
inferred effects. A report-level lobbying amount cannot be copied to every
bill or official. A contribution to a political committee is not personal
income to the member.

### LI-004 — What does the combined pattern establish?

Separate disclosed money, disclosed lobbying, official legislative roles,
resolved bill references, inferred effects, and observed timing. Surface their
intersection for investigation without claiming contact, motive, causation,
quid pro quo, bribery, or corruption.

## Comparative and global questions

### A-001 — Which candidates share important sources?

Compare terminal sources, individual contributors, organization-affiliated
contributors, and outside spenders as distinct overlap measures.

### A-002 — Which organizations or committees form communities?

Return versioned community assignments and the edge family, period, weighting,
resolution, and stability information used to compute them.

### A-003 — Which entities are structurally central?

Return named centrality measures rather than one generic "influence" score.
Explain the graph projection and why the metric is relevant.

### A-004 — What political alignment does the disclosed activity suggest?

If implemented, return a multidimensional, time-bounded derived result with
comparison cohort, inputs, uncertainty, and explanation. Do not convert the
legacy `ideological` committee category into a left-right score.

### A-005 — What changed between graph snapshots?

Return added, removed, and changed source records, identities, edges,
classifications, paths, aggregates, and global analytics. Distinguish new
source data from changed code or resolution rules.

## Data and trust questions

### D-001 — How current is this answer?

Report freshness independently for each contributing source and identify the
latest transaction or filing represented.

### D-002 — How complete is this answer?

Report itemized versus summary-only coverage, missing source partitions,
unresolved entities, stopped traversal branches, and any query result limit.

### D-003 — Why did this answer change?

Identify source amendments, new filings, parser changes, entity-resolution
changes, classifier changes, and calculation-version changes.

### D-004 — Can the answer be reproduced or exported?

Return or export snapshot ID, query parameters, result schema version, source
record identifiers, and calculation versions.

### D-005 — Which entity did the search resolve?

Return official identifiers, source identities, aliases, entity type,
resolution method, confidence, and competing candidates when a query is
ambiguous. Search is a discovery operation, not hidden identity resolution.

## Accepted product decisions

### PD-001 — Preserve disclosed grain and aggregate late

**Decision:** Do not choose a canonical collapsed funding taxonomy during
product discovery. Preserve immutable source records and typed normalized facts
at the finest reliable disclosure grain. Calculate totals, channels, terminal
attribution, organization rollups, donor tiers, lobbying summaries, ideology or
alignment, and other groupings as versioned projections.

Campaign finances, outside election spending, and lobbying activity remain
distinct semantic domains because their amounts mean different things. They
are not storage buckets. The legacy five-channel model and the proposed
three-context presentation are both candidate views over the same facts, not
competing source schemas.

Materialized aggregates are allowed for query performance when they:

- Retain source-fact lineage.
- State filters and calculation version.
- Rebuild deterministically.
- Invalidate when relevant source or resolution state changes.
- Permit drilldown and regrouping along preserved dimensions.

This preserves specificity without requiring every interactive request to scan
the full raw corpus.

### PD-002 — Reuse cycle partitions; declare calculation windows

**Decision, clarified 2026-09-13:** Treat each FEC two-year cycle as a reusable,
independently reproducible data partition and reporting dimension, not a wall
around every calculation. The default product view covers the latest four
available cycles, including the active cycle. Each calculation declares its
output period and required evidence window. Combine compatible additive
subtotals only when membership is disjoint; use detailed evidence or sufficient
mergeable state for non-additive and cross-cycle identity/graph calculations.
The [calculation-window contract](./cycle-calculation-windows.md) defines the
boundary, prior-period context and staged implementation sequence.

The default window does not limit retention. Preserve all ingested historical
facts and allow explicit cycle or range selection. Every cross-cycle result
must state its included cycles, source snapshots, coverage-through dates,
partial-cycle status, and nominal or adjusted-dollar basis. Identity and
classification evidence may be shared across cycles only through versioned
relationships with valid time; inclusion or a threshold result in one cycle
must not silently alter another cycle's amounts or categories.

## Open product decisions

The following decisions still require explicit review. Recommendations are
starting points, not accepted outcomes.

### PD-003 — How should paths be ranked by default?

**Recommendation:** Default to the highest attributed-amount paths within a
selected cycle, prefer stronger evidence when amounts tie, return a limited
top set, and let the investigator switch to shortest-hop, recency, or
confidence ranking.

### PD-004 — Which personal contributors receive dedicated dossiers?

**Recommendation:** Permit lookup of any person represented in public source
records, but build organization-resolution and expensive graph projections
only for contributors crossing a disclosed, cycle-specific materiality rule.
The threshold should control processing cost, not imply wrongdoing or special
legal status.

### PD-005 — How should employer-affiliated giving be presented?

**Recommendation:** Use the label "employee-affiliated contributions," retain
the individual as donor, state that employment is self-reported when it is,
and never include the amount in organization-controlled money.

### PD-006 — Should the product publish an ideology score?

**Recommendation:** Do not ship a single ideology score in the initial
product. First ship inspectable recipient overlap, community membership, issue
activity, and time-series comparisons. Consider a multidimensional alignment
model only after its ground truth and explanation contract are defined.

### PD-007 — How should lobbying connect to officials?

**Recommendation:** Require an explicit source to create a direct lobbying
contact edge. Show committee membership, bills, sponsorship, and votes as
contextual paths with distinct styling and claim language.

### PD-008 — Is the initial UI private or public?

**Recommendation:** Build the evidence and claim contract as if results will be
public, but keep deployment scope independent. Public release introduces
additional requirements for corrections, abuse controls, accessibility, and
plain-language methodology.

### PD-009 — Are manual identity corrections allowed?

**Recommendation:** Allow reviewed overrides only as versioned evidence with
author, rationale, effective scope, and supersession history. Never hide an
override inside code or an untracked cache.

### PD-010 — Which exports are required at first cutover?

**Recommendation:** Require stable JSON for every UI investigation and a
portable evidence bundle for saved investigations. Defer bulk CSV and graph
exchange formats until a concrete consumer requires them.
