# Legislative influence analysis

> **Status:** Working direction for a later phase, not a locked design.
> Lobbying, congressional, and beneficiary-inference sources are not part of
> the initial candidate-funding implementation. The durable constraint is that
> their facts and relationships fit the shared evidence model if that phase is
> built.

## Purpose

Legal Tender first answers how disclosed campaign money reaches or affects a
candidate. A later legislative-influence phase asks a different question:

> How do organizations' political money, reported lobbying, legislation,
> committee jurisdiction, sponsorship, and votes intersect, and which entities
> may benefit from the legislation?

This is an evidence-backed investigative view, not an extension of candidate
receipts. It must not turn association, timing, or analytical inference into a
claim that a politician received lobbying money, sold a vote, or authored a
bill for a donor.

## Separate domains

### Campaign money

FEC facts establish contributions, receipts, transfers, disbursements, and
independent expenditures. A contribution to an authorized committee is money
reported by the campaign committee; it is not personal income to the officeholder.

Only a preserved financial fact or an explicitly versioned attribution over
financial facts can carry money toward a candidate or political committee.

### Lobbying commerce and activity

An LD-2 amount is normally either lobbying-related income reported by an
outside registrant for one client or lobbying expenses reported by an
organization lobbying on its own behalf. The money belongs on the client,
registrant, and filing side of the graph.

The filing's activities may name issues, bills, congressional chambers, or
federal agencies. Those are targets or subjects of the work, not recipients of
the reported amount. When one filing contains several activities or bills, the
report-level amount cannot be copied or allocated to each one without a
separate accepted allocation model.

LD-203 contribution items remain political-payment evidence. A resolved
LD-203/FEC overlap identifies two disclosures of the same activity and is
counted once.

### Legislative record

Official congressional sources establish bill identity, official sponsor and
cosponsors, committee referral, time-bounded committee membership, actions,
amendments, and votes. Legal Tender uses `sponsor`, not `author`: introduction
does not prove who drafted a bill or at whose request.

These relationships establish legislative role and context. They do not carry
money and do not prove that a lobbyist contacted every relevant member.

### Inferred beneficiaries

Who benefits from legislation is a derived analytical result, not a publisher
fact. A beneficiary result may identify an organization, corporate family,
industry, occupation, geography, program population, or unresolved class.

The primary evidence must come from the legislation and independent context:

- exact bill and amendment text;
- official summaries, subjects, actions, and fiscal or regulatory scope;
- named entities, eligibility rules, appropriations, taxes, subsidies,
  contracts, prohibitions, or compliance burdens;
- versioned organization and industry mappings; and
- explicit lobbying positions when the source actually states support,
  opposition, or a requested change.

Donations and lobbying relationships may nominate a case for review or
corroborate an independently derived beneficiary. They must not be the sole
evidence that an entity benefits. Otherwise the analysis becomes circular:
the system would infer benefit from a donation and then present that same
donation as evidence of money from a beneficiary.

Every inferred-beneficiary relationship records its method, version, input
text and records, direction of expected effect, confidence, alternatives, and
unresolved reasons. An LLM-assisted result additionally records model, prompt,
and cited text spans. No unsupported binary `benefits=true` edge is allowed.

## Valid combined path

The graph can return a path such as:

```text
organization
  --[reported lobbying payment or expense]--> lobbying filing / registrant
  --[filing activity references]-------------> bill
  --[officially sponsored by]----------------> member
  --[official vote]--------------------------> vote position

organization or related PAC/person
  --[FEC contribution]-----------------------> political committee
  --[authorized linkage]---------------------> candidate/member

bill
  --[versioned inferred effect]--------------> possible beneficiary
```

The UI may show these branches together. It must keep the amount attached to
the relationship that reported it. It cannot add a report-level lobbying
amount to campaign receipts, assign the amount to a bill sponsor, duplicate it
across listed bills, or describe a contextual path as a payment path.

## Claim levels

An organization's explicit support, opposition or requested change is a **declared
position**, not a verified policy effect. Preserve the speaker, source date, bill
reference and stated scope. Do not infer that every member, subsidiary or parent
shares it, or that it describes every later bill version. The
[340B Health example](../audit/relationship-evidence-comparison-2026-09-16.md#one-future-legislation-example-starting-in-our-data)
shows a position and official sponsor relationship without establishing a payment,
lobbying contact or roll-call vote. This is reviewed evidence, not implemented ingestion.

The product distinguishes these claims mechanically:

| Level | Example | Meaning |
|---|---|---|
| Disclosed money | PAC contributed to committee | Direct source financial fact. |
| Disclosed lobbying | Filing reported an amount and activity | Report-level lobbying fact; no politician payee implied. |
| Official legislative role | Member sponsored, served on a committee, or voted | Congressional fact; not a monetary edge. |
| Declared position | Organization's statement supports a specified bill | Evidence of the speaker's position; not independent proof of benefit or lobbying contact. |
| Resolved reference | Filing text refers to a specific bill | Extracted or explicit identity with method and confidence. |
| Inferred effect | Bill may benefit or burden an entity or class | Versioned analysis with uncertainty and alternatives. |
| Observed intersection | Donor, lobbying client, bill, member, and vote appear in one evidence graph | Investigative pattern; not proof of motive, causation, or quid pro quo. |

## Product sequencing

The initial Go product implements campaign-finance ingestion, candidate money
flows, independent spending, terminal-source attribution, and their evidence
UI. It preserves generic evidence, entity, time, money-measure, and
relationship boundaries so legislative sources can join later without a data
model rewrite.

The legislative-influence phase then adds:

1. official bill, sponsor, cosponsor, committee, membership, action, and vote
   facts;
2. LDA filings, activities, registrants, lobbyists, and LD-203 contributions;
3. bill-reference extraction from lobbying issue text;
4. organization and corporate-family resolution across FEC and LDA;
5. versioned beneficiary and policy-effect inference; and
6. combined timelines, paths, comparisons, and evidence drilldown.

Once implemented, these production sources join the same Monday 04:00
`America/New_York` refresh schedule. They retain independent source watermarks
and source-native time fields.

## Acceptance conditions

- Candidate receipts and terminal-source totals contain no LD-2 income or
  expense amount.
- An LD-2 report-level amount is conserved once and is not duplicated across
  activities, bills, committees, or officials.
- Every sponsor, cosponsor, committee, membership, action, and vote edge has
  official time-bounded evidence.
- The system never substitutes `author` for the official `sponsor` role.
- A bill reference exposes whether it was explicit, parsed, or inferred.
- Beneficiary inference is independent of the contribution used to compare
  the beneficiary with political money.
- Contextual and inferred edges cannot participate in monetary conservation or
  terminal-source traversal.
- The UI labels co-occurrence and alignment without claiming bribery,
  corruption, motive, contact, or causation.

## Related contracts

- [Product contract](./product-contract.md)
- [Investigative question catalog](./investigative-questions.md)
- [Target source catalog](./source-catalog.md)
- [Federal lobbying source design](./lobbying-source-ingestion.md)
- [Money-measure contract](./money-measures.md)

## Official references

- [Lobbying Disclosure Act guidance](https://lobbyingdisclosure.house.gov/ldaguidance.pdf)
- [LD-2 activity instructions](https://lobbyingdisclosure.house.gov/help/wordDocuments/pagetwolobbyingactivity1.htm)
- [Congressional Research Service sponsorship and cosponsorship](https://www.congress.gov/crs_external_products/RS/PDF/RS22477/RS22477.24.pdf)
