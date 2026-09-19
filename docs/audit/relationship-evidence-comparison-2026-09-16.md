# Relationship evidence comparison

Read-only investigation, 2026-09-16. This answers the next question in the
[relationship exploration](../design/relationship-exploration.md): what can the
existing sources support, and what small implementation would be useful next?
It is not acceptance of an identity rule, financial attribution or new source feed.

## Result

Keep Wikipedia-first discovery and Wikidata's structured claims. They retrieve
useful candidates and relationships, but do not resolve every reported employer
or identify an FEC donor automatically. SEC and GLEIF supply different evidence;
neither replaces them or provides a universal company directory. Company pages
fill some observed gaps, but our HTML reader does not interpret their prose.

The next useful slice is a **queryable source-relationship view** over the
structured evidence already retained. It should return reported executives,
founders, board members, ownership and organizational hierarchy with their actual
dates and sources. This does not require first solving every donor identity.
The finite proposed boundary is below; no implementation was added in this review.

## Scope and provenance

The comparison deliberately selects different failure modes, not a representative
sample. Some cases share a person or organization. It cannot establish a resolver's
population accuracy, private-company coverage or present external-service health.

- **Our FEC data:** the retained 2024 committee-master publication and the complete
  as-filed report in the [person corpus](../../tests/fixtures/person-affiliation/corpus.json).
  Committee facts were read from the published compressed file through a read-only
  mount, not inferred from a web search. Only selected organization fields were output.
- **Existing captures:** Wikipedia/Wikidata, GLEIF, the SEC directory and filing,
  and company pages. Source readers and existing offline evaluations were exercised.
  No new enrichment capture, Arango write or full receipt scan was run.
- **New web research:** one organization's stated legislative position and official
  congressional records. These were inspected manually, not ingested or added as
  retained parser fixtures. They test the future model, not an automated capability.

The committee fact set is
`e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d`.
Its source-backed query references are retained in
[queries.json](../../tests/fixtures/organization-resolution/capture-v1/queries.json).
This is the selected historical publication, not a claim about the newest FEC filing.

| Committee ID | Reported connected organization | Fact ID |
|---|---|---|
| `C00181529` | `1ST SOURCE CORPORATION` | `68b22c730846529a613889f0b3e8f16d28ab74383f7bc936220deb8b27d8e08d` |
| `C00344531` | `1199 SEIU UNITED HEALTHCARE WORKERS EAST` | `2d513f16e71f724491d73b3aec5d3900e7689c38e30268a128b415afb0689b79` |
| `C00348540` | `1199 SEIU UNITED HEALTHCARE WORKERS EAST` | `4db1d15d165726afdd57c079d04590db8d7622f85dd86ad3ab85214613ab43ea` |
| `C00619601` | `340B HEALTH` | `a932904b91595a9c86377a6b2c9d7c5a2ff5c3929306113274b6abae5f418d03` |

The person cases retain records 58, 59, 263 and 2772 of
[FEC report 1730162](https://docquery.fec.gov/dcdev/posted/1730162.fec), filed by
SAM BROWN FOR NEVADA (`C00845032`). These are separate source appearances, not a
resolved donor directory or latest-amendment contribution totals. This review did
not calculate donations; money and residential addresses are unnecessary here.

## Case comparison

| Case | Evidence available | Supported result and remaining gap |
|---|---|---|
| Public company: 1st Source | FEC connected-organization text; Wikipedia-linked `Q4596612`; SEC directory and tagged annual-report registrant `CIK 0000034782` | Several sources identify a corresponding company. The FEC-to-external identity remains a candidate; SEC agreement does not itself prove that cross-source binding. |
| Executive and parent/child: 1st Source | Retained 2024 company announcement names Corporation and Bank, their relationship and different officer roles | The prose distinguishes Andrea Short's Corporation presidency from her Bank CEO role. These are review findings, not automatically extracted relationships or identified FEC donor appearances. |
| Former executive: John Chambers | FEC reports `JC2VENTURES`; Wikipedia returns a businessperson; retained Cisco Wikidata claim reports CEO years 1995–2015; JC2 pages describe other roles | Historical Cisco context is available. It must not replace the reported employer, establish a current Cisco job or imply current ownership. Donor identity and transaction-time JC2 role are unapproved. |
| Private-company donor appearance: David Duffield / Ridgeline | FEC employer `RIDGELINE INC`; Wikipedia person candidate; company page uses Dave Duffield; GLEIF returns two corresponding-name LEIs | A useful person candidate and company evidence exist. Nickname, legal-entity binding and historical role validity remain separate gaps. Two LEIs must not be merged on their names. |
| Trade/membership organization: 340B Health | FEC explicitly reports the PAC's connected organization; primary-source legislative statement below | Useful organizational context need not wait for an issuer identifier. Its Wikipedia query was unattempted in the retained capture, not a search that found nothing. |
| Union: 1199 SEIU | Two separate FEC committees report the same organization text; retained organization history and Wikipedia candidates | Opt-in v2 proposes reviewed `Q4547697`, not the annotated disambiguation rival. The committees remain distinct, and the proposal is not identity-publication approval. |
| Namesakes and non-leadership control cases | Chambers searches return both businessperson `Q1393271` and makeup artist `Q93784`; another FEC appearance reports Michael O'Brien, Valley Tech Services, occupation engineer | Name correspondence does not select a person. The engineer's identity/role remains unverified: neither an executive classification nor a verified ordinary-employee negative is justified. |

The [retained 1st Source announcement](../../tests/fixtures/organization-resolution/sources/1stsource-2024.html)
is especially useful because one person has different roles in two related entities.
Its page date is May 3, 2024; the announcement text is dated May 1. It describes
1st Source Corporation as the Bank's parent and Short as President of the
Corporation while continuing as President and CEO of the Bank. A single
`primary_company` or undifferentiated `executive_of` would lose meaningful data.
The extraction replay preserves those text spans; it does not interpret them.

For person evidence and its reviewed annotations, see the
[retained corpus findings](../design/person-affiliation-corpus.md). Registry ambiguity
and the limits of each SEC source are documented in
[registry discovery](../design/organization-registry-discovery.md),
[issuer discovery](../design/organization-issuer-discovery.md) and
[filed identity](../design/organization-filed-identity.md).

## What the existing automation actually demonstrated

Fresh offline CLI runs used the retained organization capture with both proposal
policies and the two person-discovery captures. They do not test today's live APIs.

- The organization capture has 20 planned queries: five usable observations,
  one HTTP 429, and fourteen unattempted searches after capture stopped. These are
  **not fifteen matching failures**. In particular, no search result exists for
  the 340B Health query in this capture.
- On the same evidence, default v1 proposes neither reviewed positive; opt-in v2
  proposes both and neither of the two annotated negative alternatives. Eighteen
  queries are unreviewed. These motivating examples are not a held-out benchmark.
- Person discovery finds corresponding human-item names for Chambers and Duffield,
  but the inspected observations do not supply corresponding employer endpoints.
  Broader searches preserve namesakes rather than resolving them. Both assessment
  outputs retain false identity, employment, financial and graph approval flags.
- The existing company-page CLI successfully extracts the retained 1st Source
  announcement as text/metadata with evidence locations. A human still interprets
  its executive and parent-company claims. No LLM or site-specific selector ran.

Eight focused Go packages passed with the ordinary project compiler container:

```sh
go test -mod=readonly -buildvcs=false -count=1 \
  ./internal/source/wikimedia ./internal/source/gleif \
  ./internal/source/sec ./internal/source/companypage \
  ./internal/audit/personaffiliation ./internal/audit/organizationresolution \
  ./internal/calculation/organizationresolution ./internal/calculation/personaffiliation
```

The CLI was built normally and ran `evaluate-organizations` for v1/v2,
`assess-affiliation-candidates` for discovery-v1/v2, and `extract-company-page`.
Inputs and invocation contracts remain in the existing
[organization evaluation](../design/organization-evaluation.md),
[affiliation discovery](../design/affiliation-discovery.md) and
[company-page extraction](../design/company-page-evidence.md) documents.
This is focused verification, not a full Go suite or production acceptance gate.

## One future legislation example, starting in our data

The retained FEC fact identifies `C00619601` as
`340B HEALTH POLITICAL ACTION COMMITTEE (340B HEALTH PAC)` and reports connected
organization `340B HEALTH`. The following manual review tests how contextual
relationships could join this existing seed. It is not a completed money path.

| Relationship | Evidence reviewed | Boundary |
|---|---|---|
| PAC → reported connected organization | The exact FEC committee fact above | The organization string is explicit; it is not proof of every external identity match or a payment. |
| Organization → stated bill position | [340B Health's May 28, 2024 statement](https://www.340bhealth.org/newsroom/statement-on-new-federal-legislation-to-restrict-340b-hospital-eligibility/) supports the 340B PATIENTS Act, H.R.7635 | This records the speaker's position, not independent proof of who benefits, a lobbying filing or every member organization's position. |
| Bill → sponsor | [118th Congress H.R.7635](https://www.congress.gov/bill/118th-congress/house-bill/7635) identifies Doris O. Matsui as sponsor and March 12, 2024 as introduction date | Sponsor is an official role, not evidence of authorship for a donor or receipt of payment. |
| Bill → actions | [Official action history](https://www.congress.gov/bill/118th-congress/house-bill/7635/all-actions) lists introduction and committee/subcommittee referrals | No roll-call vote appears in the examined action history. Do not invent a vote or interpret missing votes as opposition. |
| Candidate/member crosswalk and campaign money | Not established in this review | A FEC candidate identity, a legislative identity and actual selected financial facts must be joined separately. No donation amount or PAC-to-sponsor path is asserted. |

No LDA filing was retrieved for this example. An organization's public statement is
not evidence that it contacted a particular official. Bill identity includes its
Congress and number; a later text assessment must identify the
[introduced text version](https://www.congress.gov/bill/118th-congress/house-bill/7635/text)
or another exact version, not attach one timeless meaning to every version.

**Design consequence:** explicit organizational positions can supply useful
legislative context before an LLM-based beneficiary model exists. Keep declared
support/opposition, inferred effects and observed official actions distinct.
Donations must not be the evidence used both to infer benefit and to demonstrate
alignment with that inferred benefit. This follows the existing
[legislative-influence boundary](../design/legislative-influence.md), not a new
legislative ingestion plan. Neither corporate membership nor parentage automatically
inherits another entity's position or donation amounts.

## Legacy comparison

Python demonstrated that enrichment can supply useful context, but its recorded
resolver hit rates were not measured identity accuracy. The
[legacy validation record](../validation.md#what-we-havent-validated) documents
wrong namesakes and unresolved corporate-family issues.
[The whale resolver](../../src/rag/whale_resolver.py) also chooses a primary company
and treats several organization-to-person properties as direct person-to-company
properties. Those are not rules to preserve. The retained examples above require
multiple roles, correct source direction and explicit time, not just better search.

## Recommended next implementation, for review

Build one source-relationship read path using the existing
[evidence model](../design/evidence-model.md#source-assertion) and
[structured-role reader](../design/wikidata-role-extraction.md):

1. Given a source-qualified entity ID and retained source bodies, return its
   reported relationships with source statement, direction, endpoint, qualifiers
   and observation time. Reuse existing types and readers, not a new ontology.
2. Add the missing structured parent/child properties to that same read path.
   Preserve their broad meanings, multiple parents and absent endpoint details;
   do not reinterpret them as complete legal ownership.
3. Prove the query with the retained Cisco role, inverse Honda parent/child
   statements, distinct board terms and Workday founder statements already in the
   exploration. These are fixtures, never runtime exceptions. Keep unsupported
   dates explicit rather than inventing exact intervals.
4. Keep FEC appearances, reported employers and identity candidates beside the
   relationship evidence. Do not connect a donor to a QID merely to complete a path.
   Context relationships must leave funding queries and all amounts unchanged.

**Finish line:** ordinary code and tests can answer “what does this source report
about this entity's roles and related organizations, and when?” from the saved
inputs, with source drilldown and explicit gaps. This adds useful relationship
querying, not another discovery backend. It need not introduce an Arango publication,
Dagster job, new service, crawling, recovery machinery or an identity scoring system.

After that small slice, the specific unresolved work is FEC-person identity and
transaction-time affiliation binding. Test any proposed matching or prose-extraction
rule separately against namesakes and job changes before accepting it. Current
company pages are useful evidence but cannot silently supply historical validity.
Population-scale discovery, relationship graph publication, legislation ingestion
and terminal-dollar policy remain separate work, not hidden scope in this proposal.
