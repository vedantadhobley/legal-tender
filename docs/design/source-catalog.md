# Target source catalog and ingestion order

> **Status:** Design inventory. Priority states choose implementation order,
> not permanent product scope. Exact field mappings require individual
> [source contracts](./source-contracts.md).

## Selection rule

A source enters the target system when it answers an accepted investigative
question with evidence that a higher-priority source does not already supply.
Each fact family has one authoritative acquisition path. Prefer an official
bulk snapshot when it supplies the required complete dataset. An API or
overlapping file may validate that source but never patch it. If no adequate
bulk source exists, accepting one API as the sole authority requires an
explicit source decision; otherwise the fact family remains out of scope.

Priority means:

| Priority | Meaning |
|---|---|
| `P0` | Establishes the first source-to-answer slice and parser boundary. |
| `P1` | Required for disclosed committee money flow and outside spending. |
| `P2` | Completes major federal campaign-finance or lobbying questions. |
| `P3` | Adds official legislative or organization context. |
| `later` | Valuable adjacent evidence with its own scope and semantics. |

## What exists locally

The legacy raw store contains four cycle partitions (`2020`, `2022`, `2024`,
and `2026`) for:

- FEC candidate master (`cn`), committee master (`cm`), and
  candidate-committee linkage (`ccl`).
- Candidate summary (`weball`), current House/Senate campaign summary
  (`webl`), and PAC summary (`webk`).
- Classic individual contributions (`indiv`), committee-to-candidate
  contributions and independent expenditures (`pas2`), and other
  committee-to-committee transactions (`oth`).
- A secondary `congress-legislators` current-member YAML snapshot.

The raw store also contains downloaded headers for those FEC sources and an
operating-expenditure header, but no operating-expenditure archives. The Go
rewrite store now retains processed Schedule A artifacts and four verified
target-cycle extracts, the all-history processed Schedule E authority, and the
processed Schedule B archive in retained v3 and refreshed v4 source releases.
The earlier lossless 2024 B fact set is also published; active v4 additionally
has four release-bound committee-summary fact sets. The store still lacks a production raw `.fec` corpus, an
LDA corpus, and official congressional source archives. The original point-
in-time inventory is in the
[source-boundary audit](../audit/source-boundary-2026-08-27.md).

These local archives are valuable fixtures and legacy comparison inputs. Their
presence does not choose the target canonical source.

## Campaign-finance foundation

| Priority | Source | Grain and role | Target use |
|---|---|---|---|
| `P0` | FEC candidate master (`cn`) | One published candidate profile record per cycle. | Candidate source identity and versioned assertions. |
| `P0` | FEC committee master (`cm`) | One published committee profile record per cycle. | Committee identity, type, designation, connected-organization text, and assertions. |
| `P0` | FEC candidate-committee linkage (`ccl`) | One published candidate-to-committee linkage record. | Evidence for cycle-specific authorized relationships. |
| `P0` | FEC processed Schedule A dump | Processed itemized receipt row in the FEC's then-current all-history snapshot. | Canonical recurring detailed-receipt source. |
| `P0` | FEC all-candidate summary (`weball`) | One summary record for each candidate with financial activity during the period. | Publisher total facts and reconciliation; never transaction replacement. |
| `P0` | FEC House/Senate current campaigns (`webl`) | One published campaign summary record. | Distinct, timelier summary evidence for applicable campaigns. |
| `P1` | FEC processed Schedule B dump | Processed itemized disbursement row. | Outgoing committee payments, refunds, transfers, recipient evidence, and the second side of committee flows. |
| `P1` | FEC processed Schedule E dump | Processed independent-expenditure row. | Accepted source authority for support/oppose activity; strict parsing, release-v2 membership, selected-cycle facts, four effective spender-candidate-stance calculations, plus a readiness-bundled and automated 2024 ArangoDB projection pass. |
| `P1` | FEC committee history dump | Historical committee profile state. | Time-correct committee identity and metadata across cycles. |
| `P1` | FEC committee financial-summary CSV | Cycle-scoped financial assertion occurrences; candidate references can repeat identical committee financial fields. | Accepted summary source for opening cash, explicit unitemized individual totals, and coverage; complete v4 publication/readback passes. Not a complete cash denominator or report history. |

The FEC distributes Schedule A, B, E, and committee history in PostgreSQL
dumps covering 1975 onward. PostgreSQL is the delivery format, not the Legal
Tender domain database. Schedule A and the other required bulk fact families
publish through the accepted
[coordinated release strategy](./fec-release-strategy.md). The initial path is
bulk-only and has no OpenFEC API fallback.

The [committee-summary source review](./committee-summary-source.md) preserves
all modern CSV fields and repeated occurrences. Classic `webk` remains comparison
evidence, not the new summary authority. The selected CSV has no report version
or account identifier; those funding requirements remain unresolved.
The [report-metadata qualification](../audit/receipt-report-metadata-2026-09-10.md)
found bulk period-summary CSVs without filing/version identity in their inspected
headers. Processed OpenFEC filing/report endpoints supply useful metadata but
retain status/type disagreements and do not prove financial replacement. A
separate metadata API source is now accepted as the direction. Its
[Go local reader](./report-metadata-reader.md) verifies saved pages and preserves
endpoint assertions. The [bounded Go fetcher](./report-metadata-capture.md) adds
manual HTTP capture, not recurring activation or complete history. The
[coverage requirements](./receipt-report-coverage.md) define the next decision
and capture boundaries. Bounded original-file audits remain comparison evidence,
not a second recurring transaction ledger or an approved API fallback.
The [electronic cover reader and field binding](./report-field-binding.md) now
qualify retained 8.4 F3/F3X covers against exact observed-chain candidates.
This bounded local reader does not activate a recurring original-filing source.

### Why Schedule A and B both matter for money flow

Committee receipts show who a committee reported receiving money from.
Disbursements show what the originating committee reported sending. They are
separate disclosure facts and may differ in time, identity, categorization, or
coverage. The graph should retain both sides and a versioned reconciliation
relationship; it should not collapse matching-looking rows into one fact.

The complete 2024 classic audit proved that every `pas2` `SUB_ID` appears
exactly once in `oth`. `pas2` is therefore candidate-context and comparison
evidence over selected `oth` occurrences, not a second amount ledger. Classic
`oth` remains a useful cross-check and possible low-cost backfill input until a
real-corpus Schedule B comparison establishes sender-side historical coverage.
It is not automatically canonical because the legacy code used it.

### Independent expenditures require a distinct calculation

The classic 24/48-hour file and processed Schedule E are related but do not
have identical effective-total semantics. The FEC warns that the 24/48-hour
file retains original and amended reports. The current Schedule E relation has
action states and repeated filer-plus-transaction keys. The accepted effective
calculation now uses the publisher's processed regular-report view, excludes
memo X, keeps repeated keys, preserves exact signed cents, and separates
unrouteable and notice evidence. Its 2024 graph projection preserves those
results without treating them as candidate receipts.

The 2026-08-31 corpus audit accepted Schedule E as the occurrence authority:
it is a 43.38 MB weekly all-history dump with required payee, purpose, time,
and lineage fields. In the same-day 2026 comparison, classic `pas2` lost
fractional dollars on 9,367 of 14,732 exact shared IE rows. `pas2` cannot serve
as the IE amount authority. See the
[classic-flow and Schedule E audit](../audit/fec-classic-flow-and-schedule-e-2026-08-31.md).
The resulting calculation and graph boundaries are recorded in the
[effective-calculation audit](../audit/effective-independent-expenditures-2026-08-31.md)
and [projection design](./arango-independent-expenditure-projection.md).

The [FEC money and time mapping](./fec-money-semantics.md) records the distinct
threshold, estimate, valuation, debt, amendment, and temporal rules that each
source contract must implement.

## Campaign-finance completion

| Priority | Source | Question added | Important boundary |
|---|---|---|---|
| `P2` | FEC Schedule C loans | Candidate and committee loans and repayments. | A loan balance is not an ordinary receipt total. |
| `P2` | FEC Schedule D debts | Debts owed by or to committees. | A balance is a reported state, not a cash transaction. |
| `P2` | Historical raw electronic and paper `.fec` product | Separately labeled filing and amendment history, including paper sources absent from electronic feeds. | Deferred from the processed view; requires broader report-chain, format-history, freshness, and paper-filing contracts. |
| `P2` | Electioneering communications | Communications naming candidates. | Separate from independent expenditures and candidate receipts. |
| `P2` | Communication costs | Corporation/labor communication disbursements. | Separate activity type and reporting rules. |
| `P2` | Party-coordinated expenditures | Party spending coordinated with candidates. | Not an independent expenditure and not a candidate receipt. |
| `P2` | Lobbyist/registrant committee and bundled-contribution filings | PAC control and reported bundling. | FEC disclosure, distinct from LDA lobbying amounts and LD-203 reports. |
| `P2` | Operating expenditures and allocated disbursements | Committee operational and allocation context. | Useful for complete outflow accounting, not a terminal-source edge by itself. |

Schedule C is required before the product can claim complete loan components;
Schedule D is required for reported liability views. Both, with the applicable
receipt and disbursement schedules, are required before a complete net-cash
accounting claim. Until then, the first slice remains labeled as the
implemented receipt component plus separate publisher summaries.

## Federal lobbying

Lobbying is a later legislative-influence evidence domain. It is not another
funding channel and does not become money received by a candidate or personal
income to an officeholder.

The current official source is the LDA.gov REST API; no current all-report bulk
dump exists. The complete acquisition, update, amendment, document, and
partition proposal is in the
[federal lobbying source design](./lobbying-source-ingestion.md).
Lobbying remains outside the initial campaign-money implementation. Before its
legislative-influence phase, Legal Tender must explicitly accept the LDA API as
the sole authority for these fact families or identify an adequate official
bulk path.

| Priority | LDA.gov source | Grain and role | Graph/evidence use |
|---|---|---|---|
| `P2` | Filings | One LD-1/LD-2 filing version with client, registrant, amounts, activities, lobbyists, government entities, affiliated organizations, and foreign entities. | Filing-centered facts; client-to-registrant work, activity, issue, and government-entity relationships. |
| `P2` | Contribution reports | One LD-203 filing with zero-report state, PAC names, and contribution items. | Contribution-report facts and evidence links to resolved FEC records. |
| `P2` | Clients | Source-scoped LDA client profile. | Stable LDA identities and organization-resolution inputs. |
| `P2` | Registrants | Source-scoped registrant profile with House registrant ID. | Stable LDA identities and registration context. |
| `P2` | Lobbyists | Source-scoped lobbyist profile. | Person-resolution input; never merge by name alone. |
| `P2` | Filing, issue, government-entity, and contribution-type constants | Publisher code lists. | Pinned code evidence and display meanings. |
| `P2` | Printable filing documents | Exact LD-1, LD-2, and LD-203 HTML/PDF bytes plus form controls and as-filed text. | Historical names, amount bands, form grouping, signatures, and cross-representation evidence. |

### First lobbying slice

The useful minimum is not “download quarterly totals.” It is:

1. Capture filings, contribution reports, identities, publisher constants,
   and exact printable documents as separate immutable snapshots.
2. Preserve registration, quarterly report, termination, and amendment filing
   types rather than summing every returned filing.
3. Normalize activities at the filing-section grain so issue text, lobbyists,
   and government entities retain their shared context.
4. Preserve `income` and `expenses` as different reported meanings. Do not add
   both when one represents an outside registrant and the other an in-house
   registrant. Preserve reported-point, threshold-band, blank, not-applicable,
   invalid, and unobserved amount states.
5. Resolve clients, registrants, and lobbyists only through versioned identity
   evidence.
6. Reconcile each LD-203 contribution item to possible FEC facts. A confirmed
   match links two disclosures of the same activity; it does not create a
   second contribution.

LDA contribution items contain contributor, payee, honoree, type, amount, and
date. They are high-value evidence, but they generally lack FEC transaction
IDs. Name, amount, and date similarity produces a candidate match, not an
automatic identity.

LDA filings normally identify congressional chambers and federal agencies, not
an individual contacted office. Specific-issue text may identify legislation
or other policy detail. Legal Tender therefore creates no direct
client-to-politician lobbying edge unless a source explicitly supplies that
evidence. Congressional facts can create separately labeled contextual paths.

The source contract must also implement LDA.gov's current rate limits and
required retrieval-date/disclaimer language.

## Congressional context

| Priority | Official source | Target use | Boundary |
|---|---|---|---|
| `P3` | Congress.gov API | Bioguide member identities, bills, sponsors, committees attached to legislation, actions, subjects, and House votes where distributed. | Legislative context, not evidence that lobbying caused an action. |
| `P3` | House Clerk official data | House membership, committee assignments, and roll-call records where Congress.gov lacks the needed grain. | House-only authority and its own time model. |
| `P3` | Senate official sources | Senate membership, committee assignments, and roll-call records. | Separate source contract; do not pretend a House-vote endpoint covers Senate. |

The legacy Congress client needs replacement. It sends unsupported query
shapes for some list endpoints and calls a generic `roll-call-vote` endpoint
that is not in the current Congress.gov API. The locally cached
`unitedstates/congress-legislators` YAML is a useful secondary crosswalk, not
the canonical official source.

Legislative data can support these contextual paths:

```text
LDA activity --[mentions text resolved as]--> bill
bill --[officially sponsored by]------------> member
bill --[officially referred to]-------------> committee
member --[served on, time-bounded]----------> committee
member --[cast official vote on]------------> bill or legislative action
```

The first edge may be derived from free text and must expose extraction and
resolution confidence. The remaining edges need official congressional
records. None is a monetary edge. An official sponsor is not automatically the
bill's author and is never the recipient of an LD-2 amount merely because the
filing references the bill.

Beneficiary or burden analysis is a later versioned calculation over bill
text, official summaries, policy scope, and organization or industry evidence.
Donations and lobbying relationships may nominate or corroborate a result but
cannot be its sole evidence. The complete boundary is the
[legislative-influence design](./legislative-influence.md).

## Organization and corporate-family resolution

| Priority | Source | Target use | Boundary |
|---|---|---|---|
| `P3` | GLEIF Golden Copy/API | Legal entity identities, names, addresses, LEIs, direct/ultimate parent relationships, and mapped identifiers. | Strong corporate evidence where LEI coverage exists; not universal coverage. |
| `P3` | SEC EDGAR submissions/company data | Registrant CIK identities, filings, former names, and issuer facts. | Strong public-company evidence; an employer-name match remains unresolved until corroborated. |
| `P3` | Wikidata | Aliases and contextual entity candidates. | Community-curated evidence; never the sole authority for a high-impact merge when stronger sources exist. |
| `P3` | English Wikipedia search | Discover pages and linked Wikidata items for reported organization strings. | A bounded search window, not identity confidence or exhaustive coverage. |
| `P3` | Company-owned leadership and biography pages | Publisher claims about people and roles, including private companies. | Current claims do not identify a donor or prove historical employment; website ownership and person identity require separate binding. |

The [company-page reader](./company-page-evidence.md) now extracts lexical text,
metadata and JSON-LD from explicitly selected retained HTML with exact source spans.
Its real replay passes. The separate opt-in
[prose prototype](./prose-relationship-prototype.md) now emits narrowly recognized
role/name-form candidates; it accepts no assertions. Automatic page discovery,
general prose interpretation and accepted affiliation publication remain open.

The [same-sample supplementary review](../audit/supplementary-affiliation-sources-2026-09-16.md)
retains company prose, a government profile and a historical licensing PDF.
It confirms useful source content outside Wikimedia, not a general person-role
feed. Lexical extraction preserves the evidence, but the later
[out-of-development prose evaluation](../audit/prose-extraction-evaluation-2026-09-16.md)
finds only one of six reviewed roles. Interpretation and automatic page selection
remain gaps. No additional source adapter is implemented.

The [bounded Go capture/replay slice](./organization-resolution.md) now implements
Wikipedia discovery and saved Wikidata evidence. Its source contract remains draft;
corroborated identity decisions, entity graph edges and recurring refresh remain open.
The [structured-role extractor](./wikidata-role-extraction.md) now interprets explicit
Wikidata employment, founder, CEO, board and ownership properties in retained bodies.
It preserves source direction and temporal precision without resolving donors.
The [relationship query](./relationship-query.md) adds a read-only, entity-based
view with parent/child claims from the same retained bodies; no new acquisition,
identity join or production publication is implied.
The bounded interactive comparison passed after earlier background backoff. The
[exploratory affiliation discovery slice](./affiliation-discovery.md) now automates
name/employer searches and linked-item capture; its live replay preserves candidates
and retrieval gaps. Opt-in query variants and offline relevance checks now retain
real namesake/non-company counterexamples; they do not close the company discovery
gaps. Recursive inverse discovery, independent corroboration and
publication remain open.
The [bounded registry corroboration slice](./organization-corroboration.md) now
fetches exact LEIs asserted in saved Wikidata candidates and verifies their GLEIF
records offline. It preserves missing-ID, status, qualifier and historical-name
limits; it does not independently bind FEC text to a legal entity. The separate
[name-search path](./organization-registry-discovery.md) now captures candidates
directly from source-qualified organization names, without a prior QID or LEI.
Its live replay preserves same-name ambiguity and missing coverage. No GLEIF bulk
index, accepted FEC identity binding or corporate-role publication exists yet.

The [independent issuer path](./organization-issuer-discovery.md) adds a strict
SEC `company_tickers.json` bulk adapter and offline name-to-CIK candidates without
Wikipedia or LEI prerequisites. The live directory gate and exact replay now pass;
the public body is retained without private request-contact metadata. This directory
is narrower than all EDGAR registrants; absence is not organization nonexistence.
The [filed registrant check](./organization-filed-identity.md) now captures an
explicit filing and compares tagged registrant name/CIK evidence offline, retaining
exact bytes and context limits. Its live gate passes without identity approval.
SEC submissions acquisition, automatic filing selection and accepted identity
connections remain open.

Organization resolution is shared infrastructure for FEC employer text, FEC
connected-organization text, LDA clients and registrants, and later external
sources. Resolution is not a prerequisite for ingesting those sources. Raw
source identities and facts land first; resolution can improve later without
refetching or rewriting them.

GLEIF publishes legal-entity and parent/child relationship data through both
bulk Golden Copy files and an API. Bulk snapshots should be preferred for the
local entity index; API lookup can support targeted investigation and
validation.

## Later adjacent public sources

| Source | Investigative value | Why later |
|---|---|---|
| DOJ FARA bulk data/API | Foreign principals, registrants, short forms, and registration documents. | Separate legal regime and entity model; explicitly deferred by the product contract. |
| IRS Forms 8871/8872 | Section 527 organization identity, contributions, and expenditures outside some FEC views. | Requires overlap analysis and its own amended-filing rules. |
| IRS Form 990 data | Nonprofit identity, officers, grants, and organization context. | Large scope; financial semantics are not campaign contributions. |
| FCC political/public files | Broadcast political advertising and sponsor context. | Station- and order-document workflow differs from FEC reporting. |
| OGE disclosures | Candidate or official financial interests. | Personal financial disclosure, not campaign money. |
| State campaign-finance and lobbying systems | State/local money and influence. | Dozens of incompatible regimes; separately deferred. |

FARA currently publishes daily bulk CSV/XML and an API, so it is technically
available when product scope promotes it. “Later” reflects semantic and
validation cost, not an inability to acquire the data.

## Implementation order

The source sequence should be:

1. Land the common contract registry, manifest, occurrence, issue, fixture, and
   publication-state machinery.
2. ~~Define `fec.release.v1`, implement Monday metadata-only discovery and
   stable candidate-release selection, and prove `no_change`,
   `source_not_ready`, `update_available`, and invalid outcomes without
   downloading source bodies.~~ The 21-source contract, Go discovery, planner,
   live metadata check, and fixtures have landed.
3. ~~Implement `cn`, `cm`, `ccl`, exact `weball`/`webl` summary contracts, the
   processed Schedule A publication path, one real-cycle publication, and the
   first ArangoDB projection.~~ The coordinated 2024 slice and isolated graph
   probe have landed.
4. Project receipt-side committee flows from Schedule A. ~~Audit Schedule B
   and publish its lossless selected-cycle facts through release-inventory
   v3.~~ Then define effective Schedule B records and outgoing-flow roles
   before sender-side reconciliation. Committee history and committee
   summaries remain separate audits before complete outflow coverage.
5. ~~Add the accepted Schedule E contract to release-inventory v2, publish its
   selected-cycle occurrences and lossless facts, calculate effective IE,
   prove the first graph projection, and gate it through an immutable
   projection-readiness bundle.~~ Implemented without collapsing amendment,
   memo, timing, support/opposition, or source lineage. Dagster now eagerly
   maps exact same-cycle inputs into the bundle and graph.
6. Implement the later legislative-influence slice with official bill,
   sponsor, committee, membership, action, and vote facts; LDA filings,
   activities, LD-203 reports, and documents; and the required organization
   resolution. Keep these facts out of candidate-funding totals.
7. Add bill-reference resolution and independently grounded beneficiary or
   burden inference before combined money/lobbying/legislation analysis.
8. Add Schedule C/D, a separately labeled raw-filing product, and other federal
   activity types by accepted investigative priority.

This order delays graph ontology decisions that depend on misunderstood source
grain, but it does not delay all design. Evidence identities, source facts,
and the candidate/committee/linkage relationships are already defined well
enough to build the parser foundation.

## Primary references

- [FEC bulk data catalog](https://www.fec.gov/data/browse-data/?tab=bulk-data)
- [OpenFEC API documentation](https://api.open.fec.gov/developers/)
- [LDA API root](https://lda.gov/api/v1/)
- [LDA filings endpoint](https://lda.gov/api/v1/filings/)
- [LDA contribution-report endpoint](https://lda.gov/api/v1/contributions/)
- [Congress.gov API](https://api.congress.gov/)
- [GLEIF API and LEI data](https://www.gleif.org/en/lei-data/gleif-api)
- [SEC EDGAR API overview](https://www.sec.gov/file/api-overview)
- [DOJ FARA bulk data](https://efile.fara.gov/ords/fara/f?p=107:21)
- [IRS political-organization disclosures](https://www.irs.gov/charities-non-profits/political-organizations/political-organization-filing-and-disclosure)
