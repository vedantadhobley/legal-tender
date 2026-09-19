# Relationship-data exploration

Status: exploration with one implemented read path, 2026-09-16. The user wants person–organization,
organization–organization and person–person relationships as useful data in their
own right. The user approved the bounded
[relationship query](./relationship-query.md) after reviewing the evidence comparison.
The broader graph and acceptance design remain exploratory. No source crawler,
LLM call, graph write, donor match or financial policy follows from this approval.

Use the existing [evidence model](./evidence-model.md#source-assertion). Do not create
another provenance framework. Corporate relationships need not wait for a resolved
FEC donor, a terminal-source policy or a production recovery system.

The [bounded evidence comparison](../audit/relationship-evidence-comparison-2026-09-16.md)
now tests this direction against retained FEC organization/person records, existing
enrichment tools and one future legislative example. Its recommended structured
relationship read path is now implemented; it does not approve donor matches or
start a new pipeline.

## What the retained data already shows

The small [retained Wikidata response](../../tests/fixtures/person-affiliation/wikidata-interactive-1.json)
contains three selected items. Read-only inspection on 2026-09-16 found:

| Source property | Statement occurrences | With reference objects | With start/end/point-in-time qualifiers |
|---|---:|---:|---:|
| Employer, P108 | 2 | 1 | 0 |
| Founder, P112 | 4 | 2 | 0 |
| CEO, P169 | 6 | 5 | 6 |
| Board member, P3320 | 13 | 13 | 13 |
| Owned by / owner of, P127/P1830 | 6 | 2 | 2 |
| Child organization or unit, P355 | 16 | 2 | 10 |

These are occurrence counts in this diagnostic response, not unique real-world
relationships or representative coverage. Having a reference object does not mean
we fetched or verified that reference. Having a time qualifier does not mean the
relationship has complete dates or is current. Missing references/dates remain
properties of the evidence, not grounds for silently deleting it or asserting truth.

The original [role reader](./wikidata-role-extraction.md) still extracts the first five
groups. P355 was raw and uninterpreted during this inspection; the subsequently
implemented relationship query now adds P355/P749 without changing the old reader's scope.
This response has no P749 parent statements; the follow-up below supplies examples
from another already saved response. The existing GLEIF reader preserves registry identity records;
it does not implement a parent-relationship ingestion pipeline.

Three concrete walkthroughs are already possible without fetching another page:

1. The Cisco item reports John Chambers as CEO with start year 1995 and end year
   2015. We can represent the reported role and its year precision without asserting
   a role today or identifying a particular FEC appearance as this person.
2. The same item has a P355 statement pointing to `Q3241626` with start month
   `2003-03` and end month `2013-03`, and no reference object. Preserve the target ID,
   broad source predicate, month precision and citation gap. Do not turn this into
   a currently verified, wholly owned legal subsidiary.
3. The Workday item reports two founder targets. A query can return that both are
   listed as founders of this organization, supported by those two statements.
   That is a derived shared-organization connection, not a separately reported
   personal relationship or proof they served together at a known time.

Missing target-item bodies do not erase these statements. Source-qualified IDs can
identify their endpoints while labels, entity types and independent corroboration
remain unknown. Do not invent labels or silently combine snapshots into one current
profile. These examples are review evidence, not named runtime exceptions.

### Follow-up walkthrough: inverse links, multiple parents and nonoverlapping terms

Read-only inspection of the existing discovery captures on 2026-09-16 adds three
concrete cases. No source was fetched and no runtime rule was implemented.

**Opposite source directions can describe the same relationship.** The
[retained organization response](../../tests/fixtures/person-affiliation/discovery-v2/70550b265085b63444770915cf00c183292bc32856da77e411186b1f3b6a4f3f.body)
has a P749 statement from Honda Canada (`Q1626564`) to American Honda Motor Company
(`Q4744011`), and a P355 statement in the opposite direction between those same IDs.
A proposed parent/child query should interpret the two predicates consistently,
retaining both source statements as support. Reversing the source direction for
that query is an explicit property mapping, not a new third assertion or evidence
of an ownership cycle. The two Wikidata statements are not independently verified
confirmations merely because both orientations are present.

**Multiple parent assertions do not identify a single current direct parent.**
The same Honda Canada item also reports `Q9584` as a parent, and American Honda's
item reports that target too. None of these P749 statements has time qualifiers.
The target item itself is absent from this response. Preserve all assertions and
their references; do not select the first, discard a shortcut edge as redundant,
or guess which assertion means direct versus ultimate parent. This is an ambiguity
example, not proof that the source contradicts itself or that either company is a
candidate employer for the FEC appearance that generated the search.

**Sharing an organization does not establish serving together.** The saved Cisco
P3320 statements describe a term for `Q113119124` from `2017-03` to `2023-02` and
another for `Q5214356` from `2023-10` to `2026-05`, all at month precision. These
reported terms do not overlap. A query can show that both were listed on the same
board at different times; it must not describe these terms as contemporaneous
service. This does not prove the source lists every term or every personal connection.

The existing [registry example](./organization-registry-discovery.md#retained-live-result)
also retains two distinct, corresponding-name LEIs. That is unresolved identity,
not a reason to merge their future relationships. None of these examples establishes
a verified real-world contradiction; a conflict-resolution policy is still unchosen.

The proposed minimum record remains simple: source-qualified endpoints, reported
predicate/direction, original qualifiers (including dates, precision, quantities
and units), source statement/record locator, observation time and explicit missing
evidence. Preserve uninterpreted qualifiers rather than silently normalizing them.
Identity merges, a single current profile, derived person-to-person links and
monetary attribution are separate views or decisions. This is a proposed shape,
not a new schema implementation.

### Ownership can outlast an executive role

The user raised John Chambers's possible continuing Cisco stake. In the
[2017 Cisco proxy's ownership table](https://www.sec.gov/Archives/edgar/data/858877/000119312517319338/d448947ddef14a.htm),
Cisco reported 914,505 shares beneficially owned as of July 29, 2017, less than 1%.
The amount includes 17,000 shares subject to RSUs; retain that definition rather
than treating it as an unrestricted direct-share balance. This is evidence after
the reported CEO term ended, not evidence of his present holding.

The [2025 Cisco proxy](https://www.sec.gov/Archives/edgar/data/858877/000085887725000150/csco-20251027.htm)
does not list Chambers. Its ownership table covers specified directors, officers
and holders of at least 5%, not every shareholder. That omission does not establish
zero ownership. These filings were checked online on 2026-09-16 for this design
question; they are not new retained parser fixtures or an implemented ownership feed.

Proposed query behavior: show the executive term and ownership observation as
separate relationships. Preserve the ownership amount, basis and as-of date; do
not extend that snapshot to today or use the role's end as an ownership end.
Current ownership remains unverified in this review. Neither relationship alone
makes an individual's contribution a corporate payment or proves corporate control.

## Meanings that must stay separate

**Source-reported relationships:** employment, executive office, board service,
founding, ownership and organizational hierarchy. They carry the publisher's
predicate and direction; a display may invert an edge only with that transformation
made explicit. Founder, employee, shareholder and controlling owner are not aliases.

**Identity decisions:** whether two source identities denote the same person or
organization, or whether an FEC appearance denotes that person. Sharing a name does
not merge nodes. A QID or LEI can anchor a source identity without being mandatory
for every company, or proving a match to an unrelated FEC record.

**Derived connections:** common board, employer, founder organization or shared
parent. Return the supporting paths. Shared membership alone does not establish
friendship, coordination or overlapping service. Prefer deriving these connections
at query time initially; do not manufacture a permanent all-pairs person graph.

**Financial interpretation:** whether/how a relationship affects a money view.
Keep this separate from the relationship data. Employment and corporate ownership
are not payment edges, and neither automatically transfers donation amounts.

## Source meanings matter more than a common label

Wikidata's [P355](https://www.wikidata.org/wiki/Property:P355) and
[P749](https://www.wikidata.org/wiki/Property:P749) describe organizational children
and parents, including organizational units. Our inference for the design: preserve
that broad meaning until endpoint and additional evidence justify a narrower legal
subsidiary interpretation. Do not infer 100% ownership from the property alone.

[GLEIF Level 2](https://www.gleif.org/en/lei-data/access-and-use-lei-data/level-2-data-who-owns-whom)
reports direct and ultimate **accounting-consolidating** parents. That is a useful
relationship, not a universal beneficial-owner list or an interchangeable synonym
for every Wikidata parent statement. Keep the source-specific meaning and reporting
exceptions when that adapter is considered. These definitions were checked on
2026-09-16; no new registry data was acquired.

Structured statements are the starting point for this exploration. Company pages
remain optional supporting evidence. Their [HTML reader](./company-page-evidence.md)
does not classify prose roles. If structured coverage later justifies an LLM trial,
test extraction on saved text with supporting excerpts and counterexamples first;
model output must not independently approve identities or relationship truth.

## Proposed behavior to review before code

- Store a source claim even when donor identity, endpoint type or historical
  validity is unresolved. A missing donor match must not block unrelated
  organizational context.
- Let investigators distinguish reported claims, conflicting claims and separately
  assessed relationships. Exact labels/acceptance rules remain to be reviewed;
  do not add an unexplained confidence score or treat stored claims as verified.
- Keep asserted time and observation time separate. Unknown ends do not prove a
  role continues today. Date-window queries must expose unknown/coarse validity;
  no FEC-cycle partition should become a universal relationship interval.
- Preserve distinct source assertions and contradictory periods. Several copies of
  one assertion do not become independent confirmations. A later missing statement
  is an observation change, not automatically a relationship end date.
- Fetch missing endpoint details only through a bounded, explicit expansion rule
  if needed. Do not crawl the entire connected knowledge graph by default.
- Keep relationship queries distinct from eligible money-flow queries. Adding a
  context edge must not make it traversable as a payment or change totals.

The walkthroughs and bounded comparison informed the implemented read-only query.
Inverse orientation, ambiguous parent/identity evidence and
noncontemporaneous board terms now have real retained examples. The current sample
does not validate population coverage or conflict handling; do not manufacture a
contradiction from multiple parents, missing dates or different roles. Extend the
read-only sample only for a specific remaining identity or acceptance question.
Physical Arango collections, assertion vertices versus
direct edges, source refresh breadth and new APIs are not settled here.

## Development discipline and cleanup status

Verified against the working tree on 2026-09-16:

- The nine files for the unused [recovery input-copy feature](./funding-recovery-retention.md)
  and its fixture harness are absent. Its command and Make binding are absent too.
  The prior cleanup preserved source data, historical outputs and its source archive.
- The read-only inventory, planner, file verifier and historical-stage reviewer
  remain. Current imports are diagnostic CLI paths; the inspected Dagster package
  does not invoke the recovery work.
- The optional two-build/archive comparison tooling remains. `make build` and
  `make check` do not invoke `release-build` or the recovery runner. Docker builds
  still use the shared compiler/dependency wrapper; that is not a full recovery run.

This is a partial, deliberate cleanup—not removal of every recovery/build helper.
No further code or data was deleted during this exploration. Keep dormant tools
parked unless they cause a concrete development burden; re-scope them when an
operational requirement exists rather than restoring the old roadmap automatically.

**Keep now:** ordinary module locks, retained source examples and locators, data
integrity checks, tests, explainable rules and existing publication safeguards.
These make data work reviewable; they do not freeze evolving designs or dependencies.

**Defer:** new archival/checkpoint machinery, exact executable-rebuild campaigns,
full recovery acceptance and production hardening. No fresh release-build gate,
archive or repeated full-corpus run is required to discuss or prototype a relationship.
Do not remove existing checksum/schema checks from publishers as a shortcut.

The [build guide](../go-build.md) describes the ordinary development loop and the
separate opt-in release workflow. The initial exploration changed documentation
only; the later approved read-only query is documented separately above.
