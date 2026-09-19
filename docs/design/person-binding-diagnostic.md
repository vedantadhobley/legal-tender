# Person and dated-role binding diagnostic

Implemented as a pure Go function and executable tests, 2026-09-16. The user
approved testing how FEC appearances can connect to people and dated corporate
roles after the [relationship query](./relationship-query.md). This is a diagnostic,
not an accepted identity rule or affiliation publisher.

## What changed

`personaffiliation.AssessBinding` compares one source-qualified FEC appearance with
one verified `wikimedia.ExtractRoles` response. It reuses the source reader's
statements, existing name comparisons and employer-endpoint checks. No reviewed
subject IDs, expected matches or named-person exceptions steer the function.

The result keeps three questions separate:

1. **Name correspondence:** which loaded items have a corresponding English label
   or alias? Person names use existing format-only normalization. No nickname,
   initials, spelling repair or surname reordering is invented. A source-supplied
   alias remains an explicit name observation, not a verified identity.
2. **Employer context:** does a role's loaded organization endpoint have a name
   corresponding to the FEC employer under the existing explained organization
   rules? Keep bank/holding-company distinctions, missing endpoints and source
   issues. This is name context, not legal-entity or employment confirmation.
3. **Reported time:** how does the actual receipt day compare with the role claim's
   time qualifiers? This compares source assertions; it does not establish their
   truth or infer that an undated role is current.

Every name candidate survives even without roles, matching employer context or
usable dates. An expired/deprecated role cannot erase a namesake and manufacture
uniqueness. Each candidate keeps every holder-role occurrence, including duplicate
statements and multiple organizations. There is no primary-company selection.
Candidates are type-unverified; a matching name alone does not establish a person.

The function consumes the source reader's output, not arbitrary serialized claims.
The caller must verify the source bytes. It retains the appearance reference,
body digest, expected item set, item revision and each raw statement/locator.
It does not merge different snapshots or use observation dates as role dates.
Existing discovery and the original day-only screening policy remain unchanged.

## Date comparison rules under test

This diagnostic compares at source precision using the receipt day's corresponding
year/month/day prefix. It never emits invented January 1 or month-end role bounds.
Exact day endpoints reuse the existing inclusive screening convention.

| Source evidence relative to receipt day | Diagnostic result |
|---|---|
| Day before the entire reported start year/month/day | Before reported start precision |
| Day after the entire reported end year/month/day | After reported end precision |
| Two bounds, day safely between their units | Within reported bounds |
| Day in a coarse start/end year or month | Boundary precision unconfirmed |
| One bound missing, day not excluded by the other | Open period unconfirmed |
| No role dates | Role time unknown |
| Exact matching as-of day | On reported as-of day |
| Matching coarse as-of year/month | Within as-of precision, unconfirmed |
| Other as-of period | Different observation period, **not** exclusion of the role |
| Repeated bounds, mixed as-of/period, unsupported time or inverted bounds | Explicit unassessed/unsupported/inconsistent state |

`within_reported_bounds` describes the supplied qualifiers, not verified service
or a positive identity match. Ranks, missing references and other qualifiers remain
visible constraints; this diagnostic does not assess them away. Founder and ordinary
ownership statements retain their original role meanings, not executive/control
classification. Occupation is retained but not interpreted or cross-checked.

The receipt's source cycle does not affect these results. Missing or invalid dates
stay unknown/unusable rather than being filled from a cycle or source capture.

## Results on retained real data

The tests use the [original FEC corpus](./person-affiliation-corpus.md), both saved
discovery captures and the retained interactive Wikidata body. They select source
appearances and parse source claims in code; review annotations do not select
Wikidata candidates or dates.

| Case | Result |
|---|---|
| Two separate John Chambers FEC occurrences | Each retains the businessperson and makeup-artist name candidates in the discovery evidence. Neither capture supplies a corresponding employer endpoint. No identity winner. |
| Chambers's 2023-09-30 receipts versus the Cisco CEO claim | Both are after the reported end year 2015. The undated employer claim stays unknown; it does not inherit the CEO dates. Cisco also differs from the FEC-reported JC2 employer. |
| David Duffield / Ridgeline | A name candidate exists, but no corresponding employer endpoint is available in the inspected structured responses. This is a coverage gap, not employer absence. |
| Reported engineer | No corresponding name candidate in the inspected captures. This is not a verified ordinary-employee classification. |

A separate test compares existing normalization against the **reviewed company-page
annotations**, without pretending those annotations were extracted automatically:

- `JC2VENTURES` and `JC2 Ventures` correspond through the existing letter/number
  boundary rule, and the reviewed John Chambers name corresponds. The CEO and
  founder annotations reuse one sentence, not two independent confirmations. No
  historical role interval is supplied by the current page.
- `RIDGELINE INC` and `Ridgeline` have organization-name correspondence. The FEC
  `David Duffield` and reviewed company-page `Dave Duffield` remain unbridged in
  this comparison; there is no blanket David/Dave substitution.

That is a useful partial result: some gaps are source coverage and dated evidence,
not inadequate string matching. It is not a representative accuracy benchmark or
an accepted real donor identity. The old corpus screening results are preserved.

## Verification and boundary

```sh
go test -v ./internal/calculation/personaffiliation ./internal/audit/personaffiliation \
  -run 'TestBinding|TestRetained.*Binding|TestReviewedCompanyBindingContrast'
```

Tests cover namesakes, missing employers, source aliases versus invented nicknames,
parent/bank distinctions, multiple roles, duplicate/deprecated statements, coarse
and exact dates, open/as-of intervals, source-occurrence conservation, deterministic
replay, input-order independence and review-label independence. The full Go test
suite, `go vet ./...` and race tests for both person-affiliation packages pass in
the existing memory-capped, network-disabled Go build container.

All identity, graph-publication and financial flags stay false. No CLI, dependency,
source fetch, LLM, Dagster asset, graph write or money rule was added. The new
function is callable Go logic; the retained-corpus integration currently runs in
tests, not a scheduled resolution pipeline.

## What remains

The [dated first-party evidence check](../audit/dated-person-role-evidence-2026-09-16.md)
now supplies historical private-company examples and retained-source tests.
Name/employer context improves, but donor identity and continuity from a dated
point to a later receipt remain separate, unaccepted inferences. Review those
acceptance rules before adding another acquisition or publication pipeline.
The [acceptance policy and additive evaluator](./person-affiliation-acceptance.md)
now implement a separate candidate/day-timeline comparison over supplied claims,
including conflict states and optional continuity hypotheses. Identity acceptance
remains unimplemented; this does not change the parsed-role diagnostic rules here.
Keep identity confidence separate from role-at-date support: knowing who a person
is does not prove which role they held on every contribution date.

Company-page role interpretation is still reviewed, not automatic. Source selection
and any future extraction method need evaluation against the retained annotations;
no site-specific crawler or LLM pipeline is implied. Wider name matching alone
cannot supply missing historical evidence, and uncertain context must not silently
become a corporate donation or terminal-dollar assignment.
