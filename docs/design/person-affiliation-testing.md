# Person-to-organization affiliation rule tests

Status: bounded Go screening evaluator and synthetic contract tests implemented,
2026-09-15. The user requested testing namesakes, executive/employee distinctions,
job changes, multiple affiliations and the terminal-money boundary. These are
provisional screening rules, not accepted person resolution or graph publication.
The subsequent [retained-source corpus](./person-affiliation-corpus.md) now tests
real appearances against reviewed role annotations, with matching and temporal
gaps preserved. It is not an automated person/role source adapter.
The separate [structured-role reader](./wikidata-role-extraction.md) now extracts
explicit Wikidata statements automatically. The [exploratory discovery slice](./affiliation-discovery.md)
now supplies page-linked candidates; an accepted screening/publication bridge
remains unimplemented.
The bounded interactive-body comparison passes with explicit
historical-role agreement and discovery gaps; these screening rules are unchanged.

The later [binding diagnostic](./person-binding-diagnostic.md) now compares parsed
claims with source appearances and handles coarse date precision separately. It
does not feed invented day bounds into this evaluator or change its v1 outcomes.
An accepted identity/affiliation publication bridge remains unimplemented.
The additive [evidence evaluator](./person-affiliation-acceptance.md#implemented-scope)
now combines explained employer-name proposals with day-level timelines, explicit
denials and opt-in continuity hypotheses. This original v1 screen is unchanged;
neither evaluator accepts donor identities or publishes affiliations.

## What runs

`internal/calculation/personaffiliation.Assess` takes one source-qualified
[reported receipt appearance](./reported-identity-assertions.md) and a supplied
set of typed external person/organization role claims. Each claim has its source
digest and locator, source-qualified person and organization IDs, reported names,
role, explicit validity dates or an as-of date, and any source issue.

The function compares evidence in memory. It does not load or authenticate source
bytes, select candidates from the whole population, fetch SEC/Wikimedia records,
infer role types from prose, reconcile different identifier namespaces, or verify
that the source really supports the supplied role/dates/IDs. Positive test inputs
are invented typed assertions. No live person or corporate affiliation was resolved.

There is no CLI, Dagster asset, graph importer, persistent index or new dependency.
The existing organization name policies and all money calculations are unchanged.

## Executable rule table

| Evidence | Screening behavior |
|---|---|
| Name alone, including an exact name | No employer-context candidate selected |
| Format-normalized name and reported employer match supplied role-record names | Retain that external person as a candidate, not a resolved donor |
| Several distinct matching person IDs | Ambiguous; no positive leadership screening result |
| Several claim occurrences for the same source-qualified person ID | Preserve all occurrences; they do not become independent people or votes |
| Executive, board director, or controlling-owner claim | Eligible role category for the provisional leadership/control screen |
| Employee, founder alone, owner without control evidence, unknown role | Keep separate states; do not promote to leadership/control |
| Valid role period covering the reported contribution day | In-period evidence; endpoints are inclusive |
| As-of role observation | Supports that day only; other days remain unknown |
| Missing role boundary | No lifetime assumption; known bounds may exclude a day but cannot establish the missing side |
| Multiple dated roles on the same candidate person ID | Preserve separate organization proposals, not one primary company |
| Missing/invalid contribution date | Unknown/unusable time; never fill it from the FEC source cycle |
| A role/source issue | Preserve the issue; it cannot provide a usable role or the sole employer anchor for another affiliation |

The name rule reuses format-only `Normalize`: no spelling repair, initial expansion,
surname reordering, legal-suffix removal, model score or proper-name exception.
Both names must be nonblank after normalization. A match describes only the supplied
candidate set, not uniqueness among all people. An expired or unusable role cannot
erase an otherwise matching namesake and manufacture an unambiguous identity.

Reported occupation is retained verbatim, including missing/empty values, but is
not interpreted or cross-checked in this slice. A reported `CEO` string cannot
create an external person or role. Conversely, this test evaluator is not a
complete detector of occupation/role contradictions. That remains part of the
real-source identity/role contract before any publication decision.

FEC date-only and strict zone-free timestamp forms are compared by their reported
calendar day, with the original text retained. Invalid dates/times, timezone-bearing
forms outside this profile and arbitrary trailing text stay unusable. External
role dates must be explicit ISO dates. Retrieval/filing dates must not be passed
as role-validity bounds. Mixed as-of and interval claims fail validation.

## What a positive result means

`screening_match_not_identity_approval` requires a single name/employer candidate,
a usable employer anchor, a same-name role claim on that source-qualified person,
a qualifying role category and supported day-level validity. Another affiliation
on that same external person may qualify without matching the reported employer;
the output makes that distinction explicit. It remains a proposal about a
candidate, not permission to attach the donor to every organization in that profile.

Identity resolution, graph publication, terminal eligibility and financial
attribution flags remain false in every result. This is not a scored probability,
a production leadership classifier or a new definition of a terminal source.
The [terminal assessment](./terminal-source-assessment.md) and
[cycle/window boundary](./cycle-calculation-windows.md) remain unchanged.

All source appearances and role occurrences remain distinct. There is no amount,
donation threshold, allocation weight or primary-company field in the screening
input/decision model. Multiple affiliations are overlapping context; this slice
does not implement or claim to validate organization dollar rollups.

## Tests and verification

The tests exercise the production pure evaluator, not a duplicate resolver hidden
inside test code. They cover:

- Namesakes at the same/different employers, differing ID namespaces, repeated
  source records, missing names/employers, spelling and parent/bank counterexamples.
- Each role category, unknown/unsupported roles, and preservation of reported
  occupation without treating it as verified executive status.
- Job changes, inclusive date boundaries, unknown/open dates, as-of observations,
  invalid FEC date forms and independence from source-cycle metadata.
- Parallel corporate roles without a primary-company choice, source-appearance
  conservation, shuffled-input byte-equivalent results and no input mutation.
- Malformed/unbound or duplicated claim references, invalid role periods and
  failed-source rivals/anchors that must not manufacture usable evidence.

An additional test calls the actual funding `PathQuery.Validate` boundary. Ordinary
receipt-to-candidate queries pass; person-affiliation entry families, candidate
endings and organization targets fail. This proves the existing funding API does
not accept these associations as funding paths. It does not prove hypothetical
future graph integration or a still-unimplemented dollar allocation algorithm.

Run the focused gate with:

```sh
go test ./internal/calculation/personaffiliation ./internal/projection/arango/fundinggeneration
```

The repository `make test` includes both packages. Full Go regression, static
analysis and focused race checks also pass. There was no source download, full-data
scan, real identity merge, graph mutation or financial-policy change in this gate.

## Next gate

The [retained-source replay](./person-affiliation-corpus.md), source-role extraction
and separate binding diagnostic now pass. The diagnostic documents the actual
name, private-employer and historical evidence gaps rather than changing this
screen's baseline. Identity corroboration, occupation conflicts, real
namesake/employee coverage and incomplete discovery remain open before publication.
Reuse organization evidence only where it identifies the same entity; do not
create a per-person exception list.

The tested role whitelist is provisional screening behavior. Broader owner/founder
views, real accuracy/coverage, person identity acceptance and attribution through
pooled committee funds remain separate decisions, not consequences of these tests.
