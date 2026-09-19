# Automated FEC appearance enrichment

Implemented 2026-09-16. `pipeline entities enrich-affiliations` reads verified FEC
appearances, generates Wikipedia searches, captures linked Wikidata responses and
returns one evidence report per appearance. The same command replays a saved capture
offline. No person-specific annotation or expected entity ID drives this path.

## Implementation choice

The existing [discovery assessment](./affiliation-discovery.md) already handles
full source names and aliases. The [binding comparison](./person-binding-diagnostic.md)
already compares role dates at their reported precision. The report composes these
rules instead of converting their inputs into the manually supplied structured
names and day-only claims required by the
[v2 review classifier](../audit/person-identity-evidence-v2-2026-09-16.md).

The v2 classifier remains a review experiment. It is not the automatic ingestion
interface. This integration preserves the v2 separation of name evidence, employer
context, role meaning and time without manufacturing name components, aliases,
receipt-day role coverage or source independence. Unknown source dependence remains
an explicit limitation; multiple search hits are not independent corroboration.

## Inputs and selection

The Go calculation accepts verified `Appearance` values and `ReadDiscovery` output.
It binds the source digest, exact occurrence locator, reported name and employer
across both inputs. Duplicate appearances, different source text, unmatched selections
and missing search observations fail before reporting a successful result.

The initial CLI source reader uses the retained corpus's FEC filing, response headers
and pinned 8.4 Schedule A layout. Company HTML, reviewed roles, subject IDs and expected
outcomes are not required. It preserves exact FEC components, occupation and date in
the existing explicitly labeled as-filed view. It projects no address or amount.
This bounded diagnostic source reader does not replace the processed Schedule A
ledger or implement a bulk-donor production selector.

Default selection reuses the corpus's FEC ordinals. With `--sample-size 1..5`, the
reader scans the same filing's supported complete individual Schedule A profile,
groups exact name/employer text pairs for sample diversity and excludes the reviewed
pairs. It sorts SHA-256 hashes of the JSON name/employer pair and selects the first
N, retaining the earliest occurrence of each pair. No donor amount, occupation,
company name or expected search result controls selection.

Sampling reports the eligible/outside-profile occurrence census, excluded reviewed
pairs and distinct remaining input pairs. `population_scanned=false` marks default
ordinal selection, where those population counters have not been calculated.
The raw filing retains every row. Sample grouping does not deduplicate source facts
or define a person. One filing and a small hash sample cannot estimate population
accuracy or coverage.

## Report behavior

Each appearance contains its input state, retrieval state, all applicable search
windows and an inventory of name-corresponding source IDs. Each returned page survives,
including mismatches, unresolved name variants, disambiguation pages, missing items
and unverified types. A candidate lacking an employer or role stays in the report.
An exact match on one candidate does not exclude the other retrieved results.
Page titles, IDs and revisions remain in each search window, including pages with
no usable Wikidata entity.

Source labels and aliases retain the entity ID, revision, body digest and locator.
Each attached relationship keeps its raw source statement, direction, role category,
rank, issues, dates and receipt-date comparison. Endpoint names are included only
when loaded in that response. Statements without a usable page candidate remain in
`roles_without_usable_candidate`. No endpoint or role revision is borrowed from a
different response.

An employer-context inventory means a name-corresponding candidate has a reported
relationship to an endpoint with corresponding employer text. It is not legal-entity
resolution or verified historical employment. Source issues and time states remain
on the relationship. Reported occupation text does not promote employee/founder/owner
claims to executive authority. All identity, graph and financial approval flags remain
false.

Capture retains the existing transport limits and `maxlag=5` policy. Failed or
unattempted requests stay visible; the command emits their diagnostic report and
exits nonzero. An empty successful search is a different state. A successful run
means the bounded responses were parsed, not that discovery is exhaustive.

## Commands

Replay the existing reviewed appearances:

```sh
legal-tender pipeline entities enrich-affiliations \
  --corpus tests/fixtures/person-affiliation \
  --expected-corpus-sha256 7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4 \
  --capture tests/fixtures/person-affiliation/discovery-v1 \
  --expected-capture-sha256 c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c
```

Select additional FEC appearances and capture/report in one invocation:

```sh
legal-tender pipeline entities enrich-affiliations \
  --corpus tests/fixtures/person-affiliation \
  --expected-corpus-sha256 7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4 \
  --sample-size 3 --output NEW_CAPTURE_DIRECTORY \
  --user-agent 'LegalTender/affiliation-evidence (YOUR_PROJECT_CONTACT_URL)'
```

For offline sample replay, keep `--sample-size 3` and replace `--output` and
`--user-agent` with `--capture` and `--expected-capture-sha256`. A selection that
differs from the captured source appearances fails. The default retrieval policy is
v1; `--query-policy reported-name-employer-searches.v2` selects the existing employer
query variants for a live run, subject to the same twenty-search limit. Replay uses
the captured query policy and rejects a live-only `--query-policy` override.

## Measured result

Both existing captures replay through the new command. Chambers's two separate FEC
occurrences each retain two corresponding-name source IDs; Duffield retains one;
the engineer appearance retains no corresponding name. No inspected relationship
has a corresponding reported-employer endpoint. Every alternative page remains
available for inspection. These outcomes agree with the prior parsed-source tests.

The fresh automatic sample selected three additional appearances from 1,581 eligible
occurrences, with 1,394 other/outside-profile records and 549 distinct additional
name/employer pairs after excluding the three reviewed pairs:

| Source appearance | Reported employer | Result |
|---|---|---|
| Richard Reviglio, ordinal 317 | Western Nevada Supply Co | No corresponding name in the captured windows |
| Alton Russell, ordinal 453 | Copaco | No corresponding name in the captured windows |
| Christie Gescheider, ordinal 139 | Moana Nursery | No corresponding name in the captured windows |

The [sample capture](../../tests/fixtures/person-affiliation/discovery-sample-v1/capture.json)
retains nine search windows, fourteen successful HTTP requests, 422,390 response
bytes and 25 returned page occurrences. Its digest is
`79b2f1b49e422831ce6a704b57ae86ea66285aff4f68782d3e206aee840c52d6`.
All three reports abstain; no corporate affiliation is inferred from the employer
query or an unrelated result. Identity accuracy is unmeasured because this sample
has no independently established truth labels and the command accepts no identities.

The automation works on the selected source scope. The measured limitation is
person/employer evidence coverage, especially beyond prominent public figures.
The subsequent [supplementary-source investigation](../audit/supplementary-affiliation-sources-2026-09-16.md)
retains company role/alias prose, a historical licensing record and missing-employer
controls on the same cases. Those manually discovered sources are not consumed by
this command. Existing HTML extraction and the separate
[opt-in prose syntax baseline](./prose-relationship-prototype.md) pass retained
regressions; verified role interpretation and page selection remain separate gaps.
Production source selection, independent
identity corroboration and accepted graph publication remain later work.

## Verification

Tests exercise source joins, aliases, coarse dates, missing-employer rivals,
unassigned relationships, source failures, empty input, response isolation, review
annotation independence, deterministic sampling and offline CLI replay over all
three real captures. All live acquisition uses the existing source adapter.

The full Go test suite, focused race checks for both person-affiliation packages
and the CLI, and `go vet ./...` pass in the memory-capped offline compiler container.
A fresh development binary replays the additional sample with the same measured
scope and unresolved results. Updated documentation links and formatting checks pass.
