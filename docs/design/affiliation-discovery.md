# Exploratory affiliation candidate discovery

The [automated appearance report](./affiliation-enrichment.md) now composes this
planner, capture and replay with verified FEC inputs and receipt-date comparisons.
It offers one-command live enrichment and offline replay with explicit search gaps.

Implemented 2026-09-15. This is still evidence-backed exploration, not an accepted
person-resolution design or a production affiliation graph. The user approved
testing Wikipedia-first discovery while keeping all identity and money decisions
open. The [draft source contract](../../contracts/sources/wikimedia/affiliation-discovery/v1/contract.json)
defines the bounded slice.

## Implemented behavior

The generic Go planner takes source-qualified appearances: exact name/employer
strings, source-body digest and locator. It preserves separate appearances,
including repeated names, nulls and blanks. It validates shape, not authenticity
of an arbitrary hand-written input. The diagnostic CLI obtains its inputs through
the [existing pinned FEC corpus reader](./person-affiliation-corpus.md); no reviewed
role annotation, benchmark QID or expected outcome selects a request.

For each appearance, `reported-name-employer-searches.v1` generates available
person-name, person-name-plus-employer and employer searches. Query rendering
replaces punctuation with spaces and quotes each remaining token separately. It
preserves case, accents, token order and letter/number boundaries. It does not
reorder comma-formatted names, expand initials/nicknames, strip legal suffixes,
correct spelling or interpret occupation. This is an experimental retrieval policy,
not a normalization rule for stored identities. No amount or cycle controls it.

Identical kind/text searches share one request while retaining all contributing
appearance indexes. Source references and query keys determine stable order.
Inputs without searchable text remain present with an explicit state and make no
request. More than twenty appearances or twenty generated searches fails planning;
the planner does not silently take a prefix.

Opt-in `reported-name-employer-searches.v2` retains every v1 query and adds
employer-only proposals: split letter/number boundaries, omit one trailing complete
legal designator, and combine those operations. It reuses the organization matcher's
pure text helpers. The suffix vocabulary is `CORP/CORPORATION`, `INC/INCORPORATED`,
`LTD/LIMITED`, and `LLC`; arbitrary corporate words and dotted abbreviations are not
removed. Empty or number-only variant stems are not queried. Each shared query
retains its appearance indexes and per-appearance derivation rules. Person names
are not expanded. The same twenty-query ceiling applies; v1 remains the default.

Capture reuses the [organization transport and strict response readers](./organization-resolution.md)
under a distinct contract. It searches English Wikipedia, retains every page in
the five-hit window, and requests exactly the linked Wikidata QIDs. Search order
is not confidence; there is no winner selection. Missing page QIDs, missing items,
disambiguation pages, unverified types and observed direct human-type assertions
remain distinct. Neither an observed human assertion nor an exact page title proves
that a candidate is the FEC donor.

The [role extractor](./wikidata-role-extraction.md) reads each verified entity body.
Employer-side searches can expose inverse founder/CEO/board statements, but this
does not implement recursive inverse discovery or arbitrary endpoint fetching.
Each response keeps its own revisions and missing endpoints; items are not silently
merged across captures. Raw statements, ranks, dates and citations remain intact.
Article prose is not fetched or interpreted.

The shared capture path still uses fixed HTTPS origins, no credentials/redirects,
one-second request spacing, thirty-second request timeouts, a five-minute run limit,
four MiB per body and sixty-four MiB per capture. `maxlag=5` remains mandatory here;
there is no interactive switch or fallback. The first transport/status/schema
failure stops network work, preserving that response and later unattempted queries.
A capture manifest marks completed observation recording, not identity acceptance.

## Commands

```sh
legal-tender pipeline entities plan-affiliation-discovery \
  --corpus tests/fixtures/person-affiliation \
  --expected-corpus-sha256 CORPUS_SHA256 > plan.json

legal-tender pipeline entities capture-affiliation-candidates \
  --plan plan.json --expected-plan-sha256 PLAN_SHA256 \
  --output NEW_CAPTURE_DIRECTORY \
  --user-agent 'LegalTender/affiliation-review (YOUR_CONTACT_URL)'

legal-tender pipeline entities replay-affiliation-candidates \
  --capture NEW_CAPTURE_DIRECTORY --expected-capture-sha256 CAPTURE_SHA256

legal-tender pipeline entities assess-affiliation-candidates \
  --capture NEW_CAPTURE_DIRECTORY --expected-capture-sha256 CAPTURE_SHA256
```

Pass `--query-policy reported-name-employer-searches.v2` to the planner to opt in.
Capture and replay derive the policy from the pinned plan, not a separate override.

The corpus command is an exploratory seed adapter, not a full-corpus FEC selector.
Other verified source adapters can call `wikimedia.PlanDiscovery`; the current CLI
does not implement bulk donor selection. Plan/capture inputs are digest-pinned,
query membership is recomputed on read, and outputs retain build/source provenance.
The manifest records the capture build, not the executable performing a later
replay. Result policy/contract versions identify interpretation; production build
acceptance remains a separate gate.

Replay is offline. It verifies manifest/body pins, exact planned URLs, page-derived
item requests and the original transport limits. `capture_usable` means the selected
responses parsed, not that all people or companies were discovered. Exhaustive
discovery, identity, graph-publication and financial flags are always false.

## Offline candidate relevance

`affiliation-candidate-relevance.v1` is a separate Go calculation, exposed by
`assess-affiliation-candidates`. It verifies the capture before assessing it and
does not alter the existing replay format or screening evaluator. It preserves all
appearances, queried windows, rivals, source failures and empty windows. Body pins,
observation indexes, page IDs, item revisions and statement locators bind its output
to source evidence. The assessment records its own executable digest; repeated
assessment with that executable is byte-identical.

Person names require format-only correspondence with a saved English label, alias
or non-disambiguation page title. There is no inferred nickname, middle-initial
omission, surname reordering or spelling correction. Employer names use the existing
explained organization-name proposals. Name matches are observations, not identity
votes. A matching item without a human assertion remains type-unverified; lack of a
human assertion never establishes that another item is a company.

For each candidate, retain every selected role statement naming its QID as holder.
Check the related endpoint's names only when that item exists in the same response.
Missing, unloaded and differently named endpoints remain separate states. Do not
silently combine versions from different responses. Preserve original roles, ranks,
issues, references and date precision, including unusable assertions. Temporal
validity is explicitly `not_assessed`. A founder, owner or generic position statement
does not become employment or executive authority. A query containing an employer
is not an employer assertion. There is no winner, confidence score, accepted
affiliation, graph write or financial calculation.

## Retained live result

On 2026-09-15, the four existing FEC appearances generated nine searches. Sixteen
requests captured 514,338 response bytes, with every request using `maxlag=5`.
No interactive exception or new bulk input was needed. The full
[capture fixtures](../../tests/fixtures/person-affiliation/discovery-v1/capture.json)
include safe selected headers only; no cookies or network-identity headers.

Capture SHA-256:
`c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c`.
Fresh-process offline CLI output is byte-identical to the live result, SHA-256:
`bfd1b2a9fe871a051fbdc71e8c7beda74b1b6cd3c3e4a03c2fa381f94e7c3975`.
This used a development executable, not a new accepted production release.

| Search evidence | Observed outcome, not identity approval |
|---|---|
| John Chambers name | The previously reviewed businessperson item appears second, alongside unrelated and similar-name results. |
| David Duffield name | The expected item appears second; the name/employer search returns that item alone in its bounded window. |
| JC2 employer and name/employer | No pages in either window. The reported `JC2VENTURES` boundary is not repaired by v1. |
| Ridgeline employer | Results include unrelated Honda organizations and the Duffield page, not a verified Ridgeline company item. |
| Reported engineer | Searches ran but returned noisy, unverified candidates. This is no longer unattempted discovery, but still not a verified ordinary employee or a true negative. |

These measurements show working automatic retrieval, not high identity accuracy.
The original corpus and its frozen screening results are unchanged. The earlier
interactive three-item comparison remains separate evidence, not overwritten by
these newer independently timed responses.

## Retained variant experiment

The [separate v2 capture](../../tests/fixtures/person-affiliation/discovery-v2/capture.json)
on 2026-09-15 used the same four verified appearances: fifteen searches, twenty-eight
requests and 1,067,700 response bytes. All requests honored `maxlag=5`; all responses
passed replay. The capture pin is
`5ff30d1c86bd6e4b2b7746f88628057f30d03220a6b27a6853fc8124e04a9c81`.
Fresh-process replay matches the live output exactly, SHA-256
`633c0e7b692dcefc26027ee4d26b880b55111856bfb01488a41beb163211c193`.
These are development-executable measurements, not production activation.

The added variants did **not** recover the missing company items in these windows.
Splitting `JC2VENTURES` produced unrelated hits; omitting `INC` from Ridgeline added
unrelated items. The latter also exposed an exact `Ridgeline` alias on the Honda
vehicle item. The relevance calculation retains that name correspondence without
calling the item a company. Both a businessperson and a make-up artist have saved
`John Chambers` name evidence; both survive, not just the expected review subject.
David Duffield has name correspondence but no same-response role endpoint matching
the reported employer. The engineer remains unresolved, not a verified negative.
No captured role endpoint supplies employer-name corroboration in either fixture.

This is evidence against treating wider token searches as sufficient company
discovery. Keep v2 opt-in. The original corpus and v1 capture/output pins are
unchanged. These captures were taken at different times, and even unchanged searches
show page-order/membership drift; this is not a controlled accuracy comparison.

## Tests and next boundary

Go tests cover input-order replay, occurrence preservation, missing inputs, budgets,
query tampering, source failures, schema/transport reuse, page-derived QIDs, competing
pages, missing types/items, role extraction, privacy-filtered headers and offline CLI
replay. Real-fixture tests bind the capture to the unchanged FEC appearances, verify
observed page ranks/gaps and assert no approval. Changing reviewed role text does not
change searches. Existing organization capture/replay tests remain passing.
The full Go suite, `go vet ./...`, focused race tests and all three focused
Wikimedia contract-schema tests pass. No new dependency or Dagster asset was added.

Variant tests cover explicit derivations, shared requests, unchanged person names,
bounded suffix vocabulary, query-budget failures and pinned v2 replay. Relevance tests
cover real namesakes/non-company aliases, missing endpoints, source failures,
unavailable names, retained unusable statements and no cross-response item merge.

The separate [registry-name discovery path](./organization-registry-discovery.md)
now supplies independent employer-name candidates without an existing QID or LEI.
Its live replay retains two same-name LEIs as ambiguous, not accepted identities.
Next automate extraction from retained company sources and test independent bindings.
Do not broaden name matches to hide retrieval or corroboration gaps. Coarse-date
screening and the screening bridge remain open.
No embedding/LLM requirement, automatic donor merge, production schedule, graph
mutation, terminal definition or dollar allocation is introduced.
