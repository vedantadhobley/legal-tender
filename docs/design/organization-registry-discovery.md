# Independent organization-name registry discovery

Implemented exploratory Go slice, 2026-09-15. This removes the preexisting-LEI
requirement from one discovery path. It does not accept identities or affiliations.
The [draft source contract](../../contracts/sources/gleif/name-search/v1/contract.json)
owns its bounded acquisition and parsing rules.

## Why this path

The [Wikipedia experiment](./affiliation-discovery.md) did not retrieve the missing
company items. That must not prevent independent company discovery. The older
[registry corroborator](./organization-corroboration.md) only fetches LEIs already
asserted in Wikimedia data; it cannot discover a missing LEI.

GLEIF documents name-based lookup in its [API overview](https://www.gleif.org/en/lei-data/gleif-api)
and exposes `entity.names` in its [official demo](https://api.gleif.org/demo).
This new path starts with reported organization text, not a Wikidata candidate.
It uses `GET /api/v1/lei-records` with `filter[entity.names]`, first page, five
records. It is independent registry retrieval, not an independent binding of the
FEC employer string to one legal entity. An LEI is optional evidence, never a
requirement for representing a company in the future graph.

## Plan and capture

The generic `gleif.PlanNames` accepts exact organization-field values with source
digests and locators. It verifies shape, not the authenticity of caller-supplied
pins. The diagnostic `plan-employer-registry` command uses the existing verified
as-filed corpus. Reviewed role labels, company URLs, known LEIs and Wikipedia
results do not select requests. No donor name, address, occupation or amount is
sent to the registry.

`reported-organization-name-search.v1` preserves raw nulls, blanks and strings.
Queries use the existing case/punctuation normalization, preserving order,
accents, legal suffixes and letter/number boundaries. Commas are removed as
punctuation rather than passed as filter separators. This is retrieval syntax,
not an assertion about how the upstream search interprets every token.
No fuzzy fallback, suffix/boundary search expansion, country/status filter or
company exception table is added.

At most twenty source-field appearances are accepted. Queries share requests,
not identities; all contributing source indexes remain. Missing searchable names
remain explicit. Query membership is sorted and recomputed on replay.

Capture shares the existing GLEIF transport: fixed HTTPS origin, no credentials,
inherited proxy or redirects; one-second spacing, thirty-second request timeout,
five-minute deadline and two MiB/body. Twenty requests bound a run to forty MiB.
No retries or continuation requests occur. The first status, transport, body or
schema failure stops later requests while preserving failed/unattempted evidence.
New directories only; a final pinned manifest is required for replay.

## Parsing and candidate comparison

The strict Go page parser checks counts, bounds, same-query pagination links and
each resource's LEI/self-link identity. The exact-LEI adapter and name-search
adapter now share resource parsing and HTTP handling; the old interface and
retained outputs remain unchanged. Each source record retains its original
resource JSON, page-body digest and `/data/N` locator. No address, status or
historical-name fields are discarded from the source bytes.

`publisher_query_window_complete` means all records reported for that one query
fit in the captured page. It never means comprehensive organization coverage.
Search results can be unrelated; zero hits are not proof that a company has no
LEI or does not exist. Pages with additional records remain incomplete, even if
one captured name appears to match.

`registry-name-candidates.v1` compares every returned legal, other and
transliterated name to the original reported string using explained organization
name proposals. It preserves each name's language/type, source order and every
competing LEI. Previous names are not promoted to current legal names. Entity and
registration status remain separate; inactive/lapsed records are not silently
removed. Multiple corresponding LEIs produce explicit ambiguity; a singleton is
still unresolved. Search position and repeated source occurrences are not votes.

Every decision retains missing independent identifier binding, transaction-time
identity and person-role evidence as blockers. All exhaustive-discovery, identity,
employment, graph-publication and financial flags stay false. This is not the
person screening bridge or a corporate-role parser.

## Commands

```sh
legal-tender pipeline entities plan-employer-registry \
  --corpus tests/fixtures/person-affiliation \
  --expected-corpus-sha256 CORPUS_SHA256 > registry-plan.json

legal-tender pipeline entities capture-registry-names \
  --plan registry-plan.json --expected-plan-sha256 PLAN_SHA256 \
  --output NEW_CAPTURE_DIRECTORY \
  --user-agent 'LegalTender/company-discovery (YOUR_CONTACT_URL)'

legal-tender pipeline entities replay-registry-names \
  --capture NEW_CAPTURE_DIRECTORY --expected-capture-sha256 CAPTURE_SHA256
```

Replay is offline. Corrupt ancestry returns an error with no assessment. Failed
source observations return structured partial results and exit 1. Exit 0 means
usable observations, not identity approval. The assessment records its executable
digest separately from the capture build; exact output comparison uses the same
assessment executable.

## Retained live result

The [retained capture](../../tests/fixtures/person-affiliation/gleif-names-v1/capture.json)
preserves three successful requests, 8,078 response bytes and all four original
FEC appearances. Capture SHA-256:
`759cc1be8ff0713b1bd57655e081edc8b3820661f1af7049d9ddd62038e25e51`.
Fresh offline output matches the live output with the same development executable:
`2f8b5342ccbaab53713aeff7b400096015da8351c9d12592c846d282d58d5ff5`.
This is not an accepted production build or activation.

The Ridgeline query returned three distinct LEIs. Two have corresponding legal
names, so both remain candidates; no evidence here chooses one or establishes
which company employs the donor. The third result remains visible despite its
different name. The other two query windows were empty. All three windows were
complete under the publisher's reported counts, not exhaustive company searches.
The original corpus and Wikimedia fixtures are unchanged.

Tests cover exact raw record preservation, repeated source appearances, annotation
independence, null/blank inputs, query tampering, budgets, unknown schemas,
pagination inconsistencies, competing/historical names, failure stopping, private
header exclusion, corrupt artifacts and offline CLI replay. The original exact-LEI
and Wikimedia paths remain regression-tested.

The [company-page extractor](./company-page-evidence.md) now replays retained sources
with exact text and byte evidence. Next test prose-role interpretation and independent
bindings for ambiguous registry candidates. Source selection for those company pages
is still manual; current pages do not establish historical employment. Do not keep
adding name-only searches as a substitute for that missing evidence. No bulk crawl,
LLM requirement, graph publication or dollar policy is added by this slice.
