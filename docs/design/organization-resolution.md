# Organization candidate capture and replay

Implemented bounded Go slice, 2026-09-15. This adds external candidate evidence,
not canonical organizations, donor merges or new graph edges. Python remains an
exploration reference. The [connection-assumption review](./pre-attribution-review.md)
and terminal-allocation decisions remain separate.

## Implemented boundary

`pipeline fec build-organization-queries` reads immutable FEC fact manifests:

- Committee mode selects the first N distinct nonblank `CONNECTED_ORG_NM` strings
  in lexical order and retains every source reference for each selected string.
  This is a deterministic test population, not a representative sample or a
  materiality rule. It reads the complete small committee artifact.
- Receipt mode accepts explicit source ordinals and extracts only
  `contbr_employer` plus source references. Null/blank employers fail selection;
  they are not silently dropped. The existing loader verifies the whole Schedule A
  fact set before reading selected rows. This mode is not a per-receipt lookup
  strategy for production.

Query identity is the exact reported string. Whitespace/case variants remain
separate inputs. Source manifest digests, fact-set IDs and fact IDs or ordinals
retain provenance. Donor names, addresses and amounts are not lookup parameters.
Input digests prove byte identity, not authenticity of hand-written query files;
use the FEC builder for source-backed query membership.

`pipeline entities capture-organizations` searches English Wikipedia and fetches
the Wikidata IDs linked from returned pages. Wikipedia's search API supplies a
bounded page window; `pageprops` supplies the page-to-item linkage. Neither is
proof of identity. See the official [search API](https://www.mediawiki.org/wiki/API:Search),
[page properties](https://www.mediawiki.org/wiki/API:Pageprops) and
[Wikibase API](https://www.mediawiki.org/wiki/Wikibase/API).

The [draft source contract](../../contracts/sources/wikimedia/organization-candidates/v1/contract.json)
pins the compiled response shapes and synthetic fixtures. Each capture retains:

- Exact query JSON, build digest and descriptive user agent.
- Request URL, observation time, status, selected nonsecret response headers,
  body size and SHA-256, including bounded failed responses.
- Page IDs, revision IDs/timestamps, QIDs, labels, aliases and descriptions.
- All returned statement JSON, including ranks, qualifiers and references.
  These are not flattened into relationship edges. Article text is not fetched.

Captures use new directories only. A manifest is written after all attempt records.
An interrupted directory without that manifest is not replayable as complete.
There is no latest pointer, source-release mutation or graph write.

Limits are 20 queries, five search hits/query, at most 40 serial requests,
one-second spacing, 30 seconds/request, five minutes/run, 4 MiB/body and
64 MiB/capture. Fixed HTTPS origins, no redirects, no inherited proxy and no
credentials. `maxlag=5` is sent. The first transport, HTTP or schema failure stops
further network calls; later queries remain explicitly unattempted. No automatic
retry, scheduled refresh or pagination is implemented. Continuation remains raw
evidence of a bounded window, not exhaustive discovery.

`pipeline entities replay-organizations` has no network dependency. It verifies
the pinned capture, query and body digests, file confinement, request reconstruction
and budgets. It checks strict UTF-8/JSON, duplicate keys and closed response shapes,
and records observed path/type fingerprints. Unknown source shapes remain captured
but cannot produce usable matching decisions. API failure is not entity absence.

## Versioned proposal rules, not identity authority

The default `organization-name-proposals.v1` uppercases text, replaces punctuation
with spaces and collapses spaces. It preserves token order, accents and legal suffixes.
It compares the resulting text with English labels, aliases and page titles.
No spelling correction, abbreviation expansion, embeddings, LLMs or relevance
thresholds are used.

An exact match to one QID produces
`single_exact_name_candidate_in_search_window`. This does **not** establish
global uniqueness, corporate identity or historical employment. Multiple exact
QIDs remain ambiguous. Missing page QIDs or items prevent singleton proposals.
Nonmatching retrieved items remain in the evidence; no-result search windows and
source errors remain different states.

The opt-in `organization-name-proposals.v2` adds two bounded name transformations:

- Insert spaces at letter/number boundaries. `Local42` and `Local 42` can match;
  `AB C` and `A BC` cannot. Token order, accents and spelling remain unchanged.
- Compare a stem after removing one trailing legal-designator token. The supported
  families are `CORP`/`CORPORATION`, `INC`/`INCORPORATED`, `LTD`/`LIMITED`, and `LLC`.
  An omitted suffix or variants within one family can match. Two explicitly
  different families cannot match through this rule. The stem must contain a
  letter; no arbitrary name-length cutoff is used. These are candidate rules,
  not a claim that legal names or registration types are interchangeable.

The matcher tries normalized exact, letter/number boundaries, suffix-only and
combined transformations in that order. It records the first matching form for
each observed name. It does not drop semantic words such as `BANK`, `HOLDINGS`,
`GROUP`, `FOUNDATION` or `PAC`, strip repeated suffixes, reorder words, remove all
spaces, expand dotted `L.L.C.`, fix typos or choose a parent company.

Each matching label, alias or page title records its original text, source kind,
page ID where applicable, rule, normalized/comparison forms and transformations
on each side. Repeated identical observations are deduplicated; multiple names
for one QID are not votes or independent confirmations. `exact_names` retains
its v1 meaning and never contains a broader match.

V2 collects all matching QIDs before choosing a state. A broader rival blocks
even an exact v1 winner: `ambiguous_name_candidates`, not a ranked choice.
A singleton is `single_name_candidate_in_search_window`, still type-unverified.
Missing QIDs/items block a singleton. Every decision carries its v1 baseline
policy, state and proposed QID. V1 behavior and default output remain unchanged;
the original capture and observed entity claims are never rewritten.

Wikipedia disambiguation pages and direct nondeprecated `P31=Q5` human assertions
are not organization proposals. All other types remain unverified; there is no
subclass closure. Original statements remain intact, including deprecated claims.
We do not reinterpret founder/CEO properties as outgoing person-to-company facts.

Every result keeps `identity_resolved`, `employment_verified`,
`ownership_verified` and `financial_attribution` false. `capture_usable` describes
source parsing only, not resolution coverage or confidence. A reported employer is
still a reported employer, not a verified corporate relationship or a corporate
payment. No financial selection, graph or terminal policy changed.

## Commands

Using a built Go executable:

```sh
legal-tender pipeline fec build-organization-queries \
  --storage-root /storage --committee-facts /storage/IMMUTABLE_CM_MANIFEST.json \
  --limit 5 > queries.json
sha256sum queries.json

legal-tender pipeline entities capture-organizations \
  --queries queries.json --expected-queries-sha256 QUERY_SHA256 \
  --output NEW_CAPTURE_DIRECTORY \
  --user-agent 'LegalTender/organization-research (YOUR_CONTACT_URL)'

sha256sum NEW_CAPTURE_DIRECTORY/capture.json
legal-tender pipeline entities replay-organizations \
  --capture NEW_CAPTURE_DIRECTORY --expected-capture-sha256 CAPTURE_SHA256

legal-tender pipeline entities replay-organizations \
  --capture NEW_CAPTURE_DIRECTORY --expected-capture-sha256 CAPTURE_SHA256 \
  --proposal-policy organization-name-proposals.v2
```

Capture, replay and evaluation accept `--proposal-policy`; omission keeps v1.
Unsupported policy names fail before acquisition. There is no mutable `latest`
policy alias. Both policies consume the same capture/source schema.

Receipt mode substitutes `--schedule-a-facts IMMUTABLE_A_MANIFEST` and
`--receipt-ordinals 1,2,3` for `--committee-facts`. Ordinals are examples, not a
production selection policy. Keep capture directories under the project's
configured storage when retaining observations beyond a development probe.
Successful source parsing returns exit 0 even if every name is unresolved.
Source failures return structured decisions and exit 1; invalid/corrupt captures
return an error. No command accepts an arbitrary endpoint.

## Verification and next work

Tests cover preserved source references, exact-string deduplication, aliases,
punctuation, suffixes, misspellings, ambiguity, human/disambiguation conflicts,
missing QIDs/items, error bodies, size limits, cancellation, corrupt bodies,
symlinks, strict encoding/schema failures and deterministic offline replay.
The full Go suite, Go static checks and targeted race tests pass. The Wikimedia
metadata/fixture schema check passes. The report-scope assessment now lives in the
calculation-contract namespace, so the general source-metadata gate validates only
source-acquisition contracts.

A development smoke test read the existing pinned 2024 committee fact set
`e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d`.
Five selected names retained six references. Ten requests captured 1,009,148 raw
bytes; the complete output matched a fresh offline replay byte-for-byte. All five
remained without an exact-name proposal. This verifies the transport/replay path,
not matching accuracy, broad coverage or production readiness.

Temporary smoke evidence is at `/tmp/legal-tender-organizations-k2kd6VnN/`;
capture digest `a1c9e6c18c17010381fd910dd3b45770b5f623bb3a18e98e61274250b540b0f0`,
result digest `8e44118d0318c87c119b60a6e1e005af861519d7cc220dc3eee9823bad3e86fc`.
This is a development executable, not an accepted production release artifact.

The [offline evaluation corpus and corroboration requirements](./organization-evaluation.md)
now retain primary-source-backed review labels and distinguish retrieval, matching,
missing-entity and source-failure outcomes. V1 misses both reviewed positive
correspondences after successful retrieval; v2 proposes both on unchanged evidence
and proposes neither annotated counterexample. This small diagnostic sample does
not establish population-wide accuracy. The separate
[registry corroboration slice](./organization-corroboration.md) now retains
exact-LEI evidence and passes hard-case tests, without approving FEC identities.
Next address missing identifiers and expand real evidence before accepting edges. Person resolution,
corporate-parent/ownership evidence, verified employment, graph publication,
scheduled refresh, full-population selection and terminal attribution are not
implemented by this slice. An empty exact-match result is not a reason to silently
loosen the rule.
