# Automatic structured-role extraction

Implemented 2026-09-15: an offline Go source adapter and CLI read explicit Wikidata
statements without manually supplied role annotations. The
[draft source contract](../../contracts/sources/wikimedia/role-statements/v1/contract.json)
is separate from person identity and financial policy. The prior
[reviewed corpus](./person-affiliation-corpus.md) remains unchanged.

The separate [relationship query](./relationship-query.md) now reuses this reader
with an opt-in parent/child property set and entity filtering. `ExtractRoles`, this
contract and existing discovery outputs keep their original selection unchanged.

## What is automated

`wikimedia.ExtractRoles` verifies a pinned `wbgetentities` response and the exact
expected item set, then extracts selected statement occurrences. It reuses the
existing closed entity-envelope reader. There are no company names, donor names,
benchmark QIDs, model calls or similarity thresholds in the extraction rules.

| Publisher property | Source direction | Extracted category |
|---|---|---|
| [P108 employer](https://www.wikidata.org/wiki/Property:P108) | Holder → employer | Employment only |
| [P112 founder](https://www.wikidata.org/wiki/Property:P112) | Entity → founder | Founder, not current authority |
| [P169 CEO](https://www.wikidata.org/wiki/Property:P169) | Entity → officer | Executive assertion |
| [P3320 board member](https://www.wikidata.org/wiki/Property:P3320) | Entity → member | Board-director assertion |
| [P127 owned by](https://www.wikidata.org/wiki/Property:P127), [P1830 owner of](https://www.wikidata.org/wiki/Property:P1830) | Opposite source directions, retained explicitly | Ownership without control evidence |
| [P39 position held](https://www.wikidata.org/wiki/Property:P39), [P488 chairperson](https://www.wikidata.org/wiki/Property:P488) | Retained without a corporate-role interpretation | Unknown |

Holder and related-entity IDs describe statement orientation, not verified person
and corporation types. An owner can be an organization; an owned object can be an
asset. The adapter reports whether a loaded holder item contains an unqualified,
nondeprecated human-type assertion. Missing endpoints, source-missing items and
unverified types stay explicit. Absence of a human assertion never proves a company.

Each selected occurrence carries its body digest, JSON pointer, original statement
ID, rank, raw statement digest and exact bytes. JSON output encodes those raw bytes
as base64 so serialization does not alter their identity. Labels and aliases remain
source vocabulary, not name-resolution rules. Unselected properties remain counted
and available through the retained response. Duplicate statement IDs retain every
occurrence and carry an issue on each.

Unknown selected-statement shapes, source unknown/no-value snaks, deprecated ranks,
unsupported qualifiers and missing references do not disappear. Citation objects
are retained, not fetched or independently verified. A preferred rank is not
confidence or evidence of current employment.

## Dates retain their precision

The adapter reads start, end and point-in-time qualifiers under the
[Wikibase JSON model](https://doc.wikimedia.org/Wikibase/master/php/docs_topics_json.html).
It retains every raw time, precision, calendar, timezone and before/after field.
Supported Gregorian CE values render as year, month or day according to their
reported precision. A year does not become January 1. Unsupported calendars,
precision and uncertainty remain explicit. Multiple values are not collapsed.

This is date extraction, not an implemented interval-overlap policy. It does not
turn revision dates, retrieval dates or reference-retrieval dates into role validity.
No automatic bridge to the day-bound screening `Claim` type is added.

## CLI and bounds

```sh
legal-tender pipeline entities extract-role-evidence \
  --body /path/to/retained-entities.json \
  --expected-body-sha256 BODY_SHA256 \
  --ids QID1,QID2
```

Use the exact expected IDs from the acquisition request, not an inferred complete
population. Input is limited to one four-MiB regular body, twenty items and ten
thousand total statements. Output binds the executable, body and source-shape hashes.
The command does no network access or database writes. Invalid pins, schema/API
errors and missing expected entities fail with nonzero exit and no success output.
Transport provenance belongs to the retained acquisition; the body pin alone does
not authenticate its publisher or HTTP status.

The separate [affiliation discovery slice](./affiliation-discovery.md) now generates
Wikipedia name/employer searches and supplies linked-item bodies. This extractor
does not perform inverse-property crawling, endpoint fetching or recurring refresh.
Existing organization captures can also supply bodies.

## Verification and bounded source comparison

Synthetic tests exercise property orientation, founder/employee/control distinctions,
all ranks, missing/unknown values, duplicate occurrences, source drift, temporal
precision and unsupported qualifiers. Three already-retained entity responses
conserve 53 selected occurrences: three employment, two ownership, one CEO and one
board-member assertion, plus 46 general office/chairperson statements deliberately
not promoted to corporate authority. This is source-parser coverage, not accepted
donor affiliations or population accuracy.

The CLI replays retained bytes identically and round-trips exact raw statement
bytes. Full Go tests, vet, focused race tests and the new contract-metadata schema
check pass. Python is used only by the existing schema-test tooling, not extraction.
No dependency or Dagster asset was added. The pre-existing unrelated general
report-scope metadata-schema mismatch remains deferred.

A fresh request for review items received HTTP 200 with a `maxlag` API error.
Two later attempts, each after the requested backoff, returned the same error class.
The background-mode retry at 20:32:49 UTC on 2026-09-15 also failed. All three
bodies and a safe metadata projection are retained in
[the source-attempt record](../../tests/fixtures/person-affiliation/role-source-attempts.json).
Full headers stay private because they include cookies and request-network metadata.
The extractor and CLI reject all three bodies; none is an empty candidate result.
Those failures did not establish a general outage or missing data. The earlier
description of the data as unavailable was too broad.

The user then approved one interactive request using Wikimedia's documented
[interactive maxlag exception](https://www.mediawiki.org/wiki/Manual:Maxlag_parameter).
At 21:46:51 UTC on 2026-09-15 the same endpoint, IDs and properties, with `maxlag`
omitted, returned HTTP 200 and 615,656 usable bytes. The
[separate interactive observation](../../tests/fixtures/person-affiliation/role-source-interactive.json)
records this mode and pins the complete public body. Raw headers remain private.
This was one manual diagnostic request, not a new runtime fetch mode. The existing
background capture still sends `maxlag=5` and stops on backoff. The successful
later request does not prove whether background-mode requests had recovered then.

The unchanged extractor and CLI preserve all 31 selected occurrences across three
items: two employment, four founder, six CEO, six ownership and thirteen board-member
assertions. The tests independently enumerate the original selected arrays and
compare every raw occurrence; no statement is dropped to fit the earlier review.
Unloaded endpoints and uninterpreted qualifiers remain explicit.

| Reviewed evidence | Result on these retained Wikidata items |
|---|---|
| Historical John Chambers / Cisco CEO | Cisco's `P169` points to the loaded Chambers item; start 1995 and end 2015 retain year precision. The role/end-year agree with the reviewed biography; this is a test-only subject correspondence, not an accepted FEC identity join. |
| Cisco emeritus title | Not represented by the selected statements on these items. The undated employment assertion is not emeritus or current board evidence. |
| JC2 CEO/founder and similar-name team member | Discovery incomplete: the JC2 organization and alternative person were not fetched. No conclusion that Wikidata lacks them or that the reviewed roles are false. |
| Ridgeline board/founder | Discovery incomplete: no Ridgeline item or separate person item was fetched. Workday's founder/CEO statements do not substitute for Ridgeline roles. |
| Reported engineer | Still unsurveyed; not a verified ordinary employee or true negative. |

`go test ./internal/audit/personaffiliation -run TestInteractiveRoleSourceComparison -v`
replays this comparison against the original pinned company-page/FEC corpus.
Reviewed annotations and all four frozen screening outcomes remain unchanged.
The real-body CLI test verifies deterministic output and exact raw-byte round trips;
the diagnostic CLI also passed byte-identical fresh-process replay. This is not a
new accepted production executable or broad identity-accuracy gate.

The bounded Wikipedia-first discovery slice now retains page-to-QID evidence using
the shared transport. Both organization and person items matter: this snapshot's
dated CEO role lives on Cisco, while the
person item supplies only undated employer assertions. Discovery results remain
candidates, not identity approvals. The temporal screening bridge, independent
corroboration, affiliation publication and terminal dollars remain separate work.
