# Read-only relationship evidence query

Implemented 2026-09-16 after the user approved the small slice recommended by the
[evidence comparison](../audit/relationship-evidence-comparison-2026-09-16.md).
This is a Go query over retained source data, not an affiliation publisher or a
new graph. It answers: **what relationship claims in this response mention this
source entity, and what times and evidence did the source supply?**

## Implemented behavior

`wikimedia.QueryRelationships` reuses the existing
[role reader](./wikidata-role-extraction.md), strict entity parser and time reader.
It selects statements where the requested `wikidata:QID` is either the original
subject or a parseable original object. It does not recursively traverse or fetch
other items. The complete expected response IDs remain explicit inputs; they are
not inferred from the entity being queried.

The original role properties retain their meanings. This separate
[source contract](../../contracts/sources/wikimedia/relationship-statements/v1/contract.json)
also interprets two hierarchy properties:

| Source property | Source direction | Additional display orientation |
|---|---|---|
| [P355 child organization or unit](https://www.wikidata.org/wiki/Property:P355) | Parent → child | Parent = subject; child = object |
| [P749 parent organization or unit](https://www.wikidata.org/wiki/Property:P749) | Child → parent | Parent = object; child = subject |

The property definitions were checked on 2026-09-16. The display mapping is explicit
in each result; the original predicate, endpoints and raw statement remain intact.
It does not create a second assertion. Multiple parents, opposite-direction claims,
deprecated statements, duplicate occurrences and self-relations remain separate.
Hierarchy does not establish legal corporate type, full ownership, control or
direct/ultimate parentage. It has no personal-role holder classification.

Each match returns:

- the exact statement bytes, digest, source JSON pointer, statement ID and rank;
- source endpoint labels, aliases and revision metadata from this body only;
- the existing precision-preserving time qualifiers and explicit issues;
- all other qualifiers and references in the unchanged raw statement; and
- the optional hierarchy display mapping, without deduplication or acceptance.

Missing endpoint data is explicit: `item_not_loaded` differs from a publisher's
`source_entity_missing`. A loaded item is not automatically a person or company.
The query can return incoming claims about an unloaded item. An unknown/no-value or
unsupported object is not invented: its outgoing statement remains queryable from
the subject, but an unparseable incoming target cannot be searched.

There is no date filter or interval-overlap inference in this slice. Year/month/day
precision remains as reported. Unknown ends do not establish current service. An
optional acquisition timestamp is marked `caller_supplied`; absence stays
`not_supplied`. Neither execution time nor item-edit time fills it. A body digest
does not authenticate the publisher, HTTP status or the caller's timestamp.

## CLI

```sh
legal-tender pipeline entities query-relationships \
  --body tests/fixtures/person-affiliation/wikidata-interactive-1.json \
  --expected-body-sha256 5683ad0692e124dcd9d87bcdc81b7448def720f632a2e4e4b66b8aae4f490076 \
  --ids Q1393271,Q173395,Q8034666 \
  --entity wikidata:Q1393271 \
  --observed-at 2026-09-15T21:46:51Z
```

The timestamp in this example comes from the retained
[interactive observation](../../tests/fixtures/person-affiliation/role-source-interactive.json),
not the contribution date. It is optional for other bodies whose acquisition time
is unknown. The command never loads `.env`, contacts a service or writes a database.

Limits remain one four-MiB regular body, twenty expected items and ten thousand
total source statements. The complete response is validated before results are
returned. Wrong pins/IDs, source API errors and invalid envelopes fail; they are
not successful empty queries. A valid query with no matches means only that no
selected parseable incident claims were found in this supplied response.

Outputs include body/shape/executable digests and the limited query scope. These
reuse existing provenance conventions; no archive, checkpoint, publication or
recovery workflow was added. Different response bodies are queried separately;
there is no hidden latest-profile or cross-snapshot merge.

## Verified retained examples

The new source and CLI tests exercise the same saved data used in exploration:

| Query | Observed result |
|---|---|
| John Chambers item | Two undated outgoing employer claims plus an incoming Cisco CEO claim with years 1995–2015. Undated employment is not current employment. |
| Cisco item | Its role and child-organization claims retain unloaded endpoints and original dates. Two reviewed board terms remain 2017-03–2023-02 and 2023-10–2026-05; no contemporaneous connection is inferred. |
| Workday item | Both founder statements survive; no synthetic person-to-person edge is added. |
| Honda Canada item | Two reported parents remain, including an unloaded target. The opposite P355/P749 statements connecting the same pair remain two source occurrences with the same display orientation. |

The tests independently enumerate the selected raw arrays and incident endpoints,
then compare every returned occurrence's exact bytes. They also cover unknown and
no-value objects, schema issues, duplicate/self/deprecated claims, missing items,
observation-time independence, invalid IDs, body limits and deterministic ordering.
Example entity IDs occur in fixtures/tests, not runtime matching exceptions.

The full Go suite, `go vet`, ordinary CLI build, focused reader/CLI race tests and
contract metadata/fixture validation passed. Fresh CLI smoke tests exercised the
Chambers and Honda cases.
The original role/discovery consumers keep their old property selection and
`wikimedia/role-statements@1.0.0` output contract; the relationship query opts into
the wider set. Funding and affiliation regression tests pass unchanged.

## Deliberate boundary and next work

FEC appearances, reported employers and candidate identity assessments remain in
their existing outputs. This query accepts no FEC identity or free-text name and
does not join those outputs to a Wikidata person. All identity, graph-publication
and financial-attribution flags remain false. It changes no Arango collection,
funding traversal, terminal definition or amount.

The capability is now implemented; another discovery source is not its next gate.
The separate [binding diagnostic](./person-binding-diagnostic.md) now tests name,
employer and receipt-date comparisons on those source claims without accepting
identities. Its coarse-date comparison does not add date filters to this query.
Private-company and historical-role evidence remain gaps. Company-page prose,
graph publication and broader acquisition remain unimplemented, not implied by
either read path.
