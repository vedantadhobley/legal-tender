# Retained person-affiliation diagnostic corpus

Implemented 2026-09-15. The offline Go replay tests the unchanged
[screening evaluator](./person-affiliation-testing.md) against selected real
as-filed appearances and **reviewed** corporate-role annotations. It does not
automatically discover people, extract roles from prose, resolve identities or
publish affiliations. This is a diagnostic corpus, not an accuracy benchmark.

## Reproducible inputs

The [corpus manifest](../../tests/fixtures/person-affiliation/corpus.json) pins
one complete retained FEC report, its original response headers, the existing
Schedule A layout and three complete public company pages. Retained source inputs
total 1,076,606 bytes. No new bulk download or full-corpus scan was needed.
The [fixture notes](../../tests/fixtures/person-affiliation/README.md) identify
the source and selection boundaries.

Corpus SHA-256:
`7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4`.

`internal/audit/personaffiliation.Run` verifies the corpus and source hashes,
sizes, bounded regular files, exact unique excerpt spans and source references.
The existing FEC electronic-cover reader verifies the whole filing, transport
headers, supported 8.4 header and F3 cover. The audit maps only selected complete,
ASCII, exact-width individual Schedule A rows using the pinned field layout.
Other physical shapes fail the selected-case gate; the retained file is unchanged.
This is not an expansion of the draft general electronic-filing ingestion contract.

Each appearance retains its document hash, row ordinal, exact byte interval,
row hash, transaction ID, source name components, employer, occupation and raw date.
A clearly marked audit view joins the explicit name components in display order
and converts a valid `YYYYMMDD` date to ISO day form. It does not guess surname
order in processed names or equate the as-filed occurrence with a processed fact.
Cycle is unset. Addresses and money are not needed in this diagnostic output;
the complete original remains available as evidence. Memo, amendment and counting
decisions are not made here.

Role category, subject grouping and temporal interpretation are reviewed corpus
annotations. Their `review:` IDs identify subjects within this review, not verified
external identifiers or donor identities. The runner verifies excerpt presence
and byte spans; it cannot verify that a reviewer's interpretation is true.
Expected outcomes live in tests and never enter the screening function. Review
prose and input ordering cannot alter its decisions.

## Observed baseline

| Selected appearance | Evidence tested | Unchanged v1 result |
|---|---|---|
| Two John Chambers occurrences | FEC employer `JC2VENTURES`; company uses `JC2 Ventures` | Person-name correspondence, employer mismatch; occurrences stay separate |
| Same appearances, alternative team subject | Company also lists John J. Chambers | Initial-bearing name remains distinct; not a demonstrated ambiguous donor identity |
| Same appearances, historical Cisco context | Biography describes a former CEO role and an emeritus title | No current leadership claim; year-only end is an explicit unsupported temporal constraint |
| David Duffield occurrence | Ridgeline lists Dave Duffield under its board section | Nickname and employer-suffix differences remain unbridged; historical validity unknown |
| Reported engineer occurrence | No external role search was attempted | Coverage gap, not a verified ordinary employee or a true-negative leadership classification |

The FEC appearances come from
[report 1730162](https://docquery.fec.gov/dcdev/posted/1730162.fec).
Role review uses [JC2's team page](https://www.jc2ventures.com/about), its
[linked biography](https://www.jc2ventures.com/john-chambers), and
[Ridgeline's leadership page](https://ridgeline.ai/company/leadership).
Those current pages do not establish roles on the 2023 contribution dates.
Retrieval dates are retained separately and never supplied as role-validity dates.

All four cases return no name/employer candidate under v1. This is useful failure
evidence, not proof of high precision or successful person resolution. No role,
identity, graph or financial approval results from this corpus. A repeated source
sentence interpreted as founder and CEO is not two independent corroborations.

The additive [v2 identity-evidence evaluation](../audit/person-identity-evidence-v2-2026-09-16.md)
reuses these exact appearances and reviewed claims without changing the corpus or
its v1 baseline. It classifies the Chambers name/organization candidate and omitted-
middle rival separately, preserves the unresolved David/Dave variant, and leaves the
unsearched engineer as an abstention. Every identity, graph and financial flag stays
false.

## Verification

```sh
go test ./internal/audit/personaffiliation ./internal/calculation/personaffiliation ./internal/projection/arango/fundinggeneration
```

Tests cover exact source projection, occurrence conservation, unchanged decisions
on replay and reordered inputs, label independence, missing/nonunique excerpts,
altered bytes, invalid pins/paths, symlinks, unsupported row widths/encodings,
invalid as-filed dates, duplicate cases/roles, and unbound evidence. Full Go tests,
`go vet ./...` and focused race tests pass. No new dependency, CLI, Dagster asset,
runtime name exception, graph write or monetary policy was added.

## Next implementation boundary

The [automatic structured-role reader](./wikidata-role-extraction.md) is now
implemented over explicit Wikidata statements. A user-approved interactive fetch
now supplies a pinned three-item body after earlier background-mode backoff responses.
The executable bounded comparison confirms historical Cisco role/end-year agreement,
but preserves unobserved title and incomplete-discovery gaps for the other annotations.
It does not establish corporate-role coverage or FEC identities. The corpus itself
and its frozen v1 outcomes are unchanged.

The replay is automated; the source selection and role annotations are not.
The separate [company-page reader](./company-page-evidence.md) now extracts text,
metadata and JSON-LD automatically from these saved pages. It does not replace the
reviewed annotations or interpret prose as accepted role/identity assertions.
Do not deploy this corpus as a person directory. The [exploratory discovery slice](./affiliation-discovery.md)
now generates and captures Wikipedia-first searches from these verified appearances,
without using reviewed role labels to choose queries. Its retained live replay passes
but preserves retrieval gaps; extend comparisons without replacing annotations silently.
The [binding diagnostic](./person-binding-diagnostic.md) now compares parsed source
claims with these FEC appearances and their actual dates. It retains name/employer
gaps and source time precision; it does not accept identities or prove historical
roles. The reviewed company annotations remain a separate comparison input.

The existing organization v2 proposal rules can be evaluated separately for employer
format differences. Nicknames require independent evidence, not an unconditional
`David`/`Dave` substitution. The diagnostic now tests coarse year/month comparisons
without inventing day boundaries or turning current pages into historical proof.
Ordinary-employee and same-name ambiguity cases still need independently evidenced
real examples. Their synthetic safety tests pass, but real coverage is not accepted.

Production person identity, dated corporate affiliation publication and terminal
dollar attribution remain separate decisions.
