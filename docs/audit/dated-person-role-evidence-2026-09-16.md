# Dated person/company evidence check

The user approved checking dated first-party evidence after the
[binding diagnostic](../design/person-binding-diagnostic.md). This bounded review
finds usable historical context for both private-company cases. It does not choose
a person-identity rule, infer continuous tenure or implement a new source pipeline.

## Findings

| Retained FEC appearance | Dated evidence | What it adds; what remains |
|---|---|---|
| Two John Chambers / JC2VENTURES occurrences, 2023-09-30 | Sprinklr's proxy dated 2023-05-05 describes its director as JC2's CEO. | Name and employer correspondence pass existing rules; the role assertion precedes the receipts by 148 days. No donor identity or continuous tenure is thereby established. |
| David Duffield / RIDGELINE INC occurrence, 2023-09-30 | Ridgeline's announcement datelined 2023-09-05 describes Dave Duffield as founder/co-CEO. | A role point 25 days before the receipt, with employer-name correspondence. David/Dave remains an unaccepted person correspondence, not a global nickname substitution. |

Sources: [Sprinklr's issuer-hosted proxy](https://investors.sprinklr.com/financial-information/all-sec-filings/content/0001140361-23-023208/ny20007009x1_def14a.htm),
[SEC filing index](https://www.sec.gov/Archives/edgar/data/1569345/000114036123023208/0001140361-23-023208-index.htm),
and [Ridgeline's company announcement](https://ridgeline.ai/company/news/ridgeline-names-dave-blair-co-ceo-expands-executive-leadership-team).

The proxy is a first-party corporate disclosure about Sprinklr's own director,
not a JC2 filing or independent person registry. It offers corroboration outside
the current JC2 biography, but independence of its underlying biographical account
is not established. Its other dates concern different relationships: the Sprinklr
board, JC2 founding, and earlier Cisco service. They cannot fill a JC2 CEO interval.

An additional web-only check of [Workday's 2021 proxy](https://www.sec.gov/Archives/edgar/data/1327811/000110465921054900/tm213225-2_def14a.htm)
finds a formal David A. Duffield biography linking him to Ridgeline, with chairman
service from October 2017 and CEO service from January 2018. This supports a
cross-source identity hypothesis through shared biography and employer context;
it is not an explicit David/Dave alias assertion or a match to the FEC occurrence.
The additional middle initial remains significant under current rules. This older
filing does not establish service in September 2023. Its body was not added to the
offline fixtures, so it is research evidence, not an automated replay result.

## Dates that must not be collapsed

The old Ridgeline URL redirects to the retained `/company/news/` URL. Search-index
text at the old URL displayed April 2024. The fresh source body instead contains:

- A visible page date of 2026-08-06.
- JSON-LD publication date 2023-09-05 and modification timestamp
  2026-08-06T07:26:15.018Z.
- A narrative announcement dateline of 2023-09-05.

The old search result was discovery evidence, not a retained HTTP body. Preserve
these meanings; do not substitute search-index or page-update dates for the event.
This does not establish why the site changed its displayed date.

Likewise, a corporate disclosure's document date is not automatically a role date.
Here a reviewer interprets the present-tense biography as a point assertion in the
dated disclosure. The source reader extracts the text; it does not make that
interpretation. Neither reviewed point proves uninterrupted service until a later
contribution, and neither is evidence of non-service then.

## Retained inputs and executable checks

The separate [review fixture](../../tests/fixtures/person-affiliation/dated-roles/review.json)
pins two complete public HTML bodies, requested/final URLs, observation day, HTTP
status and byte counts. Total HTML size is 1,873,940 bytes. These are bounded
research downloads, not production capture manifests or archival proof that today's
bytes are identical to what the publisher served in 2023. No full response headers,
operator identity or credentials were retained. Original corpus bytes are unchanged.

The [Go tests](../../internal/audit/personaffiliation/dated_roles_test.go) reuse the
existing company-page reader, original FEC corpus, organization comparison and
day-level screening evaluator. They check:

- Complete-body integrity and exact source spans, deterministic extraction and
  unchanged source text/dates when the observation day changes.
- Distinct publication/modification/dateline evidence and reviewed role text.
- Existing name/employer rules, actual receipt dates and separate occurrences.
- Point assertions remaining unknown at later receipt dates, without fabricated
  starts/ends, identity approval, affiliation publication or money attribution.

Role interpretation, the person/organization names and point-date assignments are
explicit reviewed test annotations. Page selection is manual. Passing these tests
does not mean prose interpretation or historical person resolution is automated.
The reported-engineer case is still unassessed, not a verified negative.

```sh
go test -v ./internal/audit/personaffiliation -run '^TestDatedFirstPartyRoleEvidence$'
```

The focused retained-source test, full Go suite, `go vet ./...` and race tests for
the audit, company-page reader and person-affiliation calculation packages pass
in the existing memory-capped, network-disabled Go container. Formatting, relative
documentation links and the unchanged original corpus hash also pass. No runtime
code, dependency, source adapter, Dagster asset, database or financial rule changed.

## Recommendation

Follow-up: the [policy and additive evaluator](../design/person-affiliation-acceptance.md)
now make these choices explicit and replay these reviewed claims through a
candidate/time evaluator. The source findings are unchanged; identity acceptance
remains unimplemented. Neither earlier role point yields a continuity hypothesis.

The later [identity-rule feasibility review](./person-identity-rule-feasibility-2026-09-16.md)
used these cases to reject an exact-field, mandatory-locality conjunction. The
experimental evaluator was removed; the original findings, tests and source bytes
remain unchanged.

We have enough concrete evidence to review an acceptance policy instead of opening
another acquisition pipeline for these examples. Decide separately:

1. What corroboration can accept a person identity across FEC, names/aliases and
   company/registry sources, while preserving rivals and source dependence?
2. Whether a dated role observation can support a later-date affiliation inference,
   with explicit uncertainty, age and conflicting/job-change evidence?

No arbitrary age cutoff or continuity rule was selected here. Strong candidate
evidence need not be discarded because it is not an exact-day fact. The existing
FEC employer/occupation strings already remain useful reported context on each
receipt; they are not proof of a resolved legal employer, executive authority,
corporate funding or the corporation's position on legislation.
