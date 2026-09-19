# Supplementary affiliation-source investigation

Reviewed 2026-09-16 America/New_York; source bodies captured 2026-09-17 UTC.
The user approved testing supplementary sources against the same three appearances
from the [automatic enrichment sample](../design/affiliation-enrichment.md).
This is a source-feasibility test, not a new acquisition pipeline or identity rule.

Subsequent work: the [opt-in prose syntax prototype](../design/prose-relationship-prototype.md)
now implements the bounded deterministic baseline proposed below. This audit records
the earlier source investigation; its source selection and PDF interpretation remain
manual, and it does not establish general extraction accuracy.

## Result

Useful relationship evidence exists beyond Wikipedia/Wikidata on these cases.
Company prose and a licensing record supply two leads. They do not supply three
resolved donors, prove receipt-day roles, or demonstrate population coverage.
The existing HTML reader retains the relevant prose with exact byte spans.
None of the four inspected JSON-LD projections supplies a person-role record.

| Source appearance | Supplementary evidence | Remaining boundary |
|---|---|---|
| Ordinal 317: Richard Reviglio / Western Nevada Supply Co; occupation OWNER; receipt 2023-09-22 | The company's [Rick biography](https://goblueteam.com/rick-reviglio/) describes its president/general manager. Its [Jack biography](https://goblueteam.com/jack-reviglio/) explicitly supplies Richard/Rick name correspondence. | Role and alias are prose claims from one publisher. President/general manager does not prove the reported ownership, a donor identity, or a 2023 interval. |
| Ordinal 139: Christie Gescheider / Moana Nursery; occupation MEMBER; receipt 2023-09-25 | The [Nevada Contractors Board agenda](https://www.nvcontractorsboard.com/wp-content/uploads/2023/09/03-21-19-Board-Meeting-Agenda-Updated.pdf), record 154, lists Christie Ann Gescheider as VP/Secretary under Moana Nursery and license IDs 0003379A/0003379D. | The meeting is dated 2019-03-21 and application approval 2019-03-01. Neither is a proven tenure boundary. The extra middle name and later FEC binding remain unassessed. |
| Ordinal 453: Alton Russell / Copaco; occupation SALES; receipt 2023-07-17 | The [Georgia corrections profile](https://gdc.georgia.gov/alton-russell) states a government board role, with no Copaco employment claim. | An unrelated board title cannot become corporate executive authority. This primary page does not close the employer gap. |

The FEC fields above come from the existing verified local filing, not search
results. No address or contribution amount was needed. The
[review log](../../tests/fixtures/person-affiliation/supplementary-v1/review.json)
binds that source, the unchanged discovery baseline, and each complete new body.

Additional controls matter:

- Moana's [about page](https://www.moananursery.com/our-culture/) supplies company
  context and an image label with first names, but no full-name Christie role in
  the lexical text. A page mentioning the company is not sufficient evidence.
- A [republished 2013 appointment release](https://savannahceo.com/news/2013/06/governor-deal-appoints-19-georgians-state-boards/)
  describes an Alton Russell as a Copaco territory manager. The original issuing
  page was not located in this bounded search; this is a web-only historical lead,
  not retained first-party confirmation or a 2023 role assertion. Mirrors of the
  same release are not independent support.
- Search snippets for the Nevada agenda rendered the surname differently from
  the actual document. Direct PDF text extraction and rendered-page inspection
  agree on GESCHEIDER. The original document controls; do not add a name-fix rule
  from a search snippet.

## What was actually tested

Five explicit HTTPS requests completed successfully: four HTML pages and one PDF,
942,847 body bytes in total. URLs were selected through interactive research.
This is not application-driven discovery. No retries, JavaScript, subresources,
recursive crawl, private credentials, LLM or embedding calls were used.

The complete bodies live in the [supplementary fixture](../../tests/fixtures/person-affiliation/supplementary-v1/README.md).
The log records URLs, observed status/media type, byte sizes and hashes. It is a
research record, not a production HTTP manifest: exact response headers were not
retained. Earlier source bytes, discovery results and automatic sampling are unchanged.

All four HTML bodies pass the unchanged [lexical reader](../design/company-page-evidence.md):

| Body | Extracted entries | JSON-LD blocks | Measured shape |
|---|---:|---:|---|
| Rick biography | 136 | 1 | Role sentence in text; site/page metadata in JSON-LD |
| Jack biography | 121 | 1 | Alias in a longer paragraph about a different subject |
| Moana about page | 232 | 1 | Company context; no full-name role in extracted text |
| Georgia board profile | 143 | 1 | Public-board sentence; no reported-employer claim |

The retained JSON-LD has no `Person` or role-typed objects and no `jobTitle`,
`worksFor`, `employee`, `memberOf`, `founder` or `owns` keys. This is a measured
property of four snapshots, not a reason to discard structured markup elsewhere.
The two biography pages share a publisher and are not independent sources.

Page-level `datePublished`/`dateModified` values survive unchanged. Some precede
the 2023 receipt, but a timestamp inside mutable HTML fetched in 2026 does not prove
the wording existed in 2023 or establish role validity. The alias's surrounding
family narrative also does not become donor-family edges automatically.

The PDF is the complete original, not a reconstructed row. Its relevant record is
on physical page 46, printed page 43 of 107. `pdftotext -f 46 -l 46 -layout` and a
rendered-page check were diagnostic tools. No PDF parser, OCR dependency or
licensing-record source adapter was added to the application. The Go HTML reader
rejects this PDF; checksum verification is not automatic role extraction.

## Automation implications

Keep the existing structured sources, with their actual scopes. GLEIF's
[relationship format](https://www.gleif.org/en/lei-data/access-and-use-lei-data/level-2-data-relationship-record-rr-cdf-2-1-format)
describes accounting-consolidation parents. It is not a people/employment registry.
SEC's [ticker directory](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
maps names, CIKs and tickers; the SEC does not guarantee its scope. Neither directory
alone supplies the missing person-role evidence demonstrated here. No new GLEIF or
SEC query was run for this sample, and absence from either registry was not asserted.

Company pages offer useful prose but need source discovery, publisher assessment,
role/alias extraction and separate FEC binding. Nevada's
[official site](https://www.nvcontractorsboard.com/) exposes licensing searches and
agenda archives; the tested agenda is jurisdiction- and industry-specific. This
review did not establish a reusable all-company officer feed or a production API
contract for that portal. Do not create a Nevada-only runtime exception for one donor.

The next bounded experiment should compare extraction methods on the **retained**
HTML, including prior affiliation examples and these new controls:

1. Emit source-backed person, organization, role and explicitly stated alias
   candidates with exact supporting spans. Keep every original source string.
2. Distinguish current claims, historical prose, publication dates and absent role
   dates. Keep employer context separate from identity and ownership.
3. Require abstention on the Moana about-page and Copaco-employment controls, and
   preserve the government role only under its stated organization.
4. Start with a deterministic baseline. If prose requires a local-model experiment,
   compare it on the same cases with held-out examples before any acceptance rule.
   The model may propose cited assertions; it cannot select the donor or allocate money.

This is a proposed next step, not an implemented extractor or a requirement to add
an LLM. Automatic page selection is a second explicit gap and must be tested before
claiming autonomous refresh. No new scheduler, graph publisher, source-service
deployment or full-corpus scan is authorized by this research result.

## Verification

The added Go regression test checks all retained body hashes, source text/alias
preservation, exact spans, JSON-LD limitations, missing-employer controls,
deterministic offline extraction and PDF rejection. Expected names and URLs are
test inputs only; no person-specific rule was added to runtime code. All approval
flags remain false.

The full Go suite and focused `go vet` check pass in the existing memory-capped,
network-disabled compiler container. Formatting, source hashes and updated relative
documentation links pass. No application runtime code changed in this investigation.
