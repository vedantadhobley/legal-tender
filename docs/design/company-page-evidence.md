# Company-page evidence extraction

Implemented 2026-09-15 under a
[draft source contract](../../contracts/sources/company/html-evidence/v1/contract.json).
Here, a company page means the company's own leadership, team or biography page,
not Wikipedia or Wikidata. Those remain separate discovery and evidence sources.

The Go reader extracts source text automatically from the three already retained
company pages in the [person-affiliation corpus](./person-affiliation-corpus.md).
It does not fetch pages, interpret prose as accepted roles or bind people to FEC
appearances. The original corpus, annotations and screening policy are unchanged.

## Implemented boundary

`internal/source/companypage.Extract` accepts complete HTML, an exact body digest,
an explicit HTTPS source URL and an observation day. The CLI reads a bounded regular
file. The URL/day are caller-supplied provenance, not independently verified HTTP
capture metadata; neither establishes company ownership or historical validity.
The first reader disallows credentials, URL queries and fragments.

The parser uses the pinned [Go HTML5 tokenizer](https://pkg.go.dev/golang.org/x/net@v0.59.0/html),
not regular expressions or company-specific CSS selectors. This adds `x/net` v0.59.0;
its module graph requires indirect `x/sys` v0.48.0. Both are locked in the Go module
files. There is no new service, Python data-plane code, LLM or embedding dependency.

Emitted entries preserve:

- Ordinary source text, headings and title text in source order. Inline character
  references decode; whitespace collapses in the derived text. Structural block
  boundaries separate runs. Inline tags do not invent spaces inside words.
- Metadata tags and canonical-link tags with decoded attributes. Duplicate
  attributes follow the tokenizer's first-occurrence HTML5 behavior; complete tag
  spans preserve the original bytes, including duplicate attributes.
- Exact JSON-LD script bodies, including invalid JSON. JSON validity is not schema
  interpretation, identity evidence or role acceptance. Remote contexts are not loaded.
- Excluded script, style, template, noscript, SVG and MathML regions, with source
  locators. An unclosed excluded region is retained with an issue.

Every entry has a zero-based, half-open byte interval and SHA-256 into the unchanged
body. JSON-LD also has an exact payload interval. Repeated text and metadata remain
separate occurrences, not independent corroborations. Preceding heading indexes
describe document order only; they do not assert that nearby names and titles belong
to the same person, or that a section proves a particular role.

This is **lexical source extraction**, not DOM construction or browser rendering.
CSS-hidden and responsive duplicate content may remain. There is no JavaScript
execution, asset loading, link following, automatic page selection or network access.
The original HTML remains the evidence authority; this projection is not a lossless
replacement for it. Missing text or structured markup does not prove missing roles.

Limits are 2 MiB/body, 100,000 tokens, 10,000 entries and 128 levels within an
excluded region. UTF-8, body pin and accepted charset declarations are required.
Exceeding a limit or corrupting input fails without a partial usable result; nothing
is truncated into apparent success. Source text remains untrusted data.

## Offline command

```sh
legal-tender pipeline entities extract-company-page \
  --body tests/fixtures/person-affiliation/ridgeline-leadership.html \
  --expected-body-sha256 c91e3c5785ddf31a229ac50c6757dca476d8c5acb5cdaab234369ce892be4151 \
  --source-url https://ridgeline.ai/company/leadership \
  --observed-on 2026-09-15
```

Output includes the executable digest, source inputs, contract and all entries.
Exit zero means extraction succeeded, not that an affiliation was accepted.
Identity, graph publication and financial-attribution approval remain false.

The optional `--propose-relationships` flag adds the separate
[prose syntax prototype](./prose-relationship-prototype.md). It preserves this
reader's output and default behavior. Its candidates are not accepted assertions.

## Retained-source result

The later [supplementary-source test](../audit/supplementary-affiliation-sources-2026-09-16.md)
replays four additional HTML bodies through this unchanged lexical reader. It
preserves explicit role/alias prose and missing-employer controls; no structured
person-role JSON-LD was present in those snapshots. A government profile is a format
test, not a claim that it is company-owned. The retained licensing PDF is unsupported
by this reader and remains reviewed evidence. Runtime behavior is unchanged.

Fresh CLI processes using the same development executable produce byte-identical
output for all three pages. This is not a production build or activation.

| Retained page | What survives automatic extraction |
|---|---|
| JC2 team page | Founder/CEO sentence, separate initial-bearing near name, growth title and emeritus wording |
| JC2 biography | Prior-role prose and original year/month wording, without fabricated day dates |
| Ridgeline leadership page | Board heading, reported person name and founder/chairman text, without nickname expansion or inferred control |

The complete source bodies and hashes are unchanged. The two JC2 pages each contain
two JSON-LD blocks; the Ridgeline page has none. These are observations about the
retained bytes, not claims about present website content or universal markup coverage.
None of those observations closes the FEC identity or historical-employment gap.

The real-source tests independently assert expected text and exact span hashes.
Synthetic cases test inline markup/entities, separate blocks, repeated content,
heading context, script exclusion, unclosed regions, invalid JSON, duplicate
attributes, unsupported encodings, limits, cancellation and bad provenance.
CLI tests cover deterministic replay, pins, flags, symlinks and credential-safe errors.
The full Go suite, `go vet`, focused race tests, a bounded fuzz run and the focused
source-contract metadata/fixture checks pass. The original affiliation/registry
fixtures and v1 outcomes remain regression-tested.

## Next boundary

2026-09-16 scope update: the approved prose prototype tests explicit role/name-form
syntax on seven development pages. Its later
[out-of-development evaluation](../audit/prose-extraction-evaluation-2026-09-16.md)
finds one of six reviewed roles and exposes interpretation gaps despite preserved
source text. The [first bounded model comparison](../audit/local-prose-model-comparison-2026-09-16.md)
now retains useful proposals and explicit citation/semantic failures. Keep both as
research baselines, not an accepted assertion/identity bridge. Do not turn nearby heading/title text into
accepted relationships. Page selection remains manual, same-name registry candidates
remain ambiguous, and observation dates do not prove employment at contribution time.

The [stronger-model comparison](../audit/stronger-prose-model-comparison-2026-09-16.md)
uses the same entries and leaves this reader unchanged. It improves some proposals
but still loses context and mislabels roles. The separate
[code-owned attachment experiment](../audit/prose-evidence-attachment-2026-09-16.md)
now retains complete supplied context and exact reader text/spans in Go. It does
not solve semantic interpretation, automatic source selection or identity acceptance.

The [mention/interpretation boundary](./prose-mention-interpretation.md) now reuses
this reader's spans in an offline annotated proof. Derived labels and context links
remain separate proposals; it adds no reader behavior or automatic semantic extraction.
The [fresh automatic-producer trial](../audit/prose-interpretation-model-2026-09-17.md)
uses the same reader on new official-page excerpts. All three real-source answers
are rejected; useful synthetic results do not establish general extraction. The
reader and production graph remain unchanged.

Automatic discovery of company-owned domains, general prose-role interpretation, independent
person/company identity, coarse role-date semantics and the screening bridge remain
open. This slice adds no affiliation edges, terminal definitions, dollar allocation,
production refresh or change to the funding graph.
