# Prose relationship candidate prototype

Implemented 2026-09-16 after the
[supplementary-source review](../audit/supplementary-affiliation-sources-2026-09-16.md).
This is an offline deterministic baseline over retained HTML, not general entity
recognition, donor resolution or accepted affiliation extraction.

## Boundary

`personaffiliation.ProposeProse` consumes verified
[company-page lexical evidence](./company-page-evidence.md). The source reader and
its contract are unchanged. The calculation uses policy
`prose-relationship-syntax-candidates.v1`; it contains no person, company, URL or
CSS-selector exceptions. Names and companies in regression expectations are test
data, not production rules.

The implementation recognizes two English text-block prefixes:

- Name **is/was** [not] [article] role **of/at/for/with** organization, ending
  at a period or block end, with optional explicit time wording.
- Name**,** role **of/at/for/with** organization**,**.

Names use capitalized token shapes; initials, Unicode letters, apostrophes and
hyphens are supported. Organization shapes use capitalized/number tokens and a
small set of connectors. These shapes do not prove entity types. Role vocabulary
includes explicit executive, founder, board, owner and ordinary-employee titles;
conjunctions preserve compound title text without deciding what it means.
This finite vocabulary and grammar are interpretation choices, not source schema.
Unsupported casing, titles, sentence structures and abbreviations may be missed
or segmented incorrectly. No precision or recall claim follows from these rules.

Parenthetical forms such as `Rita (Ri) Anne Jones` can occur inside a block. Output
retains the leading, parenthetical and following name strings separately. It does
not synthesize a canonical name, expand a nickname or join that name to another page.

Every candidate retains:

- Source URL, observation day and exact body hash from the lexical input.
- Entry index and the original entry's half-open HTML byte span and span hash.
- Matched wording and field byte offsets in **Entry.Text**, not in the HTML.
  Entity decoding and inline tags make those coordinate systems different.
- Original predicate, title, organization and optional time wording. `was not`
  stays `was not`; an appositive comma does not become a present-tense assertion.

The full lexical evidence accompanies proposals in CLI output. Every text entry
gets a candidate count and outcome; other entry kinds are counted but not interpreted.
Repeated occurrences remain separate. A zero-candidate result means unsupported or
absent syntax, not proof that a relationship does not exist.

## What a candidate does not establish

All results retain `identity_approved=false`, `graph_publication_approved=false`
and `financial_attribution=false`. Candidate meaning is explicitly unverified.
The grammar does not interpret surrounding denial, quotations, hypothetical context,
publisher reliability or website ownership. For example, a later sentence denying
a matched first sentence remains unassessed in the accompanying source entry.
No downstream consumer may treat a syntax match as a verified positive assertion.

The baseline does not join headings, cards, adjacent titles, pronouns or page names.
It does not label an organization as a corporation or convert a public-board role
into employment, ownership or executive authority. It does not choose among namesakes.

Role validity stays unknown. Explicit `since/from/until/in/as of` wording is retained
without conversion into dates or tenure intervals. Present tense, page metadata and
observation dates never establish a role at contribution time. Identical wording on
two pages is not independent corroboration.

No fetching, model call, new dependency, PDF adapter, graph write or money calculation
is added. The bounds come from the existing 2 MiB/10,000-entry lexical reader.
The pure calculation expects that reader's verified output; it does not authenticate
caller-constructed evidence values independently.

## Opt-in command

```sh
legal-tender pipeline entities extract-company-page \
  --body tests/fixtures/person-affiliation/supplementary-v1/rick-reviglio.html \
  --expected-body-sha256 c0a0ed7fc8cd9729aa775c98d423d9ecf70175f8cda3560e94b2fba63902e7ef \
  --source-url https://goblueteam.com/rick-reviglio/ \
  --observed-on 2026-09-17 \
  --propose-relationships
```

Without the flag, output remains lexical evidence only. With it, `proposals` is
added alongside the unchanged `evidence`. Pin failures still emit no usable result.
The [automatic affiliation report](./affiliation-enrichment.md) does not consume
these proposals; no identity rule or source-selection behavior changed.

## Retained-source evaluation

These are replay measurements over seven development fixtures, not a held-out
accuracy evaluation or claims about current website content. The original bodies
and their hashes are unchanged. Role dates are unknown in all four role candidates.

| Retained body | Role candidates | Parenthetical forms | Observed boundary |
|---|---:|---:|---|
| JC2 about page | 1 | 0 | John Chambers; founder and CEO; JC2 Ventures |
| JC2 biography | 1 | 0 | Same role wording; not independent corroboration |
| Ridgeline leadership | 0 | 0 | Separate name/title layout remains unsupported |
| Rick Reviglio biography | 1 | 0 | President and General Manager; Western Nevada Supply |
| Jack Reviglio biography | 0 | 2 | Theodore (Ted) Ray Reviglio; Richard (Rick) John Reviglio |
| Moana culture page | 0 | 0 | Company context does not supply a person-role clause |
| Georgia corrections profile | 1 | 0 | Alton Russell; Member at Large; GDC Board of Corrections, not Copaco |

Regression tests bind these expectations to the retained source hashes, check exact
text coordinates and deterministic replay, and keep approval flags false. Synthetic
tests cover inline HTML, Unicode, entities, punctuation, negation, explicit time
wording, missing dates, wrong organizations, namesakes, ordinary employees, repeated
occurrences, unsupported layouts, cancellation and invalid input. CLI tests cover
opt-in behavior and preservation of body verification.

The full Go suite, `go vet ./...` and focused race tests for the person-affiliation
calculation and CLI pass in the existing memory-capped, network-disabled compiler
container. A fresh development executable replays the Rick biography in two separate
processes with byte-identical output and the expected source span. Formatting and
updated documentation-link checks pass. No production build or activation was run.

## Next decision

The [out-of-development evaluation](../audit/prose-extraction-evaluation-2026-09-16.md)
now finds only one of six reviewed role witnesses in five readable bodies, with
another selected page failing retrieval. The grammar and lexical reader stayed
unchanged. This diagnostic result rejects the baseline as a general extraction
method; it does not establish population precision/recall or alias accuracy.

The [first local-model trial](../audit/local-prose-model-comparison-2026-09-16.md)
now compares selected source excerpts and controls with the unchanged grammar.
Gemma recovers some missed wording but emits citation and semantic errors; its
nine literal passes are not nine accepted relationships. The
[same-task stronger-model run](../audit/stronger-prose-model-comparison-2026-09-16.md)
now yields eleven literal passes and three rejections, with better abstention but
remaining context and role-meaning failures. Neither profile is ready for acceptance.
The [code-owned attachment test](../audit/prose-evidence-attachment-2026-09-16.md)
now preserves original entries and full supplied context without model-rewritten
quotations. Fresh controls still expose pronoun, role-scope and retraction errors.
The [mention/interpretation contract proof](./prose-mention-interpretation.md)
now tests that separation with reviewed annotations and explicit context links.
It does not automatically produce those interpretations or fix model extraction.
The [fresh automatic-producer trial](../audit/prose-interpretation-model-2026-09-17.md)
now retains five literal/reference passes and four rejections, including all three
real-source cases. The [offline citation-selection proof](./prose-citation-selection.md)
now supplies code-owned token ranges and full-entry context. The
[token-ID trial](../audit/prose-token-selection-model-2026-09-17.md) now measures higher
input use, persistent reference/semantic errors and one capacity failure on known
cases. Do not promote the producer or equate exact citations with accepted meaning.
Do not add a source-specific exception for each page. Autonomous page discovery and
publisher assessment remain separate gaps. Neither extraction nor discovery authorizes
an accepted donor identity, affiliation edge or financial allocation.
