# Unchanged prose extraction evaluation

Reviewed 2026-09-16 America/New_York; new source bodies observed 2026-09-17 UTC.
The user approved evaluating the
[prose prototype](../design/prose-relationship-prototype.md) on pages outside its
development fixtures before connecting it to matching.

Subsequent work: the user-approved [local-model comparison](./local-prose-model-comparison-2026-09-16.md)
now retains the first bounded Gemma trial and its failures. This document preserves
the preceding unchanged-grammar evaluation.

## Result and decision

The deterministic baseline is too narrow to serve as the general role-extraction
method. Across five readable bodies, it emits one role candidate and no alias
candidates. It finds one of six manually reviewed role witnesses and misses five.
All six witnesses survive lexical extraction; the losses are in interpretation,
not missing raw source text. One additional selected page could not be retrieved.

The only emitted candidate has the reviewed person, title and organization. No
misleading candidate was observed, but one output cannot establish precision.
Alias accuracy is unmeasured: this selection supplies no reviewed positive alias
witness. Neither result is a donor-identity or receipt-time acceptance result.

Keep the existing grammar as an explicit baseline. Do not connect its proposals to
donor matching or grow a page-specific exception chain. The measured variation now
justifies a bounded comparison with source-cited local-model extraction. That is a
recommended next experiment, not a model deployment or accepted production design.
No model call, parser change, new runtime adapter, graph write or financial rule was
added in this evaluation.

## Selection and limits

The source list was fixed before replay. It includes four manually discovered
official-site URLs from companies outside the original seven-page fixture set,
plus both existing dated-role snapshots omitted from that set. The latter had
already been reviewed for another purpose; they are not unseen to the project.
Specific role witnesses were reviewed after the first output. This is a diagnostic
out-of-development test, not blinded review, random sampling or an exhaustive gold
corpus. Do not report the witness ratio as population recall.

The [selection record and source review](../../tests/fixtures/person-affiliation/prose-evaluation-v1/README.md)
retain initial code digests, exact source pins, all fetch outcomes, expected output
counts and reviewed witness spans. The implementation hashes remained unchanged:

- Prose grammar: `b719da5a1dffc21f0b3f635347d68345379c75347f97a8f8ca50a84a28516e09`.
- HTML reader: `a81649084f3270c26c61e11dbae865bb54b472143ddb8236663222f20cb447d0`.

No FEC appearance or donation by the new page subjects is asserted. Their pages
test extraction shapes only. Existing FEC, discovery and reviewed-role fixtures
remain unchanged; no missing source was replaced with a successful alternative.

## Source-level observations

| Retained source | Output role candidates | Reviewed witness result | Interpretation gap |
|---|---:|---|---|
| [Microsoft biography](https://news.microsoft.com/source/exec/satya-nadella/) | 1 | Nadella/Microsoft title found | Direct supported clause; later appointment wording remains outside the candidate |
| [IBM biography](https://newsroom.ibm.com/Arvind-Krishna) | 0 | Krishna/IBM title missed | Comma-separated compound title |
| [Ford leadership directory](https://corporate.ford.com/about/leadership/) | 0 | Farley and Ford titles both missed | Names and titles in separate cards/blocks; must not cross-assign adjacent cards |
| [Ridgeline announcement](https://ridgeline.ai/company/news/ridgeline-names-dave-blair-co-ceo-expands-executive-leadership-team) | 0 | Duffield co-CEO/founder wording missed | Organization and role precede the name inside a quote attribution |
| [Sprinklr proxy](https://investors.sprinklr.com/financial-information/all-sec-filings/content/0001140361-23-023208/ny20007009x1_def14a.htm) | 0 | Chambers/JC2 CEO wording missed | Subject in an earlier sentence; pronoun introduces the role; other companies and dates share the paragraph |

These observations describe the retained snapshots, not present-tense assertions
about the people. The source review binds each selected witness to raw HTML byte
spans and lexical entry indexes. Ford's parent card structure was inspected in the
original HTML; the reviewer pairing is not an implemented cross-block joining rule.

The emitted Microsoft candidate retains `role_validity_unknown`. The biography's
separate appointment wording does not automatically date the combined title.
The historical Ridgeline and Sprinklr texts remain subject to the earlier
[date-meaning review](./dated-person-role-evidence-2026-09-16.md); no observation,
page, filing or founding date becomes a tenure boundary.

## Acquisition outcome

Three new bodies total 1,856,017 bytes. Microsoft and IBM returned HTTP 200 at the
requested URLs. Ford redirected from its `.html` URL to the trailing-slash URL,
which returned HTTP 200. Both URLs are recorded. Requests used HTTPS only, a 2 MiB
body ceiling, bounded redirects and a time limit, without credentials, assets,
JavaScript or recursive fetching. The research log records selected transport
metadata, not full HTTP response headers or a production capture manifest.

The selected [AMD biography](https://www.amd.com/en/corporate/leadership/lisa-su.html)
failed with an HTTP/2 stream error. One explicit HTTP/1.1 retry timed out after
20 seconds with zero bytes. No usable body or HTTP status was obtained. Its result
is `not_evaluated_no_retained_body`, not zero roles or evidence of absent leadership.
Search-visible content was not substituted for original HTML.

## Repeatable evaluation

Run `go test -v ./internal/calculation/personaffiliation -run TestProseOutOfDevelopmentEvaluation`
in the project's offline Go test container. It verifies original body pins, witness
span hashes, source text, entry counts, every emitted candidate, explicit misses,
unknown role time, unchanged approval flags and deterministic replay. Retrieval
failures stay separate and cannot silently become successful empty sources.

Passing this regression means the recorded behavior is reproducible, including
known weaknesses. It is **not** a successful extraction-quality gate. No runtime
code consumes the review labels. Source selection and semantic review were manual;
the extraction and replay are in Go.

The full Go suite, focused `go vet` and prose race tests pass in the existing
memory-capped, network-disabled compiler container. Development CLI replay and
the test runner agree on every source count and candidate. Runtime implementation
hashes, formatting and updated documentation links also pass verification.

## Next experiment

Compare a bounded local-model proposal method against these exact cases and the
earlier namesake, wrong-organization, negation and date controls. Require exact
supporting entry references and quoted source text; reject invented citations and
keep identity and money approval false. Measure subject/organization binding,
unsupported claims, abstentions and date invention separately, not just output count.
Card context may need a generic structural representation; concatenating nearby
names and titles is not sufficient evidence. No company-specific selectors.

This set is now known evaluation material. Any approach tuned on it needs fresh
examples for a later generalization check. Automatic page selection and reliable
acquisition remain separate gaps; this test does not solve autonomous refresh.
