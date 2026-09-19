# Unchanged prose-baseline evaluation

Selected 2026-09-16 local time, before running the extractor on these pages.
The four new URLs were discovered manually on official company sites. The two
existing snapshots were reviewed for earlier date research but were not among the
seven prose-development fixtures. This is not blind review, random sampling,
automated discovery or a population accuracy benchmark.

Frozen implementation: `prose-relationship-syntax-candidates.v1`.
The starting SHA-256 of `internal/calculation/personaffiliation/prose.go` is
`b719da5a1dffc21f0b3f635347d68345379c75347f97a8f8ca50a84a28516e09`.
The starting lexical-reader SHA-256 is
`a81649084f3270c26c61e11dbae865bb54b472143ddb8236663222f20cb447d0`.
Do not tune either implementation during this evaluation.

## Fixed source selection

- [Microsoft biography](https://news.microsoft.com/source/exec/satya-nadella/)
- [AMD biography](https://www.amd.com/en/corporate/leadership/lisa-su.html)
- [IBM biography](https://newsroom.ibm.com/Arvind-Krishna)
- [Ford leadership directory](https://corporate.ford.com/about/leadership.html)
- [Existing Ridgeline announcement](../dated-roles/ridgeline-co-ceo.html)
- [Existing Sprinklr proxy](../dated-roles/sprinklr-proxy-2023.html)

New bodies are complete bounded HTTPS downloads, not reconstructed snippets.
Existing bodies stay in their original locations. Failed acquisition or unsupported
input remains an evaluation outcome; do not replace it with a more convenient page.
No FEC appearance or donation by these new page subjects is asserted. Extracted
roles, aliases, person identities and financial attribution remain unapproved.

## Recorded evaluation

The [review log](./review.json) records three successful new bodies, the unchanged
two earlier bodies and both failed AMD attempts. It includes exact source/witness
spans and reviewed outcomes, not runtime person rules. Witness review followed the
first output and does not constitute an independently labeled accuracy benchmark.

Replay with `go test -v ./internal/calculation/personaffiliation -run TestProseOutOfDevelopmentEvaluation`
in the project Go container. It reproduces one found role and five missed witnesses,
with retrieval failure outside that denominator. Passing the test preserves known
weaknesses; it does not approve the extractor for donor matching. The
[evaluation report](../../../../docs/audit/prose-extraction-evaluation-2026-09-16.md)
explains limitations and the next comparison.
