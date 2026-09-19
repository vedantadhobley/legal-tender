# Fresh automatic interpretation trial — 2026-09-17

The automatic producer can populate parts of the
[separated mention/interpretation contract](../design/prose-mention-interpretation.md),
but this profile does not yet provide reliable source-grounded extraction.
Five of nine answers pass literal/reference checks. Four are rejected, including
**all three real-source cases**. No answer becomes an accepted affiliation.

## Fixed experiment

One batch used live-discovered `gpt-oss-120b`, medium reasoning, 4,096 output tokens,
seed 1 and advertised sampler defaults. The task, excerpts, schema and validator
were fixed before inference. There were no retries, post-output prompt changes,
manual field repairs or partially salvaged answers. All requests returned HTTP 200;
one generation ended at the token limit and eight finished normally.
Total request time was 386.578 seconds; the harness completed in 386.60 seconds.
Its `PASS` and `completed.json` mean all attempts were recorded, not that extraction
passed. A seed does not guarantee identical future generation.

The [retained fixture](../../tests/fixtures/person-affiliation/mention-model-v1/README.md)
contains every request/response, model discovery, derived report when valid, original
HTML and the pre-run input expectations. Expectations are never sent to the model.
Manual post-run review records binding, classification, context and name/time
separately. Earlier model runs and reviewed annotation fixtures are unchanged.

Three manually selected official pages supplied new excerpts:

- [Apple biography](https://www.apple.com/leadership/tim-cook/): entries 68 and 72.
- [NVIDIA biography](https://www.nvidia.com/en-gb/about-nvidia/board-of-directors/jensen-huang/): entry 737.
- [Salesforce biography](https://www.salesforce.com/company/marc-benioff-bio/): entries 56 and 69.

These are full entries from the unchanged lexical reader, not full-page model
inputs. The selected indexes can omit intervening entries; this is a controlled
excerpt test, not a claim of complete document context. Complete HTML is retained.
Salesforce redirected to `?bc=HL`; the fixture records that effective URL separately
from the query-free requested URL accepted by the current reader.

Six newly written synthetic controls exercise known classes of failure. Fresh text
does not make this a representative or statistically independent benchmark. The
task and cases differ from previous trials; their pass counts are not comparable
accuracy scores. No claim about actual donations by these page subjects is made.

## Outcomes

| Case | Literal/reference outcome | Separate semantic review |
|---|---|---|
| Apple excerpt | Rejected: token limit, empty final content | No final interpretation to assess; internal reasoning is not an answer |
| NVIDIA excerpt | Rejected: repeated text without an occurrence offset | Roles and awards mostly distinguished; an additional mention stitches noncontiguous job wording, short-name binding is incomplete, and two executive titles are combined |
| Salesforce excerpt | Rejected: repeated organization without an occurrence offset | Reviewed roles appear in raw output; short-name/pronoun binding is not explicitly resolved |
| Bound pronoun | Pass | CTO binding and activity distinction are useful, but the role's selected clause is only a pronoun, not the supporting statement |
| Ambiguous pronoun | Pass | Both candidate subjects retained; attendance is activity, not employment |
| Targeted correction | Rejected: changed whitespace | Raw retraction points to the intended role and preserves the unrelated role; altered text still invalidates the whole answer |
| Name and succession | Pass | Founder/CEO/board roles and succession remain distinct; no alternate-name proposal emitted |
| Contradiction | Pass | Both same-period claims retained and context-blocked; neither is selected as truth |
| Conditional instruction | Pass | Hypothetical/denied statuses retained and source instruction ignored; selected affirmative clause omits condition/date wording |

The NVIDIA answer also rewrites one clause rather than copying contiguous text.
The correction answer changes one ordinary space into a nonbreaking space. These
are literal failures, even when a reader can infer the intended statement. We did
not normalize them away. Valid cases can still select insufficient evidence; the
complete source entries remain available but do not prove interpretation correctness.

## Code boundary

The opt-in test uses the existing bounded live-capture helper. A new prompt/schema
returns only mentions, interpretations and context links. The Go wrapper stamps
`model_proposal` origin and rejects forged provenance or approval fields. It calls
the unchanged literal/reference rules and publishes no partial report on rejection.
The existing capture field `citation_check` remains a literal/structural result,
not a semantic verdict. Go supplies original context, source identity and spans.

The model is still responsible for proposed meaning: pronoun resolution, role
classification, clause selection and correction/conflict targets. Go validates
references but does not validate that a referenced clause supports a normalized
label. The bound-pronoun case demonstrates this limitation directly.

All new producer/replay code lives in `internal/audit/personaffiliation/*_test.go`.
Normal tests are offline. No production model client, source parser, grammar,
FEC identity policy, graph publisher, financial calculation or deployment changed.

## Verification and next decision

Retained replay rebuilds each excerpt from the original HTML, checks the exact
request, re-runs validation and compares the derived report or rejection. It also
keeps model provenance and every approval flag false. Wrapper tests reject forged
origins, extra fields, duplicate keys, missing arrays and oversized input; they
explicitly demonstrate that structurally valid but wrong semantics can pass.

The full Go suite, focused `go vet`, focused race tests, formatting and doc-link
checks pass. Earlier prompts, the lexical reader and grammar retain their hashes.

Do not promote this producer or start an infrastructure build. The next bounded
change to consider is **code-owned occurrence selection and full-entry grounding**:
enumerate literal matches in Go, retain ambiguity, and attach complete supporting
entries without asking the model to count bytes or rewrite clauses. Do not silently
pick a first occurrence, normalize away a mismatch, or turn that change into semantic
approval. Test the contract offline before another live batch. Short-name binding,
evidence sufficiency and generation-budget failure remain separate open issues.

Autonomous page discovery, donor identity acceptance, affiliation publication and
terminal-dollar policy remain outside this experiment.

Follow-up: the [offline citation-selection proof](../design/prose-citation-selection.md)
now supplies Go-owned token ranges and full-entry context. It diagnoses repeated and
missing literal queries without repairing these answers; this trial's outcomes remain
unchanged, and no subsequent model run is included in that proof.
