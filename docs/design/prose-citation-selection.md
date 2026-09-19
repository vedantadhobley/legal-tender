# Source-owned citation selection

Implemented 2026-09-17 as an **offline, test-only proof** after the
[fresh interpretation-model trial](../audit/prose-interpretation-model-2026-09-17.md).
Go can reconstruct citations from source-provided token IDs and attach complete
selected entries. That initial proof added no model call, prompt, production parser or graph rule.
This proves the representation, not that a model selects the correct evidence.

The later [token-ID model trial](../audit/prose-token-selection-model-2026-09-17.md)
now tests automatic selection on the same retained excerpts. It adds a test-only
producer but does not establish a reliability gain or approve any affiliations.
The subsequent offline reference-list simplification below does not change that
producer, its saved responses or its rejection results.

## What changed

The earlier contract asked a model to reproduce literal text and sometimes specify
a UTF-8 byte offset. The new research boundary asks for a selection:

```json
{"id":"subject","entry":0,"first_token":0,"last_token":1}
```

The token IDs are provided with source text; they are not byte positions to count.
IDs are local to the exact supplied source/entry, not persistent entity identifiers
or references to reuse after a source changes. Go owns the text and offsets.

The catalog groups consecutive Unicode letters, numbers and combining marks into
lexical tokens. Each other non-whitespace rune is a separate token. It does not
recognize people, companies, sentences, roles or model-specific tokens. Punctuation
and accents stay intact. This simple rule needs no entity model, new dependency,
company selector or name dictionary.

A selection is one inclusive token range in one entry. Go slices the original
decoded entry text from the first token's start through the last token's end,
preserving every intervening character. It cannot join disjoint clauses or span
entries. Caller-supplied quotes and offsets are rejected. An exact substring inside
a token is not rounded outward; finer selection remains unsupported.

The adapter feeds these Go-generated literal mentions into the unchanged
[interpretation validator](./prose-mention-interpretation.md). Normalized labels,
binding, roles and contextual links remain supplied interpretations. Go attaches
the complete entries referenced by each interpretation, in source order and without
duplicates. All supplied excerpt entries also remain in the overall report, including
correction context selected by another interpretation. The raw HTML span and decoded
text offsets remain different coordinate systems.

The catalog requires output from the verified lexical reader. Like the existing
audit helpers, it does not independently authenticate caller-invented source spans.
Caller-owned provenance distinguishes reviewed/synthetic tests from model proposals; the proposal
cannot claim reviewed origin or set approval flags.

## Go-derived required references

The follow-up offline contract removes the redundant `evidence` field from each
interpretation. The caller still supplies `clause`, `subjects`, `organizations`
and an explicit `additional_evidence` array, which may be empty. It still selects
all source ranges and supplies all meaning, binding, role and context-link claims.

Go derives the required evidence list as a stable union: clause first, subjects
in supplied order, organizations in supplied order, then additional evidence.
For example, clause `role`, subject `person`, organization `company` and extras
`["date", "company"]` produce `["role", "person", "company", "date"]`.
Only the derived evidence list is deduplicated. Subject/organization arrays remain
unchanged; duplicate endpoint IDs and multiple alternatives marked `direct` still
fail the existing validator. Repeated extra references are preserved in the supplied
proposal but occur only once in the derived list.

`source-token-derived-references.v1` retains `supplied_proposal` separately from
`derived_review`, which uses the unchanged citation and interpretation validators.
Unknown IDs, missing/null required arrays, invalid ranges and forged quote,
provenance or approval fields still reject the entire input. The additional and
endpoint lists each have a 64-reference bound; existing overall limits still apply.
The old `evidence` field is rejected by this new contract, not silently migrated.

Extra references attach full source context, not implicit semantics. Selecting a
correction passage does not invent a retraction link. Ambiguous alternatives stay
ambiguous, absent unresolved endpoints stay absent, and all approvals remain false.
The tests deliberately pass a wrong subject and an insufficient role clause to
show that valid references still do not establish correct meaning.

This is a small test-only adapter with independent synthetic annotations. It adds
no prompt, model call or automatic response conversion. Old captures still replay
through the old contract with the same passes and failures. No accuracy improvement
has been established for this new shape. The later
[fresh producer trial](../audit/prose-derived-reference-model-2026-09-17.md) tests it
without changing the adapter or earlier model outcomes.

## Saved-failure diagnostic

A separate exact-match diagnostic enumerates **all** occurrences of each saved
literal query. It returns unique, ambiguous or not-found state without choosing a
first match. Overlapping matches remain visible. Case, accents, Unicode composition,
whitespace and punctuation are not normalized. Matches inside lexical tokens remain
explicit but have no selectable token range.

Against the unchanged nine-case trial, the eight complete answers contain 56 queries:

| Outcome | Queries | Meaning |
|---|---:|---|
| Unique exact occurrence | 51 | Coordinates can be enumerated; meaning is still unverified |
| Ambiguous exact occurrence | 3 | Both occurrences are retained; none is silently selected |
| Not found | 2 | One spliced clause and one whitespace alteration remain invalid |

The ninth generation was truncated and still has no usable answer. These are query
diagnostics, **not improved answer scores**. The original five literal passes and four
rejections replay unchanged. No old output is repaired or converted into an accepted
new-format answer.

Independently reviewed selections over two retained synthetic sources show that:

- Selecting a pronoun still attaches the complete role/date entry and antecedent
  entry. It does not prove that the pronoun refers to the proposed subject.
- A role, an unrelated role and a correction retain original text and separate
  context. The supplied retraction blocks only its target, without changing the
  original assertion status or approving the unrelated role.

Those selections are test annotations, not automatic extraction. Tests deliberately
show that an incorrect subject or insufficient selected clause can remain structurally
valid. A correct quote can support a wrong interpretation; no approval follows.

## Bounds and cost

The research catalog allows at most 32 entries, 64 KiB decoded text and 4,096 lexical
tokens. Proposals keep the earlier 128 KiB input, 64 mentions, 32 interpretations and
32 context-link bounds. A diagnostic query allows 64 occurrences; exceeding a limit
fails without a truncated or partial result. These are test resource limits, not
domain policy.

The explicit token catalog increases input size. For the three real excerpts, its
JSON is 3,393–4,024 bytes versus 823–899 bytes for the previous entry input. These are
bytes, not model tokens. The subsequent trial measures increased prompt-token use
and essentially unchanged completion use, with reference and semantic errors still
present. It does not establish a latency or model-accuracy improvement.

## Verification and next step

Implementation is confined to `internal/audit/personaffiliation/prose_citation*_test.go`.
Tests cover exact Unicode spans, punctuation, combining marks, repeated/overlapping
matches, partial-token matches, altered text, missing/invalid references, forged
quotes/provenance, budgets, complete context and unchanged earlier model outcomes.
The derived-reference tests add deterministic union order, explicit extra context,
proposal preservation, ambiguity, selective correction/conflict handling and
whole-answer rejection without accepting semantics.
Full Go tests, focused static/race checks and documentation-link checks pass.
Earlier reader, grammar, prompts and captures are unchanged.

The [live trial](../audit/prose-token-selection-model-2026-09-17.md) now supplies
that bounded producer and keeps its results separate from this offline proof.
The redundant-reference change passes offline, and the fresh live follow-up now
retains four structural passes, one truncation and one joint-activity contract
rejection across six cases. Endpoint errors remain in passing answers. Isolated
person/company name-span selection was tested in the subsequent
[names-only trial](../audit/prose-name-selection-model-2026-09-17.md): known cases
recover their reviewed spellings, while fresh cases retain omissions and extra
selections. The [role-binding follow-up](../audit/prose-role-binding-model-2026-09-17.md)
now reuses those uncorrected candidates and adds two fresh controls. All ten outputs
pass reference checks, but wrong direct/coreference labels, role grouping and
incomplete selected evidence remain. Ambiguous, hypothetical and missing endpoints
are preserved in the fresh controls. Address the documented interpretation-contract
gaps next; reference validity does not establish correct meaning or accepted identity.
The subsequent [whole-entry grounding experiment](../audit/prose-role-grounding-model-2026-09-17.md)
adds explicit endpoint surfaces and complete claim-context citations. Its fresh
results retain semantic ambiguity/correction failures as well as a structural
rejection; literal correspondence and referent resolution remain distinct work.
Autonomous discovery, accepted affiliations and graph publication remain separate.
