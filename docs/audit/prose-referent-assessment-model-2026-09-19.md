# Grounding-bound referent assessment comparison — 2026-09-19

The narrow assessment experiment completed on `gemma-4-31b` and
`gpt-oss-120b`. Separating caller-owned grounding from model-supplied referent and
correction-target assessments materially improves structural reliability, but the
remaining mention-versus-antecedent errors still prohibit production use. No identity,
relationship, graph edge or financial attribution is approved.

## Boundary

The rejected combined producer asked one response to recreate lexical grounding,
roles, referents and correction targets. This experiment instead supplies:

- complete source entries;
- the unchanged automatic name-candidate catalog;
- one exact grounding object that already passed the existing Go validator; and
- a SHA-256 digest of that grounding.

The response schema has only `referents` and `corrections`. It has no field for
grounding, roles, citations, status, provenance or approval. Go requires one endpoint
assessment for each supplied role and one target assessment for each supplied
correction, then joins the answer to the caller-owned grounding. A model response
cannot use `unassessed`; uncertainty must be `unresolved` or `ambiguous`. Invalid
answers are rejected whole without repair or partial salvage.

Six groundings are independent reviewed fixtures. Four are unchanged structurally
valid proposals from the earlier grounding trial. Their distinct origins remain in
the joined report. The ten-case manifest fixes three retained real-source excerpts,
five retained synthetic controls and two additional synthetic controls, along with
each grounding digest and pre-run semantic checks. These remain known diagnostics,
not a held-out accuracy estimate.

## Controls

Both runs used the same prompt, inputs, dynamic JSON Schema, seed `1`, 4,096-token
output ceiling, 900-second request deadline and sequential ten-request order. Gemma
used `reasoning_effort: none`; GPT-OSS used `medium`. Model IDs and supported controls
were checked through the live gateway before inference. There were no retries,
response repairs or output normalization.

| Model | HTTP 200 | Structurally valid | Semantic checks met | Usable | Real-source usable | Elapsed sum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Gemma 31B | 10/10 | 9/10 | 6/10 | 6/10 | 1/3 | 277.712 s |
| GPT-OSS | 10/10 | 9/10 | 6/10 | 6/10 | 1/3 | 214.030 s |

`Usable` requires both structural validity and the independent semantic review. It
still means an unverified proposal, never an accepted identity.

The task split removes most of the combined producer's structural failures: that
producer passed four of eight answers per model, while the narrow stage passes nine
of ten. It also produces the first usable retained real-source answer in this line
of experiments. That improvement is bounded evidence for task separation, not a
general quality estimate.

## Remaining failures

Both models fail the same two real-source distinctions:

- A surname-grounded role selects the short surname mention instead of the supported
  full-name antecedent.
- A role using a short organization name selects that occurrence, or marks the full
  and short name forms ambiguous, instead of proposing the supported full-name
  antecedent.

These answers are structurally valid. The problem is now isolated: candidate IDs
still represent literal mention occurrences, while the task asks for semantic
referents. A short mention can therefore be returned as its own supposed referent.
Adding person- or company-specific prompt instructions would hide this representation
problem and is rejected.

Both models also leave a clearly stated endpoint unresolved in the advisory-body
control. Gemma's multi-role answer is rejected because it marks empty candidate arrays
as `proposed`. GPT-OSS's nearby-name answer is rejected because it supplies candidate
IDs while marking both endpoints `unresolved`. The strict cardinality checks correctly
prevent either contradiction from entering the joined report.

Both models preserve the two-person ambiguity control, the anonymous endpoint and
selective correction target, the additional pronoun/employer-change control, and the
additional selective-correction control. Those successes show that the narrow shape
can express the required states; they do not validate arbitrary prose or identities.

## Decision and next boundary

Keep the narrow, grounding-bound request shape and exact Go join as the research
boundary. Do not promote either model or this test adapter into production. The next
work should be offline contract design that separates:

1. a literal mention occurrence;
2. a proposed antecedent or mention-equivalence group; and
3. a later canonical entity candidate.

That design must preserve ambiguity and provenance without merging spellings by rule.
Only after that contract has independent counterexamples should another model trial
be considered. Autonomous page selection, source refresh, donor identity acceptance,
graph publication and monetary attribution remain separate gates.

## Retention and verification

Exact captures, completion markers, model inventories, grounding hashes and manual
findings are under
[`referent-assessment-model-v1`](../../tests/fixtures/person-affiliation/referent-assessment-model-v1/).
`prose_referent_assessment_replay_test.go` reconstructs every request, validates the
same grounding and candidate inputs, checks capture hashes and token totals, reruns
strict joins, and verifies false approval flags entirely offline.

Full Go tests, focused vet, race tests, JSON validation and formatting pass; the new
relative link targets exist. Production source, identity, graph and money packages
are unchanged.
