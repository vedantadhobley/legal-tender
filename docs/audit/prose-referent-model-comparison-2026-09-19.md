# Referent and correction-target model comparison — 2026-09-19

The fixed eight-case trial completed on the live `gemma-4-31b` and
`gpt-oss-120b` models. The new representation helps expose ambiguity and
correction intent, but neither model produced a usable answer on any of the
three real-source cases. This does not justify production use, identity
acceptance, graph publication or financial attribution.

## Scope and controls

The [pre-run case manifest](../../tests/fixtures/person-affiliation/referent-model-v1/cases.json)
contains three retained real company biographies and five known synthetic
regressions. Original automatic name captures were reused byte-for-byte, including
their omissions and scope errors. Review checks did not enter requests. This is a
known-case diagnostic, not a fresh, held-out or end-to-end accuracy estimate.

Both runs used the same prompt, source-token catalog, JSON Schema, seed `1`,
8,192-token output ceiling and sequential eight-request order. Gemma used its
advertised default `reasoning_effort: none`; GPT-OSS used its advertised default
`medium`. The real IDs and controls were discovered from the live gateway before
the run. There were no retries, output repairs or partial acceptance.

The old research client stopped requests after 90 seconds. An interrupted attempt
and a first restart proved that limit was too short: completed Gemma outputs took
up to 247 seconds. The harness now accepts an explicit bounded timeout and records
it in the completion marker. The final comparison used 900 seconds per request,
below Joi's published 30-minute backend deadline. No final request timed out; all
sixteen returned HTTP 200 with `finish_reason: stop`.

## Results

`Structural valid` means the complete response passed strict source/reference,
grounding, referent and correction-target validation. `Semantic checks met` is a
manual review of the raw answer against the pre-run checks, even when the answer
was structurally rejected. `Usable` requires both. A usable research answer still
does not approve an identity or relationship.

| Model | HTTP 200 | Structural valid | Semantic checks met | Usable | Sum elapsed | Prompt / cached | Completion |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Gemma 4 31B | 8/8 | 4/8 | 6/8 | 3/8 | 933.862 s | 18,834 / 11,083 | 9,803 |
| GPT-OSS 120B | 8/8 | 4/8 | 4/8 | 2/8 | 460.415 s | 18,012 / 17,518 | 22,781 |

GPT-OSS finished in about half the summed request time but emitted more than twice
as many completion tokens. These are one-run measurements under different native
reasoning modes, not a general speed or quality benchmark.

| Case | Gemma 31B | GPT-OSS |
| --- | --- | --- |
| Microsoft / Hood (real) | Rejected; raw roles are separated, but selections are malformed and surname referents remain wrong | Rejected; selections and referents exist, but the interpretation array is empty |
| AMD / Su (real) | Rejected; raw role history and full-name referents are useful, but focus/surface references are malformed | Rejected; eleven dangling referents and no interpretations |
| AMD / McClure (real) | Rejected; raw roles and scope are useful, but focus/surface references are malformed | Structurally valid, but every referent is `unassessed` and several finance roles are overclassified |
| Multiple roles | Usable; four separate roles, employers, statuses and referents | Structurally valid, but invents a correction from the founder negation |
| Council scope | Structurally valid, but omits advisory membership and invents a correction | Usable; advisory, denied board and bare director scopes remain separate and conservative |
| Ambiguous surname | Usable; literal surname is separate from two semantic alternatives | Raw meaning is correct, but grounding labels multiple subjects as coreference instead of ambiguous, so rejected |
| Anonymous correction | Raw meaning and target are correct, but a role ID is put in an evidence-selection field, so rejected | Raw meaning and target are correct, but correction grounding violates endpoint/evidence rules, so rejected |
| Nearby names | Usable; only Inez/Ash receives a conservative role | Usable; only Inez/Ash receives a non-executive role |

The exact manual findings and capture hashes live in
[`review.json`](../../tests/fixtures/person-affiliation/referent-model-v1/review.json).
Offline replay recomputes every request, reruns structural validation and checks the
review summaries and token totals against retained bytes.

## What the trial establishes

The separate referent representation solves a real modeling problem. Gemma correctly
keeps literal `Voss` grounding separate from the two possible people, and both models
express the anonymous correction's intended target in raw output. These meanings were
not representable cleanly in the prior combined binding field.

The combined producer is still too broad. It asks the model to reproduce lexical
grounding, role interpretation, referent assessment and correction targeting in one
large response. Gemma often understands the source but puts literal or numeric values
where selection IDs are required. GPT-OSS sometimes emits referents while dropping
all interpretations, and valid answers can invent correction semantics or decline
all referent assessment. Four structural passes per model conceal different failures.

The fixed seed is not a reproducibility guarantee. GPT-OSS's earlier 90-second run
produced a structurally valid Microsoft answer; the byte-identical final request did
not. Retaining request/response bytes and evaluating distributions matters more than
assuming deterministic generation.

## Decision and next boundary

Do not promote either combined producer. Keep the representation and validator, but
test a narrower second stage next: consume one already validated grounding proposal
and ask only for per-role referent states and per-correction target states. Go should
join that response back to immutable grounding. This removes repeated source-span and
role construction from the assessment task without weakening validation or adding
case-specific rules.

That next test must preserve the same real failures, include fresh cases before any
quality claim, and score omission, abstention, structural validity and semantic
correctness separately. Autonomous page selection and accepted donor affiliation
remain later gates.

## Retained evidence and verification

The final captures are under `gemma-4-31b-20260919-long/` and
`gpt-oss-120b-20260919-long/` in the fixture directory. Earlier partial and 90-second
attempts remain separate and unchanged; they are operational evidence, not final
quality results. `prose_referent_replay_test.go` replays only the final comparison.

Full Go tests, focused static checks and prose race tests pass. No production source,
model deployment, identity rule, graph data or monetary result changed.
