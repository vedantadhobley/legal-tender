# Fresh derived-reference trial — 2026-09-17

The [simplified citation contract](../design/prose-citation-selection.md#go-derived-required-references)
passes four of six fresh answers structurally, but it does **not** establish correct
person/company extraction. One answer is truncated and one violates the joint-activity
representation rule. Passing answers still contain wrong endpoints and other semantic
errors. No affiliation is accepted.

## Fixed scope

The [input manifest and captures](../../tests/fixtures/person-affiliation/reference-model-v1/README.md)
contain two manually selected official biography excerpts and four new synthetic
controls. Expectations were written before the model run and were never included
in requests. The model receives only selected source entries and their lexical token
catalogs. "Fresh" means unused in earlier project trials, not absent from model
training or a representative held-out population.

The sources are [Microsoft's Amy Hood biography](https://news.microsoft.com/exec/amy-hood/)
and [AMD's investor-relations board page](https://ir.amd.com/governance/board-of-directors).
Retained HTML passes the existing Go lexical reader. AMD's corporate biography
failed retrieval twice; its investor-relations page supplied the retained excerpt
instead. Page and excerpt selection remain manual, not autonomous source discovery.

Live discovery confirmed `gpt-oss-120b`, medium reasoning, 4,096 output tokens and
JSON Schema support. The producer keeps seed 1 and advertised sampler defaults.
Its fixed prompt replaces the redundant reference-list instruction with
`additional_evidence` and clarifies named endpoints for pronoun-bound claims. Go
derives the required union and stamps `model_proposal` provenance, using the same
offline validator. Old prompts, captures and prior rejection results are unchanged.

No inference retries, response repair, partial report salvage or post-result
prompt/validator changes are allowed in this run. The completion marker records
attempt completion, not extraction success. This fresh sample is not a paired
comparison with the earlier model runs.

## Results

All six requests returned HTTP 200. Five completed generation; AMD reached
`finish_reason=length` with incomplete JSON and received no derived report. The
joint-activity answer completed but failed the unchanged interpretation validator.
The remaining four answers have valid derived references. All 33 token ranges in
the five complete answers select exact source text; that does not mean the ranges
identify the requested entities.

| Case | Structural outcome | Separate semantic review |
|---|---|---|
| Microsoft biography | Pass | Whole clauses serve as both person and organization endpoints; the separately named CEO is omitted |
| AMD biography | Rejected: truncated | No complete proposal or endpoint mapping; no partial answer is salvaged |
| Employer transition | Pass | Correct named person and intended employers, but one organization span includes an extra word and pronoun readings are labeled direct |
| Ambiguous surname | Rejected: multiple direct subjects | CFO correctly keeps both people as alternatives; the jointly stated speaking activity violates the one-subject contract |
| Correction and conflict | Pass | Correct selective retraction/conflict handling, but the denied-role organization span is wrong |
| Conditional role and award | Pass | President stays hypothetical and award stays activity; an embedded instruction still produces an additional unclear CEO proposal |

The [per-case review](../../tests/fixtures/person-affiliation/reference-model-v1/review.json)
records selection, binding, role and temporal/context outcomes independently.
It identifies both model errors and a contract limitation: two explicitly named
speakers are not ambiguous, even though this contract requires one subject per
direct interpretation. Do not misreport that rejection as a failure to preserve
the CFO alternatives, or relabel a joint activity to force a pass.

Useful behavior remains: employer transitions are not called contradictions;
the correction targets only its original claim; denial and original asserted status
stay distinct; the unrelated museum trusteeship is not retracted. Go attaches the
explicit extra evidence and complete entries without rewriting the proposal.
The instruction-derived CEO remains `nonpositive_unclear`, not asserted or approved,
but an instruction is still not a factual role statement and should not become one.

The removed evidence-list bookkeeping no longer causes rejection in this sample.
That is not a measured accuracy improvement: cases differ from the prior run,
there is only one attempt per case, and every structural pass still needs semantic
review. The two real-source cases do not demonstrate complete usable person/company
bindings. Source truth, donor identity and affiliation acceptance are separate.

## Usage and verification

The six responses report 11,643 prompt tokens and 12,606 completion tokens, including
the truncated answer's 4,096 completion tokens. Summed request time is 268.384 seconds;
the harness completes in 268.40 seconds. Usage includes service generation accounting
and cached prefixes. These figures are not a paired speed or token-efficiency gain.

The new producer and replay stay in `internal/audit/personaffiliation/*_test.go`.
Only the fixture-reading helper is shared with the earlier producer; its existing
requests and results replay unchanged. Replay verifies captured requests, pre-run
expectations, source bodies, supplied proposals, derived reports, exact rejection
outcomes and false approval flags. Selection-only diagnostics never salvage a
rejected interpretation report. Full Go tests, focused vet/race checks, formatting
and documentation-link checks pass. Source parsers, graph and financial code are
unchanged.

## Next decision

Keep the reference simplification, but do not promote this producer. The next
bounded experiment should isolate person/organization name-span selection from
role interpretation, using these failures as regressions and fresh examples for
evaluation. This is a proposed experiment, not an accepted multi-stage architecture.
Do not add company-specific trimming, silently repair ranges or count literal
validity as extraction accuracy. Review joint-activity representation separately;
more output tokens would not fix wrong endpoint selection.
