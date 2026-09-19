# Whole-entry role grounding — 2026-09-17

The [test-only grounding change](../design/prose-role-grounding.md) now has five
fresh two-stage captures and exact offline replay. Four role answers pass structural
checks; one is rejected for redefining a caller-reserved ID. Complete statement
context and separate role highlights work, but surname ambiguity and a missing
retraction link remain semantic failures. This is not production acceptance or a
general accuracy improvement.

## What changed

The prior [role-binding trial](./prose-role-binding-model-2026-09-17.md) exposed
misstated direct bindings, combined titles and incomplete selected evidence. This
follow-up separates the role highlight from its complete source-entry context and
separates actual endpoint text from proposed name-candidate referents.

Go now requires exact surface/candidate text equality for a claimed direct binding.
It rejects a selected pronoun presented as an exact full name. This is a necessary
literal check, **not a person-resolution rule**. Names still come from the unchanged
first-stage producer without corrections, dictionary patches or entity merges.

The revised prompt requests one role at a time and keeps advisory-body roles distinct
from corporate directorships. Unknown coarse classifications remain `unknown`.
Explicit anonymous roles are retained as unresolved evidence; this resolves the
previous trial's anonymous-role task/expectation conflict prospectively, without
relabeling the old result. No production taxonomy or financial policy changed.

## Fixed experiment

The five [synthetic inputs and expectations](../../tests/fixtures/person-affiliation/binding-model-v2/inputs.json)
were written before either inference stage. All inputs are new to these model trials.
The prompt, schema and validator stayed fixed throughout capture. Both stages used
the live-discovered `gpt-oss-120b`, medium reasoning, seed 1, and a 4,096-token output
budget, with the existing 90-second request bound. No retry or partial salvage.

Names-only capture completed five structurally valid answers. The role stage then
consumed their exact proposals, including the standalone surname and advisory-body
name. Expected semantic labels never entered the model requests. All ten requests
returned HTTP 200 and `finish_reason=stop`.

| Fresh case | Observed result |
| --- | --- |
| Multiple roles/job change | Distinct president/CFO, later CEO and denied founder; correct named endpoints, direct versus pronoun bindings and complete date context |
| Advisory council | Advisory membership stays `unknown` at the named council; no employment from attendance. Denied institute-board role has an unnecessarily unresolved organization. Bare “director” is classified as `board_director` without enough evidence to establish that meaning |
| Ambiguous conditional | Hypothetical status and full uncertainty text survive, but the model chooses the standalone surname with `direct` binding instead of alternatives or unresolved state: semantic failure |
| Anonymous correction | No person invented and source instruction ignored; correction is retained, but its `retracts` link is absent: semantic failure. Selecting extra CFO evidence does not create that link |
| Nearby unrelated names | Entire answer rejected for supplied `ctx_0`. Raw answer selects the intended secretary/person/company spans, but classifying bare secretary as executive remains unsubstantiated; nothing is salvaged |

The [case review](../../tests/fixtures/person-affiliation/binding-model-v2/review.json)
also records small focus/surface deviations and unresolved institutional scope.
“Four structural passes” must not be presented as four correct semantic answers.
The multi-role case meets its reviewed expectations; the other outcomes have explicit
limitations or failures. This tiny synthetic batch is not an estimate of real-page
precision, recall or reliability.

### Why the surname still passes

The first stage includes both full names and the literal surname. Selecting the
surname as both surface and candidate passes exact equality. It does not determine
which person was meant. The model's `direct` interpretation therefore fails the
pre-run ambiguity expectation even though it names no specific full-name person.
Hypothetical status and false identity/graph approval flags remain intact. No hidden
surname expansion, identity merge or downstream affiliation occurs.

### What complete context fixes—and does not

Go now makes the entire selected entry the claim's context citation; the short focus
is explicitly only a highlight. Dates, denial and uncertainty in that entry cannot
be trimmed from that citation. All supplied excerpt entries remain available as well.
This prevents that form of evidence loss. It cannot force the model to interpret
the qualification correctly, discover every role or attach a missing correction link.

## Execution and verification

| Stage | Prompt tokens | Completion tokens | Sum of request elapsed time |
| --- | ---: | ---: | ---: |
| Names, five requests | 4,819 | 3,212 | 243.185 s |
| Roles, five requests | 7,201 | 7,785 | 162.891 s |

The harnesses completed in 243.200 and 162.912 seconds respectively. These are
single-run measurements, not performance guarantees or a paired old/new benchmark.

[Retained fixtures](../../tests/fixtures/person-affiliation/binding-model-v2/README.md)
include original responses, requests, model inventories and explicit completion
markers. The replay re-reads source HTML and checks original names, request equality,
proposal preservation, derived evidence, rejection outcomes and false approval flags.
Post-run semantic review is separate from the structural validator.

Full Go tests, focused `go vet` and prose race tests pass. Tests independently cover
pronoun/direct rejection, full-entry context, missing endpoints, source-range/type
substitution, reserved IDs and unchanged correction handling. A valid but wrong role
interpretation remains unverified in the tests. Ordinary tests use no network.
The production lexical reader, deterministic prose baseline, earlier prompts and
captures remain unchanged. No graph, identity or monetary behavior changed.

## Next step

Do not add more prompt rules and call the interpretation problem solved. The next
small contract review should separate literal mention correspondence from referent
resolution, and make a correction's target—or inability to determine one—explicit.
Use the retained surname and omitted-retraction counterexamples to test that boundary
offline first. Coarse director/secretary classification remains unverified; do not
create title dictionaries or person-specific exceptions to make this sample pass.

Real-page evaluation, source discovery and affiliation acceptance remain separate
gates. Nothing in this trial authorizes graph publication or terminal attribution.
