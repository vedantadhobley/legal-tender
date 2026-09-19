# Role binding from uncorrected names — 2026-09-17

The bounded second-stage experiment completed ten attempts. All ten returned
complete, structurally valid answers. Ambiguity, hypothetical status and missing
endpoints were preserved in the two fresh controls. Several known cases still
mislabel coreference as direct binding, omit role-specific time evidence or collapse
distinct roles. No affiliation, identity, graph edge or money attribution is accepted.

## Method and scope

The [names-only trial](./prose-name-selection-model-2026-09-17.md) isolated literal
name selection. This follow-up feeds its uncorrected eight outputs and the original
source text into a role-binding request. It adds two synthetic controls: an ambiguous
CFO and a conditional president appointment alongside an unnamed CTO. Their names
were produced by the unchanged first-stage prompt, not supplied manually.

[Inputs and pre-run expectations](../../tests/fixtures/person-affiliation/binding-model-v1/cases.json)
were fixed before inference. The eight reused cases are known regressions, not fresh
accuracy evidence. The two new controls were not used to tune the producer. No
prompt, schema, validator or expected output changed after observing responses.
There were no retries, repaired fields, restored missing names or partial salvage.

The test-only Go adapter:

1. Re-reads retained source bodies and validates the original names-only capture.
2. Assigns local `n0`, `n1`, etc. handles to unchanged name selections. These are
   mention references, not person/company identities. Original IDs, ranges and
   proposed kinds remain in the report.
3. Sends the source-token catalog plus those names to the model. Person/company
   endpoint fields are restricted to their corresponding supplied candidate IDs.
4. Validates new clause selections and interpretation references. New clause IDs
   cannot substitute for names or overwrite them. Missing endpoints may stay empty.
5. Retains the exact supplied proposal separately from Go's derived references,
   source spans, complete supplied entries and unverified interpretation states.

The existing [citation and interpretation boundary](../design/prose-citation-selection.md)
still applies. A correctly typed endpoint can be the wrong entity. Go verifies
references and applies supplied statuses/context links; it does not prove the model's
binding, correction, role class or claim. All approval flags remain false.

## Results

The [case review](../../tests/fixtures/person-affiliation/binding-model-v1/review.json)
records post-run manual semantic judgments separately from structural replay.

| Case | Preserved behavior | Remaining issue |
| --- | --- | --- |
| Microsoft/Hood | Hood/Microsoft and Nadella/Microsoft remain separate; no employer links to acquisition targets or schools | EVP/CFO combined; short Hood mention remains separate; selected employment clause drops 2002 and 2013 role wording lacks a selection |
| AMD/Su | Employers stay separate; board roles and technical staff represented; no false job-history conflicts | Pronoun/surname bindings marked direct; opening roles combined; CEO/president since October 2014 sentence not selected |
| Pronoun employer change | Correct CTO/Copper and CEO/Larch endpoints; stated years retained | Both pronoun bindings incorrectly marked direct |
| Correction/conflict | Asserted and denied CEO claims retained; correction retracts only the first; trustee unaffected | Still model-supplied context links, not independently verified truth |
| McClure | Named historical roles, retirement wording and board mentions retained; degree not employment | Coreference marked direct; imperfect first-stage names and board/institution scope propagate; coarse role classes remain unverified |
| Speaking/thanks | Empty role output | No broader conclusion from this one control |
| Shared surname visit | Empty role output; no resolved surname identity | No broader conclusion from this one control |
| No-name instruction | No invented names or obedience to injected instructions | Anonymous chief-executive role emitted, contrary to frozen empty-output expectation |
| Fresh ambiguous CFO | Both named alternatives retained as ambiguous; no choice of one person | Selected clause omits explicit uncertainty wording, although full context retains it |
| Fresh conditional/unnamed | President hypothetical; unnamed CTO unresolved with empty endpoints; no nearby-name borrowing or instruction-derived CEO | No identity or tenure accepted |

The no-name case exposes a task/expectation tension. Its first sentence refers to
an unnamed chief executive; the prompt allows supported unnamed roles. Returning
that unresolved role is not a fabricated named affiliation, but it does not meet
the pre-run empty-output expectation. Do not change the label retrospectively or
count it as a perfect abstention result.

Similarly, full-entry preservation prevents source loss but does not make incomplete
role-specific evidence complete. The missing dates and uncertainty wording remain
available for review; downstream consumers must not assume the selected clauses alone
contain every qualification. There is no automatic date-interval extraction here.

This run demonstrates a working two-stage experiment, not production reliability.
It does not establish that splitting the task improves general accuracy or cost.
Known excerpts, changed task scope and tiny fresh coverage prevent that conclusion.

## Execution and retained evidence

Live discovery confirmed `gpt-oss-120b`; the fixed profile was medium reasoning and
a 4,096-token completion budget. Requests ran sequentially with the existing 90-second
per-request bound. Every attempt returned HTTP 200 and `finish_reason=stop`.

| Work | Attempts | Prompt tokens | Completion tokens | Sum of request elapsed times |
| --- | ---: | ---: | ---: | ---: |
| Fresh control names | 2 | 2,096 | 1,036 | 80.972 s |
| Role binding | 10 | 18,769 | 14,508 | 316.853 s |

The role harness completed in 316.892 seconds. The eight other name requests were
reused from the previous trial; their cost is excluded above, not eliminated from
an eventual end-to-end pipeline. These timings are one run, not a latency guarantee.
The AMD answer used 3,843 completion tokens and completed within this budget; this
does not retroactively repair its earlier truncated answer under another task.

[Retained fixtures](../../tests/fixtures/person-affiliation/binding-model-v1/README.md)
include both sets of complete requests/responses, model inventories and explicit
completion markers. Replay checks requests, source bodies, original name captures,
proposal preservation, exact derived reports and false approval flags. Its hash
checks protect experiment evidence; they are not a new production publication layer.

Code is confined to `internal/audit/personaffiliation/prose_binding*_test.go` and
the embedded research prompt. Independent synthetic tests cover type/reference
substitution, forged fields, malformed ranges, empty endpoints, budget rejection,
ambiguity and selective context links. A valid but semantically wrong organization
reference remains explicitly unverified in the tests. Ordinary tests use no network.

Verification passed: full Go tests, focused `go vet`, prose race tests, exact retained
replay and relative documentation links. Reader/baseline hashes, prior prompts and
the pre-run binding prompt/input hashes remain unchanged.

## Next boundary

Do not promote this producer to the enrichment or graph pipeline. First resolve the
observed interpretation-contract issues: direct versus coreference evidence,
complete qualified role clauses, distinct role labels and board/institution scope.
The anonymous-role expectation also needs an explicit scope decision. Use the
retained failures to define that small change, then test it on fresh excerpts;
do not patch individual people or add a source crawler to work around these errors.

Autonomous source selection, donor identity, accepted affiliations and terminal
dollar attribution remain separate work. Production source readers, deterministic
prose baseline, earlier model captures, graph behavior and monetary rules are unchanged.
