# Stronger local prose-model comparison

Reviewed 2026-09-16 America/New_York. The fourteen attempts completed at
2026-09-17T02:32:22Z. This is the user-approved follow-up to the
[Gemma experiment](./local-prose-model-comparison-2026-09-16.md).

## Outcome

The stronger local configuration improves some interpretation and abstention,
but it does not solve reliable source-cited extraction. Keep it as research;
do not wire either model into accepted affiliations or the money graph.

| Measured run | Gemma | GPT-OSS |
|---|---:|---:|
| Same fixed cases | 14 | 14 |
| HTTP 200 with normal completion | 14 | 14 |
| Literal-valid answers, including abstentions | 9 | 11 |
| Rejected whole answers | 5 | 3 |
| Total request time, seconds | 192.685 | 245.713 |

These are execution and literal-validation counts, **not accuracy, accepted
relationships or a production benchmark**. GPT-OSS's eleven passes include two
empty answers and semantic/context errors. There was one run per configuration,
no blind review, no retries and no significance claim.

The [retained trial and manual review](../../tests/fixtures/person-affiliation/prose-model-gpt-oss-v1/README.md)
preserve all requests, complete responses, failure messages and observations.
Neither rejected answers nor individual claims were repaired or promoted.

## Same task, different supported controls

The fourteen source windows, prompt, JSON Schema, literal validator, seed 1,
sequential execution and 90-second request timeout stayed unchanged. The model
receives only the same selected entries, not review labels, URLs or grammar results.
Go replay verifies exact source windows and request equality for both captures;
only `model`, `reasoning_effort` and `max_tokens` differ in the request bodies.

Live `/v1/models` discovery confirmed `gpt-oss-120b` ready. Unlike Gemma it does not
support `reasoning_effort=none`. This trial used its default `medium` reasoning
and 4,096 output tokens, versus Gemma's `none` and 2,048. Both used their advertised
sampler defaults, which also differ. This is a **model-configuration comparison**,
not an isolated model-size experiment or equal-compute comparison. No request hit
its token limit or timeout. Maximum observed completion usage was 3,654 tokens.

No service, model weights, memory limit, deployment, credential or `.env` changed.
The public-source excerpts went only to the existing self-hosted gateway. Captured
response envelopes include any server-supplied reasoning field; semantic review
evaluates final answers against source text, not the model's explanation of itself.

## Real-source findings

| Case | Literal check | Review of the final answer |
|---|---|---|
| Microsoft | Rejected | Separates chairman/CEO dates more carefully, but fabricates a contiguous quote by deleting intervening words |
| IBM | Pass | Exact compound title, subject and organization; time stays unknown |
| Ford | Pass, empty | Correctly abstains because supplied flat blocks lack name/title binding; does not recover the two roles |
| Ridgeline | Pass | Recovers the role-before-name mention without turning praise or family wording into extra roles/aliases |
| Sprinklr proxy | Rejected | Does not borrow founding time for CEO; still omits subject/organization from citations, normalizes `founded` to nonliteral `founder`, and leaves the issuer unresolved |
| Parenthetical names | Rejected | Avoids the fabricated family organization but still joins names across parenthetical nicknames into nonliteral forms |

The real-source inputs were selected using existing witness annotations. They
provide relevant passages in advance, not autonomous page or chunk selection.
Ford lacks DOM/card structure in both runs. AMD remains the earlier acquisition
failure, not an empty or substituted model case. This does not establish unseen
page performance or general alias/role coverage.

## Controls and semantic gaps

All eight synthetic answers pass literal checks, but not every interpretation or
quotation is adequate:

- Namesakes remain separate and ordinary employees do not become executives.
- Simple denial has the correct label and a complete negative quote.
- Retraction has a correct `denied` label, but quotes only the positive sentence
  and omits the retraction. This is weaker citation context than Gemma supplied.
- Hypothetical has the correct label but drops the opening `If` and consequence
  from its quote. Keeping `were` is not the same as preserving the full condition.
- Explicit years remain original text; a page-modification date is not tenure.
- Public-board membership stays separate, but the model uses `employer` as the
  person's second role instead of the stated `SALES` occupation. A literal word
  can have the wrong semantic function.
- The embedded instruction produces no proposals. One example does not prove
  general prompt-injection resistance.

This is not uniformly better behavior. Literal matching cannot establish that a
quote preserves its context, that a field has the right meaning or that the source
person is the FEC donor. No production identity, affiliation or financial rule changed.

## Verification and next step

Only the opt-in test harness gained explicit reasoning/token controls and ready-model
capability checks. Offline replay now covers both fourteen-case captures, including
every failure, with unchanged task inputs and citation rules. Profile tests reject
unsupported reasoning, token limits and configured-but-unavailable models.

The full offline Go suite, focused static/race checks, formatting and documentation
links pass. Source-reader, grammar and prompt hashes remain unchanged. The live
completion marker records the complete attempt set, not extraction quality.

Recommended next: test a small **code-owned evidence attachment** design. Let the
model reference source entries; let Go attach their complete text and raw spans,
rather than asking the model to rewrite quotations. Keep literal field checks and
original name forms; do not silently repair or normalize bad output. This would
remove a mechanical quotation-copy task, not prove the proposed relationship.

Test that version separately on fresh pronoun, negation, alias, role-date and
unbound-card cases before any acceptance decision. Preserve both current runs as
baselines. Automatic page selection, structured layout, donor identity and graph
publication remain separate work; this comparison does not authorize them.

Follow-up: the user approved and completed the
[entry-attachment experiment](./prose-evidence-attachment-2026-09-16.md). Go now
attaches original evidence in that separate research version; fresh tests expose
remaining pronoun, role-scope and retraction failures. This trial remains unchanged.
