# Local prose-model comparison

Reviewed 2026-09-16 America/New_York; calls completed 2026-09-17 UTC.
The user approved a bounded comparison after the
[unchanged grammar evaluation](./prose-extraction-evaluation-2026-09-16.md).

## What this means

A local-model comparison sends supplied source text to an existing self-hosted
language model and compares its proposed relationships with the deterministic
baseline and reviewed evidence. It is not model training, an embedding search,
automatic website discovery or permission for a model to decide donor identities.
No external AI provider is used. No source instructions are executed.

The project still needs source-backed entity and relationship rules. Model output
is an untrusted proposal; a citation check can verify copied text, but cannot prove
that its interpretation, person binding or date meaning is correct.

## Result

The first trial uses the live-discovered `gemma-4-12b` model. All 14 sequential
requests returned HTTP 200 and completed normally, totaling 192.685 seconds of
request time. Nine answers passed literal citation validation; five were rejected.
These are answer-validity counts, **not extraction accuracy**.

The model recognizes language the grammar misses, including IBM's compound title
and Ridgeline's role-before-name wording. It also produces unsupported fields,
incorrect role dates and out-of-scope relationships. Do not promote this profile
to production extraction or weaken validation to make its output pass.

| Real-source case | Literal check | Semantic review |
|---|---|---|
| Microsoft | Rejected | Combined chairman/CEO role gets an unsupported CEO-appointment date; also emits an empty alias |
| IBM | Pass | Exact reviewed subject, compound title and organization; no invented time |
| Ford | Pass | Both pairings match reviewer labels, but flat adjacent blocks supplied no parent-card structure; automated binding remains unsupported |
| Ridgeline | Rejected | Target role is recognized, but praise becomes another role and a family phrase becomes an alias; citations lack required field support |
| Sprinklr proxy | Rejected | Invented ellipses in quotes, unresolved pronoun subject and founding wording used as CEO time |
| Parenthetical names | Rejected | Recognizes Ted/Rick but constructs nonliteral full names and treats family relationships as organization roles |

The eight synthetic controls expose a different mix. Namesakes and executive/engineer
roles stay separate. Retraction and hypothetical language retain the correct polarity.
Explicit time wording is copied and the page-modification date is not used as tenure.
Public-board and reported-employer roles remain separate. The source-instruction
control produces an empty answer. Simple denial is interpreted correctly, but an
empty alias rejects that whole response. These few examples do not establish general
prompt-injection resistance, semantic precision or safety.

Five rejected answers remain fully available; none was repaired manually or silently
trimmed to its useful claims. One invalid claim rejects the answer in this trial.
All donor-identity, graph and financial approvals remain false.

## Inputs and experimental limits

The [retained trial](../../tests/fixtures/person-affiliation/prose-model-v1/README.md)
contains model discovery, every request, complete JSON response envelopes, timing,
usage, completion status, the completion marker and manual semantic review.
Envelopes are JSON-reformatted on storage, not exact wire/header captures; model
content is preserved without editing. Earlier source bodies remain unchanged.

Inputs are reviewer-selected lexical entries from the five readable evaluation
pages, the existing parenthetical-name paragraph and eight synthetic cases.
Selection uses earlier witness annotations. Model messages receive only entry IDs,
kinds, text and byte spans—not expected labels, source URLs or grammar outputs.
The unchanged grammar also runs over those same selected entries for comparison.
AMD remains a retrieval failure and is not replaced with search snippets.

This supplies the relevant passage in advance. It tests interpretation, **not
whole-page discovery, automatic chunk selection, unseen-source generalization or
production throughput**. Ford's structural gap is intentional and explicit: the
text-only input cannot prove relationships merely because a plausible pairing
matches a reviewer label. A later method may need generic document structure.

The prompt was fixed before the batch. There were no prompt edits, retries or
model substitutions during the run. Sampling used the model's advertised defaults
plus seed 1, `reasoning_effort=none`, strict JSON Schema and a 2,048-output-token
limit. Every request had a 90-second timeout and no tools. Ready model IDs and
capabilities came from the existing gateway's `/v1/models` response. No service,
deployment, memory budget, legacy inference setting or model weight changed.

The project agent context's old direct-node Qwen instructions were stale. Current
inference access routes through the gateway documented by the shared
[homelab topology](../../../../vedanta-dhobley/docs/topology.md); discovery, not those
old names or ports, controls this trial. No `.env` or credential was read.

## Code and replay

The implementation is an opt-in Go research test, not a production client or CLI.
Normal tests never call a model. The fixture README documents the explicit live
environment variables and capped-container invocation. Existing lexical parsing,
grammar, automatic enrichment, graph and money code are unchanged.

Offline replay uses `TestProseModelRetainedReplay`. It checks capture hashes, rebuilds
the exact verified source windows and grammar output, compares prompts/schemas/controls,
rechecks citations and preserves all measured failures. Additional tests reject
invented IDs/quotes/fields/dates, malformed or duplicate-key JSON, unknown properties,
missing/null time fields and incomplete generations. A deliberate counterexample
proves the literal validator does not validate semantics: a real quote can still
omit a retraction elsewhere in its source entry.

Semantic review is recorded separately in the fixture's `review.json`; it is not
an automatic truth evaluator. Passing replay tests means the measured behavior,
including bad proposals, is preserved. It does not approve the model.

The full Go suite and focused static/race checks pass in the existing capped,
network-disabled compiler container. Normal tests skip the live call path and replay
the saved experiment. Formatting, unchanged runtime-reader/grammar hashes and updated
documentation links pass. No production activation or service restart was performed.

## Next experiment

The user-approved [stronger-model follow-up](./stronger-prose-model-comparison-2026-09-16.md)
has now completed the unchanged task. It preserves this Gemma trial and records
eleven literal passes, three rejections and remaining semantic failures separately.
The recommendation below was this first run's next step, not outstanding work.

Run the same inputs, prompt and citation checks against the stronger available
local reasoning model, using its advertised controls, before changing the task or
relaxing constraints. Keep this Gemma trial intact. This separates model capability
from prompt changes and does not assume the larger model will pass.

Regardless of that comparison, source-qualified dates, person/organization binding,
card structure, unsupported aliases and claim scope remain explicit requirements.
Any tuned approach needs fresh evaluation cases before publication. No identity or
edge acceptance follows automatically from a better extraction result.
