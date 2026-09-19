# Fresh mention/interpretation model trial

Test-only selected-excerpt experiment. `inputs.json` fixes three official-page
excerpts and six synthetic controls before inference; its review expectations
are never sent to the model. The prompt and schema live in the Go audit tests.

The HTML bodies are unedited captures from 2026-09-17. Source hashes bind the
original bytes; entry indexes select full lexical entries, not the whole page.
Selection is manual. Salesforce redirected the query-free requested URL to a
URL with `?bc=HL`; `EffectiveURL` records that exact retrieval URL separately
because the existing reader's source identity requires a query-free URL. No
reader contract is changed or claim of autonomous retrieval made.

Model proposals are never reviewed annotations or accepted affiliations.
Go verifies literal anchors and references, not whether proposed meaning is true.
Earlier fixtures and rejected outputs stay unchanged.

Nine GPT-OSS requests completed under the fixed medium/4096 profile. Five answers
pass literal/reference checks; four fail, including every real-page input. The
completion marker records capture completion, not extraction acceptance. Individual
case files preserve raw JSON envelopes, requests and separate Go-derived reports
only when valid. `review.json` records manual meaning review, not approved truth.

Offline replay checks input reconstruction, requests and exact reports/rejections.
See the [trial audit](../../../../docs/audit/prose-interpretation-model-2026-09-17.md)
for outcomes, limits and the proposed next step. No output has been repaired.
