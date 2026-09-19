# Token-ID model trial

One fixed, opt-in GPT-OSS run on the same nine excerpts as the
[mention-model trial](../mention-model-v1/README.md). Reuse its original source bodies
and input expectations; do not copy or modify them. These are known examples, not
fresh held-out cases. The model receives only source text and source-provided token IDs.

The profile remains medium reasoning, 4,096 output tokens, seed 1 and advertised
sampler defaults. The new prompt/schema replaces copied quotations and byte offsets
with token selections; semantic vocabularies and validation remain unchanged. Each
case runs once without retries, repair or partial salvage. Source selection remains
manual and no identity, affiliation, graph or financial approval follows.

Request/response captures and derived reports are kept separately from manual
semantic review. The completion marker means attempts were recorded, not that
the model selected correct evidence. A valid range can still identify the wrong text.

Result: four structural passes, four reference-check rejections and one HTTP 429
capacity failure. All 56 ranges in the eight complete answers are valid, but semantic
errors remain. `review.json` keeps those assessments distinct from Go-derived output.
The [trial audit](../../../../docs/audit/prose-token-selection-model-2026-09-17.md)
records paired usage, source-selection limits and the next proposed contract review.
