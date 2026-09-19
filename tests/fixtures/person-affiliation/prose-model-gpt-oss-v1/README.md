# Stronger local prose-model comparison

This is the second model configuration for the same bounded research task as the
[Gemma trial](../prose-model-v1/README.md), not a new prompt or production feed.
The fourteen source windows, prompt, schema, seed and literal validator are unchanged.
The live-discovered model is `gpt-oss-120b`, with `reasoning_effort=medium` and a
4,096-output-token cap. Its advertised sampler defaults differ from Gemma's.
This compares model configurations, not an isolated parameter-count effect.

Each request has the same 90-second timeout, no tools and no retries. Discovery,
requests, complete JSON response envelopes, timings, usage and completion status
are retained. JSON envelopes are reformatted, not exact wire/header captures;
model content is unedited. The completion marker records completed attempts, not
successful extraction. Never repair a captured answer to make validation pass.

Input selection still uses reviewed witnesses, not automatic page retrieval or
chunk selection. Ford receives flat text, not the source's card structure. Literal
quotation checks do not establish semantic correctness or donor identity. Every
identity, graph and financial approval remains false.

## Repeat the experiment

Use the opt-in test in the existing capped Go compiler container, with a writable
mount and a new output directory. Ordinary tests never contact the gateway.

```sh
LT_PROSE_MODEL_URL=http://control-joi.luv \
LT_PROSE_MODEL_ID=gpt-oss-120b \
LT_PROSE_MODEL_REASONING=medium \
LT_PROSE_MODEL_MAX_TOKENS=4096 \
LT_PROSE_MODEL_OUTPUT=/out/new-trial \
go test -count=1 -buildvcs=false ./internal/audit/personaffiliation \
  -run '^TestProseModelLiveComparison$' -v -timeout 25m
```

The harness verifies the requested model, reasoning effort, JSON Schema support
and token limit against ready-model discovery. Unsupported controls fail before
inference; they never fall back to another model. Explicit seed 1 is not a promise
of byte-identical regeneration. Offline replay preserves the measured run.

See the [comparison audit](../../../../docs/audit/stronger-prose-model-comparison-2026-09-16.md)
for results and limits, and the [review record](./review.json) for capture hashes
and separate manual semantic findings.
