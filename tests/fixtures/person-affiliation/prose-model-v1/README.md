# Local prose-model comparison

Research trial approved 2026-09-16. This directory retains requests, raw responses
and review outcomes from the existing self-hosted inference gateway. It is not a
runtime relationship feed or a donor-resolution model. No graph or money changes.

The trial uses the exact ready model ID discovered through `/v1/models`, strict
JSON-shaped output, one sequential request per case and no automatic retries.
The prompt, schema, input excerpt and sampling controls are retained in every case
record. The model has no tools or network access through this harness. Public-source
excerpts and synthetic controls are sent; no FEC private fields or operator secrets.
Complete JSON response envelopes are retained, reformatted on storage rather than
preserved as exact transport bytes; content is unedited. Full HTTP headers are not
captured. The [review log](./review.json) pins every record and separates literal
checks from manual semantic findings; the [audit](../../../../docs/audit/local-prose-model-comparison-2026-09-16.md)
records the measured result and next experiment.

## Scope

The five readable sources from the
[grammar evaluation](../prose-evaluation-v1/README.md) supply reviewer-selected
witness entries, not whole pages. The prior parenthetical-name paragraph adds one
real alias example. Eight synthetic cases test namesakes/ordinary employees, denial,
retraction, hypothetical language, explicit dates, page dates, public-board versus
employer context, and source-embedded instructions. AMD remains unavailable and is
not replaced with search snippets.

Expected labels, grammar output and source URLs are not sent to the model. Input
contains only selected lexical entries with original IDs, kinds, text and byte spans.
Selection itself uses earlier review annotations and therefore supplies the relevant
evidence in advance. This is an interpretation experiment, not automatic retrieval,
whole-page extraction, blind validation or population accuracy measurement.

Ford input contains separate text blocks but no parent-card structure. A correct
reviewer-label match there does not prove the text-only model had sufficient evidence
to establish those connections. Quote validity is not relation truth.

## Replay

Normal `go test ./internal/audit/personaffiliation` runs offline. The opt-in live
test requires `LT_PROSE_MODEL_URL` (gateway origin), `LT_PROSE_MODEL_ID` (a discovered
ready ID), and `LT_PROSE_MODEL_OUTPUT` (a directory that does not yet exist). Run it
in the capped project Go container with a writable output mount:

```sh
go test -count=1 -buildvcs=false ./internal/audit/personaffiliation \
  -run '^TestProseModelLiveComparison$' -v -timeout 25m
```

The current trial requires advertised `reasoning_effort=none` and JSON Schema;
do not silently send this profile to a model that lacks those controls. Each call
has a 90-second timeout and 2,048-output-token limit. Default model samplers and
explicit seed 1 are recorded; they do not promise deterministic regeneration.
`completed.json` marks the complete attempt set, not successful extraction quality.

The offline validator rejects incomplete generations, malformed/duplicate-key JSON,
unknown fields, invented entry IDs, nonliteral quotes, unsupported field strings
and absent or null time fields. Every required field must occur literally in its
cited quotes. One invalid proposal rejects the entire answer; the raw output stays
retained. Passing validates citations only. Wrong binding, polarity, role-date
meaning and insufficient structural context still require separate review.
