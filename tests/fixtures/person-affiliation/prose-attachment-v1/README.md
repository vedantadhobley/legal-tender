# Code-owned evidence attachment trial

This test-only experiment changes the model output contract, not the source reader
or production graph. The model returns literal role/name fields and source entry
IDs. Go attaches complete selected text, original source metadata and the full
supplied context with HTML byte spans. It does not rewrite fields, resolve missing
identities, repair output or silently add references to a proposal.

Six previously reviewed real-source windows are regressions, not fresh evaluation.
The nine new synthetic [controls](./controls.json) and review expectations were
written before the run. Only their reader-produced entries reach the model; no
expectation, URL, expected identity or reviewed answer is sent. The prompt and
schema were fixed before inference. Old quotation trials remain unchanged.

Normal tests are offline. To repeat the bounded live experiment in the existing
capped Go container, mount an output directory and use a new child path:

```sh
LT_PROSE_MODEL_URL=http://control-joi.luv \
LT_PROSE_MODEL_ID=gpt-oss-120b \
LT_PROSE_MODEL_REASONING=medium \
LT_PROSE_MODEL_MAX_TOKENS=4096 \
LT_PROSE_MODEL_OUTPUT=/out/new-attachment-trial \
go test -count=1 -buildvcs=false ./internal/audit/personaffiliation \
  -run '^TestProseAttachmentLiveComparison$' -v -timeout 25m
```

Ready-model discovery validates these controls. Each sequential request has a
90-second timeout, strict JSON Schema, seed 1, advertised default samplers, no
tools and no retries. The seed does not guarantee identical future generations.
The completion marker means all attempts were recorded, not semantic success.

Records preserve complete response envelopes, reformatted as JSON rather than
exact transport bytes. Successful literal validation adds a separate Go-generated
`evidence_attachment`; it never edits the raw response. Rejected answers have no
attachment, but their entire input and output remain available. One invalid field
rejects the whole answer. Literal matching still cannot prove role/alias meaning,
negation, date binding, reference completeness or donor identity.

The [review](./review.json) pins inputs and captures; the
[audit](../../../../docs/audit/prose-evidence-attachment-2026-09-16.md) separates
mechanical validity, semantic findings and remaining gaps. No approvals or graph
writes follow from either a valid answer or a passing replay test.
