# Mention/interpretation boundary fixtures

These are manually reviewed annotations over six synthetic inputs retained by the
[entry-attachment trial](../prose-attachment-v1/README.md). They are not new model
responses, real biographies, production identity decisions or a correction whitelist.

The [review file](./review.json) records each original capture hash, literal mention
proposals, separate interpretation proposals, explicit context links and expected
review states. Source bodies are rebuilt through the unchanged reader. Go validates
literal anchors and references, attaches original context and applies the supplied
links. It does not discover the labels, bindings or corrections automatically.

Run `go test ./internal/audit/personaffiliation -run TestProseInterpretation` in the
existing capped Go container. No network, model, secret or database is required.
Passing tests preserve source grain and the conditional behavior of this contract;
they do not establish NLP accuracy. Old trial replays continue to preserve their
original failures, rather than substituting these reviewed annotations.

See the [design and implemented boundary](../../../../docs/design/prose-mention-interpretation.md)
for semantics, resource bounds, limitations and the proposed next experiment.
