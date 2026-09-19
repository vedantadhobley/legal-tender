# Roles from uncorrected name candidates

Retained, opt-in research trial. This does not publish relationships or approve
identities. See the [review](../../../../docs/audit/prose-role-binding-model-2026-09-17.md).

- `cases.json`: ten inputs, original name-capture paths and pre-run expectations.
  Expectations never enter a model request.
- `inputs.json`: two fresh synthetic controls. The other eight cases reuse the
  [names-only trial](../name-model-v1/README.md) and its original source bodies.
- `seed-names/`: unchanged first-stage names-only requests, responses, model
  inventory and completion marker for the two new controls. No hand-added names.
- Case JSON files: complete second-stage request/response, timing, source entries,
  uncorrected name candidates, original proposal and Go-attached evidence review.
- `models.json` and `completed.json`: model discovery and attempted-run completion.
  Completion means all attempts were retained, not that meanings were correct.
- `review.json`: post-run manual review of binding, role meaning, context and
  abstention, plus hashes used by offline replay. `Valid` means structural validity
  only. It is not semantic accuracy or identity acceptance.

All ten second-stage answers pass reference/range checks. Incorrect binding labels,
incomplete selected context, role grouping and institutional scope remain explicit.
The no-name control differs from its frozen empty-output expectation: it emits an
anonymous unresolved role. Neither the expectation nor the answer was repaired.

Run ordinary offline replay through `TestProseBindingModelRetainedReplay`. The
opt-in tests are `TestProseBindingSeedNamesLive` and `TestProseBindingModelLive`;
they use the shared `LT_PROSE_MODEL_*` controls documented in the
[earlier model trial](../../../../docs/audit/prose-name-selection-model-2026-09-17.md).
Use separate new output directories. Do not overwrite retained results or use
reviewed labels to repair first-stage candidates.
