# Whole-entry role-grounding experiment

Five fresh synthetic cases exercise the
[test-only grounding contract](../../../../docs/design/prose-role-grounding.md).
They are not real identity evidence or a production accuracy benchmark.
The [completed review](../../../../docs/audit/prose-role-grounding-model-2026-09-17.md)
records four structural passes, one rejection and remaining semantic failures.

- `inputs.json`: source HTML and pre-run semantic expectations. The shared lexical
  reader supplies the model's source entries; expectations are never sent.
- `names/`: unchanged names-only requests/responses, inventory and completion marker.
  These supply all candidate endpoints without hand corrections.
- Case JSON files: complete second-stage request/response, timing and either an
  explicit failure or unverified derived report.
- `models.json` and `completed.json`: live inventory and explicit attempt completion,
  not an accuracy or publication gate.
- `review.json`: capture hashes, structural outcome and post-run semantic findings.
  A structurally valid answer can still choose the wrong role, endpoint or status.

`TestProseGroundingRetainedReplay` re-reads source bodies, reproduces requests and
validates unchanged name inputs, source context, original proposals, derived reports
and false approval flags. No local model is needed for ordinary tests.

The opt-in capture tests are `TestProseGroundingNamesLive` followed by
`TestProseGroundingModelLive`, using the shared `LT_PROSE_MODEL_*` controls and
separate new output directories. Every attempt is retained; do not retry or repair
an answer to improve the recorded result. Older trial fixtures remain unchanged.
