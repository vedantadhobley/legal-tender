# Fresh derived-reference model trial

Six fixed inputs for the [source-owned citation contract](../../../../docs/design/prose-citation-selection.md).
Two manually selected official biography excerpts and four independent synthetic
controls were chosen before any model answer. `inputs.json` stores the source
provenance, exact reader entry windows and separate review expectations. The model
sees only entry text and token catalogs, never these expectations.

The Microsoft body was captured from its official biography URL, which redirected
to the `EffectiveURL` in the manifest. AMD's corporate Lisa Su biography failed
with an HTTP/2 transport error, then timed out on an HTTP/1.1 retry. The official
investor-relations board page was captured instead; the failed page supplied no
text or role evidence. These are manual source choices, not autonomous discovery.

The fixed producer uses `prose_reference_model_prompt.txt`, the new
`additional_evidence` schema and the unchanged Go derived-reference validator.
Old prompts and model outputs remain unchanged. Normal tests are offline; the
live test requires explicit gateway, profile and a new output directory.

Requests/responses, discovery and a completion marker are retained here, with
separate semantic review in `review.json`. Six HTTP 200 responses yield four
structural passes, one truncated answer and one joint-activity contract rejection.
Passing citations still include wrong endpoint spans and instruction-derived role
proposals. See the [trial audit](../../../../docs/audit/prose-derived-reference-model-2026-09-17.md).
A completion marker means all attempts were recorded, not that extraction succeeded.
No identity, graph or money approval follows from this experiment.
