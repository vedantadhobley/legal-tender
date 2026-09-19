# Prepared referent/target regression trial

Prepared offline on 2026-09-18. The complete comparison ran on 2026-09-19; see the
[audit](../../../../docs/audit/prose-referent-model-comparison-2026-09-19.md) and
[review](./review.json). The authoritative final capture directories are
`gemma-4-31b-20260919-long/` and `gpt-oss-120b-20260919-long/`.

The first trial started, then paused at the user's request.
The partial `gemma-4-31b/` directory retains its model inventory and two 90-second
client timeouts (`microsoft-hood`, `amd-su`), with no model answers or completion
marker. GPT-OSS did not start. The user reported another application testing on the
node; these transport failures are not extraction-quality results. Our client
container was stopped to prevent further requests. Backend cancellation of any
in-flight request was not checked. Await explicit permission before resuming, and
use a new output directory rather than overwriting this partial attempt. A second
90-second Gemma attempt and a complete GPT-OSS attempt are also retained in dated
directories. They established that the client limit was too short and exposed output
variability; they are not the final comparison. No directory was overwritten.
The [contract and trial boundary](../../../../docs/design/prose-referents-and-corrections.md)
describe the experiment; [cases.json](./cases.json) fixes its review checks before
any new inference.

Inputs reuse retained sources and automatic name captures:

- `microsoft-hood`, `amd-su`, `fresh-mcclure`: the
  [name-stage source cases](../name-model-v1/inputs.json) and their unchanged
  captures, validated through the existing binding-seed reader.
- The five `grounding-*` cases: the
  [grounding source cases](../binding-model-v2/inputs.json) and unchanged captures
  in that fixture's `names/` directory.

The Go reader reconstructs each source excerpt and validates the captured original
name request, response and evidence before constructing the new request. Model input
contains only the new task prompt, source-token catalog and uncorrected candidate
mentions. Review checks and prior role answers never enter requests.

All cases have been examined before. This is a known-case regression diagnostic,
not an unseen benchmark, extraction-accuracy estimate or identity-acceptance gate.
The real names retain upstream omissions and scope/punctuation errors. Later review
must separate those limitations from second-stage failures.

The final run used eight sequential generation requests per model and the same cached
names. It used a recorded 900-second client limit; every request returned HTTP 200.
Literal validation, semantic review, abstention and capacity failures remain separate
outcomes. Offline replay verifies exact requests, captures, review summaries and false
approval flags.
