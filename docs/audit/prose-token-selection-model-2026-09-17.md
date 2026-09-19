# Token-ID interpretation trial — 2026-09-17

The model can select source-token ranges, but this trial does **not** establish a
reliability improvement. Nine attempts produced four structural passes, four
reference-check rejections and one capacity failure. Valid ranges still include
wrong endpoints and unresolved pronouns. No affiliations are accepted.

## Fixed scope

The [offline citation contract](../design/prose-citation-selection.md) now has an
opt-in, test-only producer. It reuses the same three real-source excerpts and six
synthetic controls as the [previous trial](./prose-interpretation-model-2026-09-17.md).
These are known examples used during development, not a fresh accuracy benchmark.
Source HTML, selection windows and original input expectations remain unchanged.

Live discovery confirmed `gpt-oss-120b` and supported medium reasoning, 4,096 output
tokens and JSON Schema. The run kept seed 1 and advertised sampler defaults. A new
fixed prompt/schema replaced copied quotes/offsets with token IDs and clarified
selection requirements. It reused the interpretation vocabulary and validators.
This compares task formats, not a tokenizer-only ablation or equal total compute.

The [captures and manual review](../../tests/fixtures/person-affiliation/citation-model-v1/README.md)
retain every request/response, discovery and derived report where valid. The model
saw only original entry text and token catalogs, not reviewed expected meanings.
There were no retries, response repairs, partial accepted reports or post-result
prompt/validator changes. No source fetch or infrastructure change was needed.

Eight requests returned complete HTTP 200 responses with `finish_reason=stop`.
The contradiction control instead returned HTTP 429 `capacity_exceeded` after
60.010 seconds. That is an infrastructure outcome, not a semantic failure, and it
was not silently replaced with its earlier result. The complete harness took
454.99 seconds; its `PASS` means all attempts were recorded.

## Mechanical validity versus meaning

All 56 selections in the eight complete responses reference valid token ranges.
Go reconstructs source-owned text without rewriting whitespace or stitching clauses.
Four answers nevertheless fail the unchanged interpretation-reference rules: three
omit all evidence references and one omits endpoint references from those lists.
Those answers still produce no accepted partial interpretation report.

| Case | Structural outcome | Separate selection/meaning review |
|---|---|---|
| Apple | Pass | Completes where the earlier run truncated, but its organization span includes an extra word; the COO clause omits introductory time context |
| NVIDIA | Rejected: empty evidence lists | Literal job/date ranges are available without the earlier splice; combined executive titles and incomplete binding remain |
| Salesforce | Pass | Names, organizations and reviewed roles have useful spans; short surname is not explicitly linked to the full name, and age wording drops out of a selected clause |
| Bound pronoun | Rejected: omitted endpoint references | CTO endpoints select only the pronouns, not their named antecedents |
| Ambiguous pronoun | Pass | Keeps ambiguity but selects only the pronoun, losing the earlier explicit named alternatives; normalized labels are empty |
| Targeted correction | Pass | Exact clauses and named endpoints; retraction targets only the intended role and leaves the unrelated role unverified |
| Name and succession | Rejected: empty evidence lists | Distinct roles and source name survive, but later endpoints remain pronouns; no separate alternate-name proposal |
| Contradiction | No model answer: capacity failure | Unassessed in this run |
| Conditional instruction | Rejected: empty evidence list | Raw proposal selects the full condition/date/no-appointment text and stays hypothetical; no repair fills the missing references |

The Apple organization span illustrates the distinction directly: its token range
is legal but includes text outside the organization name. Go cannot establish that
the chosen span names the intended entity merely by reconstructing exact bytes.
Keeping complete selected entries makes review possible; it does not validate the
model's binding, classification, source truth or historical interpretation.

The fixture's semantic review remains separate from code-derived reports. Diagnostic
replay can print individual selected spans from rejected answers, clearly labeled
as diagnostics; it never publishes them as repaired relationship proposals.

## Measured cost

Exclude the capacity-failed case from paired usage comparison. The eight remaining
case IDs received HTTP 200 in both runs; the earlier Apple answer still consumed
its full generation budget despite producing no final answer.

| Captured measure, same eight cases | Earlier copied-mention task | Token-ID task |
|---|---:|---:|
| Reported prompt tokens | 8,114 | 12,148 |
| Reported completion tokens | 17,580 | 17,833 |
| Summed request elapsed time | 356.097 s | 394.955 s |
| Structural passes | 4 | 4 |

Input tokens increased about 49.7%; completion tokens increased about 1.4%.
Completion usage includes the service's generation accounting, not just visible
final JSON. Reported prompt counts include differing cached-prefix usage. This is
one local sample, not a stable throughput or monetary-cost estimate. Saturation and
queueing make the timings unsuitable for attributing a speed difference to format.
The equal pass counts do not establish equal semantic accuracy: the passing cases
and selected meanings changed.

## Code, verification and next decision

The producer uses the existing bounded capture helper. Go stamps `model_proposal`
origin, attaches exact citations and full context, and rejects model-supplied approval
or provenance fields. All changes stay in the test-only audit package; the lexical
reader, original prompts, prior captures, FEC calculations and graph behavior remain
unchanged. No model or affiliation extractor is promoted to production.

Offline replay rebuilds requests, checks retained hashes and task controls, recomputes
reports/rejections, and verifies all 56 ranges plus false approval flags. The full Go
suite, focused static/race checks, formatting and relative documentation links pass.

Keep this result unchanged. Before another model batch, review the output contract
for **redundant bookkeeping**: required evidence references already follow from an
explicit clause and explicit subject/organization selections, so Go can potentially
derive their union. Additional supporting references would remain explicit. Prove
that small change offline rather than silently repairing these saved responses.

That simplification would not fix wrong spans, pronoun antecedents, missing role
meaning or generation reliability. Keep those gaps separate. Any later accuracy
test needs fresh examples; accepted identity, affiliation edges and autonomous source
discovery remain out of scope. The capacity-failed control is still unmeasured here.
