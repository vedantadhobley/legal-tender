# Isolated name-selection trial — 2026-09-17

The names-only task matches all 22 reviewed regression spellings and 13 of 15 fresh
spellings, with two extra selections in the fresh biography. All eight attempts
complete and pass coordinate checks. This supports testing the narrower task further,
not promoting it to accepted identity or relationship extraction.

This experiment isolates literal person/organization name selection after the
[derived-reference trial](./prose-derived-reference-model-2026-09-17.md) exposed
wrong endpoints despite valid citations. It does not extract or approve roles,
aliases, identity links, employment or financial attribution.

## Fixed task and sample

The [fixtures and expectations](../../tests/fixtures/person-affiliation/name-model-v1/README.md)
contain four unchanged prior inputs and four fresh inputs. The regressions cover
whole-clause endpoints, a truncated longer biography and two wrong organization
spans. The fresh group contains an unused biography excerpt from retained AMD HTML,
Unicode/name punctuation, a shared surname, and a no-names instruction control.
No new source was fetched. Page/excerpt selection remains manual.

The task selects one occurrence for each distinct literal name spelling. Full names,
standalone surnames and written abbreviations remain separate; they are not merged
into identities. A repeated exact spelling may use any complete source occurrence.
The experiment does not measure extraction of every occurrence. Complete supplied
source entries remain in the report.

The fixed prompt excludes honorifics, pronouns, possessives, job titles and internal
corporate department labels; it retains name punctuation and legal suffixes. Named
companies, nonprofits, government bodies and educational institutions are in scope,
whether mentioned as employers, acquisition targets, schools or elsewhere. Selecting
a name inside an instruction is lexical evidence, not executing that instruction.

`expected.json` contains 37 reviewed `(kind, exact text)` surfaces, fixed before any
model call. These labels never enter a model request. Go checks that every expected
spelling has a source-token span, then compares the source text selected by the model
without trimming, case folding or fuzzy matching. Wrong ranges/types produce an
unexpected selection and leave the intended name missing. Duplicate selections do
not inflate matches. Scores are separate from coordinate validity and from identity
truth. Invalid or truncated answers receive no salvaged partial score.

Live discovery confirmed `gpt-oss-120b`, medium reasoning, 4,096 output tokens and
JSON Schema support. The request keeps seed 1 and advertised sampler defaults. A
fixed names-only schema removes interpretation, role, date and relationship fields;
Go still validates token ranges and stamps model provenance. This narrows the task,
not merely its output syntax. Results cannot be treated as full relationship-extraction
accuracy or an equal-task speed comparison.

Each input gets one attempt. There are no retries, response repairs or prompt changes
after seeing answers. All earlier experiment prompts, captures and outcomes remain
unchanged. The source reader, name/affiliation runtime, graph and money rules are
not modified. No downstream role producer is connected to this experiment.

## Results

All eight requests return HTTP 200 with `finish_reason=stop`. Go reconstructs exact
source text for every selection. The independent exact-name comparison reports:

| Input | Group | Matched / expected spellings | Extra selections |
|---|---|---:|---:|
| Microsoft biography | Regression | 9 / 9 | 0 |
| AMD biography | Regression | 7 / 7 | 0 |
| Employer transition | Regression | 3 / 3 | 0 |
| Correction/conflict | Regression | 3 / 3 | 0 |
| Previously unused biography | Fresh | 4 / 6 | 2 |
| Unicode/punctuation | Fresh | 5 / 5 | 0 |
| Shared surname | Fresh | 4 / 4 | 0 |
| No-names instruction | Fresh | 0 / 0, correctly empty | 0 |

No duplicate predictions occur. Regression results repair neither the earlier
responses nor their scores: these are new answers to a different, narrower task.
The whole-clause and two malformed organization selections no longer appear. The
longer known biography completes, but roles are no longer requested, so this is not
proof of equivalent work fitting the generation budget.

The [retained per-case review](../../tests/fixtures/person-affiliation/name-model-v1/review.json)
records the remaining fresh-biography errors:

- The standalone surname is omitted even though its full-name form is selected.
- A company name loses the period in `Inc.`. Exact scoring counts the altered span
  as unexpected and the complete expected spelling as missing; it does not establish
  that the model meant a different company.
- A college's board-of-visitors title is an extra selection under the explicit task
  boundary. It is literal source text, not a fabricated organization. Institution
  versus governing-body scope remains a design question before production use.

The synthetic punctuation case preserves name-internal Unicode and the legal-suffix
period. Both similar full names and the standalone surname remain distinct in the
shared-surname case; no ambiguous reference is resolved. The instruction control
returns no names or approvals. Those small controls do not establish general
injection resistance, complete name coverage or population accuracy.

## Cost and replay

Across all eight attempts, reported usage is 12,576 prompt tokens and 9,392 completion
tokens. Summed request time is 206.289 seconds; the harness takes 206.31 seconds.
Service usage includes reasoning and cached-prefix accounting. The sample and task
differ from the previous full-interpretation run, so these are not a throughput or
end-to-end extraction-cost comparison.

The test-only Go producer reuses the existing bounded capture helper, token catalog
and citation validation. A separate exact comparator uses the pre-run labels and
prints matched, missing and unexpected spellings during offline replay. Expectations
never enter model input or change source evidence. The report retains model-supplied
kinds and exact citations, with empty interpretation/link arrays and false approval
flags. There is no normalizing repair path or application import.

Replay validates source bodies, pre-run inputs/labels, discovery, fixed requests,
responses, completion status and derived reports. Unit tests cover wrong-but-literal
name spans and types, duplicate scoring, empty answers, invalid ranges and forbidden
relationship/provenance/approval fields. Full Go tests, focused vet/race checks,
formatting and documentation links pass. Earlier model outcomes replay unchanged.

## Next decision

The result justifies a small second-stage experiment: supply the uncorrected name
candidates and original context to a role-binding task, then separately measure
binding, role meaning, corrections and abstention. Do not replace missing names with
reviewed labels, silently trim the remaining spans or treat candidate IDs as verified
identities. Retain ambiguity and unsupported bindings explicitly.

That second stage is not implemented or authorized for production by this trial.
The institutional-name boundary, occurrence coverage, automatic source discovery,
FEC-person correspondence and dated affiliation acceptance remain separate gaps.
