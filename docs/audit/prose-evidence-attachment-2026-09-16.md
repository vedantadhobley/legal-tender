# Code-owned prose evidence attachment

Research performed 2026-09-16 America/New_York; captures use 2026-09-17 UTC.
The user approved this bounded follow-up to the
[stronger-model trial](./stronger-prose-model-comparison-2026-09-16.md).

## Outcome

Code-owned attachment works: successful outputs contain original reader text and
spans, never model-rewritten quotations. All fifteen requests returned HTTP 200 and
completed normally in 229.120 seconds of request time. Eleven answers passed literal
validation; four were rejected. The six known real cases have five passes, including
one abstention; the nine fresh controls have six passes, also including one abstention.
These are **literal-valid answer counts, not semantic accuracy**.

The model still misses or misinterprets relationships. The fresh unambiguous-pronoun
case loses the CFO role; the ambiguous-pronoun case labels meeting attendance as a
role; the retraction case emits a positive role despite the correction. Do not wire
this output into accepted affiliations. Evidence preservation is improved, but
relationship interpretation is not solved by changing the citation format.

## What changed

The model now proposes literal fields and `evidence_entries` IDs, not quotations.
Go resolves those IDs against verified reader output and attaches the complete
selected entries. The output also retains **every supplied entry**, including
unselected entries, with source URL, body hash and original HTML byte spans.
The model cannot supply or alter that evidence text, URL, span or hash.

The source reader already decodes HTML entities and normalizes text whitespace.
Attached text is exact **reader output**, not a claim that decoded text equals
raw HTML. Its half-open byte span and hash locate the original source bytes.
The attachment helper expects reader-verified input; it is not an independent
authenticator for arbitrary caller-constructed source objects.

This is a test-only implementation. No application model client, source parser,
accepted relationship feed, entity resolution, graph write or financial rule changed.
Old prompts and captures remain intact. The opt-in tests share bounded HTTP capture
mechanics; they do not introduce a production model abstraction.

## What is still rejected or unproven

Strict JSON decoding rejects duplicate keys, unknown properties and malformed output.
Unknown, duplicate, empty or excessive entry references fail. Missing/null time fields,
invalid polarity and nonliteral fields fail through the unchanged literal validator.
Each field must appear contiguously in a selected entry; finding its pieces across
entries does not count. No name stitching, title normalization or answer repair occurs.
One invalid proposal rejects the whole answer; the complete response is retained.

For successful literal checks, the raw model response and Go-generated attachment
are separate fields. Go never silently changes which entries a proposal selected.
Full supplied context remains available even if the model omits a relevant entry,
but that does **not** make the selected references complete or the claim correct.

Tests explicitly demonstrate remaining semantic gaps: an asserted role can still
pass literal validation despite a correction elsewhere, and a name substring can
pass the literal alias check without an actual alias statement. Role meaning, dates,
pronouns, alias correspondence and donor identity require separate assessment.

## Experiment boundaries

The [retained fixture](../../tests/fixtures/person-affiliation/prose-attachment-v1/README.md)
contains six known real-source regressions plus nine new synthetic cases. The fresh
controls and expectations were written before inference. They cover unambiguous and
ambiguous pronouns, a separate-entry retraction, a parenthetical Unicode name, distinct
role dates, unbound name/title blocks, employer versus occupation, an instruction/date
trap and a conditional appointment. Expectations are not sent to the model.

The real excerpts are the same reviewer-selected windows as before. This is not
automatic retrieval, whole-page extraction, new real-source coverage or blinded
evaluation. Ford still has no card structure in its input. Missing context outside
the supplied window cannot be recovered by code-owned attachment.

The ready local `gpt-oss-120b` model uses the previous medium-reasoning, 4,096-token
configuration, default samplers and seed 1. Each request has a 90-second timeout,
no tools and no retries. The new prompt/schema were fixed before the batch. This
changes the task contract, so higher literal-valid counts must not be presented as
an apples-to-apples extraction accuracy improvement. Regeneration is not guaranteed
byte-identical; offline replay checks the retained observation.

## Reviewed results

| Case | Literal check | Separate interpretation finding |
|---|---|---|
| Microsoft | Pass | Separate chairman/CEO dates; original passage replaces the previous invented quote |
| IBM | Pass | Exact compound title, named subject and organization; unknown time |
| Ford | Pass, empty | Does not join unbound name/title blocks |
| Ridgeline | Pass | Supported role-before-name wording without added praise/family roles |
| Parenthetical names | Pass | Ted/Rick name forms now literal; surname/alias type and contextual founding date still need interpretation |
| Sprinklr proxy | Rejected | Changes lowercase source wording to `Board of Directors`; also treats an honorific reference as an explicit alias |
| Bound pronoun | Pass | Misses CFO and its dates; emits only `joined` |
| Ambiguous pronoun | Pass | Avoids guessing CEO, but emits `attended a meeting` as two roles |
| Separate retraction | Rejected | Positive and denied CEO outputs; denied proposal cites no title, while the positive interpretation ignores the correction |
| Literal alias | Rejected | Role keeps parenthetical name, alias record still stitches the name |
| Role dates | Rejected | Years separated correctly, but `founded` becomes nonliteral `founder` |
| Unbound cards | Pass, empty | No ordinal name/title matching |
| Occupation/employer | Pass | ACCOUNTANT and food-bank treasurer stay distinct |
| Instruction/date | Pass | Denial and unknown tenure survive the embedded instruction |
| Conditional appointment | Pass | Hypothetical polarity; attached text keeps the condition and absence of an appointment |

The retraction rejection happens because one selected entry lacks a literal field,
not because the validator understands corrections. Likewise, capitalization and
normalized names may be useful in a later interpretation layer but cannot silently
replace an exact evidence field. Whole-answer rejection discarded no captured bytes;
the complete raw proposals remain inspectable. There were no prompt edits, retries,
case-specific repairs or additional model batches after reviewing these outcomes.

## Verification

Offline tests reconstruct source windows, compare the full request, validate the
complete responses and reproduce exact Go attachments. They preserve failures as
failures. Input/capture hashes and separate semantic review live with the fixture.
All identity, graph-publication and financial approval flags stay false.

The full Go suite and focused static/race checks pass in the existing capped offline
compiler container. The original prompt, grammar and reader hashes are unchanged;
formatting and documentation links pass. No runtime deployment was performed.

## Next decision

Keep code-owned evidence attachment as the useful result of this experiment; do not
keep retrying these same examples until they pass. Next define and test the boundary
between a **literal mention** and its **role/name interpretation**: normalize only in
separate derived fields, distinguish a role from incidental activity, and preserve
retracted or conflicting statements without presenting them as accepted positive
affiliations. Cross-entry subject and correction handling need explicit evaluation.

That is a proposed next bounded design/test step, not authorization for another
source pipeline or automatic acceptance. Fresh real pages, autonomous source/window
selection and accepted FEC-person binding remain separate gaps. No terminal-dollar
policy follows from this trial.

Follow-up, 2026-09-17: the user-approved
[mention/interpretation proof](../design/prose-mention-interpretation.md) now tests
this boundary using reviewed annotations over retained synthetic inputs. It changes
no captured model answer and makes no claim that these extraction failures are fixed.
