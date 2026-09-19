# Literal mentions, proposed referents and correction targets

Test-only contract and opt-in producer following the
[whole-entry grounding trial](../audit/prose-role-grounding-model-2026-09-17.md).
It addresses representation gaps exposed by the surname and missing-retraction
counterexamples. The contract proof and producer preparation ran offline; the later
model comparison is retained below. There is no production adapter, graph edge or
accepted person/company identity from this work.

## Separate literal matching from referents

The existing grounding proposal preserves a role highlight, actual name/pronoun
surfaces, candidate mention IDs and complete statement-entry context. Its `direct`
binding checks literal surface equality. That cannot resolve which person a surname
means, even when the source contains the exact same surname as a candidate.

The new wrapper requires a separate referent assessment for each role, with independent
subject and organization choices:

| State | Candidate list | Meaning |
| --- | --- | --- |
| `unassessed` | Empty | No semantic referent assessment supplied |
| `unresolved` | Empty | Explicit inability to propose a referent |
| `ambiguous` | At least two distinct, correctly typed mention IDs | Supplied alternatives; no winner |
| `proposed` | One correctly typed mention ID | Unverified referent proposal, not accepted identity |

Candidate IDs still refer to source mentions. There is no canonical-entity registry
or `resolved`/`approved` state here. Exact spelling never populates an assessment
automatically. The original surname proposal can remain unchanged while a separate
review records alternatives; neither is silently rewritten to match the other.

Go validates states, cardinality, candidate types and complete per-role coverage.
It cannot prove that the proposed person is right, that alternatives are exhaustive,
or that a short name should not have been proposed alone. A wrong single proposal
can still pass as **unverified**; this is not automatic disambiguation.

## Make correction targeting explicit

Every interpretation already labeled `correction` must have one target assessment:

- `proposed`: one or more distinct role IDs that the supplied interpretation says
  are withdrawn. Go derives one `retracts` link per target.
- `ambiguous`: at least two possible role targets. Preserve alternatives; do not
  retract them all or choose the first.
- `unresolved`: an explicit empty target list. Keep the correction visible without
  inventing a retraction.

Omitting a correction assessment is invalid. Mentioning a role in extra evidence
does not supply a target. Direct `retracts` links in the nested grounding input are
rejected by this wrapper, so correction assessments are its sole retraction authority.
Existing role-to-role contradiction links remain unchanged. Targets must be roles,
not corrections; no transitive correction rule or target-by-name heuristic is added.

Explicit proposed targets use the existing contextual flags without rewriting the
original assertion status or affecting unrelated roles. These are still supplied
interpretations, not independently verified corrections. A model that omits the
entire correction or falsely proposes a target remains a semantic failure that this
structural contract cannot discover.

## Evidence and provenance

`supplied_proposal` retains the original grounding and all supplied assessments.
`derived_grounding` contains only the additional links implied by explicit proposed
correction targets, with unchanged source entries, literal evidence and false approval
flags. Referent review states stay separate from the grounding's literal binding.

Grounding origin and assessment origin are caller-owned and distinct. The retained
counterexample tests keep original model evidence labeled `model_proposal` while
the new manual overlays are labeled `reviewed_fixture`. A model cannot supply its
own origin or approval fields. Old captures, prompts and outcomes remain unchanged.
The new producer labels both origins `model_proposal`; it never supplies reviewed
overlays or grants the model control over provenance.

## Verification and limits

Implementation is confined to `internal/audit/personaffiliation/prose_referent*_test.go`.
It reuses the existing strict JSON, grounding and context validators. Limits remain
128 KiB input, 32 role/correction assessments, typed bounded references and the
grounding contract's original limits. Invalid input yields no partial report.

Independent synthetic tests cover absent assessments, wrong/duplicate references,
state/cardinality mismatches, ambiguous and collective correction targets, preserved
conflicts, unrelated roles, forged provenance, empty proposals and deterministic
replay. The two retained counterexamples prove that:

- Exact surname correspondence does not manufacture a referent assessment; reviewed
  alternatives do not overwrite the original model's surname selection.
- An omitted correction assessment fails; an unresolved one adds no link; an explicit
  reviewed target adds only the corresponding derived retraction.

Those overlays are test annotations, not improved model answers, runtime whitelists
or an accuracy score. No new inference ran for this step. No model deployment or
configuration changed, and no source, graph or money rule changed.

Full Go tests, focused static checks and prose race tests pass, including unchanged
replay of earlier model results.

## Offline producer preparation — 2026-09-18

The additive `prose_referent_model_test.go` adapter now builds the nested contract
and uses the existing capture harness. The new prompt distinguishes literal
grounding, semantic referents and correction targets. Its schema limits endpoint
IDs to correctly typed original name candidates. Go still checks state-dependent
cardinality, reference coverage and target validity; structured output is not trusted
to enforce meaning. No new dependency or production client was added.

The [fixed evaluation set](../../tests/fixtures/person-affiliation/referent-model-v1/README.md)
contains three retained real-source excerpts and five synthetic controls. All are
known regression cases, not a fresh or held-out benchmark. Cases cover employer
history, institutional scope, surname ambiguity, anonymous roles, withdrawal targets,
nearby names and source instructions. Case-specific review checks stay outside the
request. Original automatic name captures, including their errors, are reused without
normalization or repair. This evaluates the second stage, not end-to-end extraction.

Offline tests construct every request, compare unchanged source/name inputs and
transport controls, check typed candidate schemas including empty lists, reject
malformed answers, and verify caller-owned provenance and false approval flags.
A simulated response tests the adapter; it is not recorded as model evidence.
The tests also confirm that the older generic trial opt-in cannot start this trial.

No endpoint discovery, inference or deployment ran during preparation. Networking
was disabled for the test container. Existing model captures remain unchanged.

## Completed comparison — 2026-09-19

The [Gemma 31B and GPT-OSS trial](../audit/prose-referent-model-comparison-2026-09-19.md)
completed after preserving the paused and short-timeout attempts. The research
harness now accepts an explicit 10–1,800-second client deadline and records it in
new completion markers; its default remains 90 seconds. The final comparison used
900 seconds because valid Gemma answers took as long as 247 seconds. All sixteen
requests returned complete HTTP 200 responses without retry.

Each model passed structural validation on four of eight cases. After independent
semantic review, Gemma had three usable synthetic cases and GPT-OSS had two. Neither
had a usable result on any of the three retained real-source cases. Raw rejected
answers sometimes express the desired ambiguity or correction target, proving that
the representation is useful, but invalid fields are not salvaged. Valid answers
still invent corrections, omit roles or leave all referents unassessed.

The combined producer is rejected. Keep this contract and its strict validator, but
do not keep asking one response to recreate grounding and add semantic assessments.
The next bounded experiment should consume an immutable, already validated grounding
proposal and return only referent and correction-target assessments. Go then joins
the assessment to that exact proposal. This is a task-boundary change, not a title,
person or model-specific exception.

Any follow-up must include new cases before an accuracy claim and keep structural
validity, omission, abstention and semantic correctness separate. The present result
does not approve identities, relationships, graph publication or money attribution.

## Grounding-bound assessment result — 2026-09-19

The [narrow follow-up](../audit/prose-referent-assessment-model-2026-09-19.md)
removes grounding from the response entirely. Ten pinned inputs supply already
validated grounding, full source context, original name candidates and an exact
grounding digest. The model returns only referent and correction-target arrays; Go
performs the join and rejects `unassessed`, incomplete coverage, wrong types and
state/cardinality contradictions.

Both models return all ten answers and pass nine structurally. Each has six usable
answers and one usable result among the three retained real-source excerpts. This
confirms that task separation is the better experimental shape, but it does not
establish production quality. Both models still select a short person or organization
mention as its own referent instead of the supported full-name antecedent. Another
control retains an obvious endpoint as unresolved.

Keep the narrow request and exact join, but stop prompt iteration at this boundary.
The next contract must distinguish literal occurrences, proposed antecedents or
mention-equivalence groups, and later canonical entities. Do not add named-case
rules, accept model assessments, or publish graph or money effects.

## Offline occurrence, group and canonical-candidate boundary — 2026-09-19

The next test-only contract now separates three namespaces and two joins:

1. **Literal occurrences** are the unchanged `n*` records derived from the pinned
   first-stage name selections. Their source spelling, kind and entry remain evidence;
   none is a canonical person or organization.
2. **Mention groups** are explicit unverified discourse proposals. Each group names
   one supplied occurrence as its proposed antecedent and lists one or more distinct,
   same-kind occurrences. Role assessments point to group IDs, not occurrence IDs.
3. **Canonical candidates** enter only through a second input bound to the exact
   mention-group report digest. Each candidate has an independent namespace, record
   ID and pinned HTTPS source. A proposed mapping remains unverified and never changes
   a literal occurrence or mention group.

Go never creates a group from matching text. Occurrences may remain outside every
group. An ambiguous role points to two or more separate groups; its short surface
occurrence is not inserted into either alternative. Groups cannot overlap, mix person
and organization occurrences, use an occurrence ID as a group ID, or name an
antecedent outside their members. The canonical stage likewise rejects occurrence or
group IDs as canonical candidates, wrong-kind mappings, duplicate external records,
unpinned sources, incomplete group coverage and a changed grouping digest.

Retained counterexamples cover a surname after a full person name, a short company
name after a full company name, and a surname that can refer to two different people.
The first two can share explicit proposed groups; the third keeps both people in
separate singleton groups and preserves the role-level ambiguity. These are reviewed
fixtures proving the representation, not automatic outputs or accepted identities.
Generic validation contains no person, organization or title table.

Canonical source candidates cannot be supplied with `model_proposal` provenance.
The test uses synthetic pinned registry records only to prove the second join. No
production source adapter, discovery step, model prompt, entity acceptance, graph
edge or financial effect was added. Another model trial is not implied by this
contract; a future producer would need a separate bounded design and review.
