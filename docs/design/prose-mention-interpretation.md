# Literal mentions and relationship interpretations

Implemented as an offline, test-only contract proof on 2026-09-17 after the
[evidence-attachment trial](../audit/prose-evidence-attachment-2026-09-16.md).
This applies the existing [evidence-layer separation](./evidence-model.md), not
a new database model or production source adapter.

## Result and scope

Go can now validate and retain literal mentions independently of proposed meaning,
check their references and apply explicit correction/conflict links. Six retained
**synthetic** cases have manually reviewed annotations. Tests prove the representation
and conditional rules; they do not demonstrate improved automatic prose extraction.
No model was called or prompt changed in that initial step. Earlier model failures remain
unchanged, including the missed CFO role and incorrect handling of a retraction.

The subsequent [fresh model trial](../audit/prose-interpretation-model-2026-09-17.md)
tests an automatic producer of this contract. That separate experiment does not
convert the reviewed annotations into runtime rules or approve model proposals.

The implementation stays in `internal/audit/personaffiliation/*_test.go`.
No CLI, production evaluator, graph, source parser or financial behavior consumes it.
No person, organization, URL or named-case exception exists in the validator.

## Three separate objects

| Object | Contains | Does not establish |
|---|---|---|
| Literal mention | Local ID, original text, entry ID, decoded-text byte offsets and containing HTML span/hash | A role, alias, identity or truth claim |
| Interpretation proposal | Mention references, proposed subjects/organization, binding mode, kind, normalized label and assertion status | Accepted meaning, canonical identity or affiliation |
| Context link | Explicit interpretation IDs and `retracts` or `contradicts` meaning | Automatically discovered correction, exhaustive conflict search or source authority |

The report includes the exact source identity and **all supplied entries**, not only
selected mentions. Entry text is the existing reader's decoded/whitespace-normalized
text. Its HTML span refers to raw source bytes; the two coordinate systems remain
separate. The helper requires reader-verified input, not arbitrary source objects.

### Literal anchoring

Every mention must match contiguous text in its declared entry. An explicit byte
offset selects an occurrence; without one, the match must be unique. Repeated and
overlapping text cannot silently select the first occurrence. UTF-8 boundaries,
unknown entry IDs, duplicate local IDs and nonliteral strings are checked.

This permits source `founded` and a separate proposed `founder` label. Likewise,
`Lucía (Luci) Inés Vega` remains literal while `Lucía Inés Vega` may be a separately
labeled display-name proposal. The latter is not a canonical name or accepted alias.
An arbitrary proposed label can pass structural checks and still be wrong.

### Interpretation and binding

Kinds are `role`, `activity`, `name_form`, `correction` and `unknown`. Only role
proposals carry the existing role taxonomy. Activity is not promoted to employment
or executive authority; name forms do not merge people. Classifying a phrase as
an activity rather than a role is a **supplied interpretation**, not a string rule.

Subject and organization references must identify literal mentions and appear in
the interpretation's evidence references. Binding is `direct`, `coreference`,
`ambiguous` or `unresolved`. Multiple alternatives cannot claim one resolved endpoint.
For the CFO example, the reviewed interpretation explicitly references the named
antecedent and organization in one entry and the pronoun/role clause in another.
Go verifies that those anchors exist; it does not prove that `She` means that person.
Nor does a valid `clause` reference prove that its selected text contains the role:
a name or pronoun alone can pass structural checks. Full supplied entries remain
in the report, but semantic evidence sufficiency needs separate review.

Status is `asserted`, `denied`, `hypothetical` or `unclear`, relative to a supplied
interpretation of a source statement. An asserted statement is not verified truth.
Dates remain source wording; this proof adds no date parsing, tenure intervals,
continuity hypothesis or FEC-person binding.

### Context without erasing assertions

A reviewed correction can reference an original role interpretation through
`retracts`. It need not repeat the role title. Go preserves the original positive
statement and its supplied status, but adds a contextual blocking flag to that
specific target. Other roles are unchanged. Two role interpretations joined by
`contradicts` both receive conflict flags; neither wins because of list order.
Retraction and conflict flags coexist rather than overwriting one another.

The resulting review states distinguish context-blocked, nonpositive, ambiguous,
unresolved and still-unverified role proposals. All approval flags remain false,
including for apparently straightforward positive roles. A supplied link can be
wrong; an omitted correction is not detected automatically. Tests demonstrate both
the structural boundary and the inability to infer missing semantics.

This first proof only supports correction-to-role retractions and role-to-role
contradictions. It rejects self/dangling/duplicate links, unsupported link types and
correction-of-correction chains. It does not manufacture a transitive truth rule.
Unsupported input produces no partial derived report; source/capture fixtures stay
unchanged. The eventual interpreter must preserve unsupported cases explicitly.

## Tests and limitations

The [reviewed fixtures](../../tests/fixtures/person-affiliation/mention-interpretation-v1/README.md)
bind annotations to unchanged earlier captures and reconstruct the same lexical
inputs. They cover bound/ambiguous pronouns, activity versus role, correction targets,
parenthetical names, verb/title normalization and hypothetical appointment wording.
The annotations are gold examples of the proposed representation, **not repairs to
model responses or a runtime whitelist**.

Additional synthetic tests cover contradictory assertions, unrelated concurrent
roles, absent corrections, unsupported labels, forged references, duplicate IDs,
invalid schema, repeated occurrences, UTF-8 offsets and empty interpretation sets.
Ordinary tests run offline. The full Go suite and focused static/race checks pass;
the reader, grammar, earlier prompts and all model captures are unchanged.

Bounds are 32 source entries, 128 KiB annotation input, 64 mentions, 32 interpretation
proposals and 32 context links. These are research resource limits, not domain rules.
No FEC cycle, contribution threshold or person-specific matching rule participates.

## Automatic producer experiment

The [fresh trial](../audit/prose-interpretation-model-2026-09-17.md) uses a separate,
fixed test-only prompt/schema on three official-page excerpts and six fresh controls.
Its wrapper accepts only mentions, interpretations and context links. Go stamps
`model_proposal` origin; a model cannot claim reviewed provenance or set approvals.
Model-derived reports label their semantics unverified. Strict literal/reference
checks and whole-answer rejection remain unchanged; earlier reviewed reports replay
without changes.

The retained audit separates literal anchoring, subject binding, classification,
context links and names/time. Reviewed annotations are never an extraction lookup
table. This remains before affiliation acceptance, autonomous source discovery,
graph publication or any terminal-dollar policy.

The later [source-owned citation proof](./prose-citation-selection.md) supplies
literal mentions from token-ID selections and attaches complete selected entries.
It reuses this validator without changing older captures or their outcomes. Its
reviewed offline selections do not demonstrate improved automatic interpretation.
