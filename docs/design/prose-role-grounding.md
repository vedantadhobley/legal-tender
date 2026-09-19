# Role focus, endpoint surfaces and complete statement context

Test-only follow-up to the [uncorrected-name experiment](../audit/prose-role-binding-model-2026-09-17.md).
It changes the research representation, not the production data model, graph or
person/company acceptance policy. Earlier prompts, responses and outcomes stay intact.

## Boundary

Each proposal now separates three things:

- `focus`: a source-token selection highlighting one role title or correction phrase.
  It is not a complete statement or proof of meaning.
- `statement_entry`: the containing source entry. Go derives a complete entry citation
  and attaches its dates, qualifications and negation without relying on model-selected
  clause boundaries. All supplied excerpt entries also remain in the report.
- Endpoint surfaces: selections for the actual name, surname or pronoun at that role
  statement, separate from the fixed first-stage person/company candidate IDs.

An omitted surface is an explicit empty string, not a guessed name. The source entry
must contain every supplied surface and the focus. Other evidence references remain
explicit. A generated context ID cannot be supplied or overwritten by the model.

`direct` requires one candidate per endpoint and exact text equality between each
selected surface and its candidate. Go checks this necessary condition. Different
surface text, alternatives and absent candidates remain separate diagnostics. The
validator rejects a pronoun-as-exact-name claim instead of silently relabeling it.
It does **not** prove that a selected name refers to that role, even if both occur
in one paragraph. `coreference` is still a supplied interpretation, not a verified join.

## Meaning and scope

The prompt asks for distinct roles separately, with one literal focus each. It keeps
one compound title intact when that phrase denotes one job. This is a semantic task;
Go does not split every occurrence of “and” or maintain a title/person whitelist.
The structural validator cannot prove role atomicity or completeness.

Advisory councils, boards of visitors and non-governing civic roles retain their
literal scope. When the existing coarse taxonomy cannot express that role, use
`unknown`, not a claimed corporate directorship. First-stage institution/body names
remain uncorrected candidates; council membership does not resolve the parent entity.

Explicit unnamed roles are in scope as unresolved evidence with empty unsupported
endpoints. This resolves the prior empty-output expectation's conflict with the
prompt. It does not turn meetings, thanks or source instructions into named
affiliations. Previous annotations are not relabeled retrospectively.

Statuses and correction/conflict links retain the
[existing interpretation rules](./prose-mention-interpretation.md). Source dates
remain literal wording, not automatic intervals. Complete context preserves text;
it does not prove that the model read it correctly or found every role.

## Implementation and evaluation

The implementation stays in `internal/audit/personaffiliation/prose_binding_grounding*_test.go`.
It reuses the unchanged name, citation and interpretation validators. Original
proposals remain separate from derived whole-entry references and surface checks.
Unknown fields, invalid ranges, wrong candidate types and invalid references reject
the entire answer; no repaired or partially accepted response follows.

Research bounds remain 32 interpretations and links, 64 combined name/selection/
generated-context references, and 128 KiB proposal input. Normalized labels, role
classes, endpoint choice and semantic sufficiency remain unverified. Every approval
flag stays false, including for exact direct surfaces.

Five fresh synthetic inputs and pre-run expectations are fixed in the
[fixture inputs](../../tests/fixtures/person-affiliation/binding-model-v2/inputs.json).
They use the unchanged names-only model producer first, without manual corrections.
The bounded follow-up measures separate roles, coreference, institutional scope,
qualifications, ambiguity and corrections. It is not a whole-page or production
accuracy benchmark, nor an automated source-discovery implementation.

The [completed trial](../audit/prose-role-grounding-model-2026-09-17.md) retains
four structural passes and one reserved-ID rejection. Multiple-role and pronoun
handling work in one fresh case, but surname ambiguity and an omitted correction
link remain failures. Literal surface equality is not referent resolution; review
that distinction and explicit correction targets next, without changing old answers.

The subsequent [offline referent/target contract](./prose-referents-and-corrections.md)
now makes both assessments explicit without inferring them from literal matches or
extra evidence references. Independent and retained-source annotations pass; the
automatic producer has not yet been adapted or evaluated against that new contract.
