# TODO

> **Status:** The short Go rewrite queue below is active. The Python-era backlog
> after it remains frozen and must not be executed by default.

## Active Go rewrite

### Current priority: select the next bounded identity slice

- [x] Restore the complete Go rewrite, contracts, retained fixtures, orchestration,
  and documentation to Git tracking. Anchor runtime-directory ignores at the
  repository root so `internal/storage/` remains source-controlled. See the
  [2026-09-19 checkpoint](./audit/go-rewrite-checkpoint-2026-09-19.md).
- [x] Complete the four interpretation choices in the
  [pre-attribution review](./design/pre-attribution-review.md#decisions-to-review-first):
  candidate-ID resolution, committee-flow membership, conduit associations, and
  A/B comparison. Do not choose terminal or allocation policy during this review.
  - [x] Run the [focused 2024 validation](./audit/pre-attribution-interpretation-validation-2026-09-20.md).
    It proves that current candidate replacement contains material source-ID
    conflicts, both one-sided committee-ID populations are self references, and
    unbounded A/B candidates extend to 1,099 days. No policy changed.
  - [x] The user accepted all four evidence-backed recommendations. Publish the
    separate candidate-interpretation and direct A/B pair contracts without
    mutating retained v1 evidence. The complete
    [2024 gate](./audit/pre-attribution-interpretation-publications-2026-09-20.md)
    passes exact semantic replay.
- [x] Publish one product-shaped 2024 candidate slice from the accepted generation.
  It must separate authorized-committee receipts, support, opposition, committee
  paths, unresolved states, source drilldown, and coverage. It must not present
  topology as terminal-dollar attribution. The
  [candidate dossier gate](./audit/candidate-dossier-2026-09-20.md) passes for two
  retained candidates and discloses exact cross-domain release alignment.
- [x] After that slice exposes the actual evidence boundary, compare explicit
  terminal definitions and allocation methods. Preserve unresolved amounts and
  require exact conservation before promoting any terminal-source result. The
  [comparison and recommendation](./design/terminal-policy-comparison.md) and
  [two-candidate 2024 gate](./audit/terminal-policy-comparison-2026-09-20.md)
  pass. No scenario is adopted.
- [x] Accept the narrow partial boundary: exact explicit-earmark and direct
  reported source-appearance attribution at occurrence grain, with all
  committee-chain amounts unresolved. The separate cycle-wide production
  calculation and [complete 2024 gate](./audit/direct-source-attribution-2026-09-20.md)
  conserve all source rows and pass byte-identical worker-varied replay. The
  dossier diagnostic remains unchanged.
- [x] Review and land the September 20 worktree as one
  coherent checkpoint. The [high-level inventory](./audit/go-rewrite-inventory-2026-09-20.md)
  distinguishes the durable core, verified work in progress and parked exploration.
- [ ] Select the next domain slice using the candidate-shaped evidence view. Start
  with deterministic identity resolution for direct/earmarked source appearances
  unless committee-chain evidence disproves that order. Preserve unresolved cases.
- [ ] Investigate committee-chain allocation as a separate policy and calculation
  boundary. Graph reachability alone does not establish economic ownership.
- [ ] Add durable publication, Dagster wiring or graph projection for the direct
  source-appearance result only when an accepted downstream consumer needs it.
- [ ] Resume four-cycle A/B rollout, unattended weekly operation, and a user-facing
  investigative surface only after the next domain boundary works end to end.

### Paused exploration: relationship evidence and separate identity binding

The completed work below remains retained test and research evidence. It is not the
automatic next implementation path. Autonomous page discovery, additional prose-model
trials, and production identity publication stay paused until the core candidate slice
and attribution boundary require a specific capability.

- [x] Inspect retained structured relationship examples and verify the remaining
  recovery/build footprint. The [exploration note](./design/relationship-exploration.md)
  records actual source fields, quality gaps and the partial cleanup. No graph,
  model call, new source adapter or data copy was added.
- [x] Review concrete relationship examples and obtain approval for the bounded
  read-only query. Broader identity acceptance, conflicts, time-window inference
  and financial interpretation remain separate decisions.
- [x] Inspect retained inverse-parent/child, ambiguous-parent and noncontemporaneous
  board examples. The exploration note now shows the actual query distinctions;
  multiple parents are not automatically a contradiction. No new adapter or fetch.
- [x] Check the user-raised post-CEO ownership example against Cisco disclosures.
  The exploration note separates dated ownership from role intervals and current
  holdings; missing later disclosure is not a zero balance. No ownership feed added.
- [x] Complete the [bounded relationship evidence comparison](./audit/relationship-evidence-comparison-2026-09-16.md).
  Confirm retained FEC organization records, replay existing organization/person
  tools and inspect a concrete legislative example. Source failures, identity gaps,
  declared positions and missing financial/vote links remain explicit. Eight focused
  Go packages pass; no graph, crawler, LLM or financial policy was added.
- [x] Implement the approved [read-only relationship query](./design/relationship-query.md).
  Reuse the existing source reader, add parent/child claims and query either source
  endpoint. Retained examples, full Go tests, static checks and CLI smoke tests pass;
  old discovery selection, identities, graph and financial behavior are unchanged.
- [x] Test the [person and dated-role binding diagnostic](./design/person-binding-diagnostic.md)
  against retained namesakes, private-employer gaps and actual receipt dates.
  Name/employer correspondence, coarse-date applicability and identity acceptance
  remain distinct; reviewed company-text comparisons do not become automatic roles.
- [x] Check [dated first-party evidence](./audit/dated-person-role-evidence-2026-09-16.md)
  and person/organization correspondence for the private-company gaps. Retained
  source tests preserve historical role points, date meanings and unresolved
  identities; no automatic prose-role interpretation or graph publication.
- [x] Draft the [identity/affiliation acceptance proposal](./design/person-affiliation-acceptance.md)
  using the retained examples and ambiguity tests. This is a review document,
  not an accepted policy or new runtime evaluator.
- [x] Implement the approved bounded [candidate/time evaluator](./design/person-affiliation-acceptance.md#implemented-scope).
  Preserve source role observations, explicit denials, conflicts and optional
  continuity hypotheses with known origins and matching specific roles. Retained
  examples remain candidates/unknowns; no donor identity or money is accepted.
- [x] Specify and implement a concrete [identity-rule trial](./design/person-affiliation-acceptance.md#concrete-identity-rule-trial-rejected):
  structured name, employer/occupation and independent personal locality at the
  receipt date, with explicit rivals, missing/conflicting evidence and source
  dependence. This experiment was not production acceptance.
- [x] Check [trial feasibility](./audit/person-identity-rule-feasibility-2026-09-16.md)
  on retained and official-source positives and same-employer rivals. Reject and
  remove v1: exact titles, mandatory personal locality, receipt-day identity and
  blanket rival blocking fail strong candidates for the wrong reasons.
- [x] Specify and evaluate a
  [replacement identity evidence contract](./audit/person-identity-evidence-v2-2026-09-16.md). Keep identity,
  role semantics and role timing separate; make locality typed and optional; block
  only rivals plausible under the same distinguishing evidence; preserve support,
  dependence, conflict and abstention without a numeric score. Start with Chambers,
  Duffield, Catsimatidis and ordinary-employee contrasts. The pure Go classifier and
  retained replay pass; every approval flag stays false and no publisher is authorized.
- [x] Connect the [automated affiliation report](./design/affiliation-enrichment.md).
  Reuse source-native full names, aliases and coarse date comparison rather than
  requiring manually prepared v2 components. Bind exact FEC appearances to captures,
  retain all returned candidates and failures, and test reviewed plus code-selected
  additional appearances. The live sample and retained CLI replay pass; coverage
  remains sparse and no identity or graph approval is emitted.
- [x] Investigate [supplementary affiliation sources](./audit/supplementary-affiliation-sources-2026-09-16.md)
  on the same measured cases. Retain company role/alias prose, a historical licensing
  record and missing-employer controls. The unchanged lexical reader and source tests
  pass; source selection was manual and no production adapter or identity rule was added.
- [x] Evaluate a [source-cited prose baseline](./design/prose-relationship-prototype.md)
  on retained HTML and prior examples. The opt-in Go grammar emits four role syntax
  candidates and two parenthetical name forms across seven development fixtures;
  missing roles, wrong organizations, namesakes and unknown dates remain explicit.
  It accepts no assertions, donors or affiliations and changes no money rules.
- [x] Evaluate the unchanged baseline on
  [pages outside its development fixtures](./audit/prose-extraction-evaluation-2026-09-16.md).
  Five readable bodies yield one of six reviewed roles; five witnesses are missed
  and another selected page fails retrieval. The only emitted candidate is supported,
  but precision and alias accuracy are not established. No runtime rule changed.
- [x] Compare [bounded source-cited local-model proposals](./audit/local-prose-model-comparison-2026-09-16.md)
  with the unchanged grammar and namesake/role/negation/date controls. Fourteen
  self-hosted Gemma calls complete; nine answers pass literal citation checks and
  five are rejected. Complete responses and semantic review replay offline. Excerpt
  selection is reviewed; this is not automated retrieval or production acceptance.
- [x] Compare the same inputs, prompt and validation with the
  [stronger local model](./audit/stronger-prose-model-comparison-2026-09-16.md).
  Fourteen GPT-OSS calls complete; eleven literal passes and three rejections replay
  alongside unchanged Gemma captures. Semantic/context failures remain explicit;
  model-specific controls differ and no production acceptance follows.
- [x] Test [code-owned evidence attachment](./audit/prose-evidence-attachment-2026-09-16.md)
  instead of model-rewritten quotations. Six known real cases and nine fresh controls
  replay with eleven literal passes and four rejected answers. Original context is
  retained; pronoun, role-scope, name-form and retraction failures remain explicit.
  No field repair, partial salvage, production identity or graph changes.
- [x] Define and test the [mention/interpretation boundary](./design/prose-mention-interpretation.md).
  Offline Go checks preserve literal spans and separate supplied normalized labels,
  bindings and correction/conflict links. Six reviewed synthetic fixtures prove the
  representation, not automatic extraction; prior model failures remain unchanged.
- [x] Evaluate the [automatic producer on fresh excerpts](./audit/prose-interpretation-model-2026-09-17.md).
  Nine fixed inputs retain five literal/reference passes and four rejections,
  including every real-source case. Separate semantic review exposes incomplete
  grounding and binding as well as useful synthetic context handling. No repairs,
  runtime acceptance or source-pipeline changes.
- [x] Test [source-owned citation selection](./design/prose-citation-selection.md)
  offline. Go-owned token ranges preserve exact text and full selected entries;
  saved-query diagnostics retain ambiguity and missing text without repairing old
  outputs. Reviewed selections prove the contract, not automatic extraction.
- [x] Test [token-ID model selection and cost](./audit/prose-token-selection-model-2026-09-17.md).
  Nine known-case attempts retain four structural passes, four reference failures
  and one capacity failure. Valid ranges still include semantic errors; paired input
  tokens increase about 50% without an established reliability gain.
- [x] Simplify [required citation references](./design/prose-citation-selection.md#go-derived-required-references)
  offline. Go derives only the union implied by explicit clause/endpoints and extra
  evidence. The supplied proposal stays separate; earlier model outcomes, ambiguity,
  context rules and false approval flags are unchanged. No live run or runtime change.
- [x] Test the [simpler reference contract on fresh inputs](./audit/prose-derived-reference-model-2026-09-17.md).
  Six retained attempts yield four structural passes, one truncation and one joint-
  activity contract rejection. Semantic review records wrong endpoints and an
  instruction-derived unclear role. No production acceptance or repaired responses.
- [x] Isolate [person/company name selection](./audit/prose-name-selection-model-2026-09-17.md).
  Eight retained attempts match 22/22 regression and 13/15 fresh name spellings,
  with two extra fresh selections. Pre-run labels and exact scoring preserve remaining
  surname, punctuation and institutional-scope errors without repairing outputs.
- [x] Test [role binding against uncorrected name candidates](./audit/prose-role-binding-model-2026-09-17.md).
  Ten retained second-stage attempts pass structural checks; two fresh controls keep
  ambiguity, hypothetical status and missing endpoints. Semantic review preserves
  wrong binding labels, incomplete selected context and role/institution-scope gaps.
  No repaired first-stage names, accepted identity or production changes.
- [x] Test [whole-entry role grounding and endpoint surfaces](./audit/prose-role-grounding-model-2026-09-17.md).
  Complete context is Go-owned; the prompt separates roles and clarifies anonymous
  evidence scope. Five fresh two-stage cases retain four structural passes and one
  reserved-ID rejection, with surname ambiguity and a missing retraction still wrong.
- [x] Test the [offline referent/correction-target contract](./design/prose-referents-and-corrections.md).
  Literal correspondence cannot populate a referent choice; every supplied correction
  must declare proposed, ambiguous or unresolved targets. Independent annotations and
  reviewed overlays preserve the original retained model failures and false approvals.
- [x] Prepare the [opt-in referent/target producer](./design/prose-referents-and-corrections.md#offline-producer-preparation--2026-09-18)
  and eight-case regression set offline. Reuse original names without repairs;
  exclude review checks from requests and require a separate trial opt-in. No model
  discovery or inference ran; ordinary tests use no model service.
- [x] Run and review the [referent/target comparison](./audit/prose-referent-model-comparison-2026-09-19.md)
  on Gemma 31B and GPT-OSS. A recorded 900-second deadline yields sixteen complete
  responses, but neither model produces a usable real-source answer. Preserve all
  partial attempts; no output repair or producer promotion.
- [x] Test a [narrow assessment stage](./audit/prose-referent-assessment-model-2026-09-19.md)
  over immutable, already validated grounding.
  Return only per-role referent states and per-correction target states, then join in
  Go. Ten cases add two controls; both models pass nine structures and six semantic
  reviews, with one of three real-source cases usable. No title/person patches,
  output repair or validation loosening was added.
- [x] Separate [literal mention occurrences, proposed antecedent/equivalence groups
  and canonical entity candidates offline](./design/prose-referents-and-corrections.md#offline-occurrence-group-and-canonical-candidate-boundary--2026-09-19).
  Role assessments now reference explicit unverified groups; ambiguity never merges
  alternatives. Canonical candidates enter only through a separate digest-bound,
  source-pinned input. No model call, named-case runtime rule or downstream approval.
  General joint activity remains deferred.
- [ ] Test autonomous page discovery/selection separately before claiming a
  repeatable source refresh. No general officer feed was established by the bounded
  research; production selection and graph publication remain separate follow-ups.

### Existing exploratory tools and follow-ups — not automatic next steps

- [x] Add [person-affiliation rule tests](./design/person-affiliation-testing.md)
  for namesakes, role distinctions, job changes, unknown dates, multiple organizations
  and immutable source appearances. The pure Go evaluator and actual funding-path
  API boundary tests pass; this is synthetic screening, not live identity resolution.
- [x] Add the first [retained person/role corpus](./design/person-affiliation-corpus.md)
  and offline Go replay. Exact as-filed occurrences and reviewed source spans pass;
  employer/name variants, historical role precision and missing discovery remain
  explicit. No real identity is accepted and no runtime exceptions are added.
- [x] Implement [automatic structured-role extraction](./design/wikidata-role-extraction.md)
  from pinned Wikidata bodies, with source direction, raw statement identity,
  qualifier precision and explicit unsupported cases. Existing real snapshots and
  the offline CLI pass; no person/corporation exception table or financial changes.
- [x] Complete the first bounded fresh-source comparison against the retained
  affiliation annotations. One user-approved interactive request succeeded after
  three background-mode `maxlag` responses. All 31 selected statements replay;
  historical-role agreement, unobserved title and incomplete discovery remain
  distinct. Original corpus and background request policy are unchanged.
- [x] Add [exploratory Wikipedia-first affiliation discovery](./design/affiliation-discovery.md).
  Source-derived name/employer plans, bounded capture, page-linked QIDs and per-response
  role extraction pass synthetic and retained live replay. Preserve competing pages,
  missing items and failed/unattempted searches; no identity winner or graph approval.
- [x] Evaluate opt-in query variants and offline candidate relevance on the retained
  baseline. The live v2 capture/replay passes, but variants do not recover the missing
  companies; real namesakes and a non-company alias remain explicit counterexamples.
  Keep default v1 and all approval flags unchanged. This is not identity accuracy.
- [x] Add [independent registry name discovery](./design/organization-registry-discovery.md)
  from verified employer fields, without existing QIDs or LEIs. Bounded live
  capture/replay passes; two same-name LEIs remain ambiguous and empty windows
  remain coverage gaps, not company absence. No identities or affiliations approved.
- [x] Automate [retained company-page extraction](./design/company-page-evidence.md)
  in Go. Generic HTML text/metadata/JSON-LD entries preserve exact byte spans and
  all three pages pass fresh offline replay. No company selectors, prose-role
  interpretation, donor binding or financial changes.
- [ ] Test independent identity/role bindings before accepting affiliations. The
  prose syntax prototype above is not a verified assertion bridge. Company-page selection
  remains manual, registry names alone are ambiguous, and historical validity is
  unproven. Broader token searches did not replace this missing evidence.
- [ ] Extend company/inverse discovery and add an evidence-preserving screening
  bridge. No permission to omit maxlag from background ingestion is implied.
  Cover real namesakes/ordinary employees, private-company
  gaps, occupation conflicts, job changes and incomplete discovery before publication.
  Define interval/overlap semantics for preserved coarse dates without fabricating
  day boundaries; the new reader only extracts year/month/day precision.
  No donation threshold or single primary-company shortcut defines leadership.

- [x] Implement [bounded Go organization capture/replay](./design/organization-resolution.md)
  over source-backed FEC connected-organization and employer strings. Preserve raw
  responses, exact source membership, ambiguity and failed observations. The small
  real CM capture replays byte-for-byte; this is not resolution-accuracy acceptance.
- [x] Build the [offline evaluation corpus and corroboration requirements](./design/organization-evaluation.md).
  Retain primary-source snapshots and reviewed annotations. Separate matching misses,
  retrieval misses, missing entities, unknown alternatives and source failures.
  The first corpus is diagnostic, not representative or identity-approval evidence.
- [x] Add opt-in v2 candidate proposals for letter/number-boundary and legal-suffix
  differences, with explicit reasons and counterexamples. Preserve default v1
  and expose broader rivals as ambiguity. Both reviewed misses now produce proposals
  on unchanged evidence; neither annotated counterexample is proposed. No benchmark
  identities enter production rules and no identity publication is approved.
- [x] Add [bounded exact-LEI registry corroboration](./design/organization-corroboration.md).
  Derive requests from saved candidate claims, retain GLEIF source bytes and replay
  offline. Hard-case tests cover parent/subsidiary names, old names, qualifiers,
  duplicate/competing identifiers and source/status failures. The real fixture
  observes one LEI record but approves no FEC identity; reviewed labels are unchanged.
- [x] Implement the [independent SEC issuer candidate path](./design/organization-issuer-discovery.md):
  bulk capture, closed parser and pinned offline matching of original FEC strings,
  with no Wikimedia/LEI dependency. Preserve duplicate rows and competing CIKs;
  synthetic and retained-query tests do not claim real identity accuracy.
- [x] Run the SEC live directory capture and fresh pinned-query replay. Retain the
  unchanged public body, keep operator contact/capture metadata private, and test
  the real candidate outcomes. One CIK candidate is observed; no identity is approved
  and no reviewed labels or matching rules change.
- [x] Add the [filed registrant identity check](./design/organization-filed-identity.md).
  Capture an explicit SEC filing and compare tagged name/CIK evidence offline with
  exact source spans. Live capture and fresh replay pass without approving any
  identity. Preserve the FEC registration 403 and committee-address field scope;
  do not promote either into an independent FEC-to-CIK binding.
- [ ] Evaluate candidate usefulness and independent corroboration on broader real
  evidence. GLEIF name search now supplies an independent missing-ID path; expand
  nonissuer coverage separately. Neither LEI nor directory membership is required for graph
  representation, and source observations are not transaction-time identity proof.
- [ ] Accept an organization identity policy before publishing identity edges.
  Independently binding FEC text, parent/brand/entity distinctions and temporal
  validity remain open; neither name nor registry diagnostics approve publication.
  Keep this policy review alongside the bounded affiliation tests, not an unbounded
  new source crawler.
- [ ] If filing evidence is adopted by that policy, define automatic filing-reference
  selection, amendment handling and refresh. Current commands require explicit
  CIK/accession/document inputs; a reusable parser alone is not full acquisition automation.
- [ ] Add person/organization relationship resolution and source-validity handling
  only under explicit evidence contracts. No employment, parent-company, ownership
  or terminal-dollar policy is implied by an exact name proposal.
- [x] Reclassify the report-scope document as the derived
  `contracts/calculations/fec/report-scope/v1/policy.json`. It is an assessment over
  retained filing bytes and metadata, not source-acquisition metadata. The shared
  source-contract gate remains strict and now passes without a path-specific exception.

The [2026-09-15 scope correction](./design/pre-attribution-review.md) separates the
user's node/edge assumption review from the assistant-added recovery project.
Keep candidate-ID resolution, committee-flow membership, conduit associations
and A/B matching explicit in user review. Do not add recovery machinery, fetch another
cycle or change financial interpretations as a substitute for that review.

Use the [completion plan](./design/connected-funding-graph.md) for the Python/Go
comparison, exact scope and acceptance gates. Missing original-scope connections
are unfinished core work, not optional coverage improvements.

- [x] Implement the [shared-reference profile](./design/shared-reference-profile.md)
  over retained compact inputs. Inspect roles hidden by degree rejection, prove
  complete versus partial peer coverage, retain exact signed sums and automatic
  witnesses, and preserve all existing association decisions.
  - [x] Complete and retain the [2024 run](./audit/shared-reference-profile-2026-09-14.md);
    the full decision artifact and all counts match the accepted publication.
    Complete compatible groups and uncharacterized peer populations remain distinct.
  - [x] Implement the [automatic source reviewer and group rule](./design/shared-reference-group-rule.md).
    Follow all exact peers of selected witnesses, batch full-source reads and
    preserve one-to-one decisions. Add a separately versioned complete-group rule;
    no names, memo phrases, amounts or source ordinals select an association.
  - [x] Retain the [final selected-source review](./audit/shared-reference-source-review-2026-09-14.md)
    with the additive rule and confirm unchanged original facts and prior profile
    evidence. All selected groups have full immediate-peer and source coverage.
  - [x] Publish the group rule across the complete compact input with bounded
    streaming membership, old/new disposition conservation and a new policy
    identity. The [2024 gate and varied-layout replay](./audit/shared-conduit-publication-2026-09-14.md)
    pass with every unchanged decision preserved and no new money.
  - [x] Accept the [isolated graph extension](./design/shared-conduit-generation.md)
    and fresh full read-only replay, bound to the unchanged A/B/E generation.
    The real import/replay and retained checksums pass. Fixtures also prove
    corrupted completed edges are rejected without repair.
  - [x] Extend generation-bound path/neighborhood readers to consume the
    shared-conduit extension, with source drilldown and explicit prior/new
    dispositions. Old generation readers keep their pinned coverage. The
    [real query gate and byte-identical fresh replay](./audit/shared-conduit-queries-2026-09-14.md)
    pass with unchanged input checksums and explicit success markers.
  - [x] Extend date-window readers to bind the shared-conduit generation and
    preserve original receipt dates, generation-qualified keys and explicit
    unknown-time states. No terminal rule, money selection or current-pointer
    change is implied. Reuse the source-backed query boundary, not another import.
    The [2024 gate and fresh replay](./audit/shared-conduit-windows-2026-09-14.md)
    pass byte-for-byte; two-cycle fixtures cover unknown dates and cross-source
    joins. Real multi-cycle capacity and acceptance remain separate.
  - [x] Pin the [Go release build environment](./go-build.md) before standardizing
    new executable identities. The [accepted gate](./audit/go-build-2026-09-14.md)
    passes two clean offline builds plus archive-only reconstruction, full Go
    tests and negative checks. Historical binaries keep their original identities;
    complete runtime/data recovery and compiler-image retention remain separate.

- [x] Define the [contributor/reference contract](./design/receipt-participant-publication.md)
  and benchmark bounded sort runs over retained Schedule A facts. The
  [real gate](./audit/receipt-reference-index-2026-09-11.md) passes three-shard
  all-field mapping and replay. This is not a cycle-wide index or graph publication.
- [x] Implement bounded external merge and complete-cycle reference joins.
  Preserve cross-run duplicates, reverse references and unsupported scope;
  do not load the largest report or repeat the bounded reviewer at cycle scale.
  The [2024 gate](./audit/receipt-reference-join-2026-09-11.md) passes full conservation,
  source/artifact verification and layout-varied replay; its audit is retained.
- [x] Accept the real [parallel reference join](./design/receipt-reference-parallelism.md)
  against that baseline. Eight report workers and concurrent artifact assembly
  pass fixture/race gates and a 4.29× synthetic pipeline speedup at the same memory
  cap. The retained real gate passes complete canonical-artifact equivalence and
  source verification: 22m 19s versus 46m 20s. Both used four source readers;
  the older join was serial, not the whole pipeline.
- [x] Profile the two real Parquet source passes before claiming good CPU scaling.
  They took 13m 45s (62% of the initial parallel run) with four readers and one batch dispatcher.
  The [bounded equal-budget profile](./audit/receipt-source-scan-profile-2026-09-12.md)
  separates decode, dispatch/backpressure and I/O costs. Eight readers improve
  sampled ingestion 1.4–1.5×; generic row assembly/copying dominates CPU samples.
  The 2.08× end-to-end gain is not a one-CPU/eight-CPU measurement.
- [x] Permit and validate bounded eight-reader source scans; benchmark true narrow
  column access against the current generic projection, preserving every selected
  value and full source/schema verification. Do not extrapolate sample timing into a
  full-cycle promise. The [implementation and sample gate](./audit/receipt-narrow-reader-2026-09-12.md)
  now pass with all selected values equal. The full-cycle gate and durable retention
  pass: 15m 17s versus 22m 19s, identical four-artifact evidence and complete readback.
- [x] Publish 2024 reported contributor-to-committee and supported conduit associations,
  connected to committee ancestry and candidate authorization. Pass complete
  membership/readback, ambiguity, replay and storage gates without new memo money.
  Full source membership and selected cross-graph witnesses pass; this is not
  every candidate/path or an integrated A/B/E generation.
  - [x] Extract the unchanged role/conflict policy and implement
    [endpoint safety](./design/receipt-reference-topology.md) from retained reference
    artifacts, including invalid incoming references and reciprocal deduplication.
  - [x] Accept and retain the [real topology gate](./audit/receipt-reference-topology-2026-09-12.md):
    full endpoint cross-check, identical layout-varied replay and unchanged
    association decisions for both retained report examples.
  - [x] Publish the [source-grain participant index](./design/receipt-participant-index.md):
    complete 2024 occurrence/disposition conservation, full-fact inspection, bounded
    parallel read/write/readback, real artifact replay and durable retention pass.
    This is an access publication, not Arango participant edges.
  - [x] Implement the [participant/endpoint conduit join](./design/receipt-conduit-publication.md).
    Complete 2024 publication, source membership, independent corpus checks,
    same-build varied-layout replay and durable retention all pass.
    Missing sparse endpoints do not establish transaction-key uniqueness; preserve
    that unassessed state or obtain complete membership before assigning a reason.
  - [x] Publish typed contributor and qualified conduit connections in Arango through
    a bounded streaming importer, not a whole-population in-memory model. Bind exact
    participant/conduit and committee/candidate-authorization ancestry. Pass full
    field/member readback, bounded candidate paths, source drilldown, storage and
    replay gates. This accepts the receipt-observation family, not the entire graph.
    - [x] Implement the [bounded streaming importer](./design/arango-receipt-participants.md),
      exact context binding, field/member readback and source-backed typed paths.
    - [x] Complete the [two-size real gate](./audit/arango-receipt-participants-2026-09-12.md),
      final-source replay, storage measurement and durable retention.
    - [x] Accept a compact full-cycle physical layout and storage budget, then
      complete full-population import/readback and integrate existing committee ancestry.
      - [x] Implement compact appearances with full source reconstruction;
        retain all participant fields and keep edges/context unchanged.
      - [x] Accept and retain the [compact real gate](./audit/arango-receipt-participants-compact-2026-09-12.md):
        full live v1 comparison, worker-varied replay and standalone compact replay.
        The million-row encoded payload is 45.26% smaller; this is not a disk-budget claim.
      - [x] Add [full-cycle storage admission, progress/retry and publication
        controls](./design/arango-receipt-participant-cycle.md). Fixture/race/static
        checks pass; checkpointed prefixes are reverified without rewriting.
      - [x] Accept and retain the [real cycle gate](./audit/arango-receipt-participant-cycle-2026-09-12.md):
        controlled interruption, same-identity resume, complete population,
        immutable completion, exact read-only replay and final storage evidence.
        Complete import, independent publication check, full read-only replay,
        exact equivalence and retained checksums pass. No identity policy was added.
- [ ] Integrate typed receipt, sender, authorization and outside-spending evidence
  under exact generation ancestry. Preserve ledger separation and explicit gaps.
  - [x] Implement the [typed generation binding](./design/funding-evidence-generation.md),
    exact outside-source membership, candidate/committee reference proofs and
    read-only complete resolved outside-graph field verification. Tests/race/vet pass.
  - [x] Accept and retain the [real generation gate](./audit/funding-evidence-generation-2026-09-13.md)
    and byte-identical fresh replay. Required families, source compatibility,
    complete selected A/B/E fields and all completion rechecks pass without import.
  - [x] Implement the [generation-bound neighborhood reader](./design/funding-neighborhoods.md)
    and automatic source-backed family query gate. Preserve missing endpoint
    facets, source-backed identity states and scoped pagination; tests/race/vet pass.
  - [x] Accept and retain its [real gate and fresh replay](./audit/funding-neighborhoods-2026-09-13.md).
    All eight family witnesses, cross-family endpoint cases, missing-master case,
    six continuations, exact replay identity and byte comparison pass.
    A one-hop reader is not an integrated serving API or all-candidate path gate.
  - [x] Implement [generation-bound typed multi-hop paths](./design/funding-paths.md),
    explicit traversal bounds, incomplete results and exact evidence; no path-money sum.
    Full regression/race/static checks and independent small-graph enumeration pass.
  - [x] Accept and retain the [real path gate and fresh replay](./audit/funding-paths-2026-09-13.md).
    Both ledgers and all three candidate endings, receipt/conduit entries,
    hop/work boundaries, exact replay identity and byte comparison pass.
  - [ ] Add individual source-member drilldown for resolved outside-spending groups
    and accept serving performance separately from backing verification.
  - [x] Implement the [read-only receipt-to-candidate consumer](./design/receipt-candidate-connection.md),
    binding exact shared facts and authorization to existing graph families.
    Cross-cycle, failure, readback, regression and race fixtures pass; no terminal policy.
  - [x] Implement automatic connection validation: exact pinned graph inputs,
    complete participant/conduit census, data-selected witnesses, source-backed
    replay and a deterministic machine-readable proof. No manual case selection.
  - [x] Implement explicit [reference-content equivalence](./design/reference-content-equivalence.md) for unchanged
    selected CM/CCL members in differently packaged archives. Preserve both
    occurrence/fact ancestries; recheck physical bytes, schemas and all fields;
    reject actual drift. Full fixtures/race/static checks and the real CM/CCL
    proofs pass. The [complete connection gate](./audit/reference-content-equivalence-2026-09-13.md)
    and byte-identical whole-command replay pass with no graph rebuild.
  - [x] After full receipt publication/replay acceptance, retain real direct,
    upstream, conduit and missing-master connection witnesses, exact replay and cost.
    The full census found no unresolved-recipient syntax and records that absence
    explicitly. All present case witnesses and fresh replay pass. This consumer
    is not an accepted population-wide integrated A/B/E generation or serving API.
- [x] Implement the [terminal-source boundary assessment](./design/terminal-source-assessment.md):
  full selected A/B topology, provisional rule comparisons, missing identity,
  ledger absence and root cyclic components without terminal eligibility or money.
  Regression/race/static checks and the [live gate](./audit/terminal-source-assessment-2026-09-13.md)
  pass, including complete selected populations and byte-identical fresh replay.
- [x] Implement the [reported receipt-role join](./design/terminal-receipt-roles.md)
  over the full compact index, including exact source-ID master evidence and
  complete per-recipient populations. Tests/race/static checks and the
  [live gate](./audit/terminal-receipt-roles-2026-09-13.md) pass, including full
  participant-corpus and byte-identical worker-varied replay.
  Person/corporation resolution and terminal allocation remain unperformed.
- [x] Clarify [cycle partitions versus calculation windows](./design/cycle-calculation-windows.md)
  in PD-002 and the work plan. Preserve detailed inputs and explicit cross-cycle
  dependencies; this is not implemented general multi-cycle analytics.
- [x] Complete the [pre-attribution interpretation review](./design/pre-attribution-review.md)
  with the user before choosing terminal definitions or allocation. Identify the
  reviewed policies and existing evidence pins; a complete recovery baseline is
  separate deferred work, not a prerequisite. A graph is not raw evidence.
  - [x] Prepare the [first code-backed review packet](./audit/pre-attribution-review-2026-09-14.md),
    characterize candidate-resolution precedence in tests, and freshly verify
    the [selected evidence pins](./audit/pre-attribution-checkpoint-2026-09-14.md).
    No runtime interpretation changed; this is not user acceptance.
  - [x] Refresh the [edge-rule map](./design/pre-attribution-review.md#what-actually-creates-a-connection)
    from current code, including shared conduits, separate A/B observations,
    unresolved authorization and routable unverified candidate IDs. No policy changed.
  - [x] Review [candidate-ID precedence and its graph consequences](./design/pre-attribution-review.md#candidate-id-resolution-review--2026-09-15):
    existing-ID replacement, context collisions, unverified routing and separate
    amount categories. The user accepted the four-part boundary on 2026-09-20;
    immutable replacement views now pass the complete 2024 gate.
- [x] Implement the [reported identity assertion view](./design/reported-identity-assertions.md)
  over complete published A/CM facts, preserving field values and occurrence/fact
  grain without another text corpus copy. The [live gate](./audit/reported-identity-assertions-2026-09-13.md)
  passes complete 2024 scan, byte-identical replay and sampled full-width reader
  comparison; regression/race/static checks also pass.
- [ ] Add separately evidenced person/organization resolution over reported
  assertions. Unknown identity must not prevent source-backed connections;
  employee associations are not corporate payments. Keep the user-requested
  interpretation review ahead of any terminal-policy or allocation decision.
- [ ] When a consumer needs cross-cycle evidence, add its explicit input/output
  window and multi-cycle tests before real rollout. Cover overlap, time-specific
  identities, non-additive composition and missing earlier context as applicable;
  do not remove the existing single-cycle readers' compatibility checks.
  - [x] Run the [bounded composition test](./design/cycle-calculation-windows.md#bounded-composition-test)
    before extending the application contract. Stored models and existing path
    search preserve cross-partition topology, source grain and historical facets;
    package/race/static checks pass. That experiment's date selection is test scaffolding, not a
    production multi-publication loader or temporal query API.
  - [x] Implement the first [verified multi-publication consumer](./design/funding-window-reader.md)
    for selected A/B committee paths, with source-qualified evidence routing,
    reported-date windows and historical facets. Two-cycle fixtures cover unknown
    time, overlap rejection and query boundaries without terminal classification.
  - [x] Accept and retain its [real 2024 read-only gate and fresh-process replay](./audit/funding-window-2026-09-13.md).
    Complete A/B date-population checks, six automatic query cases, byte-identical
    replay and full regression/race/static checks pass. This is not real two-cycle acceptance.
  - [x] Accept the [full-chain synthetic loader/CLI integration test](./design/funding-window-reader.md#verification-and-next-boundary)
    in disposable ArangoDB, including input-order replay and corrupted second-publication
    source, graph and completion boundaries. Race, full regression and static checks
    pass; real capacity acceptance remains separate.
  - [x] Extend explicit source routing to receipt entries and candidate authorization
    connections. Qualify receipt locators and all returned links by generation;
    filter the underlying receipt date without inventing authorization validity.
    Full-chain synthetic loader/CLI, regression, race and static checks pass.
  - [x] Retain the [automatic 2024 window connection gate](./audit/funding-window-connections-2026-09-13.md).
    All receipt/conduit/candidate cases, independent complete selected-ledger date
    counts, source routing and byte-identical fresh-process replay pass, with
    full regression/race/static checks. This does not qualify a second real cycle.
  - [x] Add [date-selected Schedule E source-member endings](./design/funding-window-reader.md#source-grain-schedule-e-connections).
    Replay existing source decisions and conserve every parent aggregate. Require
    an explicit native date field, preserve exception coverage and pass the
    synthetic full-chain loader/CLI gate without changing existing graph totals.
  - [x] Add and run the [retained 2024 Schedule E window gate](./audit/funding-window-spending-2026-09-14.md):
    complete independent source/date census, automatic direct/upstream/receipt
    witnesses, byte-identical fresh-process replay, and runtime/memory measurements.
    All cases, regression checks, artifact checks and success markers pass.
  - [ ] Before interactive serving, profile the measured per-query Schedule E
    verification/source-read cost. Preserve exact inputs, decisions, coverage and
    fresh-replay equivalence; the acceptance reader is not a low-latency API.
  - [ ] Preserve native reporting intervals for later sources without cycle-derived validity.
  - [ ] Validate a second real cycle before claiming real cross-cycle integration.
- [ ] Complete detailed A/B fact, calculation and graph gates for the remaining
  target cycles, then verify the default latest-four-cycle view. This is rollout
  acceptance, not a prerequisite for completing 2024 evidence/identity work;
  calculations requiring earlier real inputs remain gated on those inputs.
- [ ] Verify coordinated refresh, dependency invalidation, retry and retention
  before unattended weekly publication. Keep Python confined to orchestration.
- [ ] Performance follow-up, not a graph-publication blocker: benchmark overlapping
  independent source batches if ingestion latency needs further reduction. The
  narrow-reader profile now exposes consumer acknowledgment waits; preserve owned
  buffers, bounded queues, cancellation and complete-result equivalence.

Terminal and allocation policies remain separate. GUI work is deferred behind
these backend milestones; report/header investigations gate their specific
financial uses, not all graph connectivity.

### Deferred recovery engineering and scoped cleanup

Not a prerequisite for the node/edge review. Do not execute this queue by default.
The [2026-09-16 scope clarification](./design/relationship-exploration.md#development-discipline-and-cleanup-status)
also excludes recovery and exact-build campaigns from the active relationship work.
Use ordinary development builds/tests; retain source provenance and dependency locks.

- [x] Retain the read-only dependency inventory, metadata planner, scoped file
  verifier and historical-stage review. Their measured results and limits remain
  linked from the [recovery design](./design/funding-recovery-checkpoint.md).
- [x] Remove the unused [input-copying feature and its fixture harness](./design/funding-recovery-retention.md)
  after user confirmation. All nine removed files matched the retained source
  archive. CLI/build bindings and copy-only helper indirection are removed;
  historical audits, existing data and read-only tools remain. Full Go tests pass.
- [ ] When operational recovery is explicitly resumed, scope runtime/input
  protection, fixture reconstruction, a resource-bounded real rebuild and exact
  comparisons together. Reconsider the design against that concrete requirement.
  Raw-to-fact recovery remains separate; the missing historical staging bytes
  cannot be replaced by descriptor equivalence or a new execution record.

### Completed candidate evidence consumers

- [x] Build the [integrated candidate evidence command](./design/candidate-evidence-view.md):
  one result with receipt populations, unchanged upstream membership/accounting,
  source-backed connection witnesses, optional summary context and a readable report.
  Pin executable and input identities; do not choose terminal or allocation rules.
- [x] Improve the integrated report with pinned source names, deterministic
  complete witness examples, separate memo tables and readable gap explanations.
  Preserve the nested v1 evidence and unselected allocation policies; see the
  [report gate](./audit/candidate-report-2026-09-11.md).
- [x] Connect a named report's concrete observation to its full retained source
  row through `inspect-candidate-connection`, with expected report identity, exact
  source/observation checks and no financial-use promotion. See the
  [drilldown gate](./audit/candidate-connection-2026-09-11.md).

These commands inspect existing populations; they did not publish contributor,
employer or corporate graph connections. Use them to verify the milestones above.

### Completed foundations and scoped follow-ups

- [x] Implement the [candidate upstream evidence calculation](./design/candidate-upstream.md):
  exact receiver observations, same-cycle authorization, external/internal/
  unresolved-scope accounting, complete selected ancestry, shortest-hop source
  witnesses, cycles, and terminal-ineligible coverage states. The real 2024
  gate passes for two data-selected candidates. This is not terminal attribution.
- [x] Implement the [reported-receipt inventory](./design/committee-funding-basis.md)
  from existing detailed Schedule A facts, with disjoint conserving components,
  contributor/conduit/employer source lookup, and exact-source upstream
  committee assessments. The complete 2024 gate reproduces both accepted
  receipt cohorts. This is not a complete funding denominator.
- [x] Review the complete individual/committee overlap population and bounded
  original-filing earmark evidence. Implement the additive
  [source-role policy](./design/receipt-source-evidence.md): one reported committee
  observation per overlap, retained entity conflicts, and explicit conduit and
  report-reference uncertainty. Existing facts and monetary predicates stay unchanged.
- [x] Implement a bounded [same-report earmark reviewer](./design/receipt-report-association.md):
  exhaustive selected-cycle report membership, role-qualified related rows,
  duplicate/conflicting reference handling, and exact raw/clean memo IDs.
  Both complete original-file gates pass. Registration remains unverified;
  absent links and amount differences stay explicit, with no added money.
- [x] Implement the [funding coverage audit](./design/funding-coverage-and-time.md)
  over preserved inputs before selecting another bulk source. Establish the
  exact summary populations and missing per-committee/per-report fields;
  never fill unitemized receipts or opening balances from a reconciliation residual.
  Both valid-fact summary populations pass complete independent comparison and
  replay; original source exclusions and post-cycle dates remain visible.
- [x] Review official committee-level financial-summary bulk products against
  the audited requirements: committee/account/report scope, coverage interval,
  opening balance, explicit unitemized receipts, and revision identity. Select
  a source contract before wiring another input. The
  [four-cycle review](./audit/committee-summary-source-2026-09-08.md) selects the
  modern committee-summary CSV with a draft lossless schema and exact fixtures.
- [x] Implement a strict Go committee-summary reader/verifier: exact CSV header,
  all raw fields, field-level typed issues, composite/committee multiplicity,
  complete four-cycle conservation, and replay. Do not sum candidate-reference
  fan-out, repair summary equations, or publish this draft through release v3.
  The [complete reader gate](./audit/committee-summary-reader-2026-09-08.md) passes
  all four cycles with independent every-field/typed-value hashes and exact replay.
- [x] Implement immutable committee-summary occurrence/fact publication and
  opt-in release v4 without mutating v1–v3. Complete four-cycle artifact readback,
  independent stored-value comparison, replay, and fixture release integration
  pass the [publication gate](./audit/committee-summary-publication-2026-09-08.md).
- [x] Inspect real v4 source metadata and refresh cost without acquisition. Two
  observations select the same 129.57 GB candidate. The
  [refresh-plan audit](./audit/fec-v4-refresh-plan-2026-09-09.md) confirms hard-link
  double counting and separate staging-headroom risk; no source body was fetched.
- [x] Correct/test inode-aware hot-storage accounting and implement the read-only
  Go [storage review](./audit/fec-storage-review-2026-09-09.md). Its original
  full-reserve scenario exceeded the unchanged hot cap by 34.12 GB; the following
  streaming gate supersedes that model. Cleanup found no large disposable
  source-stage files. No download, deletion, or publication ran.
- [x] Implement and validate the [bounded streaming-workspace budget](./design/fec-streaming-storage.md):
  cumulative retained/temporary bytes, shared writer exclusion, runtime growth
  limits, failure cleanup, and checkpoint replay. The
  [gate](./audit/fec-streaming-storage-2026-09-09.md) passes complete Go tests,
  targeted races, independent review checks, and a small overhead benchmark.
  The saved-plan scenario fits without an unmaterialized uncompressed reserve.
  Keep the 600 GiB cap, 500 GiB floor, 25 GiB margin, and retained evidence.
  Cold retention is still required before perpetual refresh automation.
- [x] Run [fresh v4 discovery and cost review](./audit/fec-v4-preflight-2026-09-09.md)
  after the streaming gate. Both complete HEAD passes select the same 129.57 GB
  candidate; independent checks and the storage scenario pass. No source was
  captured by that review and active/default v3 is unchanged. Acquisition was
  subsequently approved and completed as recorded below.
- [x] Verify the [v4 acquisition](./audit/fec-v4-acquisition-2026-09-09.md)
  from its three durable exit markers and result. All markers are zero;
  independent verification passes with 24 acquired artifacts, three reused,
  matching post-capture publisher versions, and unchanged v3. No activation or
  staging ran in that acquisition. Do not start a duplicate capture.
- [x] Verify the [v4 stage](./audit/fec-v4-staging-2026-09-09.md) from its three
  durable exit markers and exact result. All markers are zero; all 25 selected
  outputs, five blocking checks, and independent verification pass. V3 is
  unchanged. Use this exact completed stage for the next publication gate;
  do not repeat extraction.
- [ ] Make the read-only storage review distinguish completed acquisition from
  remaining downloads. Its current pre-acquisition scenario conservatively
  counts completed CAS captures again. The current stage still fits without
  relaxing limits; preserve the exact approved plan and artifact identities.
- [x] Gate the [real coordinated v4 release](./audit/fec-v4-publication-2026-09-10.md)
  from the exact observed/acquired/staged inputs. All eight release checks,
  source publication replay, four summary publications/replays, and independent
  every-record comparisons pass. V4 is active; v3 remains immutable. Accept the
  summary source only for lossless occurrence/fact preservation, not financial grouping.
- [x] Wire the manual-only Dagster committee-summary asset with exact release/cycle
  handoff and partition-scoped blocking checks. The
  [isolated four-cycle gate](./audit/committee-summary-dagster-2026-09-10.md) passes
  against read-only published storage, with identical replay and no automatic trigger.
- [ ] Migrate default discovery from v3 to v4 as a separate operational change.
  Do not automate against the stale inventory or rebrand earlier A/B/E facts as
  v4 inputs. Weekly acquisition requires accepted cold retention. No further
  download or extraction is needed for this completed release; prioritize
  report/account scope review before expanding automation.
- [x] Implement [exact-evidence summary assertion grouping](./design/committee-summary-assertions.md):
  all non-candidate fields must match, every occurrence remains a member, and
  conflicts retain all variants without financial selection. The full four-cycle
  Go gate passes with no source/fact changes.
- [x] Complete the [independent grouping/arithmetic gate](./audit/summary-assertions-2026-09-10.md):
  all four cycles conserve raw memberships and exact diagnostics, and the
  official-summary check confirms a large discrepancy predates parsing.
- [x] Complete a [bounded report-level investigation](./audit/summary-report-review-2026-09-10.md):
  three selected 2024 witnesses isolate filer cash discontinuity, attachment-level
  amendment selection, and a paper-transcription mismatch. Retain exact source
  evidence and Go regression checks; no automatic corrections or corpus-wide
  classification is implied.
- [x] Define and implement [summary/receipt comparison readiness](./design/summary-receipt-compatibility.md).
  Nine reported fields, exact recipient cohorts, and source/scope blockers pass
  the real five-case 2024 gate and independent checks. All comparison deltas stay
  null; no funding or terminal eligibility is promoted.
- [x] Implement the [same-release report-occurrence profile](./design/receipt-report-profile.md):
  select staged Schedule A from the verified summary release, scan it once with
  full row/byte/hash validation, and preserve form-line decisions, included
  report groups, and receipt-date states. Old facts are not relabeled; occurrence
  measures are not accepted unique/effective financial results.
- [x] Implement the [bounded report-line review](./design/receipt-report-lines.md):
  complete selected report membership, independent memo/individual axes, exact
  source locators and signed measures, with no named runtime exceptions. Seven
  complete original Form 3 comparisons pass against their own cover subtotals;
  memo dates outside report periods remain evidence. These use older accepted facts.
- [x] Extend the source-aligned cycle profile with complete per-report form-line
  populations and independent memo/individual axes. The
  [v2 complete 2024 gate](./audit/receipt-report-profile-v2-2026-09-10.md) passes
  one bounded source scan, exact conservation, and full regrouping to v1. V1 stays
  available; no raw facts, financial predicates, or graph observations change.
- [x] Review the v2 profile's unresolved reviewed-line memo codes and amounts
  against source evidence. The [bounded original review](./audit/receipt-memo-review-2026-09-10.md)
  locates all 62 `Y` rows in paper transcriptions and both missing processed
  amounts as original one-cent entries. Complete selected original membership
  and grouped v4 equivalence pass. Raw values and eligibility remain unchanged;
  review completion is not accepted memo interpretation or source correction.
- [x] Complete bounded [report-metadata qualification](./audit/receipt-report-metadata-2026-09-10.md).
  Bulk period-summary headers lack file IDs and amendment chains. Processed
  OpenFEC metadata matches retained original witnesses but preserves endpoint
  status/type disagreements. Seven independent checks pass; no recurring source
  or report selector was added. The official P3.4 memo schema is now pinned.
- [x] Accept a separate processed metadata source alongside bulk-only transaction
  ingestion and implement the [Go capture-descriptor reader](./design/report-metadata-reader.md).
  Pinned page/header identity, strict JSON and endpoint shapes, all-row retention,
  scoped pagination evidence, and conflict-preserving replay pass against the
  three retained endpoints. This performs no HTTP or financial selection.
- [x] Implement [bounded Go report-metadata HTTP capture](./design/report-metadata-capture.md)
  with header-only authentication, explicit budgets, failed-attempt evidence,
  guarded retries, and reader-consumable checkpoints. Offline completion/failure
  tests and seven real-record readback pass. The
  [live audit](./audit/report-metadata-capture-2026-09-10.md) records an HTTP/2
  representation fix and an explicitly incomplete demo-rate-limited traversal.
- [ ] Finish the small live metadata traversal after API quota is available.
  Do not repeat transaction scans or fetch a wider history to test transport.
  Keep the failed attempts; full completion requires a validated empty page.
- [x] Define the [summary-value use policy](./design/summary-value-use.md): reported
  observations and diagnostic arithmetic remain usable; scoped comparison and
  funding acceptance are separate. Test field-local conflicts and unchanged
  receipt evidence against synthetic inputs and pinned real cases. Do not turn
  summary/metadata completeness into a dependency of the bulk-based graph.
- [x] Implement the [bounded report-scope assessment](./design/report-scope-assessment.md):
  exact paper cover/period fields, blank/zero separation, supplemental shape,
  unresolved layouts, and preserved metadata assertions. Four retained cases,
  independent workbook checks, full Go gates, and CLI replay pass. No financial
  replacement is selected and no processed transaction is changed.
- [x] Qualify [Form 3X report-period total receipts](./design/report-total-receipts-comparison.md)
  for numeric same-file reported-value comparisons. The retained gate yields
  one qualified pair and six blocked pairs; field-local conflicts, no selected
  financial report, and all financial/cycle/terminal guards are tested.
- [x] Implement [explicit unitemized-receipt observations](./design/report-unitemized-receipts.md)
  with distinct raw states, own report periods, subtotal diagnostics, and scoped
  same-file pairs. Four retained cases and independent checks pass; prior
  total-receipts outputs remain byte-identical. No financial eligibility changes.
- [x] Implement [observed report membership and date coverage](./design/report-period-membership.md):
  exact source-chain candidates, explicit unresolved cohorts, and requested-window
  gaps/overlaps/boundary crossings. Four retained runs and independent checks pass;
  structural readiness remains separate from financial membership and cycle totals.
- [x] Bind observed-chain candidates to [qualified electronic period fields](./design/report-field-binding.md).
  The pinned 8.4 F3/F3X reader, seven retained cases, 35 exact field bindings,
  independent workbook/amount checks, and prior-command equivalence pass.
  Superseded, prefix, and mixed-origin cases remain blocked; cash eligibility stays false.
- [x] Implement [reported-window membership and aggregation](./design/report-window.md)
  with exact bound-candidate field coverage, independent sums, boundary cash stocks,
  and separate subtotal/carry-forward diagnostics. Four retained cases and independent
  checks pass; 19 prior diagnostic results remain identical.
- [x] Implement [summary/window reported comparisons](./design/summary-report-window.md).
  Reverify both inputs, retain independent snapshot identities and all assertion
  variants, and gate exact differences by field-specific time/scope. The
  [four-case gate](./audit/summary-report-window-2026-09-10.md) qualifies one real
  closing-stock pair without erasing cash discrepancies or filling cycle prefixes.
- [x] Complete [early-cycle source investigation and v2 reported comparisons](./audit/cycle-prefix-2026-09-10.md).
  Registration and cumulative columns do not prove inactivity. Compare exact
  declared reported spans without requiring an invented cycle prefix; retain
  unknown outside dates, internal gaps, stock boundaries, and identical v1 replay.
- [x] Compare the source-aligned reviewed itemized line over the
  [exact reported window](./design/receipt-reported-window.md). The pinned profile
  passes full group/content revalidation; the retained line/report/summary
  comparison, empty-report corroboration, blocked alternatives, original checks,
  and replay pass without another bulk scan. Financial guards stay false.
- [x] Review the remaining F3/F3X receipt-line families in the
  [source map](./design/receipt-families.md). Official field labels, disjoint
  subtotal equations, complete profile routing and bounded originals pass.
  Distinguish thresholded detail, explicit cover observations, loans, transfers
  and other-account schedules; do not make this a runtime cash policy.
- [x] Extend Go's report-field comparison additively for committee contributions,
  transfers and loan receipts. The [bounded family comparison](./design/receipt-family-comparison.md)
  binds exact form/report/period fields and compares source-aligned occurrences.
  Independent source checks and byte-identical prior replay pass; absent detail
  stays null, including zero covers. No new family window or cash use is accepted.
- [x] Add a [complete F3X witness](./audit/receipt-family-witnesses-2026-09-11.md)
  with positive other-committee contributions and affiliated/party transfers.
  Retained metadata/profile plus one bounded original capture pass all-row
  grouped checks and unchanged Go replay; memo dates and signed amounts remain.
- [x] Add [positive witnesses for the remaining initial family/form shapes](./audit/positive-receipt-families-2026-09-11.md).
  F3 transfers/other loans and F3/F3X party/loan fields pass original/profile
  checks. Existing processed nulls and unreviewed debt records remain explicit;
  the application binary is unchanged. This is not population-wide financial
  acceptance or new report-metadata binding.
- [x] Extend report-level evidence comparisons to remaining mapped F3/F3X receipt
  categories: candidate contributions, loan repayments, offsets, refunds and other
  receipts. Preserve required-itemization versus thresholded/cover-only scope;
  never label a cover-minus-detail residual unitemized or use it as inferred cash.
  The [v2 gate](./audit/receipt-families-v2-2026-09-11.md) uses shared form/field
  rules, preserves a real thresholded component and passes exact v1 replay.
- [x] Add bounded positive original/profile/metadata witnesses for newly mapped
  categories not covered by the retained v2 reports. Reuse existing evidence and
  source-profile selection; do not make entity IDs into runtime policy.
  The [real gate](./audit/positive-receipt-families-v2-2026-09-11.md) covers every
  new shape and preserves three blocked bindings; it does not establish universal
  report qualification or financial membership.
- [ ] Review original-report header sequence 7 (`0` versus blank) as a general
  source rule. Three new retained cases have matching cover/metadata, `N` forms,
  blank prior-report IDs and singleton metadata chains but the current binder
  rejects zero numbering. Check authoritative semantics and counterexamples;
  do not add file, committee or software-name exemptions or erase raw values.
- [ ] Extend new-family reported-window/summary qualification only with explicit
  total-versus-detail scope. Keep thresholded components separate, preserve date
  gaps and missing detail, and independently review absent-population rules.
- [ ] Reconcile unused copied `publisher_name` labels in the draft original
  Schedule A layout with the pinned workbook before claiming complete label
  transcription. The first street-address label has a whitespace difference;
  consumed amount/identity/date/memo positions pass and source bytes are unchanged.
- [x] Qualify [family-specific absent-population evidence](./design/receipt-family-absence.md).
  Exact bound zero plus complete original/profile line inventories produces a
  separate reported-zero observation. Memo-only, partial, superseded and conflicting
  cases stay blocked; retained-source gates and old-command exact replay pass.
  Old detail stays null. No bulk rescan or financial eligibility change.
- [x] Add [family-specific date-window comparisons](./design/receipt-family-window.md).
  Reported and comparison operands have separate exact day coverage and sums;
  missing covers, unknown dates and qualified reported zeroes stay distinct.
  Retained spans, blocked alternatives, source/day checks and prior replay pass.
  No new family summary differences or financial eligibility follow.
- [x] Qualify exact [family-to-summary field and date mappings](./design/receipt-family-summary.md)
  and add separate reported/detail comparisons. The retained gate, raw-summary
  checks and prior replay pass. Assertion identities, variants and blank values
  remain; no fan-out sum, cross-form loan fallback, gap fill or financial promotion.
- [ ] Qualify F3P receipt/report fields and complete bounded original evidence,
  including processed `17A` and federal funding. F3P, F4, F9 and unrecognized
  references stay preserved outside the current F3/F3X map, not silently excluded
  from source storage or remapped by stripping line suffixes.
- [ ] Establish source-backed full-cycle financial coverage where that use requires
  it. The late first-report prefix remains unknown, but does not block narrower
  reported comparisons. Accept financial report/account scope before financial
  cycle totals or cash use. Never infer unitemized funds from a detail gap. Old-report refresh,
  full population, cost, freshness, immutable metadata publication, and recurring
  activation remain separate gates.
- [ ] Accept source-qualified memo interpretation and an additive field-correction
  contract only where evidence justifies them. Preserve raw `Y`, source nulls,
  checked-box/cover conflicts, exact ancestry, and unresolved states; no named
  runtime exceptions or blanket one-cent filling.
- [ ] Qualify cumulative field time bases before adding a consumer: candidate
  Form 3 election-cycle-to-date is not PAC/party calendar-year-to-date or the
  bulk two-calendar-year partition. Review `_ytd` meanings by source/form rather
  than inferring them from field names. Existing raw fields stay unchanged.
- [ ] Qualify financial use beyond the accepted narrow itemized-individual
  occurrence comparison: establish exact unique/effective membership and
  reporting-period/account coverage for the source-aligned form-line population.
  Do not repeat the single-committee reviewer per committee or infer unitemized
  amounts from a detail gap. Effective-report
  selection and source corrections need separate contracts, including attachment
  versus financial replacement and paper/electronic provenance.
- [ ] Define compact immutable publication for accepted summary calculations
  before orchestration or financial consumers. The current verbose JSON is a
  diagnostic artifact, not the accepted recurring storage format. The v2 audit
  records substantial full-JSON-schema validation cost and a bounded flat-key
  benchmark; accept cheaper complete validation before recurring automation.
- [ ] Review the pre-existing source-invalid summary occurrences separately
  (30 `weball`, 9 `webl` in the pinned 2024 input). Keep the populations separate
  and preserve issue evidence; this audit did not create those exclusions.
- [ ] Accept complete funding-basis coverage and time semantics: unitemized
  receipts, other receipt families, opening balances, prior-cycle money, in-kind
  amounts, and negative adjustments. Keep identity resolution separate.
- [ ] Extend conduit evidence after the funding gate: cycle-wide bounded-memory
  reference indexing/publication, cross-cycle report completeness, registration
  verification, more filer/role shapes, and a complete original-file gate for
  original-to-memo references. Do not run the bounded report reviewer per
  candidate or raise its in-memory limit into a production strategy.
- [ ] Accept and test direct, earmarked, and modeled pooled-fund allocation
  with cycle handling and exclusive unresolved amounts. Do not normalize an
  incomplete committee-only graph into a fully explained donor distribution.
- [ ] Add publication and orchestration for the accepted attribution consumer;
  the current candidate upstream command remains a read-only diagnostic.

### Implemented boundaries and remaining work

- [x] Implement exact 21-source FEC discovery and release planning.
- [x] Implement resumable, storage-gated acquisition with post-capture version
  verification, container checks, immutable storage, and Dagster sensor wiring.
- [x] Extract the four selected Schedule A relations into verified zstd
  artifacts and stage the other coordinated source products.
- [x] Run release-level checks and atomically publish `fec.release.v1` without
  letting acquisition replace the active release pointer.
- [x] Publish immutable Schedule A occurrences and issues before normalized
  facts, then emit natural-key and semantic-digest change sets.
- [x] Publish immutable occurrences, issues, natural-key changes, and lossless
  normalized source facts for `cn`, `cm`, `ccl`, `weball`, and `webl` across
  the coordinated release's `dataset` and `cycle` multi-partitions.
- [x] Normalize immutable Schedule A record versions into typed
  `fec.schedule_a_receipt.v1` facts without applying counting or entity-
  resolution policy.
- [x] Implement the versioned candidate itemized-individual receipt
  calculation: one decision per Schedule A fact, same-cycle `A`/`P` committee
  scope, checked signed subtotals, separate `weball`/`webl` evidence, and
  date-bounded `TTL_INDIV_CONTRIB` reconciliation. Publish immutable decision
  and result artifacts through a manual Dagster cycle asset.
- [x] Publish the first coordinated FEC release, all five 2024 classic fact
  families, and 2024 Schedule A occurrence evidence. Run the candidate
  itemized-individual calculation directly over the staged relation and record
  runtime, storage, decision, route, candidate, and summary-gap evidence in the
  [direct probe audit](./audit/direct-receipt-probe-2026-08-30.md).
- [x] Compare equal-row zstd COPY and a lossless Parquet candidate over one
  million and ten million real 2024 Schedule A rows. The candidate passed
  semantic, decision, size, throughput, column-pruning, and memory gates. See
  the [physical-layout benchmark](./audit/schedule-a-layout-benchmark-2026-08-30.md).
- [x] Replace the rejected Schedule A JSON fact artifact with one partitioned,
  lossless, columnar representation. The complete 2024 gate published all
  264,085,606 facts as 265 content-addressed 99-column Parquet shards, passed
  full semantic readback and an independent DuckDB scan, and consumed 16.76
  GB. See the
  [columnar publication audit](./audit/schedule-a-columnar-publication-2026-08-31.md).
- [ ] Optimize the columnar publisher. Benchmark
  bounded overlap or parallelism for full-shard semantic readback while
  retaining deterministic checkpoints, exact SHA and semantic verification,
  the 2 GiB Go memory target, and current failure semantics. The first complete
  run achieved 38,646 rows/s versus 117,848 rows/s in the bounded write-only
  benchmark; do not weaken verification to close the gap. Weekly automation
  is now safe and enabled because changed-cycle publication is resumable and
  bounded; this remains throughput debt, not a correctness gate.
- [x] Replace full-row occurrence, natural-index, and bootstrap-change JSON
  persistence with compact manifest, fixed-width partitioned key-index,
  sparse-exception, and actual inter-release delta artifacts. The complete
  2024 gate preserved 264,085,606 valid unique rows in 10.277 GB, reduced the
  113.448 GB legacy footprint by 90.94%, and passed a complete backing rehash.
  The Parquet publisher now consumes the compact contract and adopts already-
  verified shards when clean source membership is unchanged. The real 2024
  migration rebound all 265 existing shards to compact ancestry in 39.95s
  without rewriting fact files. See the
  [compact occurrence publication audit](./audit/schedule-a-compact-occurrence-publication-2026-08-31.md).
- [x] Replace one JSON receipt decision per Schedule A row with a versioned
  predicate plus exact input-index identity, compact exception membership, and
  drilldown over the canonical columnar facts. Prove equivalence against the
  complete direct-probe result. The 2024 publication reproduced all decision,
  route, candidate, amount, reconciliation, and result-artifact identities
  while materializing only two exceptions. See the
  [compact calculation audit](./audit/compact-receipt-calculation-publication-2026-08-31.md).
- [ ] Remove the unpublished 33.68 GB interrupted Schedule A fact staging
  artifact after explicit cleanup authorization.
- [x] Add a coordinated fact-bundle readiness mapping before automating the
  calculation. The immutable bundle joins the `cycle` Schedule A partition to
  exactly three required classic `dataset`/`cycle` multi-partitions, rejects
  mixed releases, verifies all backing, and drives the compact calculation
  through eager Dagster automation. The real 2024 readiness, idempotence, and
  calculation-replay gates passed. See the
  [fact-bundle audit](./audit/candidate-receipt-fact-bundle-2026-08-31.md).
- [x] Use the accepted real-cycle results to run the ArangoDB probe for query-
  bearing entity, monetary, and path projections. The isolated 2024 graph
  conserved 31,035 entities, 8,175 results, 8,584 relationship edges, and
  2,116 receipt-component edges; idempotent replay reused the same content-
  addressed database. Raw receipt rows remained in Parquet. See the
  [projection audit](./audit/arango-candidate-receipt-projection-2026-08-31.md).
- [x] Explain the 143 candidate and 156 committee IDs used by the 2024 receipt
  calculation but absent from same-cycle master facts. The exact audit found
  298 zero-dollar relationship/summary assertions and one two-row −$8.4M
  adjustment component. A different 2024 snapshot resolved none; active-cycle
  history resolved one zero-dollar committee only. Keep explicit placeholders
  and separate assertions. See the
  [master-gap audit](./audit/receipt-master-gaps-2026-08-31.md).
- [x] Measure and accept the first receiver-reported committee-flow boundary
  over complete 2024 Schedule A. The role-aware rule retains 320,731 rows,
  180,283 source-recipient-role groups, and $4,672,820,179.49 while keeping
  outbound, memo, earmarked, noncommittee, unknown-role, and uncertain-ID
  populations explicit. The executable Go policy and canonical fixture are
  versioned under `fec/receiver-reported-committee-flows@1.0.0`. See the
  [cohort audit](./audit/receiver-reported-committee-flow-cohort-2026-08-31.md).
- [x] Publish the accepted receiver-reported committee-flow calculation as an
  immutable compact predicate, sparse exceptions, grouped results, and exact
  conservation manifest. The CLI, verified loader, and minimal eager Dagster
  asset are implemented. The complete 2024 run and unchanged replay passed.
  See the [publication audit](./audit/receiver-reported-committee-flow-publication-2026-09-01.md).
- [x] Publish the receiver-flow projection-readiness bundle with exact same-
  cycle committee-master lineage, then add the content-addressed ArangoDB
  projection and benchmark multi-hop paths and cycles before terminal-source
  tracing. The 2024 graph conserved 180,283 edges and $4,672,820,179.49,
  measured 35 cyclic strong components, and passed bounded neighborhood,
  ranked-path, shortest-path, cycle, replay, and Dagster gates. See the
  [graph audit](./audit/arango-receiver-reported-committee-flows-2026-09-01.md).
- [x] Audit the 707 committee IDs represented by `missing_master_fact`
  vertices in the 2024 receiver-flow graph. Compare same-release linkage and
  summary facts, other cycles, and official committee history before terminal-
  source classification. Official 1980–2026 cycle masters confirm 675 as
  historical registrations; 32 remain unmatched reported IDs backed by 50
  receipts and $173,821.08. All 707 are source-only. See the
  [master-gap audit](./audit/receiver-flow-master-gaps-2026-09-01.md).
- [x] Add distinct historical-registration and unresolved-reported-ID
  assertion states to the receiver-flow graph boundary. The immutable
  calculation published 675 historical and 32 unresolved decisions, the
  additive v2 bundle preserved v1, and exact ArangoDB readback matched all
  identity states, 180,283 edges, and $4,672,820,179.49. All 707 non-current
  states are terminal-identity-ineligible. Dagster remains on v1 until the
  historical-source refresh policy is accepted. See the
  [v2 identity graph audit](./audit/arango-receiver-flow-identity-coverage-2026-09-01.md).
- [x] Define the narrow receipt, disbursement, candidate-context,
  reconciliation, independent-expenditure, and terminal-attribution fact
  boundaries. The complete 2024 classic scan proved `pas2` is an exact
  `SUB_ID` subset of `oth` and measured the legacy unreconciled transfer
  projections. See the [fact requirements](./design/fec-flow-fact-requirements.md)
  and [corpus audit](./audit/fec-classic-flow-and-schedule-e-2026-08-31.md).
- [x] Define and measure the processed Schedule B physical boundary and classic
  `oth`/`pas2` comparison. The complete artifact, exact 81-field schema,
  strict 157,544,163-row 2024 parser, `SUB_ID` uniqueness, endpoint direction,
  amount precision, acquisition/storage cost, and runtime gates pass. This
  first audit left source acceptance pending the later gates below. See the
  [corpus audit](./audit/schedule-b-source-and-classic-overlap-2026-09-01.md).
- [x] Align processed Schedule A and Schedule B from the immutable 2026-08-30
  publisher batch before accepting sender-side source use. The complete audit
  conserved 264,085,606 A facts, 157,544,163 B rows, and all 320,731 accepted
  receiver-flow rows. Unique exact or same-amount/different-date candidates
  cover 57.35% of A flow rows and 82.02% of their signed amount; conflicts,
  ambiguity, and one-sided evidence remain separate. No amounts were merged.
  See the [alignment audit](./audit/schedule-ab-alignment-2026-09-04.md).
- [x] Add processed Schedule B to coordinated release-inventory v3, then
  publish lossless selected-cycle columnar facts with immutable occurrence
  identity and complete replay. V1 and v2 remain unchanged. The complete 2024
  gate conserved 157,544,163 source rows as 157,544,163 facts in 158 verified
  Parquet shards with zero invalid rows or duplicate `SUB_ID`s. Source-stable
  identity prevents unrelated FEC changes from replaying the archive. See the
  [publication audit](./audit/schedule-b-columnar-publication-2026-09-04.md).
- [ ] On the next changed v3 release, verify that the release manifest's
  `stage_sha256` exists at
  `control/fec/release/stages/<stage_sha256>.json`. The manual 2026-09-04
  migration lost the standalone consumed stage bytes when two containers used
  one stdout file. The release embeds the exact outputs, and the CLI now
  preserves content-addressed inputs before pointer advance; this follow-up
  confirms the repaired path under a changed release.
- [x] Profile complete 2024 Schedule B action, memo, form, line, recipient,
  transaction, original-submission, back-reference, and filing evidence from
  the verified Parquet facts. The Go diagnostic conserves all 157,544,163 rows
  in 1,717 disjoint shapes and reproduces the exact source signed amount.
  See the [semantics audit](./audit/schedule-b-semantics-2026-09-08.md).
- [x] Implement the precisely scoped Schedule B processed non-memo reporting
  calculation and exact form-line roles. The complete 2024 gate conserves all
  source rows and cents, keeps separate reporting scopes distinct, and leaves
  55 records unresolved. Targeted source review confirms that beneficiary
  names and raw recipient IDs cannot define ownership. See the
  [reporting calculation gate](./audit/schedule-b-reporting-calculation-2026-09-08.md).
- [ ] Add immutable publication, persistent reuse, and thin Dagster wiring for
  the accepted reporting calculation when its downstream consumer is defined.
  The Go command currently emits deterministic JSON without an active pointer.
- [x] Define a conservative typed Schedule B sender cohort using reviewed
  reporting roles, exact matching endpoint IDs, and transaction-type agreement.
  Keep absent types, self references, earmarks, and unreviewed role/type
  combinations explicit. The complete 2024 gate selects 341,720 observations;
  it does not establish own-money funding or complete outgoing-flow coverage.
  See the [reconciliation gate](./audit/committee-flow-reconciliation-2026-09-08.md).
- [ ] Extend sender selection only after source-backed review of excluded and
  unresolved evidence. Most 2024 Schedule B rows lack `disb_tp`; a missing code
  cannot identify ordinary committee-funded contributions. Do not use names,
  purpose text, or raw-only self-recipient IDs as fallback flow endpoints.
- [ ] Investigate the remaining 55 unreviewed Schedule B form-line records
  before extending reporting-policy v1. Do not alias F3X/21 from its label or
  reinterpret F3X/17, F3/21B, or F3/22 without source evidence.
- [x] Persist fact-level Schedule A/B candidate reconciliation after both
  selection policies exist. The complete 2024 gate conserves every selected
  occurrence exactly once across exact, date-disagreement, conflicting,
  ambiguous, and one-sided components, with separate amounts and full artifact
  readback. Three complete component replays reproduce identical evidence.
- [x] Profile all reconciliation conflicts and ambiguous components, then
  verify targeted examples against full source facts. The Go review conserves
  all component profiles and verifies 76 source rows across 32 examples. Split
  reporting, recurring payments, generic/in-kind codes, signs, and source date
  anomalies remain explicit. See the
  [source review](./audit/committee-flow-source-review-2026-09-08.md).
- [x] Define the [additive evidence-graph boundary](./design/arango-committee-flow-evidence.md):
  separate A/B observation edges and candidate-component documents. No
  candidate group becomes an economic payment or a combined money total.
- [x] Implement immutable reconciliation result publication, persistent no-scan
  reuse, and exact readiness for the accepted observation-graph consumer.
  Go verifies immutable evidence and exact A/B/master source ancestry before
  advancing a pointer. Reuse avoids source-row decoding, not shard hashing.
  See the [publication contract](./design/committee-flow-publication.md).
  The [2024 gate](./audit/committee-flow-publication-2026-09-08.md) preserves
  exact prior result bytes and reuses in 15.738 seconds. The real readiness
  check rejected old master archive ancestry; the unchanged classic publisher
  supplied release-matched facts from the identical already-staged member.
- [ ] Extend reconciliation reuse across coordinated releases with unchanged
  relevant inputs without weakening pinned release ancestry. V1 intentionally
  retains the existing calculation identity, which includes the coordinated
  release; exact-identity reuse is implemented, cross-release reuse is not.
- [x] Implement the isolated committee-flow evidence graph, exact per-ledger
  readback, source drilldown, and explicit ledger-specific query gates.
  The [2024 gate](./audit/arango-committee-flow-evidence-2026-09-08.md) preserves
  each selected occurrence and complete component membership. The graph stays
  partial where same-cycle committee masters are absent; no terminal or
  economic-flow eligibility is inferred.
- [x] Wire reconciliation publication, evidence readiness, and the accepted
  observation projection through thin Dagster assets with exact partitions,
  validated metadata, and checks that block downstream consumers on failure.
  The [control-plane gate](./audit/committee-flow-dagster-2026-09-08.md) verifies
  exact immutable input handoff, local schema references, cycle-isolated
  blocking checks, and real 2024 execution/replay. This is not live weekly
  daemon activation or an all-cycle A/B gate.
- [x] Add the investigative serving boundary and source/evidence pagination.
  The [read-only Go API](./design/committee-flow-api.md) pins one projection,
  requires a ledger for observations/paths, authenticates keyset cursors, and
  verifies requested source shards without repeating full startup validation.
  The [2024 gate](./audit/committee-flow-api-2026-09-08.md) passes with read-only
  source storage. No resident service or public endpoint was deployed.
- [ ] Deferred behind upstream funding: add a thin investigative client and
  deploy the observation API with an
  explicit Compose memory budget, read-only database account, read-only source
  mount, and accepted proxy/access-control routing. Do not expose the current
  unauthenticated API publicly or add a host-port binding by default.
- [ ] Add measured reported-date filtering, committee-name search, and a
  multi-cycle projection catalog. Preserve source-cycle versus reported-date
  semantics and invalidate cursors on projection changes. Dense path queries
  must retain explicit bounds; no exhaustive/global claim from a capped query.
- [ ] Research versioned economic-flow resolution separately: split reports,
  recurring-payment ambiguity, cash/in-kind evidence, refund/sign semantics,
  and source date anomalies. Do not resolve these with names, purpose
  substrings, an arbitrary date window, or a greedy exact-match preference.
- [x] Select processed Schedule E as the recurring independent-expenditure
  occurrence authority. The real body and current 2024/2026 `pas2` comparisons
  proved freshness, field, coverage, and fractional-dollar advantages at low
  acquisition cost. Classic `pas2` remains comparison evidence.
- [x] Implement the Schedule E source contract and strict Go parser. The
  complete 548,318-row relation passed with zero invalid rows and exact
  367,801,873-byte/SHA conservation in 1.298 seconds. See the
  [source-contract audit](./audit/schedule-e-source-contract-2026-08-31.md).
- [x] Add Schedule E to coordinated release-inventory v2 as one all-history
  selected relation, then publish immutable selected-cycle occurrences and
  lossless facts and add the minimal Dagster asset wrappers. Do not mutate v1
  or encode Schedule E as four physical relations. The active v2 release and
  all four verified publications are recorded in the
  [publication audit](./audit/schedule-e-v2-publication-2026-08-31.md).
- [x] Define and publish the independent-expenditure effective-record
  calculation, then compare Schedule E with the 24/48-hour product. The
  accepted regular-report policy excludes memo X, keeps exact signed cents,
  retains repeated transaction keys, separates notices, and preserves 1,306
  included-but-unattributed exceptions across four cycles. See the
  [calculation audit](./audit/effective-independent-expenditures-2026-08-31.md).
- [x] Project effective independent-expenditure results into ArangoDB as
  evidence-backed spender-to-candidate support/opposition edges. The complete
  2024 probe conserved 5,495 result edges and $4,337,242,339.31, read every
  exact amount back, and reused the content-addressed database on replay. It
  remains outside candidate-controlled receipts. See the
  [projection audit](./audit/arango-independent-expenditure-projection-2026-08-31.md).
- [x] Publish an independent-expenditure projection-readiness bundle that
  freezes the calculation, candidate-master, and committee-master sets from
  one cycle and release, then automate the Dagster projection asset. The real
  2024 bundle passed immutable backing verification and a bundle-only graph
  replay reused the exact existing projection. See the
  [readiness audit](./audit/independent-expenditure-projection-bundle-2026-08-31.md).
- [x] Define and implement a versioned per-fact Schedule E candidate-reference
  calculation before another graph cycle. The same-release exact-context
  method passed the complete 2024 gate with 58,288 conserved decisions:
  45,185 confirmed, 1,811 resolved, 10,996 unverified, zero ambiguous, and 296
  unresolved. It preserves every reported ID, amount, source fact, and evidence
  code. See the
  [candidate-resolution audit](./audit/independent-expenditure-candidate-resolution-2026-08-31.md).
- [x] Derive resolved spender-candidate-stance groups from the per-fact
  candidate decisions, retain confirmed/resolved/unverified counts on each
  group, conserve the 296 unresolved facts outside candidate edges, and make
  that calculation the readiness-bundle and Arango projection input. Do not
  relabel the existing reported-ID probe as resolved. The 2024 aggregate
  published 5,303 groups and 296 sparse exceptions; the separate v2 graph has
  5,303 edges, zero missing masters, exact amount readback, and idempotent
  replay. See the
  [resolved projection audit](./audit/resolved-independent-expenditures-2026-08-31.md).
- [x] Run the unchanged independent-expenditure candidate-resolution,
  aggregate, readiness-bundle, and graph gates for 2020, 2022, and 2026. All
  four cycles are `ready`, have zero missing masters, conserve exact counts and
  signed cents, and reuse immutable calculations and content-addressed graph
  databases. See the
  [cross-cycle audit](./audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md).
- [x] Explain the 92 candidate IDs referenced by the 2024 effective Schedule E
  calculation but absent from same-cycle candidate masters. They cover 1,053
  included facts and $52,059,391.58. Fifty-five are historical official IDs
  with no 2024 cycle, 37 have no official candidate record, one reported ID
  serves different candidate names, and 10 IDs have direct office/state
  contradictions. They require per-fact resolution, not history backfill. See
  the [candidate-reference audit](./audit/schedule-e-candidate-reference-integrity-2026-08-31.md).
- [ ] Audit the official committee-history dump and committee summaries before
  expanding the required release inventory. The available 2020/2022/2024/2026
  cycle masters now explain the Schedule A projection's placeholders but are
  not a substitute for the separate history product. Use history for time-
  correct identity and outgoing-flow reconciliation, not as silent Schedule A
  patches. See the
  [completed cycle-master comparison](./audit/receipt-master-gaps-2026-08-31.md).
- [ ] Pin the current ArangoDB 3.11 runtime to an exact supported patch or
  digest, then test the 3.12 storage-compatibility path as a separate change.
  Do not add the separately packaged Graph Analytics product without a
  measured need; implement batch centrality/community work in Go and Dagster
  first.
- [ ] Normalize or explicitly preserve the publisher-derived `\\z` URL-regex
  anchor in the draft LDA filing schema. Python's Draft 2020-12 meta-schema
  validator rejects that anchor, so an all-contract schema sweep cannot pass
  until the compatibility rule is decided.

## Frozen Python-era backlog

The current rewrite phase is routed through the
[design index](./design/README.md). Each surviving legacy item must be
classified as keep, correct, replace, remove, or unresolved before reuse.

The persistent, project-scoped scratchpad. Cross-references @audit/ for source of each item and @plan.md for phase context.

This file is editable by both humans and the agent during sessions. Append-friendly; use markdown checkboxes; reference file:line where helpful.

---

## Next session (queued 2026-05-13)

Validation contract: every fix runs the four gates in @docs/validation.md. Diff against `docs/audit/baseline-2026-05-12.md`.

### Done in the May 12-13 arc (cleared from queue)

- [x] ~~Capture baseline~~ — `docs/audit/baseline-2026-05-12.md` 2026-05-12.
- [x] ~~Phase 2a/2b parent-org inheritance generalization~~ — 52 committees + $350M flipped 2026-05-12. CFG Action / NAR Cong Fund / NRA ILA target cases hit.
- [x] ~~`super_pac_unclassified` trace-through~~ — already in `PASSTHROUGH_TYPES`; the actual fix was **recursive IE trace** through passthrough Super PACs. Done 2026-05-12. by_pac collapsed from catch-all to near-zero; by_corporation captures 22% IE+ / 27% IE- (~$2.5B attributed across all candidates).
- [x] ~~Surface `by_individual` in IE output~~ — done 2026-05-12. Recursive-trace's individual attributions now visible in candidate document.
- [x] ~~`scripts/view_candidate.py`~~ — ~470 LOC `rich`-based renderer shipped 2026-05-12. Sanity-checks pass on all 5 named candidates.
- [x] ~~M-ORG_TP refinement via Wikidata P31~~ — 33 committees + $117M flipped 2026-05-13. Token-list approach replaced with reconci.link + 10-Q-id trade-class set.

### This week (small, high-impact fixes)

- [ ] **Audit-derived terminal-node cleanup** (per `docs/audit/terminal-node-classification-2026-05-13.md` — corrected reading after hardcode-discipline self-check).

  **Ship now (clean / principled):**
  - Add `" SEPARATE SEGREGATED FUND"` to `_CMTE_NAME_SUFFIXES` (catches AANA $6.7M + others). SSF is FEC's own legal term for the PAC arm of a corp/union — recognizing FEC vocabulary, same shape as existing entries. Target case after rerun: AANA flips to trade_association via Phase 2b name-cluster.
  - Add prefix-stripping for `"POLITICAL ACTION COMMITTEE OF THE X"` / `"PAC OF X"` in `_pac_search_name` (catches AAOS $7.4M). Symmetric to existing suffix-stripping; FEC has two equivalent naming conventions.
  - Provenance-flag cleanup at the start of Phase 1b — clear stale `terminal_type_refined_from_m_org_wikidata` / `terminal_type_wikidata_*` flags before recomputing (cleans up IFW + ASIS stale flags).
  - Relax Phase 2a CONNECTED-match to prefix-match for super_pac_unclassified → labor_union inheritance (catches UFCW SPAC $20M, UNITE HERE PAC $27M, IUOE SPAC $20M, USW WORKS $9M, CA Nurses PAC $8M — ~$85M proper org-rollup). Guardrail: require `CONNECTED ≥ 10 chars` to avoid over-matching short prefixes; direction is `parent.CMTE_NM.startswith(c.CONNECTED_ORG_NM)`.

  **Deferred (slope to hardcode-creep or bigger scope):**
  - ~~"farm bureau" Q-id to `_TRADE_CLASS_QIDS`~~ — defensible (Wikidata HAS the class) but each addition slopes toward eye-curated growth. Principled alternative is P279 walk from Q2178147 / Q829080 — but that's the ontology walker we deleted on 2026-05-11 for complexity / cache-corruption reasons. Defer until we have a third or fourth requested addition (then accept the curated-list shape OR re-introduce the walker with better safeguards). Today's cost of deferring: ~$7M (Texas Farm Bureau + Ohio Farm Bureau).
  - ~~Add `"DEMOCRACY ENGINE"` to `CONDUIT_PATTERNS`~~ — **arbitrary hardcode-creep, not shipping**. Replace with: **EARMARKED-memo-share conduit detection**. A committee where >X% of incoming `indiv.MEMO_TEXT` records contain `"EARMARKED FOR Y"` is a conduit by behavior, regardless of CMTE_TP / ORG_TP. That's FEC's own way of flagging conduit flow. Implementation: compute `earmarked_share` per committee at parse time (probably in `committee_receipts`); if > 0.8 AND `total_receipts > $1M`, override terminal_type to `passthrough` regardless of ORG_TP. Removes the `CONDUIT_PATTERNS` list entirely (WinRed/ActBlue would be detected structurally, not by name). Bigger code change — own session.

  Combined target case for the ship-now subset: re-run `view_candidate.py "BACON, DONALD J"` should show AANA / AAOS / other newly-trade committees in the trade_association sub-table; bulk median gate stays ≤5% (this is reclassification, not re-attribution — total receipts unchanged).

- [ ] **Generic-string rejection in `name_match.py`** — visible misresolutions in `by_organization`:
  - "TARGETED VICTORY" → corp (it's a digital ad agency)
  - "PRESIDENT" / "CEO" / "CONSULTANT" → corp (job titles donors typed as employer)
  - "State of Illinois" / "State of Nebraska" / "United States Department of the Army" → corp (geographic / agency leakage)
  - "Asana Journal" → corp (wrong Wikidata Q-id for a generic-word input)

  Implementation plan: extend `_accept_candidate` in `wikidata_resolver.py` with two principled signals — (a) reject if normalized input matches a US state name, "UNITED STATES" / "FEDERAL GOVERNMENT" / "DEPARTMENT OF X" / "STATE OF X" pattern; (b) reject if input is in a small set of common job titles donors type as employer (these are anti-values, similar shape to existing `NON_EMPLOYERS`).

  Target case: Cruz's IE+ `by_corporation` no longer shows TARGETED VICTORY $1.22M; Trump's by_organization no longer shows "United States Department of the Army" / "State of Nebraska"; Harris's no longer shows "State of Illinois" $2.78M / "Afghanistan War Commission" $1.66M.

- [x] ~~**Same-entity merge in `corporate_families`**~~ — DONE 2026-05-14 (commit `fd2c7d1`). Phase 3.5 in `wikidata_corporate_resolution` fetches each family's P112/P127/P749 upstream Q-ids; merges pairs sharing any upstream Q-id with guardrails: n=2 cluster only, shared name prefix ≥6 chars (post-legal-suffix-strip), edu-institution exclusion. Union-find handles transitive clusters.

  **Results**: 11 merges. Headline: Pan Am Systems $616M + Pan Am Railways $308M → **$923M one Mellon entity**. Also merged: Bloomberg TV/Beta, Marvel Comics+Entertainment+Games (union-find), DreamWorks+Animation, Rocket Companies+Mortgage+Loans, Hilton, Coca-Cola, Capitol Records. Two minor false-positive corporate-subsidiary mergers accepted (Universal Music/TV ~$1M, Samsung Electronics/Heavy ~$1M).

  Cache: `<cache_dir>/wikidata_upstream.json`. Provenance: surviving family docs record `merged_from: [{canonical_name, wikidata_id}, ...]`.

  **NOT caught** (no shared upstream — name-only variants): GREYLOCK + Greylock Partners; Adelson Drug Clinic + Adelson Clinic. These remain handled by `EMPLOYER_FAMILY_ALIASES` hardcoded dict (so that table still has work). Future: name-similarity-only merger pass, OR delete EMPLOYER_FAMILY_ALIASES once Adelson/Uline get proper Wikidata Q-ids.

  Four-gate validation passed: bulk median 2.4% unchanged, Pan Am consolidates on Trump's `by_organization` ($19.89M merged), pytest 65/65.

- [ ] **Audit the 33 Wikidata-flipped trade_association classifications.** Eyeball cache + the 380 not-flipped for false-negatives (real trade orgs Wikidata didn't classify with our P31 set). Add `docs/audit/trade-class-2026-05-13.md`.

- [ ] **Phase 1 rule for IE-only Super PACs without ORG_TP** — JDPAC (`C00630665`) currently lands in `terminal_type=passthrough` while UDP (`C00799031`) lands in `super_pac_unclassified`; both are CMTE_TP=W Super PACs with empty ORG_TP and null CONNECTED_ORG_NM. Add a Phase 1 rule in `committee_classification` BEFORE Phase 2 inheritance: `CMTE_TP='W' AND (ORG_TP IS NULL OR ORG_TP = '') AND CONNECTED_ORG_NM IN (NULL, 'NONE', '') → super_pac_unclassified`. Companion: scripts/audit_classification_consistency.py that surfaces (CMTE_TP, ORG_TP, CONNECTED_ORG_NM) tuples landing on different terminal_types — work-list for class-consistency cleanup. Discovered 2026-05-21; see @docs/decisions.md.

### UI work (target: 2026-05-17 / next weekend)

- [ ] **Web view consuming `funding_channels.aggregate`** — probably FastAPI + a single HTML template + Alpine.js. Reads Arango directly. Same `by_organization` + per-channel tables as the view tool, but rendered as clickable HTML.
- [ ] **`scripts/view_candidate.py --json`** — output mode emitting tables as JSON. Used by the web UI's static fallback.
- [ ] **`scripts/view_candidate.py --show-path <ORG>`** — walks the trace for a specific org, prints the multiplier-weighted chain ("AIPAC → JFC1 → JFC2 → candidate cmte, mult=0.42, attributed $X").

### Hardcoded data structures — survey + plans

Comprehensive sweep of every list / set / dict in `src/` (full audit in `docs/audit/terminal-node-classification-2026-05-13.md`). Categorized as: reference data (clean), calibrated parameter (clean), configuration sprawl (centralize), or arbitrary growth-prone list (the problem).

**Reference data — 17 entries, all clean.** Includes `_STOPWORDS`, `_CMTE_NAME_SUFFIXES`, `LEGAL_SUFFIXES`, `ABBREVIATIONS`, `NON_EMPLOYERS`, `CAMPAIGN_COMMITTEE_MARKERS`, `PER_ELECTION_LIMITS`, `_PERSON_TO_COMPANY_PROPS`, `_CEO_POSITION_QIDS`, FEC schema fields, our own bucket type system. Each maps to real-world taxonomy or our own architectural types. No action needed.

**Calibrated parameters — 3 entries, all clean.** Includes the HIGH/LOW reconci confidence thresholds (70/40), the `_MIN_ROOT_LENGTH` for name clustering (8), the trade-class confidence threshold (70). Each is a single number with documented rationale.

**Configuration sprawl — known issue, already on todo:**

- [ ] **`CYCLES = ["2020", "2022", "2024", "2026"]`** duplicated in 21 files — centralize in `src/config.py`. Long-standing item.
- [ ] **`LOG_PROGRESS_EVERY` intervals** — `pas2.py` % 250000, `contributed_to.py` % 50000, `arango_dump.py` % 500000. Pick one (probably `100_000`).

**Arbitrary / growth-prone lists — 3 entries, all have structural fix plans:**

- [x] ~~**`CONDUIT_PATTERNS`** in `assets/aggregation/candidate_upstream.py`~~ DONE 2026-05-21 (commit `4878e51`). Replaced with EARMARKED-memo-share structural detection: committee_receipts computes `earmarked_share` per committee; committee_classification Phase 1c flips terminal_type to `passthrough` when share > 0.8 AND receipts > $1M. ALSO: donors.py + contributed_to.py now parse `(CXXXXXXXX)` from MEMO_TEXT to re-attribute earmarked donations to the named target (key win: Altman went 0 → 86 graph edges, surfaced as expected). MAX(direct, earmark) dedupes against recipient-side 15E receipts. The old donor-NAME regex was also deleted (it was matching only 9 records across 2026, all of them legitimate "McConduit"-style surnames — net positive). Three remaining caveats filed as follow-ups: (a) data_quality.detail_coverage reads from committee_receipts which doesn't yet see earmark-redirected records, so detail_coverage shows 0 for Altman even though she has 86 graph edges — need to add earmark-target counting in committee_receipts. (b) WinRed has 0 earmark memos across all cycles in FEC bulk data — WinRed-bundled donations remain hidden via this mechanism (FEC bulk file appears to strip them; would need FEC web disclosure). (c) `(PENDING)` placeholder records (~1% of earmarks) can't be CMTE_ID-extracted; could be resolved with a name-match path.

- [ ] **`EMPLOYER_FAMILY_ALIASES`** in `rag/employer_normalization.py` (2 entries: ADELSON CLINIC, ULINE INDUSTRIES). Per-entity hand-curated aliases.
  - **Fix**: ride along with the queued `corporate_families` P749 (parent organization) Wikidata walk (already on todo). When that ships, "ADELSON CLINIC" and "ADELSON DRUG CLINIC" merge under their shared P749 parent; same for "ULINE INDUSTRIES" → "ULINE". The dict becomes obsolete and deletable. No standalone work needed — just delete EMPLOYER_FAMILY_ALIASES + `_apply_family_alias` callers after P749 walk lands.

- [ ] **`_TRADE_CLASS_QIDS`** in `assets/enrichment/committee_classification.py` (10 Wikidata Q-ids). Defensible (each entry IS a Wikidata class) but each addition slopes toward growth.
  - **Two principled paths**:
    1. **Keep curated** — accept the list as Wikidata-vocabulary reference data. Acceptable today; re-evaluate if it crosses ~20 entries.
    2. **P279 subclass-of walk** — traverse Wikidata's subclass graph from Q2178147 / Q829080 roots. This is the ontology walker we deleted on 2026-05-11 because of complexity + cache corruption. Could be re-introduced with better safeguards (cache TTL, fail-closed-on-fetch-error, explicit subtree-root list maintained externally). Bigger change. Defer until the curated list crosses ~20 entries or gets requested more than every 2-3 months.

**Whale path filter chain — VERIFIED GONE** (greppd 2026-05-13). `_NON_CORPORATE_P31`, `_GENERIC_DESCRIPTION_PATTERNS`, `_GOVERNMENT_DESCRIPTION_PATTERNS`, `_RETRY_SUFFIX_TOKENS`, `_EMPLOYER_OVERRIDES` — all deleted in the May 11-12 simplification arc.

**Dead-code leftovers** — ~~`src/cli/pies_v3.py` (679 LOC, on delete list) has stale copies of `TERMINAL_TYPES` / `PASSTHROUGH_TYPES` / `CONDUIT_PATTERNS`.~~ DELETED 2026-05-14 (commit `576bee0`) alongside `src/cli/check_funding.py` — 753 LOC removed, zero callers verified.

### Later this month / next month

- [ ] **OpenCorporates Layer 3** (waiting on API key approval). Would close ~40% of currently-not-found employer strings.
- [ ] **Audit other UPSERT sites** for the `mergeObjects: false` stale-merge pattern beyond `committee_receipts`.
- [ ] **Automated diff against baseline** — script that compares current state to `docs/audit/baseline-2026-05-12.md` and flags top-15 orgs that swing >20%. Currently the named-candidate gate is manual.
- [ ] **Single-cycle dev mode** — `dagster job execute --config` recipe for one-cycle iteration runs. Cuts iteration loop from ~22 min to ~5 min.
- [ ] **JSON-output mode for `validation_report.py`** for CI integration.

---

## Phase 3 — Ready to execute (after Phase 2 brain integration)

### Critical bug fixes

- [x] **`spent_on.py:202-203` int/float type mismatch.** Wrap `stats['support_amount']` and `stats['oppose_amount']` with `float(...)`. FIXED 2026-05-08.
- [x] **Drop `wikidata_corporate_resolution` dep from `candidate_funding`.** FIXED 2026-05-08 — see decisions.md.
- [x] **Trace algorithm cycle blowup** — `trace_committee_sources` lacked cycle detection AND multiplier cap, producing $16T totals for House races. FIXED 2026-05-08 with `propagated_from` set, `min(1.0, amount/receipts)`, and `min(1.0, all_mults[...]+new_mult)`. See decisions.md for details.

### Trace algorithm follow-ups (Phase 3 round 2)

- [ ] **Replace cap-based fix with proper fixed-point iteration.** Current fix prevents catastrophic over-report by capping at 1.0 but may slightly under-report cases of legitimate compounded mults. Clean fix is iteration-to-convergence rather than 8 levels with caps.
- [ ] **Normalize $1-receipts committees at parse time.** Some campaign committees have `total_receipts = $1` (likely FEC bulk-data quirks — technical filings without matching receipts). Decide: filter these from the trace, or normalize their receipts during parse.
- [ ] **Verify validation harness assertions.** `scripts/validate_funding_channels.py` checks magnitude, BWC sanity, distribution. After current run completes, expand if any new failure modes appear.

### Wikidata client overhaul

- [x] ~~**Persistent negative cache.**~~ Done in commit `9ac4321`. Asset now caches every name regardless of whether Wikidata had a match (`source='not_found'` for misses, distinct from `source='error'` which is intentionally NOT cached so failed requests retry next run).
- [x] ~~**Batched VALUES queries.**~~ Done in commit `fc0d2a3`. New `resolve_companies(names, chunk_size=50)` and `resolve_people(names, chunk_size=25)` collapse 5,000 sequential requests into ~100 batched ones via SPARQL VALUES.
- [x] ~~**Exponential backoff + circuit breaker.**~~ Done in commit `fc0d2a3`. 1s base delay doubling to 60s cap; 3 retries per query; circuit trips after 3 consecutive global failures (asset returns empty results rather than hanging for hours). `reset_circuit_breaker()` re-arms between runs.
- [x] ~~**Incremental cache flush.**~~ Done in commit `9ac4321`. Cache saved after every 5 employer batches and 4 whale batches plus a final flush.
- [x] ~~**Make cache path configurable via storage helpers.**~~ Done in commit `9ac4321`. Now `<cache_dir>/wikidata.json` via `get_cache_dir()`. Legacy `/workspace/wikidata_cache.json` read as one-time migration fallback.
- [x] ~~**End-to-end live validation.**~~ DONE 2026-05-10. Wikidata's SPARQL endpoint stayed unreachable across multiple retry attempts (timeouts, 429s, 502/500s). Switched primary path to MediaWiki's REST API (`wbsearchentities` + `Special:EntityData`) which is on different infrastructure and consistently fast (~0.7s/name). Asset gained a `resolution_path` config knob ('rest' default, 'sparql' kept as fallback for when their query service recovers).

  Run results (run id `66b60add-fc0f-45e2-b190-9ec29ac5f49c`, 45 min total):
  - **employers**: 4,999 processed → 2,281 wikidata-resolved + 2,201 not_found + 517 cache hits
  - **whales**: 1,964 processed → 1,098 wikidata-resolved + 845 not_found + 43 cache hits
  - **corporate_families**: 5 → **4,941**
  - **whale_corporate_links**: 5 → **352**
  - **employer_canonical_mapping**: 0 → **4,999**
  - **whale money attributed to corporate identities**: ~$50M → **$851.9M**

  Trump's `whale.corporate_connected` went from empty to $8.26M of real connections (Beal Bank, Anduril Industries, Allegiance Health, etc.). `by_organization` cross-cut now shows: Department of Government Efficiency $81.5M via Musk, Pan Am Railways $20M via Mellon, ULINE $4.9M via 3 Uihlein siblings consolidated, Marvel via Perlmutter, etc.

  Cruz's `corporate_connected`: BGR Group via Rogers/Rzepka/Eisner, Winklevoss Capital via the Twins, GeoSouthern Energy, Holland & Knight, PACK AUTO GROUP, etc. — real Texas + lobbying connections.

  Trade-offs accepted in REST path: doesn't capture inverse relations (founder is stored on the company side as P112, REST can't query inverse cheaply), so e.g. KOUM, JAN doesn't link to WhatsApp via "founder" relationship — only via P108 employer (which lists Yahoo as his prior employer). SPARQL UNION query handled this; REST does not. Acceptable miss; can be revisited when SPARQL is healthy.

- [x] ~~**Quirks to follow up:**~~ Resolved 2026-05-10 across two hit-rate-improvement passes (commits `12c04d0`, `a795735`, `815675c`):
  - ~~"ENTREPRENEUR" / "INVESTOR" / "PHILANTHROPIST" leaking through as employers~~ → expanded `NON_EMPLOYERS` in `employer_normalization.py`. Also added `CAMPAIGN_COMMITTEE_MARKERS` to filter "FOR CONGRESS" / "FOR SENATE" / "VICTORY FUND" leakage from self-funder donors who list their own committee in the employer field.
  - ~~"Federal Government of the United States" via Kelly Craft~~ → added `_is_government_entity` filter in `wikidata_client.py` rejecting descriptions matching "federal government", "government of", "sovereign state", "ministry of". Wired into both company and person resolution paths. Catches Kelly Craft's P108-target step.
  - ~~Generic-concept abbreviation matches (SIG → "advocacy group", ATT → "lawyer", BCG → "brightest cluster galaxy", BUSINESS → "business" Q-id, etc.)~~ → three-layer fix:
    - description-keyword blacklist (`_GENERIC_DESCRIPTION_PATTERNS`)
    - lowercase-label heuristic (real entities are title-cased)
    - top-N candidate filtering (wbsearchentities limit=5) + P31 instance-of blacklist (`_NON_CORPORATE_P31`: humans Q5, films Q11424, books Q571, vaccines, magazines, countries, languages, given names, submarines, Creative Commons licenses, etc.)
  - ~~Suffix variations splitting families (BLACKSTONE GROUP vs BLACKSTONE)~~ → `_alternate_employer_forms` retries with one suffix stripped (GROUP/HOLDINGS/PARTNERS/INVESTMENTS/CAPITAL/etc.). Verified: BLACKSTONE GROUP → Blackstone Inc., CITADEL INVESTMENT GROUP → Citadel Enterprise Americas LLC, BRIDGEWATER ASSOCIATES → Bridgewater Associates.

- [x] ~~**Wikidata resolution architectural overhaul**~~ — Completed in two phases. First pass (2026-05-10) added ontology walker + YAML overrides. Second pass (2026-05-11) deleted all of that and went minimal: reconci.link top-hit + GLEIF fallback. See `docs/decisions.md` 2026-05-11 entry for full context and `docs/corporate-resolution.md` for the portable architecture writeup.

- [x] ~~**Delete dead code in `wikidata_client.py`**~~ — DONE 2026-05-11 PM. `wikidata_client.py` shrank from 1309 → 150 LOC across two commits (resolver simplification + whale-path simplification + audit-driven purge). Deleted: entire employer-resolution legacy filter chain, all SPARQL paths, `_STRICT_CORPORATE_P31`, `_RETRY_SUFFIX_TOKENS`, `_alternate_employer_forms`, `_build_company_batch_query`, `_execute_sparql`, `resolve_company_to_canonical`, `ORG_TYPE_QID`, `_wbsearchentities`, the entire whale-path filter chain (`_should_reject_match`, `_NON_CORPORATE_P31`, `_GENERIC_DESCRIPTION_PATTERNS`, `_GOVERNMENT_DESCRIPTION_PATTERNS`, etc.), legacy worker helpers.

- [x] ~~**Apply resolver simplification to whale path**~~ — DONE 2026-05-11 PM. New `src/rag/whale_resolver.py` (~230 LOC) parallels `wikidata_resolver`: reconci.link → corroboration → entity-data → corporate-relationship property walk (P1830/P112/P169/P488/P1037/P3320/P108 + P39+P642). Adding P1830 was the key recovery — catches Griffin/Citadel which P108 doesn't. Disambiguation gap accepted explicitly (Arnold/Singer pick wrong Wikidata person; OpenCorporates Layer 3 would address).

- [x] ~~**Multi-signal corroboration for low-confidence reconci**~~ — DONE 2026-05-11 PM. New `src/rag/name_match.py` adds four deterministic signals (acronym, in-order portmanteau, Levenshtein ≤2, bidirectional token containment) plus the short-input rule. Rejects the RDV→North-Vietnam class while recovering the WILMERHALE / BCG / BAUPOST class. 42 unit tests, microsecond-fast.

- [ ] **OpenCorporates as Layer 3 for coverage extension** — current Wikidata+GLEIF combined hit rate is ~55-65%. OpenCorporates (~200M companies including most US small private LLCs) would close most of the long-tail "Wikidata gap" category per `docs/audit/coverage-gaps-2026-05-11.md` (pending). Free tier 500 reqs/day; paid tier for production. Implementation: thin client + strict-match acceptance similar to GLEIF, slotted between Layer 2 (GLEIF) and not-found in `wikidata_resolver.resolve_batch`.

- [ ] **LLM-based input normalization for garbled FEC names** — typos, partial entries, malformed multi-word employer fields. ~20% of not-founds per the coverage-gap categorization. Cheap LLM call: "normalize this employer string to its canonical company name" before passing to the resolver. Use joi-hosted Qwen — no API costs. Layer above reconci.link.

- [ ] **Cache TTL enforcement** — currently `_is_cache_hit()` treats any non-error source as permanent. Should be tiered: wikidata.json 30-day TTL; GLEIF results (split into separate cache file) 7-day TTL (corporate `status` field flips). Implementation: extend `_is_cache_hit()` to inspect `cached_at`. Lower priority now that the cache-corruption pressure from the old ontology walker is gone.

- [ ] **Remaining hit-rate residuals (subsumed by overhaul above, kept for reference):**
  - Some corporate families that should be merged remain split: ADELSON DRUG CLINIC vs ADELSON CLINIC (different Q-ids in Wikidata or one not_found), Pan Am Systems vs Pan Am Railways (different real entities owned by Mellon). Need follow-up: SPARQL parent-resolution would catch most of these via P749 once SPARQL endpoint is healthy.
  - KKR HOLDINGS suffix-strips to KKR which still ranks Kolkata Knight Riders (cricket team Q1156894, P31=Q12973014) above Kohlberg Kravis Roberts (Q1570773, real KKR private equity). Add Q12973014 (cricket team) and other sport-team P31 Q-ids to `_NON_CORPORATE_P31` so the cricket team gets skipped and the real KKR is picked from top-N.
  - FEC name format vs Wikidata search ranking: "SIMONS, JAMES H" → "James Simons" → top hit is 19th-century lawyer Confederate general (Q109713137), not Jim Simons hedge fund mathematician (Q560847). "Jim Simons" gets the right hit but the FEC normalization doesn't always produce that form. Hard to fix without per-name disambiguation hints.
  - Moskovitz-shaped gaps: founders are listed on the company side (P112) not the person side, so REST path can't follow that edge cheaply. Acceptable miss; deferred until SPARQL is healthy enough to do the inverse query.

### Configuration centralization

- [x] ~~**Single `ACTIVE_CYCLES` constant.**~~ DONE 2026-05-14 (commit `2302859`). `src/config.py` created; 21 files updated; `list(ACTIVE_CYCLES)` everywhere. Adding 2028 now needs only one file change.
- [x] ~~**`PER_ELECTION_LIMITS` constant.**~~ DONE 2026-05-14 (commit `2302859`). Moved from `donors.py` to `src/config.py`; re-imported in `donors.py` so existing internal references stay working.
- [x] ~~**`TERMINAL_TYPES`, `PASSTHROUGH_TYPES`, `CONDUIT_PATTERNS`** in `candidate_upstream.py:67-74`~~ — duplication was with pies_v3.py, which is gone (commit `576bee0`). Sole-site constants now; tightly coupled to the trace algorithm. Leave in-file.
- [ ] **Progress logging interval constants.** `pas2.py:130` (% 250000), `contributed_to.py:272` (% 50000), `arango_dump.py:248` (% 500000) all use different intervals. Pick one (`LOG_PROGRESS_EVERY = 100_000`), use everywhere.

### Cache + data hygiene

- [x] ~~**Move repo-root caches to `cache/`.**~~ Done in commit `9ac4321`. `wikidata_cache.json` migrated to `~/workspace/data/legal-tender/cache/wikidata.json`. Both `/wikidata_cache.json` and `/corporate_families.json` removed from tracking and added to `.gitignore` so they can't be re-committed.

### Logging hygiene

- [ ] **Replace 158 `print()` calls with logger / context.log.** Concentrations: `src/utils/preflight.py`, `src/utils/arango_schema.py`, scattered through assets. Critical impact: many warnings (including the wikidata 429s) hit stdout but not Dagster's UI. Source: @audit/code-quality-findings.md §5a.
- [ ] **Audit 6 bare `except Exception:` clauses.** Three are around metadata-emit in `*_summaries.py`; should at minimum log the swallowed exception. Two are reasonable fallbacks. One in `canonical_employers.py:243` needs investigation. Source: @audit/code-quality-findings.md §5b.

### Dead code removal (verify before deleting)

- [x] ~~**Delete `src/resources/embedding.py`**~~ (320 LOC) — DONE 2026-05-11 PM. Registered as a Dagster resource but no asset consumed it. Pure facade.
- [x] ~~**Delete `src/api/election_api.py`**~~ (67 LOC) — DONE 2026-05-11 PM. Placeholder, never wired.
- [x] ~~**Delete `src/cli/pies_v3.py`** (679 LOC)~~. DONE 2026-05-14 (commit `576bee0`). Zero callers verified.
- [x] ~~**Delete `src/cli/check_funding.py`** (74 LOC)~~. DONE 2026-05-14 (commit `576bee0`). Zero callers verified.
- [ ] **Decide on `src/api/lobbying_api.py`** (63 LOC). Aspirational; keep if the lobbying-integration plan is live, delete otherwise.
- [x] ~~**Move root-level dev scripts.**~~ DONE 2026-05-14 (commit `c94a4c2`). Moved to `scripts/` with descriptive names: `check_fec_download.py`, `check_fec_schema.py`, `validate_fec_schemas.py`.
- [ ] **Verify no orphan imports** for the 4 deleted enrichment files (`committee_financials.py`, `corporate_hierarchy.py`, `employer_cluster_integration.py`, `employer_clustering.py`). Source: @audit/code-quality-findings.md §2c.
- [x] ~~**Verify `compute_normalized_key` / `find_potential_matches`**~~ DONE 2026-05-14 (commit `7d6ae79`). Zero external callers confirmed; both functions + their re-exports deleted; unused `hashlib` import removed alongside.

### Doc structure (Phase 1 stub → Phase 3 content)

- [ ] **Fill in `docs/architecture.md`** with the system view. Current high-level diagram is in `README.md`; extract.
- [ ] **Fill in `docs/funding-channels.md`** by extracting sections 4-5 of `pipeline.md` (the 5-channel breakdown + two-phase trace algorithm).
- [ ] **Fill in `docs/storage.md`** by consolidating dump format, mount semantics, and the recent Arango memory tuning details.
- [ ] **Trim `docs/pipeline.md`**: remove the "4 jobs" section (only `fec_pipeline_job` exists), update the database list to include `fec_2026`, drop or relocate the "Validation Results (Feb 7, 2026)" snapshot, fix the directory structure block. Source: @audit/doc-accuracy.md.
- [ ] **Fix `docs/fec-data.md`** directory-structure block (currently shows `legal-tender/data/fec/20xx/`, real is `~/workspace/data/legal-tender/raw/<cycle>/`). Add a note about the FEC 302→S3 redirect quirk. Source: @audit/doc-accuracy.md.
- [ ] **Reframe `docs/decisions.md`** as a dated decision log (it's structurally that, but framed as "current state"). Update the "Last Updated" header to acknowledge it's a historical record. Source: @audit/doc-accuracy.md.
- [ ] **Rewrite `README.md`** as a human-facing project pitch. Strip the stale "4 jobs" table and stale collection counts. Modeled on flux-studio's README structure. (Phase 1 also targets this — may already be done by the time you read this; if so, check off.)

### Larger refactors (sequence carefully)

- [x] ~~**Refactor `candidate_upstream.py`.**~~ Done 2026-05-09 (commits `a259def`, `242a04c`, `06cbb7c`, `b0f2400`). 1,545 LOC monolith with one ~1,050-line nested function lifted into module-level helpers: `is_conduit`, `_resolve_company`, `trace_committee_sources`, `trace_ie_sources`, `_safe_pct`, `_top_sources`, `_top_companies`, `_merge_named_list`, `compute_funding_channels`, `merge_funding_channels`, `load_lookup_data`, plus `_compute_for_candidate` / `_process_one_candidate` for the ProcessPool worker path. Validated byte-identical output for Trump/BWC/Cruz across each extraction. Closing step (`b8faa23`) wrapped the candidate loop in ProcessPool for the deferred 15-25× perf goal — got 3.8× in practice (180s → 47s on this workstation), bottlenecked by Phase 1 serial load + serial DB UPSERTs.
- [ ] **Centralize ArangoDB resource boilerplate.** 31 sites of `arango.get_client()/get_database()/get_collection()`. Add a `arango.scoped(db_name, [collections...])` context manager. Source: @audit/code-quality-findings.md §6a.
- [ ] **Centralize 75 collection-management calls.** `truncate()`, `create_collection()`, `add_persistent_index()` repeat across asset files. A schema-aware helper would DRY this.
- [ ] **Move 88 inline AQL queries to a `src/queries/` module.** Phase 3 candidate, lower priority than the other refactors.

### "4 jobs" misclaim cleanup

- [ ] **Decide: define the missing 3 jobs OR remove all references.** The misclaim is in 3 places: `README.md:70-77`, `docs/pipeline.md:132-156`, `src/__init__.py:7-12`. Possible jobs to define: `enrichment_job`, `aggregation_job`, `upstream_job` (the docstrings already specify their selections). Or remove the claims entirely. Either is fine; pick one. Source: @audit/code-quality-findings.md §1d.

## Phase 4 — Production-readiness

- [ ] **Set up `.github/workflows/` CI.** Build on push to main → push image to GHCR or local registry. Source: @audit/production-gaps.md §1.
- [ ] **Self-hosted GitHub Actions runner OR webhook receiver** so production stack auto-pulls + restarts.
- [ ] **Backup automation for ArangoDB.** Versioned snapshots, off-host copy, restore runbook. Source: @audit/production-gaps.md §2.
- [ ] **Secret management for prod.** Adopt the pattern from `~/workspace/monitor/secrets/`. Source: @audit/production-gaps.md §3.
- [ ] **Resource limits in compose.** Memory and CPU bounds for arangodb, postgres, dagster. Source: @audit/production-gaps.md §4.
- [ ] **Prometheus metrics from the pipeline.** Wire Dagster to the existing `~/workspace/monitor/` Prometheus. Per-asset duration, success counter, row counts, wikidata cache hit rate. Source: @audit/production-gaps.md §5.
- [ ] **Loki log shipping.** Add Promtail config to scrape Docker logs by container label. Source: @audit/production-gaps.md §6.
- [ ] **Schedule env toggle.** Add `DAGSTER_SCHEDULES_ENABLED` env so `weekly_fec_refresh` can default-stopped in dev, default-running in prod. Source: @audit/production-gaps.md §7.
- [ ] **Re-enable `fec_pipeline_job` in prod after Phase 3 fixes.** Don't enable until wikidata + spent_on are fixed. Source: @audit/production-gaps.md §8.
- [ ] **Rollback story.** Keep last 3 image tags. Document the swap procedure in `operations.md`. Source: @audit/production-gaps.md §9.
- [ ] **Fill in `docs/operations.md`** runbook. Source: @audit/production-gaps.md §10.
- [ ] **`justfile` for common ops commands.** Source: @audit/production-gaps.md §11.
- [ ] **Versioning + CHANGELOG.** Semver tags, populate from `decisions.md`. Source: @audit/production-gaps.md §12.
- [ ] **Add `LICENSE` file at root.** README claims MIT; commit it. Source: @audit/production-gaps.md §13.
- [ ] **Test infrastructure.** `tests/` directory, `pytest.ini`, smoke tests for FEC parsers with fixture data. Source: @audit/production-gaps.md §14.
- [ ] **Type checking.** Add `mypy` or `pyright` config + CI check. Source: @audit/code-quality-findings.md §8.

## Phase 5 — Continuous operation

- [ ] **Adopt `claudewatch`** (`blackwell-systems/claudewatch`) for AGENTS.md drift detection. 29 MCP tools including `get_drift_signal`. CLI: `scan` / `gaps` / `suggest` / `fix` / `track` / `watch`. Replaces what we'd otherwise hand-build. Source: research delegated 2026-05-07.
- [ ] **Wire RSS Feed Parser MCP** to monitor upstream agentic-coding ecosystem (agentsmd, khoj, basic-memory, claude-code). Weekly digest into the vault.
- [ ] **First setup-currency audit** due 2026-08-07. Use a different model as auditor (Qwen on joi, or a different Claude version) per the published "audit-fix-loop" pattern.

## Cross-project — extending the brain to other repos

- [ ] **Onboard `found-footy`** to the brain stack: add AGENTS.md + docs/ structure mirroring legal-tender, symlink `~/workspace/obsidian/brain/projects/found-footy → ~/workspace/dev/found-footy/docs/`.
- [ ] **Onboard `spin-cycle`** similarly.
- [ ] **Verify cross-project Khoj queries work** as expected once 2+ projects are indexed.

## Open questions

- Should `corporate_families.json` at repo root be deleted or kept? It's tracked in git but shouldn't be (it's derived). Investigate whether anything still loads it before removal.
- Should we keep `feature/employer-enrichment` branch around (now merged into main) or delete it? Same for `feature/five-pies-by-cycle`, `feature/rag-implementation`, `feature/upstream-money-tracing`, `refactor/arango`.
- Auto-memory cleanup: what specifically to keep vs. move to AGENTS.md? See "Memory model" section in AGENTS.md.

## Validation follow-ups (revealed by bulk weball cross-reference)

After the unitemized-grassroots fix landed (commit `1298db7`), median delta dropped 33% → 10.8%. The next layer of issues showed up in the worst-offenders list:

- [x] ~~**Self-funder gap.**~~ FIXED 2026-05-09 (commit `306a61e`). committee_receipts pulls weball `CAND_CONTRIB + CAND_LOANS` per cycle, subtracts the indiv.zip ENTITY_TP=CAN overlap to avoid double-counting with whale_indiv_total, surfaces as `individuals.self_funded` sub-bucket. Trone/Lamon/Gibbons/Bloomberg/Steyer all gone from worst-offenders.

- [x] ~~**Leadership-PAC / coordinated party expenditure inflation.**~~ FIXED 2026-05-09 (commits `662bd96`, `2701a71`). Root cause was *not* leadership-PAC scope as initially hypothesized — the `cmte_type` filter in candidate_funding already excludes type N. The actual bug was in `transferred_to`: oth records included 24K-coded coordinated party expenditures (NRCC paying $14.65M for ads about Scalise) as if they were committee transfers. Per FEC rules, coordinated party expenditures are not contributions; the money never enters the candidate's account. Fixed by filtering oth to receipt-only TRANSACTION_TPs (11/15/18G/H/K/L family + 22Z) and pas2 to real contribution codes (24K/24P/24Z). Scalise/Pelosi/Schumer/Haley all dropped out of worst-offender lists.

- [x] ~~**Sanders 2020 +35% whale/grassroots double-count.**~~ FIXED 2026-05-09 (commit `306a61e`). The actual root cause was *not* whale double-counting — it was `cn.CAND_PCC` pointing at the dormant BERNIE 2016 cmte instead of operational BERNIE 2020. weball.TTL_INDIV_CONTRIB got synthesized into BERNIE 2016 (zero indiv records) and double-counted alongside the real BERNIE 2020 indiv data. Phase 3.5 now routes auth values to whichever principal committee has indiv activity in that cycle. Diagnosis fully written up in commit message. Affects any recurring candidate with retired-but-not-terminated old principals.

- [ ] **Stale-merge masquerading as live data.** Discovered alongside the PCC reroute: ArangoDB UPSERT...UPDATE deep-merges nested objects by default. When `receipts_by_cycle` drops a cycle entry between runs, the old entry survives the merge and looks live. Fixed in `committee_receipts` with `OPTIONS { mergeObjects: false }`. Same pattern probably exists elsewhere — audit other UPSERT call sites in graph/enrichment/aggregation assets and add the option where the structure of nested fields can change between runs.

- [ ] **Bulk validation — automate as part of Phase 4 production-readiness.** Add `validation_report.py` to a CI step or scheduled job. Set thresholds: median should stay <15%, within-±10% rate should stay >40%. Alert if regressions.

### Remaining outliers after 2026-05-09 filter pass

Headline metrics now: median |delta| **2.5%**, within ±5% **62%**, within ±10% **73%**, within ±25% **85%**. The remaining outliers are individual edge cases, not systematic bugs. Logged here for future investigation; not blocking.

- [ ] **BWC consistent -3% drift across all cycles.** Was +0.7% pre-transferred_to-filter, now -3% on every cycle. Suggests a small amount of legitimate pas2 PAC contributions were filtered out (records where the giver filed Schedule B but the receiver didn't file matching Schedule A, so they're not in oth). ~$25-30K per cycle on BWC. To address: smarter pas2/oth merge that takes pas2 records when no oth match exists. Lower priority — small absolute drift.
- [ ] **Biden 2022 -94%** ($718K vs FEC $12.1M). POTUS edge case — Biden wasn't actively fundraising in 2022. His weball shows TTL_RECEIPTS=$12.1M but only $6.18M is in TRANS_FROM_AUTH (matching our 18G); the other $5.9M is in unidentified accounting categories. His oth has only 18G and memos — no other transaction types. Worth a deep-dive on what FEC includes in TTL_RECEIPTS that's not in our parsing. Probably needs cross-referencing with `cm` (committee summary) records.
- [ ] **Krishnamoorthi 2026 -63%, Allred 2024/2026 -66%, Crockett 2026 -44%.** Newer candidates with light data — investigate whether their principal cmte routing is correct (could be similar to the Sanders-2020 PCC issue but with a different shape).
- [ ] **Trump 2022 +100%, Perdue 2022 +122%, Conley 2026 +189%, Ode +91%.** Small-cmte over-attribution. Likely JFC partner-cmte issues (candidate is partnered in a JFC but we're attributing JFC totals to them when we shouldn't). Different from the leadership-PAC issue but in the same family.
- [ ] **Loeffler 2020 -20%, Porter 2024 -41%.** Mid-sized under-attribution. Investigate whether their affiliated_with edges are correctly connecting them to the right cmtes.
- [ ] **Ramaswamy 2024 +28%.** Self-funder — verify our self-funding netting handled his case correctly. May have CAND_LOANS that were also itemized differently.
- [ ] **CHRISTINA CLEMENT 2024 +75476%** ($12.1M vs $16K). Almost certainly a different-candidate-with-same-name aliasing problem (data joining on NAME instead of CAND_ID at some point). Verify CAND_ID handling in candidate_funding.

### Issues surfaced by output spot-checks (2026-05-09)

Ran `scripts/output_check.py` against BWC, Cruz, Trump, Sanders, Bloomberg, Pelosi, Scalise. Structural data quality is high; whale corporate-connections work where Wikidata data exists. Concrete issues found:

- [x] ~~**`NEA FUND FOR CHILDREN AND PUBLIC EDUCATION` classified as `corporation`.**~~ FIXED 2026-05-09. Root cause: FEC's bulk file labels NEA Fund's `ORG_TP="C"` even though `CONNECTED_ORG_NM="NATIONAL EDUCATION ASSOCIATION"` and the parent committee NEA itself is correctly `ORG_TP="L"`. Fixed by adding a Phase 2 inheritance pass in `committee_classification`: any cmte classified `corporation` whose `CONNECTED_ORG_NM` matches another cmte's `CMTE_NM` (and that other cmte has a more specific terminal_type — labor_union/trade_association/ideological/cooperative) inherits it. Sets a `terminal_type_inherited_from_connected: true` flag for transparency. Self-extends as more parent committees get correctly classified — no manual override list. Currently catches NEA; future runs will catch any new mis-filings of the same shape.

- [x] ~~**`WhatsApp LLC` $10M IE Oppose against Trump.**~~ FIXED 2026-05-09 (commit `fc99cec`). Real root cause was *not* the IE attribution model — that math was actually correct (donor_amount × multiplier where multiplier scales by the PAC's spending direction). The bug was upstream: FEC's `webk.INDV_CONTRIB` for Super PACs / Hybrid PACs reports $0-$100 instead of the real itemized donations ($75 vs $81M for SFA Fund). Phase 3.5 trusted that, making SFA's `total_receipts=$2.15M` instead of $83.6M, which made the IE-trace multiplier saturate at 1.0, dumping the donor's full contribution onto every candidate the Super PAC spent on. Fix: prefer `max(auth_total, indiv_summed)` for `total_from_individuals` so Super PACs' real itemized totals override FEC's broken summary numbers. Trump anti-Trump WhatsApp dropped $10M → $1M (proportional and plausible). Pan Am Railways IE Trump dropped $47M → $20M.

- [x] ~~**Whale → corporate attribution labels are interpretively misleading.**~~ FIXED 2026-05-09 (commit `c7dcedc`). Added `via_donors` field to each `by_organization` entry showing the top 5 donor names that produced any IE / employee attribution, with their per-direction amounts (IE+, IE-, employees). Real corporate PACs (Citizens United, Bankers PAC, Comcast) have empty via_donors because their amounts come from direct_pac, not founder personal donations — clean separation. Output-shape change ripples through per-cycle output, aggregate output, and `output_check.py` rendering. Surfaced a real previously-hidden data point: KOUM, JAN funded both pro-Trump and anti-Trump PACs (Trump WhatsApp shows IE+ $690K AND IE- $552K, both via Koum).

### Donor name normalization (surfaced via via_donors output 2026-05-09)

- [ ] **Donor records split by trailing-period in name.** `BIGELOW, ROBERT T` and `BIGELOW, ROBERT T.` appear as two distinct donor entries — same person, different normalizations. Same likely true for many "MR." / "MR" / "JR." / "JR" suffixes, and for variations like `T` vs `T.` (with period). Donor canonicalization in `donors.py` AQL doesn't strip trailing punctuation. Fix in the AQL: `LTRIM(RTRIM(REGEX_REPLACE(name, '\\.+$', '')))` or a Python normalization step. Watch out for over-normalization (`SMITH, J. R.` should stay distinct from `SMITH, J R`).

### Donor fragmentation at scale (audit 2026-05-09)

47 of the top 50 megadonors by aggregate $ have multiple donor records — both within-name fragmentation (KOUM JAN: 7 records across RETIRED/SELF-EMPLOYED/WHATSAPP/MANZANITA/etc.) and cross-name (BLOOMBERG MICHAEL vs BLOOMBERG MICHAEL R., STEYER TOM vs THOMAS F., GRIFFIN KENNETH C. vs KENNETH C. MR., ADELSON SHELDON / SHELDON G. / MIRIAM / MIRIAM DR.).

Dominance heuristic (max_record / total > 50%) reliably flags fragmentation: top whales 38-100% (mostly 60-100%), common-name controls (SMITH MICHAEL with 70 records of distinct people) at 17-24%. But raw name-based merging is risky for common names — SMITH MICHAEL has $7.8M of one notable Megadonor Smith mixed with $4.9M of 69 distinct small Michael Smiths.

- [ ] **Wikidata-keyed donor canonicalization (gated on Wikidata being run).** The clean fix uses `wikidata_id` from `whale_corporate_links` as the merge key (not name). Name-search normalization in `_whale_name_to_search` strips middle initials so cross-name variants resolve to the same Q-id. Implementation: new `donor_canonical` collection mapping each donor_key → canonical_donor record with merged totals; soft-merge so it's reversible. candidate_funding's lookup follows the indirection. Doesn't touch raw `donors` / `contributed_to`.

### Employer canonicalization audit (2026-05-09)

`canonical_employers` has 100,372 entries (1,312 above $1M aggregate, 95 above $10M). Top entries surface clear fragmentation:

- **Google/Alphabet split into 5**: `ALPHABET ($1M, 47 donors)`, `GOOGLE ($1M, 3)`, `GOOGLE CLIENT SERVICES ($0.5M, 11)`, `GOOGLE FIBER ($0.3M, 6)`, `GOOGLE VENTURES ($0.5M, 7)`. Same parent company.
- **Blackstone split**: `BLACKSTONE ($118M, 349 donors)` + `BLACKSTONE GROUP ($46M, 144)` = $164M actual. Same firm.
- **Citadel split**: `CITADEL INVESTMENT GROUP ($124M, 4)` + `CITADEL ASSET MANAGEMENT ($71M, 5)` — Ken Griffin's empire.
- **Adelson Clinic split**: `ADELSON DRUG CLINIC ($201M, 5)` + `ADELSON CLINIC ($109M, 5)` — same Miriam Adelson clinic.
- **Suspicious "CORPORATION"** ($57M, 2 donors) — generic word as employer.

Current `employer_normalization.py` handles legal suffixes (LLC/INC/CORP/LP/LLP) and abbreviation expansion (INTL→INTERNATIONAL), but NOT:
- Generic business suffixes (GROUP, HOLDINGS, MANAGEMENT, PARTNERS) — risky to add ("Group Health" is its own entity)
- Parent-company resolution (Google → Alphabet) — fundamentally needs Wikidata
- Same-family sub-entities (Citadel Investment vs Citadel Asset Management)

- [ ] **Wikidata-driven corporate-family consolidation** is what fixes most of this. Once `wikidata_corporate_resolution` runs successfully, the `corporate_families` collection rolls these up. This audit reinforces Wikidata's importance — without it, the corporate attribution model is structurally degraded by name fragmentation that no rule-based approach can fix.

### Audits that surfaced no real issues (2026-05-09)

These were items in the "honest sequence" of things that might be wrong; spot-checking confirmed they're fine. Logging so future audits know they're not load-bearing.

- [x] **Cycle assignment.** indiv.TRANSACTION_DT distribution within each fec_YYYY shows 99%+ of records dated within the cycle's expected 2-year window (fec_2020 has 99% in 2019-2020, fec_2022 has 99% in 2021-2022, etc). Tail of cross-cycle dates (~0.5% of records) are amendments/corrections; small enough to be noise. FEC's bulk-file boundaries align with cycle conventions — no cycle-mis-assignment bug.
- [x] **JFC passthrough accounting.** TEAM SCALISE 2022 spot-check: received $28.2M from individuals (matches FEC webk.INDV_CONTRIB exactly), transferred $12M to SCALISE FOR CONGRESS. Trace multiplier = 12/28.2 = 42.5% — correct passthrough math. JFCs work.
- [x] **spent_on support/oppose classification.** Top 2024 edges look correct: FF PAC supports Harris/Biden, AMERICA PAC supports Trump (Musk's PAC), MAGA Inc supports Trump AND opposes Harris (same PAC doing both, correctly tagged), WINSENATE/SLF oppose opposing-party Senate candidates. $10.3B 2024 IE volume matches reality.
- [x] **Conduit filter coverage.** `WINRED|ACTBLUE|EARMARK|CONDUIT|UNITEMIZED` catches the major aggregator donor names. Top "donors" with org-shaped names are mostly candidate self-funders (`ISSA - PERSONAL FUNDS, DARRELL`, `STEEL - PERSONAL FUNDS, MICHELLE`) or one-off org-as-IND filings (`NEA ADVOCACY FUND`) — not missed conduits. No expansion needed.

### Terminal-classification audit (2026-05-09)

- [x] ~~**CMTE_TP=I/E unclassified.**~~ FIXED 2026-05-09 (commit `60461c4`). 993 IE-only entities (Reid Hoffman, SEIU PEAF, AFL-CIO COPE Treasury, Worker Power, etc.) moved from `unknown` → `super_pac_unclassified`. Trace now routes upstream through them.
- [ ] **Professional/trade associations classified as `ideological`.** `NATIONAL ASSOCIATION OF REALTORS PAC` ($63M), `AMERICAN ASSOCIATION FOR JUSTICE PAC` ($24M, trial lawyers), `COUNCIL OF INSURANCE AGENTS & BROKERS PAC` ($17M), AICPA ($12M), ADA ($8M), AOA ($8M), AANA ($7M) all classified `ideological` because their FEC `ORG_TP=M` ("Membership organization") spans both single-issue advocacy AND profession-of-X societies. Could refine with name-pattern heuristics ("ASSOCIATION OF [profession]", "ACADEMY OF X", "COUNCIL OF X PROFESSIONALS") but these get subjective fast. Document as known semantic limitation OR adopt a high-precision heuristic.
- [x] ~~**`DEMOCRACY ENGINE, INC., PAC` ($46M) classified as `corporation`.**~~ Handled structurally as part of the 2026-05-21 EARMARKED conduit detection (commit `4878e51`). Re-classification is automatic for any committee with earmarked_share > 0.8 AND receipts > $1M.
- [ ] **Phantom committees in `unknown`** with `$50M`, `$61M`, `$100M` totals — bogus FEC filings (e.g. DODO GOVERNMENT $100M from a single record with txn_tp=19). They don't connect to any candidate's funding_channels via affiliated_with so they're cosmetic noise in the committees collection, not affecting outputs. Could filter them at `committee_receipts` parse time if any are detected via "donation_count == 1 && total_receipts > $5M" heuristic.

## Performance / iteration speed (2026-05-09)

- [x] ~~**Cycle-level parallelism via threads.**~~ Done in commit `2542b86`. Added `src/utils/parallel.py` with `parallel_cycles` (threads, I/O-bound) and `parallel_map` (processes, CPU-bound, reserved for future use). Wired into `committee_receipts` Phases 3+3.5, `donors`, `transferred_to`. `committee_receipts` ~12min → ~6min (2× — Amdahl-limited because Phases 1, 2, 4 are still sequential single AQL queries / single UPSERT loop). `indiv`, `pas2`, `oth` were already parallel.
- [x] ~~**Phase 4 of `committee_receipts` parallel UPSERT.**~~ Done in commit `662bd96`. 8 thread workers, each owning a chunk of disjoint cmte_ids and its own batched UPSERT stream. Speedup turned out modest (~30s saved on a ~6min run) — ArangoDB UPSERT is server-side bottlenecked; more client workers just queue at the server. Worth keeping for slightly faster runs and as a pattern, but Phase 3+3.5 cycle parallelism remains the bigger win.
- [x] ~~**`candidate_funding` ProcessPool.**~~ Done 2026-05-09 (commits `a259def`, `242a04c`, `06cbb7c`, `b0f2400`, `b8faa23`). The candidate_upstream.py monolith refactor and ProcessPool wire-in shipped together. Five-step extraction: is_conduit + trace helpers → compute_funding_channels → merge_funding_channels → load_lookup_data → ProcessPool the candidate loop. Module globals + explicit `mp.get_context('fork')` so workers inherit parent memory copy-on-write — no pickling of the ~hundreds of MB of edges/cmte_info dicts. Result: 180s → 47s on this workstation (~3.8× — less than theoretical max because Phase 1 loading and final UPSERTs are still serial, but materially faster iteration pace). Asset down from 1,545 LOC to ~1,260 LOC with most of the body now module-level helpers; the asset function itself is ~100 LOC of orchestration.
- [ ] **`contributed_to` first loop**. Mutates shared `committees_dict` / `candidates_dict` across cycles — needs per-cycle local dicts + post-loop merge before it's parallel-safe. The second loop (the heavy one) is straightforward and follows the donors/transferred_to pattern.
- [ ] **Single-cycle dev mode.** When iterating on a code-correctness fix, running 4 cycles is wasted work — most bugs surface in any one cycle. Add a launcher convention or run-config helper so a dev-iteration run takes ~1/4 the time. Combined with the parallelism above, code-iteration loop should drop from ~22 min to ~2-3 min. Asset Configs already accept `cycles: List[str]` — just need a documented GraphQL incantation or `dagster job execute --config`-style helper. Document in `operations.md`.
- [ ] **Audit other UPSERT sites for `mergeObjects: false`.** The stale-merge bug discovered in `committee_receipts` likely exists elsewhere — anywhere we write nested dict fields (`receipts_by_cycle`, `funding_channels.by_cycle`, etc.) via UPSERT. If the structure of those nested fields ever shrinks between runs, old entries leak through. Sweep all `UPSERT...UPDATE` AQL in src/ and add the option where applicable.

## Brain stack — known issues (Phase 2 follow-ups)

Location: `~/workspace/obsidian/` (not yet a git repo).

- [x] ~~**Khoj chat doesn't use the vault for retrieval.**~~ FIXED 2026-05-08. Root cause: anonymous mode resolves requests to user `username="default"` (per `configure.py:187`), but our init script was attaching content to `vedanta@brain.local` (the Django admin user). Indexed entries lived on the wrong user. Fixed in `init-khoj.py` to attach LocalMarkdownConfig + API token + uploads to the `default` user. Verified: chat returns 17 context items citing real vault docs.
- [x] ~~**Khoj streaming None-content bug.**~~ FIXED 2026-05-08. Patched in `~/workspace/obsidian/Dockerfile.khoj` (lines 513 + 613 in `openai/utils.py`). Still TODO: submit issue/PR upstream to `khoj-ai/khoj`.
- [x] ~~**Khoj telemetry slowing chat.**~~ FIXED 2026-05-08. Set `KHOJ_TELEMETRY_DISABLE=true` in `.env` — Khoj was trying to phone home to `khoj.beta.haletic.com` and timing out.
- [ ] **Submit Khoj patches upstream**: streaming None-content (lines 513, 613) — file issue/PR.
- [ ] **Verify Khoj-as-MCP** when upstream confirms — currently deferred per research 2026-05-07.
- [ ] **`docs/operations.md`** entry for the brain stack: how to add a project, how to re-index, how to recover Khoj DB.
- [ ] **`~/workspace/obsidian/` as a git repo** — user noted this should become a private repo. Not yet `git init`'d.

## Done in this professionalization effort

- [x] Storage relocation `~/workspace/.legal-tender/` → `~/workspace/data/legal-tender/{raw,dumps,cache}/` (commit `284a84b`)
- [x] Tailscale FQDN redacted from committed files (commit `284a84b`)
- [x] Sync HEAD redirect-following fix (commit `ddebc02`)
- [x] Cycle list extended to include 2026 (commit `f2805d2`)
- [x] Arango memory tuning, dev + prod (commit `fb34a44`)
- [x] Phase 0 audit complete (commits `ea5ae72` through `24da113`)
- [x] AGENTS.md + CLAUDE.md symlink + doc reorg (Phase 1)
- [x] Brain stack at `~/workspace/obsidian/` — `docker compose up -d --build` end-to-end self-driving: 4 services, idempotent config init, initial vault upload, patched Khoj. (Phase 2)
- [x] `~/.claude/CLAUDE.md` user-global: identity, port allocation, joi infra, Docker-first install policy, anti-patterns.
- [x] Khoj streaming None-content bug patched via `Dockerfile.khoj`.
