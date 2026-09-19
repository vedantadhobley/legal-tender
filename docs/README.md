# Legal Tender documentation

Routing index for project behavior, target design, operations, decisions, and
historical evidence.

## Authority map

Use the documents in this order when sources disagree:

1. [`design/product-contract.md`](./design/product-contract.md) defines the
   desired product. It is a draft until its open decisions are accepted.
2. Accepted source/evidence contracts and [`decisions.md`](./decisions.md)
   define the rules justified for the rebuild.
3. Focused current-system documents describe the existing implementation.
4. [`audit/`](./audit/) preserves point-in-time evidence and can be stale.

The [legacy functional specification](./design/legacy-functional-spec/) is a
reference for the Python exploration, not target authority or an implementation
guide. It suggests questions and counterexamples; only independently accepted
`KEEP` decisions justify parity tests. Feature counts and old totals are not the
rebuild's acceptance criteria.

Code and live data remain authoritative for current runtime behavior. A target
document does not become as-built truth until the implementation lands and the
document is updated accordingly.

## Redesign

- [Go rewrite checkpoint](./audit/go-rewrite-checkpoint-2026-09-19.md) —
  durable source checkpoint, implemented/not-implemented boundary, paused identity
  research, and the accepted path to one product-shaped 2024 candidate slice.
- [Grounding-bound referent assessment comparison](./audit/prose-referent-assessment-model-2026-09-19.md) —
  ten-case Gemma/GPT-OSS trial with immutable grounding and exact replay; task
  separation improves structure, but mention-versus-antecedent errors block promotion.
- [Referent and correction-target model comparison](./audit/prose-referent-model-comparison-2026-09-19.md) —
  complete Gemma 31B/GPT-OSS comparison with exact replay; a few synthetic successes
  do not overcome zero usable real-source answers or combined-task instability.
- [Proposed referents and correction targets](./design/prose-referents-and-corrections.md) —
  tested literal-occurrence, mention-group, correction-target and separately joined
  canonical-candidate boundaries; all proposals remain unverified and test-only.
- [Role focus and endpoint grounding](./design/prose-role-grounding.md) —
  test-only whole-entry evidence, literal endpoint surfaces and separate role scope;
  structural checks do not approve model meaning or identity.
- [Fresh role-grounding trial](./audit/prose-role-grounding-model-2026-09-17.md) —
  five retained two-stage cases; complete context and role separation coexist with
  surname ambiguity, an omitted retraction and a reserved-ID rejection.
- [Roles from uncorrected name candidates](./audit/prose-role-binding-model-2026-09-17.md) —
  ten retained second-stage attempts; valid references coexist with coreference,
  role-scope and selected-context errors, without accepted affiliations.
- [Isolated name-selection trial](./audit/prose-name-selection-model-2026-09-17.md) —
  separate names-only task, pre-run labels and exact offline comparison; known-case
  recovery and explicit fresh-name misses, without relationship or identity acceptance.
- [Fresh derived-reference trial](./audit/prose-derived-reference-model-2026-09-17.md) —
  simpler reference contract on six new inputs; endpoint errors, truncation and a
  joint-activity contract limitation remain separate from structural passes.
- [Token-ID model trial](./audit/prose-token-selection-model-2026-09-17.md) —
  known-case format comparison, exact source ranges, reference/meaning failures and
  paired usage; no established reliability gain or accepted affiliations.
- [Source-owned citation selection](./design/prose-citation-selection.md) —
  token-ID selections, Go-derived required references and explicit extra context;
  contract proof and linked model trials keep exact citations separate from meaning.
- [Fresh interpretation-model trial](./audit/prose-interpretation-model-2026-09-17.md) —
  fixed automatic producer on new excerpts; separate literal, binding, role and
  context review, with complete failures retained and no accepted affiliations.
- [Literal mentions and relationship interpretations](./design/prose-mention-interpretation.md) —
  offline contract proof separates exact source anchors from supplied normalization,
  binding and context links; reviewed fixtures are not automatic prose extraction.
- [Code-owned prose evidence attachment](./audit/prose-evidence-attachment-2026-09-16.md) —
  test-only entry references, exact Go-attached context and fifteen retained outcomes;
  fresh controls expose remaining role, pronoun and retraction errors.
- [Stronger local prose-model comparison](./audit/stronger-prose-model-comparison-2026-09-16.md) —
  unchanged fourteen-case task on GPT-OSS; improved abstention and explicit remaining
  citation/meaning failures, with both model runs replayed offline.
- [Local prose-model comparison](./audit/local-prose-model-comparison-2026-09-16.md) —
  opt-in self-hosted Gemma trial, exact source citations, retained failures and
  offline replay; model proposals remain untrusted and no graph behavior changes.
- [Unchanged prose extraction evaluation](./audit/prose-extraction-evaluation-2026-09-16.md) —
  out-of-development replay finds one of six reviewed roles; records five misses
  and a separate retrieval failure without changing the grammar or accepting links.
- [Prose relationship candidate prototype](./design/prose-relationship-prototype.md) —
  opt-in deterministic role/name-form syntax extraction with exact citations;
  retained-source regression results, unknown dates and no accepted donor links.
- [Supplementary affiliation-source investigation](./audit/supplementary-affiliation-sources-2026-09-16.md) —
  same-sample company/government evidence, retained HTML/PDF and extraction tests;
  prose interpretation and autonomous source discovery remain separate gaps.
- [Automated FEC appearance enrichment](./design/affiliation-enrichment.md) —
  one-command selection, Wikipedia/Wikidata capture and per-appearance evidence
  report; retained replay and an automatic additional sample expose coverage gaps.
- [Person identity and affiliation evidence policy](./design/person-affiliation-acceptance.md) —
  implemented candidate/time evaluator and separated v2 identity-evidence classifier;
  production acceptance remains open with no graph/money change.
- [Person identity evidence v2 evaluation](./audit/person-identity-evidence-v2-2026-09-16.md) —
  retained and synthetic replay of structured name, organization, role, provenance,
  locality and rival states without selecting a donor.
- [Person identity-rule feasibility review](./audit/person-identity-rule-feasibility-2026-09-16.md) —
  retained cases and official-source contrasts that rejected exact fields, mandatory
  locality, receipt-day identity and blanket rival blocking.
- [Dated person/company evidence check](./audit/dated-person-role-evidence-2026-09-16.md) —
  historical corporate disclosures, retained-source tests and separate identity/
  continuity decisions; no new runtime rule or accepted affiliation.
- [Person and dated-role binding diagnostic](./design/person-binding-diagnostic.md) —
  tested source-to-appearance comparison, separate name/employer/time results and
  coarse-date uncertainty; no accepted identity or financial attribution.
- [Read-only relationship query](./design/relationship-query.md) —
  implemented entity-based role and parent/child queries over retained source
  bodies; both directions, original dates and explicit gaps, without identity joins.
- [Relationship evidence comparison](./audit/relationship-evidence-comparison-2026-09-16.md) —
  source-backed FEC cases, measured enrichment gaps and one future legislative
  example; recommends a small relationship read path, not another source pipeline.
- [Relationship exploration and development scope](./design/relationship-exploration.md) —
  current read-only findings, proposed relationship meanings and verified partial
  cleanup; review before graph implementation, with recovery work parked.
- [Company-page evidence extraction](./design/company-page-evidence.md) —
  offline Go text/metadata extraction with exact byte locations; no prose-role
  interpretation, accepted donor identity or affiliation publication.
- [Independent company registry discovery](./design/organization-registry-discovery.md) —
  source-derived GLEIF name queries without existing QIDs/LEIs, retained ambiguity
  and offline replay; no identity or affiliation approval.
- [Exploratory affiliation discovery](./design/affiliation-discovery.md) —
  Go query variants, Wikipedia-linked candidates and source-bound relevance checks;
  retained live counterexamples, no identity or financial approval.
- [Automatic structured-role extraction](./design/wikidata-role-extraction.md) —
  bounded Go extraction of Wikidata relationships and date precision; retained-source
  replay and bounded source comparison pass with explicit discovery gaps; no identity approval.
- [Retained person-affiliation corpus](./design/person-affiliation-corpus.md) —
  exact real-source replay, reviewed role annotations and explicit name/time gaps;
  no automated role discovery or identity approval.
- [Person-affiliation rule tests](./design/person-affiliation-testing.md) —
  synthetic namesake, role, date and multiple-affiliation screening; no live person
  resolution, identity publication or financial attribution.
- [Filed registrant identity evidence](./design/organization-filed-identity.md) —
  bounded SEC inline-XBRL name/CIK capture and replay, exact byte locators and
  explicit limits on FEC identity inference; no graph approval.
- [Independent issuer discovery](./design/organization-issuer-discovery.md) —
  SEC bulk snapshot adapter and offline name-to-CIK candidates; no Wikipedia/LEI
  dependency or identity approval; live fetch and exact offline replay pass.
- [Organization registry corroboration](./design/organization-corroboration.md) —
  bounded GLEIF capture, preserved identifier claims, offline comparisons and
  explicit missing-ID/time/identity boundaries; no graph approval.
- [Organization evaluation and corroboration](./design/organization-evaluation.md) —
  retained source-backed corpus, v1/v2 offline comparison, separate failure/coverage counts
  and requirements before identity publication.
- [Organization candidate capture and replay](./design/organization-resolution.md) —
  current work: Go source-backed queries, Wikipedia/Wikidata evidence and offline
  versioned name proposals with explicit reasons; no identity merges or graph publication.
- [Node/edge assumption review](./design/pre-attribution-review.md) —
  implemented connection rules, interpretation choices and user review;
  recovery engineering is separate deferred work.
- [Reproducible Go builds](./go-build.md) — pinned compiler, offline clean builds,
  exact executable comparison and retained source/module inputs.
- [Go build acceptance gate](./audit/go-build-2026-09-14.md) — matching clean and
  archive-only builds, failure-mode tests and measured resource use.
- [Publication-independent funding paths](./design/funding-window-reader.md) —
  verified date windows, receipt entries, candidate context, dated spending members and historical facets.
- [Committee date-window gate](./audit/funding-window-2026-09-13.md) —
  complete selected-ledger date counts, automatic real witnesses and exact fresh replay.
- [Receipt/candidate window gate](./audit/funding-window-connections-2026-09-13.md) —
  automatic source-qualified entries, date exclusions, authorization context and replay status.
- [Source-grain spending window gate](./audit/funding-window-spending-2026-09-14.md) —
  independent full-source census, dated members, parent preservation and real replay status.
- [Cycle partitions and calculation windows](./design/cycle-calculation-windows.md) —
  reusable cycle evidence, explicit cross-cycle dependencies and rollout boundaries.
- [Implemented-interpretation review](./audit/pre-attribution-review-2026-09-14.md) —
  historical first packet; the current review also covers shared conduits.
- [Checkpoint evidence proposal](./audit/pre-attribution-checkpoint-2026-09-14.md) —
  exact checksum pins, retained-source availability and remaining recovery requirements.
- [Reported identity assertions](./design/reported-identity-assertions.md) —
  exact employer/occupation and committee-organization views without resolution.
- [Reported identity assertion gate](./audit/reported-identity-assertions-2026-09-13.md) —
  source-field conservation, complete retained scan and replay acceptance status.
- [Reported receipt roles at boundaries](./design/terminal-receipt-roles.md) —
  complete compact-index profiles, exact reported-ID joins and preserved source grain.
- [Receipt-role profile live gate](./audit/terminal-receipt-roles-2026-09-13.md) —
  whole-corpus verification, source witnesses and worker-varied replay status.
- [Terminal-source boundary assessment](./design/terminal-source-assessment.md) —
  complete selected-ledger topology, provisional hypotheses and explicit origin blockers.
- [Terminal assessment live gate](./audit/terminal-source-assessment-2026-09-13.md) —
  pinned population comparison, automatic source witnesses and fresh replay status.
- [Generation-bound typed paths](./design/funding-paths.md) —
  receipt/committee/candidate routes, separate ledgers and explicit search boundaries.
- [Typed path live gate](./audit/funding-paths-2026-09-13.md) —
  data-selected multi-hop witnesses and retained independent replay status.
- [Generation-bound neighborhoods](./design/funding-neighborhoods.md) —
  typed one-hop queries, source drilldown, facet gaps and scoped pagination.
- [Neighborhood live gate](./audit/funding-neighborhoods-2026-09-13.md) —
  automatic family witnesses and retained fresh-process replay status.
- [Typed funding-evidence generation](./design/funding-evidence-generation.md) —
  exact receipt/A/B/E binding, typed routing, source reuse and explicit coverage.
- [Typed generation live gate](./audit/funding-evidence-generation-2026-09-13.md) —
  retained inputs, complete selected-graph readback and fresh generation replay.
- [Reference-content equivalence](./design/reference-content-equivalence.md) —
  full archive/member/fact proofs preserve separate provenance across repackaging.
- [Reference equivalence live gate](./audit/reference-content-equivalence-2026-09-13.md) —
  retained source proofs, full connection census and automatic replay status.
- [Receipt-to-candidate connection](./design/receipt-candidate-connection.md) —
  read-only cross-graph witnesses, automatic case selection and exact replay gate.
- [Automatic connection gate](./audit/receipt-candidate-connection-2026-09-12.md) —
  retained executable/inputs, full-index census and live acceptance status.
- [Full-cycle participant publisher](./design/arango-receipt-participant-cycle.md) —
  explicit disk guards, shard checkpoints, verified resume and immutable completion.
- [Participant cycle gate](./audit/arango-receipt-participant-cycle-2026-09-12.md) —
  exact whole-cycle identity, failure/recovery evidence and live acceptance status.
- [Compact Arango participant gate](./audit/arango-receipt-participants-compact-2026-09-12.md) —
  lean appearances, exact full-field reconstruction and live expanded-graph equivalence.
- [Streaming receipt-participant graph](./design/arango-receipt-participants.md) —
  bounded typed import/readback, exact authorization ancestry and explicit sample scope.
- [Arango participant gate](./audit/arango-receipt-participants-2026-09-12.md) —
  real sample edges, worker-varied replay, source drilldown and measured storage costs.
- [Shared receipt-reference profile](./design/shared-reference-profile.md) —
  complete group-role diagnosis of shared-degree rejections without new links or money.
- [Shared-memo group rule and source review](./design/shared-reference-group-rule.md) —
  automatic whole-neighborhood witnesses and a separate complete-group association rule.
- [Shared-conduit graph extension](./design/shared-conduit-generation.md) —
  isolated new associations, unchanged base graphs and exact generation binding.
- [Shared-conduit query gate](./audit/shared-conduit-queries-2026-09-14.md) —
  explicit path/neighborhood routing, source drilldown and fresh replay status.
- [Shared-conduit window gate](./audit/shared-conduit-windows-2026-09-14.md) —
  original-receipt dates, outer-generation binding and byte-identical live replay.
- [Complete shared-conduit gate](./audit/shared-conduit-publication-2026-09-14.md) —
  full-cycle upgrades, old/new conservation, graph readback and replay status.
- [Cycle-wide conduit qualification](./design/receipt-conduit-publication.md) —
  exact participant/endpoint join, shared role policy and explicit unresolved membership.
- [Conduit publication gate](./audit/receipt-conduit-publication-2026-09-12.md) —
  cross-shard replay, corpus qualification and retained-data acceptance status.
- [Source-grain participant index](./design/receipt-participant-index.md) —
  conserving appearance publication, shared source-role policy and full-fact drilldown.
- [Participant index gate](./audit/receipt-participant-index-2026-09-12.md) —
  real worker-varied replay, all-value comparison and complete-cycle acceptance status.
- [Receipt reference endpoint safety](./design/receipt-reference-topology.md) —
  invalid-incident propagation, shared association policy and compact candidate-pair topology.
- [Reference topology gate](./audit/receipt-reference-topology-2026-09-12.md) —
  fixture/replay/failure checks and full retained-input acceptance status.
- [Narrow receipt reader](./audit/receipt-narrow-reader-2026-09-12.md) —
  exact sample-field equivalence, reduced row-copy cost and the full-cycle gate.
- [Real-source scan profile](./audit/receipt-source-scan-profile-2026-09-12.md) —
  measured reader scaling, Parquet row-copy overhead and bounded optimization targets.
- [Parallel receipt-reference processing](./design/receipt-reference-parallelism.md) —
  complete-report workers, shared resource caps and unchanged reference semantics.
- [Parallel reference measurements](./audit/receipt-reference-parallelism-2026-09-11.md) —
  accepted real equivalence, retained results and separate source/worker performance limits.
- [Complete-cycle reference join](./design/receipt-reference-join.md) —
  bounded exact-key joins, duplicate preservation, shared policy and reverse-reference evidence.
- [Complete-cycle join gate](./audit/receipt-reference-join-2026-09-11.md) —
  passing full-cycle execution, layout-varied replay and source/artifact checks.
- [Receipt participant publication](./design/receipt-participant-publication.md) —
  source-backed appearances, exact report-reference scope and bounded sort-run benchmark.
- [Receipt reference benchmark gate](./audit/receipt-reference-index-2026-09-11.md) —
  three real shard samples, full projected-field checks, memory/storage measurements and replay.
- [Connected funding-graph completion plan](./design/connected-funding-graph.md) —
  current backend priority, Python/Go capability comparison, missing core connections and acceptance gates.
- [Candidate connection drilldown](./audit/candidate-connection-2026-09-11.md) —
  exact report-to-source lookup, complete row readback, preserved parents and replay.
- [Named candidate report gate](./audit/candidate-report-2026-09-11.md) —
  pinned source names, complete path examples, unchanged core evidence and exact replay.
- [Integrated candidate evidence view](./design/candidate-evidence-view.md) —
  one reproducible receipt/upstream/summary result, readable report, and unselected attribution policies.
- [Integrated candidate evidence results](./audit/candidate-evidence-2026-09-11.md) —
  real 2024 reports, complete joined-source checks and exact JSON/Markdown replay.
- [Positive remaining receipt-family witnesses](./audit/positive-receipt-families-v2-2026-09-11.md) —
  all new source shapes, real metadata binding and a shared zero-numbered-header blocker.
- [Remaining receipt-family comparison gate](./audit/receipt-families-v2-2026-09-11.md) —
  shared v2 form rules, thresholded detail boundaries and exact prior-command replay.
- [Positive receipt-family source gate](./audit/positive-receipt-families-2026-09-11.md) —
  remaining initial loan/transfer/party shapes, preserved source exceptions and identical runtime binary.
- [Receipt-family summary comparisons](./design/receipt-family-summary.md) —
  exact form-specific fields, separate reported/detail comparisons and preserved summary assertions.
- [Receipt-family summary gate](./audit/receipt-family-summary-2026-09-11.md) —
  real matching and blocked spans, blank loan evidence, independent raw checks and prior replay.
- [Receipt-family date-window comparisons](./design/receipt-family-window.md) —
  exact family sums with independent reported/detail coverage, explicit zero bases and blocked gaps.
- [Receipt-family window gate](./audit/receipt-family-window-2026-09-11.md) —
  retained F3/F3X windows, independent per-day/source checks and byte-identical prior replay.
- [Receipt-family absence qualification](./design/receipt-family-absence.md) —
  bound reported zero with complete original/profile inventories, without synthetic detail or window totals.
- [Receipt-family absence gate](./audit/receipt-family-absence-2026-09-11.md) —
  qualified and blocked retained cases, raw census checks and exact older-command replay.
- [Complete F3X receipt-family witness](./audit/receipt-family-witnesses-2026-09-11.md) —
  original/profile contribution and transfer checks, memo-date preservation and unchanged Go replay.
- [Receipt-family Go comparisons](./design/receipt-family-comparison.md) —
  additive report-level contribution, transfer and loan comparisons with explicit missing detail.
- [Receipt-family comparison gate](./audit/receipt-family-comparison-2026-09-11.md) —
  retained loan matches, blocked alternatives, exact replay and independent source checks.
- [Receipt-family source map](./design/receipt-families.md) —
  form-specific contributions, loans, transfers, refunds, offsets, detail coverage and cash limits.
- [Receipt-family audit](./audit/receipt-families-2026-09-11.md) —
  official field mapping, complete saved-profile conservation, and positive cover amounts without detail.
- [Receipt detail versus reported windows](./design/receipt-reported-window.md) —
  pinned source-aligned occurrence comparisons, explicit empty-report evidence, and separate financial guards.
- [Receipt-window gate](./audit/receipt-reported-window-2026-09-10.md) —
  exact retained line/report/summary match, blocked alternatives, original checks, and no bulk rescan.
- [Reported span versus cycle coverage](./design/summary-reported-span.md) —
  v2 exact-window flow comparisons with unknown prefix/suffix activity and unchanged stock rules.
- [Early-cycle source gate](./audit/cycle-prefix-2026-09-10.md) —
  first-report/date evidence, six real reported comparisons, explicit outside spans, and v1 replay.
- [Summary/window comparison](./design/summary-report-window.md) —
  field-specific date scope, exact reported differences, preserved conflicts, and independent snapshots.
- [Summary/window gate](./audit/summary-report-window-2026-09-10.md) —
  one real closing-stock pair, explicit cycle-prefix blockers, raw-source checks, and replay.
- [Reported date-window calculation](./design/report-window.md) —
  field-specific coverage, exact sums, separate cash boundaries, and explicit arithmetic discrepancies.
- [Report-window gate](./audit/report-window-2026-09-10.md) —
  qualifying and incomplete windows, missing covers, preserved cash differences, and exact replay.
- [Report-period field binding](./design/report-field-binding.md) —
  pinned electronic 8.4 covers, exact chain-candidate field pairs, and separate cash eligibility.
- [Electronic binding gate](./audit/report-field-binding-2026-09-10.md) —
  seven retained reports, 35 bound fields, independent workbook checks, and unchanged prior results.
- [Report membership and period coverage](./design/report-period-membership.md) —
  explicit source-chain candidates, conserving interval coverage, and separate financial eligibility.
- [Report membership gate](./audit/report-period-membership-2026-09-10.md) —
  two retained populations, four windows, attachment exclusions, and independent day checks.
- [Explicit unitemized receipts](./design/report-unitemized-receipts.md) —
  preserved report-period fields, independent subtotal checks, and unknown donor composition.
- [Unitemized retained-source gate](./audit/report-unitemized-2026-09-10.md) —
  positive/zero/blank observations, blocked pairs, and byte-identical prior results.
- [Same-report total-receipts comparison](./design/report-total-receipts-comparison.md) —
  first qualified field/interval numeric comparison, exact pair scope, and separate financial eligibility.
- [Report-period receipts gate](./audit/report-total-receipts-2026-09-10.md) —
  one numeric pair, six blocked pairs, field-local conflict tests, and unchanged ledgers.
- [Bounded report-scope assessment](./design/report-scope-assessment.md) —
  exact paper cover/supplement evidence, preserved metadata disagreements, and no financial replacement selection.
- [Retained report-scope gate](./audit/report-scope-2026-09-10.md) —
  four offline witnesses, independent workbook mapping, raw conservation, and unchanged processed data.
- [Summary-value use policy](./design/summary-value-use.md) —
  reported values versus qualified funding inputs, field-local blockers, and independent bulk graph availability.
- [Summary-use regression gate](./audit/summary-value-use-2026-09-10.md) —
  retained discrepancy cases, unmodified receipt evidence, and no financial eligibility promotion.
- [Bounded report-metadata capture](./design/report-metadata-capture.md) —
  manual Go HTTP transport, explicit budgets, failed-attempt evidence, and bulk-source separation.
- [Report-metadata capture gate](./audit/report-metadata-capture-2026-09-10.md) —
  live seven-record readback, HTTP/2 compatibility fix, and explicit demo-quota stop.
- [Go report-metadata reader](./design/report-metadata-reader.md) —
  local capture verification, raw endpoint assertions, bounded pagination evidence, and no financial selection.
- [Report-metadata reader gate](./audit/report-metadata-reader-2026-09-10.md) —
  complete retained-page readback, endpoint conflicts, exact hashes, and CLI replay.
- [Report-metadata qualification](./audit/receipt-report-metadata-2026-09-10.md) —
  bulk header limits, endpoint disagreements, pinned paper schema, and a separate metadata-source proposal.
- [Receipt memo and amount source review](./audit/receipt-memo-review-2026-09-10.md) —
  complete bounded paper/electronic membership, preserved memo conflicts, and original one-cent witnesses.
- [Report and amendment coverage](./design/receipt-report-coverage.md) —
  missing metadata, report/account scope, and source qualification before financial selection.
- [Complete-cycle receipt report-line profile v2](./design/receipt-report-profile-v2.md) —
  every occurrence grouped by report/line, independent memo/individual axes, and v1 equivalence checks.
- [Complete 2024 v2 profile gate](./audit/receipt-report-profile-v2-2026-09-10.md) —
  exact full-cycle conservation, complete v1 regrouping, report-reference scope, and unresolved memo codes.
- [Bounded receipt report-line review](./design/receipt-report-lines.md) —
  complete report membership with independent memo/individual axes and exact lineage.
- [Original report-line gate](./audit/receipt-report-lines-2026-09-10.md) —
  seven complete original-file comparisons, exact cover subtotals, and preserved date exceptions.
- [Same-release receipt report profile](./design/receipt-report-profile.md) —
  one-pass physical-occurrence form/line, report-reference, and date diagnostics.
- [Complete 2024 report-profile gate](./audit/receipt-report-profile-2026-09-10.md) —
  changed source bytes, full strict scan, actual form/date scope, and independent conservation.
- [Summary/receipt comparison readiness](./design/summary-receipt-compatibility.md) —
  nine-field Go review, source/scope blockers, exact cohorts, and no financial promotion.
- [Summary/receipt readiness gate](./audit/summary-receipt-readiness-2026-09-10.md) —
  five real 2024 cases, source-version mismatch, independent checks, and replay.
- [Report-level summary investigation](./audit/summary-report-review-2026-09-10.md) —
  exact filer, attachment-selection, and paper-transcription witnesses; no automatic repairs.
- [Summary assertion grouping](./design/committee-summary-assertions.md) —
  exact evidence groups, preserved membership/conflicts, and arithmetic diagnostics.
- [Summary arithmetic investigation](./audit/summary-assertions-2026-09-10.md) —
  four-cycle groups, exact residuals, official-source checks, and unresolved financial scope.
- [Manual committee-summary Dagster gate](./audit/committee-summary-dagster-2026-09-10.md) —
  exact release/cycle handoff, partition-scoped checks, four-cycle replay, and no automation.
- [Published v4 release and summaries](./audit/fec-v4-publication-2026-09-10.md) —
  active source release, complete same-release summary facts, independent readback, and replay.
- [Verified v4 staging](./audit/fec-v4-staging-2026-09-09.md) — completed selected
  extraction, passing readback and integrity checks, storage result, and unchanged v3.
- [Verified v4 acquisition](./audit/fec-v4-acquisition-2026-09-09.md) — completed
  source capture, passing validation, unchanged v3, and the separate staging gate.
- [Fresh v4 preflight](./audit/fec-v4-preflight-2026-09-09.md) — fresh HEAD
  observations, unchanged candidate cost, fitting storage, and acquisition boundary.
- [Streaming source storage](./design/fec-streaming-storage.md) — implemented
  write-time caps, cumulative compressed workspace, safe stops, and retry.
- [Streaming storage gate](./audit/fec-streaming-storage-2026-09-09.md) — fixture
  failure/replay proof, bounded benchmark, and fitting read-only saved-plan review.
- [FEC storage review](./audit/fec-storage-review-2026-09-09.md) — inode-aware
  accounting, historical full-reserve scenario, and cleanup findings.
- [Committee-summary source](./design/committee-summary-source.md) — implemented
  Go reader/publisher, opt-in release v4, repeated-row safeguards, and scope limits.
- [Real v4 refresh plan](./audit/fec-v4-refresh-plan-2026-09-09.md) — metadata-only
  download cost, hard-link accounting defect, and whole-staging budget risk.
- [Committee-summary publication gate](./audit/committee-summary-publication-2026-09-08.md) —
  full stored-value readback and replay; fixture release integration, not live activation.
- [Committee-summary reader gate](./audit/committee-summary-reader-2026-09-08.md) —
  complete four-cycle Go verification, independent all-value comparison, and replay.
- [Committee-summary source review](./audit/committee-summary-source-2026-09-08.md) —
  complete four-cycle files, schema differences, typed exceptions, and arithmetic checks.
- [Report-linked earmark evidence](./design/receipt-report-association.md) —
  bounded same-report reference resolution, explicit role/ID gates, and no extra money.
- [Report-association gate](./audit/receipt-report-association-2026-09-08.md) —
  two whole-original-file comparisons, real links, amount mismatch, and replay.
- [Funding coverage and time](./design/funding-coverage-and-time.md) — implemented
  input-scope audit and cash/timing requirements before pooled attribution.
- [Funding coverage gate](./audit/funding-coverage-2026-09-08.md) — complete
  valid-fact summary scans, source exclusions, independent comparisons, and replay.
- [Receipt source-role policy](./design/receipt-source-evidence.md) — reviewed
  overlap routing and explicit conduit/reference uncertainty without donor-ID guesses.
- [Receipt source review](./audit/receipt-source-evidence-2026-09-08.md) — all
  overlap occurrences and a bounded original-filing earmark comparison.
- [Committee receipt inventory](./design/committee-funding-basis.md) — donor-bearing
  source lookup, disjoint receipt components, and exact-source upstream assessment;
  not a complete cash denominator or terminal allocation.
- [Receipt inventory gate](./audit/committee-funding-basis-2026-09-08.md) — complete
  2024 conservation, accepted-cohort equivalence, source pages, and candidate coverage.
- [Candidate upstream evidence](./design/candidate-upstream.md) — current
  priority: candidate-boundary accounting, selected committee ancestry, cycles,
  and explicit limits before terminal-dollar attribution.
- [Candidate upstream gate](./audit/candidate-upstream-2026-09-08.md) — real
  2024 candidate traces, exact membership, replay, and compact outputs.

- [`design/`](./design/) — Go rewrite product definition, legacy behavior
  specification, and future target-design documents.
- [`design/product-contract.md`](./design/product-contract.md) — product
  promise, evidence rules, required surfaces, scope, and success conditions.
- [`design/investigative-questions.md`](./design/investigative-questions.md) —
  concrete questions the investigative UI and API must answer.
- [`design/go-dagster-boundary.md`](./design/go-dagster-boundary.md) — accepted
  minimal-Python control-plane boundary for the Dagster and Go rebuild.
- [`design/first-vertical-slice.md`](./design/first-vertical-slice.md) — first
  source-to-API slice for preserved FEC receipts, Dagster partitions, Go
  operations, and targeted recomputation.
- [`design/first-vertical-slice-execution.md`](./design/first-vertical-slice-execution.md) —
  asset graph, Go command protocol, publication states, checks, and acceptance
  scenarios for the first slice.
- [`design/arango-candidate-receipt-projection.md`](./design/arango-candidate-receipt-projection.md)
  — implemented content-addressed ArangoDB projection boundary, physical
  model, source authority, query gates, and Schedule B/E exclusions.
- [`design/arango-independent-expenditure-projection.md`](./design/arango-independent-expenditure-projection.md)
  — implemented exact-lineage Schedule E support/opposition graph projection,
  physical model, query gate, replay boundary, and measured 2024 result.
- [`design/arango-receiver-reported-committee-flow-projection.md`](./design/arango-receiver-reported-committee-flow-projection.md)
  — implemented exact-lineage receiver-reported committee-flow graph,
  topology, bounded path queries, cycle gate, and measured 2024 result.
- [`design/evidence-model.md`](./design/evidence-model.md) — immutable source,
  occurrence, fact, assertion, relationship, and calculation identities.
- [`design/money-measures.md`](./design/money-measures.md) — common monetary
  observation, interval, uncertainty, conservation, and presentation contract.
- [`design/fec-money-semantics.md`](./design/fec-money-semantics.md) — FEC
  itemization, unitemized totals, estimates, valuations, debts, amendments,
  and independent time meanings.
- [`design/fec-flow-fact-requirements.md`](./design/fec-flow-fact-requirements.md)
  — accepted receipt, disbursement, candidate-context, independent-
  expenditure, reconciliation, and terminal-attribution fact boundaries.
- [`design/schedule-b-calculations.md`](./design/schedule-b-calculations.md) —
  accepted Schedule B non-memo reporting calculation, form-line roles,
  source evidence, and remaining economic-flow acceptance gates.
- [Committee-flow reconciliation](./design/committee-flow-reconciliation.md) —
  typed sender selection, fact-level A/B candidate components, exact lineage,
  separate ledger conservation, and graph exclusions.
- [Committee-flow evidence graph](./design/arango-committee-flow-evidence.md) —
  implemented separate observation ledgers and candidate documents, complete
  readback, source drilldown, and bounded single-ledger query gates.
- [Committee-flow orchestration](./design/committee-flow-orchestration.md) —
  exact manifest handoff, cycle-scoped checks, local schema validation, and
  thin Dagster publication/readiness/projection assets.
- [Committee-flow API](./design/committee-flow-api.md) — read-only Go serving,
  pinned projection identities, bounded path pagination, and source lookup.
- [Committee-flow API gate](./audit/committee-flow-api-2026-09-08.md) — real
  2024 HTTP pagination, ledger isolation, source verification, and timings.
- [Committee-flow Dagster gate](./audit/committee-flow-dagster-2026-09-08.md) —
  isolated real 2024 command execution, partitioned checks, and stable replay.
- [Committee-flow graph gate](./audit/arango-committee-flow-evidence-2026-09-08.md) —
  complete 2024 per-occurrence import, exact conservation, unresolved master
  coverage, query isolation, and idempotent replay.
- [Committee-flow publication](./design/committee-flow-publication.md) —
  immutable result publication, no-source-row-scan reuse, and exact readiness
  with same-cycle committee-master source-byte verification.
- [Committee-flow readiness contract](../contracts/bundles/fec/committee-flow-evidence/v1/) —
  deterministic observation-only bundle schema and immutable loading boundary.
- [Committee-flow publication gate](./audit/committee-flow-publication-2026-09-08.md) —
  exact 2024 result equivalence, no-source-row-scan reuse, source-ancestry
  rejection, and completed observation-readiness verification.
- [Committee-flow source review](./audit/committee-flow-source-review-2026-09-08.md) —
  complete conflict profiles, targeted source-row evidence, and graph implications.
- [`design/fec-source-contracts.md`](./design/fec-source-contracts.md) — initial
  bulk and deferred API/raw FEC source contracts, local artifact evidence,
  exact Schedule A/E processed-dump catalog evidence, and remaining acceptance
  gates.
- [`design/schedule-a-source-strategy.md`](./design/schedule-a-source-strategy.md)
  — accepted processed-dump acquisition, selective restore, validation,
  storage, and deferred raw-filing research.
- [`design/calculation-contracts.md`](./design/calculation-contracts.md) — first
  accepted Schedule A receipt and candidate-summary calculations.
- [`design/effective-efile-calculations.md`](./design/effective-efile-calculations.md)
  — deferred raw report-family selection, complete-amendment Schedule A
  projection, semantic row digests, and targeted change propagation research.
- [`design/schedule-a-reconciliation.md`](./design/schedule-a-reconciliation.md)
  — deferred exact raw-to-processed Schedule A identity, field comparison,
  revision-lag states, conservation, and targeted change propagation research.
- [`design/source-contracts.md`](./design/source-contracts.md) — common
  acquisition, physical-schema, normalization, drift, and publication contract
  for every Go source adapter.
- [`design/fec-release-strategy.md`](./design/fec-release-strategy.md) —
  accepted coordinated bulk-only FEC release invariants, Monday discovery,
  weekly acquisition, failure behavior, and retention.
- [`../contracts/`](../contracts/) — shared API schemas plus machine-readable
  source and calculation contracts, record/result schemas, canonical
  fixtures, release inventories, and manifests; initial source contracts
  remain draft while normalization and real-corpus publication gates are open.
- [`../contracts/releases/fec/v1/`](../contracts/releases/fec/v1/) — exact
  historical 21-source FEC inventory plus discovery, release-plan, and
  published-manifest schemas and planner fixtures.
- [`../contracts/releases/fec/v2/`](../contracts/releases/fec/v2/) — historical
  22-source inventory, including one all-history processed Schedule E
  relation, plus the versioned release boundary schemas.
- [`../contracts/releases/fec/v3/`](../contracts/releases/fec/v3/) — historical
  23-source inventory, adding processed Schedule B through four
  `archive_direct` cycle relations without adding staged COPY extracts.
- [`../contracts/releases/fec/v4/`](../contracts/releases/fec/v4/) — active
  27-source inventory, adding four whole committee-summary CSVs; manual Dagster
  handoff is verified, while discovery migration and weekly activation remain separate.
- [`../contracts/evidence/fec/schedule-a/v1/`](../contracts/evidence/fec/schedule-a/v1/)
  — Schedule A occurrence, issue, natural-key index, semantic-change, and
  occurrence-set schemas; their first JSON physical layout is now rejected
  after the 2024 corpus measurement.
- [`../contracts/evidence/fec/schedule-a/compact/v1/`](../contracts/evidence/fec/schedule-a/compact/v1/)
  — shipped dense-occurrence manifest, fixed-width partitioned key index,
  sparse exception, and actual inter-release delta contracts.
- [`../contracts/sources/fec/schedule-b/v1/`](../contracts/sources/fec/schedule-b/v1/)
  — processed Schedule B boundary with complete artifact/catalog
  identity, strict 2024 corpus verification, classic direction evidence, and
  a passing same-publisher-batch Schedule A alignment gate; v3 release
  membership and lossless fact publication implement its physical boundary.
- [`../contracts/audits/fec/schedule-ab-alignment/v1/`](../contracts/audits/fec/schedule-ab-alignment/v1/)
  — strict diagnostic result contract for exact, compatible, conflicting,
  ambiguous, and one-sided Schedule A/B candidates without money merging.
- [`../contracts/evidence/fec/classic/v1/`](../contracts/evidence/fec/classic/v1/)
  — shipped occurrence, issue, natural-key, change, and manifest schemas for
  the five classic FEC products in the coordinated release.
- [`../contracts/facts/fec/classic/v1/`](../contracts/facts/fec/classic/v1/)
  — shipped lossless normalized-fact and fact-set schemas for candidate,
  committee, linkage, and the two independent summary populations.
- [`../contracts/facts/fec/schedule-a/v1/`](../contracts/facts/fec/schedule-a/v1/)
  — fixture-tested lossless normalized receipt-fact and fact-set schemas for
  processed Schedule A; its full-row JSON representation is rejected for
  complete cycles.
- [`../contracts/facts/fec/schedule-a/columnar/v1/`](../contracts/facts/fec/schedule-a/columnar/v1/)
  — shipped 99-column Parquet physical schema and immutable columnar fact-set
  manifest for complete processed Schedule A cycles.
- [`../contracts/facts/fec/schedule-b/columnar/v1/`](../contracts/facts/fec/schedule-b/columnar/v1/)
  — shipped 98-column Parquet physical schema and immutable selected-cycle
  fact-set manifest streamed directly from the release-owned archive.
- [`../contracts/evidence/fec/schedule-e/v1/`](../contracts/evidence/fec/schedule-e/v1/)
  — shipped selected-cycle physical-row evidence over the all-history
  Schedule E relation.
- [`../contracts/facts/fec/schedule-e/v1/`](../contracts/facts/fec/schedule-e/v1/)
  — shipped lossless 80-field independent-expenditure facts with a narrow
  typed projection and no effective-record policy.
- [`../contracts/calculations/fec/effective-independent-expenditures/v1/`](../contracts/calculations/fec/effective-independent-expenditures/v1/)
  — accepted effective Schedule E predicate, sparse-exception, grouped-result,
  and immutable calculation-set contract.
- [`../contracts/calculations/fec/receiver-reported-committee-flows/v1/`](../contracts/calculations/fec/receiver-reported-committee-flows/v1/)
  — accepted exact-ID and receipt-role policy for conservative receiver-
  reported committee-flow components.
- [`../contracts/calculations/fec/processed-disbursement-reporting/v1/`](../contracts/calculations/fec/processed-disbursement-reporting/v1/)
  — accepted Schedule B reporting subtotals with separate scopes, explicit
  unresolved records, deterministic membership, and no graph eligibility.
- [`../contracts/calculations/fec/receiver-flow-committee-identity-coverage/v1/`](../contracts/calculations/fec/receiver-flow-committee-identity-coverage/v1/)
  — shipped exact-ID historical-registration and unresolved-reported-ID
  decisions with terminal-identity guards and immutable source lineage.
- [`../contracts/calculations/fec/independent-expenditure-candidate-resolution/v1/`](../contracts/calculations/fec/independent-expenditure-candidate-resolution/v1/)
  — accepted per-fact Schedule E candidate-reference decisions, conservative
  exact-context resolution, unverified-ID preservation, and immutable lineage.
- [`../contracts/calculations/fec/resolved-independent-expenditures/v1/`](../contracts/calculations/fec/resolved-independent-expenditures/v1/)
  — accepted resolved spender-candidate-stance groups, quality-state
  components, sparse unprojectable exceptions, and exact conservation.
- [`../contracts/bundles/fec/candidate-itemized-individual-receipts/v1/`](../contracts/bundles/fec/candidate-itemized-individual-receipts/v1/)
  — shipped exact-input readiness bundle for the compact candidate-receipt
  calculation.
- [`../contracts/bundles/fec/independent-expenditure-projection/v1/`](../contracts/bundles/fec/independent-expenditure-projection/v1/)
  — shipped exact-input readiness bundle for the effective Schedule E graph
  projection.
- [`../contracts/bundles/fec/resolved-independent-expenditure-projection/v1/`](../contracts/bundles/fec/resolved-independent-expenditure-projection/v1/)
  — active resolved graph-readiness bundle with exact candidate-resolution
  ancestry and candidate-master identity.
- [`../contracts/bundles/fec/receiver-reported-committee-flow-projection/v1/`](../contracts/bundles/fec/receiver-reported-committee-flow-projection/v1/)
  — shipped exact receiver-flow calculation and same-release committee-master
  readiness boundary.
- [`../contracts/bundles/fec/receiver-reported-committee-flow-projection/v2/`](../contracts/bundles/fec/receiver-reported-committee-flow-projection/v2/)
  — shipped additive identity-aware readiness bundle over the immutable v1
  flow boundary.
- [`../contracts/projections/arango/independent-expenditures/v2/`](../contracts/projections/arango/independent-expenditures/v2/)
  — active resolved outside-spending graph result, lineage, unresolved
  coverage, readback, storage, and query contract.
- [`../contracts/projections/arango/receiver-reported-committee-flows/v1/`](../contracts/projections/arango/receiver-reported-committee-flows/v1/)
  — shipped committee-flow graph result, exact role conservation, topology,
  storage, missing-master coverage, and query contract.
- [`../contracts/projections/arango/receiver-reported-committee-flows/v2/`](../contracts/projections/arango/receiver-reported-committee-flows/v2/)
  — active identity-aware graph result with distinct current, historical,
  alternate-release, and unresolved states plus terminal-identity exclusion.
- [`../contracts/calculations/fec/candidate-itemized-individual-receipts/compact/v1/`](../contracts/calculations/fec/candidate-itemized-individual-receipts/compact/v1/)
  — accepted compact membership-exception and calculation-set schemas over an
  exact columnar fact index and versioned predicate.
- [`../contracts/calculations/fec/candidate-itemized-individual-receipts/v1/`](../contracts/calculations/fec/candidate-itemized-individual-receipts/v1/)
  — fixture-tested receipt-decision, candidate-component, independent-summary
  reconciliation, and immutable calculation-set contracts.
- [`design/source-catalog.md`](./design/source-catalog.md) — target FEC,
  lobbying, congressional, entity-resolution, and later-source inventory with
  implementation order.
- [`design/legislative-influence.md`](./design/legislative-influence.md) —
  provisional later-phase direction for campaign money, lobbying, bills,
  committees, votes, and versioned beneficiary inference.
- [`design/lobbying-source-ingestion.md`](./design/lobbying-source-ingestion.md)
  — official LDA source status and deferred API-authority proposal for seed,
  refresh, reconciliation, amendments, documents, and Dagster partitions.
- [`design/lda-source-schema.md`](./design/lda-source-schema.md) — audited LDA
  source grain, field groups, time meanings, amendment evidence, and observed
  OpenAPI mismatches.
- [`design/legacy-functional-spec/system-boundaries-and-sources.md`](./design/legacy-functional-spec/system-boundaries-and-sources.md) —
  observed source inventory, snapshots, ingestion boundary, and failure model.
- [`design/legacy-functional-spec/data-grain-and-lineage.md`](./design/legacy-functional-spec/data-grain-and-lineage.md) —
  current preservation, aggregation, overwrite, filtering, and lineage map.
- [`design/legacy-functional-spec/cycle-semantics.md`](./design/legacy-functional-spec/cycle-semantics.md) —
  current cycle assignment, windowing, aggregation, temporal leakage, and
  recomputation behavior.

## Deferred recovery engineering

These tools and historical results do not make recovery a prerequisite for the
node/edge review. Do not resume implementation or data copying by default.

- [Removed recovery-copy feature](./design/funding-recovery-retention.md) — historical
  fixture design, approved removal scope and retained source archive.
- [Fact-start recovery checkpoint](./design/funding-recovery-checkpoint.md) —
  implemented read-only planner/verifier; deferred reconstruction requirements.
- [Recovery file verification gate](./audit/funding-recovery-files-2026-09-15.md) —
  full selected-byte checks, exact fresh replay and live byte-ceiling rejection.
- [Recovery planner gate](./audit/funding-recovery-plan-2026-09-15.md) — retained-generation
  dependency recipe, unchanged historical inventory, offline builds and exact replay.
- [Recovery dependency inventory](./design/funding-recovery-inventory.md) — typed,
  read-only source-to-generation dependency inspection and explicit verification levels.
- [Recovery inventory audit](./audit/funding-recovery-inventory-2026-09-15.md) —
  retained-generation file checks, replay and the missing historical staging record.
- [Historical staging review](./audit/release-stage-evidence-review-2026-09-15.md) —
  documented overwrite, repeatable descriptor comparison and explicit byte-history gap.

## Current system

- [`architecture.md`](./architecture.md) — high-level architecture stub for
  the Python and Dagster implementation.
- [`pipeline.md`](./pipeline.md) — current Dagster assets, graph collections,
  and data flow.
- [`funding-channels.md`](./funding-channels.md) — current five-channel model
  and proportional terminal-attribution algorithm.
- [`fec-data.md`](./fec-data.md) — FEC bulk datasets, field layouts, and parser
  references.
- [`corporate-resolution.md`](./corporate-resolution.md) — current employer
  normalization and external-entity resolution pipeline.
- [`data-quality.md`](./data-quality.md) — current donor-detail coverage fields
  and honest-rendering requirements.
- [`validation.md`](./validation.md) — current validation gates, baselines, and
  known limitations.
- [`lobbying-integration.md`](./lobbying-integration.md) — unimplemented legacy
  design for federal lobbying data; redesign evidence only.

## Operation and project state

- [`go-rewrite.md`](./go-rewrite.md) — as-built Go module, commands, source
  adapters, verification state, and explicit unimplemented boundaries.
- [`operations.md`](./operations.md) — current operational runbook.
- [`storage.md`](./storage.md) — persistent raw, dump, and cache layout.
- [`decisions.md`](./decisions.md) — append-only decision log.
- [`todo.md`](./todo.md) — active Go rewrite queue followed by the frozen
  Python-era backlog awaiting explicit redesign dispositions.
- [`plan.md`](./plan.md) — historical Python professionalization plan.
- [`audit/`](./audit/) — dated baselines and audits from the Python system.
- [`audit/schedule-e-candidate-reference-integrity-2026-08-31.md`](./audit/schedule-e-candidate-reference-integrity-2026-08-31.md)
  — 2024 Schedule E candidate-reference failures, amount exposure, history
  evidence, and the required per-fact resolution boundary.
- [`audit/independent-expenditure-candidate-resolution-2026-08-31.md`](./audit/independent-expenditure-candidate-resolution-2026-08-31.md)
  — complete 2024 per-fact candidate-resolution states, exact money and
  membership conservation, rejected first rule, and immutable replay evidence.
- [`audit/resolved-independent-expenditures-2026-08-31.md`](./audit/resolved-independent-expenditures-2026-08-31.md)
  — complete 2024 resolved aggregate, readiness bundle, v2 Arango graph,
  exact coverage, readback, master completeness, latency, and replay evidence.
- [`audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md`](./audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md)
  — four-cycle resolved outside-spending gate, executable policy-contract
  conformance, exact coverage, and inventory rollover invariants.
- [`audit/source-boundary-2026-08-27.md`](./audit/source-boundary-2026-08-27.md)
  — local raw/header/parser audit and current official-source findings for the
  Go redesign.
- [`audit/lda-api-printable-comparison-2026-08-27.md`](./audit/lda-api-printable-comparison-2026-08-27.md)
  — LDA API-versus-form comparison and its consequences for historical names,
  amount bands, form grain, and amendment handling.
- [`audit/schedule-a-classic-overlap-2026-08-28.md`](./audit/schedule-a-classic-overlap-2026-08-28.md)
  — complete processed Schedule A comparison with the 2024 classic `indiv`
  and `oth` products, including accepted-calculation coverage.
- [`audit/direct-receipt-probe-2026-08-30.md`](./audit/direct-receipt-probe-2026-08-30.md)
  — complete 2024 direct-source candidate receipt calculation, runtime and
  storage measurements, conservation results, and JSON fact-layout verdict.
- [`audit/schedule-a-layout-benchmark-2026-08-30.md`](./audit/schedule-a-layout-benchmark-2026-08-30.md)
  — one-million and ten-million-row Schedule A zstd-versus-Parquet benchmark,
  semantic equivalence, projected-scan result, and full-corpus gate.
- [`audit/schedule-a-columnar-publication-2026-08-31.md`](./audit/schedule-a-columnar-publication-2026-08-31.md)
  — complete 2024 Parquet fact publication, integrity, independent DuckDB,
  storage, and throughput gate.
- [`audit/schedule-a-compact-occurrence-publication-2026-08-31.md`](./audit/schedule-a-compact-occurrence-publication-2026-08-31.md)
  — complete 2024 compact occurrence/index publication, integrity,
  idempotence, storage, and throughput gate.
- [`audit/compact-receipt-calculation-publication-2026-08-31.md`](./audit/compact-receipt-calculation-publication-2026-08-31.md)
  — exact complete-corpus equivalence, sparse membership, result identity,
  storage, runtime, and replay evidence for the compact receipt calculation.
- [`audit/candidate-receipt-fact-bundle-2026-08-31.md`](./audit/candidate-receipt-fact-bundle-2026-08-31.md)
  — complete 2024 same-release readiness, backing integrity, idempotence, and
  bundle-fed compact-calculation replay evidence.
- [`audit/arango-candidate-receipt-projection-2026-08-31.md`](./audit/arango-candidate-receipt-projection-2026-08-31.md)
  — complete 2024 isolated projection counts, query latency, master-fact
  coverage gap, and idempotent replay evidence.
- [`audit/receipt-master-gaps-2026-08-31.md`](./audit/receipt-master-gaps-2026-08-31.md)
  — all 299 receipt-projection placeholders traced to exact linkage, summary,
  same-cycle comparison, and cross-cycle master evidence.
- [`audit/receiver-reported-committee-flow-cohort-2026-08-31.md`](./audit/receiver-reported-committee-flow-cohort-2026-08-31.md)
  — complete 2024 Schedule A flow-candidate scan, rejected broad rule,
  accepted receipt roles, unresolved exposure, and next publication gate.
- [`audit/receiver-reported-committee-flow-publication-2026-09-01.md`](./audit/receiver-reported-committee-flow-publication-2026-09-01.md)
  — immutable complete-2024 calculation, exact conservation, artifacts,
  Dagster boundary, and byte-identical replay evidence.
- [`audit/arango-receiver-reported-committee-flows-2026-09-01.md`](./audit/arango-receiver-reported-committee-flows-2026-09-01.md)
  — complete 2024 readiness bundle, exact graph readback, topology, bounded
  path and cycle queries, storage, missing-master coverage, and replay.
- [`audit/receiver-flow-master-gaps-2026-09-01.md`](./audit/receiver-flow-master-gaps-2026-09-01.md)
  — all 707 v1 receiver-flow placeholders classified against official
  1980–2026 committee masters, including exact money and optional Schedule A
  source-row evidence for the 32 unmatched reported IDs.
- [`audit/arango-receiver-flow-identity-coverage-2026-09-01.md`](./audit/arango-receiver-flow-identity-coverage-2026-09-01.md)
  — immutable identity calculation, v2 readiness bundle, distinct vertex
  states, exact ArangoDB readback, terminal guard, and query gate.
- [`audit/schedule-b-source-and-classic-overlap-2026-09-01.md`](./audit/schedule-b-source-and-classic-overlap-2026-09-01.md)
  — complete Schedule B source, parser, endpoint/amount, runtime, and storage
  gate and the source-alignment blocker that remained at that observation.
- [`audit/schedule-ab-alignment-2026-09-04.md`](./audit/schedule-ab-alignment-2026-09-04.md)
  — complete same-publisher-batch Schedule A/B candidate comparison, source-
  adoption disposition, exact conservation, and next graph boundary.
- [`audit/schedule-b-columnar-publication-2026-09-04.md`](./audit/schedule-b-columnar-publication-2026-09-04.md)
  — coordinated release-v3 evidence and the complete lossless 2024 Schedule B
  Parquet publication, source-stable adoption, and replay gate.
- [`audit/schedule-b-semantics-2026-09-08.md`](./audit/schedule-b-semantics-2026-09-08.md)
  — complete 2024 action/memo and form-line profile, exact conservation,
  recipient self-reference evidence, and remaining sender-flow policy gates.
- [`audit/schedule-b-reporting-calculation-2026-09-08.md`](./audit/schedule-b-reporting-calculation-2026-09-08.md)
  — complete reporting-calculation gate, reviewed source records, distinct
  reporting scopes, and byte-identical cross-worker replay.
- [`audit/python-fec-arango-state-2026-08-31.md`](./audit/python-fec-arango-state-2026-08-31.md)
  — legacy Python FEC source use, surviving ArangoDB state, and the
  classic-product versus processed Schedule B/E decision gate.
- [`audit/fec-classic-flow-and-schedule-e-2026-08-31.md`](./audit/fec-classic-flow-and-schedule-e-2026-08-31.md)
  — complete 2024 classic-flow scan plus real 2024/2026 Schedule E and `pas2`
  identity, freshness, precision, coverage, and source-selection evidence.
- [`audit/schedule-e-source-contract-2026-08-31.md`](./audit/schedule-e-source-contract-2026-08-31.md)
  — exact 80-column Schedule E contract, strict Go parser, complete 548,318-row
  replay, anomaly profile, and release-v2 boundary.
- [`audit/schedule-e-v2-publication-2026-08-31.md`](./audit/schedule-e-v2-publication-2026-08-31.md)
  — active coordinated v2 release, verified lossless cycle publications, and
  direct-dump versus restored-table COPY reconciliation.
- [`audit/effective-independent-expenditures-2026-08-31.md`](./audit/effective-independent-expenditures-2026-08-31.md)
  — four-cycle effective outside-spending publication, notice-source
  separation, exact conservation, and idempotent replay evidence.
- [`audit/arango-independent-expenditure-projection-2026-08-31.md`](./audit/arango-independent-expenditure-projection-2026-08-31.md)
  — complete 2024 spender-to-candidate graph projection, exact readback,
  storage, master coverage, query latency, and idempotent replay evidence.
- [`audit/independent-expenditure-projection-bundle-2026-08-31.md`](./audit/independent-expenditure-projection-bundle-2026-08-31.md)
  — complete 2024 same-release projection readiness, immutable replay, eager
  Dagster mapping, and bundle-fed graph reuse evidence.

## Supporting infrastructure

- [`second-brain.md`](./second-brain.md) — project knowledge-stack design.
- [`setup-currency.md`](./setup-currency.md) — setup and dependency currency
  tracking.
