# Legal Tender — Agent Context

Current priority, 2026-09-18: the [automated affiliation report](docs/design/affiliation-enrichment.md)
now connects verified FEC appearances to the existing Wikipedia/Wikidata discovery,
source extraction and name/role/date comparison in one command. It replays retained
captures and selects additional appearances in code. The live three-appearance
sample completed all requests but supplied no corresponding names or person/employer
links. The [supplementary-source investigation](docs/audit/supplementary-affiliation-sources-2026-09-16.md)
now retains company role/alias prose, historical licensing evidence and missing-employer
controls on those same cases. Four HTML bodies pass the unchanged lexical reader;
the PDF remains reviewed evidence. URL selection was manual. The opt-in
[prose syntax prototype](docs/design/prose-relationship-prototype.md) now extracts
source-cited role/name-form candidates from seven retained HTML bodies. Its bounded
grammar keeps unknown dates and unsupported layouts explicit; it verifies no
assertion or donor identity. The
[out-of-development evaluation](docs/audit/prose-extraction-evaluation-2026-09-16.md)
now finds only one of six reviewed role witnesses across five readable bodies;
a sixth selected page failed retrieval. Keep the unchanged grammar as a baseline,
not the general role extractor. The user-approved
[local-model comparison](docs/audit/local-prose-model-comparison-2026-09-16.md) now
retains fourteen Gemma requests/responses and offline replay: nine literal-citation
passes, five rejected answers, with semantic and structural gaps still explicit.
The [stronger-model comparison](docs/audit/stronger-prose-model-comparison-2026-09-16.md)
now retains the same fourteen cases with GPT-OSS: eleven literal passes and three
rejections. Better abstention and some role handling coexist with missing quotation
context and wrong field meaning; this is not extraction accuracy or acceptance.
The [code-owned evidence attachment trial](docs/audit/prose-evidence-attachment-2026-09-16.md)
now attaches original entries and complete supplied context in Go. Six known real
cases plus nine fresh controls retain eleven literal passes and four rejections;
pronoun, role-scope and retraction failures remain explicit. No quotes are rewritten,
fields repaired or invalid answers partially accepted. It is a selected-excerpt
research test, not whole-page automation or a production model client. The
[mention/interpretation boundary](docs/design/prose-mention-interpretation.md) now
passes an offline contract proof using reviewed annotations over six retained
synthetic cases. It preserves literal spans, separate normalized proposals, explicit
bindings and correction/conflict links. Go validates and applies supplied semantics;
it does not prove them. The [fresh automatic-producer trial](docs/audit/prose-interpretation-model-2026-09-17.md)
now retains nine GPT-OSS outcomes: five literal/reference passes and four rejections,
including all three new real-source cases. Repeated occurrences, a rewritten clause,
changed whitespace, insufficient selected evidence and a token-budget failure remain
explicit. Some synthetic binding/context behavior is useful, not acceptance evidence.
The [source-owned citation proof](docs/design/prose-citation-selection.md) now
reconstructs exact text from supplied token IDs and attaches complete selected entries.
Its saved-query diagnostic preserves ambiguous occurrences and rejects altered text;
independently reviewed selections are not repaired model answers. Earlier trial
outcomes remain unchanged. The [token-ID model trial](docs/audit/prose-token-selection-model-2026-09-17.md)
now retains nine known-case attempts: four structural passes, four reference failures
and one capacity failure. All returned ranges are valid source spans, but wrong
endpoints and unresolved pronouns remain. Paired input tokens increase about 50%
without an established reliability gain. Keep this as research evidence, not a
production upgrade. The subsequent offline contract now derives required references
from explicit clause/subject/organization selections in Go. Extra supporting
references remain explicit; the supplied proposal stays separate from the derived
review. Tests preserve ambiguity, corrections, failures and false approval flags.
Old model captures and producers are unchanged. The
[fresh derived-reference trial](docs/audit/prose-derived-reference-model-2026-09-17.md)
now retains six attempts: four structural passes, one truncated answer and one
joint-activity contract rejection. Passing answers still contain wrong endpoints
and an instruction-derived unclear role. CFO ambiguity was preserved in the rejected
joint-activity answer; do not confuse that contract limit with a wrong identity choice.
The [isolated name-selection trial](docs/audit/prose-name-selection-model-2026-09-17.md)
now retains eight names-only attempts. Exact comparison with pre-run labels matches
22/22 regression spellings and 13/15 fresh spellings, with two extra fresh selections.
An omitted surname, legal-suffix punctuation and institution/board scope remain
explicit; no source text or old answer is repaired. This does not extract roles or
merge spellings into identities. The [role-binding follow-up](docs/audit/prose-role-binding-model-2026-09-17.md)
now retains ten second-stage answers using uncorrected candidates and original
context, including two fresh controls with automatic first-stage names. All pass
structural checks. Fresh ambiguity, hypothetical status and missing endpoints are
preserved, but direct/coreference labels, selected context, role grouping and
institutional scope still have errors. The anonymous-role control differs from its
frozen empty-output expectation. The [whole-entry grounding follow-up](docs/audit/prose-role-grounding-model-2026-09-17.md)
now adds literal endpoint surfaces and complete statement-entry citations, and
clarifies anonymous-role scope without rewriting the old expectation. Five fresh
two-stage cases retain four structural passes and one reserved-ID rejection. Separate
roles and pronouns work in one case; a surname-as-direct ambiguity failure and an
omitted retraction link remain. The [offline referent/target contract](docs/design/prose-referents-and-corrections.md)
now requires separate unverified referent states for each role and explicit target
states for every supplied correction. Exact spelling never populates referent choices;
only explicit proposed correction targets derive retractions. Synthetic tests and
separately labeled reviewed overlays on the two retained counterexamples pass without
changing model outputs or their provenance. No model call or deployment changed.
The [complete referent/target comparison](docs/audit/prose-referent-model-comparison-2026-09-19.md)
now retains exact Gemma 31B and GPT-OSS captures for eight fixed cases. A recorded
900-second client deadline replaces false research-harness timeouts. Both models
return all eight answers and pass structure on four; only three and two synthetic
answers, respectively, also meet pre-run semantic checks. Neither has a usable
real-source answer. Preserve the representation and strict validator, but reject the
combined grounding-plus-assessment producer. Next test only referent/target assessment
over immutable validated grounding, with fresh cases before any quality claim. Do not
repair saved outputs, add title/person-specific exceptions or promote the producer.
The [grounding-bound follow-up](docs/audit/prose-referent-assessment-model-2026-09-19.md)
now supplies exact validated grounding and accepts only referent/target arrays. Ten
requests per model all return; each model passes nine structural joins and six
semantic reviews, including one of three retained real-source cases. Task separation
is the retained research shape, but short person and organization mentions are still
confused with full antecedents. The
[offline occurrence/group contract](docs/design/prose-referents-and-corrections.md#offline-occurrence-group-and-canonical-candidate-boundary--2026-09-19)
now keeps literal occurrences immutable, points role referents to explicit unverified
mention groups and accepts source-pinned canonical candidates only through a separate
digest-bound join. Ambiguous alternatives never merge. Generic validation has no
named-person/company rules; no additional inference or production behavior changed.
This is not an accepted production architecture. Valid spans are not meaning.
Do not approve identities or start a source pipeline. No new model deployment, crawler,
runtime source adapter or publisher was added. Autonomous page selection remains
a separate gap. Model calls stay opt-in; ordinary tests are offline.
The [v2 classifier](docs/audit/person-identity-evidence-v2-2026-09-16.md) remains a
manual-input review experiment. Automatic reports use source-native names, aliases
and date precision from existing parsed-source rules, with all candidates preserved.
No production identity, graph or monetary rule changed.
The [affiliation evidence evaluator](docs/design/person-affiliation-acceptance.md#implemented-scope)
now implements candidate correspondence, source-qualified role timelines, explicit
denial/conflict states and opt-in continuity hypotheses in Go. Its synthetic and
retained-source tests preserve source grain and keep every approval flag false.
It is not donor identity acceptance: v2 classifies evidence and abstention but does
not select a person. Existing screening, graph and money
behavior are unchanged. Do not add a source pipeline or publisher automatically.
The [dated evidence check](docs/audit/dated-person-role-evidence-2026-09-16.md)
now retains historical private-company role context and tests separate document,
publication, modification and role-point dates against the original FEC appearances.
Accepted corporate prose interpretation remains reviewed. The automated report uses structured
Wikidata claims and does not supply independent identity acceptance. Do not accept
affiliations automatically. The user-approved
[binding diagnostic](docs/design/person-binding-diagnostic.md) now tests FEC
appearances against parsed role evidence, separating name correspondence, employer
context and receipt-date comparison at source precision. It accepts no identities;
private-company evidence now has dated examples, while accepted donor binding and
transaction-time support remain gaps. The
[read-only relationship query](docs/design/relationship-query.md) now makes retained
structured roles and parent/child claims queryable by source entity, with original
direction, time precision and evidence. The [exploration](docs/design/relationship-exploration.md)
and comparison remain the context for later decisions. This is not permission to
implement a shared graph, add crawlers/LLMs, accept donor identities or change money
rules. Production FEC-person and dated-affiliation acceptance remain separate work.
Company-page syntax candidates are opt-in; accepted role extraction remains open.
Use ordinary development builds/tests. Keep source
provenance and dependency locks; recovery and exact-build acceptance work are parked,
not prerequisites for exploration. The linked note records the verified partial cleanup.

Existing exploration tooling: the user previously approved bounded
[organization candidate capture and replay](docs/design/organization-resolution.md)
after reviewing connection rules. The Go slice now selects source-backed query
text, captures Wikipedia/Wikidata evidence and replays versioned name proposals offline.
It does not merge identities or publish edges. The
[offline evaluation corpus](docs/design/organization-evaluation.md) now retains
primary-source review evidence, tests source/retrieval/matching outcomes separately,
and preserves the v1 baseline. Opt-in v2 adds explained letter/number-boundary and
legal-suffix proposals, recovering both reviewed matching misses without proposing
annotated counterexamples. This is a small diagnostic result, not identity acceptance.
The [registry corroboration slice](docs/design/organization-corroboration.md)
now derives exact-LEI requests, retains GLEIF responses and assesses them offline.
Its real fixture and harder-case tests pass without approving any FEC identity.
The [independent SEC issuer path](docs/design/organization-issuer-discovery.md) now
implements bulk capture, strict parsing and offline FEC-name-to-CIK proposals without
requiring Wikipedia or LEI success. Its live fetch and fresh CLI replay now pass,
with one directory candidate and no identity approval on the retained query set.
The approved request contact stays in `.env`; contact-bearing capture metadata stays
private, and only the public body is a shared fixture. The
[filed registrant check](docs/design/organization-filed-identity.md) now captures an
explicit SEC filing and replays tagged name/CIK comparisons with byte-level evidence.
Its real capture and fresh replay pass without identity approval. Filing selection
is explicit, not an implemented automated latest/amendment selector. The user then
approved [person-affiliation rule testing](docs/design/person-affiliation-testing.md).
A pure Go screening evaluator now passes synthetic namesake, role, date and
multi-affiliation tests; the funding-path API rejects affiliation families.
The [retained source-backed corpus](docs/design/person-affiliation-corpus.md) now
replays real as-filed appearances and reviewed role annotations with exact byte
binding. Name/employer and historical-date gaps remain unresolved. The
[structured-role extractor](docs/design/wikidata-role-extraction.md) now reads explicit
Wikidata relationships automatically from pinned bodies, retaining source direction,
ranks, raw statements and temporal precision. After three background-mode `maxlag`
responses, one user-approved interactive request succeeded under Wikimedia's
documented exception. All 31 selected statements replay; the bounded comparison
finds historical-role agreement and explicit title/discovery gaps. Background
request policy, reviewed annotations and screening rules are unchanged. The
[exploratory affiliation discovery slice](docs/design/affiliation-discovery.md) now
generates source-backed name/employer searches and follows Wikipedia-linked QIDs.
The bounded live capture and offline replay pass, preserving rivals and retrieval
gaps. This is still exploration, not accepted person resolution. Recursive inverse
discovery, the screening bridge, identity-edge approval and financial allocation
are not added. Opt-in query variants and offline candidate relevance now pass live
capture/replay and regression tests. The variants did not recover the missing
companies; real namesakes and non-company aliases remain explicit. Default v1 and
the original corpus are unchanged. The [independent registry-name path](docs/design/organization-registry-discovery.md)
now derives employer-only GLEIF searches without a preexisting QID/LEI. Its live
capture and offline replay pass, retaining two corresponding-name LEIs as ambiguous.
The [company-page reader](docs/design/company-page-evidence.md) now extracts source
text, metadata and JSON-LD from retained HTML with exact byte locations. All three
pages pass fresh offline replay. It does not interpret prose roles or approve donor
identities. The separate opt-in prototype only proposes syntax candidates. Page
selection, a verified role bridge and independent identity/role bindings
remain possible follow-ups after the relationship exploration; broader name matches
alone do not close the gaps.
Do not silently expand into full-corpus
person resolution or terminal allocation. Keep the [implemented node/edge assumptions](docs/design/pre-attribution-review.md)
explicit before selecting terminal definitions or dollar allocation. The
2026-09-15 scope correction separates that request from the assistant-added recovery
project. Do not resume recovery infrastructure, another large scan, a GUI or a new
cycle as the automatic next step. Do not silently change financial policies while
reviewing them.

The [fact-start recovery work](docs/design/funding-recovery-checkpoint.md) is deferred,
not a prerequisite for reviewing graph semantics. Keep the existing read-only
inventory/planner/verifier, reproducible builds and historical audit evidence.
The unused fixture-scale [input copying feature](docs/design/funding-recovery-retention.md)
and its dedicated test harness were removed with user approval on 2026-09-15.
Their source archive and historical audits remain; no real generation was captured.
The missing exact historical
v3 staging record remains explicit; checksum verification is not recovery acceptance.

The [shared-reference profile](docs/design/shared-reference-profile.md) explains
the one-to-one conduit rule's shared-degree exclusions. The
[automatic source reviewer and additive group rule](docs/design/shared-reference-group-rule.md)
now recover complete immediate neighborhoods and original rows for all selected
profile witnesses. The [retained source review](docs/audit/shared-reference-source-review-2026-09-14.md)
confirms supported complete groups and explicit conflicting/unsupported shapes.
The [complete compact publication](docs/audit/shared-conduit-publication-2026-09-14.md)
now passes 2024 conservation and varied-layout replay. The
[isolated graph extension](docs/design/shared-conduit-generation.md) now passes
real import, complete field readback and byte-identical fresh read-only replay.
Explicit exit markers and retained checksums pass. Typed path and neighborhood
readers are now extension-aware, with a separate shared family, exact old/new
decision evidence and source drilldown. The [real query gate and fresh replay](docs/audit/shared-conduit-queries-2026-09-14.md)
pass byte-for-byte with unchanged input checksums and explicit success markers.
The [date-window extension and fresh replay](docs/audit/shared-conduit-windows-2026-09-14.md)
now pass, preserving original receipt dates, unknown-time states and outer-generation
identities. The [Go release build gate](docs/audit/go-build-2026-09-14.md) now passes
clean offline builds and archive-only reconstruction under a digest-pinned compiler.
Use ordinary `make build`/`make check` for development. The
[release build command](docs/go-build.md) remains opt-in for explicitly scoped
release acceptance, not a gate on ongoing exploration or prototypes; do not replace
historical binary identities. Real multi-cycle capacity and the
complete data/runtime recovery checkpoint remain open. Do not copy the full receipt graph, promote compatible
rows wholesale or fetch another cycle. Existing graph publications, money
selection, terminal and identity rules remain unchanged.

Cycles are [reusable partitions, not universal calculation boundaries](docs/design/cycle-calculation-windows.md).
Continue the 2024 evidence pipeline without requiring a full four-cycle rollout
first; later consumers explicitly pin any cross-cycle evidence they need.
This is a contract clarification, not implemented general cross-cycle analytics.
The [publication-independent committee path reader](docs/design/funding-window-reader.md)
now implements selected A/B paths with reported-date windows, exact source routing
and historical facets. Existing cycle checks remain. The
[2024 gate and fresh replay](docs/audit/funding-window-2026-09-13.md) pass;
real multi-cycle acceptance remains separate work. The additive
`inspect-funding-window-connections` now supports generation-qualified receipt
entries and candidate authorization context, preserving unknown validity and
distinct historical assertions. Its full-chain synthetic loader/CLI and
regression/race/static gates pass. Its [retained 2024 connection gate](docs/audit/funding-window-connections-2026-09-13.md)
also passes all automatic cases and byte-identical fresh-process replay.
Date-selected Schedule E source-member endings now pass the synthetic full-chain
loader/CLI gate and the [retained 2024 spending gate](docs/audit/funding-window-spending-2026-09-14.md),
including complete source/date census and byte-identical fresh-process replay.
They require an explicit expenditure/dissemination date field, replay existing
source decisions, conserve every parent aggregate and preserve exclusions.
Real second-cycle acceptance and interactive-serving performance remain open.
No terminal policy or allocation is added.
`make test-window-integration` now builds synthetic two-cycle publications through
the normal publishers and tests the real loader/CLI in disposable ArangoDB,
including wrong-source/graph/completion rejection. It does not use development
data or replace the real-corpus and memory-capacity gates.

The [reported identity assertion view](docs/design/reported-identity-assertions.md)
now exposes exact receipt identity/employer/occupation text and committee
organization fields without copying the corpus or resolving identities.
The [live gate](docs/audit/reported-identity-assertions-2026-09-13.md) owns
acceptance status. Before selecting terminal definitions or dollar allocation,
complete the user-requested [interpretation review](docs/design/pre-attribution-review.md).
That review is not yet accepted; Arango is a derived projection, not raw evidence.
The [first code-backed review](docs/audit/pre-attribution-review-2026-09-14.md)
and [selected evidence pins](docs/audit/pre-attribution-checkpoint-2026-09-14.md)
are prepared. Candidate-resolution characterization tests and selected artifact
checks pass. User acceptance remains open; complete data/runtime recovery is
separate deferred operational work. No runtime interpretation changed during that review.

The [receipt-role profiler](docs/design/terminal-receipt-roles.md) now joins
complete compact participant evidence and exact reported source-ID master facts
to the selected committee boundary assessment. It preserves appearances,
conflicts, nulls and occurrence grain; person/corporation resolution and dollar
allocation remain unperformed. Regression/race/static checks and the
[live gate](docs/audit/terminal-receipt-roles-2026-09-13.md) pass, including the
complete corpus and byte-identical worker-varied fresh replay.
The separate reported identity view extends this evidence; person/organization
resolution remains next and no terminal definition is adopted.

The [terminal-source assessment](docs/design/terminal-source-assessment.md) now
compares provisional frontier, same-cycle-master frontier and root-SCC hypotheses
over complete selected A/B endpoint populations. It preserves ledger absence,
identity gaps and cyclic components without terminal classification or dollars.
Regression/race/static checks and the [live gate](docs/audit/terminal-source-assessment-2026-09-13.md)
pass, including byte-identical fresh replay. The separate receipt-role profiler
extends this evidence; person/organization resolution remains unperformed.

The [typed path reader](docs/design/funding-paths.md) now follows exact receipt or
conduit entries through one selected A/B ledger and an optional authorized or
support/opposition candidate ending. It preserves source evidence, identity gaps
and explicit search limits; no path-money or terminal classification is inferred.
Full regression/race/static checks and the [live gate](docs/audit/funding-paths-2026-09-13.md)
pass, including both ledgers, source entries, candidate endings and byte-identical
fresh-process replay. Terminal-source definitions and dollar allocation remain open.

The [generation-bound neighborhood reader](docs/design/funding-neighborhoods.md)
now queries typed one-hop receipt/A/B/E relationships with source-backed evidence,
explicit identity facets and scoped pagination. Full Go/race/static checks and
the [real gate](docs/audit/funding-neighborhoods-2026-09-13.md) pass, including
byte-identical fresh replay of data-selected family and endpoint witnesses.
The typed path reader extends these queries; terminal allocation and serving remain separate.

The [typed generation verifier](docs/design/funding-evidence-generation.md) now
binds receipt, selected A/B and resolved Schedule E graph families read-only,
preserving source ancestry and explicit overlap/coverage. Go/race/static checks
and the [real gate](docs/audit/funding-evidence-generation-2026-09-13.md) pass,
including byte-identical fresh replay and unchanged graph completion boundaries.
The neighborhood reader consumes this generation; weekly publication remains separate.

The [receipt-to-candidate consumer](docs/design/receipt-candidate-connection.md)
now joins exact receipt evidence to existing receiver-chain and authorization
graphs read-only. Cross-cycle fixtures, targeted race and static checks pass;
the first live integration attempt rejected archive-only reference differences.
The [reference-content proof](docs/design/reference-content-equivalence.md) now
verifies both ZIP histories, complete members and every reference fact without
replacing occurrence identities. Its real CM/CCL proofs pass. The
[2026-09-13 gate](docs/audit/reference-content-equivalence-2026-09-13.md) passes
the full 2024 participant census, automatically selected cross-graph witnesses
and byte-identical fresh replay. This is not every candidate/path or an integrated
A/B/E generation. The retained publisher executable and graph are unchanged.
No new terminal policy, graph copy, Dagster activation or all-cycle claim is implied.

Latest full-cycle work: the [participant publisher](docs/design/arango-receipt-participant-cycle.md)
implements explicit disk guards, source-shard checkpoints and immutable completion.
Go, race and static checks pass. The [cycle audit](docs/audit/arango-receipt-participant-cycle-2026-09-12.md)
records passing real interruption/resume, complete 2024 publication and independent
full read-only replay; it owns complete-corpus status. All explicit success markers,
equivalence checks and retained checksums pass. Do not infer integrated graph
acceptance from implementation or a checkpoint alone.

Latest connected-graph work: compact-v2 appearances pass retained 100,000/million-row
gates with exact participant-row reconstruction, full live v1 comparison and
standalone replay. The million-row encoded payload is 45.26% smaller. Source
grain and every projected edge/context field remain unchanged; see the
[compact gate](docs/audit/arango-receipt-participants-compact-2026-09-12.md) for
acceptance and retention. Full-cycle corpus acceptance now passes above;
connected-generation integration remains.

The bounded Go Arango participant importer is
implemented. It preserves source appearances, separates receipt and non-monetary
conduit edges, binds exact candidate-authorization context, and verifies every
sample document. The retained 100,000/million-row gates and exact read-only
replay pass. See [the current contract](docs/design/arango-receipt-participants.md)
and [real gate](docs/audit/arango-receipt-participants-2026-09-12.md). Full-cycle
corpus acceptance now passes; graph integration remains open. Do not label a
sample as a complete generation or terminal attribution.

Current priority: [complete the connected funding-evidence graph](docs/design/connected-funding-graph.md)
before a GUI. Cycle-wide 2024 contributor/conduit publication and the selected
cross-graph connection gate now pass. The typed generation boundary now binds
receipt, sender, authorization and outside-spending evidence; its generation-bound
typed one-hop consumer and real source-backed query/replay gate now pass.
Donor/corporate integration and the remaining detailed A/B cycles are unfinished
core work. Python reached broader product behavior; Go has stronger verified
evidence boundaries but has not restored that breadth. See the linked plan for
acceptance gates and the dated legacy comparison.

Python was an exploration, not the implementation guide. Choose target behavior
from product requirements and source evidence; no legacy schema, heuristic,
threshold, total or feature receives a presumption of parity. The
[participant publication contract](docs/design/receipt-participant-publication.md)
starts with source-backed appearances and bounded report-reference indexing.
Its [three-shard benchmark](docs/audit/receipt-reference-index-2026-09-11.md)
passes full projected-field checks and replay. The
[full-cycle reference join](docs/design/receipt-reference-join.md) passes the complete
2024 source/artifact gate and layout-varied replay. Its subsequent
[bounded parallel execution](docs/design/receipt-reference-parallelism.md) passes
fixture/race, synthetic throughput and full-corpus equivalence gates; its verified
2024 run and checks are retained. Wall time fell from 46m 20s to 22m 19s, comparing
a serial join with four source readers against eight report workers, not one CPU
against eight. That run's two source passes consumed 13m 45s. Complete-cycle
participant graph publication now passes as recorded above. Reference
connectivity is not conduit or financial eligibility.

The [narrow-reader implementation](docs/audit/receipt-narrow-reader-2026-09-12.md)
now removes source-width temporary row construction and permits/defaults to eight
source readers. Every selected field matches the previous reader over eight
million real rows; sampled ingestion takes about nine seconds versus twenty-two.
Full tests/race/vet and the retained full 2024 equivalence/resource gate pass:
15m 17s versus 22m 19s, with all four output artifacts exactly unchanged.
Stored facts and graph publications are unchanged. Dispatcher overlap remains a
separate possible optimization, not part of this change.

The [reference topology consumer](docs/design/receipt-reference-topology.md) now
propagates invalid references to both potential pair endpoints and verifies exact
peer counts from retained join artifacts. The bounded report reviewer uses the
extracted shared association policy without changing its version or output.
Fixture/replay/failure/race and the retained full 2024 gates pass, including
complete endpoint readback and both earlier report-policy comparisons; see the
[topology audit](docs/audit/receipt-reference-topology-2026-09-12.md).
Topology alone is not participant publication or source-role qualification. An omitted
endpoint does not establish uniqueness of an unrelated transaction key.

The [source-grain participant publisher](docs/design/receipt-participant-index.md)
now retains one appearance/access row per occurrence with unchanged shared-policy
dispositions and full-fact inspection. Eight independent shard jobs remove the
row-dispatch bottleneck. The eight-million-row gate passes all-value comparison
and worker-varied replay. Its complete 2024 publication/readback, artifact replay
and source inspection now pass and are retained: 264,085,606 source appearances
in 868,576,646 bytes. See the [gate](docs/audit/receipt-participant-index-2026-09-12.md).
The separate [conduit publisher](docs/design/receipt-conduit-publication.md) now
joins both publications through the unchanged role policy. Its fixture/race,
complete 2024 publication, independent corpus, full same-build replay and retention
gates pass: 33,262,189 dispositions and 14,143,626 qualified reported associations.
It preserves unassessed absence and adds no money; bounded typed Arango
connections and full-cycle receipt publication now pass. See the
[conduit gate](docs/audit/receipt-conduit-publication-2026-09-12.md).

Completed consumer milestone: an integrated, reproducible candidate evidence view,
before choosing terminal and allocation policies. The
[candidate evidence command](docs/design/candidate-evidence-view.md) joins the
existing trace, receipt populations, shortest-hop source witnesses and optional
candidate-linked summary context. It preserves local gaps and separate source
versions, with no combined network money total or new source scan. Terminal
classification and allocation remain independent, unselected policies; do not
make receipt/header comparison work a blanket prerequisite for this view.
The opt-in v2 presentation adds pinned source names, complete example paths,
separate memo tables and plain-language coverage warnings. It nests the unchanged
v1 evidence; names are labels, and path amounts remain unallocated.
`inspect-candidate-connection` now checks an expected v2 report ID and opens one
concrete connection's full retained source row through the existing pinned source
reader. The parent is content-checked, not recalculated; selected observation and
source fields are verified. No source/graph/Dagster or financial policy changes.

Existing upstream foundation, not complete product graph coverage: the
[candidate upstream slice](docs/design/candidate-upstream.md) now traces the
complete selected receiver-reported committee ancestry for one candidate,
conserves external/internal/unresolved-scope observations, and exposes cyclic
groups and coverage gaps. Its real 2024 gate passes; it does not allocate
terminal dollars. The [reported-receipt inventory](docs/design/committee-funding-basis.md)
adds donor-bearing source lookup and exact-source upstream committee assessments.
Its full 2024 gate passes without copying the receipt corpus. The additive
[source-role policy](docs/design/receipt-source-evidence.md) now retains overlaps
once as reported committee observations, preserves conflicting entity labels,
and exposes missing conduit IDs and unresolved report references. The complete
overlap review and a bounded original-filing earmark comparison pass. The
[same-report reviewer](docs/design/receipt-report-association.md) now establishes
role-qualified earmark/memo associations and passes two complete original-file
comparisons. It adds no money or verified identity. The
[funding coverage audit](docs/design/funding-coverage-and-time.md) now verifies
both pinned summary populations and conserves receipt-role inventory. It exposes
source exclusions and missing committee/report financial scope without filling
gaps. The [committee-summary review](docs/design/committee-summary-source.md) now
selects the modern cycle CSV and pins an accepted 92-field contract with complete
four-cycle checks. Repeated candidate references, blanks, invalid dates, and
arithmetic differences remain explicit. Its strict Go reader/verifier now passes
all four cycle scans, independent all-field/typed-value comparison, and identical
replay. Invalid candidate references remain source evidence, not corrected IDs.
The immutable summary occurrence/fact publisher and release v4 are implemented.
The [real v4 publication](docs/audit/fec-v4-publication-2026-09-10.md) now passes
all release checks and complete four-cycle raw/fact readback and replay. V4 is
the active source release. The [manual summary Dagster gate](docs/audit/committee-summary-dagster-2026-09-10.md)
now passes exact release/cycle handoff, blocking checks, and four-cycle replay.
Default discovery still selects v3; weekly automation remains disabled. The
[real v4 plan audit](docs/audit/fec-v4-refresh-plan-2026-09-09.md) now measures
download cost and finds hard-link double counting plus separate staging-budget
risk. The [storage review](docs/audit/fec-storage-review-2026-09-09.md) now fixes
inode accounting. The [streaming storage gate](docs/audit/fec-streaming-storage-2026-09-09.md)
now passes with write-time budgets, shared acquisition/staging exclusion,
safe checkpoint retry, and a fitting read-only saved-plan scenario. The hot cap,
floor, and margin stay unchanged; that gate fetched or deleted no bulk source. The
[fresh v4 preflight](docs/audit/fec-v4-preflight-2026-09-09.md) now confirms the
same 129.57 GB candidate and a fitting storage scenario. The approved
[v4 acquisition](docs/audit/fec-v4-acquisition-2026-09-09.md) completed with all
three exit markers zero, independent verification passed, and v3 unchanged.
Do not repeat the capture. The [verified v4 stage](docs/audit/fec-v4-staging-2026-09-09.md)
now has all 25 selected outputs, five passing blocking checks, all three exit
codes zero, and passing independent verification. Do not repeat the completed
stage. V4 now has four immutable committee-summary fact sets; earlier A/B/E
facts and graphs retain their original release ancestry. The
[exact summary assertion calculation](docs/design/committee-summary-assertions.md)
now groups only identical non-candidate evidence, retains every occurrence and
conflicting variant, and emits exact arithmetic diagnostics. Its four-cycle Go
and independent raw-CSV gates pass; the [investigation](docs/audit/summary-assertions-2026-09-10.md)
documents remaining cash/subtotal differences, not repairs. The bounded
[report investigation](docs/audit/summary-report-review-2026-09-10.md) now explains
three selected discrepancies through filer cash discontinuity, attachment-level
amendment selection, and a paper-transcription mismatch. Exact Go source checks
pass; no source correction or general report selector was introduced. The
[summary/receipt readiness review](docs/design/summary-receipt-compatibility.md)
now preserves nine reported fields, exact inventory cohorts, and comparison
blockers for one committee without a numeric difference or eligibility promotion.
Its real five-case 2024 gate, replay, and independent checks pass. Those A and
summary inputs retain different release ancestry. The additive
[same-release report profile](docs/design/receipt-report-profile.md) now passes a
complete 2024 scan of summary-selected v4 Schedule A, with exact physical hashes,
form-line decisions, included report groups, and receipt-date states. Its
[real gate](docs/audit/receipt-report-profile-2026-09-10.md) proves source-aligned
occurrence coverage, not unique/effective financial membership or report-period
coverage. The [bounded report-line reviewer](docs/design/receipt-report-lines.md)
now retains independent memo/individual axes and full report membership. Seven
complete original-file comparisons pass with exact cover subtotals and preserved
memo date exceptions. It uses older accepted facts, not v4 facts. The
[v2 cycle profile](docs/design/receipt-report-profile-v2.md) now preserves those
independent axes for every same-v4 occurrence. Its complete 2024 verification and
exact regrouping to all v1 observations pass. The
[memo/amount source review](docs/audit/receipt-memo-review-2026-09-10.md) now
traces all 62 reviewed-line `Y` occurrences to ten paper transcriptions and both
null amounts to original electronic entries reporting one cent each. Complete
bounded original membership and grouped v4 equivalence pass without relabeling
older facts or changing money. Memo interpretation remains unresolved. The
[report-metadata audit](docs/audit/receipt-report-metadata-2026-09-10.md) now passes
bounded source checks, finds bulk-header history gaps and endpoint status/type
disagreements, and pins the official paper schema. The separate metadata-source
direction is accepted. The [Go reader](docs/design/report-metadata-reader.md)
now verifies saved pages and preserves all endpoint assertions, with complete
retained-page readback and deterministic replay. The
[bounded Go HTTP capture](docs/design/report-metadata-capture.md) now implements
manual metadata-only requests, explicit budgets, retained failures, and guarded
pagination. Recurring activation, history completeness, and financial selection
remain unimplemented. A/B/E stay bulk-only transaction sources.
The [summary-value use policy](docs/design/summary-value-use.md) now separates
reported observations, arithmetic, scoped comparisons, and funding components.
Its retained-case and isolation tests preserve the existing runtime boundary;
no new financial selector is implemented. Summary/metadata blockers do not gate
the accepted bulk observation graph. The
[bounded report-scope assessment](docs/design/report-scope-assessment.md) now
passes four retained witnesses, with exact paper-schema mapping, blank/zero
separation, metadata disagreements, and no financial replacement selection.
The [same-report receipts comparison](docs/design/report-total-receipts-comparison.md)
now qualifies Form 3X report-period total receipts for exact-file numeric pairs;
one retained pair compares and six remain explicitly blocked. Source/amount
conflicts stay visible; funding-component and cycle-comparison guards stay false.
The [unitemized-receipts review](docs/design/report-unitemized-receipts.md) now
preserves explicit period amounts, raw types, subtotal diagnostics, and unknown
donor composition. Retained-case, replay, and independent gates pass; all prior
total-receipts outputs remain identical. The
[report-membership review](docs/design/report-period-membership.md) now checks
explicit electronic chains and requested date-window coverage. Four retained
runs and independent source/day checks pass; mixed paper/electronic cohorts
remain unresolved. The [electronic field binding](docs/design/report-field-binding.md)
now qualifies pinned 8.4 F3/F3X covers and binds seven period fields for five
retained chain candidates. Superseded and prefix/mixed-cohort cases stay blocked;
prior diagnostics remain byte-identical. The [reported-window calculation](docs/design/report-window.md)
now passes exact field membership, sum/boundary aggregation, and separate subtotal/
cash diagnostics across four retained cases. Incomplete windows stay explicit.
The [summary/window comparison](docs/design/summary-report-window.md) now passes
field-specific scope checks, exact subtraction, retained-source verification, and
replay. The [v2 reported-span comparison](docs/design/summary-reported-span.md)
now qualifies five flow comparisons and closing cash over the exact retained
reported window, preserving v1. The early-cycle investigation does not establish
inactivity; it shows that proof is unnecessary for the narrower comparison.
Prefix/suffix dates and internal gaps stay explicit. Cash and cycle financial use remain unaccepted.
The [receipt/window comparison](docs/design/receipt-reported-window.md) now
revalidates the pinned source-aligned profile and matches the retained non-memo
itemized line to bound reports and summary assertions. Complete original/group
checks and blocked alternatives pass without another bulk scan. Empty-report
zero requires original/cover corroboration; transaction uniqueness and financial
or terminal eligibility are not promoted. Other receipt families remain separate.
The [receipt-family source map](docs/design/receipt-families.md) now pins F3/F3X
receipt categories, itemization scope and cover equations. Workbook, complete
saved-profile routing and retained original-file checks pass. It is not a new
runtime financial policy. The additive
[Go family comparison](docs/design/receipt-family-comparison.md) now binds
committee-contribution, transfer and loan fields to exact report evidence and
source-aligned occurrence groups. Its four-case gate, independent checks and
prior-output replay pass without a bulk scan. Absent detail stays null, even
with a zero cover. A [complete F3X witness](docs/audit/receipt-family-witnesses-2026-09-11.md)
now validates positive committee contributions and affiliated/party transfers,
all original Schedule A rows at saved-profile grain, and unchanged Go replay.
One bounded original capture reused existing metadata; no bulk rescan ran.
Memo dates and negative evidence remain. The additive
[absence reviewer](docs/design/receipt-family-absence.md) now requires explicit
bound zero and complete matching original/profile line inventories for a separate
qualified reported zero. Its retained gate and exact prior-command replay pass;
old absent detail stays null, with no synthetic receipts or financial promotion.
The separate [family-window comparator](docs/design/receipt-family-window.md)
now aggregates each accepted family over exact dates, with independent reported
and comparison coverage. Complete spans pass; missing covers and uncovered dates
block full-window values. Its retained gate, independent day/source checks and
exact prior replay pass without promoting financial or terminal eligibility.
The [family-summary comparator](docs/design/receipt-family-summary.md) now maps
the accepted form-specific fields and compares each preserved assertion with
reported and qualified detail windows. Its real gate, raw-summary checks and
exact prior replay pass; date gaps, mismatched spans and source blanks remain
blocked. No financial or terminal eligibility is promoted. The
[positive-source gate](docs/audit/positive-receipt-families-2026-09-11.md) now
checks the remaining initial transfer/loan/party shapes through four tiny new
originals and one reused filing. Its debt-census and processed-null exceptions
remain explicit. Runtime binary identity, Go source replay and independent tests
pass; these checks add no metadata binding or financial selection. The
[v2 report comparison](docs/design/receipt-family-comparison.md) now includes the
remaining mapped non-individual Schedule A categories. General form/field rules
keep thresholded detail separate from reported totals, including equal values;
all previous window/summary outputs replay exactly. The
[positive v2 source gate](docs/audit/positive-receipt-families-v2-2026-09-11.md)
now covers all new form/category shapes with originals/profile and real metadata.
Four target fields bind as thresholded components; three remain blocked because
the generic binder requires blank rather than zero amendment numbering on new
reports. That gate leaves runtime unchanged. Review that shared source-shape rule
before extending financial comparisons, not before integrating observation evidence;
new-family windows, F3P and a complete cash basis remain open.
Do not rewrite
processed transactions or fill unitemized amounts from detail gaps.
Follow the [coverage requirements](docs/design/receipt-report-coverage.md); do not
rescan the completed corpus or download every original filing. Additional report
evidence is required only for uses whose scope or relevant conflicts need it.
Discovery migration, weekly activation, full-cycle associations, and terminal
allocation remain separate gates. The summary asset has no automatic trigger.

Political campaign-finance and lobbying investigation project. The legacy
system is Python + Dagster + ArangoDB and traces disclosed money through PAC
chains into five funding channels. A fresh Go + Dagster + ArangoDB rewrite is
underway. Its processed Schedule A reader, source audit, coordinated FEC
release boundary, first real source release, classic 2024 facts, immutable
Schedule A occurrence evidence, direct candidate-receipt corpus probe,
complete 2024 Parquet fact set, and compact occurrence/change layer have
landed. Compact calculation-membership and candidate-result publication have
also passed the complete 2024 equivalence gate. Coordinated fact-bundle
readiness, exact Dagster partition mapping, and calculation automation have
landed. The prior coordinated v3 release includes Schedule E and Schedule B;
active v4 adds committee-summary CSVs without rebuilding the following graph/fact gates.
Schedule E has verified lossless occurrence and fact publications for all four
target cycles.
The accepted effective independent-expenditure calculation is also published
for all four cycles with exact conservation and sparse exceptions. The
unchanged candidate-reference method, resolved grouping, exact readiness
bundle, and isolated v2 ArangoDB graph now pass for all four cycles. Every v2
graph is `ready`, has zero missing masters, exact amount readback, and
idempotent replay. Across the four cycle-scoped publications, 1,198 decisions
and $32,673,571.32 remain explicit outside candidate edges. Schedule B's
physical, classic-comparison, same-publisher-batch Schedule A alignment,
release-v3, and lossless 2024 columnar-publication gates now pass. Its accepted
98-column fact set conserves all 157,544,163 rows in 158 verified Parquet
shards. The accepted Go processed-disbursement reporting calculation now
conserves the complete 2024 corpus, defines scoped non-memo subtotals and
form-line roles, and retains 55 unresolved records. It emits deterministic
JSON without a publication pointer or Dagster asset and is not graph-eligible.
A separate Go typed sender cohort and fact-level A/B candidate reconciliation
now pass the complete 2024 gate. Source-indexed evidence conserves both ledgers
independently and reproduces exact candidate artifacts on replay. Its manual
diagnostic command does not advance a publication pointer or mutate a graph;
the separate publication and orchestration boundary is described below. See
@docs/design/committee-flow-reconciliation.md and
@docs/audit/committee-flow-reconciliation-2026-09-08.md.
The read-only Go source review now profiles every candidate component and
verifies targeted full source rows, with byte-identical replay. The accepted
graph boundary separates A/B observation edges from candidate-component
documents; it does not infer payments, cash, or terminal sources. Immutable
reconciliation publication, exact-input no-source-row-scan reuse, and the
same-cycle-master observation-readiness bundle are implemented in Go. The new
2024 publication is byte-identical to the earlier audit and reuses in 15.738
seconds; exact readiness passes after release-matched master publication from
the unchanged staged member. See @docs/audit/committee-flow-publication-2026-09-08.md.
The isolated observation graph now passes the complete 2024 gate with exact
per-occurrence readback, complete component membership, bounded single-ledger
queries, source drilldown, and idempotent replay. Missing same-cycle masters
remain explicit and terminal-ineligible. Components never become graph hops
or payment amounts. Thin Dagster publication/readiness/projection wiring now
passes exact upstream handoff, cycle-scoped blocking checks, and isolated real
2024 replay. See @docs/design/committee-flow-orchestration.md and
@docs/audit/committee-flow-dagster-2026-09-08.md. This does not establish live
weekly daemon operation or other-cycle A/B readiness. See
@docs/audit/arango-committee-flow-evidence-2026-09-08.md and
@docs/design/arango-committee-flow-evidence.md. The preceding immutable input
boundary lives in @docs/design/committee-flow-publication.md.
The read-only Go investigative API now pins that completed projection, checks
returned documents against verified inputs, pages identities/observations/
component members and bounded paths, and serves full source rows. Source
backing is verified at startup; each lookup then hashes only requested shards.
The isolated 2024 HTTP gate passes with read-only source storage. No resident
API service, public endpoint, or new Compose budget was deployed. See
@docs/design/committee-flow-api.md and
@docs/audit/committee-flow-api-2026-09-08.md.
The complete 2024 Schedule B semantics audit now
conserves every row in 1,717 reporting/identity shapes and exposes the action,
memo, self-recipient, and exceptional-form populations; sender membership is
owned by the separate calculation above. See @docs/audit/schedule-b-semantics-2026-09-08.md. The receiver-
reported committee-flow calculation, same-release readiness bundle, isolated
ArangoDB graph, topology and bounded query gate, and Dagster chain now pass for
2024. That graph is partial because 707 referenced committee IDs lack a same-
cycle committee-master fact. The complete history audit found 675 official
historical registrations and 32 unresolved reported IDs backed by 50 receipts
and $173,821.08. The additive identity calculation, v2 bundle, and isolated v2
graph now expose those states, preserve v1, pass exact readback, and make all
707 non-current identities terminal-identity-ineligible. V2 remains manual
until the historical-source refresh boundary is accepted.
Python is restricted to Dagster's control plane; the five-channel
model is legacy behavior under review, not the automatic target.

This file is your front door. Read it first; follow the imports below for deeper detail.

## Cross-cutting context

Workspace-wide rules, node topology, and cross-project decisions live in [`~/workspace/vedanta-dhobley/`](../../vedanta-dhobley/). Every agent session reads its global `AGENTS.md` automatically via symlinks (`~/.claude/CLAUDE.md`, `~/.codex/AGENTS.md`, `~/.gemini/GEMINI.md`); this pointer exists so anyone browsing the repo sees the pattern.

- [`AGENTS.md`](../../vedanta-dhobley/AGENTS.md) — operating model, commit conventions, Docker-first policy, host-port scheme, `mem_limit` rules, tailnet FQDN rule, privacy preferences
- [`docs/topology.md`](../../vedanta-dhobley/docs/topology.md) — aerial view of nodes, services, routing, messaging, roadmap
- [`docs/decisions.md`](../../vedanta-dhobley/docs/decisions.md) — timestamped rationale for locked-in choices (joi model swap affects this project's LLM endpoints)
- [`docs/plans/`](../../vedanta-dhobley/docs/plans/) — active time-bounded plans

**Where things belong:** if a decision in this project turns out to be cross-project, raise it in dhobley — do not duplicate it here.

## Run

The following starts the legacy Python development stack. It is not the target
Go topology:

```bash
docker compose -f docker-compose.dev.yml up -d
```

- Dagster UI: http://localhost:4300
- ArangoDB UI: http://localhost:4301 (root / ltpass)

Run the Go gates with `make check`. The shipped release commands are
documented in @docs/go-rewrite.md.

## Stack

- **Legacy orchestrator**: Dagster (assets, schedules, jobs)
- **Legacy/domain database**: ArangoDB (multi-model: documents + named graph)
- **Redesign direction**: Go data plane and application, Dagster control plane,
  and ArangoDB as the primary investigative domain database. Python is limited
  to Dagster definitions and a thin Go-process adapter. See
  @docs/design/go-dagster-boundary.md.
- **Inference**: the old direct-node Qwen/Tailscale instructions are stale. Use the
  existing gateway routed by the shared [homelab topology](../../vedanta-dhobley/docs/topology.md)
  and discover exact ready IDs and controls via `/v1/models`. The bounded prose
  trial uses explicit opt-in environment variables and leaves legacy `.env`
  inference settings unchanged; no production Go inference client is deployed.
- **Anthropic API**: for me (Claude Code). Independent of `joi`.

## Where to look first

- @docs/README.md — documentation routing and authority map
- @docs/design/product-contract.md — desired product behavior and evidence
  boundaries; draft for product review
- @docs/design/investigative-questions.md — required questions and open product
  decisions
- @docs/design/go-dagster-boundary.md — accepted Dagster, Go, and minimal-Python
  responsibility boundary
- @docs/design/first-vertical-slice.md — first source-to-API implementation
  contract and incremental-processing proof
- @docs/design/arango-candidate-receipt-projection.md — implemented isolated
  ArangoDB physical-model probe, exact input lineage, query results, and
  Schedule B/E boundaries
- @docs/design/arango-independent-expenditure-projection.md — implemented
  Schedule E readiness bundle, support/opposition graph, exact lineage, and
  measured 2024 gate
- @docs/design/arango-receiver-reported-committee-flow-projection.md —
  implemented Schedule A committee-flow readiness bundle, graph topology,
  bounded path and cycle query gates, exact readback, and measured 2024 result
- @docs/design/evidence-model.md — target source preservation, normalized fact,
  assertion, relationship, and calculation identities
- @docs/design/calculation-contracts.md — accepted first-slice Schedule A,
  memo, amendment, authorized-committee, and reconciliation rules
- @docs/design/fec-flow-fact-requirements.md — accepted receipt,
  disbursement, candidate-context, Schedule E, flow-reconciliation, and
  terminal-attribution boundaries
- @docs/design/source-contracts.md — accepted strict acquisition, raw parsing,
  schema-drift, validation, and publication boundary for Go source adapters
- @docs/design/source-catalog.md — prioritized FEC, lobbying, congressional,
  entity-resolution, and later-source inventory
- @docs/design/fec-release-strategy.md — accepted coordinated FEC release,
  weekly discovery, acquisition, atomic publication, and retention invariants
- @docs/go-rewrite.md — Go code, commands, live verification, and unimplemented
  boundaries that exist now
- @docs/design/lobbying-source-ingestion.md — official LDA source audit and
  proposed full seed, incremental refresh, reconciliation, amendment, and
  partition design
- @docs/design/legacy-functional-spec/README.md — method and routing for the
  next behavioral-excavation phase
- @docs/architecture.md — high-level system design, layer diagram
- @docs/pipeline.md — the 5-layer ETL detail (sync → parse → graph → enrich → aggregate)
- @docs/funding-channels.md — the 5-channel model and the trace algorithm
- @docs/fec-data.md — FEC bulk file shapes and reference
- @docs/storage.md — `~/workspace/data/legal-tender/{raw,dumps,cache}/` layout
- @docs/operations.md — runbook (how to do common ops tasks)
- @docs/decisions.md — historical decision log (formerly PIPELINE_FIXES.md)
- @docs/todo.md — active Go rewrite queue followed by the frozen Python-era backlog
- @docs/validation.md — validation methodology, four-gate contract, current numbers
- @docs/data-quality.md — how to read `individuals.data_quality` (detail_coverage, primary_source) and what it catches
- @docs/second-brain.md — self-hosted second brain stack design
- @docs/setup-currency.md — meta-tooling currency tracking
- @docs/plan.md — historical Python professionalization plan
- @docs/audit/ — point-in-time codebase + doc + production audits (Phase 0)

## Conventions

- **Legacy active election cycles**: `["2020", "2022", "2024", "2026"]`,
  centralized in `src/config.py`.
- **Target cycle behavior**: use independently reproducible cycle partitions
  as building blocks; calculations explicitly declare output and evidence
  windows, which may span cycles. Only compatible additive results can be summed
  from cycle subtotals. The latest-four-cycle default includes the active cycle,
  is not a retention boundary, and exposes partial coverage. See
  [PD-002](docs/design/investigative-questions.md#pd-002--reuse-cycle-partitions-declare-calculation-windows)
  and the [window contract](docs/design/cycle-calculation-windows.md).
- **Legacy per-election limits**: `{2020: 2800, 2022: 2900, 2024: 3300,
  2026: 3500}`, centralized in `src/config.py`.
- **Legacy funding terminology**: "funding channels" — NOT "Five Pies."
  Product decision PD-001 preserves source and normalized fact grain; the
  legacy channels and target presentations are derived projections.
- **Storage paths**: bind-mounted at `/storage` inside containers, mapped from `~/workspace/data/legal-tender/`. Subdirs `raw/` (FEC zips), `dumps/` (Arango JSONL), `cache/` (regeneratable).
- **Tailnet identifier**: `.env` is gitignored; `.env.example` uses `<host>.<your-tailnet>.ts.net` placeholder per the workspace tailnet-FQDN rule.
- **Print vs log**: use `context.log.<level>` inside Dagster assets, `logger.<level>` (stdlib `logging`) elsewhere. Avoid bare `print()` — many existing call sites are anti-patterns to fix during Phase 3.

## Things to check before doing X

- **Editing legacy FEC parsers** (`src/assets/fec/*.py`): read @docs/fec-data.md
  first. The parsers reference
  `~/workspace/data/legal-tender/raw/headers/` for FEC's official column
  headers, never hardcode field positions. Target Go ingestion instead follows
  @docs/design/evidence-model.md and does not treat classic `indiv.zip` as the
  canonical detailed receipt source.
- **Adding a target source adapter**: read @docs/design/source-contracts.md and
  the source's entry in @docs/design/source-catalog.md. Publisher schemas are
  pinned evidence; an unreviewed remote schema never changes parsing behavior
  during a scheduled run.
- **Adding LDA lobbying ingestion**: read
  @docs/design/lobbying-source-ingestion.md. There is no current official bulk
  dump; the target proposal uses the current LDA.gov API, not the retired
  `lda.senate.gov` host or the 1999–2022 Q1 XML archive.
- **Editing graph assets** (`src/assets/graph/*.py`): read @docs/funding-channels.md to understand which edges feed what. Especially check `donors.py` for the per-election whale threshold.
- **Editing the funding-channels algorithm** (`src/assets/aggregation/candidate_upstream.py`): 1,263 LOC after the May 2026 refactor (module-level helpers + ProcessPool worker). The orchestration function is ~100 LOC; the trace/attribution helpers are extracted. Don't expand the orchestration body — add new module-level helpers and call them.
- **Editing corporate resolution** (`src/rag/wikidata_resolver.py`, `src/rag/whale_resolver.py`, `src/rag/name_match.py`, `src/assets/enrichment/wikidata_resolution.py`): the resolver pipeline is reconci.link → GLEIF → not_found, with multi-signal corroboration for low-confidence matches. No description-filter chain. Cache lives at `~/workspace/data/legal-tender/cache/wikidata.json`. Pytest in `tests/test_name_match.py`, `tests/test_wikidata_resolver.py`, `tests/test_whale_resolver.py` (~68 tests, 25s).
- **Editing legacy cycles or limits**: update `src/config.py`, then use `rg` to
  find any remaining copied values before changing consumers.
- **Adding a legacy Dagster asset**: register it in `src/assets/__init__.py` AND
  in `src/__init__.py`'s `Definitions(...)`. Pick the right `group_name=`
  (sync, fec, graph, enrichment, aggregation, mapping).
- **Adding a rewrite Dagster asset**: keep it in `orchestration/`, invoke Go
  through the shared `GoPipelineResource`, and map only validated result fields
  into Dagster metadata. Domain imports or calculations in this package violate
  @docs/design/go-dagster-boundary.md.
- **Bulk ArangoDB writes**: prefer `collection.import_bulk(batch, on_duplicate="replace")` over per-doc inserts. Batch sizes 10K-50K depending on doc shape. Memory limits in compose were tuned for this in commit `fb34a44`.

## Active state

- **Branch**: `feature/professionalization`.
- **Current priority**: preserve the complete rewrite in Git, finish the
  pre-attribution interpretation review, and build one product-shaped 2024
  candidate slice before choosing terminal or allocation policy. Further prose-model
  and autonomous page-discovery work is paused. See
  @docs/audit/go-rewrite-checkpoint-2026-09-19.md.
- **Current phase**: first vertical-slice implementation for the Go rewrite.
  The processed Schedule A reader/verifier, complete classic-overlap audit,
  exact 21-source FEC release contract, metadata-only discovery command, pure
  release planner, resumable storage-gated acquisition, selected-data staging,
  release checks, and atomic source publication have landed. A partitioned Go
  occurrence publisher conserves every Schedule A physical row, emits parse
  and duplicate issues, builds bounded-memory `SUB_ID` indexes, compares
  semantic values across releases, and atomically publishes one immutable
  occurrence set per cycle. The same Go boundary now publishes occurrences and
  normalized source facts for `cn`, `cm`, `ccl`, `weball`, and `webl`, keeping
  both summary populations separate and money exact. It also publishes
  lossless `fec.schedule_a_receipt.v1` facts with all 81 source fields and no
  counting or resolution policy at fixture scale. A Go calculation implements
  candidate authorized-committee subtotals and separate `weball`/`webl`
  reconciliations against `TTL_INDIV_CONTRIB`; summary values never fill
  detail. The separate `legal_tender_rewrite` Dagster code location wires the
  rewrite assets and guarded sensors while keeping record logic in Go. The first coordinated
  release, five 2024 classic fact sets, and 2024 Schedule A occurrence set are
  published. The occurrence representation consumed 113.45 GB for a 14.77 GB
  staged relation. The full-row Schedule A JSON fact attempt was stopped at
  113 million rows after producing 33.68 GB and must not resume. A direct probe
  then validated all 264,085,606 rows and calculated 8,175 candidates in
  439.454 seconds, writing a 1.02 MB result artifact. A 99-column zstd Parquet
  contract and deterministic one-million-row shard publisher now preserve all
  81 source fields, exact row locators, and policy-free typed values with full
  readback before checkpoint. Its complete 2024 publication gate passed with
  264,085,606 valid facts in 265 shards and 16.76 GB. End-to-end publication
  took 1h 53m 53s; serial full-shard readback is an explicit optimization
  target before weekly automation. The compact occurrence replacement then
  conserved all 264,085,606 rows in 10.277 GB, reduced the 113.448 GB legacy
  footprint by 90.94%, and completed in 17m 3.66s. The Parquet publisher can
  consume the compact contract and adopt equivalent verified shards without a
  rewrite; the real 2024 current pointer now uses compact ancestry over the
  same 265 verified shards. The compact receipt calculation then reproduced
  every direct-probe count and the exact 8,175-result artifact while storing
  only two sparse membership exceptions; its full tree uses 1.078 MB. The
  immutable 2024 fact bundle then bound the exact Schedule A, linkage, and two
  summary fact sets from one source release, passed complete backing
  verification and idempotence in about eight seconds, and replayed the same
  compact calculation set through a single bundle path. Dagster now maps real
  `dataset` and `cycle` dimensions into that bundle and eagerly launches the
  calculation only after readiness. The isolated Go-only ArangoDB probe now
  projects 31,035 entities, 8,175 results, 8,584 candidate-committee edges,
  and 2,116 receipt components from those exact inputs without copying raw
  receipt facts into the graph. Count conservation and idempotent replay
  passed; 143 absent candidate masters and 156 absent committee masters remain
  explicit placeholders, so the result is partial. The complete classic-flow
  audit then proved all 703,597 2024 `pas2` IDs are shared `oth` occurrences
  and rejected the legacy unreconciled transfer sum. A current Schedule E body
  audit accepted Schedule E as the independent-expenditure occurrence
  authority; current `pas2` loses fractional dollars. The exact 80-column
  Schedule E contract and strict Go parser now pass all 548,318 current rows
  with exact COPY identity. The accepted coordinated v3 release preserves the
  all-history relation once and publishes verified lossless occurrence/fact
  sets for 2020, 2022, 2024, and 2026. The accepted Go effective-independent-
  expenditure publisher now conserves all four cycle calculations, keeps
  support and opposition separate, and preserves unrouteable included amounts
  as sparse exceptions. Dagster only invokes that boundary. The isolated 2024
  outside-spending graph now projects 1,953 referenced entities and 5,495
  spender-to-candidate edges while conserving $4,337,242,339.31 in exact
  signed amounts. It uses about 10.10 MiB, passes sub-millisecond representative
  queries, and reuses the content-addressed database on replay. Its 92 missing
  candidate masters remain explicit placeholders; no spender master is
  missing. The immutable 2024 projection-readiness bundle now binds the exact
  calculation and master manifests, passes six blocking integrity checks, and
  eagerly drives the graph through Dagster. A bundle-only replay reused the
  exact projection. The per-fact candidate-reference calculation now passes
  the complete 2024 gate with 45,185 confirmed, 1,811 resolved, 10,996
  unverified, zero ambiguous, and 296 unresolved decisions. The resolved
  aggregate, exact replacement bundle, and separate v2 graph now pass for all
  four cycles with zero missing masters, exact amount readback, and idempotent
  replay. The receiver-reported flow bundle and graph now conserve 180,283
  edges and $4,672,820,179.49, expose 35 cyclic strong components containing
  2,304 committees, and pass exact readback and bounded path queries. Its 707
  missing committee masters remain explicit placeholders. Official 1980–2026
  history confirms 675 registrations; 32 reported IDs remain unmatched and
  cover 50 receipts and $173,821.08. The additive identity calculation and v2
  graph now replace those generic placeholders with 675 historical-
  registration and 32 unresolved-reported-ID states, retain 3,481 exact
  historical assertions, conserve all edge money, and mark every non-current
  identity terminal-identity-ineligible. The v1 graph and Dagster chain remain
  unchanged. Processed Schedule B now has a complete artifact, strict
  157,544,163-row 2024 parser pass, and exact classic orientation/amount
  comparison. Its immutable 2026-08-30 Schedule A/B alignment also passes with
  exact conservation and without merging the ledgers. The accepted v3 release owns
  the archive and four archive-direct cycle relations. The accepted lossless
  2024 fact set conserves all rows in 158 verified Parquet shards and reuses by
  Schedule B source identity across unrelated release changes. See
  @docs/audit/schedule-b-columnar-publication-2026-09-04.md.
- **Working architecture direction**: Go + Dagster + ArangoDB. Dagster owns
  assets, partitions, schedules, sensors, and operational lineage. Go owns all
  source, domain, graph, calculation, fine-grained invalidation, and serving
  behavior. See @docs/design/go-dagster-boundary.md and the 2026-08-27 entry in
  @docs/decisions.md.
- **Legacy disposition rule**: no Python behavior defaults to parity. Preserve
  it only after an independent product, source, or domain contract justifies a
  `KEEP` disposition.
- **Target detailed-receipt source**: FEC processed Schedule A weekly dumps.
  Classic `indiv.zip` is a thresholded legacy comparison source, not the target
  canonical receipt source. PostgreSQL is only a possible dump-extraction
  mechanism; ArangoDB remains the working domain database.
- **Target independent-expenditure source**: FEC processed Schedule E weekly
  dumps. Classic `pas2` supplies candidate-context and comparison evidence
  keyed to shared `oth` occurrences; it is not another amount ledger. The
  Schedule E parser passes the complete corpus; release-v2 facts and effective
  calculations for all four target cycles are published. The isolated 2024
  reported-ID graph projection remains historical evidence. The per-fact
  candidate resolver, resolved grouping, replacement readiness bundle, and
  separate v2 graph pass unchanged for all four target cycles; unified
  production publication remains. See
  @docs/audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md.
- **Target source rule**: capture unknown or changed source bytes, but fail the
  affected publication until a reviewed contract accepts the schema. Never
  pad short rows, truncate extra fields, ignore decoding failures, convert
  money to float, or overwrite duplicate source occurrences.
- **Legacy system**: remains the behavior and differential-validation source.
  Go implementation has started, but no destructive migration or production
  cutover has occurred.
- **Legacy data state**: raw metadata and parser dumps show mixed refreshes,
  with 2022/2026 transactions and all summary files refreshed 2026-06-21,
  while 2020/2024 transaction archives remain from 2026-05-06. The last
  documented full database validation remains 2026-05-16. No Legal Tender
  containers were running during the 2026-08-27 excavation. See
  @docs/design/legacy-functional-spec/system-boundaries-and-sources.md.
- **Legacy validation**: the four-gate process and named-candidate baseline live
  in @docs/validation.md and @docs/audit/baseline-2026-05-12.md. These are
  evidence inputs; the rewrite requires stronger attribution and provenance
  contracts.
- **Lobbying**: not implemented. `src/api/lobbying_api.py` is an unused client
  for the retired Senate-era API contract. Current research preserves LDA.gov
  filings, identities, constants, and LD-203 contribution reports as a
  possible later legislative-influence evidence domain. That detailed design
  remains provisional; lobbying amounts are not candidate-funding totals.
- **Python backlog**: the legacy section of @docs/todo.md is frozen pending the
  disposition pass. Do not resume those items by default.

## Memory model (for me, the agent)

This project uses **`AGENTS.md` (this file) as the canonical agent context**, not auto-memory. Auto-memory (`~/.claude/projects/-home-vedanta-workspace-dev-legal-tender/memory/`) holds only **user-scoped preferences** (e.g., commit-message conventions); project facts live here in the repo so they're version-controlled and visible to all tools.

Once the brain stack is stood up (Phase 2), I'll have MCP access to:
- `mcp__khoj__search` — RAG over the vault, including this project's docs and (optionally) cross-project notes
- `mcp__basic_memory__write_note` — write notes during sessions that persist to the vault

Until those are wired, I read this file (auto via Claude Code) and the imported docs (via the `Read` tool when needed).

## When something I learn doesn't fit anywhere

- **Project fact** → update the relevant `docs/*.md` (or this file if it's load-bearing context)
- **User preference** → save to auto-memory
- **Deferred work / TODO** → @docs/todo.md
- **Architectural decision** → @docs/decisions.md (append to bottom with date)
- **In-flight session note** (after Phase 2) → `mcp__basic_memory__write_note`

The Stop hook (after Phase 1 deploy) will prompt if `src/` changed without `docs/` updates, to enforce the discipline of "code change → doc update in the same session."
