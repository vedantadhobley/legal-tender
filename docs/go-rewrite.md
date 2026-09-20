# Go rewrite as-built ledger

## Current checkpoint

The [2026-09-20 high-level inventory](./audit/go-rewrite-inventory-2026-09-20.md)
is the concise current status. The ledger below preserves detailed implementation
history and evidence, but its reverse-chronological experiments do not define the
next work item. The rewrite is a verified FEC evidence/calculation development
system, not yet a unified or deployed investigative product.

The [offline occurrence/group boundary](./design/prose-referents-and-corrections.md#offline-occurrence-group-and-canonical-candidate-boundary--2026-09-19)
now preserves literal name occurrences, requires explicit unverified mention groups
for role referents and admits source-pinned canonical candidates only through a
separate digest-bound input. Retained surname, short-company-name and two-person
ambiguity counterexamples pass without text matching or named-case runtime rules.
This is test-only representation proof: no new inference, accepted identity, graph
edge or financial behavior exists.

The preceding [grounding-bound assessment comparison](./audit/prose-referent-assessment-model-2026-09-19.md)
supplies immutable validated grounding and accepts only referent/target arrays. Exact
offline replay covers ten Gemma 31B and GPT-OSS responses. Both models pass structure
on nine and semantic review on six, including one of three retained real-source cases.
The split is materially more stable than the combined task, but both models confuse
short mention occurrences with full antecedents. Keep the trial test-only; the new
offline grouping contract addresses the representation gap, not automatic quality.

The [referent/target comparison](./audit/prose-referent-model-comparison-2026-09-19.md)
now retains and exactly replays eight fixed cases on Gemma 31B and GPT-OSS. A recorded
900-second client deadline eliminates the research harness's false 90-second transport
failures. Both models returned all eight answers and passed structure on four, but
manual pre-run-check review found only three and two usable synthetic results,
respectively, with zero usable real-source results. The separate referent and target
representation survives; the combined grounding-plus-assessment producer does not.
Next test assessment alone over immutable validated grounding. No production model,
identity, graph or money behavior changed.

The [whole-entry grounding follow-up](./audit/prose-role-grounding-model-2026-09-17.md)
now attaches complete claim context and checks literal endpoint surfaces separately
from name candidates. Five fresh two-stage cases retain four structural passes and
one rejection. Role separation and pronoun handling work in one case; surname
ambiguity, a missing retraction link and coarse role-class uncertainty remain.
Offline replay/tests pass. This remains test-only, with no graph or identity approval.

The [role-binding experiment](./audit/prose-role-binding-model-2026-09-17.md) now
retains ten second-stage answers using uncorrected name candidates and original
context. All pass structural checks; fresh controls preserve ambiguous, hypothetical
and missing endpoints. Incorrect direct/coreference labels, incomplete selected
context, combined roles and institutional scope remain explicit. Test-only code and
offline replay publish no affiliations and change no source, graph or money behavior.

The [isolated name-selection trial](./audit/prose-name-selection-model-2026-09-17.md)
now retains eight names-only attempts and exact offline scoring against pre-run
labels: 22/22 regression spellings and 13/15 fresh spellings match, with two extra
fresh selections. No names are trimmed, merged into identities or connected to
roles. This is test-only evidence for a narrower task, not production extraction.

The [fresh derived-reference trial](./audit/prose-derived-reference-model-2026-09-17.md)
now retains six requests/responses and offline replay. Four pass structural checks;
one truncates and one violates the joint-activity contract. Wrong entity spans and
instruction-derived proposals remain despite valid references. No extractor is
promoted; production sources, identity, graph and money behavior remain unchanged.

The [offline citation contract](./design/prose-citation-selection.md#go-derived-required-references)
now derives required references from supplied clause and endpoint selections in Go.
Additional evidence remains explicit; the original proposal and derived review stay
separate. Tests preserve ambiguity, context rules and rejection boundaries. No live
model run, saved-response repair or runtime change accompanies this simplification.

The [token-ID model trial](./audit/prose-token-selection-model-2026-09-17.md) adds an
opt-in producer and replay on the same nine excerpts. Four answers pass structural
checks, four fail references and one has no model answer because capacity was full.
All returned ranges are valid, but semantic errors remain and paired input tokens
rise about 50%. This establishes no reliability gain and changes no runtime behavior.

The [source-owned citation proof](./design/prose-citation-selection.md) adds test-only
token catalogs, range selection and full-entry attachment without model-written
quotes or offsets. Exact-match diagnostics preserve ambiguous and missing source
text; earlier model outcomes are unchanged. Reviewed offline selections prove the
contract, not model accuracy or acceptance. No runtime code changed.

The [fresh interpretation-model trial](./audit/prose-interpretation-model-2026-09-17.md)
adds a test-only automatic producer of the separated contract and retained replay.
Five of nine answers pass literal/reference checks; all three real-source cases
are among the four rejections. Synthetic context handling is useful but incomplete
grounding and other failures prohibit automatic acceptance. Source, graph and
financial behavior are unchanged.

The [mention/interpretation boundary](./design/prose-mention-interpretation.md)
adds test-only Go validation of literal anchors, separate normalized proposals and
explicit binding/correction/conflict references. Six retained synthetic cases use
reviewed annotations, not model output. This proves representation and conditional
rules; production interpretation and affiliation acceptance remain unimplemented.

The [code-owned attachment experiment](./audit/prose-evidence-attachment-2026-09-16.md)
adds a separate test-only output contract: model-selected entry IDs, Go-attached
original text/spans and full supplied context. Fifteen retained outcomes replay
offline, including four whole-answer rejections and explicit semantic misses among
literal passes. Existing source, identity, graph and financial code are unchanged.

The [stronger-model follow-up](./audit/stronger-prose-model-comparison-2026-09-16.md)
replays the same fourteen cases with GPT-OSS and explicit supported controls.
Eleven answers pass literal checks; three fail. Semantic review still finds context
loss and wrong role meaning, so no model is accepted for production. Both retained
runs replay offline; only the research harness changed, not runtime behavior.

The [local prose-model comparison](./audit/local-prose-model-comparison-2026-09-16.md)
adds an opt-in Go research test, exact-citation validator and retained offline replay.
Fourteen Gemma calls complete; nine answers pass literal checks and five are rejected.
Semantic review records wrong dates, unsupported fields, alias problems and missing
card structure. Input selection is reviewed, not automatic whole-page discovery.
No production client, source parser, grammar, identity, graph or financial behavior
changed. Existing local infrastructure was used without deployment or `.env` edits.

The [unchanged prose evaluation](./audit/prose-extraction-evaluation-2026-09-16.md)
adds three new retained public HTML bodies and a source-bound regression over those
plus two earlier dated snapshots not used in grammar development. It finds one of
six reviewed roles, misses five, and records AMD retrieval failure separately.
The one emitted candidate is supported; broad precision, recall and alias accuracy
remain unmeasured. No runtime code changed. Keep the grammar as a baseline for a
bounded source-cited local-model comparison, not a production extraction method.

The [prose relationship prototype](./design/prose-relationship-prototype.md) adds
opt-in `--propose-relationships` to `extract-company-page`. Its generic Go grammar
emits source-cited role and parenthetical-name syntax candidates while preserving
the unchanged lexical output. Seven retained pages yield four role occurrences and
two name forms, with no known role dates. This is a development baseline, not
general extraction accuracy or donor acceptance. No fetch, model, graph or money
behavior changed; the automatic affiliation command does not consume these candidates.

The [supplementary-source investigation](./audit/supplementary-affiliation-sources-2026-09-16.md)
retains four HTML bodies and one licensing PDF for the same automatic sample.
The unchanged HTML reader and new offline regressions preserve role/alias prose,
source spans and missing-employer controls. Source selection and PDF interpretation
were manual; that investigation changed no runtime parser, graph or identity rule.
The prototype above now tests narrow role/alias syntax; general interpretation and
automatic page discovery remain separate gaps.

The [automated affiliation report](./design/affiliation-enrichment.md) now connects
verified FEC appearances, existing discovery/capture and source-native name, role
and date comparison through `pipeline entities enrich-affiliations`. It supports
offline replay and a live run with code-selected additional appearances. The new
three-appearance sample completed all requests but found no corresponding person
names or person/employer links. The supplementary evidence above is not yet an input
to that command; source coverage remains incomplete.
No manual person annotations drive the command, and no identity or edge is accepted.

The [v2 person-identity evidence classifier](./audit/person-identity-evidence-v2-2026-09-16.md)
now keeps structured name, organization correspondence, role meaning, role time,
source dependence, optional typed locality and rival plausibility separate. Retained
Chambers, Duffield and unsearched-employee cases replay; the Catsimatidis structure
is covered without manufacturing retained source bytes. Every identity, graph and
financial flag remains false. This manual-input review classifier is not the source
interface used by the automated report above; full names, aliases and coarse role
dates use the existing parsed-source comparison rules.

The first [concrete identity-rule trial](./audit/person-identity-rule-feasibility-2026-09-16.md)
was rejected after real-source review. Exact title text, mandatory personal locality,
receipt-day identity evidence and blanket rival blocking rejected strong candidates
for the wrong reasons. Its experimental Go evaluator, synthetic tests and trial-only
locality projection were removed. The candidate/time evaluator below remains; no
source, graph, identity or financial behavior changed.

The [affiliation evidence evaluator](./design/person-affiliation-acceptance.md#implemented-scope)
adds pure Go candidate comparisons and source-role timelines, with explicit denials,
conflicts and opt-in continuity hypotheses. Retained reviewed claims replay without
accepting donor identities. Existing screening, sources, graph and money are unchanged;
this earlier evaluator added no identity rule or publication pipeline.

The [dated evidence check](./audit/dated-person-role-evidence-2026-09-16.md) adds
retained public HTML and Go regression tests for historical corporate disclosures.
Existing readers and evaluators preserve date meanings and keep point assertions
separate from continuous tenure. No runtime rule or accepted identity was added.

The [binding diagnostic](./design/person-binding-diagnostic.md) adds a pure Go
comparison of FEC appearances and extracted roles, with separate name, employer
and coarse-date results. Retained-source and synthetic tests exercise it; there
is no new CLI or production identity/affiliation publication. Existing source,
screening and money policies are unchanged.

The user-approved [relationship query](./design/relationship-query.md) now adds
`pipeline entities query-relationships` over retained Wikidata responses. It reuses
the role reader, adds parent/child statements and returns incident source claims
with their original direction, time precision and endpoint gaps. It does not bind
FEC identities, publish a graph or change money. The
[exploration](./design/relationship-exploration.md) guides later scope; ordinary
development builds/tests remain sufficient and recovery work stays deferred.

The [company-page reader](./design/company-page-evidence.md) adds offline
`pipeline entities extract-company-page`. It preserves source text, metadata,
JSON-LD and exact byte spans without company selectors or role interpretation.
All three retained pages pass fresh-process replay and regression gates. Company
page selection and independent identity/role binding remain open; no graph changes.

The [independent registry-name slice](./design/organization-registry-discovery.md)
adds `plan-employer-registry`, `capture-registry-names` and `replay-registry-names`.
It derives employer-only queries from verified FEC appearances without Wikimedia
or preexisting LEIs. The bounded live capture and exact offline replay pass;
same-name registry candidates remain ambiguous and no affiliation is accepted.

The [exploratory affiliation discovery slice](./design/affiliation-discovery.md)
adds `plan-affiliation-discovery`, `capture-affiliation-candidates`,
`replay-affiliation-candidates` and offline `assess-affiliation-candidates`.
The generic Go planner preserves source appearances;
the diagnostic CLI seeds it from the existing verified FEC corpus. The small live
Wikipedia/Wikidata capture and fresh offline replay pass with explicit rivals and
retrieval gaps. No identity approval, graph write or production activation is added.
Opt-in v2 query variants and separate candidate-relevance checks now pass retained
live replay. The variants did not recover missing companies and expose namesake/
non-company counterexamples. Default v1 and the screening evaluator are unchanged.

The [structured-role extractor](./design/wikidata-role-extraction.md) adds
`pipeline entities extract-role-evidence` over exact retained Wikidata bodies.
It preserves direction, statement occurrences, ranks, qualifiers and date precision.
Existing-source, interactive-body and CLI replay pass. The bounded comparison against
the unchanged corpus preserves historical-role agreement and missing discovery.
Background capture still honors `maxlag=5`. The bounded discovery slice now supplies
candidates; the bridge to identity screening is not implemented. Graph and money
policies are unchanged.

The [retained person-affiliation corpus](./design/person-affiliation-corpus.md)
adds an offline Go audit runner over exact FEC appearances and reviewed role
annotations. All selected appearances replay with explicit name/time gaps and no
identity approval. This is not automatic role discovery or a production directory.

The [person-affiliation test slice](./design/person-affiliation-testing.md) adds a
pure Go evaluator and synthetic tests for namesakes, executive/employee distinctions,
job changes and multiple affiliations. Existing funding-path validation rejects
affiliation families. There is no new command, source adapter, live person resolution,
graph publication or financial calculation; passing tests are not identity accuracy.

Existing exploratory tooling includes [organization candidate capture and replay](./design/organization-resolution.md).
The [independent SEC issuer path](./design/organization-issuer-discovery.md) adds
`capture-issuer-directory` and `discover-issuer-organizations`. It preserves one
bulk name/ticker/CIK snapshot and assesses source-backed FEC queries offline without
requiring Wikipedia or LEI success. The live directory gate and fresh CLI replay
pass, producing one candidate on the retained query set without identity approval.
`SEC_USER_AGENT` supplies the private request contact; explicit flags override it.
The public body is a fixture; contact-bearing capture/results stay private. No new
graph coverage is claimed.
The [filed registrant check](./design/organization-filed-identity.md) adds
`capture-issuer-filing` and `inspect-issuer-filing`: preserve an explicitly selected
SEC filing, extract only tagged name/CIK facts with source contexts and byte spans,
and compare against pinned FEC queries/directory candidates offline. Its real capture
and byte-identical fresh replay pass. Automatic filing selection is not implemented;
the result supports a candidate but does not approve identity or financial edges.
The [registry corroboration slice](./design/organization-corroboration.md) adds
`capture-organization-registry` and `corroborate-organizations`: derive bounded
exact-LEI requests from saved candidate claims, retain GLEIF records and replay
the comparison offline. Its real fixture passes without approving FEC identities;
missing identifiers, qualifiers, old names, conflicts and source failures remain
explicit. That GLEIF path remains identifier-only; neither path includes an
identity graph rollout or financial change.
`evaluate-organizations` now runs a selected versioned proposal policy against a
[retained source-backed corpus](./design/organization-evaluation.md), without network
access or graph writes. It separates retrieval/matching misses, missing entities,
source failures and unreviewed alternatives; labels never feed the resolver.
Opt-in `--proposal-policy organization-name-proposals.v2` adds explained
letter/number-boundary and legal-suffix proposals. It retains v1 baseline outcomes
and all matching rivals; omitted flags still run v1. Both reviewed misses now produce
proposals on the unchanged small corpus, not verified organization identities.
`build-organization-queries` reads immutable FEC organization/employer fields;
`capture-organizations` retains bounded Wikipedia/Wikidata responses;
`replay-organizations` verifies those bytes and produces conservative name proposals
offline. No canonical identity, employment, ownership or money edges are published.
The [node/edge assumption review](./design/pre-attribution-review.md) remains explicit.
Recovery engineering is deferred and is not required to conduct that review.
The unused `capture-funding-recovery-inputs` command and its dedicated fixture
harness were removed with user approval on 2026-09-15. The
[historical design](./design/funding-recovery-retention.md) records the recoverable
removal; data, graph generations, audit evidence and read-only tools remain.

`verify-funding-recovery-files` now hashes the exact execution/comparison set
selected by a freshly reconstructed plan, with explicit byte and worker limits.
Its [contract](./design/funding-recovery-checkpoint.md#implemented-scoped-file-verification)
keeps file integrity separate from runtime retention, normal decoder validation
and fresh graph recovery. Historical-only bodies are not rescanned.

`plan-funding-recovery` now derives the metadata-only
[fact-start dependency recipe](./design/funding-recovery-checkpoint.md#implemented-metadata-planner).
It separates execution inputs, comparison outputs and historical provenance;
runtime, storage, retention and reconstruction remain explicit blockers.

`inspect-funding-recovery-inventory` now enumerates typed generation dependencies
without graph access or source scans. It hashes manifests, checks artifact sizes,
preserves prior attestations separately and returns explicit missing-input states.
The [inventory contract](./design/funding-recovery-inventory.md) and
[retained-generation audit](./audit/funding-recovery-inventory-2026-09-15.md)
define its scope; the exact historical v3 staging record remains a recovery gap.

`review-release-stage-evidence` now compares a separately pinned stage record
with the exact release, plan and acquisition metadata. The
[historical review](./audit/release-stage-evidence-review-2026-09-15.md) identifies
the old manual-output overwrite and verifies all published descriptors, without
substituting the candidate for the lost original bytes or accepting recovery.

The [release build contract](./go-build.md) now pins the Go compiler image,
Linux/amd64/CGO policy and flags across the release runner and Dockerfiles.
`make release-build BUILD_OUTPUT=NEW_DIRECTORY` verifies offline clean builds
and retains exact source/module inputs. This does not change historical binary
identities or establish complete raw-to-graph recovery.

`publish-shared-receipt-conduit-associations` now applies the separate group rule
across complete compact inputs. It reproduces the old publication first, changes
only qualified shared-degree exclusions and retains complete group decisions.
`publish-shared-conduit-generation` binds a small isolated extension to the
existing A/B/E generation, without copying receipt ledgers or changing old graphs.
See the [extension contract](./design/shared-conduit-generation.md) and
[full-cycle gate](./audit/shared-conduit-publication-2026-09-14.md) for acceptance
status. `inspect-funding-neighborhood` and `inspect-funding-paths` now accept the
extended generation with exact base/shared locators and a separate
`shared_conduit_association` family. They return old/new decisions, group evidence
and both full source occurrences. `validate-shared-conduit-queries` exercises this
boundary with automatic witnesses and exact replay. The
[query gate](./audit/shared-conduit-queries-2026-09-14.md) owns live acceptance.
The date-window loader now accepts it through explicit v2 input specifications,
rejects base/extension overlap and keeps original receipt dates, source identities
and historical facets. `inspect-funding-window-connections` routes
`shared_conduit_association` entries through the same source-backed reader;
committee and candidate endings retain their existing rules. The
[shared-window contract](./design/funding-window-reader.md#shared-conduit-generation-inputs)
and [gate](./audit/shared-conduit-windows-2026-09-14.md) describe its exact boundary.

`review-shared-receipt-references` now performs the automatic
[original-source group review](./design/shared-reference-group-rule.md).
It reads full selected neighborhoods and original rows, validates reference
directions, and evaluates a separate complete-group association rule. Existing
one-to-one decisions, graphs and amount selection remain unchanged. The bounded
source-batch reader shares full-row reconstruction with normal receipt paging.

`profile-shared-receipt-references` adds a
[group-level conduit diagnostic](./design/shared-reference-profile.md). It
replays the accepted complete participant/topology join and examines shared-degree
rejections without changing decisions. It separates full peer coverage, source
roles/ID agreement and signed amount comparisons, with deterministic witnesses
and no new raw scan, graph write or allocation rule.
The [complete 2024 result](./audit/shared-reference-profile-2026-09-14.md) matches
the accepted association artifact byte-for-byte. The original-source reviewer
and additive group rule above are separate from this unchanged diagnostic.

`inspect-funding-window-paths` adds the
[publication-independent committee reader](./design/funding-window-reader.md).
It reopens explicit generations through existing checks, filters selected A/B
observations by reported dates, preserves unknown-time counts and historical
facets, and routes source drilldown to the originating publication. It adds no
source acquisition, graph writes or financial interpretation. Full regression,
targeted race/static checks and the [2024 gate and fresh replay](./audit/funding-window-2026-09-13.md)
pass. Real multi-cycle integration remains open.

`inspect-funding-window-connections` extends the same verified loader to
generation-qualified receipt/conduit entries and candidate authorization context.
It filters receipt and selected A/B observation dates, preserves authorization's
unknown day-level validity, and qualifies returned link IDs by publication without
changing original graph keys. Repeated authorization assertions are evidence
variants, not added payments. Full regression, targeted race/static checks and
two-publication full-chain loader/CLI replay pass. The separate
[retained 2024 connection gate](./audit/funding-window-connections-2026-09-13.md)
passes automatic receipt/conduit/candidate cases, independent complete selected-ledger
date counts and byte-identical fresh-process replay. The additive v2 spending
query now uses individual Schedule E source members, with explicit expenditure
or dissemination date selection, unchanged aggregate parents, full source/decision
replay and conserving exception coverage. Its synthetic full-chain loader/CLI
gate and [retained 2024 census/replay gate](./audit/funding-window-spending-2026-09-14.md)
pass. Measured source-verifying queries are not yet a low-latency serving API. See the
[connection contract](./design/funding-window-reader.md#receipt-and-candidate-connections).

`make test-window-integration` adds a disposable full-chain synthetic loader/CLI
test, including reversed-input reopening and second-publication failure probes.
See the [fixture boundary and isolated runner](./design/funding-window-reader.md#verification-and-next-boundary).
The fixture uses no production data or runtime verification bypass.

The [cycle/window clarification](./design/cycle-calculation-windows.md) changes
the product contract and sequencing, not this ledger's runtime capabilities.
Existing cycle-scoped facts and readers remain unchanged. The selected-ledger
consumer above extends them; general cross-source identity/graph/attribution
consumers still require implementation and gates.

`build-reported-identity-assertions` adds the
[source-backed reported text view](./design/reported-identity-assertions.md).
It verifies complete receipt and committee populations, preserves exact fields
and locators, and reuses immutable source facts instead of copying the corpus.
It accepts pinned A/CM manifests, cycle, workers and an expected view ID for
replay. The [live gate](./audit/reported-identity-assertions-2026-09-13.md) owns
complete-cycle acceptance. No graph mutation or identity resolution occurs.
Complete the [user-requested interpretation review](./design/pre-attribution-review.md)
and evidence checkpoint before selecting terminal definitions or allocation.
The [first review packet](./audit/pre-attribution-review-2026-09-14.md) now records
the implemented assumptions and matching boundaries; selected evidence checksums
pass. User acceptance and a complete recovery dependency inventory remain open.

`profile-terminal-receipt-roles` adds [reported-role profiles](./design/terminal-receipt-roles.md)
for every committee in the selected boundary scope. It verifies the complete
compact participant publication with one to eight workers, joins exact routed
committee IDs to pinned masters and retains source-backed witnesses. It accepts
the shared generation read flags, `--workers` and `--expected-profile-id`.
Regression/race/static checks and the [live gate](./audit/terminal-receipt-roles-2026-09-13.md)
pass, including complete-corpus and byte-identical worker-varied replay. No identity resolution,
terminal allocation, graph import or Dagster activation is added.

`assess-terminal-sources` adds the [boundary assessment](./design/terminal-source-assessment.md)
over complete selected committee-ledger topology. It compares three provisional
hypotheses, preserves explicit identity/coverage/cyclic states and selects source
witnesses automatically. It uses exact generation read flags and optional
`--expected-assessment-id`; no cycle, candidate or terminal-policy override exists.
Regression/race/static checks and the [live gate](./audit/terminal-source-assessment-2026-09-13.md)
pass, including full selected populations and byte-identical fresh replay.
No stored graph or dollar calculation changes.

`inspect-funding-paths` and `validate-funding-paths` add the
[typed multi-hop reader](./design/funding-paths.md). It preserves exact generation
ancestry, one-ledger committee chains, receipt or qualified conduit entry and separate
candidate endings. Bounded search outcomes are not terminal-source classifications.
Full regression/race/static checks and the [live gate](./audit/funding-paths-2026-09-13.md)
pass: all ten data-selected cases and byte-identical fresh replay. No stored
graph or financial policy changed.

`inspect-funding-neighborhood` and `validate-funding-neighborhoods` add the
[generation-bound typed reader](./design/funding-neighborhoods.md): exact input
reverification, one-hop family pages, source drilldown, explicit missing facets
and scoped continuations. Full Go regression and targeted race/static checks
pass. The [real gate](./audit/funding-neighborhoods-2026-09-13.md) and byte-identical
fresh replay also pass for all declared family witnesses and selected endpoint
cases. No graph import, serving process or terminal policy is added.

`verify-funding-generation` adds the [typed generation boundary](./design/funding-evidence-generation.md)
for completed receipt, selected A/B and resolved Schedule E graphs. It preserves
original source identities, verifies exact release membership and reference content,
and declares ledger-separated relationship families and physical endpoint namespaces.
The new Schedule E reader checks every stored model field without invoking import
or schema creation. Tests/race/vet and the [real gate](./audit/funding-evidence-generation-2026-09-13.md)
pass, with byte-identical fresh replay. No current pointer, all-candidate query
or weekly activation is added; the typed neighborhood consumer is documented above.

`inspect-receipt-candidate-connection` adds a
[read-only contributor/committee/candidate connection](./design/receipt-candidate-connection.md).
It binds a completed receipt graph to the existing selected receiver graph and
authorization calculation, checks exact shared facts and references, and returns
one source-backed typed path without duplicating the graph or summing path money.
Cycle comes from the publication; cross-cycle fixtures and targeted race/static
checks pass. The new `validate-receipt-candidate-connections` command selects
its own cases, verifies complete index membership and replays source-backed
cross-graph witnesses. Its first live gate rejected archive-only provenance changes.
The [reference-content context](./design/reference-content-equivalence.md) now
verifies both complete ZIPs, selected members and every stored reference fact;
the real CM/CCL proofs pass while preserving original identities. The
[complete connection/replay gate](./audit/reference-content-equivalence-2026-09-13.md)
passes the full 2024 index census, selected cross-graph witnesses and byte-identical
fresh replay. Ordinary exact-source loaders and upstream output remain unchanged.
Neither retained publisher
executable nor its verification launcher was changed.

`publish-arango-receipt-participants` now adds the
[complete-cycle publication boundary](./design/arango-receipt-participant-cycle.md).
It derives scope from exact source manifests, requires explicit filesystem and
encoded-byte limits, checkpoints only after complete shard readback, and resumes
the same graph without rewriting its verified prefix. Completion is immutable
and last; samples and older graphs remain untouched. Go/race/static checks pass.
The [cycle gate](./audit/arango-receipt-participant-cycle-2026-09-12.md) records
real interruption/recovery and whole-population status, not an assumed completion.
Its complete 2024 import, independent publication check, full read-only replay,
exact equivalence and retained checksums now pass. This accepts receipt-observation
coverage, not an integrated A/B/E generation or terminal amounts.

`benchmark-arango-receipt-participants` adds the first
[streaming appearance/receipt/conduit graph](./design/arango-receipt-participants.md)
with exact candidate-authorization context. Bounded parallel import and full
field readback replace whole-population buffering for this slice. The
[real gate](./audit/arango-receipt-participants-2026-09-12.md) records sample scope,
source drilldown, replay and resource measurements. This is an isolated bounded
graph, not complete-cycle coverage, terminal amounts or production publication.
The retained million-row sample has one million receipt edges and 103,051
qualified conduit associations; full document readback, exact worker-varied
replay, source inspection and Go checks pass; no GUI or weekly activation was added.

The opt-in `--layout compact-v2` now removes the duplicated participant row from
each appearance while preserving every edge/context field. Full reconstruction
from the exact retained row, live expanded-graph comparison, varied-layout replay
and standalone replay pass in the retained
[compact gate](./audit/arango-receipt-participants-compact-2026-09-12.md). The
million-row encoded payload falls from 1,760,113,025 to 963,570,458 bytes (45.26%);
this is not a database-disk saving claim. All source fields remain accessible.
The comparison flags are optional acceptance tooling, not a full-cycle dependency.
The full-cycle corpus acceptance above now passes; integration with existing
committee ancestry remains separate acceptance work.

`publish-receipt-conduit-associations` now joins the exact participant and
reference-topology publications through the unchanged role/conflict policy.
The [contract](./design/receipt-conduit-publication.md) preserves every applicable
disposition, explicit unassessed absence and separate amount comparisons, with no
additional contribution amount or identity resolution. Shard work and disjoint
merge groups are bounded and parallel; ordered final merges remain streaming.
Fixture/race and the [complete 2024 gate](./audit/receipt-conduit-publication-2026-09-12.md)
pass and are retained: 33,262,189 dispositions, 14,143,626 qualified reported
associations, full source membership, independent checks and identical full
same-build replay. The bounded Arango consumer above projects selected occurrences;
complete-cycle participant/conduit graph publication remains open.

`publish-receipt-participants` adds a [source-grain index](./design/receipt-participant-index.md)
with one appearance per occurrence, shared source-role dispositions, exact nullable
amounts and raw role IDs. `inspect-receipt-participant` opens all original fact
fields and verifies the index against them. Eight independent shard workers own
source read, classification, writing and full readback; raw facts are not copied
into Arango. The [real sample gate](./audit/receipt-participant-index-2026-09-12.md)
passes all-value checks and worker-varied replay. The full 2024 publication and
readback are accepted and retained: 264,085,606 rows in 868,576,646 bytes, exact
artifact equality across both complete runs, and unchanged full-source inspection.
The participant index remains unchanged; the separate command above qualifies
memo-conduit evidence. Graph integration remains.

`build-receipt-reference-topology` now consumes the completed reference join and
publishes [sparse endpoint safety](./design/receipt-reference-topology.md). It
propagates invalid references to all matching targets, rechecks exact peer counts
and exposes the sole peer when there is one. The bounded report reviewer now uses
the same extracted role/conflict policy intended for cycle publication. Fixture,
replay, race, failure and the retained [real gate](./audit/receipt-reference-topology-2026-09-12.md)
pass, including full endpoint cross-checks and both earlier report examples.
This prepares pair evidence, not contributor vertices,
qualified conduit associations, additional money or Dagster activation.

`join-receipt-references` implements a
[complete-cycle external-memory join](./design/receipt-reference-join.md) with
shared reference semantics, complete candidate-key membership, sparse decisions
and reverse-reference evidence. The real 2024 serial source/artifact gate and
layout-varied replay pass. [Bounded report workers](./design/receipt-reference-parallelism.md)
now parallelize sorting, joining, classification and artifact assembly; fixture,
race, synthetic and real equivalence gates pass. The retained full-cycle run took
22m 19s versus 46m 20s; both versions used four source readers, so this is not a
one-CPU/eight-CPU scaling result. That earlier run spent 13m 45s in source passes.
Contributor graph publication and conduit eligibility are not implied.

The [narrow-reader improvement](./audit/receipt-narrow-reader-2026-09-12.md) now
constructs only selected access fields after complete file/schema verification,
and `--scan-workers` permits/defaults to eight. It matches all ten selected fields
over eight million existing rows; sampled ingestion falls from about twenty-two
to nine seconds. Full tests/race/vet and full-cycle equivalence pass: 15m 17s versus
22m 19s, exact equality of all four output artifacts, and complete corpus readback.
Stored facts, source pointers, reference policy and graph publications are unchanged.

`benchmark-receipt-reference-index` now measures source-order and report-key
sorted runs from exact retained Schedule A facts. The
[participant contract](./design/receipt-participant-publication.md) preserves
occurrence identity without merging people, resolves no references from partial
samples, and requires bounded full-cycle joins before publication. This is a
physical access benchmark, not another financial or identity policy.
The [three-shard gate](./audit/receipt-reference-index-2026-09-11.md) passes
complete projected-field readback and exact evidence replay with bounded memory.

The current priority is the [connected funding-graph completion plan](./design/connected-funding-graph.md),
not a GUI. The Go rewrite has stronger source preservation and reproducibility
than the legacy projections, but narrower connected product coverage. General
contributor/conduit publication, employer/corporate integration and the remaining
detailed A/B cycles are core unfinished work. The consumer results below do not
constitute a complete four-cycle funding graph.

The [integrated candidate evidence command](./design/candidate-evidence-view.md)
now assembles candidate-boundary committee observations, per-committee receipt
populations, exact upstream membership, source-backed shortest-hop witnesses and
optional candidate-linked summary context. It emits reproducible JSON plus an
optional new Markdown report. The executable SHA-256 and exact inputs enter the
result identity. Terminal and allocation policies remain null; financial summary
blockers do not suppress the observation view. No bulk scan, graph mutation,
new runtime service or Dagster activation is required.

The opt-in `--view-version v2` presentation adds pinned source-backed names,
complete witness-path examples and plain-language scope warnings. Candidate names
use optional `--candidate-master-facts`; committee names use the trace's exact
master input. The unchanged v1 evidence is nested under `evidence`. Memo tables
stay separate and path amounts remain null. See the
[report gate](./audit/candidate-report-2026-09-11.md).

`inspect-candidate-connection` now connects a pinned v2 report's concrete source
ordinal to its full retained row, using the existing source reader. It checks
the expected report ID, exact published ancestry and complete observation equality;
all source fields remain intact. Parent reports are not recalculated or modified.
The [drilldown gate](./audit/candidate-connection-2026-09-11.md) passes physical-row
readback, independent membership checks and byte-identical replay.

`build-candidate-dossier` now creates the first compact product-shaped candidate
slice from a pinned v2 report and the complete candidate-interpretation publication.
Candidate-linked receipts, support, opposition, representative committee paths,
reported endpoints, safe defaults and alternative endpoints remain separate. The
four Schedule E views overlap and are not additive. Exact Schedule A,
committee-flow and Schedule E release IDs are disclosed instead of forced equal.
The [2024 dossier gate](./audit/candidate-dossier-2026-09-20.md) passes for both
retained candidates. Terminal and allocation fields remain null.

`compare-terminal-policies` authenticates one dossier and compares six explicit
receipt-allocation scenarios over its disjoint candidate-linked components. Memo
subtotals remain visible outside the numeric input. Direct, explicitly earmarked,
proportional and unresolved buckets conserve every included row and known signed
cent. Blocked pro-rata and FIFO methods emit no modeled dollars. The
[comparison contract](./design/terminal-policy-comparison.md) and
[2024 gate](./audit/terminal-policy-comparison-2026-09-20.md) pass for both retained
candidates; every scenario remains unselected and terminal eligibility remains false.

`calculate-direct-source-attribution` applies the subsequently accepted narrow
boundary across one complete immutable Schedule A participant publication. It
verifies the exact Schedule A ancestry and candidate-receipt fact bundle, routes
only uniquely authorized committees, and partitions candidate-linked occurrences
into direct, explicitly earmarked, unresolved and memo-evidence populations. It
does not allocate committee chains or resolve source identities. The
[calculation contract](./design/direct-source-attribution.md) and
[complete 2024 gate](./audit/direct-source-attribution-2026-09-20.md) pass exact
cycle/candidate conservation and byte-identical eight/four-worker replay. Durable
publication, Dagster wiring and graph projection remain unimplemented.

The [positive remaining receipt-family gate](./audit/positive-receipt-families-v2-2026-09-11.md)
now checks all seven new form/category combinations against bounded originals,
the saved profile and real metadata. Four target fields bind as thresholded
components; three remain blocked by zero-numbered original headers. The runtime
binary and earlier source outputs are unchanged. Review the shared header rule
before adding new-family windows; do not add entity-specific exemptions.

The [remaining receipt-family gate](./audit/receipt-families-v2-2026-09-11.md)
adds `compare-receipt-families --comparison-version v2`. Shared form/field rules
cover the remaining mapped non-individual Schedule A leaves. Thresholded detail
is not a total comparison, even when amounts match; missing detail stays null.
Retained-source checks and v1 window/summary byte replay pass. No entity-specific
runtime exception, bulk rescan, source/graph/Dagster or financial-use change.

The [positive receipt-family source gate](./audit/positive-receipt-families-2026-09-11.md)
now verifies the remaining initial transfer, loan and party-contribution shapes.
Four tiny originals and one reused filing pass cover/profile checks. The reused
filing keeps two unknown processed amounts and eleven unreviewed debt rows; its
layout-completeness guard stays false. The rebuilt binary is byte-identical to
the preceding gate. These are tests/source evidence, not new financial use.

The [receipt-family summary comparator](./design/receipt-family-summary.md) now
adds `compare-receipt-family-summary`. Exact form-specific mappings compare each
retained summary assertion separately against reported and qualified detail windows.
The five-case gate preserves gaps, mismatched dates and blank loan values; old
outputs replay exactly. No source/graph/Dagster or financial eligibility change.

The [receipt-family window comparator](./design/receipt-family-window.md) now adds
`compare-receipt-family-window`. Each family has independent reported and comparison
coverage, exact signed sums and explicit missing members. Nonempty occurrence
subtotals and qualified reported-zero observations remain distinct bases. Complete
retained spans qualify; gaps and missing covers block window values, not their
underlying evidence. The real gate, independent source/day checks and prior-output
replay pass. No new family summary differences or financial/terminal eligibility.

The [receipt-family absence reviewer](./design/receipt-family-absence.md) now adds
`review-receipt-family-absence`. Explicit bound zero plus complete matching
original/profile inventories can qualify a separate reported-zero observation.
Memo-only populations, incomplete originals and source disagreements stay blocked.
The retained gate passes; old commands replay exactly and absent detail stays
null. No bulk rescan, family-window totals, financial eligibility or graph change.

The [complete F3X witness](./audit/receipt-family-witnesses-2026-09-11.md) now
validates positive contribution and transfer families through the unchanged Go
comparator. One bounded original capture reuses retained metadata/profile;
complete original/profile grouped checks and exact replay pass. Memo dates and
negative amounts remain visible. This adds tests and source evidence, not new
counting rules, window totals, financial eligibility or production publication.

The [receipt-family comparison](./design/receipt-family-comparison.md) now adds
`compare-receipt-families` with the same input flags as the itemized-window
command. Go binds contribution, transfer and loan period fields to exact report
evidence, then compares source-aligned nonmemo occurrence groups. The retained
gate and independent source checks pass; previous outputs replay byte for byte.
Absent detail remains null even with an explicit zero cover. Family windows now
use the separate commands above, including scoped summary differences. Financial
eligibility and terminal attribution remain open.

The [receipt-family source map](./design/receipt-families.md) now distinguishes
the remaining F3/F3X receipt categories, their cover positions, itemization scope
and nested totals. Official workbook checks, complete saved-profile routing and
seven retained original-file checks pass. This is a reviewed source contract;
the bounded Go consumer above accepts only its initial contribution/transfer/loan
subset. A complete cash basis remains unimplemented. The source-map review
itself changed no source facts, graph or Dagster behavior.

The [receipt/window comparison](./design/receipt-reported-window.md) now matches
the retained reported itemized-individual subtotal against source-aligned
Schedule A occurrence groups. It revalidates the existing profile without a bulk
rescan, preserves blocked alternatives, and requires original/cover evidence
for the empty zero report. Independent original/group checks and replay pass;
financial and terminal eligibility remain false.

The [v2 reported-span comparison](./design/summary-reported-span.md) now separates
reported-window matching from full-cycle coverage. Five real flow-field pairs and
closing cash compare exactly; opening cash and outside-period activity remain
unqualified. The bounded source investigation and independent checks pass, and
all four v1 outputs replay identically. No graph, funding, or source data changes.

The [summary/window comparison](./design/summary-report-window.md) now qualifies
field-specific reported subtraction from reverified inputs. Four retained cases
replay identically; one closing-stock pair qualifies, while late cycle starts,
missing coverage, and cash discrepancies stay explicit. Go tests and independent
raw-source checks pass. No financial or terminal eligibility is promoted.

The [report-window calculation](./design/report-window.md) now aggregates exact
bound period fields, retains field-specific missing coverage, and selects cash
boundary stocks without summing balances. Four retained cases and independent
checks pass; cash discrepancies stay explicit and prior diagnostics replay
identically. Reported-window readiness does not qualify financial funding use.

The [electronic report-field binding](./design/report-field-binding.md) now reads
pinned 8.4 F3/F3X covers and binds exact reported amounts to observed-chain
candidates. Seven retained cases and independent source/schema checks pass;
five reports bind seven fields each. Superseded and prefix/mixed-origin cases
remain blocked. Cash and cycle-total eligibility do not change.

The [report-period membership review](./design/report-period-membership.md)
now verifies source-reported electronic chains and explicit date-window coverage.
Four retained runs pass source/day conservation and replay; paper/mixed chains
remain unresolved. Structural readiness does not qualify financial membership
or a cycle total. The command performs no monetary aggregation.

The [unitemized-receipts review](./design/report-unitemized-receipts.md) now
preserves explicit report-period amounts with separate subtotal diagnostics and
same-file pair checks. Four retained cases, full Go gates, and independent
checks pass. It does not infer donors, fill detail gaps, or select funding inputs.

The [same-report total-receipts comparison](./design/report-total-receipts-comparison.md)
now implements the first qualified field/interval numeric pair in Go. Its
retained-corpus and independent gates pass without qualifying a funding component
or changing cycle-summary/receipt readiness-v1 behavior.

The [bounded report-scope reviewer](./design/report-scope-assessment.md) now
implements local pinned-document assessment in Go. Four retained cases and
independent paper-workbook checks pass; supplemental shape is not a financial
replacement, and electronic layouts remain unqualified by this paper reader.

The [summary-value use policy](./design/summary-value-use.md) separates usable
reported observations from qualified funding inputs. New isolation and retained-
case tests confirm existing behavior; no runtime selector or eligibility change
is introduced. Summary/metadata qualification is not a new graph dependency.

Existing foundation: [candidate upstream evidence](./design/candidate-upstream.md).
The Go `trace-candidate-committee-receipts` command now
passes its real 2024 gate for exact candidate-scope accounting, complete
selected committee ancestry, cycles, source witnesses, and compact membership.
It is a read-only attribution-readiness diagnostic, not terminal-dollar
allocation. Its focused design owns the command, inputs, and remaining gates.

The [committee receipt inventory](./design/committee-funding-basis.md) now adds
cycle-wide receipt components, full donor-bearing source-row lookup, and
exact-source candidate upstream assessments in Go. It reuses the existing
Parquet corpus; it does not define a cash denominator or allocate terminal
dollars. These commands remain manual and leave Arango and Dagster unchanged.

The additive [receipt source-role policy](./design/receipt-source-evidence.md)
now passes complete overlap review and bounded earmark inspection. New commands
retain committee observations once and expose unverified conduit/reference
states; the original-file audit confirms missing structured evidence in its
sample. No source field or historical aggregate was corrected by inference.

The [same-report reviewer](./design/receipt-report-association.md) now resolves
bounded exact references and role-qualified earmark/memo associations. Its
two-report original-file gate and replay pass, with amount differences explicit
and no new graph money. The [funding coverage audit](./design/funding-coverage-and-time.md)
now passes both complete valid-fact summary scans and exact receipt-role
conservation. Source exclusions, candidate-versus-committee scope mismatch,
and unmodeled cash timing remain explicit. The
[committee-summary source review](./design/committee-summary-source.md) now selects
the modern CSV and pins an accepted preservation contract with four-cycle evidence.
The `verify-committee-summary` Go command now passes all four cycles and exact
independent raw/typed fingerprints with identical replay. The separate
`publish-committee-summary` command now writes release-bound immutable facts,
with complete raw/fact readback and idempotent replay. Opt-in release v4 adds
four whole CSV artifacts. The original
[publication gate](./audit/committee-summary-publication-2026-09-08.md) separates
complete real artifact tests from fixture-only coordinated publication. The
[real v4 gate](./audit/fec-v4-publication-2026-09-10.md) now passes source
publication, all four release-bound summary publications, complete independent
raw/fact comparison, and identical replay. V4 is the active source release;
the [manual Dagster gate](./audit/committee-summary-dagster-2026-09-10.md) now
passes exact release/cycle handoff, partition-scoped blocking checks, and
four-cycle replay. The registered `fec_committee_summary_fact_job` has no
automatic trigger. Default discovery remains v3; its migration and weekly
activation are separate. The new
[summary assertion command](./design/committee-summary-assertions.md) now groups
exact non-candidate source evidence, preserves complete membership and conflicts,
and emits signed arithmetic diagnostics. Its
[four-cycle investigation](./audit/summary-assertions-2026-09-10.md) rejects
blank filling and a federal-column fallback. The bounded
[report-level investigation](./audit/summary-report-review-2026-09-10.md) now pins
three distinct failure modes and exact Go regression witnesses. This adds only
audit tests and retained evidence, not a new source, report selector, or repair.
The [summary/receipt readiness command](./design/summary-receipt-compatibility.md)
now preserves nine reported fields and exact recipient cohorts with explicit
source/scope blockers. Its [five-case gate](./audit/summary-receipt-readiness-2026-09-10.md)
passes replay and independent checks. No numeric difference or eligibility
promotion is made. The [same-release report profile](./design/receipt-report-profile.md)
now scans staged Schedule A through the verified summary's exact release and
preserves form/line decisions, included report references, and receipt-date states.
It does not relabel older facts or establish unique/effective financial membership.
The [bounded report-line reviewer](./design/receipt-report-lines.md) now preserves
complete report membership with independent memo/individual axes. Its seven
original Form 3 comparisons reproduce exact cover subtotals and retain memo dates
outside report periods. This uses older accepted facts, not v4 facts. Cycle-wide
occurrence profiling now uses the [v2 profile](./design/receipt-report-profile-v2.md)
with a passing complete 2024 gate and exact regrouping to every v1 observation.
It preserves all report-line/memo/individual/date states, including unresolved
memo codes and amounts. The [bounded memo/amount review](./audit/receipt-memo-review-2026-09-10.md)
now traces those exceptions to paper memo evidence and two original one-cent
entries, verifies complete selected original membership, and preserves all source
disagreements. This adds audit tests, not runtime repairs. The
[report-metadata audit](./audit/receipt-report-metadata-2026-09-10.md) now passes
bounded original-witness checks and records bulk-header limits plus endpoint
status/type disagreements. The separate metadata-source direction is now accepted;
the [Go reader](./design/report-metadata-reader.md) implements local page/header
verification and raw assertion preservation through `review-report-metadata`.
The [bounded HTTP command](./design/report-metadata-capture.md),
`capture-report-metadata`, now captures one explicit metadata query with budgets,
failure evidence, and reader verification. No financial selector, transaction API
fallback, history-wide acquisition, or recurring activation is added.
Effective report/account coverage, full-cycle associations,
and terminal allocation remain open.
The [real refresh-plan audit](./audit/fec-v4-refresh-plan-2026-09-09.md) now measures
metadata-only acquisition cost and identifies hard-link double counting and a
separate staging-budget risk. The [storage review](./audit/fec-storage-review-2026-09-09.md)
now fixes inode accounting and adds read-only `review-release-storage` in Go.
Its original full-reserve scenario exceeded the hot cap. The
[streaming storage gate](./audit/fec-streaming-storage-2026-09-09.md) now passes:
actual write-time limits, cumulative retained/temporary bytes, and safe retry
replace the unused uncompressed reserve. The saved-plan scenario fits the same
cap/floor/margin. The [fresh v4 preflight](./audit/fec-v4-preflight-2026-09-09.md)
also passes with the same 129.57 GB candidate. The subsequently approved
[acquisition](./audit/fec-v4-acquisition-2026-09-09.md) completed with all three
exit markers zero and independent verification passed: 24 acquired artifacts,
three reused, and no issues. The [verified stage](./audit/fec-v4-staging-2026-09-09.md)
now conserves all 25 selected outputs in 58.00 GB of compressed output, with
five passing blocking checks, all three exit codes zero, and independent
verification passed. The subsequent v4 source and summary publications are now
complete. Reuse these completed inputs; no additional acquisition or extraction
is needed for the orchestration handoff. Existing A/B/E facts and graphs retain
their original release ancestry; this refresh did not rebuild them.

> **Status:** Complete coordinated FEC source-release boundary, real 2024
> Schedule A occurrence evidence, and occurrence plus normalized-fact
> publication for the five classic reference/summary products. The first
> candidate itemized-individual receipt calculation now has a complete 2024
> direct-source probe. The full-row Schedule A JSON fact publisher remains
> fixture tested but its physical representation failed the real-corpus cost
> gate and must not be automated. The replacement 99-column Parquet contract,
> deterministic resumable publisher, CLI, and compact occurrence/change
> replacement are implemented; both complete 2024 corpus gates passed, and the
> active Parquet set now uses compact ancestry. The compact receipt-calculation
> publisher also passed exact complete-corpus equivalence and replay. An
> immutable same-release fact bundle, exact Dagster multi-partition mappings,
> and eager calculation automation are implemented and passed the real 2024
> readiness and replay gates. The Go-only, content-addressed ArangoDB
> candidate-receipt probe also passed its complete 2024 import, query, and
> replay gates. Release-inventory v2 now adds processed all-history Schedule E,
> and selected-cycle occurrence, lossless fact, CLI, and minimal Dagster
> publication are implemented. The effective calculation, immutable projection-
> readiness bundle, eager Dagster mapping, and content-addressed 2024 outside-
> spending graph have also passed their real-corpus and replay gates. The
> per-fact Schedule E candidate-reference calculation, resolved grouping,
> exact replacement bundle, v2 ArangoDB projection, CLI and Dagster assets,
> and complete replay gates now pass unchanged for all four target cycles. The
> v1 reported-ID graph is retained unchanged as historical evidence. The
> complete 2024 Schedule A committee-flow cohort is also measured. Its exact-
> ID and receipt-role policy, immutable publisher, verified loader, CLI,
> minimal Dagster asset, and complete-corpus replay gate are implemented. Its
> same-release projection-readiness bundle, content-addressed ArangoDB graph,
> topology and query gate, CLI, Dagster assets, and replay gate now pass for
> 2024. The result is partial because 707 referenced committee IDs lack a same-
> cycle committee-master fact. Processed Schedule B now has a source
> contract, strict parser/verifier, classic overlap/orientation audit, and a
> complete 157,544,163-row 2024 corpus pass. Its same-publisher-batch Schedule
> A alignment gate also passes with exact conservation. Active release-
> inventory v3 owns its immutable archive and four `archive_direct` relations.
> The 98-column lossless Parquet publisher, CLI, and minimal Dagster asset are
> implemented. The complete 2024 publication conserves all 157,544,163 rows in
> 158 verified shards with zero invalid or duplicate submission IDs; canonical
> adoption and replay are byte-identical. A Go calculation now implements
> reviewed non-memo reporting subtotals and form-line roles. Typed sender
> membership and fact-level A/B candidate reconciliation now pass the complete
> 2024 gate; economic ownership, automated publication, and graph policy remain
> separate.
> The
> legacy Python runtime remains available; no application cutover has occurred.

This document records Go code that exists now. Target behavior remains in the
[redesign index](./design/README.md), and the current Python system remains
documented separately.

## Shipped tree

```text
go.mod                                      root Go module
cmd/legal-tender/                           single thin command entry point
internal/app/cli/                           command dispatch and JSON output
internal/audit/fecclassicflow/              classic flow and IE-source profiler
internal/audit/feccommitteeflow/            complete Schedule A receiver-flow cohort probe
internal/audit/fecflowmastergaps/           receiver-flow master-history and source-evidence audit
internal/audit/fecschedulea/                classic-file overlap audit
internal/audit/fecscheduleab/               same-batch Schedule A/B candidate alignment audit
internal/audit/fecscheduleb/                Schedule B/classic overlap, direction, and precision audit
internal/audit/fecschedulealayout/          bounded physical-layout benchmark
internal/calculation/fec/candidateresolution/ per-fact identity decisions, resolved grouping, and v2 bundle
internal/calculation/fec/committeeflows/    receiver-flow policy, publisher, and loader
internal/calculation/fec/independentexpenditures/ effective Schedule E calculation and projection bundle
internal/calculation/fec/receipts/          receipt decisions and candidate-summary reconciliation
internal/projection/arango/candidatereceipts/ isolated candidate-receipt graph projection and query probe
internal/projection/arango/committeeflows/   isolated receiver-reported committee-flow graph and topology probe
internal/projection/arango/independentexpenditures/ isolated Schedule E support/opposition graph projection
internal/source/fec/classic/                strict five-product row parser and schemas
internal/source/fec/occurrence/             Schedule A, Schedule E, and classic evidence/fact publication
internal/source/fec/release/                discovery through atomic source-release publication
internal/source/fec/schedulea/              processed Schedule A source reader
internal/source/fec/scheduleb/              processed Schedule B source reader and verifier
internal/source/fec/scheduleaparquet/       Schedule A Parquet schema and readback verifier
internal/source/fec/schedulebparquet/       Schedule B Parquet schema and readback verifier
internal/storage/artifact/                   shared immutable zstd JSONL artifacts
orchestration/                              minimal rewrite Dagster code location
contracts/calculations/fec/                  versioned receipt calculation contracts
contracts/bundles/fec/                       calculation-specific fact-bundle contracts
contracts/projections/arango/                versioned graph-projection result contracts
contracts/evidence/fec/classic/v1/          five-product occurrence-ledger schemas
contracts/evidence/fec/schedule-a/v1/       occurrence-ledger schemas
contracts/evidence/fec/schedule-e/v1/       all-history-to-cycle occurrence schemas
contracts/facts/fec/classic/v1/             five-product normalized-fact schemas
contracts/facts/fec/schedule-a/v1/          lossless receipt-fact schemas
contracts/facts/fec/schedule-e/v1/          lossless independent-expenditure schemas
contracts/facts/fec/schedule-a/columnar/v1/ Parquet physical and publication schemas
contracts/facts/fec/schedule-b/columnar/v1/ Parquet physical and publication schemas
contracts/releases/fec/v1/                  release inventory, schemas, and fixtures
contracts/releases/fec/v2/                  historical Schedule E-expanded release contract
contracts/releases/fec/v3/                  active Schedule B-expanded release contract
Makefile                                    pinned containerized Go checks
```

The module path is `github.com/vedantadhobley/legal-tender`. Go 1.26 is the
language baseline. `github.com/klauspost/compress/zstd` provides streaming
compression. `github.com/parquet-go/parquet-go` v0.32.0 owns the pinned
Schedule A and Schedule B writers and readback implementations. Its pre-v1
status makes the exact physical schemas and full readback gates mandatory;
library-native struct
inference is not the storage contract.

## FEC release discovery and planning

The immutable initial v1 release contract contains exactly 21 artifacts: `cn`, `cm`, `ccl`,
`weball`, and `webl` for 2020, 2022, 2024, and 2026, plus the processed
Schedule A dump with those four two-year relations selected. The
[language-neutral contract](../contracts/releases/fec/v1/) contains the exact
inventory, discovery, plan, and published-manifest schemas plus canonical
planner fixtures. Historical v2 adds one required processed Schedule E dump and
selects its single 80-field all-history relation. It has 22 source artifacts
and 25 staged outputs. Historical v3 adds one processed Schedule B archive and its
four 81-field two-year relations, for 23 source artifacts and still 25 staged
outputs. Schedule B is `archive_direct`: selected-cycle facts stream from the
immutable archive, so the release does not retain a second full COPY extract.
V4 adds four whole committee-summary CSVs for 27 artifacts and the same 25 staged
outputs. V1–v3 remain replayable and unchanged.

Five commands implement the source-release boundary. Additional commands
publish or probe the first evidence, normalized-fact, and calculation layers
from an accepted release:

```text
legal-tender pipeline fec discover [--timeout 30s] [--concurrency 4]

legal-tender pipeline fec plan-release \
  --observations <discovery.json> \
  [--current <published-release.json>]

legal-tender pipeline fec acquire \
  --plan <update-available-plan.json> \
  [--current <published-release.json>] \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id>

legal-tender pipeline fec stage-release \
  --plan <update-available-plan.json> \
  --acquisition <acquired-result.json> \
  [--current <published-release.json>] \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id>

legal-tender pipeline fec publish-release \
  --plan <update-available-plan.json> \
  --acquisition <acquired-result.json> \
  --stage <staged-release.json> \
  [--current <active-release.json>] \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id>

legal-tender pipeline fec publish-schedule-a-occurrences \
  --release <published-release.json> \
  --cycle <even-year> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-occurrence-set.json>] \
  [--shards 512]

legal-tender pipeline fec publish-schedule-a-compact-occurrences \
  --release <published-release.json> \
  --cycle <even-year> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-compact-occurrence-set.json>] \
  [--partitions 512]

legal-tender pipeline fec publish-classic-occurrences \
  --release <published-release.json> \
  --dataset <classic-dataset> \
  --cycle <even-year> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-occurrence-set.json>] \
  [--shards 64]

legal-tender pipeline fec publish-classic-facts \
  --release <published-release.json> \
  --occurrences <published-classic-occurrence-set.json> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-fact-set.json>]

legal-tender pipeline fec publish-schedule-a-facts \
  --release <published-release.json> \
  --occurrences <published-schedule-a-occurrence-set.json> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-fact-set.json>]

legal-tender pipeline fec publish-schedule-a-columnar-facts \
  --release <published-release.json> \
  --occurrences <published-schedule-a-occurrence-set.json> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-columnar-fact-set.json>] \
  [--rows-per-shard 1000000] \
  [--rows-per-row-group 128000]

legal-tender pipeline fec publish-schedule-b-columnar-facts \
  --release <published-v3-release.json> \
  --cycle <even-year> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-columnar-fact-set.json>] \
  [--pg-restore pg_restore] \
  [--work-dir <temporary-shard-parent>] \
  [--rows-per-shard 1000000] \
  [--rows-per-row-group 128000]

legal-tender pipeline fec publish-schedule-e-occurrences \
  --release <published-v2-release.json> \
  --cycle <even-year> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-occurrence-set.json>]

legal-tender pipeline fec publish-schedule-e-facts \
  --release <published-v2-release.json> \
  --occurrences <published-schedule-e-occurrence-set.json> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-fact-set.json>]

legal-tender pipeline fec publish-candidate-itemized-receipts \
  --schedule-a-facts <published-schedule-a-fact-set.json> \
  --linkage-facts <published-linkage-fact-set.json> \
  --all-candidates-summary-facts <published-weball-fact-set.json> \
  --current-campaigns-summary-facts <published-webl-fact-set.json> \
  --storage-root <durable-storage-root> \
  --run-id <stable-run-id> \
  [--current <active-calculation-set.json>]

legal-tender pipeline fec probe-candidate-itemized-receipts \
  --cycle <even-year> \
  --release <published-release.json> \
  --occurrences <published-schedule-a-occurrence-set.json> \
  --linkage-facts <published-linkage-fact-set.json> \
  --all-candidates-summary-facts <published-weball-fact-set.json> \
  --current-campaigns-summary-facts <published-webl-fact-set.json> \
  --storage-root <durable-storage-root> \
  --run-id <stable-probe-run-id>

legal-tender pipeline fec benchmark-schedule-a-layout \
  --source <extract.copy.zst> \
  --cycle <even-year> \
  --rows <bounded-row-count> \
  --source-total-rows <complete-relation-row-count> \
  [--source-sha256 <complete-compressed-sha256>] \
  --output <new-probe-directory> \
  [--rows-per-file 1000000] \
  [--rows-per-row-group 128000]
```

`discover` issues bounded concurrent `HEAD` requests only. It records the
official request URL, final redirect URL, HTTP status, publisher version ID,
ETag, digest metadata when available, last-modified value, content length,
byte-range support, the discovery start/completion window, and each source's
own response observation time. It never reads a response body.

`plan-release` performs no network access. It strictly decodes a saved
discovery and optional prior published manifest, validates exact inventory
membership (21 sources for v1, 22 for v2, 23 for v3, or 27 for v4), and emits one of
`update_available`, `no_change`,
`source_not_ready`, or `invalid`. The latter two states contain no selected
acquisition set. A candidate release ID hashes the inventory version and the
canonical source-ID/version-identity pairs; schedule time and observation time
cannot manufacture a new candidate from unchanged source versions.

`acquire` accepts no status except `update_available`. Before a `GET`, it
requires positive selected content lengths and enforces the Schedule A hot cap
and filesystem-free floor. Changed sources use candidate-specific partials,
strict `Content-Range`, `If-Match`, and `If-Range` resume behavior. Unchanged
sources must match immutable artifacts in the prior published manifest.

After all changed bodies are complete, the command repeats the full selected-inventory
metadata discovery. Any version or length change fails before container tools
run. It then streams selected ZIP members to EOF to prove CRCs, checks the
Schedule A `PGDMP` header, runs bounded `pg_restore --list` output, and requires
all four selected relations. Only then are staging files moved into immutable
SHA-256 paths. A successful acquisition result is persisted by candidate and
run. It is not a published release.

`stage-release` hashes the exact plan and acquisition files and requires them
to form one candidate. It extracts each of the 20 exact ZIP members into a
zstd level-3 stream. It invokes `pg_restore --data-only` separately for each
selected Schedule A relation, requires exactly one COPY section, validates
the COPY framing, and writes every physical data row without semantic or
field-level filtering. Row validation belongs to the downstream occurrence
ledger so malformed evidence survives as an explicit issue. Every output
records compressed and uncompressed byte counts and SHA-256 values and passes
a full decompression digest check. Output CAS files and per-output checkpoints
make extraction independently retryable.
An unchanged source reuses its validated selected outputs from the prior
manifest. The [streaming storage guard](./design/fec-streaming-storage.md)
checks actual compressed writes and cumulative retained bytes against the
accepted cap/floor/margin. Acquisition and staging share a writer lock;
staging cannot update the active pointer.

`publish-release` hashes all three exact input artifacts, revalidates the
inventory-selected source objects and staged outputs, and takes an exclusive filesystem
lock. It requires `current.json` to equal the plan's prior release, writes an
immutable `releases/fec/manifests/<release-id>.json`, then uses a synced
temporary file and rename to replace `current.json`. If a crash leaves the
immutable manifest before pointer replacement, a matching retry reuses it. If
the same evidence chain is already active, publication returns the original
manifest without changing its publication time.

## Schedule A occurrence ledger

> **Legacy physical layout:** The logical behavior below remains valid, but
> the four large JSONL artifacts are rejected for future complete-cycle
> publication. Use the compact publisher in the next section.

`publish-schedule-a-occurrences` consumes one exact published source-release
manifest and one selected cycle. It streams the complete selected Schedule A
relation and publishes four immutable zstd JSONL artifacts:

- one occurrence for every physical COPY row, including invalid rows;
- structured parse and duplicate-key issues;
- a globally sorted `SUB_ID` natural-key index; and
- added, changed, newly absent, and invalid records relative to the prior
  occurrence set.

An occurrence identifies its source snapshot, selected relation, cycle,
one-based row ordinal, exact raw byte offset and length, and raw SHA-256. A
record-version identity combines the publisher, dataset, cycle, `SUB_ID` (or
an explicit unkeyed occurrence), and raw row digest. A separate semantic
digest hashes all 81 decoded values with null-aware framing. Equivalent COPY
escape spellings therefore do not trigger a meaningful row change, while the
exact source bytes remain distinguishable.

The implementation uses bounded hash shards and an external merge instead of
holding a complete cycle in memory. It publishes content-addressed artifacts,
then an immutable occurrence-set manifest, then atomically replaces only the
per-cycle current pointer. It validates occurrence conservation, row states,
natural-key accounting, source-release ancestry, semantic-change accounting,
and storage gates. If a later coordinated release reuses the same selected
Schedule A output and parser versions, the current occurrence set is reused
without parsing it again.

### Compact occurrence and change replacement

`publish-schedule-a-compact-occurrences` preserves the same evidence boundary
without repeating one JSON object per valid source row. The selected immutable
source artifact and one-based source-row range define dense occurrence
membership. The exact row bytes and ordinal derive occurrence, raw-content,
and record-version identities on demand.

Every valid unique `SUB_ID` enters one of 512 deterministic FNV-1a partitions.
Each partition is numerically sorted and stores a fixed 48-byte binary record:
unsigned 64-bit big-endian `SUB_ID`, unsigned 64-bit big-endian row ordinal,
and the 32-byte semantic digest. Invalid rows, invalid keys, and duplicate keys
remain sparse JSONL evidence. Bootstrap additions are declared in the
manifest; later snapshots materialize only actual added, changed, absent, and
invalid deltas.

The publisher replays every source byte, fully reads every completed binary
shard, verifies sort order, partition membership, row ordinals, sizes, and
compressed and uncompressed SHA-256 identities, then atomically advances the
cycle pointer. A same-input run rehashes all immutable backing before reuse.

The complete 2024 gate preserved all 264,085,606 valid unique rows in
10,276,745,806 physical bytes and completed in 17m 3.66s. This is 90.94% less
storage than the 113,447,910,338-byte legacy representation. The exact evidence
is in the
[compact occurrence publication audit](./audit/schedule-a-compact-occurrence-publication-2026-08-31.md).

## Schedule A receipt facts

> **Real-corpus disposition:** Do not run this publisher again in its current
> form. A 2024 attempt produced 33.68 GB after only 113 million of 264 million
> rows and was interrupted. The complete output projected to about 78.7 GB,
> duplicating the 14.77 GB staged relation as verbose source and typed JSON.
> The logical fact contract remains useful; its physical representation is an
> open redesign item.

`publish-schedule-a-facts` consumes one exact source release and its exact
immutable occurrence set. It emits one `fec.schedule_a_receipt.v1` fact for
every unique, source-valid `SUB_ID`. Source-invalid and duplicate occurrences
remain explicit evidence exclusions; the publisher never chooses a duplicate
winner.

Every fact retains the source's complete 81-field record and adds typed
recipient, contributor, candidate, conduit, election, receipt, and filing
groups. Money observations preserve the raw decimal, signed checked cents as
a JSON string when the value is exactly representable, and source scale.
Local receipt and publisher-load timestamps remain timezone-free. Null, blank,
zero, negative, invalid, and memo-X values remain distinct. Memo-X derives an
explicit `memoed_subtotal` flag but does not remove or zero the amount.

The publisher uses the occurrence natural index's one-based source-row ordinal
to build a compact one-bit-per-row selection map, then streams the selected
Schedule A source once. It therefore avoids retaining hundreds of millions of
`SUB_ID` values in memory. The resulting zstd JSONL artifact, fact-set
manifest, and cycle pointer are immutable or atomically replaced under
`facts/fec/schedule-a/`.

These facts apply no receipt-counting, effective-amendment, entity-resolution,
authorization, graph, or aggregation policy. Those remain separately
versioned calculations and projections over preserved facts.

### Columnar replacement

`publish-schedule-a-columnar-facts` replaces only the rejected physical JSON
representation. Its logical fact type remains `fec.schedule_a_receipt.v1`. It
binds the exact coordinated release and either legacy or compact Schedule A
occurrence manifest, then
streams the immutable selected COPY relation into deterministic source-row
ranges. It bypasses the large natural-index bitmap when occurrence evidence
proves that every source row is valid and uniquely keyed; other cycles still
use the compact selection bitmap.

When a clean compact occurrence manifest names the exact source-row membership
already represented by an active verified Parquet fact set, the publisher
adopts those content-addressed shards under the compact ancestry without
rewriting identical facts. Dirty cycles build their selection bitmap from the
compact index's one-based row ordinals.

The physical contract contains 99 flat columns: all 81 publisher columns plus
18 Legal Tender columns for source ordinal and byte locator, normalization
state and issue codes, signed parsed money and source scales, typed dates and
timezone-free local timestamps, election/report periods, and the policy-free
memo-X flag. All non-Boolean source values remain their decoded UTF-8 source
lexemes. The immutable COPY artifact remains the exact-byte authority.

The default layout is one million source rows per zstd Parquet shard and
128,000 rows per row group. Each shard is written to a temporary file, fully
reread through the exact expected schema, and checked against its semantic
digest before it enters content-addressed storage and the checkpoint. A retry
replays source rows to prove the same semantic ranges while reusing unchanged,
digest-verified completed shards. The immutable manifest and cycle pointer are
published only after compressed and uncompressed source digests, source row
and byte counts, shard ranges, facts, and valid/invalid counts conserve.

The semantic digest covers the exact source locator and all 81 decoded source
values. The readback decodes the complete 99-column row and requires the exact
physical schema, while typed values remain deterministic, rebuildable views
covered by normalization and cross-reader tests rather than the source-
semantic digest.

Per-row occurrence and record-version hashes are not repeated in every
Parquet row. The manifest supplies release and occurrence ancestry, and each
row supplies the exact source locator needed to derive those identities and
recover the raw bytes. This keeps provenance lossless without paying a large
high-cardinality storage tax.

## Classic FEC occurrence and fact layers

`publish-classic-occurrences` applies the same preservation boundary to `cn`,
`cm`, `ccl`, `weball`, and `webl`. It strictly parses LF-terminated, unquoted
pipe-delimited rows; preserves empty fields; validates contracted widths,
UTF-8, identifiers, independent year fields, summary dates, and exact decimal
syntax; and emits occurrences, issues, natural-key states, and semantic changes
for one dataset and cycle. It never pads, truncates, silently overwrites, or
uses later-row-wins behavior.

`publish-classic-facts` consumes one exact occurrence set and emits one
normalized fact for every unique source-valid publisher key. Duplicate and
malformed occurrences remain in evidence and are counted as exclusions.
Candidate, committee, and linkage facts retain every source field while
exposing typed years and optional identifiers. `weball` and `webl` remain
different fact types and populations. Their money fields retain the raw
decimal and expose losslessly parsed signed cents as JSON strings with
`summary_value` measurement semantics. Blank or unsupported values never
become zero.

These facts do not create current profiles, authorized relationships, graph
edges, or totals. The `A`/`P` authorization rule remains a separately versioned
calculation over linkage facts.

## Candidate itemized-individual receipt calculation

`publish-candidate-itemized-receipts` pins the exact Schedule A, `ccl`,
`weball`, and `webl` fact-set manifests from one cycle and coordinated source
release. It streams Schedule A once for all candidates, applies the versioned
publisher-classification and memo-X rules, and writes one immutable decision
for every receipt fact. Included signed cents conserve through per-committee
and per-candidate subtotals. Conflicting linkage evidence and committees
claimed as authorized by more than one candidate remain unresolved rather
than being double attributed.

Each FEC summary remains an independent source assertion. The calculation
date-bounds resolved detail to that summary's coverage date and reports
`TTL_INDIV_CONTRIB - resolved detail` as `individual_detail_gap`.
`TTL_RECEIPTS` remains broader context and never fills or validates this
narrower component by equality. The publisher emits content-addressed decision
and candidate-result artifacts, an immutable manifest, and one atomically
replaced cycle pointer. Identical inputs reuse the prior calculation set.

This publisher proves the logical contract on fixtures. Its dense one-decision-
document-per-fact layout is rejected for complete cycles. Do not automate it.

### Compact calculation publisher

`publish-candidate-itemized-receipts-compact` is the accepted complete-cycle
publisher. It pins the same four fact sets from one source release, rehashes
every columnar shard, and reads only the nine columns declared by
`legal-tender.fec.itemized-individual-receipt-membership-predicate.v1`.

Ordinary included and excluded membership is the exact columnar fact-set ID,
one-based source row ordinal, and ordered predicate version. The publisher
materializes only unresolved or invalid row membership as sparse exceptions.
It writes candidate results with the existing result schema, then publishes an
immutable calculation manifest and atomic cycle pointer. A same-input run
rehashes immutable backing and returns the existing calculation set.

The complete 2024 gate classified all 264,085,606 facts and reproduced every
direct-probe decision, amount, route, candidate, and reconciliation count. Its
8,175-result artifact is byte-identical to the direct probe. Only two exception
rows were materialized. The complete calculation tree uses 1,077,859 bytes and
the first run completed in 14m 36.907s. See the
[compact calculation publication audit](./audit/compact-receipt-calculation-publication-2026-08-31.md).

### Coordinated fact bundle

`publish-candidate-itemized-receipts-fact-bundle` freezes the exact Schedule A
columnar, candidate-committee linkage, all-candidates summary, and current-
campaigns summary fact sets required by one calculation cycle. It rejects
missing roles, mixed cycles, mixed source releases, pointer-to-immutable
manifest mismatches, failed blocking checks, or corrupt backing artifacts.

The bundle identity is derived from the ordered roles, exact fact-set IDs, and
immutable manifest digests. It copies no facts and performs no calculation.
`publish-candidate-itemized-receipts-compact --fact-bundle <path>` validates the
bundle against its immutable domain copy and resolves only the four immutable
manifests it names. The four direct fact flags remain for manual diagnostics.

The real 2024 bundle passed complete backing verification in 8.084 seconds. A
same-input replay returned the original manifest in 8.039 seconds, and a
bundle-fed compact calculation replay returned the existing calculation set
in 8.027 seconds without changing any count or artifact identity. See the
[fact-bundle audit](./audit/candidate-receipt-fact-bundle-2026-08-31.md).

## ArangoDB candidate-receipt projection probe

`probe-arango-candidate-receipts` consumes the exact ready fact bundle, compact
calculation, and same-release candidate and committee master facts. It derives
one content-addressed projection ID and creates only the corresponding
`lt_probe_*` database. It cannot target the legacy database.

The named graph contains candidate and committee vertices, disclosed
candidate-committee relationship edges, and calculated receipt-component
edges. Complete candidate results remain directly addressable documents. Fine
Schedule A facts stay in immutable Parquet. Deterministic JSON Lines imports
are resumable, collection counts must match, and projection metadata is the
last completion write.

The complete 2024 run projected 31,035 entities, 8,175 results, 8,584
relationship edges, and 2,116 receipt-component edges. It surfaced 143 absent
candidate masters and 156 absent committee masters as explicit placeholder
vertices, so the state is partial. A second invocation reused the completed
database without imports. See the
[projection design](./design/arango-candidate-receipt-projection.md) and
[real-cycle audit](./audit/arango-candidate-receipt-projection-2026-08-31.md).

`audit-receipt-master-gaps` now reconstructs that population from the exact
calculation, selected masters, a different-release 2024 comparison, and the
other active-cycle masters. It verifies every supporting linkage and summary
fact. The complete result found 298 zero-dollar assertion-only placeholders
and one two-record −$8.4 million adjustment component; comparison masters do
not provide a safe backfill. See the
[master-gap audit](./audit/receipt-master-gaps-2026-08-31.md).

This probe is not a Dagster asset yet. It proves the physical boundary before
production publication. It does not claim donor paths, PAC transfers, or
terminal sources. Outside spending is implemented separately through the
resolved Schedule E calculation and graph.

## Receiver-reported committee-flow policy

`probe-receiver-committee-flows` verifies the immutable Schedule A columnar
manifest and every shard, then classifies the full selected cycle with the
shared policy in `internal/calculation/fec/committeeflows`. The policy requires
exact agreement between `contbr_id` and `clean_contbr_id`, a valid receiving
committee, a non-memo exact amount, and an accepted inbound receipt role.
Names, `entity_tp`, and publisher-derived `is_individual` remain diagnostics.

The complete 2024 scan classified 264,085,606 rows in 110.781 seconds. It
accepted 320,731 rows, 180,283 source-recipient-role groups, and
$4,672,820,179.49. It leaves 792,018 unknown-role rows and 2,101 one-sided-ID
rows explicit. The broad exact-ID-only predecessor was rejected because it
mistook outgoing and intermediary rows for inbound money.

`publish-receiver-committee-flows` now consumes that exact fact set and writes
one content-addressed calculation manifest, sparse unresolved exceptions, and
grouped source-recipient-role results. Ordinary included and excluded rows are
reconstructed from the exact fact set and embedded predicate. The loader
verifies current-to-immutable manifest equality plus both artifact digests.

The complete 2024 publication conserved every source row and known signed
cent. It emitted 794,121 sparse exceptions and 180,283 groups. The first run
took about 106.87 seconds; an unchanged replay took about 8.83 seconds and
returned the byte-identical manifest. The partitioned Dagster asset invokes
only the Go command and runs eagerly when the Schedule A fact set changes.

`publish-receiver-committee-flow-projection-bundle` freezes the calculation,
its Schedule A ancestry, and the exact same-release committee master. The
bundle publisher and loader verify mutable-pointer equality, immutable backing,
cycle coherence, release coherence, and all artifact digests.

`probe-arango-receiver-committee-flows` consumes only that bundle. It projects
8,397 referenced committee vertices and all 180,283 grouped edges into
`receiver_reported_committee_flows`, reads every role count and signed amount
back, computes weak and strong components plus directed cycles, and benchmarks
bounded neighborhoods, ranked paths, shortest paths, and cycle traversal. The
complete 2024 graph conserves $4,672,820,179.49. It has 35 cyclic strong
components containing 2,304 committees. Its state is partial because 707
referenced committee IDs lack a same-cycle master fact; placeholder vertices
preserve their relationships. The eager Dagster chain is calculation → exact
bundle → graph. See the
[cohort audit](./audit/receiver-reported-committee-flow-cohort-2026-08-31.md)
and [graph audit](./audit/arango-receiver-reported-committee-flows-2026-09-01.md).

`audit-receiver-flow-master-gaps` reconstructs those 707 IDs from the exact
bundle and grouped results, then compares them with a different 2024 master,
normalized active-cycle masters, official historical cycle archives, and
same-release linkage and summary facts. The complete audit found 675 official
historical registrations and 32 IDs absent from every audited 1980–2026
master. All gaps are source-only. The optional `--trace-source-receipts` mode
rehashes and scans the exact Schedule A ancestry and reproduced 50 receipts
and $173,821.08 behind the unmatched IDs. It is a forensic command, not a
Dagster asset or weekly calculation. See the
[master-gap audit](./audit/receiver-flow-master-gaps-2026-09-01.md).

`publish-receiver-flow-committee-identities` turns the fast audit's exact-ID
registration evidence into one immutable decision per selected-master gap.
The calculation identity binds only evidence that can change a decision: the
v1 flow bundle and calculation, selected master, alternate-release masters,
normalized historical masters, and raw official archive storage keys and
digests. Linkage and summary facts remain audit diagnostics. The complete 2024
publication took 8.9 seconds and produced 675 `historical_registration`, zero
`alternate_release_registration`, and 32 `unresolved_reported_id` decisions.
All 707 decisions are terminal-identity-ineligible. The 7,690 referenced
selected-cycle masters are eligible identity inputs but are not thereby
classified as terminal sources.

`publish-receiver-committee-flow-projection-bundle-v2` composes that identity
calculation with the immutable v1 flow bundle. It verifies that both layers
name the same flow calculation, selected committee master, cycle, and source
release. The v1 bundle remains unchanged and replayable.

`probe-arango-receiver-committee-flows-v2` writes a separate
`lt_flow_probe_v2_*` database. Historical vertices retain every registration
assertion but no invented current canonical name. Unresolved vertices retain
the reported ID and decision lineage. Exact ArangoDB readback matched 8,397
vertices, 180,283 edges, every identity and role count, and
$4,672,820,179.49. The state is `partial` only because 32 reported IDs remain
unresolved. The v1 Dagster chain is unchanged while this additive boundary is
evaluated. See the
[v2 identity graph audit](./audit/arango-receiver-flow-identity-coverage-2026-09-01.md).

## ArangoDB independent-expenditure projection probes

`publish-independent-expenditure-projection-bundle` freezes one published
effective Schedule E calculation plus same-cycle, same-release candidate and
committee master facts. It verifies each immutable manifest and backing
artifact, copies no data, and publishes one immutable manifest plus an atomic
cycle pointer. A repeated publication returns the original bundle.

`probe-arango-independent-expenditures` consumes that bundle by default. It
resolves only the immutable manifests named by the bundle. Direct calculation
and master flags remain a mutually exclusive diagnostic path. The projection
ID still binds every underlying set identity and manifest digest, so the
readiness wrapper does not alter graph identity. The command can create only a
content-addressed `lt_ie_probe_*` database.

The named graph contains only referenced candidate and spender-committee
vertices plus one exact spender-to-candidate edge per calculation result.
Support and opposition remain distinct relation types. Each edge retains exact
signed minor units, source counts, result ID, calculation-set ID, Schedule E
fact-set ID, and release ID. Raw Schedule E facts remain outside ArangoDB.

The complete 2024 run projected 1,953 entities and 5,495 edges. Exact database
readback conserved $4,337,242,339.31: $1,819,924,212.53 support and
$2,517,318,126.78 opposition. The graph used about 10.10 MiB and all measured
query medians were below one millisecond. It retained 92 missing candidate
masters as explicit placeholders and had no missing spender masters, so its
state is `partial`. A replay verified and reused the same database.

The real 2024 readiness bundle passed all six blocking checks, and a bundle-
only graph invocation reused the measured graph without imports. Dagster maps
the same-cycle calculation plus exactly the candidate- and committee-master
partitions into the bundle, then eagerly targets the downstream graph asset.
Python only launches Go and records validated artifacts. See the
[projection design](./design/arango-independent-expenditure-projection.md) and
[real-cycle audit](./audit/arango-independent-expenditure-projection-2026-08-31.md),
plus the
[readiness-bundle audit](./audit/independent-expenditure-projection-bundle-2026-08-31.md).

`publish-independent-expenditure-candidate-resolution` consumes the exact
effective calculation, its Schedule E ancestry, and the same-release candidate
master. It replays attributed membership and publishes one immutable decision
per fact. The v2 method uses exact normalized name and office context to
confirm or repair candidate identity, retains an existing but uncorroborated
reported ID as `unverified`, and preserves ambiguous or unresolved facts
without a candidate ID. No fuzzy, cross-cycle, LLM, or `pas2` matching runs.

The complete 2024 publication conserved 58,288 facts and
$4,337,242,339.31. It produced 45,185 confirmed, 1,811 resolved, 10,996
unverified, zero ambiguous, and 296 unresolved decisions. An unchanged replay
reused the same calculation and 5,487,677-byte decision artifact. The
partitioned Dagster asset depends on the effective cycle plus coordinated
classic facts and invokes only this Go command. See the
[candidate-resolution audit](./audit/independent-expenditure-candidate-resolution-2026-08-31.md).

The accepted interpretation boundary now has a separate additive publisher:

```text
legal-tender pipeline fec publish-independent-expenditure-candidate-interpretations \
  --storage-root /storage --cycle 2024 \
  --candidate-resolution <exact-or-current-resolution-manifest> \
  --run-id <stable-run-id>
```

It preserves one row per existing decision and the full reported candidate
assertion. A unique exact-context alternative is separate: `inferred` means the
reported ID is absent from the pinned master, while `conflicting` means the reported
ID exists but disagrees with context. Only `confirmed` agreement supplies a safe
default endpoint. The complete 2024 publication conserves all 58,288 decisions and
$4,337,242,339.31, including 757 inferred and 1,054 conflicting rows. Semantic
replay decodes every row and recomputes all state counts and amounts. It does not
change the old resolution aggregate or graph. See the
[accepted publication gate](./audit/pre-attribution-interpretation-publications-2026-09-20.md).

`publish-resolved-independent-expenditures` reads only those dense decisions.
It groups confirmed, resolved, and unverified rows by spender, resolved
candidate, and stance while retaining state-specific counts and signed
amounts. Ambiguous and unresolved rows become exact sparse exceptions outside
candidate edges. The 2024 publication produced 5,303 groups covering 57,992
facts and $4,318,692,700.10, plus 296 exceptions covering $18,549,639.21.

`publish-resolved-independent-expenditure-projection-bundle` pins that
aggregate, its candidate-resolution ancestry, the exact candidate master used
by resolution, and the committee master. The active Dagster graph automation
targets this bundle.

`probe-arango-resolved-independent-expenditures` creates a separate v2
`lt_ie_probe_resolved_*` database. Each edge carries resolution-state quality
components, and projection metadata carries exact unprojectable coverage. The
complete 2024 graph has 5,303 edges and 1,858 referenced entities, conserves
$4,318,692,700.10 on readback, has zero missing master facts, passes all three
sub-millisecond query gates, and reuses the database on replay. The v1
reported-ID graph remains unchanged and manually replayable. See the
[resolved outside-spending audit](./audit/resolved-independent-expenditures-2026-08-31.md).

## Direct candidate-receipt probe

`probe-candidate-itemized-receipts` bypasses the rejected Schedule A fact JSON
artifact while preserving the accepted calculation semantics. It requires the
exact coordinated release, the exact Schedule A occurrence manifest, and the
three classic fact sets. The occurrence manifest must already prove that every
source row is valid, keyed, and unique.

The probe performs one independent pass over the staged Schedule A relation.
It validates all 81 fields, verifies compressed and uncompressed digests,
applies the same receipt-decision function used by normalized facts, and
routes only relevant committees into the shared cycle calculator. It emits one
small candidate-results artifact and a run manifest under `/storage/probes`.
It emits no receipt facts, per-row decisions, production calculation manifest,
or active pointer and is not a Dagster asset.

The complete 2024 run processed 264,085,606 rows in 439.454 seconds. It wrote
8,175 candidate results as a 1,016,233-byte zstd artifact. All source,
uniqueness, row, decision, route, and candidate conservation checks passed.
See the [direct receipt probe audit](./audit/direct-receipt-probe-2026-08-30.md).

## Schedule A physical-layout benchmark

`benchmark-schedule-a-layout` is a bounded manual probe, not a Dagster asset or
publisher. It writes an equal-row zstd COPY baseline and a lossless Parquet
candidate from the same sequential source rows. It then proves all-column
semantic round-trip identity and compares the accepted five-column receipt
predicate through the source and Parquet readers. Output directories are
create-only and carry exact source-row ordinal ranges.

The ten-million-row 2024 gate wrote ten one-million-row Parquet files. The
candidate was 1.0754 times the equal-row zstd size, wrote 117,848 rows per
second, scanned the five required columns 4.177 times faster, used 267 MB peak
RSS, and made identical receipt decisions and signed-minor-unit totals. It
passed every bounded gate and was accepted for a complete-corpus probe. See the
[physical-layout benchmark audit](./audit/schedule-a-layout-benchmark-2026-08-30.md).

The complete 2024 publisher then produced 264,085,606 valid facts in 265
content-addressed Parquet shards and 16,759,429,988 bytes. It conserved all
182,881,299,512 uncompressed source bytes, published zero invalid or excluded
rows, and passed an independent DuckDB scan. Publication took 1h 53m 53s;
serial full-shard readback is the next performance target. Full evidence is in
the [columnar publication audit](./audit/schedule-a-columnar-publication-2026-08-31.md).
The candidate retains decoded source lexemes and types only
`is_individual`; the immutable COPY remains the exact-byte authority. The
complete-corpus work must decide the normalized typed projection and prove
immutable publication, retry, partition, representative-scan, and drilldown
behavior before replacing the paused JSON fact publisher.

## Dagster invocation boundary

The `legal_tender_rewrite` Dagster code location now exposes this asset graph:

```text
fec_release_discovery -> fec_release_candidate
                              |
                              +--> fec_release_acquisition
                                         |
                                         +--> fec_release_stage
                                                    |
                                                    +--> fec_release_publication
                                                               |
                                                               +--> fec_schedule_a_occurrences[cycle]
                                                               |        |
                                                               |        +--> fec_schedule_a_facts[cycle] -------+
                                                               |                    |                        |
                                                               |                    +--> fec_receiver_reported_committee_flows[cycle] --+
                                                               |                                                                  |
                                                               +--> fec_classic_occurrences[dataset,cycle]      |
                                                                        |                                      |
                                                                        +--> fec_classic_facts[dataset,cycle] --+------------------+
                                                                                                               |
                                                               fec_candidate_receipt_fact_bundle[bundle,cycle]
                                                                                     |
                                                                                     +--> fec_candidate_itemized_receipts[cycle]

                                                               fec_receiver_committee_flow_projection_bundle[bundle,cycle]
                                                                                     |
                                                                                     +--> arango_receiver_reported_committee_flows[cycle]

                                                               +--> fec_schedule_e_occurrences[cycle]
                                                                        |
                                                                        +--> fec_schedule_e_facts[cycle]
                                                                                 |
                                                                                 +--> fec_effective_independent_expenditures[cycle] --+
                                                                                                                                      |
                                                               fec_classic_facts[candidate-master,cycle] -----------------------------+
                                                               fec_classic_facts[committee-master,cycle] -----------------------------+
                                                                                                                                      |
                                                               fec_independent_expenditure_projection_bundle[bundle,cycle]
                                                                                 |
                                                                                 +--> arango_independent_expenditures[cycle]
```

All assets invoke the compiled `legal-tender` binary through one subprocess
adapter. The adapter never uses a shell, requires a successful process exit,
validates the single JSON stdout document against the checked-in discovery,
release-plan, acquisition, stage, release-manifest, occurrence, fact, or
calculation schema, and preserves the exact bytes under the corresponding content-
addressed control directory. Contract-valid nonzero acquisition or stage
output is also retained. Python maps only protocol fields into Dagster
metadata. It does not perform HTTP requests, select source versions, compare
releases, or interpret FEC records.

The `monday_fec_release_planning` schedule materializes discovery and planning
at 04:00 `America/New_York` every Monday. `DAGSTER_SCHEDULES_ENABLED=0`
disables it alongside the legacy schedule. A missing active manifest means
initial-release planning; after publication exists, the default baseline is
`/storage/releases/fec/current.json`. An asset sensor starts the separate
acquisition job only for `update_available`; it skips normal unchanged and
not-ready plans. Separate sensors then start staging only for `acquired` and
publication only for `staged`, always carrying exact upstream artifact paths.
Acquisition and staging use three exponential retries and 24-hour process
timeouts. A staging retry never reruns acquisition.

After source publication, one sensor adds selected cycles to dynamic
`fec_cycle` partitions and requests compact Schedule A occurrence and columnar
fact publication in dependency order. A second sensor creates classic multi-
partitions with separate static `dataset` and dynamic `cycle` dimensions and
runs their occurrence and fact assets in dependency order. Python passes exact
artifact paths and metadata only. It does not parse rows, calculate semantic
digests, normalize facts, compare natural keys, or decide which records
changed.

A v3-only sensor maps one published release directly to the four Schedule B
cycle partitions. The Python asset passes the exact release manifest, cycle,
storage root, and run ID to Go and validates the returned fact manifest. It
does not extract the archive, parse rows, normalize money, or choose policy.

The eager automation sensor maps one Schedule A cycle and exactly the linkage,
all-candidates summary, and current-campaigns summary classic datasets into one
`bundle`/`cycle` multi-partition. It materializes the Go-owned readiness bundle
only after all four upstream fact partitions are available, then materializes
the compact calculation's matching cycle from the bundle path. Go independently
enforces the same-cycle, same-release, immutable-manifest, and backing-
integrity barriers.

Another eager sensor maps each Schedule A fact partition directly into the
receiver-reported committee-flow calculation for the same cycle. Python passes
the exact upstream manifest path. Go alone verifies lineage, applies the
ordered policy, conserves rows and signed cents, and publishes artifacts.

A separate eager sensor maps that calculation and exactly the same-cycle
`committee-master` classic-fact partition into a receiver-flow projection
bundle. It then targets the matching Arango graph asset. Go rejects mixed
releases, verifies immutable backing, imports the content-addressed graph, and
performs complete count and signed-amount readback plus bounded path and cycle
queries.

A second eager sensor maps one resolved Schedule E calculation and exactly the
same-cycle candidate- and committee-master fact partitions into the resolved
independent-expenditure projection bundle. It then targets the v2 graph
partition from that bundle. Go rechecks the candidate-resolution ancestry,
exact candidate-master identity, cycle, release, immutable manifests, backing
digests, projection identity, unprojectable coverage, and graph readback. The
earlier reported-ID bundle and graph remain registered for manual replay.

A separate eager candidate-resolution sensor maps each effective Schedule E
cycle plus the coordinated classic cycle into the per-fact Go calculation.
The current classic fact asset combines five datasets, so Dagster observes the
whole same-cycle classic slice; Go reads and pins only `candidate-master`.
Because classic artifacts publish as one coordinated release, this does not
create a second source refresh.

The development and production images compile the Go binary in a separate
builder stage and copy it into the Dagster runtime. `workspace.yaml` keeps the
rewrite and legacy definitions in separate code locations during migration.
Both runtime images pin Debian Bookworm and `postgresql-client-15`, matching
the accepted Schedule A corpus extraction major instead of inheriting a
floating `pg_restore` major from the Python base image.
The rewrite uses Dagster's filesystem IO manager under shared `/storage` so a
manual run and a daemon run do not depend on one container's writable layer.
The verified control-plane packages are pinned to Dagster 1.13.20 and the
matching Dagster PostgreSQL integration release.

## Processed Schedule A reader

`internal/source/fec/schedulea` implements the physical boundary proven by the
2026-08-23 FEC archive observation:

- stream zstd input with two decoder workers, low-memory mode, and a 64 MiB
  decoder-memory limit;
- scan LF-terminated rows without `bufio.Scanner`'s token limit;
- decode PostgreSQL COPY text nulls, control escapes, octal escapes, hex
  escapes, escaped delimiters, and backslashes;
- require the exact 81-column relation width;
- preserve SQL null separately from empty text, false, zero, and negative
  values;
- validate UTF-8, signed decimal, integer, timestamp, boolean, `sub_id`,
  `filing_form`, and selected-period lexemes without converting money through
  floating point;
- yield malformed physical rows with an explicit issue instead of dropping
  them; and
- expose an ephemeral zero-retention row for streaming consumers plus an
  explicit `Freeze` operation when a caller needs an owned record.

The compiled schema is not accepted on its own. A test compares all 81 names,
types, nullability flags, and ordinals with the exact checked-in archive
observation. Eight paired exact COPY and canonical JSON fixtures prove null,
escape, action-code, negative-money, memo, and back-reference behavior.

## Verification command

The first independently runnable operation is:

```text
legal-tender pipeline fec verify-schedule-a \
  --input <extract.copy.zst> \
  --period <even-year> \
  --expected-rows <count> \
  --expected-bytes <uncompressed-bytes> \
  --expected-sha256 <uncompressed-sha256> \
  --expected-compressed-bytes <compressed-bytes> \
  --expected-compressed-sha256 <compressed-sha256>
```

The command emits
`legal-tender.schedule-a-verification.v1` JSON on standard output and
diagnostics on standard error. A complete pass checks row validity, selected
period, compressed and uncompressed byte counts, and both SHA-256 digests.
`--max-rows` provides a smoke pass but deliberately emits no whole-stream
digest or size claim.

## Classic-file overlap command

The rewrite also exposes a bounded-storage, exact comparison with the legacy
cycle products:

```text
legal-tender pipeline fec audit-schedule-a-overlap \
  --schedule-a <extract.copy.zst> \
  --indiv <indiv-cycle.zip> \
  --oth <oth-cycle.zip> \
  --period <even-year>
```

The command selects the exact `itcont.txt` and `itoth.txt` archive members,
hashes all three consumed streams, fully validates processed rows, validates
the classic comparison shape and identifier, indexes classic `SUB_ID`
membership, and uses temporary hash shards to prove Schedule A `SUB_ID`
uniqueness without retaining the complete relation. It reports exact signed
cents and source-code breakdowns for all processed rows, rows absent from
classic products, the accepted itemized-individual cohort, and explicitly
labeled legacy or diagnostic receipt shapes. Temporary shards are removed on
completion.

## Classic flow audit command

The rewrite also profiles the complete classic committee-flow inputs without
loading ArangoDB:

```text
legal-tender pipeline fec audit-classic-flows \
  --pas2 <pas2-cycle.zip> \
  --oth <oth-cycle.zip> \
  --period <even-year>
```

The command strictly validates and hashes the exact `itpas2.txt` and
`itoth.txt` members, profiles signed cents and endpoint/lineage coverage by
source code, proves exact `pas2` `SUB_ID` membership in `oth`, reproduces named
legacy transfer and IE predicates, and measures possible two-sided transfer
and repeated IE-key evidence. It does not deduplicate or accept those legacy
predicates as target calculations.

## Schedule B verification and classic comparison

The accepted processed-disbursement adapter can verify an exact inherited
relation directly from the official custom archive:

```text
legal-tender pipeline fec verify-schedule-b \
  --dump <fec_fitem_sched_b.dump> \
  --period <even-year> \
  --work-dir <temporary-shard-parent> \
  [--max-rows <bounded-smoke-count>]

legal-tender pipeline fec audit-schedule-b-overlap \
  --dump <fec_fitem_sched_b.dump> \
  --period <even-year> \
  --pas2 <pas2-cycle.zip> \
  --oth <oth-cycle.zip> \
  --work-dir <temporary-shard-parent>
```

Both commands require the exact compiled 81-field COPY header and stream only
the selected relation. Verification checks every source lexeme, profiles raw
field coverage, hashes the uncompressed relation, and proves `SUB_ID`
uniqueness with temporary fixed-width shards. The audit adds exact classic
membership, endpoint orientation, and amount comparison. It never merges the
sources or selects effective records. Bounded `--max-rows` runs cancel the
extractor cleanly and make no complete-stream claim.

The same-publisher-batch alignment gate consumes the accepted Schedule A
columnar facts and a separately pinned Schedule B archive:

```text
legal-tender pipeline fec audit-schedule-ab-alignment \
  --storage-root /storage \
  --cycle 2024 \
  --schedule-a-facts <columnar-fact-manifest> \
  --schedule-a-release <exact-release-manifest> \
  --schedule-b-dump <fec_fitem_sched_b.dump> \
  --schedule-b-observation <archive-observation.json> \
  --work-dir /storage/cache
```

The command rehashes all Schedule A fact backing, verifies the complete
Schedule B artifact and selected relation, and emits exact, compatible-date,
conflicting-amount, ambiguous, A-only, and B-disposition counts. It indexes
only the accepted A receiver-flow cohort and streams B once. It never publishes
facts, selects effective B rows, or merges amounts. The complete 2024 result
is in the
[alignment audit](./audit/schedule-ab-alignment-2026-09-04.md).

### Schedule B columnar facts

`publish-schedule-b-columnar-facts` requires a published v3 release and the
selected cycle's exact `archive_direct` relation. It verifies the release-owned
archive size and SHA-256, streams the relation through `pg_restore`, strictly
decodes the 81 source columns, and proves global `SUB_ID` uniqueness with
bounded temporary shards. It does not retain a full COPY extract.

The physical contract contains 98 flat columns: all 81 publisher lexemes plus
source ordinal and byte coordinates, exact money values and source scales,
typed dates and local timestamps, report and election periods, publisher
timestamp, and the policy-free memo-X flag. The source lexemes remain the
authority. Typed columns are deterministic conveniences and contain no
amendment, recipient, counting, or outgoing-flow policy.

The default layout is one million source rows per zstd Parquet shard and
128,000 rows per row group. Each shard is reread through the exact schema and
matched to its semantic digest before content-addressed publication. A
checkpoint records completed ordinal ranges. A retry replays the relation to
prove those ranges and reuses matching shards; the manifest and active cycle
pointer advance only after source COPY identity, fact conservation, shard
coverage, independent readback, and uniqueness checks pass.

Fact-set identity binds the Schedule B artifact, selected relation, physical
schema, publisher version, and shard configuration. It does not bind unrelated
members of the coordinated release. A descendant release that reuses the same
Schedule B artifact verifies the existing Parquet digests and returns the same
fact set without invoking `pg_restore`.

The complete 2024 gate published 157,544,163 facts in 158 shards and 1,261 row
groups. It preserved zero invalid rows and zero duplicate `SUB_ID`s, reduced
114.82 GiB of streamed COPY text to 7.32 GiB of Parquet, and completed in
4,195.543 seconds. Canonical adoption took 8.9 seconds; the next full backing-
verified replay took 8.2 seconds and produced identical manifest bytes. See
the [publication audit](./audit/schedule-b-columnar-publication-2026-09-04.md).

The Go `audit-schedule-b-semantics` command now scans the published Parquet
facts with bounded workers, validates every backing digest, checks the full
physical schema and row ordinals, and conserves reporting/identity groups with
exact cents and filing-reference evidence. It avoids archive extraction.
The Go `calculate-disbursement-reporting` command implements the accepted
non-memo reporting subtotal over reviewed regular-committee form lines. It
keeps memo, separate reporting scopes, and unresolved records in disjoint
accounting buckets, with exact row/cent conservation and source-index
membership. The deterministic JSON result is not graph-eligible. There is no
active publication pointer or Dagster asset for this calculation yet.
Its complete 2024 gate conserved all 157,544,163 facts into 39,989 groups in
114.175 seconds; a four-worker replay produced byte-identical output. See the
[reporting calculation gate](./audit/schedule-b-reporting-calculation-2026-09-08.md).
The [Schedule B calculation design](./design/schedule-b-calculations.md) owns
the reporting contract and remaining economic-flow gates.

### Fact-level A/B candidate reconciliation

```text
legal-tender pipeline fec reconcile-committee-flows \
  --storage-root /storage \
  --output-root <isolated-evidence-root> \
  --schedule-a-facts <published-A-columnar-manifest> \
  --schedule-b-facts <published-B-columnar-manifest> \
  --release <coordinated-release-manifest> \
  --cycle 2024 --workers 8
```

The command verifies exact coordinated source selection and both complete
fact sets, selects the unchanged receiver cohort and the typed sender cohort,
then persists source-indexed observations and candidate-component assertions.
It reads existing Parquet; no archive download or extraction is required.
Stdout contains deterministic result JSON only on success; stderr contains
progress and elapsed time. Evidence files are relative to `--output-root`.
This manual command does not update an active pointer, skip source selection,
or mutate a graph. Existing content-addressed evidence is verified on replay.

The complete 2024 gate passed in 298.312 seconds. Independent validation
checked every saved record, digest, and separate ledger amount. Three
complete candidate replays used saved evidence without rescanning the source
corpus. See the [design](./design/committee-flow-reconciliation.md) and
[measured gate](./audit/committee-flow-reconciliation-2026-09-08.md).

The read-only source review is also implemented:

```text
legal-tender pipeline fec review-committee-flows \
  --result <saved-reconciliation-result.json> \
  --evidence-root <root-of-its-evidence-artifacts> \
  --storage-root /storage
```

It verifies and replays complete candidate artifacts, profiles every component,
and validates exact source ancestry and all backing shard hashes. It then
seeks to selected source ordinals, preserves their full physical rows, and
requires each saved observation to match its source and membership predicate.
It does not rescan every source row or change any match, source, graph, or
publication pointer. Result JSON goes to stdout only on success; timing goes
to stderr. The 2024 gate profiled 166 shapes and reviewed 76 source rows in
21.778 seconds; complete review replay produced identical JSON in 19.166
seconds. See the [source review](./audit/committee-flow-source-review-2026-09-08.md)
and [implemented graph contract](./design/arango-committee-flow-evidence.md).

The pre-attribution interpretation audit composes that replay with the exact
candidate-resolution decisions, receiver-flow exceptions and complete Schedule B
semantics profile:

```text
legal-tender pipeline fec audit-pre-attribution-interpretations \
  --storage-root /storage \
  --candidate-resolution <candidate-resolution-manifest> \
  --receiver-flows <receiver-flow-manifest> \
  --schedule-b-semantics <complete-schedule-b-semantics-result> \
  --committee-flow-result <saved-reconciliation-result.json> \
  --committee-flow-evidence-root <root-of-its-evidence-artifacts> \
  [--output <immutable-result-path>]
```

It is diagnostic only. It verifies exact lineage and complete backing, profiles
reported-versus-inferred Schedule E endpoints, classifies one-sided committee IDs,
and emits A/B date-gap bands. It changes no policy, current pointer, graph or money.
The measured 2024 result and recommendations are in the
[pre-attribution validation](./audit/pre-attribution-interpretation-validation-2026-09-20.md).

The accepted A/B replacement is also an additive immutable calculation:

```text
legal-tender pipeline fec publish-committee-flow-comparison-candidates \
  --storage-root /storage --cycle 2024 \
  --reconciliation <exact-or-current-reconciliation-manifest> \
  --run-id <stable-run-id> \
  --max-candidate-pairs 10000000
```

It emits direct candidate pairs before transitive component union. Every row keeps
both source ordinals, sub-IDs, roles, types, dates and reported amounts; it adds
exact match signals, signed and absolute date gaps, a gap band and both candidate
degrees. The 2024 publication contains 471,229 pairs: 155,364 mutual one-to-one and
315,865 with competing candidates. Its operational capacity is fail-closed and
never filters output. Every pair has `financial_effect=none`; no amount is combined,
deduplicated or made graph-eligible. Fresh semantic replay validates every row and
recomputes the complete census. See the
[accepted publication gate](./audit/pre-attribution-interpretation-publications-2026-09-20.md).

The immutable publisher and exact observation-readiness commands now exist:

```text
legal-tender pipeline fec publish-committee-flow-reconciliation \
  --storage-root /storage --cycle 2024 --workers 8 \
  --schedule-a-facts <published-A-columnar-manifest> \
  --schedule-b-facts <published-B-columnar-manifest> \
  --release <coordinated-release-manifest>

legal-tender pipeline fec publish-committee-flow-evidence-bundle \
  --storage-root /storage --cycle 2024 \
  --calculation <published-reconciliation-manifest> \
  --committee-facts <published-committee-master-manifest>

legal-tender pipeline fec verify-committee-flow-evidence-bundle \
  --storage-root /storage --bundle <published-observation-bundle>
```

The publisher owns `calculations/fec/committee-flow-reconciliation/v1/`,
including its immutable results, compact evidence, and per-cycle pointers.
Exact input/policy reuse rehashes source backing and replays compact evidence,
but does not decode source rows again. Readiness owns
`bundles/fec/committee-flow-evidence/v1/` and verifies exact calculation and
committee-master ancestry, including selected source bytes across releases.
V1 does not accept historical identity inputs or confer economic-flow
eligibility. See the [publication contract](./design/committee-flow-publication.md).
The isolated observation graph now passes the complete 2024 gate:

```text
legal-tender pipeline fec probe-arango-committee-flow-evidence \
  --storage-root /storage --cycle 2024 \
  --projection-bundle <published-observation-bundle> \
  --endpoint http://legal-tender-dev-arango:8529 \
  --password-env ARANGO_PASSWORD
```

It preserves one edge per selected A/B occurrence in separate collections and
keeps reconciliation components outside traversal. It verifies every stored
field, each ledger's signed amount, complete component membership, bounded
single-ledger queries, and full source drilldown before completion metadata.
Same-cycle master gaps remain explicit. Replay rechecks completed documents
without replacing them. See the [command and contract](./design/arango-committee-flow-evidence.md)
and [measured gate](./audit/arango-committee-flow-evidence-2026-09-08.md).
The [thin Dagster chain](./design/committee-flow-orchestration.md) now passes
exact input mapping, partition-aware checks, and real 2024 replay. The
[read-only investigative API](./design/committee-flow-api.md) now implements
paginated graph/evidence queries and targeted source lookup. Neither gate
deploys a public service or verifies live weekly daemon operation.

## Schedule E verification command

The processed independent-expenditure adapter exposes a data-row-only COPY
verification boundary:

```text
legal-tender pipeline fec verify-schedule-e \
  --input <schedule-e.copy-or-stdin> \
  [--cycle <even-year>] \
  [--expected-rows <rows>] \
  [--expected-bytes <bytes>] \
  [--expected-sha256 <sha256>]
```

The command uses the shared PostgreSQL COPY decoder, requires exactly 80
fields, validates UTF-8, source nulls and escapes, required identifiers,
numeric precision and scale, integer precision, timestamps, and an optional
row-level election cycle. It reports action, type, memo, date, negative,
fractional, and null profiles without selecting effective records or writing
database state. `--input -` streams from standard input.

The v2 publication path stages that one all-history relation, then selects
cycles only in the occurrence layer. Every selected physical row retains its
global row ordinal, byte coordinates, source digest, `sub_id`, and release
lineage. Fact publication replays the exact staged bytes and emits one
`fec.schedule_e_independent_expenditure.v1` fact per occurrence with all 80
source fields plus typed money, time, spender, payee, candidate, support/
oppose, conduit, certification, filer, and filing fields. It applies no
action, amendment, memo, estimate, or effective-spending policy.

## Verification state

The Go suite currently covers:

- compiled-schema drift against the exact archive observation;
- all eight exact COPY/canonical fixture pairs;
- null versus empty text and the full COPY escape families;
- wrong-width, missing-LF, and invalid-escape issue preservation;
- decimal, boolean, required-identifier, required-form, and period failures;
- complete zstd byte, digest, row, and validity checks;
- complete classic-flow physical validation, `SUB_ID` membership, named legacy
  cohort, possible two-sided signature, and repeated IE-key profiling;
- exact Schedule B catalog/schema binding, COPY decoding, numeric and time
  validation, selected-period enforcement, bounded cancellation, temporary-
  shard uniqueness, complete byte/digest conservation, and classic membership,
  endpoint-orientation, and amount-precision comparison;
- exact same-publisher-batch Schedule A/B object and fact lineage, complete A
  and B row conservation, bounded cohort matching, cross-class candidate
  conservation, and explicit no-money-merge output;
- exact Schedule E DDL, record-schema, and fixture-digest binding; strict
  80-field parsing; numeric precision and calendar-time checks; cycle
  selection; corpus profiling; and complete byte/digest conservation;
- v2 inventory equality, v1-to-v2 reuse of all 21 unchanged artifacts,
  one all-history Schedule E staged output, selected-cycle occurrence
  conservation, lossless source fields, exact signed cents, and idempotent
  fact publication;
- v3 inventory equality, v2-to-v3 reuse, one release-owned Schedule B
  archive with four `archive_direct` relations, unchanged 25-output staging,
  exact 98-column Parquet schema, lossless source lexemes and locators, typed
  money/time projections, deterministic shards, complete readback, global
  `SUB_ID` uniqueness, checkpoint replay, idempotence, and same-size corruption
  rejection;
- partial-pass behavior and cancellation; and
- compiled release-inventory equality with the language-neutral contract;
- metadata-only `HEAD` behavior, redirect metadata, closed unread bodies, and
  bounded concurrency;
- deterministic `update_available`, `no_change`, `source_not_ready`, and
  `invalid` planning, including all checked-in result fixtures; and
- storage gating before body requests, exact partial resume, complete
  acquisition, idempotent success replay, post-capture publisher-race failure,
  immutable finalization, and result validation;
- direct release-publication capture of exact plan, acquisition, and stage
  bytes into immutable content-addressed control paths, including idempotence
  and collision rejection;
- exact one-section COPY filtering with lossless preservation of malformed
  data rows, zstd round-trip conservation, per-output checkpoint recovery,
  and idempotent completed-stage replay;
- locked publication, immutable-manifest reuse after interruption, atomic
  active-pointer replacement, stale-baseline rejection, and idempotent active
  release replay;
- occurrence conservation, exact byte locators, invalid-row and duplicate-key
  issue preservation, globally sorted bounded-memory natural indexes,
  semantic escape equivalence, added/changed/absent comparison, storage gates,
  source ancestry, and idempotent publication;
- exact classic fixture mapping for all five products, strict physical and
  semantic validation, per-dataset change sets, immutable-pointer checks,
  normalized source assertions, signed-cent summary parsing, and duplicate
  exclusion from singleton facts;
- Schedule A source-to-fact ancestry, complete source-field preservation,
  signed and scaled money values, null timestamps, memo-X derivation,
  duplicate exclusion, and bounded row-selection state;
- receipt decision order, signed positive/negative/zero conservation,
  date-bounded summary comparison, conflicting/shared authorization isolation,
  checked overflow, immutable dual-artifact publication, and idempotent reuse;
- direct staged-source routing through the same receipt decision function,
  complete source-digest checks, candidate result conservation, and exact-zero
  reconciliation-band reporting;
- equal-row zstd and Parquet layout generation, immutable ordinal-range
  shards, all-column semantic round-trip, projected-column equivalence,
  receipt-decision conservation, and bounded acceptance gates;
- exact 99-column Schedule A Parquet schema mapping, lossless source and typed
  projection round-trip, deterministic shard and byte ranges, resumable
  checkpoint reuse, corrupt published-shard rejection, source/fact
  conservation, and idempotent columnar publication;
- receiver-reported committee-flow decision order, exact receipt-role
  classification, sparse exceptions, grouped-result and signed-cent
  conservation, immutable replay, and loader backing verification;
- CLI dispatch, strict JSON inputs, and versioned JSON output;
- Dagster definition loading, Monday planning schedule, six guarded sensors,
  separate release and evidence jobs, dynamic cycle and 20-slice classic
  fanout, and retry wiring;
- exact subprocess argument forwarding, contract validation, immutable result
  storage, and materialization metadata; and
- a Python import-boundary test that rejects domain and data-client imports
  from `orchestration/`.

A historical v1 metadata-only run on 2026-08-29 resolved every required FEC URL through
the documented redirect path and obtained usable S3 object-version metadata
for all 21 sources. It transferred no artifact bodies.

A live one-million-row smoke pass over the retained 2025/2026 zstd extract
validated every row without materializing an uncompressed file. The subsequent
complete pass validated all 166,293,056 rows with zero invalid rows and exactly
reproduced the accepted 114,190,988,781-byte uncompressed digest and
10,587,087,035-byte compressed digest. It completed in 469.606 seconds with a
measured peak resident set of 8,316 KiB and no swap or output-file writes.

The complete 2024 overlap pass compared 264,085,601 processed rows with
58,208,756 classic `indiv` rows and 18,667,435 classic `oth` rows. Every input
row passed its applicable audit gates and every source-specific `SUB_ID` was
unique. The processed relation contained 168,856,391 accepted itemized-
individual rows worth $3.158 billion that appeared in neither classic product.
Full findings are in the
[classic-file overlap audit](./audit/schedule-a-classic-overlap-2026-08-28.md).

The complete classic-flow pass validated 703,597 `pas2` rows and 18,667,435
`oth` rows in 17.735 seconds. Every `pas2` ID appeared exactly once in `oth`.
The subsequent real Schedule E comparison accepted Schedule E as the IE source
authority and found that current `pas2` truncated fractional dollars on 9,367
of 14,732 exact shared 2026 rows. Full evidence is in the
[classic-flow and Schedule E audit](./audit/fec-classic-flow-and-schedule-e-2026-08-31.md).

The strict Schedule E adapter then replayed the complete 548,318-row current
relation in 1.298 seconds with zero invalid rows and reproduced its exact
367,801,873-byte COPY identity. It preserved 3,112 negative amounts, 11 null
amounts, 2,895 null expenditure types, 45,669 memo-X rows, and every observed
action state. Full evidence is in the
[Schedule E source-contract audit](./audit/schedule-e-source-contract-2026-08-31.md).

The coordinated v2 release then published the same all-history relation once
and selected 75,884, 68,356, 67,292, and 14,935 lossless facts for cycles 2020
through 2026. Every fact passed normalization and all eight compressed
occurrence/fact artifacts passed a post-publication digest check. The direct
dump and restored-table COPY streams differ only in row order and have the
same complete sorted-row digest. Full evidence is in the
[Schedule E v2 publication audit](./audit/schedule-e-v2-publication-2026-08-31.md).

The effective independent-expenditure publisher then applied the accepted
processed-regular-report policy to all four cycle fact sets. It conserved
202,556 included facts and $10,365,314,341.60 in exact signed amounts into
201,250 attributed facts plus 1,306 sparse unattributed exceptions. Memo-X
facts remained evidence but did not enter the total; no notice-like or
unresolved non-memo amount reached publication. A 2024 replay reused the exact
calculation identity and artifacts. Dagster invokes this Go boundary through
one partitioned asset; it contains no Python record logic. Full evidence is in
the [effective independent-expenditure audit](./audit/effective-independent-expenditures-2026-08-31.md).

The first coordinated source release and all five 2024 classic fact families
were published on 2026-08-30. The 2024 Schedule A occurrence pass validated
264,085,606 unique rows but produced 113.45 GB of occurrence, natural-index,
and bootstrap-change JSON. A subsequent Schedule A fact attempt was stopped at
113 million rows after producing 33.68 GB of unpublished JSON.

The replacement direct calculation probe validated the complete 2024 relation
and calculated all candidate results in 439.454 seconds. It wrote only a
1,016,233-byte compressed result artifact. Full findings are in the
[direct receipt probe audit](./audit/direct-receipt-probe-2026-08-30.md).

A subsequent ten-million-row layout gate compared equal-row zstd COPY with a
lossless Parquet candidate. Parquet added 7.54% compressed bytes, scanned the
five calculation columns 4.18 times faster, round-tripped the complete logical
row digest, and made identical receipt decisions with 267 MB peak RSS. It was
accepted for a complete-corpus probe at that point. Full findings are in the
[physical-layout benchmark audit](./audit/schedule-a-layout-benchmark-2026-08-30.md).

## Not yet shipped

No production application cutover has occurred. The active coordinated v4 source
release and four release-bound committee-summary fact sets now exist. Earlier
2024 classic facts, compact 2024 Schedule A occurrence evidence, and
the accepted columnar Schedule A fact set and compact candidate-receipt
calculation exist. The coordinated fact bundle and calculation automation also
exist. The per-row decision JSON representation remains rejected for real-
corpus use. The implementation does not yet publish production ArangoDB domain
state, entity/current-profile projections, transfer or total-receipt
calculations, or a deployed investigative application. The read-only
committee-flow API and isolated receiver-flow/history gates now exist; unified
production publication remains separate work. The outside-spending readiness
bundle and Dagster automation are complete. The candidate-resolution,
resolved grouping, replacement readiness bundle, and v2 projection gates pass
unchanged for 2020, 2022, 2024, and 2026 with zero missing graph masters.
Schedule B's physical, classic-comparison, same-publisher-batch Schedule A,
v3 release-membership, and complete lossless 2024 columnar-publication gates
pass. The accepted reporting calculation defines a precise non-memo subtotal
and form-line roles. Typed sender membership and fact-level candidate
reconciliation also pass for 2024. Uncertain selection remains explicit;
economic-flow assertions, automated publication, and a new graph contract
still block Schedule B graph use.
Committee history refresh policy still requires separate source work.
Committee-summary source publication and exact assertion grouping pass;
scope-qualified same-release receipt comparison remains separate. Schedule E has an accepted source boundary, complete-corpus
parser, v2 membership, selected-cycle occurrences, lossless facts, and four
published policy-bearing calculation sets. The cross-cycle methodology and
identities are in the
[resolved outside-spending audit](./audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md).
Raw electronic filings remain deferred.
