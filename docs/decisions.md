# Decisions Log

Append-only record of architectural and operational decisions, ordered by date. Earlier entries (Feb 6-7, 2026, originally `PIPELINE_FIXES.md`) are the data-model decisions for the funding-channels work. Later entries are the professionalization-effort decisions.

When a non-obvious choice gets made, append a dated entry here with: what we decided, what we considered, why we picked the chosen path, and what still leaves open.

---

## 2026-08-27 — Rebuild Legal Tender by product contract, legacy specification, and fresh Go implementation

The current Python system was designed incrementally while the campaign-finance
domain was being discovered. It contains valuable tracing, classification,
entity-resolution, and validation knowledge, but its module boundaries and
data model are not the target architecture.

Decision:

1. Follow the method used for the successful Found Footy rebuild: define the
   desired product, freeze legacy behavior as a separate functional
   specification, classify which behaviors survive, prove the risky design
   choices against real data, implement vertical slices alongside the legacy
   system, and cut over with differential validation and rollback.
2. Build a fresh Go application rather than porting Python assets or
   refactoring the Dagster implementation in place.
3. Use ArangoDB as the working primary domain database because path retrieval,
   neighborhood exploration, graph projections, and investigative graph output
   are product requirements. Validate the exact ontology, queries, indexes, and
   memory budget with real-corpus probes before locking the implementation.
4. Use Dagster as the data control plane. Restrict Python to Dagster
   definitions and one thin adapter that launches versioned Go operations and
   maps their structured results to Dagster events. Go owns every source,
   parsing, domain, graph, calculation, change-propagation, API, and UI concern.
   Temporal, OpenLineage, and Marquez are not initial dependencies. Dagster
   provides operational asset lineage; Go preserves evidence and calculation
   lineage as domain data.
5. Keep desired product behavior, observed legacy behavior, target design, and
   as-built truth in separate documents so historical implementation does not
   silently become a requirement.
6. Preserve source records and normalized facts at the finest reliable
   disclosure grain, then derive totals and classifications as versioned
   projections. The legacy five channels, three semantic domains, terminal
   attribution, donor tiers, corporate rollups, and future alignment measures
   are views over facts rather than destructive storage categories. Materialized
   aggregates may optimize reads only when they retain lineage and can be
   rebuilt and regrouped.
7. Treat each FEC two-year election cycle as an independent calculation and
   query dimension. Default the product to the latest four available cycles,
   including the active cycle, but retain all ingested history. Cross-cycle
   views aggregate only after per-cycle calculation and must disclose their
   cycle set, source coverage, partial-cycle status, and monetary basis.
8. Give no legacy behavior a presumption of parity. Existing parsers, storage
   boundaries, thresholds, identity heuristics, edges, classifications,
   aggregates, and presentations are evidence to reconsider. A behavior is
   preserved only when an independent product, source, or domain contract
   justifies it; differential parity applies only to explicit `KEEP`
   dispositions.

The initial product definition is
[`design/product-contract.md`](./design/product-contract.md), with concrete
questions and unresolved product choices in
[`design/investigative-questions.md`](./design/investigative-questions.md).
The legacy-behavior method starts at
[`design/legacy-functional-spec/README.md`](./design/legacy-functional-spec/README.md).
The enforceable language boundary is in
[`design/go-dagster-boundary.md`](./design/go-dagster-boundary.md). This
supersedes the earlier working direction toward Temporal; the Python
constraint is an architecture invariant, not a best-effort target.
The first source-to-query implementation contract is
[`design/first-vertical-slice.md`](./design/first-vertical-slice.md).

Still open: the initial default presentation over the preserved facts,
historical backfill depth, public/private presentation, path-ranking defaults, personal
donor materiality, ideology analysis, lobbying-to-official context, override
governance, and cutover exports. These remain product decisions rather than
being chosen implicitly by the current code.

---

## 2026-08-27 — Use processed Schedule A snapshots and explicit receipt calculations

The first vertical-slice review tested the legacy `indiv.zip` assumption against
the local corpus and current FEC source contracts. The classic individual-
contributions file is a thresholded subset of itemized Schedule A, while the
FEC's weekly processed Schedule A dump contains richer contributor, conduit,
filing, action, transaction, and classification fields. The local million-row
2024 sample also contains negative and zero amounts, reused transaction IDs,
several action states, and memo-code rows with heterogeneous text. A simple
bulk-file sum or “latest amendment” heuristic cannot be the target contract.

Decision:

1. Use the FEC processed Schedule A weekly database dump as the canonical
   detailed-receipt source. Keep classic `indiv.zip` only as a legacy and source
   comparison input. Its PostgreSQL distribution format does not change the
   ArangoDB domain-database decision.
2. Preserve every captured Schedule A publisher snapshot and every row at
   source grain. Retention starts when Legal Tender begins capturing the weekly
   source; it does not claim to recover historical filing revisions that the
   processed snapshots no longer contain.
3. Treat the processed snapshot as the FEC's then-current row set. Preserve
   action, filing, transaction, original-sub-ID, and back-reference fields, but
   do not reconstruct amendment chains or economic-event identity from those
   fields alone. Raw `.fec` filing history requires a later source and
   calculation contract.
4. Define the first itemized-individual subtotal as valid signed Schedule A
   amounts where the FEC `is_individual` classification is true and memo code is
   not `X`. This matches openFEC's `memoed_subtotal` definition. Preserve `X`
   rows as evidence; memo text alone does not control subtotal inclusion.
5. Count an included earmarked receipt once as reported to the filing
   committee. Preserve conduit and memo fields. Do not replace the contributor,
   add a conduit receipt, take a maximum of direct and earmarked totals, or use
   a name-substring conduit rule.
6. Calculate candidate scope only through same-cycle `ccl` facts with `A` or
   `P` designations. Keep conflicting relationships unresolved instead of
   inferring authorization from a name or another cycle.
7. Keep calculated itemized amounts and FEC summary amounts separate. Compare
   itemized detail to an explicit source itemized field when available. A gap
   from a total-individual field is not automatically “unitemized,”
   “grassroots,” or “unknown.” Never replace missing detail with a summary
   value.
8. Store monetary values as signed integer cents plus raw source text. Preserve
   negative and zero rows and expose partial or blocked results when
   classification, amount, linkage, or publisher-reference evidence is
   unresolved.

The logical contracts are
[`design/evidence-model.md`](./design/evidence-model.md) and
[`design/calculation-contracts.md`](./design/calculation-contracts.md). The
source-to-API slice and asset graph now use Schedule A receipt facts instead of
the legacy individual-contributions parser.

Still open: streaming versus temporary-PostgreSQL extraction from the custom
dump, the exact ArangoDB document/edge shape, raw-filing historical backfill,
and later calculations for transfers, refunds, loans, candidate contributions,
conduit relationships, and terminal-source attribution.

---

## 2026-08-27 — Put reviewed source contracts before parsing and graph design

The legacy implementation contains useful publisher research and current FEC
header files, but its parser treats an ordered field-name list as the complete
schema. It pads short rows, truncates long rows, ignores decoding errors,
converts money to floats, and replaces duplicate document keys. Those behaviors
can make a stale schema look successful while losing the evidence needed to
find the error.

Decision:

1. Define every target source through three versioned boundaries: transport,
   physical schema, and semantic normalization. Graph projections and
   calculations consume normalized facts; they do not repair source parsing.
2. Store machine-readable source contracts as reviewed JSON embedded in the Go
   build. Validate the contracts with a repository JSON Schema. Keep complex
   semantic calculations in versioned Go code referenced by contract ID.
3. Treat official headers, data dictionaries, code lists, API responses, and
   database metadata as pinned evidence. Record their digests and require them
   to agree with accepted contracts, but never let a remote schema change a
   scheduled parser without review.
4. Capture a changed or unknown artifact immutably, then block publication of
   the affected dataset. Do not discard the bytes. Accepting drift requires
   old/new fixtures, updated source evidence, a contract-version change, parser
   tests, and an explicit reprocessing decision.
5. Preserve every record occurrence, raw field value, physical field count,
   decode state, duplicate state, and immutable locator. Missing fields, extra
   fields, invalid values, absent identifiers, and duplicate publisher keys are
   evidence states. They are never repaired by padding, truncation, ignored
   bytes, skipped rows, float conversion, or replacement writes.
6. Build the common contract/manifest/occurrence/publication machinery first;
   prove it with the small 2024 `cn`, `cm`, and `ccl` sources; then complete the
   accepted processed-Schedule-A vertical slice. Schedule B and E follow for
   committee flow and independent spending.
7. Rebuild lobbying ingestion against LDA.gov filings, clients, registrants,
   lobbyists, constants, and LD-203 contribution reports. Lobbying stays a
   distinct evidence domain. A resolved LD-203/FEC overlap links two records of
   the same activity and never adds the amount twice.
8. Replace the legacy Congress client when congressional context is needed.
   Use official House and Senate sources for facts the current Congress.gov API
   does not distribute; retain the community-maintained legislator YAML only as
   a secondary crosswalk.

The common contract is
[`design/source-contracts.md`](./design/source-contracts.md), the implementation
order is [`design/source-catalog.md`](./design/source-catalog.md), and the local
evidence is in the
[`2026-08-27 source-boundary audit`](./audit/source-boundary-2026-08-27.md).

Still open: the exact JSON contract schema, storage format for large occurrence
indexes, Schedule A extraction path, source-specific effective-record rules,
and which later federal or organization sources move into the first complete
product.

---

## 2026-05-21 — Memory-cap discipline + JDPAC vs UDP classification inconsistency

Two findings worth capturing while the context is fresh.

### Memory caps as shared-host discipline (commits `9b1fc65`, `a03ea8b`; long-exposure companion `347c8cd`)

The 2026-05-16 OOM event (kernel killed arangod mid-pipeline at 40 GB anon-rss; docker auto-restarted; data preserved on bind volume) had a structural root cause that goes beyond any single project: **luv is a 125 GB shared host, but every memory-hungry container was running without an explicit cap.** TimescaleDB in long-exposure auto-tuned shared_buffers to ~25% of host RAM (≈ 31 GB resident even idle); legal-tender's arangod advertised a 64 GB total budget assuming it owned the box; long-exposure's worker JVM was sized at -Xmx32g from a Sprint 2 OOM episode that was later refactored away.

Discipline adopted across both projects:

1. **Every container gets an explicit `mem_limit`.** Docker enforces it via cgroups; the kernel back-pressures (cache eviction, query rejection) instead of OOM-killing random host processes. A soft cap is not a cap.
2. **Internal tuning is sized for the cap, not for host RAM.** ArangoDB's `ARANGODB_OVERRIDE_DETECTED_TOTAL_MEMORY: 28GB` tells it "you live in a 28 GB world" instead of letting it auto-tune for the 125 GB it can see. Same for RocksDB block-cache (12 GB), query memory-limit (12 GB), arango cache (4 GB). Worst-case: 12 + 6 + 4 = 22 GB steady + 12 GB transient query = 34 GB, slightly over the 32 GB cap, kernel evicts cache rather than killing the process.
3. **Prod compose mirrors dev** even when prod isn't deployed. A foot-gun parked in `docker-compose.yml` will go off the day prod first runs.
4. **Host-wide budget table** in `~/.claude/CLAUDE.md` (user-global) is the cross-project source of truth. Per-project compose files cite it.

Net result: declared-cap sum across legal-tender + long-exposure dropped from 80+ GB observed peak to ~48 GB. Host went from 49 GB used / 75 GB avail to 20 GB used / 105 GB avail immediately after applying.

**Discipline takeaway:** on a multi-tenant host, "the container will only use what it needs" is a lie that auto-tuned databases tell themselves. Every long-lived container needs (a) an explicit Docker `mem_limit`, (b) internal config sized for that limit, not for host RAM. Cgroups enforcement is the only honest budget mechanism.

### JDPAC vs UDP classification inconsistency (deferred)

Discovered while investigating Hamawy's donor network: Justice Democrats PAC (`C00630665`) is classified `terminal_type=passthrough`, but the AIPAC Super PAC United Democracy Project (`C00799031`) is classified `super_pac_unclassified`. Both are *the same shape* — CMTE_TP=W (Independent Expenditure-Only PAC), ORG_TP empty, CONNECTED_ORG_NM null. Both do massive IE spending ($14.9M and $209M total receipts respectively).

The Phase 2a/2b parent-org / name-cluster inheritance rules (shipped 2026-05-12) put JDPAC into `passthrough` via some name-pattern path that UDP didn't match. Both classifications lead to "trace upstream" behavior in the algorithm, so the practical attribution result is similar — but the inconsistency is real and worth fixing.

**Fix queued (not shipping today)**: a Phase 1 rule that handles IE-only Super PACs without industry classification before the Phase 2 inheritance gets a chance:

```
IF CMTE_TP = 'W' AND ORG_TP IS NULL/empty AND CONNECTED_ORG_NM IN (NULL, 'NONE')
   → super_pac_unclassified
```

Catches JDPAC, brings it in line with UDP. Also exposes any other IE-only Super PACs we've miscategorized via name-pattern inheritance.

**Companion audit script**: enumerate committees with identical (CMTE_TP, ORG_TP, CONNECTED_ORG_NM=NULL) that landed on different terminal_types. The output is the work-list for class-consistency cleanup. One-shot tool; could become a periodic gate.

### Smaller things worth noting

- **The "$0 amount on JDPAC edges" alarm was a script bug, not a data bug.** Edge collections store amounts under `total_amount`, not `amount`. The pipeline (`candidate_upstream.py:675`) reads the correct field. Investigation queries that used `e.amount` returned None and were misread as missing data. JDPAC's real IE: $1.35M for Bush, $946K for Bowman, $943K for Summer Lee. Mentioned here so the false alarm doesn't get re-investigated.
- **Donor-name substring matching audit**: only one offender remains — `is_conduit()` / `CONDUIT_PATTERNS` in `candidate_upstream.py`. The committee-classification side moved off name substrings months ago; the resolver uses name as lookup key only. The conduit filter is the last hardcoded donor-name list and is queued for replacement by EARMARKED-memo-share structural detection. Employer-side substring matching (`RETIRED`, `SELF-EMPLOYED`, `HOMEMAKER`, etc. in `employer_normalization.py`) is defensible — FEC's `EMPLOYER` field is free-text human input with no structured taxonomy, so substring-detecting "what people type when they're not employed at a real company" is the only available signal.

---

## 2026-05-16 — The pipeline was confidently misrepresenting candidates with thin FEC data; weekly sync turned on; donor_detail_coverage now surfaced

Session triggered by a user question: "what about Adam Hamawy in NJ-12, I think he's clean." The pipeline confidently reported Hamawy as `99.4% grassroots, 0.4% whale, 0% unaccounted, $544K direct funding`. **Every part of the breakdown was made up.** We had zero itemized donor records for him; the pipeline silently fell back to FEC's summary totals and dumped the whole amount into "grassroots" as if that were a real classification.

### What we'd actually built vs what we'd actually shipped

By the time this session started we had:

- A working two-phase trace algorithm with cycle break + multiplier caps
- A simplified Wikidata resolver (reconci.link → GLEIF) with comprehensive _NON_EMPLOYER_QIDS reject set
- Phase 3.5 same-entity merge consolidating Pan Am Systems + Railways into one Mellon entity ($923M)
- Trade-class P31 refinement, recursive IE trace, parent-org inheritance generalization
- A four-gate validation contract (bulk median |Δ| 2.4%, named-candidate diff, target case, pytest)

What we had NOT built:

- A data-freshness mechanism that actually ran
- Honest surfacing of when our donor-level detail is missing vs present
- Validation that distinguishes "our totals match FEC's totals" from "we have donor detail to back our totals"

The validation methodology had a hole. The bulk median |Δ| check compared our `funding_channels.total_funding` to FEC's `weball.TTL_RECEIPTS`. For candidates with no itemized records, our number literally *is* FEC's number copied through (via the `total_from_individuals_source='fec_summary'` fallback in `committee_receipts`). The check was tautological exactly for the cases that needed checking.

### The three bugs

**Bug 1 — Data was 5+ weeks stale.**

Last sync 2026-05-07. Raw FEC files from 2026-05-06. `indiv.zip` in our DB only covered through 2025-12-31 (Q4 2025); Q1 2026 filings (deadline April 30) weren't ingested. The bug isn't that the FEC parser was wrong — the bug is that *nothing was scheduled to refresh the data*.

Root cause: `src/schedules/__init__.py` had `default_status=STOPPED`. The original design said "manual start in dev, flip on in prod" — and no prod deployment ever happened. The schedule sat in STOPPED indefinitely.

Compounding root cause: `src/schedules/` was being matched by the too-broad `.gitignore` pattern `schedules/` (intended to ignore Dagster's runtime schedule-storage directory). The source-of-truth schedule file was never tracked in git, never appeared in PR review, never got challenged. Nobody could see that STOPPED was the default because git was hiding the file.

**Bug 2 — fec_summary fallback silently created fake whale/grassroots splits.**

When `committee_receipts` doesn't have itemized indiv records for a committee, it falls back to FEC's webl/weball summary totals. That fallback is correct (the alternative is reporting $0 total receipts, which is worse). But:

- `whale_donor_total = 0` (no records to identify whales from)
- `small_donor_total = total_from_individuals` (entire amount → grassroots by default)
- `donor_detail_coverage` was never computed or surfaced

Downstream, `candidate_funding` produced output that looked identical for a candidate with full indiv detail vs a candidate with zero indiv records — same shape, same fields, same percentages. The output was structurally incapable of expressing the difference.

Hamawy's case: $544K showed as "99% grassroots." Zero itemized records existed. The grassroots number was a relabeled summary.

**Bug 3 — `unaccounted: 0.0%` lied.**

Because total_receipts (from summary) matched total_accounted (the same number routed to grassroots), the unaccounted residual was 0 — implying perfect attribution. The actual uncertainty (we have no donor-level detail for the entire amount) wasn't anywhere in the output. `unaccounted` was reading the data-flow tautology as confidence.

### Fixes shipped this session

1. **Weekly schedule turned on by default.** `weekly_fec_refresh` now reads `DAGSTER_SCHEDULES_ENABLED` env (default RUNNING; opt-out via 0/false/off/no). Cron `0 2 * * 0` America/New_York. Goes live this Sunday 2 AM.

2. **`.gitignore` exception for `src/schedules/`.** Force-added the file. Future schedule changes are PR-reviewable.

3. **`individuals.data_quality` block in candidate_funding output.** Per-cycle and aggregate. Fields:
   - `detail_coverage` — fraction of individuals total backed by itemized records
   - `individuals_itemized` — dollars from indiv.zip
   - `individuals_summary_only` — dollars from FEC summary fallback
   - `primary_source` — `indiv_zip` | `fec_summary` | `mixed` | `unknown`

4. **view_candidate.py warning banner** before Ch4 breakdown when coverage < 99.9%. Names the dollar amount that's summary-only and the primary source.

5. **Manual sync kicked off.** `data_sync` re-ran 2026-05-16; got fresh `indiv.zip` (~60MB larger). FEC parsers re-materializing as this is written. Then candidate_funding + validation.

### What's NOT yet fixed

- The `unaccounted` field still says 0% for summary-only candidates. Should be expanded to include the `individuals_summary_only` dollars as "uncertain attribution" — but that's a follow-up; this commit focused on making the data quality visible, not yet on changing what `unaccounted` reports.

- Validation methodology still compares totals to totals. Needs a separate gate: "for candidates with ≥N itemized records, our whale/grassroots breakdown matches the records." Otherwise the bulk-median gate continues to be tautological for thin-data candidates.

- Per-candidate data freshness (max TRANSACTION_DT under each candidate) not surfaced. Currently you have to query the DB to find out which cycle's data is "current" for a given candidate.

- The schedule turning on doesn't mean the pipeline succeeds. Need monitoring/alerting on the Sunday job: did data_sync complete, did indiv parse, did committee_receipts update? Otherwise we discover a stuck pipeline only when someone notices the data is stale again.

### The non-code lesson

This is the second time in the project history that "the validation passed" gave false confidence. The first was the trace algorithm producing $16T totals for House races (May 2026, fixed via cycle break + caps). Both times the validation methodology was missing the failure mode it was supposed to catch.

When validation is too narrow, "passing" is worse than "failing" — it converts an open problem into a closed one in our heads, and we stop looking. Worth being more honest about what each validation gate actually proves (totals vs detail vs attribution vs coherence) instead of treating "all gates pass" as a synonym for "the system is correct."

---

## 2026-05-14 — Generic-string rejection + same-entity merge for corporate_families

Session focused on cleaning up two classes of misresolution visible in `by_organization` cross-cuts:

1. Wikidata's reconci.link returning the wrong kind of entity for short/ generic FEC employer strings — "TARGETED VICTORY" → corp (actually a digital ad agency), "PRESIDENT" → corp (a job title FEC donors typed in the employer field), "United States Department of the Army" / "State of Nebraska" → corp (federal-agency / state-name leakage), "Asana Journal" → wrong Q-id for a generic word.

2. Genuinely-same real-world entities resolving to distinct Wikidata Q-ids and showing up as separate corporate_families. Pan Am Systems ($616M) and Pan Am Railways ($308M) are both Timothy Mellon entities; showing them as $923M one Mellon entity is what the data actually means.

Three commits this session: `eb38496` (audit-derived terminal-node cleanup, shipped earlier in the arc), `c47543a` + `0a7c334` (P31 reject set evolution), `fd2c7d1` (P749 same-entity merge).

### Audit-derived terminal-node cleanup (commit `eb38496`)

Four principled fixes from the 2026-05-13 terminal-node audit:

- `" SEPARATE SEGREGATED FUND"` added to `_CMTE_NAME_SUFFIXES`. SSF is FEC's own legal term for the PAC arm of a corp/union; recognizing FEC vocabulary, same shape as other suffix entries. Catches AANA + similar.
- Prefix-stripping for "POLITICAL ACTION COMMITTEE OF THE X" / "PAC OF X" in `_pac_search_name`. Symmetric to existing suffix-stripping; FEC has two equivalent naming conventions. Catches AAOS.
- Phase 1b stale-flag cleanup pass clears `terminal_type_refined_from_m_org_wikidata` / `terminal_type_wikidata_*` at start. Prevents IFW/ASIS-style cases where a re-classification doesn't take because the flag from a prior run persists.
- Phase 2a CONNECTED-match relaxed to prefix-match (≥10 chars guardrail) for super_pac_unclassified → labor_union inheritance. Catches UFCW SPAC ($20M), UNITE HERE PAC ($27M), IUOE SPAC ($20M), USW WORKS ($9M), CA Nurses PAC ($8M) — ~$85M proper org-rollup.

Each fix passes the four-gate contract.

### Resolver discipline shift: from reactive list-growth to comprehensive categorical reference data (commits `c47543a` + `0a7c334`)

The initial fix for the misresolutions was reactive: add a list of "bad" Q-ids to reject when seen as top reconci hit. This was the right *shape* (using Wikidata's own classification to gate acceptance) but the *application* was wrong: I added entries one-at-a-time as new wrong matches surfaced. The user pushed back hard:

> "make sure that your solutions arent arbitrary hardcodes"
> "it seems like we're hardcoding again"

The principled resolution: **comprehensive categorical reference data, built once up front by enumerating Wikidata's relevant taxonomy categories, not grown reactively per-case.**

Concretely, `_NON_EMPLOYER_QIDS` expanded from 12 ad-hoc entries to ~35 entries organized in 9 categories matching Wikidata's own ontology:

- Government / political entities (Q35657 US state, Q910252 head of state, etc.)
- Sovereign-state / country (Q6256, Q3624078)
- Administrative-territorial / settlement (Q15642541 admin region, Q486972 human settlement, Q515 city, Q3957 town, Q532 village, Q15284 municipality)
- Offices, positions, occupations (Q17279032 federal department, Q4164871 position, Q12737077 occupation, Q28640 profession, Q11488158 administrative occupation)
- Creative works (Q11424 film, Q571 book, Q7725634 literary work, Q47461344 written work, Q5398426 TV series, Q21191270 TV series episode, Q482994 album, Q7366 song, Q386724 work, Q7889 video game, Q13442814 scholarly article, Q5633421 scientific journal)
- Concepts / abstract (Q34770 language, Q11879003 academic discipline, Q101352 family name, Q133327 patronymic, Q11173 chemical compound, Q12140 medication, Q134808 polysaccharide)
- Geographic features (Q23397 lake, Q4022 river, Q8502 mountain)
- Wikimedia administrivia (Q4167410 disambiguation page, Q4167836 category, Q13406463 list article)
- Humans (Q5 — top reconci hit being a person means the resolver should NOT attribute the donation to that person as if they were a corporation)

Plus an empty-types structural rule: if reconci returns a Q-id with no P31 classification at all, that's "an entity Wikidata has but hasn't classified" — too risky to accept as a corporation. Single rule, not a list. Method tagged `empty_types_no_classification` in the cache.

The Q5 (human) inclusion required making `_accept_candidate` parametric so the whale path (which DELIBERATELY wants human Q-ids as input — that's how we resolve "JAN KOUM" → companies he founded) doesn't get broken. New signature: `_accept_candidate(input_name, candidate, reject_types: frozenset = frozenset(), require_typed: bool = False)`. Employer path passes `reject_types=_NON_EMPLOYER_QIDS, require_typed=True`. Whale path passes the defaults.

Also: removed the "fall through to candidates[1] if candidates[0] rejected" logic. It generated absurd worse-than-reject fallbacks: "CEO" → "Clinical and Experimental Otorhinolaryngology" (real Wikidata journal Q-id), "Department of the Army" → "badges of the United States Army" (Q-id about military insignia). When the top hit is rejected, we return not_found rather than scraping the bottom of the candidate barrel. Lower hit rate, much higher precision — for FEC-corporate-attribution use the right trade-off.

GLEIF Layer 2 needed a parallel filter: when reconci rejected via these methods, the rejection should propagate; without it, GLEIF was rescuing some names with strict-match (USA → "United States" via GLEIF's strict_match_us). Added `_REJECTION_PREFIXES` filter at the resolve_batch boundary so GLEIF doesn't see those names.

The asset (`wikidata_resolution.py`) also gained a broader rejection-prefix list at the Phase 2 assemble step to drop these from corporate_families entirely (vs preserving them as not_found).

**Discipline takeaway**: when a categorical filter is the right structural answer, build it comprehensively by enumerating the relevant taxonomy, NOT reactively per-case. The categories themselves are reference data — generic, principled, bounded. Adding entries within an existing category as new patterns surface is acceptable; growing a list of one-off Q-ids per misresolution is not.

### Same-entity merge via shared Wikidata upstream Q-id (commit `fd2c7d1`)

Pan Am Systems (Q7129582) and Pan Am Railways (Q2048811) both have P112 (founder) = Q7807399 (Timothy Mellon). They're the same real-world Mellon entity. Without merging, by_organization shows them as $616M + $308M as if they were unrelated; with merging it shows $923M one Mellon entity, which is what the data means.

Mechanism (Phase 3.5 in `wikidata_corporate_resolution`):

1. For each corporate_family with a Wikidata Q-id, fetch P112 (founder), P127 (owned by), P749 (parent organization) upstream Q-ids.
2. Group families by shared upstream Q-id.
3. Apply guardrails; union-find for transitive clusters.
4. Pick canonical (highest total_influence), fold others' totals + member_employers + linked_whales in, record `merged_from` provenance.

**Guardrails** (each one rejects a real-data false positive surfaced in the 146-cluster dry-run):

- **n=2 only.** n≥3 catches "located in USA" (Q30) with 64 unrelated megacaps clustered, "founded by Elon Musk" (Q317521) with OpenAI/SpaceX/X/PayPal/Tesla clustered. Founder/HQ/exchange-listing are *not* same-entity signals.
- **Shared name prefix ≥6 chars after legal-suffix strip.** "Pan Am" / "Pan Am" = 7 ✓. "Citadel" / "Citadel" = 8 ✓. "Bain " / "Bain " = 5 ✗ (intentionally rejects Bain Capital + Bain & Company — same founder, distinct PE vs consulting firms).
- **Educational-institution exclusion.** Both names containing University/College/School → reject. Otherwise university campuses sharing a parent system (UIUC + UIC under "University of Illinois system") would over-merge.

**Considered alternatives:**

1. **Shared P749 (parent org) only — most strict.** Rejected because Pan Am's connection is via P112 (Mellon = founder), not P749. Loses the headline win.
2. **Match-canonicalize via Wikidata's `wd:parentItem` SPARQL closure.** Equivalent to walking parent orgs to a fixed point. Too aggressive — would walk Microsoft + Google + IBM up to "American multinational technology corporation" and try to merge.
3. **LLM-based same-entity detection on top-N candidates.** Doable on joi, but adds a moving part for a problem that's mostly solved with shape rules.
4. **Hand-curated `EMPLOYER_FAMILY_ALIASES` table.** Already exists; doesn't scale.

**Results, this run**: 11 merges. Headline Pan Am ($923M one Mellon entity). Clean wins: Bloomberg TV/Beta, Marvel Comics+Entertainment+Games (union-find), DreamWorks+Animation, Rocket Companies+Mortgage+Loans, Hilton, Coca-Cola, Capitol Records. Two minor false positives accepted: Universal Music Group + Universal Television (~$1M, different corporate trees — UMG ≠ NBCUniversal but the rule can't tell), Samsung Electronics America + Samsung Heavy Industries (~$1M, genuinely distinct subsidiaries that share parent corp).

**NOT caught** (no shared upstream — name-only variants): GREYLOCK + Greylock Partners; Adelson Drug Clinic + Adelson Clinic; ULINE INDUSTRIES + ULINE. These keep the `EMPLOYER_FAMILY_ALIASES` table relevant. Future path: name-similarity-only merger pass operating on canonical_employers, OR delete EMPLOYER_FAMILY_ALIASES if Adelson/Uline get proper Wikidata coverage.

**Cache**: `<cache_dir>/wikidata_upstream.json`, schema `{qid: {p112, p127, p749, fetched_at}}`. Incremental flush every 200 entities, atomic .tmp rename.

**Validation (all four gates pass):**

- Gate 1 (bulk median): 2.4% unchanged from baseline (merge changes WHO gets credit, not HOW MUCH — total receipts are conserved).
- Gate 2 (named-candidate): Trump by_organization shows Pan Am Systems at $19.89M consolidated (was 2 separate entries). Other top orgs (Department of Government Efficiency / Musk, U Line / Uihlein, Marvel Entertainment / Perlmutter, America First Policies / McMahon, Budget Suites / Bigelow) unchanged. Bacon, Sanders also stable.
- Gate 3 (target case): Pan Am surviving family doc has `merged_from: [{name: "Pan Am Railways", qid: "Q2048811"}]`.
- Gate 4 (pytest): 65/65.

**Open follow-ups:**

- The Universal Music/TV and Samsung Electronics/Heavy false positives could be filtered by examining whether the shared upstream Q-id is itself a "corporate holding company" type vs a "founder" type. P112 (founder) tends to over-merge sibling companies founded by same person; P749 (parent org) tends to over-merge subsidiaries that share a holding company. Future work: track which property the upstream came from and apply different thresholds.
- The dry-run found Citadel Enterprise Americas LLC + Citadel Securities as a candidate merge, but only Citadel Enterprise existed in this run's corporate_families (Citadel Securities was resolved to a different canonical or below threshold). Not a regression; just a coverage gap.

---

## 2026-05-12 + 2026-05-13 — Terminal-node fixes + view tool + classification refinement

Two-session arc focused on (a) making the terminal-node classification layer more trustworthy and (b) finally making the pipeline's output human-viewable. Validation methodology formalized in @docs/validation.md.

### 2026-05-12 morning — Baseline snapshot

`docs/audit/baseline-2026-05-12.md` captures the known-good comparison point every subsequent fix diffs against:

- Bulk validation: 11,567 candidate-cycle comparisons, median |Δ| = 2.4%, 65% within ±5%, 77% within ±10% of FEC `weball.TTL_RECEIPTS`.
- BWC sanity (Bacon NE-2) across all 4 cycles: -2.7% to -5.8%, consistent.
- Named-candidate top-15 `by_organization` for Cruz, Trump, Harris, Bacon, Sanders.
- terminal_type distribution: 16,587 campaign / 9,339 passthrough / 6,277 super_pac_unclassified / 2,179 corporation / 809 trade_association / 427 ideological / 394 labor_union / 57 cooperative / 31 unknown.
- Top 20 `super_pac_unclassified` by receipts: SLF PAC $1.14B, SMP $1.11B, MAGA Inc $521M, FAIRSHAKE $358M, AMERICA PAC $311M, DEMOCRACY PAC $279M, Club for Growth Action $263M, AIPAC's UDP $209M, etc. Cumulative ~$6.3B+ that was previously terminating the trace at these committees.

### 2026-05-12 — Phase 2a/2b parent-org inheritance generalization

The May-9 NEA-Fund inheritance fix was narrow: `corporation`-typed committees inherit from a sibling matched by `CONNECTED_ORG_NM`. Generalized:

- **Phase 2a**: source filter expanded to `corporation` OR `super_pac_unclassified` OR `unknown`. Same parent-lookup by `CONNECTED_ORG_NM`, with FEC's literal-string 'NONE' value treated as empty.
- **Phase 2b (new)**: when CONNECTED is empty/NONE, derive a `root_name` by stripping known committee-form suffixes (POLITICAL ACTION COMMITTEE, CONGRESSIONAL FUND, VICTORY FUND, INSTITUTE FOR LEGISLATIVE ACTION, ACTION, PAC, OF AMERICA, etc.). Cluster by root_name. Within each cluster of ≥2 members, if any has a specific terminal_type, all loosely-typed siblings inherit. Conservative min_root_length=8 prevents stripping-to-junk over-clustering.

52 committees ($350M in receipts) reclassified. Target cases all hit:
- Club for Growth Action ($263.5M): super_pac_unclassified → ideological (cluster=CLUB FOR GROWTH)
- NAR Congressional Fund ($60M): super_pac_unclassified → ideological (CONNECTED match)
- NRA Institute for Legislative Action: super_pac_unclassified → ideological (cluster=NATIONAL RIFLE ASSOCIATION)
- Communications Workers of America Working Voices ($35M): super_pac_unclassified → labor_union
- No Labels Action ($3.5M), Gun Owners of America ($1.1M), Moms for Liberty Action ($500K), American Chemistry Council: smaller flips

### 2026-05-12 — Recursive IE trace through passthrough Super PACs

`trace_ie_sources` was single-level: walk one hop upstream of the spending PAC, attribute terminal-type committees to by_corporation, dump everything else (including passthrough PACs) into by_pac. Stopped at the first passthrough — and because the dominant funding pattern post-Citizens-United is Super PACs funded by other Super PACs (SLF Fund ← One Nation ← donors), the *actual* donors disappeared into by_pac as opaque line items.

Made it recursive with the same Phase-1-propagate + Phase-2-attribute structure as `trace_committee_sources`. Same correctness guards (cycle break, per-edge fraction cap at 1.0, accumulated multiplier cap at 1.0). by_pac now only collects passthrough PACs whose receipts are zero/missing (rare — "trace ended here" signal rather than silently dropped attribution).

Aggregate effect:
- IE+ resolved to by_corporation: 22.3% (~$840M across all candidates)
- IE+ stuck in by_pac: 0.0%
- IE- resolved to by_corporation: 26.6% (~$1.73B)
- IE- stuck in by_pac: 0.1%

Also surfaced `by_individual` in the output (the recursive trace's individual attributions were happening internally but not written to the candidate document pre-fix). New `_top_individuals` helper preserves {name, amount, employer}; `_merge_individuals_list` for cross-cycle merge.

### 2026-05-12 — `scripts/view_candidate.py`

~470 LOC, `rich`-based terminal renderer. First time the pipeline's output is viewable by a human without writing an ad-hoc AQL query. CLI: `python view_candidate.py "<name or CAND_ID>" [--cycle YYYY] [--top N]`. Renders header, channels summary, Ch1 by type, Ch2/3 with top_pacs/by_corporation/by_individual/stuck-by_pac, Ch4 whale corp-connected + independent + grassroots, Ch5 unaccounted with breakdown, by_organization cross-cut with via_donors. Disambiguates name substrings to a CAND_ID list when ambiguous.

Visible output-quality issues surfaced (not blockers; expected per the resolver's documented trade-offs):
- "TARGETED VICTORY" misresolved as a corporation (it's a campaign-services firm)
- "Asana Journal" instead of "Asana"
- "PRESIDENT" / "United States Department of the Army" / "State of Nebraska" as org names (job-title / geographic leakage from FEC employer fields)

These are documented in @docs/validation.md "What we haven't validated" and `docs/todo.md` "Session 2 follow-ups."

### 2026-05-13 — M-ORG_TP refinement: token list → Wikidata P31

Initial implementation used a 50-entry hand-curated `_TRADE_PROFESSION_TOKENS` list (REALTORS, BANKERS, MANUFACTURERS, DENTISTS, ...) to refine ORG_TP=M committees from the catch-all `ideological` bucket to `trade_association`. Worked ($215M flipped, including AAJ, Council of Insurance Agents) but Vedanta correctly identified it as the exact "growing hardcoded list" shape we've been removing all session.

Considered alternatives:
1. Keep the list as reference data (same shape as `_STOPWORDS` / `_LEGAL_SUFFIXES`)
2. **Wikidata P31 lookup via reconci.link** ← chosen
3. LLM-based classification via Qwen on joi
4. Wikidata SPARQL with type filter (rejected — reintroduces the spaghetti we deleted on 2026-05-11)
5. OpenCorporates' industry codes (rejected — paid tier, single-source)
6. Collapse the distinction entirely

Chose option 2 because:
- Same infrastructure as employer/whale resolvers (no new dependency)
- Deterministic (same input → same output)
- Provenance is auditable (Q-id + class name stored per refined committee)
- The "list" is ~10 Q-ids from Wikidata's own taxonomy of professional-org types, all empirically observed in our corpus — different shape from the eye-curated string list

Implementation:
- For each Phase-1-classified `ideological` ORG_TP=M committee, derive a search name by stripping parenthetical content + PAC suffixes
- Reconcile via reconci.link, cached at `<cache_dir>/trade_assoc_classification.json` (schema-versioned: cache stores full P31 type list, `is_trade` derived at read-time so set evolution doesn't require cache invalidation)
- Top candidate's P31 types checked against `_TRADE_CLASS_QIDS`. If any match → flip.

`_TRADE_CLASS_QIDS` set (all empirically observed):
- Q2178147 trade association
- Q829080 professional association
- Q10729872 medical association
- Q4287745 medical organization
- Q1865205 bar association
- Q897399 chamber of commerce and industry
- Q18325460 501(c)(6) organization (US tax code for business leagues)
- Q16904718 agricultural organization
- Q114301854 veterinary medical association
- Q70363673 pharmaceutical societies

Run result: **33 committees ($116.7M) refined**. Coverage is ~$50M smaller than the token-list approach was, because Wikidata classifies AAJ ($24M trial lawyers) and Council of Insurance Agents ($17M) as `advocacy group` (Q431603) rather than trade. Defensible — those orgs do as much advocacy as trade representation; deferring to Wikidata's judgment is more principled than my eye-curated list.

Coverage gap accepted explicitly: AOPA / NFIB / BCBS Michigan / APTA / ACOG (~$30M total) — real trade orgs but Wikidata's P31 doesn't include any of our trade-class Q-ids. They stay ideological. Better default (under-flip rather than over-flip).

### Validation methodology documented

`docs/validation.md` written as the canonical reference for the four-gate validation contract:

1. **Bulk median** vs FEC weball.TTL_RECEIPTS (≤5% threshold; currently 2.4%)
2. **Named-candidate diff** for Cruz / Trump / Harris / Bacon / Sanders top-15 by_organization
3. **Target case** explicit per-fix pass/fail
4. **Pytest** 68/68

Plus catalog of what we've validated, what we haven't, and how to run each gate. All five fixes in this two-session arc passed all four gates.

---

## 2026-05-11 PM — Multi-signal corroboration + whale-path simplification + audit-driven deletions

Three commits this afternoon, building on the morning's resolver simplification.

### Commit 1 — `feat(resolver): multi-signal corroboration for low-confidence reconci matches`

Reconci.link returns a single 0-100 relevance score. Trusting only the high end (≥70) misses real matches that are under-scored because the FEC employer is a contraction or acronym of a much longer canonical name (WILMERHALE → "Wilmer Cutler Pickering Hale and Dorr" at reconci 49). Trusting the high end uncritically lets short-input fuzz matches through (RDV → "North Vietnam" at score 100, the poster-child false-positive).

Built `src/rag/name_match.py` with four deterministic signals:
- `acronym_match` (tries both with-and-without stopwords so "BCG" → "Boston Consulting Group" works AND "WH" → "Wilmer and Hale" works)
- `token_subset_concat_match` (in-order subset concatenation for portmanteau cases)
- `edit_distance_close` (Levenshtein ≤ 2 for typos)
- `token_containment_score` (bidirectional substring coverage)

Decision rule in `wikidata_resolver._accept_candidate`:
- score ≥ 70 → accept, unless input is ≤4 chars AND no signal fires
- 40 ≤ score < 70 → accept iff at least one signal fires
- score < 40 → reject

No LLM, no hardcoded case overrides — each signal is a general property of FEC-name variation. Live smoke: RDV correctly rejected via `short_input_no_corroboration`; WILMERHALE / GREYLOCK / BCG / BAUPOST GROUP all resolve. 42 unit tests added (microsecond-fast, no network).

### Commit 2 — `feat(whale): minimal person→company resolver, drop legacy filter chain`

The whale path (person-name → companies via P108/P1830/P39) was the last consumer of the pre-simplification filter chain in `wikidata_client.py`. Same architectural overhaul as the morning's employer path:

1. Reconcile name via reconci.link (no type filter — empirically reliable for distinctive person names)
2. Accept via the same HIGH/LOW threshold + corroboration rule used for employers
3. Extract company Q-ids from a fixed set of corporate-relationship properties: P1830 (owner of), P112 (founder), P169 (CEO), P488 (chair), P1037 (manages), P3320 (board member), P108 (employer), and P39 (position held) with the P642 ("of") qualifier
4. Fetch each company's English label via existing `_entity_data`

**Adding P1830 was the key recovery.** Kenneth Griffin's connection to Citadel is stored there, not P108. Wikidata's coverage of US hedge-fund founders is uneven; P1830 catches Musk/Adelson/Griffin while P108 catches Koum/Mellon/Koch.

`wikidata_client.py` shrank 416 → 150 LOC after deleting `_resolve_person_rest`, `_resolve_person_safe`, `resolve_people_rest`, `_should_reject_match`, `_is_generic_match`, `_is_government_entity`, the description-pattern constants, the 60-entry `_NON_CORPORATE_P31` set, and the parallel-worker helpers.

**Disambiguation gap accepted explicitly.** "JOHN ARNOLD" and "PAUL SINGER" disambiguate to wrong-person Wikidata entries (historical/religious figures rank higher than the hedge-fund founders). Same disambiguation problem OpenCorporates Layer 3 would address; deferred until that ships.

### Commit 3 — `chore: hardcode + dead-code audit, delete confirmed-dead modules`

Audit doc `docs/audit/hardcodes-2026-05-11-pm.md` catalogs every remaining hardcoded constant in `src/rag/` with keep/trim/delete verdicts. Executed the safe deletions from section 7:

- `src/resources/embedding.py` (320 LOC) — `EmbeddingResource` was registered in `Definitions(...)` but no asset declared it as a dep or called `embed_*`. Pure facade.
- `src/api/election_api.py` (67 LOC) — placeholder, never wired.
- `_wbsearchentities` in `wikidata_client.py` (20 LOC) — last caller was the legacy whale path deleted earlier in the session.
- `WIKIDATA_API_ENDPOINT` constant — only used by the deleted `_wbsearchentities`.
- `_FOUNDER_POSITION_QIDS` empty placeholder set in `whale_resolver.py`.
- `ORG_TYPE_QID` constant + 18-line dead comment about the long-deleted `wikidata_ontology` module.

~440 LOC net removed. 68 tests still pass.

### Honest summary of where the day landed

Three valid criticisms:
- We spent a full day on infrastructure (resolver simplification, audit, dead-code purge) with zero user-visible feature progress.
- Hit rate on corporate resolution sits ~55-65%. OpenCorporates Layer 3 + LLM input normalization would close most of the long-tail gap; both deferred.
- `candidate_upstream.py` is still 1,263 LOC and `pies_v3.py` is still in the tree at 679 LOC.

Three real wins:
- The resolver pipeline is now ~500 LOC of principled code (`wikidata_resolver.py` + `whale_resolver.py` + `name_match.py`) instead of ~1,800 LOC of filter-chain band-aids.
- Bulk validation median |delta| is 2.5%, 62% within ±5% of FEC published totals. The pipeline is more correct than it feels.
- The next session has a concrete plan with validation gates (see `todo.md` "Next session"): extend the parent-org-inheritance pattern, stop treating `super_pac_unclassified` as terminal, and ship a thin CLI to actually look at `funding_channels.by_organization`.

---

## 2026-05-11 — Corporate-identity resolution: delete the filter layer, accept reconci-top + GLEIF

**The 24-hour summary.** Spent a full day iterating through three increasingly elaborate filter architectures on top of the Wikidata reconciliation API:

1. Q-id whitelist (`_STRICT_CORPORATE_QIDS`) + description-pattern blacklist + suffix-retry list. Grew with every wrong match.
2. Ontology walker — replace the Q-id whitelist with a P279 (subclass-of) traversal that asks Wikidata itself "is this Q-id descended from an organization root, or from a non-employer root?" Required maintaining short subtree-root sets (ORG_ROOTS, NON_EMPLOYER_ROOTS) but grew the latter set each time a test exposed an ambiguous case (municipality, state, etc.).
3. Test suite — built a 79-test pytest fixture to catch corruption in seconds instead of after 90-minute full runs. Caught real bugs (Q22687 bank misclassified, parallel-walk cache corruption, mixed-ancestry walks producing non-deterministic results across cache states).

After all of this: the filter layer was fundamentally fighting Wikidata's classification. Wikidata thinks municipalities are organizations; our domain doesn't. Wikidata thinks the village of "Greylock" is at search-rank parity with the firm "Greylock Partners"; our domain wants the firm. Every new "fix" was a band-aid on top of a band-aid. We had several "this is the architectural fix" pivots in the same day, each one introducing new edge cases.

**The actual fix: stop classifying.** Delete the entire filter layer. Replace with:

1. Wikidata reconci.link top hit at `score >= 70` → accept
2. GLEIF strict-match fallback for not-founds → accept
3. Everything else → `not_found`

That's it. No `_NON_CORPORATE_P31` (60 entries — deleted). No `_GENERIC_DESCRIPTION_PATTERNS` (25 entries — deleted). No `_GOVERNMENT_DESCRIPTION_PATTERNS` (deleted). No ontology walker (deleted entirely — `src/rag/wikidata_ontology.py` gone). No YAML overrides (`config/wikidata_overrides.yaml` deleted). No subtree-root sets, no P279 traversal, no cache-corruption-recovery scripts.

Code: ~550 LOC of resolver → ~150 LOC. Tests stayed (~18 cases, 6 seconds). All 18 pass.

**Trade-off accepted explicitly.** We will accept some bogus matches that Wikidata's data quality produces:

- `RDV` → "North Vietnam" (score 100 fuzzy match)
- `STEYER` → "Steyr" (Austrian city, score 100)
- `CO-OWNER` → some unrelated entity

These show up in downstream `by_organization` cross-cuts where they're inspectable. For the FEC-corporate-attribution use case, missing a corporation (false negative) is worse than misattributing one (false positive — visible and recoverable). The earlier filter chain was tuned for precision over recall and silently rejected real entities (`LINKEDIN`, `BAUPOST GROUP`, `BALLMER GROUP` — all returned at score 100 from reconci, all wrongly rejected by the type filter).

**Architectural pattern documented**: `docs/corporate-resolution.md`. Layered fallback (knowledge graph → official registry → extension slots) with confidence threshold + provenance for review. Portable to other entity-resolution problems.

**Hardcode audit shipped**: `docs/audit/hardcodes-2026-05-11.md`. Each remaining hardcoded data structure classified as legitimate reference data (legal-form suffixes, non-employer placeholders), calibrated parameter (confidence threshold), or dead code targeted for deletion (~600+ LOC of the old filter chain still sits in `wikidata_client.py` waiting for removal).

**Open follow-ups deferred from earlier filter-era plans**:
- ~~Cache TTL enforcement~~ — the new resolver doesn't have the same cache-corruption pressure; the cache is just a per-name resolution result. TTL still useful but lower priority.
- ~~Re-walk False entries to fix corruption~~ — moot, ontology cache deleted.
- ~~OpenCorporates as Layer 2~~ — still on the table as Layer 3 for coverage extension. ~200M companies including most US private LLCs. Free tier 500/day; paid for production scale.
- LLM-based input normalization for garbled FEC names. Future Layer.

**Acceptance metric for "no regression on resolver simplification"**: 18-test pytest suite green. Full 5K validation deferred — the elaborate filter layer it was meant to validate is gone, and the trade-off is now explicit.

---

## 2026-05-10 — Wikidata corporate-identity resolution: pivoting from band-aid filters to reconciliation-API + OpenCorporates

**Context.** Corporate identity resolution is structurally central to legal-tender — the whole project is "trace dollars to corporate origins," and without correct mapping from FEC employer strings to corporate identities, the `whale → corporation` claim that powers `by_organization` cross-cuts is unreliable. Over this session we pushed hit-rate from ~600 (pre-this-session, mostly via SPARQL UNION queries that the WDQS endpoint couldn't reliably serve) to ~3,049 wikidata-resolved out of 5,054 canonical employers (60% hit-rate) by switching the primary path from SPARQL to MediaWiki REST (`wbsearchentities` + `Special:EntityData`), then layering in:

- top-N candidate filtering (`limit=5`)
- description-keyword blacklist for generic-concept matches
- government-entity description filter
- P31 (instance-of) blacklist for non-corporate types (60+ Q-ids)
- strict P31 corporate whitelist gate on suffix-retry path
- alternate-form retry (suffix stripping)
- hardcoded `_EMPLOYER_OVERRIDES` per-name forced Q-ids (NEA, CITADEL family)
- hardcoded `EMPLOYER_FAMILY_ALIASES` for entity-merge cases (ADELSON CLINIC → ADELSON DRUG CLINIC)
- bare-generic-word filter (CORPORATION, COMPANY, BUSINESS as full-string employer values)

This brought visible quality to the top-30 corporate_families and zeroed the obvious bogus matches (advocacy group, Sigmund Freud, Federal Government, Corporation video game). But it's a band-aid layer — ~250 lines of filter-shaped code across two files, growing one entry at a time as new wrong matches surface. Each new wrong match teaches us a new filter pattern. That's whack-a-mole.

**The structural mistake.** wbsearchentities ranks by string relevance over *all of Wikidata* (humans, films, books, vaccines, cities, given names, organizations, video games — everything). Then we post-filter the wrong ones. The right structure: never let non-organizations into the candidate set in the first place.

**Three architectures considered.**

1. **Bulk SPARQL fetch + local corporate index.** Pre-fetch every Wikidata entity that's `wdt:P31/wdt:P279* wd:Q43229` (organization or subclass), with labels, `skos:altLabel` aliases, `wdt:P749` parent links, and sitelink counts. Local JSON file (~100-300MB), refreshed quarterly. Lookup: exact label/alias match against pre-filtered org-only index, sitelink-count tiebreak for ambiguity. Eliminates the entire P31 blacklist (entities not in index can't be candidates), most of `_EMPLOYER_OVERRIDES` (Wikidata's own aliases catch NEA-style cases), and the suffix-retry mechanism (aliases handle suffix variants). Trade-off: 100MB local file, quarterly maintenance, a one-time bulk-fetch cost (~1-2 hrs of paginated SPARQL).

2. **Wikidata Reconciliation API** (`https://wikidata.reconci.link/`). Third-party hosted ElasticSearch over Wikidata, designed for OpenRefine-style entity reconciliation. Supports `type` filter (Q43229) and returns ranked candidates with scores and types. Same architectural fix (typed candidate space, scored matches) without the bulk fetch — query on demand. Built and maintained by Antonin Delpeuch.

3. **OpenCorporates** (`https://api.opencorporates.com/`). Authoritative corporate registry data covering 200M+ companies including small US private (where Wikidata's coverage is poor). Free tier 500 reqs/day, paid for production. Best for the entities Wikidata genuinely doesn't have (ULINE-style, FAHR-style — though our spike of reconci.link found ULINE in Wikidata after all, just not via wbsearchentities).

**WDQS health-check today (2026-05-10 03:55 UTC).** Out of 16 test queries against `query.wikidata.org/sparql`: 8 timeouts (30-60s), 5 × 502 Bad Gateway, 1 × 429, only 2 successes. Trivial liveness query (one rdfs:label fetch) timed out at 30s. We landed in the middle of WDQS migration weather (the team is migrating off Blazegraph to a different backend, with periodic multi-hour outages during the transition). Bulk SPARQL fetch is not viable today.

**Reconciliation-API spike (2026-05-10 04:00 UTC).** 20 hard cases batched in 7.3s. Findings:
- Better matching where wbsearchentities failed: ULINE → Uline (Q7879030, Wikidata DOES have it), BCG → Boston Consulting Group, BLACKSTONE GROUP → Blackstone Inc. via alias, CITADEL → Citadel LLC (no override needed), CITADEL INVESTMENT GROUP → same.
- Sanity cases all pass (Goldman, Apple, Google, IBM, Pan Am Railways).
- Still wrong: NEA → Newspaper Enterprise Association (defunct press agency) at top, same as wbsearchentities. Need irreducible override.
- Still ambiguous: KKR cricket team + Kohlberg Kravis Roberts + Federation of Mutual Aid Assocs all score 100. Need sitelinks tiebreak.
- Type filter is soft, not hard: STEYER → Steyr Austrian city scored 100 even with `type=Q43229`. Need client-side P31 verification using returned types.
- Confidence threshold needed: FAHR → Deutz-Fahr at score 57 — fuzzy match on partial token, should be rejected.
- `CORPORATION → IBM` at score 85 from fuzzy matching the literal word in descriptions — handled at normalization layer (existing `NON_EMPLOYERS`).
- True not-founds: PRATT INDUSTRIES, ADELSON CLINIC, ADELSON DRUG CLINIC.

**Decision.** Pivot to **reconciliation API as primary + OpenCorporates as fallback for not-founds**, with client-side scoring/threshold/tiebreak.

- Phase 1: Thin client around `wikidata.reconci.link` (batch up to 50 names per request).
- Phase 2: Resolver that filters returned candidates by P31 corporate whitelist, applies confidence threshold, fetches sitelinks for tiebreak when multiple score-100 candidates survive.
- Phase 3: OpenCorporates lookup as second-layer fallback for names where reconci.link returned no acceptable candidate (gated by daily-budget on the 500-req/day free tier; cache results aggressively).
- Phase 4: Wire into asset, replace `_resolve_company_rest` body.
- Phase 5: Validation harness — diff old vs new on 5K employers, categorize improved/regressed/unchanged, spot-check regressions.
- Phase 6: Delete obsolete band-aids. Targets: `_NON_CORPORATE_P31`, `_GENERIC_DESCRIPTION_PATTERNS`, `_GOVERNMENT_DESCRIPTION_PATTERNS`, `_RETRY_SUFFIX_TOKENS`, `_alternate_employer_forms`, most of `_EMPLOYER_OVERRIDES`, `_resolve_company_one_query`. Move surviving ~3-5 overrides + `EMPLOYER_FAMILY_ALIASES` to YAML data files with rationale fields.

**Pre-commit acceptance metrics.** Verify the architectural improvement is real, not feel-good:
1. Lines of "filter-shaped" code in `src/rag/`: today ~250. After: 0 in code, OK to have ≤30 lines of declarative scoring/threshold logic.
2. Hardcoded Q-id mappings: today ~12 across `_EMPLOYER_OVERRIDES` + `EMPLOYER_FAMILY_ALIASES`. After: 0 in code, ≤5 in `data_gaps.yaml` with verified rationale per entry.
3. No regressions on validation harness for the previously-correct ~3,000 wikidata-resolved employers.
4. Hit-rate same or higher than current 60%.

**Why not bulk-fetch + local index.** Considered as the primary architecture but two factors flipped the choice:
- WDQS is currently down for the migration — blocking. The reconciliation API runs on separate infrastructure (ElasticSearch) and is healthy.
- Reconciliation API gives the same architectural property (typed candidate space, ranked results with scores) on demand without the 100MB local file or quarterly maintenance.
- If reconci.link becomes flaky in production, falling back to bulk-fetch is still available — the local-index plan stays in this decisions log as a documented fallback architecture.

**Trade-offs accepted.**
- Third-party dependency on `wikidata.reconci.link` (single maintainer; if it goes down, fall back to bulk-fetch + local index).
- OpenCorporates 500-reqs/day free-tier ceiling means we can't bulk-resolve all 5K employers via OpenCorporates — only the ~10-20% that reconci.link doesn't cover. Acceptable.
- Per-name HTTP round-trip cost (~365ms in batched mode = ~30 min for 5K names). Same as today, no regression.

**Open follow-ups.**
- Sitelinks tiebreak requires entity-data fetch per ambiguous candidate — adds a few hundred more REST calls. Acceptable.
- OpenCorporates parent-company resolution — not free on the free tier, deferred.
- For lobbying integration (separate effort, see `docs/lobbying-integration.md`), the same reconciliation infrastructure will resolve LDA client names — keep this layer reusable.

---

## 2026-05-08 — Bulk validation against FEC weball + unitemized grassroots gap

After parsing FEC's `weball`/`webl`/`webk` summary files (per-candidate / per-PAC totals from FEC's own aggregation), wrote `scripts/validation_report.py` to cross-reference our `direct_funding` against `weball.TTL_RECEIPTS` for every candidate. The bulk validation revealed a systematic undercount for grassroots-heavy candidates.

**The pattern**: 100% of the worst offenders by absolute dollar delta are major Senate/Presidential candidates with massive small-donor bases. All under FEC's number, none over.

| Cycle | Top offenders (% under FEC) |
|---|---|
| 2020 | Sanders -58%, Warren -61%, Trump -40%, Biden -33% |
| 2022 | Oz -69%, Fetterman -56%, Demings -47%, Warnock -40% |
| 2024 | Trone -99%, Allred -47%, Tester -36%, Harris/Biden -26% |
| 2026 | Krishnamoorthi -67%, Kelly -66%, Talarico -57% |

Median delta across 10,634 cycle-candidate comparisons: **33%**. Only 12.5% within ±5% of FEC.

**Root cause**: We compute `total_from_individuals` by summing records in FEC's `indiv.zip`. But `indiv.zip` only contains *itemized* donations (typically donors who gave $200+ cumulative per committee per year). **Unitemized small donations are NOT in indiv.zip** — FEC reports them only as a sum in candidate filings.

For BWC (incumbent, mostly $200+ donors): negligible gap → 0.7% delta.
For Sanders (huge sub-$200 grassroots base): we miss ~$76M → 58% delta.

**Fix (in flight)**: Modify `committee_receipts` to use `weball.TTL_INDIV_CONTRIB` (for principal campaign committees) and `webk.INDV_CONTRIB` (for PACs) as the authoritative individuals total. Compute grassroots as `TTL_INDIV_CONTRIB − whale_donor_total`. Whale machinery untouched — they continue being graph-traced for corporate attribution.

**What this is NOT a fix for**:
- The whale-threshold model (already documented in `funding-channels.md` as a "high-engagement proxy" — accurate within its definition)
- The whale → employer conflation (intentional, used by `by_organization` cross-cut, not double-counting)
- Trace algorithm bugs (those were the prior fix; cycle-break + multiplier caps)

**What this exposes about our prior fix**: the cycle-break/cap fix prevented catastrophic blowup but masked this real gap. Without cross-validating against FEC's published numbers, we'd have shipped "looks plausible by magnitude" data with structural undercounts for the most-watched candidates.

**Validation harness as a permanent check**: `scripts/validate_funding_channels.py` does smoke tests (magnitudes, BWC sanity, channel-sum). `scripts/validation_report.py` does the bulk cross-reference. Run after every aggregation re-run; flag regressions.

## 2026-05-08 — Per-election limit threshold model documented as "high-engagement proxy"

The whale threshold (`PER_ELECTION_LIMITS` in `donors.py`) is hardcoded per cycle:
```python
{"2020": 2_800, "2022": 2_900, "2024": 3_300, "2026": 3_500}
```

Source: FEC publishes contribution limits at fec.gov/help-candidates-and-committees/candidate-taking-receipts/contribution-limits. Inflation-adjusted every 2 years.

**What we're computing isn't strictly "FEC max-out"**: we mark a donor as a whale if they aggregated `≥ per_election_limit` at any single committee in a cycle, regardless of whether any single election (primary, general, runoff) was technically maxed. So a donor giving $3,000 to primary + $3,000 to general at one candidate ($6,000 cycle total) is a whale per our definition; technically they didn't max either election.

This is documented in `funding-channels.md` as a "high-engagement donor proxy" — useful for graph traversal and corporate attribution. The mis-classification at the boundary is small and doesn't affect totals (they end up in `whale.independent` or `whale.corporate_connected` based on employer link, contributing the same dollar amount either way).

**Trade-offs of hardcoded values**:
- ✅ Simple, predictable, no runtime API dep
- ✅ Changes only every 2 years (trivial PR)
- ❌ Requires explicit human update for each new cycle

**Next maintenance window**: 2027 — when FEC publishes 2028 cycle limits, add `"2028": <new_limit>` to `PER_ELECTION_LIMITS` AND to `ACTIVE_CYCLES` (still hardcoded in 17 files; centralization is on the todo list).

## 2026-05-08 — Trace algorithm bug: cycle-break + multiplier caps

**Problem.** First successful aggregation run (after fixing the spent_on int/float crash and dropping the wikidata_corporate_resolution dep) produced wildly wrong totals: the top candidate at $118 quintillion, BWC's NJ-12 House race at $16 trillion (vs ~$1-3M raw indiv data suggests). Validation immediately surfaced the bug.

**Root cause.** Two compounding issues in `trace_committee_sources` (the multi-hop trace in `src/assets/aggregation/candidate_upstream.py`):

1. **No cycle detection.** The committee graph has bidirectional transfers — `A↔B` 2-cycles are common (sampled 10+ instances on first query). The trace propagates level-by-level for `max_trace_depth=8` levels without tracking visited committees, so cycles cause unbounded multiplier accumulation.

2. **No upper bound on per-edge `amount / from_receipts`.** Some committees have `total_receipts = $1` (tiny technical filings) but outgoing transfers of $10K+. Dividing produces `amount/receipts = 10,000`, blowing up a single hop's multiplier by 10,000× before any compounding.

The IE trace function (`trace_ie_sources`) already had the right guards (`min(1.0, ie_amount / total_receipts)`) and is single-level so doesn't have the cycle issue. The committee trace was the only buggy one.

**Fix.** Three guards added to `trace_committee_sources`:

1. **Track propagated-from committees.** A `propagated_from` set marks committees whose outgoing edges have already been processed. The trace skips re-processing them, breaking 2-cycles cleanly.
2. **Cap per-edge fraction at 1.0.** `edge_fraction = min(1.0, amount / from_receipts)` enforces the physical constraint that a committee can't transfer out more than it received. Handles the $1-receipts data quirk.
3. **Cap accumulated multiplier at 1.0.** `all_mults[cmte_id] = min(1.0, all_mults[cmte_id] + new_mult)` enforces the semantic constraint that no committee can be responsible for >100% of a candidate's money.

**What this is NOT.** This isn't a full algorithmic refactor. It's a minimal-edit correction. A clean rewrite would do proper fixed-point iteration with convergence detection and could potentially recover legitimate compounded mults that the cap may slightly under-report. For now we accept the small under-report as the price of preventing the catastrophic over-report.

**Side benefit.** The fix made `candidate_funding` ~3× faster (1h44m → ~50m). The previous run was wasting work iterating through pathological graph cycles to `max_depth=8`. The cycle break terminates much earlier.

**Validation harness.** `scripts/validate_funding_channels.py` was added to surface obvious algorithm bugs in seconds (top-candidate magnitude check, BWC sanity check, distribution outliers, channel-sum consistency). Run after every aggregation: `docker exec legal-tender-dev-webserver python3 /workspace/scripts/validate_funding_channels.py`.

**Open follow-ups (logged in `docs/todo.md`):**
- Replace the cap-based fix with proper fixed-point iteration with convergence detection.
- Investigate why some campaign committees have `total_receipts = $1`. Is this an FEC bulk-data quirk we should normalize at parse time, or do those committees genuinely have those records?
- Re-run wikidata_corporate_resolution after the upstream client fixes (negative cache + batched VALUES + backoff) so corporate attribution is real.

## 2026-05-08 — Brain stack architecture (Phase 2 of professionalization)

Built `~/workspace/obsidian/` as a self-hosted second-brain stack. Decisions made along the way:

### Where it lives

**Decision**: Brain stack at `~/workspace/obsidian/`, NOT under `~/workspace/monitor/`.

**Considered**: Bolting it onto the existing monitor stack (Prometheus, Grafana, Loki, etc.) since both are "always-on infrastructure I want web-accessible from any tailnet device."

**Why separate**: monitor is observability (read-only metrics, alerting); brain is knowledge (read+write, RAG, MCP). Different lifecycle, different data shapes, different security posture. Combining them would muddy both stacks. The "everything in one place" experience comes from a Homepage/Dashy dashboard linking to both, not from co-location.

### Port allocation

**Decision**: Brain stack uses **300x range** (specifically 3006-3008).

**Considered**: 32xx/42xx (the project-prod/dev pattern used by found-footy/legal-tender/etc.).

**Why 300x**: brain is *infrastructure*, not a project — it aggregates across projects, has no prod/dev split, and matches the always-on services already in 300x (grafana, prometheus, vikunja). The user's port allocation table (now in `~/.claude/CLAUDE.md`) reserves 300x for infra services. 421x or other ranges would have been awkward.

### Containerized everything

**Decision**: All four services (Khoj, Open WebUI, brain-postgres, basic-memory) run in Docker. **No host-side `pipx install`.**

**Considered**: `pipx install basic-memory` on the host (the basic-memory docs' default suggestion).

**Why Docker-only**: matches the user's preference (codified in `~/.claude/CLAUDE.md` "Tooling installation policy"). Reproducible from `docker compose up -d`. Survives host reprovisioning.

### `docker compose up -d` does everything

**Decision**: A `khoj-init` one-shot service runs after Khoj is healthy, idempotently configures it, and exits 0. No manual UI clicking required.

**Considered**: A README-documented manual setup ("log into the admin UI, click here, paste this URL, etc.").

**Why automated**: the user explicitly stated "perfect world is `docker compose up` does everything." Manual setup steps are the wrong shape — they don't survive reprovisioning, can't be code-reviewed, and document drift the moment Khoj's UI changes.

**Implementation**: `~/workspace/obsidian/scripts/init-khoj.py` uses Khoj's Django ORM directly to create AI Model APIs, ChatModel, SearchModelConfig, ServerChatSettings, LocalMarkdownConfig, and an API token; then walks the vault and uploads markdown via `PUT /api/content`. All steps use `update_or_create` for idempotency.

### Anonymous-mode user resolution (the trickiest gotcha)

**Decision**: Vault content (LocalMarkdownConfig + uploaded files) attaches to user `username="default"`, NOT the Django admin user `vedanta@brain.local`.

**Why**: Khoj's anonymous-mode middleware (`configure.py:187`) resolves all incoming requests to `KhojUser.objects.filter(username="default")`. The Django admin user (created from `KHOJ_ADMIN_EMAIL`) is a *separate* user, only used for `/server/admin/` access. If we attach content to the admin user but anonymous chat queries land on the default user, the LLM sees a user with zero entries → no Notes tool → falls back to web search.

**This took 30 minutes to find** — the bug surfaced as "chat returns generic answers about NGOs and World Bank instead of legal-tender's actual funding channels." Easy to mistake as a search-config issue or RAG ranking problem; the real cause was multi-tenancy.

### `KHOJ_ALLOWED_DOMAIN=*`

**Decision**: Set the env var to `*` so Django's `DisallowedHost` doesn't reject requests with `Host: luv` or other tailnet hostnames.

**Considered**: Setting it to a specific domain or the tailnet name.

**Why wildcard**: tailnet is the perimeter; we don't need Django's host-header protection. Tightening this is on the TODO list for if/when the brain is ever exposed beyond tailnet (which is "never" per the user's stance on Google ties).

### Khoj patches via local Dockerfile

**Decision**: Build Khoj from a local `Dockerfile.khoj` that applies sed-based patches to the upstream image. Tag it `brain-khoj:patched`. Both `khoj` and `khoj-init` services use the patched image.

**Considered**: Submitting upstream PRs and waiting for merge. Forking khoj-ai/khoj.

**Why local Dockerfile**: needed the fixes immediately, not blocked on upstream review. The Dockerfile is small (one `RUN sed`), self-documenting (comment block lists each patch's purpose), and verifies on build (greps for the patched lines, fails if absent). When upstream merges, drop the Dockerfile and use the official image.

**Patches applied (2026-05-08)**:
- `src/khoj/processor/conversation/openai/utils.py:513,613`: `buf += <delta>` lacks `or ""` guard. llama.cpp's OpenAI-compatible streaming sends final chunks with `delta.content=None`; Khoj's code crashes with `TypeError: can only concatenate str (not "NoneType") to str`. OpenAI's own server papers over this by sending `""`. Fix: `buf += <delta> or ""`.

### Telemetry off

**Decision**: `KHOJ_TELEMETRY_DISABLE=true`.

**Why**: Khoj phones home to `khoj.beta.haletic.com/v1/telemetry`. From this network the call times out, blocking each chat by ~5-10s before falling through. Match the privacy-conscious / no-Google-ties principle.

### Host symlinks don't translate to containers

**Decision**: The vault on the host has `brain/projects/legal-tender → ~/workspace/dev/legal-tender/docs` symlinks for human convenience. But for the containers, we **bypass the symlinks entirely** and Docker-mount each repo's `docs/` directly:

```yaml
- ${HOME}/workspace/dev/legal-tender/docs:/data/brain/projects/legal-tender:ro
```

**Why**: a symlink stores an absolute host path. Inside the container, that host path doesn't exist — `os.walk` falls into the symlink and finds nothing. Docker-mounting the real source bypasses this entirely. Costs: one line of compose per project. Onboarding a new project = add one mount line to `khoj` and `khoj-init` services.

### What's NOT yet in the vault

`AGENTS.md` and `CLAUDE.md` (the agent-context files) live at each repo's root, not in `docs/`. So they're NOT in the vault and Khoj does NOT index them. They're loaded directly by Claude Code at session start (separate retrieval path).

**Open question**: should AGENTS.md also be in the vault for cross-project search? Right now if you ask Khoj "what conventions does legal-tender use?", it finds the docs but not AGENTS.md's terse "things to check before X" list. Might add a mount.

### What's deferred

- Khoj-as-MCP: not officially shipped per upstream as of 2026-05-07. Once confirmed, wire `khoj` into `~/.claude/mcp.json` alongside basic-memory.
- claudewatch (drift detection for AGENTS.md / docs): planned for Phase 5 (continuous operation).
- Quartz publishing (static site of the vault): only if we ever want a public-facing read-only view.
- Homepage dashboard at `~/workspace/homepage/`: nice-to-have for unified tailnet entry, separate from brain.
- Laptop-side native Obsidian + Syncthing: only if we want desktop editing of vault notes. Browser-based Khoj covers most use cases.

---

# Pipeline — Historical Decisions (Feb 2026, originally PIPELINE_FIXES.md)

**Started**: February 6, 2026
**Last Updated**: February 7, 2026
**Branch**: `feature/employer-enrichment` (now merged into main)

## The Goal

For any federal candidate, compute **how they are funded** by tracing money backwards through the FEC committee graph to its origin. The output is broken into funding channels — distinct ways money reaches or affects a candidate.

## Funding Channels

These are the categories of money flow we compute for each candidate, per election cycle and in aggregate.

### Channel 1: Organizational Direct Funding

PAC money that flows through the committee graph into the candidate's affiliated committees, traced upstream through JFCs, victory funds, and passthrough committees to terminal organizational sources.

**How it works**: Start at the candidate's affiliated committees. Walk backwards through `transferred_to` edges. At each committee, check `terminal_type`:
- **Terminal** (corporation, trade_association, labor_union, ideological, cooperative) → stop, attribute the money to that organization.
- **Passthrough** (JFC, party committee, unknown) → keep tracing upstream. Proportionally attribute using `edge_amount / committee_total_receipts`.

**Graph edges used**: `affiliated_with` (candidate → committee), `transferred_to` (committee → committee)

**What this captures**: Corporate PAC money, trade association money, labor union money, ideological PAC money — ALL organizational money that flows into a candidate's committees through any number of hops. The terminal organizations are the answer.

**What this does NOT capture**: Money from committees where `terminal_type = super_pac_unclassified` — Super PACs with no `ORG_TP` in FEC data. These currently stop tracing (we can't classify them further without additional data). This is part of the unaccounted gap.

### Channel 2: Independent Expenditure Support

Outside money spent FOR the candidate by Super PACs and other committees. This money never touches the candidate's committees — it's spent independently (TV ads, mailers, etc.).

**How it works**: Look at `spent_on` edges where `support_oppose = 'S'`. For each committee that spent on the candidate, trace its funding upstream via `transferred_to` and `contributed_to` to find who funded it. Proportionally attribute using `ie_amount / committee_total_receipts`.

**Graph edges used**: `spent_on` (committee → candidate), then `transferred_to` and `contributed_to` for upstream tracing.

**What this captures**: The organizations and individuals behind the Super PACs that spend in support of the candidate.

### Channel 3: Independent Expenditure Opposition

Same as Channel 2, but `support_oppose = 'O'` — money spent AGAINST the candidate. This is important context: a candidate might have $10M in IE support but $50M in IE opposition.

### Channel 4: Individual Contributions

All money from individual people to the candidate's affiliated committees, or proportionally attributed from upstream passthrough committees.

**Two tiers**:
- **Whale donors** (per-election max-out): Anyone whose largest single-committee total meets or exceeds the FEC per-election individual contribution limit for that cycle ($2,800 in 2020, $2,900 in 2022, $3,300 in 2024). Fully traced through graph with per-donor detail — name, employer, corporate connection. Split into corporate-connected (employees of known corps) and independent.
- **Grassroots donors** (below max-out limit): Known total from raw FEC `indiv` data but no per-donor detail in the graph (the max-out threshold is a principled, FEC-regulation-based graph optimization for employer analysis). Split into:
  - **Direct**: grassroots to candidate's own affiliated committees (from `committee_receipts.small_donor_total`)
  - **Upstream**: grassroots at passthrough committees (JFCs, conduits, party committees) attributed proportionally through the transfer chain

**Graph edges used**: `contributed_to` (donor → committee) for whale tier, `committee_receipts` raw FEC aggregation for grassroots tier.

### Channel 5: Unaccounted (True Residual)

The gap between committee total_receipts and all accounted money (traced whale + org + grassroots). Should be **3-5%** for well-traced candidates.

**What's in here**:
- Unitemized individual contributions (<$200 aggregate, not in FEC indiv file at all)
- Deep proportional trace loss (multiplier falls below 0.0001 threshold)
- Committees with no receipt data (no `total_receipts` set)
- Data gaps and edge cases

### Future: Lobbying

Lobbying disclosures are a completely different data source (we have the API via `lobbying_api.py`). The connection to a specific candidate is indirect — organizations lobby Congress, not individual candidates. The link would be "Organization X spent $Y lobbying on bills that Candidate Z voted on / committees Z sits on."

This is its own project and does not block the other channels.

### The Unaccounted Gap

At every committee hop in the trace, we compute: `total_receipts - SUM(all_traced_inflows) = gap`. This gap is real money we can't attribute, coming from:
- Unitemized small donors below the $200 reporting threshold (partially quantifiable from committee filings)
- 501(c)(4) dark money where donor disclosure isn't required
- Data we simply don't have edges for (timing mismatches, FEC data gaps)
- `super_pac_unclassified` committees with no ORG_TP that we stop tracing at

This is a first-class number in every channel, not an afterthought. It's arguably the most interesting signal — it's where the dark money lives.

## Output Structure

Per candidate, per cycle (and aggregate across cycles):

```
funding_channels: {
    organizational_direct: {
        total: $X,
        traced: $Y,           -- money successfully traced to terminal orgs
        unaccounted: $Z,       -- gap lost in passthrough committees
        by_organization: [     -- terminal orgs, sorted by amount
            { name, terminal_type, amount, pct }
        ]
    },
    ie_support: {
        total: $X,             -- total IE spend supporting this candidate  
        traced: $Y,            -- traced to upstream funders of those Super PACs
        unaccounted: $Z,
        by_organization: [...],
        by_committee: [...]    -- which Super PACs did the spending
    },
    ie_oppose: {
        total: $X,
        traced: $Y,
        unaccounted: $Z,
        by_organization: [...],
        by_committee: [...]
    },
    individuals: {
        itemized_total: $X,    -- sum of contributed_to edges to candidate's committees
        unitemized_total: $Y,  -- from committee summary filings (no donor detail)
        total: $X + $Y,
        top_donors: [...],
        corporate_connected: {
            total: $X,         -- whale/employer-linked individual money (metadata only)
            by_company: [...]
        }
    },
    summary: {
        total_pro: organizational_direct + ie_support + individuals,
        total_against: ie_oppose,
        total_traceable: sum of all traced amounts,
        total_unaccounted: sum of all gaps,
        pct_organizational: organizational_direct.traced / total_pro,
        pct_individual: individuals.total / total_pro,
        pct_ie: ie_support.total / total_pro,
        pct_dark: total_unaccounted / (total_pro + total_unaccounted)
    }
}
```

## Current Graph Edges

| Edge Collection | Count | What It Represents |
|---|---|---|
| `contributed_to` | 5,680,106 | Individual/candidate donor → committee (ENTITY_TP IN ['IND','CAN']) |
| `transferred_to` | 654,530 | Committee → committee (PAC-to-PAC, party, JFC transfers) |
| `affiliated_with` | 22,808 | Committee → candidate (one edge per cycle — deduplicate with UNIQUE) |
| `spent_on` | 20,373 | Committee → candidate (IE spending, support or oppose) |
| `employed_by` | 147,810 | Donor → employer |
| **donors** | 955,137 | Unique per-election max-out donors in graph |

## Committee Classifications

`terminal_type` on 30,840 committees (set by `committee_classification` asset):

| terminal_type | Count | % | Behavior |
|---|---|---|---|
| campaign | 13,493 | 43.8% | Candidate's own committee — starting point for tracing |
| passthrough | 8,139 | 26.4% | Trace upstream (JFCs, party committees, conduits) |
| super_pac_unclassified | 4,533 | 14.7% | Stop — no ORG_TP to classify further |
| corporation | 2,048 | 6.6% | Terminal → organizational direct |
| unknown | 994 | 3.2% | Trace upstream |
| trade_association | 786 | 2.5% | Terminal → organizational direct |
| ideological | 399 | 1.3% | Terminal → organizational direct |
| labor_union | 387 | 1.3% | Terminal → organizational direct |
| cooperative | 54 | 0.2% | Terminal → organizational direct |

## Committee Receipts

16,488 committees with `total_receipts` populated. $40.7B total individual contributions across all committees.

---

## Completed Fixes

### FIX 1 — ENTITY_TP filter in donors and contributed_to ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/graph/donors.py`, `src/assets/graph/contributed_to.py`

The FEC's `indiv.txt` is actually Schedule A itemized receipts — ALL contributions, not just individuals. Contains `ENTITY_TP` field: IND, ORG, PAC, COM, CAN, CCM, PTY.

Added `FILTER doc.ENTITY_TP == 'IND'` to both assets. This excludes ORG/PAC/COM/CCM/PTY (already captured via `oth` → `transferred_to`) and CAN (candidate self-funding — see Fix 9 below).

| Metric | Before | After |
|---|---|---|
| Donors | 287,744 | 279,624 (-8,120) |
| contributed_to edges | 3,340,147 | 3,326,235 (-13,912) |
| donor_classification orgs | 4,921 (1.7%) | 254 (0.1%) |

### FIX 2 — Conduit filter ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/graph/donors.py`, `src/assets/graph/contributed_to.py`

Conduits (ActBlue, WinRed) aggregate earmarked donations. Their bulk transfers double-count money already captured as individual contributions. Added `FILTER NOT REGEX_TEST(doc.NAME, '(ACTBLUE|WINRED|EARMARK|CONDUIT)', true)`.

Removed 11 conduit donors, 410 edges, $518M of double-counted money.

### FIX 8 — committee_classification dependency ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/aggregation/candidate_upstream.py`

`candidate_upstream` reads `terminal_type` from committees but never declared `committee_classification` as a dependency. The classification asset had never run — all 30,841 committees had `terminal_type: null`, causing all organizational money to be misclassified.

Added dependency, ran classification. Now 3,674 committees correctly typed as terminal organizational sources.

### FIX 9 — Include candidate self-funding (ENTITY_TP='CAN') ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/graph/donors.py`, `src/assets/graph/contributed_to.py`

Fix 1 filtered to `ENTITY_TP == 'IND'` only. This excluded candidate self-funding (`ENTITY_TP='CAN'`, 36,392 records). Self-funding IS an individual contribution — it's a person writing a check to their own committee. The corporate connection (Bloomberg → Bloomberg LP) is metadata tracked via employer linkage, same as any other whale.

Changed filter to `ENTITY_TP IN ['IND', 'CAN']`.

| Metric | After Fix 1 | After Fix 9 |
|---|---|---|
| Donors | 279,624 | 280,513 (+889 candidates) |
| contributed_to edges | 3,326,235 | 3,327,970 (+1,735) |

### FIX 10 — Rewrite candidate_upstream → candidate_funding ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/aggregation/candidate_upstream.py` (824 lines, complete rewrite)

Rewrote the core asset from the broken terminal-source-type model to the funding channels model. Key changes:
- Renamed asset from `candidate_upstream` to `candidate_funding`
- Config class renamed `CandidateFundingConfig`
- `trace_committee_sources()` returns `organizational` dict (all 5 org types) + `individuals` + `traced_total`
- New `trace_ie_sources()` function (extracted from old `trace_ie_corporate_sources`)
- New `compute_funding_channels()` replaces old computation
- Added `TERMINAL_TYPE_BUCKET` mapping dict and `safe_pct()` helper
- Output writes to `candidates.funding_channels` (was `candidates.funding_sources`)
- Validation section prints all 5 channels with tree-style formatting

BFS tracing engine preserved — same proportional attribution algorithm, restructured output.

### FIX 3 — Remove committee_financials (redundant) ✅

**Date**: Feb 6, 2026  
**Files**: Deleted `src/assets/enrichment/committee_financials.py`, updated deps in `committee_receipts.py`, `candidate_summaries.py`, `committee_summaries.py`, all `__init__.py` files

`committee_receipts` overwrites everything `committee_financials` computed. Removed the redundant 162-line asset.

### FIX 4 — Remove dead employer enrichment chain ✅

**Date**: Feb 6, 2026  
**Files**: Deleted `employer_clustering.py` (443 lines), `employer_cluster_integration.py` (192 lines), `corporate_hierarchy.py` (339 lines). Removed `employer_unification_job`. Updated all `__init__.py` and job files.

6-asset chain producing 540 usable records. Kept `employers`, `canonical_employers`, and `wikidata_resolution`. Removed O(n²) clustering and fragile parent detection (974 lines total).

### FIX 6 — Remove dead Python classification functions ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/enrichment/committee_classification.py`, `src/assets/enrichment/donor_classification.py`

Removed dead `classify_committee()` and `classify_donor()` Python functions — all classification happens in AQL. Also fixed a runtime bug: the logging code in `committee_classification` still called the deleted `classify_committee()` function. Replaced with inline Python logic mirroring the AQL ternary chain.

### FIX 7 — wikidata_resolution field name mismatch ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/enrichment/wikidata_resolution.py`

Changed `ce.employee_count` → `ce.employee_donor_count` (the actual field name set by `canonical_employers`).

### FIX 11 — Deduplicate affiliated committee IDs + unaccounted breakdown ✅

**Date**: Feb 7, 2026  
**Files**: `src/assets/aggregation/candidate_upstream.py`

**Root cause**: The `affiliated_with` edge collection has one edge per (committee, candidate, cycle). The AQL query `FOR v IN INBOUND c affiliated_with RETURN v._key` returned duplicate committee IDs — Cruz's senate committee appeared 3× (once per cycle), tripling its receipts in the unaccounted calculation.

**Fix**: Added `UNIQUE()` to the AQL query: `UNIQUE(FOR v IN INBOUND c affiliated_with RETURN v._key)`.

Also enriched the unaccounted channel with receipt breakdown data already available from `committee_receipts`:
- `breakdown.small_donor_estimate` — individual contributions not in our graph (mostly unitemized <$200)
- `breakdown.from_individuals` — total individual receipts for candidate's committees
- `breakdown.from_committees` — total committee transfer receipts

| Candidate | Old Receipts (duped) | New Receipts (deduped) | Inflation | Old Unaccounted | New Unaccounted |
|---|---|---|---|---|---|
| Cruz | $215.7M | $71.9M | 3.0× | 91.9% | 75.6% |
| Trump | $2.51B | $984.7M | 2.6× | 93.8% | 84.1% |
| Harris | $1.85B | $1.80B | 1.03× | 86.2% | 85.8% |

---

## Validation Results (Feb 7, 2026)

Pipeline run: `donors` → `contributed_to` → `committee_classification` → `committee_receipts` → `candidate_funding`

**candidate_funding**: 11,796 candidates processed, 6,305 with funding data, completed in 6m11s.

| | **Harris (Pres)** | **Trump** | **Cruz (Senate)** |
|---|---|---|---|
| **Total Funding** | $2.27B | $1.24B | $78.1M |
| **Ch1 Org Direct** | $13.7M (0.6%) | $3.3M (0.3%) | $2.1M (2.6%) |
| — Corp | $1.7M | $1.5M | $806K |
| — Trade | $2.5M | $816K | $777K |
| — Labor | $7.5M | $144K | $174K |
| — Ideological | $1.8M | $760K | $289K |
| — Cooperative | $139K | $66K | $9K |
| **Ch2 IE Support** | $548M (24.1%) | $300M (24.2%) | $8.7M (11.2%) |
| **Ch3 IE Oppose** | $561M | $492M | $2.9M |
| **Ch4 Individuals** | $1.71B (75.3%) | $937M (75.5%) | $67.4M (86.2%) |
| — Whale (max-out) | $719M (31.6%) | $280M (22.6%) | $22.6M (28.9%) |
|   — Corp-connected | $52.8M | $20.1M | $877K |
|   — Independent | $666M | $260M | $21.7M |
| — Grassroots (sub-limit) | $991M (43.6%) | $656M (52.9%) | $44.8M (57.3%) |
|   — Direct | $729M | $205M | $43.5M |
|   — Upstream | $262M | $451M | $1.3M |
| **Ch5 Unaccounted** | $73.7M (4.1%) | $44.8M (4.6%) | $2.5M (3.5%) |
| **Receipts** | $1.80B | $985M | $71.9M |
| **Accounted** | $1.72B | $940M | $69.4M |

**Observations**:
- Unaccounted now 3-5% across all candidates (down from 75-86%)! Residual is unitemized <$200 donors + deep trace loss.
- Grassroots individuals (sub-$10K aggregate) are the dominant funding channel: 43-57% of total funding.
- Trump's upstream grassroots ($451M) is massive — reflects the WinRed/JFC small-dollar fundraising machine.
- Harris's whale donors ($719M, 31.6%) exceed Trump's ($280M, 22.6%) in both absolute and relative terms.
- IE spending is roughly equal for both presidential candidates (~$300M support, ~$500M+ oppose).
- Org direct is small for presidential races (<1%) but meaningful for Senate (Cruz at 2.6%).
- Labor strongly favors Harris ($7.5M vs $144K). Corp/trade more balanced but still tilt slightly Harris.

---

### FIX 12 — Grassroots channel + two-phase proportional trace

**Problem**: Unaccounted was 75-86% — absurdly high. Two distinct bugs:

**Bug A — Missing grassroots channel**: The `donors` graph had a $10K aggregate threshold (since replaced by per-election max-out in Fix 13). Only whale donors got graph vertices/edges. But `committee_receipts` correctly sums ALL raw FEC `indiv` transactions. The difference (sub-threshold itemized donors) was dumped into "unaccounted" even though it's a known, quantified amount.

**Bug B — visited_edges BFS bug**: When a committee transfers money via multiple edges (one per cycle), the BFS enqueued the source committee multiple times but `visited_edges` meant only the FIRST dequeue processed any edges. Harris Victory Fund→Harris: 3 edges ($586M, $237M, $6M) but only the $586M edge's mult was used for whale/org tracing. Lost $244M from HVF alone, cascading through DNC ($138M more).

**Fix A**: Fold sub-$10K individuals into the individuals channel as "grassroots". Direct (to candidate's committees) comes from committee_receipts. Upstream (at passthrough committees) is attributed proportionally during trace.

**Fix B**: Replaced BFS with two-phase proportional trace:
- Phase 1: Propagate multipliers level-by-level through passthrough graph, accumulating total mult per committee. Terminal org attributions happen here. Multiple transfer edges between same committees → correctly accumulated.
- Phase 2: Process each committee ONCE with its total mult. Attribute whale individuals and upstream grassroots.

**Result**: Unaccounted dropped from 75-86% to 3-5%. The $10K threshold stays as a graph optimization (we only need per-donor employer detail for whales), but the accounting now properly classifies ALL traceable money.

---

## Pending Fixes

### FIX 5 — Consolidate normalization functions

**Priority**: MODERATE  
**Files**: Create `src/utils/normalize.py`, update all assets

Three different normalization functions across the codebase → silent key mismatches. Consolidate to one.

---

## Completed Fix: Per-Election Max-Out Threshold

### FIX 13 — Replace $10K arbitrary threshold with FEC per-election max-out limit ✅

**Date**: Feb 7, 2026  
**Files**: `src/assets/graph/donors.py`, docstring updates to `contributed_to.py`, `committee_receipts.py`, `candidate_upstream.py`

**Problem**: The $10K aggregate threshold for whale donors was arbitrary and backwards. It summed a donor's contributions across ALL committees — someone giving $3,300 to 4 different candidates ($13,200 total) qualified as a whale, but someone maxing out $6,600 to a SINGLE candidate didn't. The threshold had no connection to FEC regulations.

**Insight**: The FEC sets per-election individual contribution limits, indexed for inflation every odd year:
- 2020: $2,800/election ($5,600/cycle)
- 2022: $2,900/election ($5,800/cycle)
- 2024: $3,300/election ($6,600/cycle)
- 2026: $3,500/election ($7,000/cycle)

Anyone who maxes out to even ONE committee is demonstrating intentional, strategic giving — the exact behavior that makes employer/corporate linkage interesting.

**Solution**: Rewrote `donors.py` with double-COLLECT AQL pattern:
1. First COLLECT by (name, employer, cmte_id) → per-committee totals
2. Second COLLECT by (name, employer) → roll up with MAX(cmte_total)
3. FILTER max_single_cmte >= per-election limit for that cycle

Single scan per cycle, no nested joins. ~3 minutes per cycle on tuned ArangoDB.

**Also tuned ArangoDB** for the host hardware (AMD Ryzen AI MAX+ 395, 16c/32t, 128GB RAM):
- 16GB detected memory (up from 8GB default)
- 64 max threads, 16 IO threads, 16 min threads
- 4GB query memory limit
- RocksDB: 8 high/low priority threads, 16 max background jobs, 3GB edge cache
- Both `docker-compose.dev.yml` and `docker-compose.yml` updated

| Metric | Old ($10K agg) | New (max-out) | Change |
|---|---|---|---|
| Donors (2020) | 142,654 | 510,752 | 3.58× |
| Donors (2022) | 99,291 | 362,298 | 3.65× |
| Donors (2024) | ~82,000 | 369,736 | ~4.5× |
| Total unique donors | 280,513 | 955,137 | 3.40× |
| Materialization time | ~4m | 10m28s | Larger dataset |

**Impact**: 3.4× more donors in the graph, each one a person who demonstrated max-out giving behavior to at least one committee. The whale/grassroots split is now grounded in FEC law rather than an arbitrary dollar amount. Downstream: contributed_to edges grew from 3,327,970 → 5,680,106 (1.71×, 14m36s). Committee receipts and candidate funding pending rematerialization.

---

## Execution Plan

| Phase | Fixes | Status |
|---|---|---|
| ✅ Done | 1, 2, 8 | Clean donor data, committee classifications, basic tracing |
| ✅ Done | 9, 10, 3, 4, 6, 7 | Self-funding, funding channels rewrite, dead code removal |
| ✅ Done | 11, 12 | Dedup affiliated committees, grassroots channel, two-phase trace |
| ✅ Done | 13 | Per-election max-out threshold, ArangoDB tuning |
| Next | 5 | Normalize functions |

---

## 2026-08-27 — Make monetary uncertainty a first-class result

Source disclosures do not all provide ledger-grade point amounts. LDA LD-2
reports can disclose only an under-$5,000 band, and reported numeric values can
be method-specific rounded estimates. Missing coverage, unresolved filing
versions, identity decisions, and proportional graph attribution add different
kinds of uncertainty.

Decision:

1. Preserve a source value's exact representation separately from its
   measurement meaning. Lossless decimal parsing does not imply exact economic
   money.
2. Represent calculated money as a point, bounded interval, one-sided bound,
   unbounded amount, not-applicable state, or invalid state. Preserve signed
   integer minor units and boundary inclusivity. Never manufacture a midpoint,
   threshold-minus-one value, or zero.
3. Carry coverage, attribution, revision, identity, and evidence states beside
   the amount. Do not compress those independent dimensions into one confidence
   score or range.
4. Preserve alternative amendment, resolution, or classification outcomes as
   explicit scenarios. A minimum/maximum summary cannot replace the scenarios.
5. Require compatible interval arithmetic, deterministic allocation, and
   conservation for aggregates. Incomplete signed records do not automatically
   establish a lower bound.
6. Treat graph paths as explanatory evidence. Terminal-source calculations
   allocate one named input measure into exclusive components plus an
   unresolved remainder; overlapping paths and cycles cannot duplicate money.
7. Apply the contract to every financial domain while preserving distinct
   meanings for campaign receipts, transfers, expenditures, lobbying income,
   lobbying expenses, and attributed money.

The semantic contract is
[`design/money-measures.md`](./design/money-measures.md). The accepted v1 API
shape and examples live under
[`contracts/common/money-measure/v1/`](../contracts/common/money-measure/v1/),
and the concrete Go domain type is pinned in the design without starting
runtime code in the legacy tree. Source-specific threshold, rounding, and
accounting rules stay in versioned domain contracts, not in the common
arithmetic package.

---

## 2026-08-27 — Seed Schedule A from processed dumps and increment from raw electronic filings

The earlier Schedule A decision selected the FEC processed dump as the
canonical detailed source but left extraction and update transport open. Live
source measurements changed the operational conclusion: the FEC publishes one
weekly all-history Schedule A object rather than per-period dumps. It was
89,883,424,294 bytes on 2026-08-27. Dagster can prevent unnecessary downstream
work after a diff, but it cannot make that monolithic transfer incremental.

The processed row API is also unsuitable as the main change feed. A recent
eleven-day 2026 load window reported about 2.68 million rows, requiring at
least 26,820 requests at the endpoint's 100-row page size. The electronic-
filings endpoint reported 2,388 relevant filings over the same dates: 24 list
pages plus one exact `.fec` document per filing. Filing grain also preserves
the amendment evidence that a processed snapshot cannot reconstruct.

Decision:

1. Use the processed Schedule A dump for the initial target-period seed,
   period close, release gates, and periodic reconciliation. Observe its S3
   metadata weekly without downloading every changed object.
2. Stream selected two-year partition `COPY` data from a version-pinned
   `pg_restore` container into Go. Do not add a permanent PostgreSQL domain
   database. Benchmark a temporary, memory-capped PostgreSQL restore only as a
   fallback.
3. Use OpenFEC electronic-filing listings plus exact publisher `.fec` files as
   the routine incremental lane. Capture complete closed receipt-date windows,
   converge their file-number sets, preserve every physical row, and retain
   originals and amendments as separate filing versions.
4. Keep processed and raw as-filed assertions separate. Raw facts are
   provisional and cannot publish in a total until a versioned effective-
   filing calculation is accepted. Reconciliation relates the two views and
   never overwrites either source.
5. Reserve the processed Schedule A API for targeted lookup, fixtures, repair,
   and sampling. Its load-date filter is discovery evidence, not proof that no
   prior processed row disappeared.
6. Keep classic `indiv` as a separately versioned threshold-limited comparison
   and coverage check. It is neither complete Schedule A nor a silent patch for
   the processed baseline.
7. Partition Dagster acquisition by the source's natural boundary: `fec_cycle`
   for period ZIPs and processed extracts, daily `efile_received_date` for raw
   filing capture, and an observable data version for the monolithic dump.
   Go change sets map filing and record changes to affected cycles, committees,
   report families, and candidates.
8. Expose processed-through, raw-received-through, effective-filing,
   reconciliation, and summary-coverage watermarks independently. Do not
   manufacture one source-agnostic freshness timestamp.

This supersedes the weekly-download and every-weekly-dump-retention portions
of the earlier processed Schedule A decision. Its source-grain preservation,
processed itemized-individual calculation, memo handling, candidate linkage,
and summary-separation rules remain in force.

The complete acquisition and propagation contract is
[`design/schedule-a-source-strategy.md`](./design/schedule-a-source-strategy.md).

Still open before implementation: raw e-file source and physical contracts,
effective report-family and processed/raw reconciliation calculations, the
full-dump streaming benchmark, and the content-addressed dump retention and
cold-storage budget.

---

## 2026-08-27 — Select raw report families structurally and project amendments as complete replacements

The raw electronic-filing contracts now preserve exact listing pages, filing
documents, versioned headers, and Schedule A occurrences. The first exact
original/amendment family also shows that the FEC listing's convenience
`amendment_chain` projection is asymmetric: the original exposes only itself,
while the amendment exposes both filings. It cannot be the sole family key.

Decision:

1. Assemble report families from explicit listing `amends_file` and
   `amended_by` edges. Treat the document HDR `original_report_id` as a root
   assertion, not an immediate-predecessor edge. Use amendment-chain and
   current-filing projections only as corroboration.
2. Derive stable family identity from the unique explicit root file number.
   Select an effective leaf only for a closed listing observation with all
   referenced documents present, one linear sequence, supported parsers,
   compatible committee/base-form/coverage assertions, and agreeing publisher
   current-filing projections.
3. Preserve blocked incomplete, conflict, invalid-document, and unsupported-
   format states. A later observation creates a new immutable result; it never
   rewrites an earlier effective selection.
4. Treat the selected electronic amendment as a complete report replacement.
   Never patch its rows onto the predecessor. Compare Schedule A rows across
   versions only by report-family plus exact report-life transaction ID.
   Missing or duplicate selected IDs block the complete projection; no
   contributor/date/amount fallback is allowed.
5. Keep physical-occurrence identity separate from a versioned logical digest
   of the 45 Schedule A fields. Emit `added`, `modified`, `removed`,
   `representation_changed`, and `carried_forward_identical` states.
6. Record source lineage for every new selected document. Trigger downstream
   semantic work only from added, modified, and removed logical rows. Equal
   logical content can update provenance without rebuilding money, entity, or
   graph calculations.

The exact fixture selects amendment `2009982` over original `1997074` and
reproduces 2 added, 5 modified, 0 removed, and 18 carried-forward Schedule A
rows. The contracts and fixture schemas live under
[`contracts/calculations/fec/`](../contracts/calculations/fec/). Their corpus
gates remain open, so this decision does not yet publish a user-facing raw
total. Processed/raw reconciliation is the next calculation boundary.

---

## 2026-08-28 — Reconcile Schedule A at occurrence grain before aligning filing revisions

Processed Schedule A and raw electronic filings describe related evidence but
do not advance on the same schedule. Comparing only the processed current view
with the newest raw amendment would misclassify normal FEC processing lag as a
content change. The processed API also provides targeted evidence, not a
complete period snapshot.

Decision:

1. Reconcile in two explicit stages. First match a processed occurrence to the
   exact raw occurrence from the same file. Then align that direct evidence to
   the currently effective raw filing revision.
2. Require exact committee ID, file number, and transaction ID for direct
   identity. Use form/line, back reference, date, amount, and content only as
   corroboration. Never create identity from names, fuzzy text, or amount
   proximity, and never invent a processed `sub_id` for raw evidence.
3. Map all 45 raw fields. Compare 43 through declared text, date, decimal, and
   schedule-line rules; retain the two fields without accepted processed
   equivalents as `unmapped`. Keep publisher enrichment, omission, material
   difference, representation, and non-applicability distinct.
4. Treat revision alignment as independent evidence. Identical content carried
   from an older processed filing can corroborate a current raw row. Modified
   or added rows ahead of processed evidence become
   `raw_only_pending_processing`, not `processed_changed` or `conflict`.
5. Conserve every raw occurrence, processed occurrence, and effective row.
   Preserve processed-only and ambiguous groups explicitly. Only a pinned
   complete processed partition can produce a publishable complete snapshot.
   Exact API observations remain diagnostic and cannot feed user totals.
6. Emit row, committee, and two-year-period change keys from pending and
   conflicting effective rows. Go owns the record calculation; Dagster maps
   the resulting domain keys to targeted asset partitions.

The first exact fixture matches all 23 raw original rows in file `1997074` to
23 processed rows with no mapped omission or material difference. The complete
raw family selects amendment `2009982`; the targeted processed observation
still exposes only the original. Eighteen identical carried-forward rows are
corroborated, while five modified and two added current rows remain
`raw_only_pending_processing`. This is a `processed_lags_raw` result, not a
source conflict.

The calculation and evidence contracts live under
[`contracts/calculations/fec/schedule-a-reconciliation/`](../contracts/calculations/fec/schedule-a-reconciliation/)
and [`contracts/sources/fec/schedule-a-api/`](../contracts/sources/fec/schedule-a-api/).
The semantic design is
[`design/schedule-a-reconciliation.md`](./design/schedule-a-reconciliation.md).
Exact processed-dump extraction, blocked-state fixtures, and representative
corpus proof remain acceptance gates.

---

## 2026-08-28 — Bound processed-dump storage and validate selected COPY output

The processed Schedule A source is one growing all-history artifact. The
2026-08-23 object is 89,883,424,294 bytes, while the FEC estimates roughly 300
GB for a full restore without indexes. Atomic acquisition and extraction must
coexist with the last accepted evidence, but this shared host cannot permit
unbounded weekly snapshots.

The first archive probe also found that `pg_restore` can exit successfully when
a schema-qualified table pattern matches no relation. Process exit alone is
therefore insufficient evidence that a selected partition was extracted.

Decision:

1. Cap the processed Schedule A hot-storage lane at 600 GiB. Before acquiring
   a dump, budget current hot bytes, the discovered candidate content length,
   the largest accepted target extract, and a 25 GiB working margin.
2. Block complete-dump acquisition if that sum exceeds the cap or would leave
   less than 500 GiB free on the data filesystem. Do not evict evidence to make
   the check pass.
3. Keep the newest two accepted dump artifacts hot. Older dumps may move only
   to a verified content-addressed cold tier. If no cold tier exists, block the
   next complete acquisition while allowing the independent raw electronic-
   filing lane to continue.
4. Retain exact manifests, digests, selected four-period extracts, and record
   indexes hot. Never build the publisher's PostgreSQL indexes; PostgreSQL is
   an extraction tool, not the domain database.
5. Accept a selected restore only when it contains exactly one `COPY` section
   for the catalog relation, every physical row has all 81 fields, the process
   exits successfully, and row count, byte count, and SHA-256 are recorded.
   A successful zero-section restore is a blocking failure.
6. Recheck the 600 GiB cap after measuring all four target extracts. Any
   change is an explicit measured storage decision.

The source strategy and archive observation contract carry these gates. This
decision closes the undefined-budget item but not the complete-dump or
four-partition extraction acceptance gates.

Measured outcome: the first selected relation contains 166,293,056 rows and
114,190,988,781 COPY bytes (about 106.35 GiB). Restore, row-width validation,
and digesting completed in 493 seconds under the pinned 1 GiB/1-CPU container
cap. The result fits the initial budget model, but the cap remains unconfirmed
until the other three target extracts are measured together.

---

## 2026-08-28 — Store selected Schedule A extracts as verified zstd streams

The complete four-period benchmark invalidated the assumption that
uncompressed COPY extracts could remain hot. The selected relations contain
891,016,965 rows and 620,474,534,709 bytes (577.86 GiB). With the current dump,
that working set is 661.57 GiB before refresh headroom and therefore breaches
both the 600 GiB cap and 500 GiB free-space floor.

Zstd level 3 reduced the four extracts to 58,987,591,718 bytes (54.94 GiB,
9.51%). Every compressed artifact passed a frame test and a full decompression
whose SHA-256 exactly reproduced the accepted uncompressed extract.

Decision:

1. Store accepted selected-period extracts as content-addressed zstd streams.
   Retain uncompressed byte count, row count, SHA-256, compression parameters,
   compressed byte count and SHA-256, and the decompression validation result.
2. Treat the uncompressed COPY file as transient working state. Remove it only
   after the compressed artifact passes both frame and decompressed-digest
   gates. The immutable publisher dump remains retained independently.
3. Stream decompression into the Go parser. Do not materialize all four
   uncompressed extracts together in routine operation.
4. Keep the 600 GiB hot cap. The measured canonical set is about 138.65 GiB
   for one dump plus four compressed extracts. A candidate dump, the largest
   measured uncompressed working partition, and 25 GiB margin still fit under
   the cap.

The four redundant uncompressed benchmark files remain present pending the
required explicit deletion approval. Until they are removed, live disk use is
temporarily outside this decision's cap and free-space floor.

Execution result (2026-08-28): explicit deletion approval was received and the
four redundant uncompressed COPY files were removed. This reclaimed
620,474,534,709 bytes (577.86 GiB), increasing live free space from 318 GiB to
895 GiB. The immutable publisher dump, four verified zstd streams, compressed
and uncompressed digests, row counts, and validation markers remain retained.

---

## 2026-08-28 — Start the Go data plane with a streaming Schedule A source boundary

The first rewrite code needs to prove the largest accepted source boundary
without inheriting the Python parser, loading a 106 GiB relation into memory,
or making a runtime JSON schema file the parser's hidden control plane.

Decision:

1. Establish one root Go module and one thin `legal-tender` command. Put source
   behavior under `internal/source`, separate from command dispatch and future
   domain calculations.
2. Decode the retained zstd artifacts directly. Bound decoder memory, avoid
   `bufio.Scanner` token limits, and keep each decoded row valid only until the
   next scan unless a caller explicitly freezes an owned record.
3. Compile the accepted 81-column Schedule A shape into typed Go metadata and
   test every name, ordinal, PostgreSQL kind, and nullability flag against the
   exact archive observation. Do not load repository contract files as runtime
   configuration.
4. Preserve exact physical row bytes and decoded source lexemes. Keep SQL null,
   empty text, false, zero, negative money, and timestamps distinct. Validate
   decimals without floating point.
5. Yield malformed physical rows with explicit issues so later ingestion can
   conserve them. A verification operation may fail publication, but parsing
   never silently drops the row.
6. Expose the source boundary through an independently runnable, versioned JSON
   verification command. Keep the general Dagster operation envelope,
   occurrence persistence, and normalization as later boundaries rather than
   hiding unfinished behavior behind this command.
7. Add the Go module alongside the current Python runtime during the proof.
   This is not a cutover and does not change the accepted later archive and
   rollback process.

Measured result: the implemented command streamed the complete retained
2025/2026 extract in 469.606 seconds with an 8,316 KiB peak resident set. All
166,293,056 rows passed the 81-field, lexeme, required-field, and partition
checks. The observed 10,587,087,035 compressed bytes and 114,190,988,781
uncompressed bytes exactly matched both accepted SHA-256 digests.

Still open: immutable occurrence and issue indexes, source-record identities,
normalized fact persistence, staged publication, the common pipeline result
envelope, Dagster invocation, the raw electronic-filing adapter, and parallel
multi-period throughput policy.

---

## 2026-08-28 — Retain processed Schedule A after classic-product overlap proof

The legacy pipeline already captured the cycle-specific classic `indiv` and
`oth` products. Before accepting the cost of a complete processed Schedule A
baseline, the rewrite needed to establish what product-relevant evidence the
larger relation adds rather than equating row volume with coverage.

An exact 2024 `SUB_ID` audit compared 264,085,601 processed rows with
58,208,756 `indiv` rows and 18,667,435 `oth` rows. All rows passed their
applicable comparison gates and source-specific identifiers were unique. The
processed relation contained 189,514,597 rows in neither classic product.

Under the already accepted itemized-individual rule, the processed-only set
contains 168,856,391 included rows and $3,158,365,091.91 in signed amount.
That is 75.99% of the calculation's row grain and 19.88% of its amount. A
separate non-individual, non-memo diagnostic found another 1,334,580 rows and
$1,774,437,614.67 absent from both classic products. The latter is coverage
evidence, not an accepted flow total.

Decision:

1. Keep selected processed Schedule A partitions as the canonical processed
   receipt baseline and reconciliation source. Do not replace them with a
   union of classic `indiv` and `oth`.
2. Keep classic products as independently versioned comparison and legacy-
   parity evidence. Do not patch their snapshot-relative rows into a newer
   processed view.
3. Preserve processed ledger grain before applying memo, entity, conduit, or
   receipt-role rules. The raw Schedule A amount sum is never a money-flow
   total.
4. Keep the existing transport policy: raw electronic filings provide routine
   increments; the monolithic processed dump is acquired only for seed,
   closeout, and scheduled or triggered reconciliation.

The measured evidence and reproduction command are in the
[classic-product overlap audit](./audit/schedule-a-classic-overlap-2026-08-28.md).

---

## 2026-08-28 — Publish coordinated bulk-only FEC releases

The processed Schedule A overlap proof established that the complete bulk dump
is required for the target receipt grain. The remaining question was whether
routine freshness should combine that source with OpenFEC API increments or
instead preserve one authoritative acquisition path per fact family.

Combining representations creates an avoidable ambiguity: a missing bulk row,
an API row, and a classic-file row can describe different publisher states.
Using one as a fallback for another makes completeness and deletion impossible
to state honestly. The FEC's monolithic Schedule A transport is expensive, but
that cost does not justify synthesizing one source from incompatible views.

Decision:

1. Use one authoritative acquisition path per FEC fact family. Prefer an
   official bulk product when it supplies the required complete dataset.
   Overlapping files and APIs may validate it but never patch it.
2. Make the initial production FEC path bulk-only. Processed Schedule A is the
   canonical itemized-receipt source. Candidate, committee, linkage, and
   summary fact families use their accepted cycle bulk products. Schedule B,
   Schedule E, and committee history remain intended bulk authorities pending
   their own audits.
3. Defer the OpenFEC electronic-filing lane, raw `.fec` publication, and
   processed/raw reconciliation from the initial production slice. Preserve
   their completed research and contracts for a possible separately labeled
   as-filed product. They do not feed or repair the processed view.
4. Observe all required bulk-object metadata every Monday at 04:00
   `America/New_York`. Discovery is body-free and does not publish data.
5. Start complete acquisition on the first Monday of each month after
   discovery. Select the newest stable observed versions, wait and retry when
   a weekend publication is late, and never label an unchanged prior version
   as a new release. Moving to every Monday is a configuration change, not an
   architecture change.
6. Freeze one candidate `fec.release.v1` manifest containing the exact version
   and independent watermark for every required fact family. The manifest
   coordinates Legal Tender publication without claiming the FEC published all
   products atomically.
7. Stage, validate, and publish the complete release atomically. A failed,
   missing, changed-during-capture, or invalid source leaves the prior release
   active. No API call, overlapping file, summary value, or empty result fills
   the gap.
8. Treat the newest four two-year periods as a rolling hot view, not fact
   identity or destructive retention. Full source acquisition still emits
   record-level change sets so only affected domain projections rebuild.
9. Keep current and prior accepted artifacts hot. Retain manifests, digests,
   schemas, changes, calculations, and publication records indefinitely. Move
   older reproducibility bytes only to verified content-addressed cold storage;
   perpetual scheduling cannot begin without a valid retention path.
10. Apply the no-hidden-fallback rule outside FEC as well, but do not impose a
    universal no-API rule. A fact family without adequate official bulk must
    explicitly accept one API as its sole authority or remain out of scope.

This decision supersedes the initial-slice requirements that made daily
OpenFEC electronic-filing ingestion the routine Schedule A freshness path and
that limited full-dump acquisition to seed, quarter close, or drift-triggered
reconciliation. It does not invalidate the source evidence or contracts
created while evaluating that alternative.

The complete operational contract is the
[coordinated FEC bulk-release strategy](./design/fec-release-strategy.md).

---

## 2026-08-28 — Record lobbying as a later legislative-influence direction

LDA lobbying amounts do not describe money paid to candidates or officials.
An outside registrant reports lobbying-related income for a client; an
organization lobbying on its own behalf reports expenses. Activities describe
issues, legislation, congressional chambers, and agencies associated with that
work. They do not allocate the report-level amount among bills, committees, or
members.

Legal Tender's longer-term product is broader than candidate funding. It is
intended to connect disclosed political money with legislation, committee
jurisdiction, sponsorship, votes, lobbying, and careful analysis of who may
benefit or be burdened by policy.

Current implementation constraints and working direction:

1. Exclude LD-2 income and expense amounts from candidate receipts, outside
   spending, terminal-source attribution, and all other FEC funding totals.
   Campaign contributions are receipts of political committees, not personal
   income to officeholders.
2. Keep the initial Go implementation focused on FEC campaign money,
   independent spending, terminal sources, paths, and evidence drilldown.
   Lobbying is not required for that cutover.
3. If the later legislative-influence phase proceeds, add LDA and congressional
   data together. Preserve the common source, evidence, entity, time, monetary,
   and graph boundaries now so the later design is not forced into a physical
   model rewrite.
4. Represent the LDA amount on the client/registrant filing. Activities can
   reference issues or bills, but the amount cannot be copied to each activity,
   bill, committee, sponsor, or member without an accepted allocation model.
5. Use official `sponsor`, `cosponsor`, committee referral, membership, action,
   and vote facts. Do not relabel a sponsor as the bill's author or infer that
   the sponsor received lobbying money.
6. Treat bill beneficiaries and burdens as versioned analytical hypotheses.
   Ground them primarily in bill text, official summaries, policy scope, and
   independent organization or industry evidence. Donations and lobbying may
   nominate or corroborate a hypothesis but cannot be its sole evidence; that
   would make the later money comparison circular.
7. Keep disclosed money, disclosed lobbying, official legislative roles,
   resolved bill references, inferred effects, and observed alignment as
   mechanically distinct claim levels. Their intersection is an investigative
   result, not proof of contact, motive, causation, quid pro quo, bribery, or
   corruption.
8. The current operating hypothesis is that implemented LDA and congressional
   sources join the same Monday 04:00 `America/New_York` production refresh as
   FEC while retaining independent source watermarks and time semantics. This
   remains subject to source behavior and the later design.

The provisional direction is documented in the
[legislative-influence design](./design/legislative-influence.md), with working
question-level behavior in the
[investigative catalog](./design/investigative-questions.md). Only the
separation between lobbying amounts and candidate-funding totals is locked for
the initial implementation.

---

## 2026-08-29 — Freeze the initial FEC release inventory and pre-download decision boundary

The first Go production boundary must decide whether a coordinated FEC source
set is ready and changed before it spends storage or bandwidth on the
processed Schedule A dump.

Decision:

1. Version the exact initial inventory as 21 required artifacts: `cn`, `cm`,
   `ccl`, `weball`, and `webl` for 2020, 2022, 2024, and 2026, plus one
   processed Schedule A dump selecting those four two-year relations.
2. Discover every artifact with `HEAD` against its official FEC URL. Record the
   discovery start/completion window, each source's response observation time,
   final redirect URL, and publisher metadata, but never read the response
   body.
3. Choose a metadata version identity in this order: publisher-native object
   version ID, ETag, digest header, then last-modified plus content length. A
   successful response without one of those identities is not ready.
4. Treat missing, duplicated, unknown, or contract-incompatible inventory
   members as `invalid`. Treat a structurally valid observation whose publisher
   object is unavailable as `source_not_ready`. Neither result may expose a
   selected acquisition set.
5. Emit `no_change` only when every selected version matches the prior
   published manifest. Otherwise emit one `update_available` plan containing
   all exact selected versions and explicit changed/reused source membership.
6. Derive the candidate release ID from the inventory version and canonical
   source-ID/version-identity pairs. Exclude schedule and discovery times so an
   unchanged Monday run cannot manufacture a new candidate release.
7. Keep discovery and planning independently runnable in Go. Dagster will
   schedule and record their outputs later; Python does not select or compare
   versions.

The machine contract is
[`contracts/releases/fec/v1/`](../contracts/releases/fec/v1/). A live
metadata-only run on 2026-08-29 verified usable object-version metadata and
redirect behavior for every inventory member without transferring artifact
bodies. The next boundary is resumable, storage-gated acquisition with a
post-capture metadata recheck.

---

## 2026-08-30 — Separate FEC planning from resumable acquisition

The Monday planning path must remain cheap and observable even when the
processed Schedule A object is unchanged, late, too large for the current disk
budget, or changes during a long capture. Putting body acquisition directly in
the scheduled planning job would blur those states and turn every metadata
observation into a possible 90 GB transfer.

Decision:

1. Keep `monday_fec_release_planning` limited to metadata discovery and pure
   release planning. Use a separate Dagster asset sensor and acquisition job.
   Only an `update_available` candidate materialization authorizes that job.
2. Pass the exact content-addressed plan path through run config. Go hashes the
   exact plan bytes and accepts no plan status other than `update_available`.
   Python does not reopen the plan to make domain decisions.
3. Gate before every publisher-body request. For changed Schedule A, require
   the current hot-lane bytes plus remaining dump bytes plus the measured
   206,363,392,958-byte largest selected extract plus 25 GiB margin to stay at
   or below 600 GiB. Require all remaining downloads plus that extraction
   workspace and margin to leave at least 500 GiB free. Non-Schedule-A
   candidates retain the 25 GiB working margin.
4. Stage changed bodies by candidate release ID. Hash existing partial bytes,
   resume through an exact `Range`, use the selected ETag or last-modified
   value as a condition, and reject an incorrect `Content-Range`. If a server
   legitimately ignores range and returns the complete body, restart only that
   staging file.
5. Reuse unchanged sources only from the exact prior published manifest and
   only when their version identities and immutable artifact metadata match.
   Do not redownload or revalidate their multi-gigabyte bytes in routine runs.
6. After capture, repeat metadata discovery for all 21 required sources and
   compare selected version identity and content length. Any difference fails
   before ZIP reads or `pg_restore` invocation.
7. Validate every selected ZIP member through EOF for CRC. Require `PGDMP`, a
   bounded `pg_restore --list`, and all four selected Schedule A relations.
   Link validated staging files into SHA-256 paths only after every captured
   container passes, persist acquisition state, then remove the partials.
   A state-write failure therefore remains locally resumable and may leave safe
   unreferenced immutable objects; it never advances publication.
8. Persist successful acquisition state by candidate and Dagster run ID. Step
   retries share the run ID and resume; later Monday plans use a plan-digest
   sensor run key and can retry the same candidate after an earlier block.
9. Give acquisition three exponential Dagster retries and a 24-hour process
   timeout. Preserve contract-valid blocked or failed Go stdout as an immutable
   control artifact and attach it to the Dagster failure.
10. Keep publication as the next distinct boundary. Acquisition cannot create
    or replace `/storage/releases/fec/current.json`.

The implementation is in `internal/source/fec/release/`, the machine result
contract is
[`acquisition-result.schema.json`](../contracts/releases/fec/v1/acquisition-result.schema.json),
and the operational mapping is in `orchestration/`. No production dump was
downloaded while implementing or testing this boundary.

---

## 2026-08-30 — Checkpoint selected streams and publish the FEC source release atomically

Acquisition makes the 21 publisher bodies immutable, but Schedule A relation
extraction can run for hours. Retrying a failed later relation must not repeat
either the 90 GB download or earlier accepted extracts. The active release
must also remain unchanged until all selected products and blocking checks
form one exact evidence chain.

Decision:

1. Keep acquisition, staging, and publication as separate Go commands and
   separate Dagster assets. Sensors pass content-addressed control-artifact
   paths; Python does not inspect FEC rows or make publication decisions.
2. Stage the 20 exact classic ZIP members as zstd level-3 streams. Stage each
   of the four Schedule A relations as a data-row-only PostgreSQL COPY text
   zstd stream.
3. Accept a Schedule A extract only when `pg_restore` exits successfully,
   emits exactly one COPY section for the selected catalog relation, and a
   complete decompression reproduces the recorded physical-row count,
   uncompressed byte count, and SHA-256. Preserve every physical data row.
   Field width, period, and value problems become explicit downstream
   occurrence issues instead of making staging discard or hide the evidence.
4. Write an immutable selected-output object and then a candidate/run
   checkpoint after every completed output. A retry validates checkpoint
   objects before reuse. An unchanged source may reuse only a matching output
   from the prior published manifest.
5. Repeat the 600 GiB Schedule A hot-cap and 500 GiB filesystem-free-floor
   checks before extraction, before each new output, and after measuring the
   completed staged set. Reserve the largest measured extraction plus 25 GiB
   during working checks.
6. Publish only a `staged` result with all 21 acquired artifacts, 24 selected
   outputs, and passing blocking checks. Revalidate their physical immutable
   objects before publication.
7. Serialize publication with a filesystem lock. Under that lock, require the
   active release ID to equal the plan baseline, write one immutable manifest,
   sync it, and only then replace `releases/fec/current.json` by synced
   temporary-file rename. Reuse a matching orphan immutable manifest after a
   crash; reject one with different evidence.
8. Treat this as coordinated source-release publication. It establishes the
   exact selected input baseline and planner state; it does not claim that
   occurrence indexes, normalized facts, graph projections, or API results
   exist.

The machine contracts are
[`staged-release.schema.json`](../contracts/releases/fec/v1/staged-release.schema.json)
and
[`release-manifest.schema.json`](../contracts/releases/fec/v1/release-manifest.schema.json).
Focused tests cover exact COPY framing, injected mid-stage failure and
checkpoint recovery, immutable output verification, atomic publication, and
idempotent replay. Dagster wiring tests cover all three guarded sensors. No
production download, extraction, or publication ran while implementing this
boundary.

---

## 2026-08-30 — Publish Schedule A occurrences before normalized facts

A published FEC source release proves which source bytes and selected streams
were accepted, but it does not yet make individual rows durable evidence.
Normalization cannot be allowed to drop malformed records, overwrite repeated
publisher identifiers, or confuse a serialization change with a meaningful
publisher-value change.

Decision:

1. Publish one immutable Schedule A occurrence for every physical COPY data
   row before creating a normalized fact. Identify it by source artifact,
   relation, cycle, one-based row ordinal, and exact raw byte offset and
   length. Keep the raw row SHA-256.
2. Treat `SUB_ID` scoped by FEC dataset and two-year transaction period as the
   publisher natural key when valid. Preserve unkeyed and duplicate rows as
   occurrences and emit structured issues; never choose a winner during
   ingestion.
3. Identify a source-record version by publisher, dataset, cycle, publisher
   reference, and exact raw row digest. Compute a separate semantic digest
   over all 81 decoded values with null-aware length framing. Equivalent
   PostgreSQL COPY escape representations remain distinct raw evidence but do
   not produce a false semantic change.
4. Compare globally sorted natural-key indexes between accepted occurrence
   sets and report added, changed, newly absent, unchanged, duplicate, and
   invalid states. These are changes in publisher observations, not claims
   that the underlying contribution happened, changed, or disappeared at the
   same time.
5. Use bounded hash shards and an external merge so the complete cycle never
   needs to fit in memory. Store occurrences, issues, natural indexes, and
   change sets as immutable content-addressed zstd JSONL artifacts.
6. Publish an immutable occurrence-set manifest, then atomically replace only
   `evidence/fec/schedule-a/current/<cycle>.json` under a per-cycle lock.
   Require source-release ancestry and reuse the active occurrence set when a
   later coordinated release selects the same Schedule A output with the same
   parser and semantic versions.
7. Let Dagster fan a published release into dynamic `fec_cycle` partitions.
   Python passes the exact release artifact and maps Go metadata; Go owns row
   parsing, identity, issues, comparison, checks, and publication.
8. Keep this boundary free of normalized facts, receipt-counting policy,
   entity resolution, graph edges, candidate totals, and API projections.

The machine contracts are under
[`contracts/evidence/fec/schedule-a/v1/`](../contracts/evidence/fec/schedule-a/v1/),
and the implementation is in `internal/source/fec/occurrence/`. Focused tests
cover conservation, exact locators, malformed rows, duplicate keys, semantic
escape equivalence, added/changed/absent comparison, global sort order,
storage gating, strict ancestry, immutable-pointer backing, missing artifacts,
idempotent replay, and Dagster cycle fanout. No production occurrence
publication ran while implementing this boundary.

## 2026-08-30 — Publish classic FEC identity and summary facts before Schedule A facts

Schedule A receipts cannot be interpreted into candidate paths without stable
candidate, committee, and linkage assertions. The coordinated release already
contains `cn`, `cm`, `ccl`, `weball`, and `webl`, but staging their exact ZIP
members alone does not make their rows queryable or change-aware.

Decision:

1. Apply the occurrence-first boundary to all five classic products. Publish
   one exact occurrence for every physical row, retain malformed rows as
   issues, isolate duplicate publisher keys, and compare semantic values only
   within the same dataset and two-year source partition.
2. Use the source contracts' publisher keys: cycle-scoped `CAND_ID` for `cn`,
   `weball`, and `webl`; cycle-scoped `CMTE_ID` for `cm`; and `LINKAGE_ID`
   inside the selected `ccl` period. Never restore the legacy later-row-wins
   behavior.
3. Normalize one source fact only for each unique source-valid key. Preserve
   every raw field beside typed years, dates, and optional identifiers.
   Duplicate and malformed occurrences remain available in evidence and are
   explicit fact-projection exclusions.
4. Preserve `weball` and `webl` as separate fact types and publisher
   populations. Parse monetary points into checked signed cents only when the
   source decimal is losslessly representable. Retain raw decimals and keep
   blanks, invalid precision, overflow, negative values, and zero distinct.
5. Do not turn `ccl` into a timeless graph edge during normalization. The
   `A`/`P` authorized-committee rule remains a separately versioned
   cycle-scoped calculation over linkage facts.
6. Let Dagster fan each published coordinated release into 20 dynamic
   `dataset:cycle` partitions. Python passes exact artifacts and metadata. Go
   owns parsing, identity, changes, normalization, checks, and publication.
7. Keep Schedule B, Schedule E, and committee history outside the v1 release
   until their own corpus and semantic audits pass. Adding them changes the
   required inventory contract; no classic overlap file silently fills them.

The contracts are under
[`contracts/evidence/fec/classic/v1/`](../contracts/evidence/fec/classic/v1/)
and
[`contracts/facts/fec/classic/v1/`](../contracts/facts/fec/classic/v1/).
The implementation uses `internal/source/fec/classic/` and
`internal/source/fec/occurrence/`. No production occurrence or fact
publication ran while implementing this boundary.

---

## 2026-08-30 — Normalize Schedule A without selecting effective or countable receipts

The occurrence ledger preserves every processed Schedule A physical row, but
the first graph probe needs typed receipt assertions. Normalization must not
silently decide which amendments win, which memos count, or which source
identity represents a real entity.

Decision:

1. Publish one `fec.schedule_a_receipt.v1` fact for every unique, source-valid
   `SUB_ID`. Keep source-invalid and duplicate occurrences as explicit evidence
   exclusions and never select a duplicate winner.
2. Retain the exact complete 81-field source object in every fact. Add typed
   recipient, contributor, candidate, conduit, election, receipt, and filing
   groups as a lossless view over those fields.
3. Represent money with its raw decimal, checked signed cents as a JSON string
   when exactly representable, source scale, semantic role, measurement kind,
   state, and rule version. Preserve null, blank, zero, negative, overflow,
   and unsupported sub-cent precision as distinct states.
4. Preserve local receipt and publisher-load times without inventing a time
   zone. Derive `memoed_subtotal` only from exact source code `X`; retain the
   source amount and memo back-reference unchanged.
5. Do not apply effective-amendment selection, receipt-counting policy, entity
   resolution, authorization, graph projection, or aggregation at this layer.
   Each is a separately versioned calculation or projection over facts.
6. Add the unique source row's one-based ordinal to the occurrence natural
   index. The fact publisher builds a compact one-bit-per-source-row selection
   map and streams the selected source once instead of retaining hundreds of
   millions of natural keys in memory.
7. Publish immutable zstd JSONL facts and an immutable fact-set manifest, then
   atomically replace only `facts/fec/schedule-a/current/<cycle>.json`. Require
   exact source-release and immutable occurrence-manifest ancestry.
8. Put the partitioned fact asset directly after the Schedule A occurrence
   asset. Dagster passes paths and metadata only; Go owns normalization,
   conservation, validation, and publication.

The machine contracts are under
[`contracts/facts/fec/schedule-a/v1/`](../contracts/facts/fec/schedule-a/v1/),
and the implementation is in `internal/source/fec/occurrence/`. Fixture-scale
tests cover signed money, source scale, null time, memo-X, complete source-field
preservation, duplicate exclusion, and source/occurrence ancestry. No
production fact publication ran while implementing this boundary.

---

## 2026-08-30 — Reconcile detailed candidate receipts with summaries without using a fallback

The first calculation needs a separate control total, but the legacy system
showed that copying a summary into missing detail can make both the result and
its validation tautological. Candidate summaries and Schedule A answer related
but different questions.

Decision:

1. Calculate only the processed Schedule A itemized-individual component for
   same-cycle committees established by valid `ccl` designation `A` or `P`.
   This is not total candidate-controlled receipts.
2. Give every Schedule A fact one immutable source-level decision:
   `included`, `excluded_non_individual`, `excluded_memo_subtotal`,
   `unresolved_individual_class`, or `unresolved_amount`. Keep negative and
   zero included amounts and sum checked signed cents.
3. Treat conflicting linkage assertions and a committee claimed as authorized
   by multiple candidates as unresolved. Never route one committee subtotal to
   two candidates. A Schedule A fact set that excluded duplicate source
   references blocks the calculation rather than silently understating it.
4. Preserve `weball` and `webl` as separate source assertions. For each one,
   bound resolved detail to the two-year cycle and that summary's
   coverage-through date, then publish
   `TTL_INDIV_CONTRIB - resolved detail` as `individual_detail_gap`.
5. Keep `TTL_RECEIPTS` as broader context. Do not compare it directly with the
   itemized-individual component, copy either summary into detail, or call the
   residual unitemized, missing, unknown, or erroneous without separate
   evidence. A gap is a diagnostic to investigate, not an automatic failure.
6. Pin the exact Schedule A, linkage, and both summary fact-set manifests from
   one coordinated source release. Publish content-addressed receipt-decision
   and candidate-result artifacts plus an immutable calculation manifest and
   atomic per-cycle pointer. Identical inputs and method version reuse the
   prior calculation set.
7. Expose the calculation as a cycle-partitioned Dagster asset with Python
   limited to paths and metadata. Keep it manual until a fact-bundle readiness
   mapping joins the `cycle` Schedule A partition with all required
   `dataset:cycle` classic partitions without a timing race.

The machine contract and schemas are under
[`contracts/calculations/fec/candidate-itemized-individual-receipts/v1/`](../contracts/calculations/fec/candidate-itemized-individual-receipts/v1/).
The Go implementation is in `internal/calculation/fec/receipts/`; the Dagster
asset is `fec_candidate_itemized_receipts`. Fixture tests cover receipt decision
order, signed arithmetic, date bounds, authorization conflicts, immutable
publication, and idempotent reuse. No production calculation publication ran
while implementing this boundary.

---

## 2026-08-30 — Reject full-row Schedule A JSON materialization after the corpus probe

Fixture tests proved the logical Schedule A fact and calculation contracts,
but they did not prove the physical representation. The first real 2024
occurrence publication expanded a 14.77 GB staged relation into 113.45 GB of
occurrence, natural-index, and bootstrap-change JSON. The subsequent fact pass
produced 33.68 GB after only 113 million of 264 million rows and was stopped.
Its complete output projected to about 78.7 GB before any per-row calculation
decision artifact.

A direct probe then validated and hashed all 264,085,606 staged rows, applied
the same accepted receipt predicate and candidate routing, and wrote 8,175
candidate results in 439.454 seconds. Its only result artifact was 1.02 MB
compressed. Source decisions conserved every row and every blocking check
passed.

Decision:

1. Keep the logical occurrence, source-version, normalized-fact, receipt-
   decision, and calculation identities. Reject only their current repeated
   full-row JSONL representation.
2. Do not resume or automate `publish-schedule-a-facts` or its downstream
   per-row decision publisher in their current form. Keep both implementations
   as fixture-tested contract evidence until replacements land.
3. Treat the immutable FEC dump and verified staged relation as the lossless
   physical evidence backing. Build one partitioned, typed, columnar Schedule A
   fact representation that retains all 81 source values and exact row lineage
   once. Prove format, partition, retry, and query behavior on the real 2024
   corpus before accepting it.
4. Replace full occurrence, natural-index, and bootstrap-change JSON with
   compact manifests, key indexes, explicit exception records, and actual
   inter-release deltas. A first snapshot declares bootstrap membership at the
   manifest level; it does not need one “added” JSON object per source row.
5. Represent a calculation's included, excluded, and unresolved membership by
   a versioned predicate plus exact input-index identity when equivalence is
   proven. Materialize exceptions or query indexes needed for drilldown, not
   one duplicate decision document for every source row.
6. Keep candidate and committee results materialized for interactive use. Put
   query-bearing entity, monetary, and path projections in ArangoDB; do not
   load every raw Schedule A receipt as a verbose graph document.
7. Retain `probe-candidate-itemized-receipts` as a manual, non-production corpus
   gate. It creates no current pointer and cannot substitute for the accepted
   columnar fact design.

This decision supersedes the physical JSONL choices in the 2026-08-30 Schedule
A occurrence, fact, and receipt-calculation entries. Their evidence semantics,
strict source preservation, signed-money rules, authorization rules, and
summary-independence requirements remain in force. The measurements and exact
input identities are recorded in the
[direct receipt probe audit](./audit/direct-receipt-probe-2026-08-30.md).

---

## 2026-08-31 — Adopt Parquet as the Schedule A physical fact layout

The bounded layout benchmark accepted a 99-column Parquet candidate for a
complete-corpus gate. The complete 2024 publication then converted all
264,085,606 selected Schedule A rows into 265 content-addressed Parquet shards
and 2,113 row groups. It produced 16,759,429,988 bytes, 13.506% more than the
14,765,199,882-byte selected zstd COPY relation, while preserving all 81 source
values, exact row locators, and 18 typed or lineage columns. No row was invalid
or excluded.

Every new shard passed exact-schema and complete semantic readback before
checkpoint. The finished publisher matched the compressed and uncompressed
source digests, conserved all 182,881,299,512 uncompressed bytes, atomically
published the cycle pointer, and removed its checkpoint. A hardened idempotent
rerun rehashed every referenced file and returned the same fact set. An
independent DuckDB scan proved the complete row, column, ordinal, byte-range,
byte-length, and normalization-state counts.

Decision:

1. Keep `fec.schedule_a_receipt.v1` as the logical fact. Adopt
   `legal-tender.fec.schedule-a-parquet.v1` as its complete-cycle physical
   layout.
2. Store all 81 source lexemes plus the accepted typed and locator projection
   in flat Parquet columns. Use one-million-source-row shards, 128,000-row
   groups, zstd compression, and `parquet-go` v0.32.0 until a versioned layout
   change passes equivalent gates.
3. Keep the immutable zstd COPY relation as the exact-byte source authority.
   Treat Parquet as a lossless query and calculation fact layout, not a source
   replacement.
4. Require content-addressed shard paths, close-time file byte and SHA checks,
   full Parquet semantic readback, resumable deterministic checkpoints,
   SHA-on-reuse, complete source conservation, and atomic manifest publication.
5. Keep fine-grained Schedule A facts in filesystem manifests and Parquet.
   ArangoDB consumes query-bearing entity, monetary, and path projections from
   an exact fact-set manifest; it does not store every wide raw receipt as a
   graph document.
6. Do not enable the Dagster Schedule A fact leg yet. First replace the rejected
   occurrence/change JSON and per-row calculation-decision JSON with compact,
   equivalent evidence and add coordinated fact-bundle readiness.
7. Treat publication throughput as open engineering work. The first complete
   run took 6,833.490 seconds at 38,646 rows/s because full shard readback was
   serial. Benchmark bounded overlap or parallel verification without sampling
   or weakening any integrity check before weekly automation.

The complete measurements and input identities are in the
[columnar publication audit](./audit/schedule-a-columnar-publication-2026-08-31.md).

---

## 2026-08-31 — Adopt compact Schedule A occurrence and change evidence

The first complete 2024 Schedule A occurrence publication preserved the right
logical evidence but repeated each of 264,085,606 rows across verbose
occurrence, natural-index, and bootstrap-change JSON. Those artifacts consumed
113,447,910,338 compressed bytes. The replacement complete-corpus publication
preserved the same occurrence, key-state, semantic-change, and exception
invariants in 10,276,745,806 physical bytes, including both manifests. It
finished in 1,023.662 seconds, fully verified every shard, and passed a
same-input backing rehash in 30.375 seconds.

Decision:

1. Represent complete physical-row occurrence membership densely through the
   exact selected source artifact and its one-based row range. Derive row
   occurrence, raw-content, and record-version identities from the immutable
   source bytes and ordinal when requested; do not persist them as one JSON
   object per valid row.
2. Store every valid unique `SUB_ID` in deterministic FNV-1a hash partitions.
   Sort numerically within each partition and encode one 48-byte record with
   unsigned 64-bit big-endian `SUB_ID`, unsigned 64-bit big-endian source row
   ordinal, and the 32-byte semantic digest.
3. Preserve invalid rows, invalid keys, and duplicate keys as sparse,
   content-addressed JSONL evidence. A dirty cycle excludes invalid and
   duplicate states from facts but never silently chooses a winner.
4. Declare first-snapshot unique-key membership at manifest grain. Do not emit
   one materialized `added` delta per bootstrap key. On later accepted
   snapshots, materialize only actual added, changed, absent, and invalid
   transitions; unchanged membership stays implicit.
5. Require full source replay, full binary-shard readback, sort and partition
   checks, compressed and uncompressed byte and SHA checks, sparse-artifact
   verification, immutable manifests, an atomic cycle pointer, and SHA-on-
   reuse. Sampling cannot replace these gates.
6. Allow the columnar fact publisher to consume either the legacy or compact
   occurrence contract during migration. For clean equivalent source
   membership, adopt already-verified content-addressed Parquet shards under
   compact ancestry without rewriting identical facts. Dirty cycles build
   their selection bitmap from compact index row ordinals.
7. Retain the legacy 2024 evidence until an explicit cleanup decision. Accept
   the compact layout for future publications, but keep its Dagster leg paused
   until compact calculation membership and coordinated fact-bundle readiness
   pass.

The exact input identities, byte accounting, bounded gate, complete-corpus
measurements, and replay evidence are in the
[compact occurrence publication audit](./audit/schedule-a-compact-occurrence-publication-2026-08-31.md).

---

## 2026-08-31 — Adopt compact receipt-calculation membership

The accepted receipt calculation logically classifies every Schedule A fact,
but the original publisher repeated one JSON decision document per row. The
direct 2024 probe proved the calculation without that artifact. The production
compact publisher then evaluated all 264,085,606 rows from the accepted
Parquet fact set through the same ordered predicate and reproduced the direct
probe exactly: every decision, route, amount, candidate state, reconciliation,
and both result-artifact hashes matched.

The compact publication materialized two unresolved-amount exceptions and
8,175 candidate results. Its complete tree, including both manifest copies,
uses 1,077,859 bytes. The first publication completed in 876.907 seconds. A
same-input backing replay returned the immutable calculation in 8.032 seconds.

Decision:

1. Represent ordinary receipt-calculation membership with the exact columnar
   fact-set identity, one-based source row ordinal, and versioned ordered
   predicate. Do not materialize a dense decision artifact.
2. Project only the predicate's declared columns, but rehash every referenced
   Parquet shard before use and require a complete valid fact set.
3. Materialize unresolved individual class, unresolved amount, and invalid
   routed-date membership as sparse content-addressed exceptions.
4. Keep candidate results materialized under the existing result schema.
   Require exact row, decision, route, candidate, amount, exception, result,
   and reconciliation conservation before atomically advancing the cycle
   pointer.
5. Reject the dense per-row decision publisher for complete cycles. Retain it
   only as fixture-tested logical-contract evidence.
6. Keep Dagster automation paused until a coordinated fact-bundle readiness
   mapping binds Schedule A and all three classic inputs from one source
   release without a timing race.

The exact inputs, predicate, corpus counts, artifact hashes, runtime, storage,
and replay evidence are in the
[compact calculation publication audit](./audit/compact-receipt-calculation-publication-2026-08-31.md).

---

## 2026-08-31 — Bind candidate-receipt inputs with an immutable fact bundle

The compact calculation requires four independently published fact sets:
Schedule A receipts, candidate-committee linkage, all-candidates summaries,
and current-campaigns summaries. Reading four active pointers at calculation
start could mix publication generations. Dagster also could not express the
join precisely while classic partitions encoded `dataset:cycle` as one dynamic
string.

The 2024 bundle publisher resolved the four active pointers, matched them to
their immutable manifests, required one cycle and coordinated source release,
and rehashed all 265 Parquet shards plus the three classic artifacts. It
published bundle
`b00ce42a65696310b8c1f8c3f8f3bc28077f5bc774e4955657cc16b210d629a3`
in 8.084 seconds. A same-input replay returned the original manifest in 8.039
seconds. A compact calculation replay using only the bundle path returned the
existing calculation set in 8.027 seconds with every count and artifact
identity unchanged.

Decision:

1. Make an immutable, calculation-specific fact bundle the readiness boundary
   for candidate itemized-individual receipts. The bundle contains only exact
   fact-set and manifest identities, counts, and checks; it copies no facts and
   performs no calculation.
2. Require the Go publisher to enforce the exact four roles, one cycle, one
   source release, active-to-immutable manifest equality, all blocking fact
   checks, and complete referenced-artifact integrity.
3. Require the compact calculation to validate the supplied bundle against its
   immutable domain copy and resolve only the immutable fact manifests named
   by it. Retain direct fact flags only for controlled manual diagnostics.
4. Replace opaque classic `dataset:cycle` partition strings with a Dagster
   multi-partition containing separate static `dataset` and dynamic `cycle`
   dimensions. Map only linkage and the two required summary datasets into the
   receipt bundle's `bundle`/`cycle` multi-partition.
5. Use eager Dagster automation for the bundle and downstream compact
   calculation. Dagster decides when mapped assets are available; Go remains
   the independent release-coherence and integrity authority.
6. Accept the current verified columnar path for weekly automation. Keep
   bounded parallel readback as throughput debt, not a readiness or integrity
   gate. Do not weaken complete verification to optimize it.
7. Make the real-corpus ArangoDB projection probe the next implementation gate.

The exact inputs, checks, manifest identity, runtime, idempotence, and
calculation replay are in the
[fact-bundle audit](./audit/candidate-receipt-fact-bundle-2026-08-31.md).

---

## 2026-08-31 — Keep fine FEC facts in Parquet and project graph-bearing candidate receipts into ArangoDB

The accepted compact calculation made the first complete-cycle graph probe
possible without scanning Schedule A again or loading each wide receipt as a
graph document. The Go probe consumed the exact 2024 fact bundle, compact
calculation, candidate master, and committee master. It projected 31,035
entities, 8,175 candidate results, 8,584 candidate-committee relationships,
and 2,116 receipt components into an isolated content-addressed ArangoDB
database. All collection counts matched. Direct lookup and one-hop inbound
graph queries completed repeatedly at sub-millisecond local warm-cache
latencies. A replay reused the completed database without importing again.

The run also found 143 candidate IDs and 156 committee IDs used by the
calculation but absent from same-cycle master facts. Those IDs are explicit
`missing_master_fact` vertices and make the projection partial.

Decision:

1. Keep immutable source evidence and large fine-grained fact relations in
   filesystem artifacts. Processed Schedule A remains exact zstd COPY source
   authority and its accepted fact layout remains Parquet.
2. Use ArangoDB for query-bearing entities, traversable relationships,
   monetary components, complete calculation results, and exact projection
   lineage. Do not store every Schedule A fact as a verbose graph document.
3. Bind every graph projection to the exact fact bundle, calculation set,
   candidate master, committee master, and manifest byte digests. Derive the
   projection ID and isolated probe database name from those inputs plus the
   model version.
4. Publish projection metadata last after deterministic imports and exact
   count checks. Resume an incomplete probe through same-key replacement;
   reuse a completed matching projection; never automatically drop or truncate
   a database.
5. Preserve source IDs missing from master files as explicit placeholders and
   a partial state. Audit candidate and committee history before adding display
   attributes; never infer those attributes from names or discard the edges.
6. Treat the current graph as a physical-model proof only. It supports direct
   candidate-result and inbound committee-component queries. It does not prove
   donor-to-candidate paths, PAC transfer chains, cycles, terminal sources,
   centrality, community detection, or independent spending.
7. Keep `cn`, `cm`, `ccl`, `weball`, and `webl` as cycle-scoped bulk inputs.
   Keep processed Schedule A as detailed-receipt authority. Require audited
   processed Schedule B before committee-flow paths and processed Schedule E
   before support/oppose independent-spending edges. `indiv`, `oth`, and
   `pas2` remain comparison evidence rather than silent substitutes.
8. Keep the probe manual until the master-history gap and production
   publication shape are decided. Dagster will orchestrate the eventual graph
   asset; Python will not implement graph logic or use an ArangoDB client.
9. Use ArangoDB's HTTP API from the Go standard library for this boundary. Do
   not add a driver dependency without a concrete capability or maintenance
   benefit.
10. Handle the end-of-life ArangoDB 3.11 development image as a separate
    compatibility upgrade. Do not combine a storage-engine migration with this
    physical-model decision.

The physical contract and exact real-cycle measurements are in the
[projection design](./design/arango-candidate-receipt-projection.md) and
[projection audit](./audit/arango-candidate-receipt-projection-2026-08-31.md).

---

## 2026-08-31 — Separate flow observations and use Schedule E for independent expenditures

The legacy graph treated selected `pas2` disbursements and `oth` receipts as
flow edges and summed `pas2` `24A`/`24E` rows as independent expenditures. A
complete 2024 scan proved that all 703,597 `pas2` `SUB_ID`s occur exactly once
in `oth`. It also showed that the legacy sender and receiver transfer cohorts
had only 427 possible reversed-endpoint/date/amount row pairs and that the IE
cohort contained 2,987 repeated filer-plus-transaction keys.

The current processed Schedule E body is only 43.38 MB for all history and is
updated weekly. A same-day 2026 comparison found 14,732 exact shared IE rows
between Schedule E and `pas2`. Candidate and type agreed, but `pas2` lost
fractional dollars on 9,367 shared rows, including 535 nonzero Schedule E
amounts represented as zero. The aggregate precision loss was $4,576.32.

Decision:

1. Preserve committee receipts, committee disbursements, and independent
   expenditures as separate source ledgers. Relate a possible sender/receiver
   pair through a versioned reconciliation fact; never sum or merge it merely
   because endpoints, date, and amount resemble one another.
2. Use processed Schedule A as the authority for receipt observations and the
   first inbound committee-flow graph. Candidate-controlled receipts join
   those facts to accepted cycle-specific authorization relationships.
3. Treat classic `pas2` as candidate-context and comparison evidence over its
   shared `oth` occurrences. A shared `SUB_ID` does not create another money
   fact.
4. Accept processed Schedule E as the recurring independent-expenditure
   occurrence authority. Keep classic `pas2` and 24/48-hour reports as
   separately identified comparison evidence; neither patches Schedule E.
5. Keep the Schedule E source selection separate from the effective-record
   calculation. Action states, repeated keys, amendments, memo records,
   missing transaction types, estimates, and time meanings remain explicit
   calculation gates.
6. Defer Schedule B authority until its real-corpus comparison passes. The
   first receipt-side graph does not require it, but complete sender-side
   outflows and two-sided reconciliation do.

The exact source identities and measurements are in the
[corpus audit](./audit/fec-classic-flow-and-schedule-e-2026-08-31.md). The
accepted target facts and invariants are in the
[flow fact requirements](./design/fec-flow-fact-requirements.md).

---

## 2026-08-31 — Keep immutable facts outside the graph and use ArangoDB as a derived investigative projection

The product must return traversable money paths, explore neighborhoods in
either direction without a fixed depth, and later support shortest-path,
cycle, centrality, and community analysis. The accepted receipt probe also
showed that copying every fine-grained Schedule A fact into a graph database
would add cost without improving those queries. PostgreSQL graph extensions
do not yet provide a simpler, proven replacement for this workload.

Decision:

1. Keep immutable raw, occurrence, fact, and calculation artifacts on the
   filesystem in their accepted content-addressed formats. They are the
   replayable authority; ArangoDB is not the only copy of evidence.
2. Use ArangoDB for derived query-bearing entities, monetary relationships,
   path projections, and projection metadata. Every projection names its exact
   fact and calculation ancestry and can be rebuilt.
3. "One database type" does not mean one logical database. Separate logical
   databases or versioned projection databases are allowed when they improve
   isolation, atomic publication, replay, or rollback.
4. Keep graph construction, traversal contracts, and batch algorithms in Go.
   Let Dagster orchestrate them. Do not move graph logic into Python.
5. Do not add ArangoDB's separately packaged Graph Analytics product merely
   because it exists. Implement and measure the required batch centrality and
   community jobs in Go first; add another service only for a demonstrated
   capability or performance gap.
6. Keep the current ArangoDB engine while the projection model develops. Pin
   the floating 3.11 image to an exact patch or digest, then test 3.12 storage
   compatibility and rollback in a separate infrastructure change. Do not
   couple that upgrade to Schedule E or graph-model implementation.
7. Re-evaluate PostgreSQL graph extensions when they provide equivalent
   traversal, path-return, indexing, operational maturity, and measured
   performance. A future migration remains possible because the graph is a
   derived projection rather than source authority.

---

## 2026-08-31 — Use direct custom-dump COPY order as physical source identity

The Schedule E source audit initially measured a restored relation through
`COPY TO STDOUT`. The coordinated release stager preserves the original data
rows emitted from the custom dump's `COPY FROM` section. Both complete streams
have 548,318 rows, 367,801,873 bytes, and the same sorted-row digest, but a
valid row first changes position at row 302 after restore.

Decision:

1. Use direct custom-dump COPY payload order as the immutable staged-source
   identity for processed Schedule A and Schedule E.
2. Bind every selected relation's exact COPY column list and order to its
   compiled source schema. Reject a relation before staging if the header
   differs, even when field count is unchanged.
3. Derive occurrence row ordinals and byte locators from direct dump order.
   They are physical locators, not semantic ordering claims.
4. Treat restored-table COPY output as diagnostic semantic evidence only. Do
   not use heap scan order as a release identity or compare its ordered digest
   with the direct dump digest.
5. Use a complete canonical sorted-row digest only for bounded audits that
   need to prove row-multiset equivalence. Do not add that external sort to
   every weekly release.

The exact identities and live publications are in the
[Schedule E v2 publication audit](./audit/schedule-e-v2-publication-2026-08-31.md).

---

## 2026-08-31 — Calculate effective independent expenditures from processed regular reports

Processed Schedule E and the 24/48-hour notice product report related outside
spending at different filing stages. OpenFEC marks regular-report `fec_fitem`
rows as most recent, while the notice feed needs its own amendment selection.
The FEC Schedule E candidate aggregate reads the processed relation, excludes
memo-code `X`, requires an amount, and uses the signed sum. A four-cycle notice
scan also found large amendment populations, repeated transaction keys, and
tens of thousands of transaction keys shared with the processed relation.

Decision:

1. Use the exact cycle-scoped processed Schedule E fact set as the only input
   to the first effective independent-expenditure calculation.
2. Exclude memo-code `X`; include every other exact reported signed amount.
   Keep null or invalid non-memo amounts as unresolved exceptions.
3. Do not implement a second action-code winner or transaction-key dedupe over
   the processed relation. Preserve those source fields and repeated-key
   diagnostics for investigation.
4. Keep 24/48 notices as separate timeliness and filing-chain evidence. Reject
   notice-like rows in this calculation and never add notice amounts to the
   processed result.
5. Route included amounts only with spender committee, candidate, and valid
   support/oppose context. Preserve every unrouteable included amount and its
   exact cents as a sparse exception.
6. Group results by spender committee, candidate, support/oppose, and cycle.
   Never merge support with opposition or add either to candidate-controlled
   receipts.
7. Keep immutable calculation artifacts on disk. Project them into ArangoDB
   only through a separately versioned, evidence-backed graph publication.
8. Keep domain rules and arithmetic in Go. Python remains the thin Dagster
   asset wrapper.

The contract and all four real publications are recorded in the
[effective independent-expenditure audit](./audit/effective-independent-expenditures-2026-08-31.md).

---

## 2026-08-31 — Project effective independent expenditures as exact support/opposition graph edges

The accepted Schedule E calculation produces one result for each spender
committee, candidate, stance, and cycle. The investigative graph needs that
relationship, but it does not need another copy of raw Schedule E facts or a
candidate-receipt fiction. The complete 2024 calculation is small enough to
verify every derived edge and exact amount after import.

Decision:

1. Project one effective calculation result as one directed spending-
   committee-to-candidate edge. Keep support and opposition as separate
   relation types and never add either to candidate-controlled receipts.
2. Preserve exact signed decimal minor units and source counts on each edge.
   Bind the edge to its result, calculation set, Schedule E fact set, and
   coordinated source release.
3. Keep raw Schedule E occurrences and facts outside ArangoDB. Immutable
   artifacts remain authoritative; the graph is a rebuildable query projection.
4. Project only candidates and spending committees referenced by an edge.
   Represent an absent same-cycle master as an explicit placeholder and mark
   the projection `partial`; never drop the monetary relationship or invent
   identity attributes.
5. Derive the projection ID from the model version, cycle, calculation,
   Schedule E facts, and both master-fact identities and manifest digests.
   Reject mixed-cycle or mixed-release inputs.
6. Import into an isolated content-addressed database, read all counts and
   exact amounts back, and write projection metadata last. Reuse an identical
   completed projection only after those checks pass.
7. Keep the Dagster asset manual until an immutable readiness bundle selects
   the calculation and both master sets from one release. Python invokes the
   Go boundary and maps metadata; it contains no graph or money logic.

The physical model and exact measurements are in the
[projection design](./design/arango-independent-expenditure-projection.md) and
[2024 audit](./audit/arango-independent-expenditure-projection-2026-08-31.md).

---

## 2026-08-31 — Bind independent-expenditure graph inputs with an immutable readiness bundle

The outside-spending graph requires one effective Schedule E calculation plus
candidate and committee master facts from one cycle and coordinated release.
Mutable current pointers are convenient operational selectors but are not an
exact automated lineage boundary. The real 2024 bundle passed six blocking
checks and a bundle-only graph invocation reused the existing content-addressed
projection without imports.

Decision:

1. Make an immutable projection-readiness bundle the automation boundary for
   the independent-expenditure graph. It names the exact effective calculation,
   Schedule E facts, candidate facts, committee facts, and immutable manifest
   digests.
2. Keep the bundle metadata-only. It contains no copied fact, calculation
   result, graph vertex, or graph edge.
3. Require one cycle and one coordinated source release. Match each supplied
   pointer to its canonical immutable manifest and verify every referenced
   backing artifact before publication or consumption.
4. Derive the bundle ID from its exact ordered inputs. Publish one immutable
   manifest plus an atomic cycle pointer. An identical input set returns the
   original publication, including its original run evidence.
5. Keep graph identity independent of the wrapper. The projector resolves the
   bundle's immutable inputs, then derives the same projection ID it would
   derive from those inputs directly.
6. Map one same-cycle effective calculation and exactly the `candidate-master`
   and `committee-master` fact partitions into the Dagster bundle asset. Eagerly
   target the matching graph asset after the bundle materializes.
7. Keep all selection validation, artifact verification, money logic, and
   graph logic in Go. Python launches the Go commands, validates their wire
   contracts, and records lineage metadata only.

The contract, real identities, replay behavior, and automation boundary are in
the
[projection-readiness bundle audit](./audit/independent-expenditure-projection-bundle-2026-08-31.md).

---

## 2026-08-31 — Resolve Schedule E candidate identity per fact before graph grouping

The accepted effective independent-expenditure calculation groups by the
candidate ID reported on Schedule E. The complete 2024 graph gate exposed 92
reported IDs absent from the same-cycle candidate master, while the source
audit found both historical IDs and objective name or office contradictions.
The grouped result no longer contains enough evidence to repair those cases.

Decision:

1. Insert an immutable per-fact candidate-reference calculation after
   effective Schedule E membership and before resolved graph grouping. Preserve
   the reported candidate ID and every source field; add a separate nullable
   resolved candidate ID and explicit evidence codes.
2. Pin the exact effective calculation, its Schedule E fact ancestry, and the
   candidate-master fact set from the same cycle and coordinated release.
3. Use only exact normalized name and office context in the first method.
   Normalize an alphanumeric token multiset; require office for president,
   office plus state for Senate, and office plus state and district for House.
   Do not use fuzzy, nickname, semantic, LLM, `pas2`, or cross-cycle matching.
4. Publish five quality states. `confirmed` has reported-ID and exact-context
   corroboration. `resolved` uses one unique exact context to supply or replace
   an ID. `unverified` retains an ID that exists in candidate master when
   context cannot corroborate it. `ambiguous` and `unresolved` carry no
   resolved ID.
5. Permit a candidate edge for confirmed, resolved, and unverified decisions,
   while retaining the decision state on the derived group. Never drop the
   amount. Conserve ambiguous and unresolved facts outside candidate edges.
6. Keep the 58,288 per-fact decisions as a dense immutable zstd JSONL artifact.
   This layer is small enough to retain directly and is the evidence required
   to regroup without rereading source semantics downstream.
7. Reject exact-context corroboration as a prerequisite for retaining a
   present ID. The first measured method falsely made 10,936 identified facts
   non-projectable because of punctuation, honorific, middle-name, district,
   or candidacy-history differences. Preserve its immutable audit artifact,
   but advance the calculation method and canonical pointer.
8. Do not modify the existing reported-ID ArangoDB probe in this change. First
   publish resolved spender-candidate-stance groups with decision-state counts;
   then version the readiness bundle and graph projection to consume them.

The accepted v2 method conserved 58,288 facts and $4,337,242,339.31. It
produced 45,185 confirmed, 1,811 resolved, 10,996 unverified, zero ambiguous,
and 296 unresolved decisions. Exact identities, coverage, artifacts, and
replay evidence are in the
[candidate-resolution audit](./audit/independent-expenditure-candidate-resolution-2026-08-31.md).

---

## 2026-08-31 — Group resolved outside spending before candidate-edge projection

The per-fact candidate-resolution calculation preserves the evidence needed to
repair Schedule E references, but a graph should not carry one edge per source
fact. The earlier compact results group by the reported candidate ID and cannot
be relabeled safely. The replacement boundary must preserve identity quality
and the money excluded from candidate edges.

Decision:

1. Publish a separate immutable `fec/resolved-independent-expenditures`
   calculation from the dense candidate-resolution decisions only. Do not
   reread or reinterpret Schedule E at this layer.
2. Group `confirmed`, `resolved`, and `unverified` decisions by spender
   committee, separate resolved candidate ID, and support-or-oppose stance.
   Retain exact signed totals, sign counts, and separate count and signed-amount
   components for all three identity states on every group.
3. Never create a candidate group for `ambiguous` or `unresolved` decisions.
   Publish one sparse exception per decision with its upstream decision, source
   fact, reported candidate, spender, stance, method, and exact amount.
4. Block publication unless all input decisions and signed cents conserve by
   state and through exactly one of the group or exception routes. Reject
   legacy candidate-resolution methods.
5. Publish a new resolved projection-readiness bundle. Require its candidate
   master to be the exact fact set used by candidate resolution, in addition to
   cycle and coordinated-release coherence. Pin the dense decision, grouped
   result, sparse exception, and both master-artifact identities.
6. Publish Arango projection v2 into a new content-addressed database. Preserve
   the v1 reported-ID database as historical physical and arithmetic evidence;
   do not mutate or relabel it.
7. Put candidate-resolution quality components on each v2 edge and explicit
   unprojectable count and signed amount in projection metadata. A missing
   candidate master is a blocking failure because every projectable identity
   must derive from the exact candidate master.
8. Move Dagster outside-spending automation to the resolved aggregate, bundle,
   and v2 graph. Retain v1 assets for manual historical replay.

The complete 2024 aggregate conserved 58,288 decisions and
$4,337,242,339.31. It published 5,303 groups covering 57,992 decisions and
$4,318,692,700.10, plus 296 exceptions covering $18,549,639.21. The v2 graph
matched all 5,303 edges and exact amounts on readback, had zero missing master
facts, passed sub-millisecond representative query gates, and reused its
content-addressed database on replay. Exact identities and measurements are in
the
[resolved outside-spending audit](./audit/resolved-independent-expenditures-2026-08-31.md).

---

## 2026-08-31 — Bind policy fixtures to Go and roll FEC periods only through a new inventory version

The first resolved outside-spending graph passed for 2024, but one successful
cycle could hide a cycle-specific identity assumption. Source authority,
source-contract maturity, runtime policy, and rolling release membership also
need mechanically distinct states.

Decision:

1. Promote the processed Schedule E source contract to `accepted`. Its exact
   source-object, schema, complete-corpus parser, occurrence, fact, effective-
   calculation, and downstream identity gates pass. The source contract still
   does not define an outside-spending total by itself.
2. Keep the processed Schedule A source contract `draft`. Schedule A remains
   the selected authority for the implemented receipt slice, but broader
   historic memo/conduit coverage is an explicit remaining source-contract
   gate. Authority selection does not imply that every future interpretation
   of the source is accepted.
3. Validate every checked-in source contract against the shared source-
   contract JSON Schema in the normal test suite. Lock the intentional
   Schedule A `draft` and Schedule E `accepted` distinction with a regression
   test.
4. Load the checked-in candidate-resolution and resolved-grouping policy
   fixtures from Go tests. Runtime decisions, methods, grouping, exceptions,
   counts, and signed cents must match the machine-readable contract. Do not
   maintain unconnected prose, fixture, and implementation copies.
5. Treat the 2020, 2022, 2024, and 2026 period set as immutable membership of
   release inventories v1 and v2. Advancing the rolling four-cycle hot view
   requires a new inventory and schema version, exact new source membership,
   and an explicit compatibility plan. Never edit a replayable inventory in
   place or derive its periods from the wall clock.
6. Derive Dagster cycle partitions from the published release manifest. Keep
   the frozen period list in the versioned Go inventory as replay identity,
   not as an independently editable scheduler constant.
7. Run the exact candidate-resolution method and resolved projection gates on
   every frozen cycle without candidate- or cycle-specific exceptions. Treat a
   failure as source reality, missing source coverage, or a method defect. A
   method defect requires a new version; it never receives an inline ID patch.

The unchanged method passed all four cycles. Across 201,250 decisions it
conserves $10,358,126,288.01. All four graph projections are `ready`, have zero
missing masters and exact readback, and reuse their content-addressed
databases. The 1,198 unprojectable decisions and $32,673,571.32 remain explicit
outside candidate edges. Exact identities and the coverage matrix are in the
[cross-cycle audit](./audit/resolved-independent-expenditures-cross-cycle-2026-08-31.md).

---

## 2026-08-31 — Define receiver-reported committee flow by exact identity and receipt role

The complete processed Schedule A relation contains inbound receipts,
outbound and intermediary records, semantic memos, earmarked records, and
noncommittee receipt types. A first probe that treated matching C-shaped
contributor IDs as sufficient flow evidence admitted millions of records whose
receipt semantics did not describe money received from that contributor.

Decision:

1. Use processed Schedule A as the first receiver-reported committee-flow
   authority. Keep Schedule B as independent sender-reported reconciliation
   evidence; never add both reports as two amounts.
2. Require valid normalization, an exact recipient committee ID, agreement
   between exact raw and cleaned contributor committee IDs, non-memo status,
   and an exact reported signed amount before evaluating receipt role.
3. Include only exact inbound registered-filer contribution, registered-filer
   in-kind, affiliated-transfer-in, and received-refund/repayment codes. Keep
   each role separate in the result key.
4. Exclude exact outbound, semantic-memo, earmarked, and noncommittee receipt
   roles from direct committee-flow edges. Retain their facts and measured
   amounts. Earmarked records remain inputs to later conduit attribution.
5. Preserve one-sided or conflicting IDs, unknown amounts, and missing or
   unmapped receipt roles as explicit unresolved states. Never repair them
   with names, entity type, `is_individual`, committee masters, fuzzy matching,
   or hand-coded committee exceptions.
6. Treat publisher-derived entity type and `is_individual` as diagnostic
   signals only. They can disagree with exact identity and receipt-role
   evidence and cannot override the calculation.
7. Version decision order and receipt-code mapping together. A changed mapping
   requires a new calculation version and full fixture and corpus replay.
8. Implement the immutable calculation publisher before adding the
   content-addressed ArangoDB projection. Terminal-source tracing consumes the
   published calculation, not the diagnostic cohort.

The accepted complete 2024 cohort contains 320,731 rows, 180,283 source-
recipient-role groups, and $4,672,820,179.49. It leaves 792,018 unknown-role
rows and 2,101 one-sided-ID rows explicit. Exact input identity, rejected-rule
evidence, role distributions, and conservation are in the
[receiver-flow cohort audit](./audit/receiver-reported-committee-flow-cohort-2026-08-31.md).

The immutable publisher, verified loader, CLI, schemas, minimal Dagster asset,
and complete 2024 replay gate implemented this decision on 2026-09-01. The
publication conserves all 264,085,606 facts and $53,409,157,201.64 of known
signed observations. See the
[receiver-flow publication audit](./audit/receiver-reported-committee-flow-publication-2026-09-01.md).

---

## 2026-09-01 — Project receiver-reported committee flows through an exact bundle and bounded graph queries

The accepted receiver-flow calculation is small enough for graph use and
contains the PAC-to-PAC relationships needed for multi-hop and cycle analysis.
Its committee display facts still come from a separate cycle-scoped master.
Those inputs must not drift independently, and unrestricted path enumeration
is unsafe on a large cyclic graph.

Decision:

1. Publish an immutable projection-readiness bundle that pins one receiver-
   flow calculation, its Schedule A ancestry, and exactly one committee-master
   fact set from the same cycle and coordinated release.
2. Accept only that bundle at the graph boundary. Do not support scheduled
   projection from independent mutable input flags.
3. Store one vertex per referenced committee and one directed edge per source,
   recipient, and receipt-role calculation result. Preserve exact signed cents,
   sign counts, result identity, calculation identity, fact-set identity, and
   release identity.
4. Preserve referenced IDs without same-cycle master facts as explicit
   `missing_master_fact` vertices and mark the projection `partial`. Never drop
   their edges or infer display metadata.
5. Compute weak components, strong components, cyclic strong components, and
   cycle membership over the complete deduplicated adjacency before import.
6. Use bounded neighborhoods, capped ranked shortest paths, directed shortest
   paths, and bounded cycle traversal for the interactive query gate. Do not
   make exhaustive simple-path enumeration the default.
7. Import into a content-addressed isolated database, read every count and
   signed amount back through paginated cursors, and write completion metadata
   last. Reuse only an exact completed projection.
8. Keep receiver-reported Schedule A edges distinct from later sender-reported
   Schedule B assertions. Reconcile them in a separate calculation before any
   terminal-source total.

The 2024 graph contains 8,397 committees, 180,283 edges, and
$4,672,820,179.49. It has 35 cyclic strong components containing 2,304
committees. Exact readback and all bounded query gates pass. Its state is
partial because 707 referenced committees lack same-cycle master facts. See
the [graph audit](./audit/arango-receiver-reported-committee-flows-2026-09-01.md).

The required history audit landed later on 2026-09-01. Official cycle masters
from 1980 through 2026 confirm 675 of the 707 IDs as historical registrations;
32 remain unmatched reported identifiers backed by 50 exact Schedule A rows
and $173,821.08. This evidence does not change decision 4: history is an
attached assertion, not a silent replacement for the bundle's selected
master. The next graph version must distinguish historical registration from
an unresolved reported ID before either can participate in terminal-source
classification. See the
[committee-master gap audit](./audit/receiver-flow-master-gaps-2026-09-01.md).

---

## 2026-09-01 — Publish receiver-flow identity coverage as an additive calculation and graph version

The v1 receiver-flow graph preserved 707 exact reported committee IDs as
generic missing-master vertices. The official-history audit established two
materially different populations: 675 exact historical registrations and 32
IDs absent from every audited 1980–2026 committee master. Backfilling the
selected master would erase time and release semantics, while leaving one
generic state would hide evidence needed by the investigative API and future
terminal-source traversal.

Decision:

1. Publish one immutable identity-coverage decision for each referenced
   committee absent from the selected cycle master. Join registration evidence
   only by exact reported committee ID. Never repair an ID by name, linkage,
   candidate summary, fuzzy matching, or a correction table.
2. Use `historical_registration`, `alternate_release_registration`, and
   `unresolved_reported_id` as distinct gap states. Preserve all historical and
   alternate-release assertions at source grain. Do not copy one historical
   name or classification into current canonical vertex fields.
3. Make every gap state terminal-identity-ineligible. Treat
   `current_cycle_master` as eligible for later classification, not as a
   terminal-source classification by itself.
4. Bind calculation identity only to evidence that can change the decision:
   the exact v1 flow bundle and calculation, selected master, explicit
   comparison masters, normalized histories, and raw archive storage keys and
   digests. Keep linkage and summary inputs in the broader diagnostic audit,
   not the calculation identity.
5. Preserve the v1 readiness bundle, command, Dagster assets, and ArangoDB
   database. Publish an additive v2 bundle that composes v1 with the identity
   calculation, then write a new content-addressed `lt_flow_probe_v2_*`
   database.
6. Require exact ArangoDB readback for entity states, terminal eligibility,
   edges, receipt roles, and signed cents. Keep the projection `partial` while
   any reported ID remains unresolved.
7. Keep v2 manual until official historical committee-master acquisition and
   refresh have an accepted recurring source boundary. Do not add Python data
   logic to promote it.

The 2024 calculation published 675 historical and 32 unresolved decisions in
8.9 seconds. The v2 graph matched 8,397 vertices, 180,283 edges, every state
and role count, and $4,672,820,179.49 on readback. All path and cycle gates
passed. The database is
`lt_flow_probe_v2_2024_0c82fa480c9d9825`; v1 remains unchanged. Exact lineage
and measurements are in the
[identity-coverage graph audit](./audit/arango-receiver-flow-identity-coverage-2026-09-01.md).

---

## 2026-09-03 — Close the Schedule A/B source-alignment gate without merging ledgers

The retained processed Schedule A and Schedule B objects are distinct
immutable versions published on 2026-08-30. A complete 2024 comparison can
therefore measure sender/receiver agreement without the one-week source drift
that blocked the first Schedule B audit. The accepted Schedule A receiver-flow
cohort is small enough to index while Schedule B streams once.

Decision:

1. Treat equal UTC publisher date plus exact immutable object versions and
   digests as the batch-alignment gate for this audit. Bind the accepted
   Schedule A fact and release manifests and rehash both source backings.
2. Compare directed committee endpoints, calendar date, and signed exact
   cents. Permit only a unique endpoint-and-amount candidate to receive the
   `compatible_date_disagreement` diagnostic state and only a unique endpoint-
   and-date candidate to receive `conflicting_amount`.
3. Make any source multiplicity ambiguous. Preserve exact, compatible,
   conflicting, ambiguous, A-only, unmatched-known-endpoint, outside-endpoint,
   and physically ineligible populations explicitly.
4. Keep Schedule A and B as separate assertions. Matching evidence never adds,
   replaces, or silently repairs either amount.
5. Close the Schedule B source-alignment blocker. Do not promote the draft
   source contract or alter the graph in this step. First add Schedule B to a
   new immutable coordinated release inventory and publish lossless selected-
   cycle facts.
6. Define effective Schedule B records and outgoing flow roles as a separate
   calculation over those facts. Publish fact-level A/B reconciliation after
   that policy exists, then expose one economic-flow hypothesis with both
   evidence identities in a new graph version.

The complete audit conserved 264,085,606 Schedule A facts, 157,544,163
Schedule B rows, all 320,731 accepted A flow rows, and every candidate state.
Unique exact or same-amount/different-date candidates cover 57.35% of the A
flow rows and 82.02% of their signed amount. Unique amount conflicts cover
0.10% of rows; ambiguity and A-only evidence remain material. Exact lineage,
amounts, runtime, and caveats are in the
[Schedule A/B alignment audit](./audit/schedule-ab-alignment-2026-09-04.md).

---

## 2026-09-04 — Add Schedule B through release v3 and archive-direct columnar facts

Processed Schedule B passed its physical, classic-product, and same-publisher-
batch Schedule A alignment gates. The official archive is 36.61 GiB, while the
2024 relation expands to about 115 GiB of COPY text. Retaining that extract in
addition to the archive and lossless Parquet facts would add storage and weekly
I/O without adding evidence.

Decision:

1. Preserve release inventories v1 and v2 unchanged. Make v3 the active exact
   inventory by adding one required processed Schedule B artifact and four
   selected two-year relations.
2. Mark those relations `archive_direct`. Validate their catalog presence at
   acquisition, but do not add Schedule B COPY files to release staging. V3
   therefore has 23 artifacts and the same 25 staged outputs as v2.
3. Publish one lossless selected-cycle Schedule B fact for every valid physical
   row. Preserve all 81 decoded source lexemes and exact archive, relation,
   ordinal, byte-offset, and byte-length coordinates. Add only deterministic,
   policy-free typed projections.
4. Use the versioned 98-column Parquet schema, one-million-row shards,
   128,000-row groups, content-addressed files, and independent full-schema
   readback. Require exact source-byte and semantic replay plus global `SUB_ID`
   uniqueness before publication.
5. Checkpoint each completed shard. A retry must replay the source range and
   digest-verify reused shards. An unchanged active fact set must also hash all
   backing shards before idempotent reuse.
6. Bind fact-set identity to the Schedule B artifact, selected relation,
   physical schema, publisher version, and shard configuration rather than the
   whole coordinated FEC release. If a later descendant release reuses the
   same Schedule B bytes, reuse the verified fact set without extracting the
   relation again. Retain the first exact source release as its ancestry.
7. Keep amendment selection, memo counting, recipient resolution, outgoing-
   flow roles, Schedule A/B reconciliation, and graph projection out of the
   source publisher. Each is a later versioned calculation or projection.
8. For already published release-stage outputs, validate immutable compressed
   byte identity on release reuse instead of decompressing every output again.
   Initial staging still performs full decompression and uncompressed-digest
   validation; the immutable manifest retains that evidence. This removes a
   redundant roughly 625 GiB decompression pass from unchanged v3 publication.
9. Make the direct `publish-release` CLI preserve the exact plan, acquisition,
   and stage input bytes under their content-addressed control paths before it
   can advance the active pointer. Dagster already performs this capture; the
   CLI must provide the same evidence-retention guarantee.

The release and fact contracts are
[`contracts/releases/fec/v3/`](../contracts/releases/fec/v3/) and
[`contracts/facts/fec/schedule-b/columnar/v1/`](../contracts/facts/fec/schedule-b/columnar/v1/).

Outcome: the active 23-artifact v3 release published successfully. The complete
2024 fact gate conserved 157,544,163 source rows as 157,544,163 facts with zero
invalid rows and zero duplicate `SUB_ID`s. Its 158 Parquet shards contain
1,261 row groups and 7,862,059,821 bytes, compared with 123,284,784,602 streamed
COPY bytes. The full run took 4,195.543 seconds. Source-stable canonical
adoption took 8.9 seconds, and a complete backing-verified replay took 8.2
seconds with byte-identical manifest output. The exact evidence is in the
[Schedule B publication audit](./audit/schedule-b-columnar-publication-2026-09-04.md).

The manual v3 migration exposed one control-retention defect: two stage
containers shared a convenience stdout path, so the canceled process later
overwrote the successful standalone stage result. The active release embeds
all exact output descriptors, and a regenerated stage matches them; source and
fact evidence are intact. The CLI control-capture rule above now prevents this
failure. Verify the referenced canonical stage artifact on the next changed v3
release.

## 2026-09-08 — Profile Schedule B reporting semantics before accepting flow rules

The lossless Schedule B fact gate proves source preservation, not economic
meaning. Accept a reusable Go diagnostic over the existing verified Parquet
facts before implementing effective-disbursement and sender-flow calculations.

Decision:

1. Verify the exact immutable manifest and every backing shard. Scan projected
   columns with bounded workers and conserve all rows and signed integer cents.
   Do not download or extract the archive again for this diagnostic.
2. Group by exact filing form, line, schedule, action, memo, transaction type,
   and recipient-identity state. Preserve null separately from empty text and
   retain the lowest-ordinal source example for each group.
3. Derive reporting categories from reviewed form-and-line pairs. Transaction
   type alone cannot classify Schedule B activity. Keep unreviewed shapes
   explicit; neither names nor purpose substrings resolve them automatically.
4. Measure a non-memo subtotal as a hypothesis, not accepted total spending.
   Action codes alone do not select winning amendments. Reference presence
   does not establish cross-row amendment families or authorize deduplication.
5. Keep raw-only recipients and self-recipient records explicit. A committee
   identifier or candidate field does not establish a contribution, beneficial
   ownership, or candidate-controlled receipt.
6. Keep this manual diagnostic outside Dagster's publication chain. It writes
   a versioned evidence report, not new facts, calculation pointers, or graph
   edges. Sender-flow membership and two-sided reconciliation remain separate
   acceptance gates.

Outcome: the complete 2024 audit conserved 157,544,163 rows and
$23,765,207,537.31 of signed source amount. The final eight-worker scan took
120.928 seconds. The report passed strict schema validation and independent
row/cent summation; its backing digest and completion marker are retained.
The source includes 8,394 rows outside the reviewed form-line map and 97,850
raw-only self-recipient records, so a generic committee-to-committee sum is not an
accepted flow calculation.

See the [audit contract](../contracts/audits/fec/schedule-b-semantics/v1/),
[complete-corpus findings](./audit/schedule-b-semantics-2026-09-08.md), and
[remaining calculation gates](./design/schedule-b-calculations.md).

## 2026-09-08 — Accept scoped Schedule B reporting calculations, not economic-flow ownership

The complete semantics audit and targeted raw-record review distinguish
reporting categories from economic flows. Beneficiary names occur on ordinary
vendor payments, refunds, and loans. Some raw recipient IDs point to the filer
while the named payee is a vendor. Form 3X line 23 includes forwarded earmarks
and in-kind activity as well as committees' own contributions.

Accept `fec/processed-disbursement-reporting@1.0.0`:

1. Use the exact publisher-processed snapshot without local amendment
   reconstruction or transaction-key deduplication. Keep action codes as
   source evidence; action A is not a standalone membership predicate.
2. Calculate non-memo itemized disbursements only on reviewed Schedule B lines
   of F3, F3P, and F3X. Keep refunds, loans, transfers, contributions, operating
   costs, and other reporting categories distinct. This is not total spending.
3. Preserve memo-X, separate reporting scopes, and unresolved records in
   disjoint accounting buckets. Convention, electioneering, Levin, and
   independent-expenditure-on-SB amounts do not silently enter the regular-
   committee subtotal or supplement the Schedule E ledger.
4. Preserve recipient identity agreement, self-reference state, transaction
   type, and beneficiary/conduit-name presence as independent evidence. None
   resolves ownership or authorizes a graph endpoint.
5. Conserve every source row and exact signed cent. Require deterministic
   result identities, policy-bound membership, verified backing shards, full
   physical-schema checks, and explicit unresolved coverage.
6. Emit a manual deterministic calculation artifact. Defer an immutable
   publisher, persistent cache, and Dagster asset until their downstream
   consumer is defined. Do not add infrastructure solely for this gate.
7. Set `graph_eligible=false`. Sender-flow membership and fact-level A/B
   reconciliation are separate gates before economic-flow graph use.

The full 2024 run passed in 114.175 seconds and retained 55 unresolved rows
with $88,695.97 of signed reported amount. Source facts, existing graph
projections, and the legacy system remain unchanged. See the
[calculation contract](../contracts/calculations/fec/processed-disbursement-reporting/v1/)
and [corpus gate](./audit/schedule-b-reporting-calculation-2026-09-08.md).

## 2026-09-08 — Typed sender cohort and fact-level A/B candidate components

Accept `fec/committee-flow-reconciliation@1.0.0` as a manual evidence
calculation, not economic-flow graph publication:

1. Keep the accepted receiver policy unchanged. Select sender observations
   only through reviewed reporting-role/type agreement, non-memo membership,
   known amounts, valid filer IDs, and agreeing raw/clean recipient IDs.
   Missing codes, self references, earmarks, conduit evidence, and unreviewed
   combinations remain explicit outside the cohort.
2. Distinguish contribution, in-kind, affiliated transfer, refund/repayment,
   and loan roles. A reported endpoint is not proof of the cash payee or
   beneficial funding source. Do not infer own-money contributions from names
   or missing transaction types.
3. Connect opposite ledgers using exact directed endpoints and the reviewed
   role/amount/date candidate rules. Preserve every competing candidate in
   connected components. Do not greedily claim exact pairs, choose a winner,
   impose an arbitrary date tolerance, or assign a confidence score.
4. Persist each selected fact's immutable source ordinal and separate A/B
   component amounts. Conserve every source row in selection buckets and every
   selected occurrence exactly once in component assertions. An unmatched
   state describes selected-cohort coverage, not missing disclosure.
5. Accept earlier fact publications under a newer coordinated release only
   after verifying their exact selected source bytes and original immutable
   release ancestry. Rehash backing shards and validate physical schemas.
6. Fully read back content-addressed evidence before emitting deterministic
   result JSON. Keep `graph_eligible=false`, existing graphs unchanged, and
   automated publication/readiness/Dagster wiring deferred until the consumer
   contract is defined.

The complete 2024 gate and independent record validation pass. Three complete
candidate replays reproduce identical artifacts without another source scan.
See the [design](./design/committee-flow-reconciliation.md) and
[measured gate](./audit/committee-flow-reconciliation-2026-09-08.md).

## 2026-09-08 — Observation graph before economic-flow resolution

The complete reconciliation profile and targeted source review show that a
candidate component can combine recurring payments, split receipt reporting,
generic and explicit in-kind codes, signed corrections, and source dates
outside the cycle. Neither component identity nor equal amounts establish
one economic payment.

Accept the next [committee-flow evidence graph boundary](./design/arango-committee-flow-evidence.md):

1. Use an isolated content-addressed graph. Keep current receiver-flow and
   outside-spending projections unchanged.
2. Preserve one edge per selected source observation, in separate A and B
   edge collections. Keep candidate components as evidence documents, never
   as extra payment edges or path hops.
3. Require an explicit ledger for financially interpreted traversal. Do not
   add both ledgers, sum amounts along paths, or infer an economic amount from
   a component's totals. Show unresolved evidence and original source fields.
4. Treat current type-based roles as policy-assigned reporting evidence, not
   proof of cash, economic purpose, beneficial ownership, or terminal status.
   A narrative description is review evidence, not a silent classifier rule.
5. Implement immutable reconciliation result publication and exact readiness
   before graph creation, followed by independent ledger conservation,
   readback, source drilldown, and query-isolation gates. Thin Dagster wiring
   follows the accepted Go boundary.

The read-only Go review and its machine schema are implemented. Full profiles,
76 source examples, independent validation, and deterministic replay pass.
The new publisher, readiness bundle, and graph are not implemented by this
decision. Economic-flow resolution remains separate versioned calculation
work. See the [source review](./audit/committee-flow-source-review-2026-09-08.md).

## 2026-09-08 — Immutable reconciliation and observation-only readiness

The [Go publication boundary](./design/committee-flow-publication.md) now uses
the unchanged candidate-reconciliation result as its immutable manifest. No
new calculation wrapper, matching policy, or record-specific exception is
introduced. Its new namespace owns compact evidence and atomic per-cycle
pointers; existing graphs and manual audit artifacts remain unchanged.

1. Verify exact source backing on every invocation. For an existing input and
   policy identity, replay compact evidence without decoding all source rows.
   This still reads Parquet bytes for hashing. V1 retains coordinated-release
   identity; reuse across different releases remains explicit deferred work.
2. Serialize publication per cycle, create immutable manifests without
   replacement, verify persisted evidence before pointer advance, and recover
   a completed immutable result after interruption without source rescanning.
3. Publish a deterministic observation-readiness bundle only after checking
   A/B calculation ancestry and same-cycle committee facts. Pin master bytes
   through occurrence and source manifests, including archive and selected
   compressed/uncompressed member digests and sizes. Older fact publications
   require identical selected bytes; a matching cycle alone is insufficient.
4. Keep readiness narrowly scoped to `committee_flow_evidence`, with
   `same_cycle_master_only` identity inputs and economic-flow eligibility
   false. A ready bundle is not a built graph or a completeness claim.
   Historical-identity automation still requires its own refresh contract.

The observation graph, its ledger-isolation gate, and thin Dagster wiring
follow this boundary. Economic-flow and terminal-source resolution remain
separate versioned calculations.

## 2026-09-08 — Isolated per-occurrence committee-flow evidence graph

The [observation graph](./design/arango-committee-flow-evidence.md) is now
implemented in Go and passes the [complete selected 2024 gate](./audit/arango-committee-flow-evidence-2026-09-08.md).
The source/matching policies and previous graphs are unchanged.

1. Derive observation keys from ledger, fact-set identity, and physical row
   ordinal. Preserve every selected occurrence, including signed/zero and
   parallel observations. Components keep their unchanged candidate IDs and
   full memberships as ordinary documents, never graph edges.
2. Bind the isolated content-addressed database to the verified immutable
   readiness bundle. Retain the full projection identity in completion
   metadata; truncate only the database-name suffix to fit Arango's limit.
3. Compare every imported document field with the source-derived model, not
   merely a stored digest. Write completion only after readback, bounded
   ledger-isolated query checks, source drilldown, and storage measurement.
   Completed replay revalidates and does not replace documents.
4. Require an explicit single ledger for paths. Amounts are not path weights
   or additive path totals. Shortest-path samples are bounded BFS hop paths;
   component and opposite-ledger edges cannot participate. Expose self-loops
   and unresolved same-cycle masters without granting terminal eligibility.
5. Keep the source-row lookup in Go. Python adds no data-plane behavior; the
   separate one-off independent validator is retained audit evidence only.

Thin Dagster wiring follows this accepted Go gate. The graph command is not a
serving API or economic-flow resolver. Historical identity automation and
cross-host publication coordination remain separate explicit boundaries.

## 2026-09-08 — Thin, partition-checked committee-flow orchestration

The [Dagster chain](./design/committee-flow-orchestration.md) invokes the
accepted Go publication, readiness, and observation graph commands. No source
selection, money policy, or graph resolution moves into Python.

1. Pass actual upstream immutable manifest outputs. Map the readiness input
   only to the same-cycle committee-master partition, then map the graph to
   that cycle's exact bundle. Never resolve a current pointer in these assets.
   Go must still reject incompatible source versions across materializations.
2. Expose blocking checks with explicit asset-matching partition definitions.
   Failures emit no materialization. Successful checks follow their output
   so they target the new materialization rather than an earlier one.
3. Combine eager automation with explicit upstream blocking-check readiness.
   A passed check in one cycle cannot authorize another cycle. Pin Dagster
   1.13.20 and test upgrades because partitioned check specs are a preview API.
4. Use logical Go identities for data versions and exact control-result bytes
   for artifact identities. Preserve partial master coverage and separate
   ledger measures. Keep complete source rows out of routine Dagster metadata.
5. Resolve JSON schema references only through checked-in local contracts.
   Reject foreign authorities, path escapes, and mismatched schema IDs without
   HTTP fallback. The adapter adds validation, not domain interpretation.

This adds jobs and one automation sensor under the existing default-status
switch. It adds no source schedule or service. The isolated real-data gate
does not claim live weekly daemon operation, other-cycle A/B publication,
cross-release reuse, or a serving API.

## 2026-09-08 — Read-only pinned committee-flow API

The [Go investigative API](./design/committee-flow-api.md) serves the accepted
observation graph without changing source, matching, or attribution policy.

1. Pin one completed projection at startup. Verify source ancestry, graph
   definition, completion metadata, and full document readback before opening
   a listener. The reader cannot publish, import, repair, or create schema.
2. Require one ledger for observation and path queries. Keep candidate
   components as summaries with independently paged ledger members. Preserve
   exact signed strings, source locators, unresolved masters, and explicit
   economic-flow/terminal exclusions.
3. Use stable keyset pagination with authenticated query/projection-bound
   cursors. Restart invalidates cursors. Bound depth, page size, concurrency,
   response size, runtime, and query memory; return no success page after a
   resource or integrity failure. No pre-sort sample represents full coverage.
4. Retain verified source membership/indexes in a read-only source reader.
   Each source request rehashes its selected shard and checks the full row
   against the accepted observation. Full-corpus verification belongs at
   startup, not on every source lookup.
5. Keep the application boundary in Go, with no additional Python runtime
   behavior. Python only validates the wire contract in tests and the retained
   one-off audit. No new runtime dependency or storage representation is added.

This accepts the read-only implementation and isolated 2024 gate, not public
deployment. A resident service needs a declared Compose budget, read-only
database credentials, and accepted proxy/access control. Multi-cycle routing,
date/name filters, economic-flow resolution, and terminal attribution remain
separate work.

## 2026-09-08 — Prioritize candidate upstream funding before UI

The [candidate upstream slice](./design/candidate-upstream.md) now connects
accepted candidate authorization to the complete selected receiver cohort.
UI/deployment work is deferred behind the funding calculation.

1. Reuse same-cycle A/P authorization, shared/conflicting-linkage exclusions,
   and the accepted Schedule A committee-flow policy. Verify exact linkage
   archive/member ancestry against the graph's source release.
2. Count each candidate-linked observation once across external, internal,
   and unresolved-authorization buckets. Internal transfers do not become
   another external receipt. Keep signs and source roles explicit.
3. Traverse complete selected committee ancestry and identify cyclic SCCs
   without assigning dollars to paths or components. Missing masters and
   empty incoming adjacency never establish terminal-source eligibility.
4. Reference upstream occurrences in shared immutable evidence rather than
   copying every source row into each candidate result. Retain source witnesses
   and all membership; compactness does not authorize grain loss.
5. Keep all terminal attribution unresolved until a donor-bearing funding
   basis and allocation policy are accepted. This diagnostic does not select
   a proportional denominator, reconstruct opening balances, or resolve people
   and corporations. The next slice targets those funding inputs, not a UI.

The real 2024 calculation/replay and schema gates pass. The command is manual
and read-only; no graph, canonical current pointer, Dagster automation, or
production serving boundary changed. Release-matched linkage preparation added
immutable artifacts and audit-local pointers only.

## 2026-09-08 — Inventory donor-bearing receipts before allocating funds

The [reported-receipt inventory](./design/committee-funding-basis.md) attaches
existing Schedule A evidence to reached committees without declaring it a
complete cash denominator.

1. Reuse the accepted individual and committee-flow predicates. Preserve both
   decisions, separate their overlap, and conserve every source row in one
   inventory component. Keep memo, unknown, residual, and receipt-role evidence.
2. Retain full source grain in the existing Parquet facts. Add exact manifest
   identities, grouped measures, and shard-presence membership, not another
   receipt corpus. Source lookup reevaluates the predicate and returns all fields.
3. Keep contributor, conduit, and employer observations separate. The measured
   absence of structured conduit IDs does not authorize name-to-ID fabrication;
   memo-text earmark descriptions remain evidence for a later reviewed linkage.
4. Join only exact-source candidate traces. Expose receipt inventory per reached
   committee, but never sum network receipts into a candidate funding total or
   infer terminal status from missing incoming rows or masters.
5. Leave opening balances, unitemized coverage, prior-cycle cash, chronology,
   economic receipt roles, identity resolution, and allocation explicit and
   unresolved. No proportional attribution policy is accepted by this slice.

The manual Go commands pass their complete 2024 inventory, accepted-cohort
equivalence, source-page, and candidate-assessment gates. Python adds wire/audit
tests only. Arango graphs, canonical current pointers, and Dagster are unchanged.

## 2026-09-08 — Separate receipt source roles from publisher individual membership

The [source-role review](./design/receipt-source-evidence.md) adds decisions to
verified source occurrences without changing the existing inventory or either
accepted monetary predicate.

1. Retain each accepted committee-flow occurrence once as reported committee
   evidence even when the publisher individual predicate also includes it.
   Preserve that overlap and conflicting `IND`/`CAN` entity labels; do not
   interpret publisher aggregate membership as resolved person identity.
2. Only a dedicated, valid source conduit-ID field plus a reviewed receipt role
   supplies a reported conduit-ID assertion. It remains identity-unverified.
   Preserve absent/invalid IDs, names, memo text, and report-reference gaps;
   do not manufacture a link from a name or an ID-like memo substring.
3. Keep back-reference semantics distinct from conduit semantics. A positive
   association needs complete same-filing membership and an accepted role rule,
   not a match on transaction ID alone or across amendments/reports.
4. Add no conduit amount and no terminal eligibility. The observed contributor,
   the conduit, and later person/corporate identity resolution remain separate.
5. Bound the complete component review and fail if membership or amounts differ
   from the pinned inventory. Keep full source fields and deterministic order.

The complete 2024 overlap gate and original-file earmark comparison pass. The
raw filing was a bounded research fetch, not a new production source or receipt
authority. The unexplained publisher line-17 classification and positive conduit
association remain explicit research/implementation boundaries. Full funding
coverage, timing, and allocation still require acceptance.

## 2026-09-08 — Resolve report references before inferring earmark memo roles

The [same-report reviewer](./design/receipt-report-association.md) adds a bounded
positive association gate without changing source facts or monetary predicates.

1. Exhaust the exact committee/report population in the selected published
   Schedule A cycle before linking. Do not join across reports, infer completeness
   of another cycle, or assume related records are physically adjacent.
2. Require unique source/target transaction IDs and an explicit compatible
   reference schedule. Preserve missing, duplicate, contradictory, and shared
   relationships as unresolved. Evaluate both reference directions.
3. A positive association requires the reviewed non-memo earmark role, original
   `IND`/`CAN` label, and one related committee memo with matching raw/clean IDs.
   Broader roles and registration remain unverified. This is a structural
   reported association, not a donor resolver or an accepted cash-flow leg.
4. Keep money comparison separate from association. A difference never becomes
   a fee estimate or source correction. The annotation creates zero additional
   money and cannot enable terminal attribution.
5. Bound the manual report reviewer at 10,000 rows; fail without partial success
   above that limit. Production cycle-wide indexing requires a separate
   bounded-memory design, not per-candidate repetition of this command.

The [two-report original-file gate](./audit/receipt-report-association-2026-09-08.md)
passes with complete selected membership and byte-identical replay. Raw files
remain research evidence, not a second production receipt authority. The next
implementation is the [funding coverage audit](./design/funding-coverage-and-time.md)
over existing inputs, before choosing any missing bulk summary product or pooled
allocation model. Arango graphs, canonical pointers, and Dagster are unchanged.

## 2026-09-08 — Audit funding scope before acquiring another financial source

The [funding coverage audit](./design/funding-coverage-and-time.md) is a manual,
read-only Go consumer of the existing receipt inventory and exact summary bundle.

1. Bind the exact Schedule A identity and source release. Scan each selected
   candidate-summary population independently; never combine their rows into a
   financial total or distribute candidate balances across upstream committees.
2. Verify all reviewed source/typed monetary fields and coverage dates, including
   signed and blank values. Reuse the source normalizer's money-field list.
3. Preserve source publication exclusions separately from valid-fact scan
   completeness. An excluded occurrence does not vanish merely because the
   remaining fact artifact passes integrity checks.
4. Distinguish supported observations, absent source fields, incompatible scope,
   and unresolved economic meaning. A populated beginning-cash field at candidate
   scope cannot satisfy committee/report opening-balance requirements.
5. Reuse the conserving receipt-role inventory without decoding all receipt rows
   again. Do not infer unitemized receipts from residuals, available cash from
   arbitrary dates, or terminal amounts from a partial denominator.

The [2024 source gate](./audit/funding-coverage-2026-09-08.md) and independent
artifact comparisons pass with deterministic replay. Next review committee-level
financial-summary bulk options against the missing scope and fields. No new
source, allocation algorithm, graph publication, or Dagster automation is
accepted by this audit.

## 2026-09-08 — Select cycle committee summaries without claiming report history

The [four-cycle source review](./audit/committee-summary-source-2026-09-08.md)
selects the official committee-summary CSV as the next committee-cycle summary
fact family. Its [contract is draft](./design/committee-summary-source.md) until
Go parsing and publication pass. This is not a release-v3 inventory change.

1. Preserve all 92 physical fields and every occurrence. The embedded header
   and actual source bytes resolve reviewed dictionary discrepancies; future
   drift still requires review.
2. Keep typed validity separate from raw preservation. Retain blank and signed
   money, invalid dates, reversed intervals, and coverage outside the cycle.
3. Do not count candidate-reference fan-out as repeated committee receipts.
   Grouping needs exact equal-value proof, complete memberships, and explicit
   conflict handling; normalization itself does not collapse rows.
4. Use explicit unitemized assertions, never detail/summary residuals. Preserve
   arithmetic differences without source correction or a complete-cash claim.
5. Keep cycle summaries distinct from report and account evidence. No API
   fallback, raw-filing authority switch, report-time allocation method, or
   terminal-dollar attribution is accepted by this selection.

Research snapshots remain outside current publication pointers. Next implement
the bounded Go reader, then accept source publication and a versioned release
change before any same-release funding consumer uses these assertions.

## 2026-09-08 — Verify committee summaries without publishing financial assertions

The [strict Go reader](./design/committee-summary-source.md) now verifies captured
CSV bytes and exposes every raw field, typed value, issue, and record locator.

1. Require the capture's expected SHA-256 and byte count before returning records.
   Bound artifacts at 16 MiB, accepted logical records at 1 MiB, and records per
   scan at 100,000. This small source does not require a streaming database stage.
2. Enforce the pinned CSV header, LF encoding, complete framing, and byte/row
   conservation. Physical corruption, partition mismatch, read failure, limits,
   and cancellation never emit a successful partial result.
3. Preserve invalid dates and candidate references beside valid monetary fields.
   Blank, invalid, and zero are distinct. Do not change the shared FEC money
   parser's accepted syntax; adapt the reviewed leading-decimal representation
   locally while retaining raw text.
4. Keep identity multiplicity and diagnostic equations separate from grouping,
   repair, or financial eligibility. Codes remain raw and uninterpreted.
5. Bind every raw field and typed value with deterministic, length-framed hashes.
   Independent corpus tests reconstruct these from the CSV. No path or execution
   timestamp enters the result, so replay is byte-identical.

The [four-cycle gate](./audit/committee-summary-reader-2026-09-08.md) passes.
This lands a reader/verifier, not source publication or a release change. The
source contract remains draft until immutable occurrence/fact publication and
versioned release membership pass; terminal allocation remains unimplemented.

## 2026-09-08 — Publish lossless committee-summary facts through opt-in release v4

The [publication boundary](./design/committee-summary-source.md) now implements
immutable, exact-release occurrence/fact preservation. The
[gate](./audit/committee-summary-publication-2026-09-08.md) separates complete real
artifact verification from synthetic coordinated release integration.

1. Preserve v1–v3 membership. Add four whole CSV artifacts in v4; no ZIP/COPY
   stage or alternative amount ledger is created. Keep default/active v3 until
   fresh all-source discovery and a real v4 release pass.
2. Combine occurrence locators and normalized facts physically in the existing
   zstd JSONL artifact format for this small source. Retain all raw fields,
   typed values, issues, and duplicate occurrences. Use unkeyed record versions
   rather than claiming a stable committee/candidate revision key.
3. Bind each fact manifest to exact source-release bytes. Same CSV bytes may
   reuse row artifacts across releases, but may not inherit false release ancestry.
4. Require complete raw/fact comparison and verified EOF before publication and
   on replay. Create immutable manifests atomically without a current pointer;
   a historical replay cannot roll back another release's data.
5. Preserve the existing FEC free-space floor with a small-publication working
   allowance. Do not start a large source refresh merely to complete a fixture gate.
6. Keep grouping, financial eligibility, same-release funding comparison, and
   terminal attribution out of source normalization. The source contract remains
   draft until real coordinated release acceptance; no summary automation is enabled.

## 2026-09-09 — Count retained inodes once and expose full staging scenarios

The [storage review](./audit/fec-storage-review-2026-09-09.md) confirms that path
size sums double-count the same source inode under snapshot/staging aliases.

1. Acquisition and staging count logical regular-file bytes once per device/inode
   within the Schedule A tree. Separate copies and sparse logical bytes still
   count fully; symlinks are not followed. Do not use content hashes or link
   counts alone as proof of physical deduplication.
2. Keep the 600 GiB hot cap, 500 GiB filesystem floor, 25 GiB margin, and largest-
   extract allowance unchanged. Fail on byte-count overflow rather than wrapping.
3. Add a read-only Go review over saved plan/prior inputs. Derive every selected
   staged output from the inventory. Reuse only unchanged source versions with
   existing backing; represent unseen output sizes as prior-size scenarios or
   unknowns, not bounds. Do not grant speculative CAS/checkpoint credit.
4. Expose the cumulative retained-output envelope separately from acquisition's
   initial check. A scenario that fits is not acquisition authorization. Keep
   old acquisition/stage schemas and Dagster execution unchanged.
5. Do not delete historical captures to make the cap pass. The reviewed aliases
   free no data blocks; the older extracts are distinct source-snapshot evidence.

The corrected initial acquisition check passes, but the real prior-size scenario
still exceeds the hot cap. Next validate and enforce an actual streaming-
workspace budget before downloading. Cold retention remains a separate required
gate for perpetual refreshes; no source-release activation landed in this review.

## 2026-09-09 — Enforce actual source streaming growth within unchanged limits

The [streaming contract](./design/fec-streaming-storage.md) and
[gate](./audit/fec-streaming-storage-2026-09-09.md) replace the former full-
uncompressed-extract allowance. Cap/floor/margin remain 600/500/25 GiB.

1. Set the default uncompressed working reserve to zero because extraction
   streams directly into zstd. Enforce actual file growth during acquisition
   and staging; do not substitute another guessed maximum output size.
2. Keep all selected temporary outputs in the counted Schedule A tree. Completed
   A outputs remain counted; non-A outputs leave it on CAS finalization. Count
   crashed remnants; block on legacy uncounted temporary files pending review.
3. Serialize acquisition/staging ownership with one nonblocking local file
   lock. Share download allowances and refresh staging allowances per output.
   Check available space before bounded writes, refresh hot inode totals, and
   decrease allowances by actual written bytes. The margin is not a host-wide
   space reservation against unrelated writers.
4. Reject complete over-budget prior-size scenarios before GETs. Treat unseen
   sizes as unknown, not zero or a guarantee. Seeds may proceed under runtime
   guards when known acquisition costs pass; the review diagnostic still
   returns nonzero for incomplete scenarios.
5. Preserve download prefixes and completed extract checkpoints on storage
   stops. Reject shared/symlink partial targets and oversized response bodies.
   Remove only an attempt's private failed extraction file; do not delete
   retained CAS or historical evidence. Keep full digest readback and the
   separate release-publication boundary.
6. Emit diagnostic schema v2; preserve historical v1 reports and compatible
   acquisition/stage result schemas. Keep storage policy entirely in Go.

The bounded tests and saved-plan review pass, but no real bulk acquisition,
staging, release activation, or graph change ran. Fresh coordinated v4 review
is next. Cold retention and downstream materialization budgets remain separate.

## 2026-09-10 — Accept the real v4 release and lossless committee-summary source

The [real publication gate](./audit/fec-v4-publication-2026-09-10.md) now passes
the exact coordinated source chain and all four release-bound summary fact sets.

1. Activate v4 only from the completed approved plan/acquisition/stage chain.
   Preserve v3's immutable manifest and artifacts. Publication rechecks source
   metadata and staged hashes; it does not redownload or re-extract source bodies.
2. Accept `fec/committee-summary@1.0.0` for lossless occurrence/fact preservation.
   Preserve every raw/typed value, locator, issue, and duplicate occurrence.
   Source acceptance does not accept financial-assertion grouping, a cash
   denominator, report/amendment/account selection, or terminal allocation.
3. Require all summary fact sets to bind the exact published release digest.
   Full Go readback, independent stored-value comparison, and byte-identical
   publication replay pass for every selected cycle. Do not substitute older
   research artifacts or relabel existing A/B/E graphs/facts as v4.
4. Keep source activation separate from discovery-default migration, thin
   summary Dagster wiring, graph refresh, and perpetual scheduling. The first
   two are next; cold retention remains required before weekly acquisition.

Only source/control metadata and committee-summary outputs changed on disk.
No graph mutation, resident service change, new bulk capture, or evidence deletion ran.

## 2026-09-10 — Accept manual-only summary orchestration; separate operational activation

The [summary Dagster gate](./audit/committee-summary-dagster-2026-09-10.md)
passes exact release/cycle handoff and read-only replay for every published cycle.

1. Register a cycle-partitioned summary asset and a job selecting only that asset.
   Pass the actual upstream release output to the existing Go publisher. Return
   the canonical immutable fact manifest; use its fact-set ID as the data version.
2. Keep source and financial policy in Go. Python validates the pinned envelope
   and requested cycle, forwards allowlisted metadata, and emits a partitioned
   blocking check. Failure cannot materialize a successful asset output.
3. Add no automatic trigger. The asset has no automation condition, schedule,
   or sensor, even when the global scheduling default is enabled. Do not change
   existing sensor defaults, default discovery inventory, or resident services.
4. Separate this integration gate from discovery migration and weekly activation.
   Cold retention remains required before perpetual acquisition. Prioritize
   financial assertion grouping and arithmetic investigation next; summary
   preservation is not a cash denominator or terminal-dollar attribution.

No new data acquisition, extraction, graph mutation, or evidence deletion ran.

## 2026-09-10 — Group exact committee-summary evidence without financial repair

The [assertion calculation](./design/committee-summary-assertions.md) preserves
source grain while exposing repeated financial presentations and arithmetic
differences. The [four-cycle investigation](./audit/summary-assertions-2026-09-10.md)
records measured results and remaining scope gaps.

1. Group only within one verified fact set, cycle, and valid committee when all
   non-candidate source fields are exactly equal. Exclude only `CAND_ID` from
   equality. Keep every occurrence and candidate reference as a member, including
   exact duplicates and invalid candidate references. Do not fill blanks, trim
   text, reformat money for equality, or clip dates.
2. Retain all differing variants and their conflict fields. There is no
   first/last-wins financial selector. Invalid committee/cycle identities remain
   separate unindexed occurrences. A representative fact is a drilldown pointer
   within an equivalent group, not an amendment selection.
3. Compute explicit signed-cent residuals with arbitrary-precision integers.
   Preserve unavailable operands and a null result. Cash and individual-subtotal
   diagnostics do not authorize corrections, missing donors, or unitemized money.
   The separate federal-column sensitivity check is not a fallback or accepted
   form-specific cash identity.
4. Keep financial-use and terminal-attribution eligibility false. Official
   summary corroboration can establish that a discrepancy predates parsing;
   it does not establish the report/account/amendment cause.
5. Keep this boundary read-only and manual. No new Dagster asset, graph amount,
   source refresh, or calculation publication pointer is introduced. Accept a
   compact publication format and specific financial consumers separately.

## 2026-09-10 — Separate report-discrepancy evidence from source corrections

The [bounded original-report investigation](./audit/summary-report-review-2026-09-10.md)
reproduces three different failure modes. It does not establish a universal
repair or a report-level source replacement.

1. Keep a reported summary observation distinct from an accepted cash denominator.
   A later field-scoped consumer must expose its source, period, account/report
   coverage, and arithmetic blockers. No donor or unitemized amount is created
   from a residual, and observed source disagreements do not overwrite raw facts.
2. Treat any future report selector as a separate domain contract. A latest-file
   flag alone does not distinguish a complete financial amendment from an
   attachment. Test inter-report cash continuity as well as within-report
   arithmetic. Keep paper transcription separate from electronic-file layouts.
3. Preserve original images, machine transcriptions, FEC review letters, and filer
   explanations as distinct evidence. A filer explanation is not an accepted
   corrected ledger. Any future correction needs explicit provenance and review.
4. Keep named source examples in audit fixtures, not production exception tables.
   The bounded public API lookups were research discovery only; bulk acquisition,
   source-release membership, Arango, Dagster, and eligibility remain unchanged.

## 2026-09-10 — Assess summary/receipt compatibility before numeric comparison

The [manual Go review](./design/summary-receipt-compatibility.md) and its
[five-case gate](./audit/summary-receipt-readiness-2026-09-10.md) establish a
field-scoped readiness boundary, not an accepted funding denominator.

1. Preserve nine reported fields and every selected assertion/member beside
   verified recipient inventory cohorts. Other source fields remain in the
   immutable facts. Keep blanks, invalid values, signs, and unknown amounts.
2. Bind both source releases and calculation identities. Different releases
   remain visible with an explicit comparison blocker; the same release ID with
   conflicting manifest hashes fails. Same-release fixtures still require
   report/form-line and reporting-period compatibility evidence.
3. Separate comparison blockers from cash-allocation blockers. Cash timing and
   valuation do not automatically invalidate a future scoped reported-subtotal
   comparison, but they do block complete cash allocation.
4. Retain all variants without selection or summation. Attach field conflicts
   to their fields and coverage/type/designation conflicts to scope. Federal
   sensitivity neither rescues nor fails an accepted cash identity.
5. Keep every comparison delta null and all readiness/funding/terminal guards
   false in this version. It introduces no source, correction table, publication
   pointer, graph amount, or orchestration. Next prove the narrow individual
   comparison's compatible ancestry and population in a cycle-wide pass.

## 2026-09-10 — Profile same-release occurrences before rebuilding receipt facts

The [report-occurrence profile](./design/receipt-report-profile.md) makes the next
scope investigation source-aligned without relabeling older facts or requiring a
large fact rewrite merely to inspect form/line membership.

1. Select Schedule A only through the verified summary's exact immutable release.
   Recheck the selected relation's complete physical identities and every source
   row. A net row-count change does not prove append-only changes or shard reuse.
2. Preserve physical occurrences, exact form/report labels, predicate decisions,
   signs, unknown amounts, and receipt-date states. Reuse the existing individual
   predicate; this audit does not replace it with a new counting policy.
3. Keep occurrence profiling separate from unique/effective membership, summary
   comparison, and cash allocation. Date extrema do not establish report coverage;
   a source-selected report reference is not a complete report/account model.
4. Keep the diagnostic manual and read-only, with bounded group caches and exact
   overflow failure. Add no source download, fact pointer, graph write, report
   selector, correction table, or Dagster trigger. Accept a recurring publication
   format and numeric comparison separately.

## 2026-09-10 — Separate report-line membership from individual and date predicates

The [bounded report-line review](./design/receipt-report-lines.md) now preserves
every selected report occurrence and passes a seven-file original Form 3 gate.

1. Review `F3`/`F3X` `SA11AI` as an explicit form-line population. Keep raw memo
   status and publisher individual classification independent. The existing
   individual predicate is neither replaced nor used to define this population.
2. Preserve unsupported lines, unknown memo/amount states, signs, nulls/blanks,
   transaction-identity issues, and exact source ordinals. Do not silently count
   unresolved evidence or discard it. Keep original receipt-source ancestry.
3. Compare a pinned original report's period subtotal only after complete original
   membership is independently checked. That bounded audit does not accept a
   cycle-summary comparison, complete cash coverage, or terminal allocation.
4. Preserve receipt dates outside report coverage. Physical report membership
   comes from the filing reference, not receipt-date clipping. Runtime readiness
   guards stay false until report/account and effective-selection contracts land.
5. Keep original cover offsets and named files in audit tests, not runtime
   corrections. Retained older facts are not active-v4 facts. Extend the future
   source-aligned cycle consumer with these independent axes before numeric use.

## 2026-09-10 — Version complete report-line occurrence profiling separately

The [v2 profile](./design/receipt-report-profile-v2.md) adds every selected-cycle
report-line population without changing the v1 wire contract or financial policy.

1. Preserve memo code and publisher individual flag as independent axes beside
   the old predicate decision and reviewed form-line disposition. Both tables
   conserve every occurrence; their diagnostic subsets overlap and are not added.
2. Share strict source selection, byte/row verification, amount normalization,
   and date parsing across versions. V2 reuses the bounded reviewer's line rule;
   it introduces no parser, named-entity exception, or inferred source correction.
3. Keep dates for excluded and unresolved rows as well as included rows. Source
   ordinal extrema locate group bounds, not a contiguous or exhaustive member list.
4. Select v2 explicitly with `--profile-version 2`; keep v1 as the default.
   Require full same-source regrouping to v1 for the complete-cycle acceptance gate.
5. Keep occurrence profiling distinct from uniqueness, effective-report selection,
   original report/account coverage, and financial comparison. Both readiness
   guards remain false. Do not infer report coverage from receipt-date extrema or
   amendment selection from the largest file number. No recurring asset is added.

## 2026-09-10 — Keep original-source discrepancy review separate from corrections

The [memo/amount review](./audit/receipt-memo-review-2026-09-10.md) traces the
complete reviewed-line exception cohort to bounded official originals.

1. Do not apply electronic format conventions to paper `P3.4` by assumption.
   Preserve raw memo code, checked-box evidence, cover arithmetic, and cash/in-kind
   meaning separately. The review does not authorize `Y` conversion or inclusion.
2. Keep original one-cent values and processed nulls as conflicting evidence.
   No source fallback, named repair, or historical calculation rewrite is added.
3. Keep older fact locators and v4 profile ancestry distinct. Matching grouped
   measures and dates does not establish all-field cross-release equality.
4. Qualify report-level metadata against the
   [coverage requirements](./design/receipt-report-coverage.md) before selecting
   reports. Include zero-itemization and attachment-only witnesses. This does
   not select a new recurring source or authorize a full original-file download.

## 2026-09-10 — Preserve endpoint-scoped report assertions before selection

The [report-metadata qualification](./audit/receipt-report-metadata-2026-09-10.md)
completes bounded bulk/API/schema research against retained originals. This is
not acceptance of recurring API acquisition or a financial report selector.

1. Bulk period-summary headers without filing IDs are separate observations,
   not an exact report-history join. Do not reconstruct identity from equal totals.
2. Preserve processed endpoint status/link assertions independently. Checked
   endpoints disagree; latest/amended flags and zero cover amounts do not prove
   complete financial replacement. Negative references and raw JSON types remain
   evidence, not silently normalized URLs or numeric values.
3. Keep source observation times and ancestry separate. Same-week acquisition
   does not establish a common publisher snapshot; receipt-date-only polling
   does not prove current status for old reports.
4. Treat account scope as disclosed reporting/form/field scope, not a requirement
   for private bank-account identifiers. Unknown scope stays explicit.
5. The newly pinned paper P3.4 schema documents the memo field's `X` convention.
   It does not justify converting the observed `Y` values or changing money.

The proposed separate metadata source needs acceptance and a small Go capture
contract before history acquisition. A/B/E remain bulk-only detailed ledgers.

## 2026-09-10 — Accept separate report metadata, starting with local Go review

The user accepted the separate OpenFEC metadata layer alongside bulk transaction
ingestion. The [Go reader](./design/report-metadata-reader.md) implements its
first capture-descriptor and assertion-preservation boundary.

1. Keep A/B/E bulk-only. Metadata is a separate source with its own endpoint,
   query, observation time, and exact body/header ancestry; it is not an API
   fallback or another transaction ledger.
2. Preserve every raw result and known endpoint disagreement. Normalize only
   lookup-key syntax here. Missing fields, unknown types and duplicate keys do
   not authorize dropping rows, changing money, or selecting the latest report.
3. Separate exact-count satisfaction, observed empty-page termination, complete
   history, and financial readiness. The latter two remain false in this reader.
4. Use explicit local read budgets, confined artifact paths, strict JSON and
   credential-free descriptors. Keep server Date and client-clock evidence distinct.
5. Land bounded HTTP capture next. Full history, retry/freshness reconciliation,
   report-family closure, immutable publication, and weekly activation remain
   separate gates. No new service, Python runtime logic, or Dagster asset is added.

## 2026-09-10 — Bound manual metadata HTTP capture and retain incomplete evidence

The [Go fetcher](./design/report-metadata-capture.md) implements one explicit
metadata query. It does not change bulk-only transaction ingestion.

1. Use the fixed OpenFEC HTTPS origin and header-only authentication. Do not
   follow redirects, send credentials through implicit proxies, or retain raw
   transport errors. Suppress detected credential echoes, even in error bodies.
2. Require page, request, retry, byte, and time limits. Preserve failed attempts
   and bounded rejected source bytes. Record Go-decoded header values explicitly;
   do not claim original wire casing/order or invent source timestamps.
3. Use a new private directory for each run, exclusive synced files, and exact
   hashes. A valid final result plus verified references and exit zero defines
   capture success; process disappearance and checkpoints do not.
4. Require an observed empty page for completed query traversal. An exact count
   is weaker evidence. Neither state proves an atomic API snapshot, complete
   report history, financial replacement, or cash available for attribution.
5. Keep manual transport acceptance separate from production source publication.
   The live test preserves seven valid records but stops on the demo-key quota;
   do not label it a complete traversal. Report-family design can use those
   retained records while the final transport check remains open.

## 2026-09-10 — Separate summary uses from processed-graph availability

The user accepted a narrower next step: qualify summary inputs for attribution
without making repair of the FEC's entire reporting history a prerequisite.
The [summary-value use policy](./design/summary-value-use.md) records that boundary.

1. Keep the accepted processed bulk ledgers and observation graph independent of
   missing summary/report metadata. Retain each source's existing counting rules;
   do not rebuild Schedule A amendment logic or bypass Schedule E's calculation.
2. Distinguish reported scalar, intra-summary arithmetic, scoped comparison,
   qualified funding component, and pooled allocation. A conflict does not erase
   a reported number; equality does not establish complete cash coverage.
3. Attach blockers to exact evidence and the fields, scopes, periods, and uses
   that depend on it. Preserve unrelated observations. A dependent allocation
   stays unresolved; never drop the troublesome path and renormalize donors.
4. Keep explicit unitemized amounts separate from unknown donor composition and
   opening balances separate from their unknown origins. Neither is a residual
   inferred from missing detail or a zero implied by a cycle boundary.
5. Use retained source evidence for a bounded attachment/report assessment next.
   Do not add another fetch, reviewer service, graph dependency, or corrected
   ledger merely to define this policy. No new financial component is qualified
   by this decision; existing runtime guards and published data remain unchanged.

## 2026-09-10 — Assess report representations without selecting financial replacements

The user accepted implementation of the retained attachment assessment. The
[bounded Go reader](./design/report-scope-assessment.md) now enforces that scope.

1. Pin exact local body/header bytes and the reviewed P3.4 schema. Preserve all
   physical records, fields, blanks, and metadata assertions. Never apply the
   paper layout or an unqualified electronic version to another format.
2. Separate financial-cover presence, supplemental transcription shape, and
   unresolved scope. Complete transport is not complete financial history.
   Blank amount fields are not zero-valued financial replacements.
3. Retain endpoint disagreements, nulls, negative/self references, and missing
   evidence. No latest flag, date match, or attachment classification selects
   an effective financial predecessor.
4. Keep original-image review separate from machine-transcription interpretation.
   All original-image, history, and financial-selection readiness flags stay
   false. Do not infer a correction or financial eligibility from cover presence.
5. Keep the reader manual and outside existing graph dependencies. No new
   acquisition, Python runtime path, Dagster asset, transaction rule, or money
   ledger is required. Qualify one field/period before any financial consumer.

## 2026-09-10 — Qualify a same-report field comparison without promoting financial use

The user accepted the next field/interval qualification step. The
[Go total-receipts comparison](./design/report-total-receipts-comparison.md)
implements the narrow first use.

1. Scope the field to Form 3X Column A total receipts, not year-to-date,
   federal-only, donation-only, or cycle totals. Both line 6(c) and line 19
   assertions must be explicit, valid, and equal; never choose between them.
2. Compare each exact-file metadata observation separately after required
   filer/form/report/period checks. Preserve every raw assertion and difference.
   A matching period does not join other files or select an effective version.
3. Emit a signed metadata-minus-cover delta only for qualified reported pairs.
   Missing, blank, invalid, or conflicting required evidence yields a null delta.
   Never turn blank attachment covers into zero-valued financial replacements.
4. Keep unrelated field/cash/YTD and financial-selection status conflicts visible
   without making them global blockers of this local comparison. Equality is
   agreement between correlated representations, not proof of accuracy.
5. Retain false financial-component, cycle-comparison, and terminal-allocation
   guards. Existing cycle-summary readiness-v1 behavior, processed ledgers,
   graph projections, publication, and orchestration remain unchanged.

## 2026-09-10 — Preserve explicit unitemized amounts without inferring donor composition

The user accepted the [bounded unitemized review](./design/report-unitemized-receipts.md).

1. Map the explicit Form 3X Column A field from pinned paper and PAC report
   schemas. Preserve JSON types, blanks, nulls, invalid values, and signed cents.
   Endpoint absence is not a supplied null or zero; YTD is not a period fallback.
2. Retain each source's own amount and interval even when another representation
   is unavailable. Keep exact-file pair scope separate from financial selection.
   No latest/amended flag chooses a winner across assertions.
3. Check individual subtotals using all three explicit operands. A mismatch is
   a diagnostic, not an inferred unitemized amount or an automatic correction.
   It does not erase an explicit scalar; financial consumers still need their
   own conflict qualification.
4. Keep donor composition unknown. Do not infer a donor count, fixed donation-size
   population, corporate origin, or terminal identity from a summary amount.
5. Reuse the two concrete reviews' scope checks without changing prior output.
   Financial/cycle/terminal guards remain false. Keep acquisition, publication,
   processed ledgers, Arango, and Dagster unchanged; financial report membership
   and period/account coverage are the next gate.

## 2026-09-10 — Separate observed report-chain partitions from financial membership

The user accepted the next membership/coverage step. The
[Go diagnostic](./design/report-period-membership.md) implements a conservative
structural gate over already captured report endpoints.

1. Preserve one endpoint's raw observations and every cohort member. Same form,
   report type/year, and dates define a scope bucket, not an amendment family.
   Do not join conflicting endpoints or discard superseded occurrences.
2. Require one explicitly not-amended member, electronic origin throughout,
   and complete consistent chain prefixes within the observed cohort. Latest
   flags, numeric file order, predecessor sentinels, and amounts do not select
   the candidate. Paper/mixed replacements remain unresolved without fallback.
3. Check an explicit inclusive date window with a conserving interval sweep.
   Keep gaps, overlaps, original dates, and boundary-crossing reports explicit.
   Do not infer inactive periods or prorate money using days.
4. Permit observed-partition readiness within a narrower window independently
   of unrelated outside-window conflicts. Never equate it with complete history,
   financial membership, cash continuity, or cycle monetary eligibility.
5. Keep the diagnostic manual and bounded. Existing source/fact publication,
   processed ledgers, monetary calculations, Arango, and Dagster are unchanged.
   Bind chain candidates to qualified report-period fields next.

## 2026-09-10 — Bind exact electronic period fields without promoting cash eligibility

The accepted next step is implemented as a bounded
[electronic report-field binding](./design/report-field-binding.md).

1. Pin the actual 8.4 source workbook; do not reinterpret 8.4 filings using
   paper P3.4 or electronic 8.5 positions. Preserve all original fields and bytes.
2. Re-verify document and metadata inputs. Bind only complete supported covers
   whose identity, period, amendment indicator, and original-chain root agree
   with an observed-chain candidate. Do not rank amendment sequence numbers.
3. Map seven explicit period fields. Every repeated position must agree with
   the metadata amount in exact cents. Preserve blank/null/zero/conflict states;
   failures in one amount do not erase another field's independent binding.
4. Keep superseded observations and visible prefix covers as evidence without
   using them as candidate fallbacks. A matching cash field does not establish
   a continuous cash balance, financial correctness, or terminal identity.
5. Keep cycle-total, cash-basis, and terminal guards false. Existing paper
   diagnostics replay identically. Window-level reported-field membership and
   aggregation come next; bulk sources, graphs, and Dagster remain unchanged.

## 2026-09-10 — Aggregate bound reported periods without summing cash stocks

The user accepted the [window-level calculation](./design/report-window.md).

1. Verify one metadata capture and a bounded descriptor of pinned documents.
   Reuse verified membership internally and emit shared metadata only once.
   Reject duplicate/competing file captures rather than choosing a variant.
2. Keep each field's exact members, missing witnesses, and day coverage. A
   complete reported-window value requires a valid observed partition and
   complete non-overlapping bound coverage for that field. Partial signed sums
   are observations, not complete totals or lower bounds; absence is not zero.
3. Sum five period-flow fields independently. Use opening and closing cash only
   as boundary stocks, never as sums of balances. Do not apportion cross-boundary
   report amounts or count superseded reports as replacements.
4. Emit source-referenced subtotal, cash, and adjacent carry-forward residuals.
   Keep missing operands unavailable; do not bridge unresolved/gapped periods or
   correct source values. Arithmetic mismatches do not erase reported fields.
5. Keep financial-cycle, cash-basis, and terminal-attribution guards false.
   Retained gates and prior-command byte equivalence pass. Compatible reported-
   summary comparison is next; bulk data, Arango, and Dagster remain unchanged.

## 2026-09-10 — Compare reported scalars with field-specific dates and explicit snapshot limits

The user accepted the [summary/window comparison](./design/summary-report-window.md).

1. Reverify published summary evidence and retained report-window inputs. Derive
   the exact committee and cycle from the capture, retain all summary variants,
   and preserve each source's identity. No API fetch or transaction rescan is needed.
2. Compare compatible reported observations without claiming synchronized source
   snapshots or proven common constituent report membership. Keep both limits
   separate from numeric equality and from financial eligibility.
3. Respect the publisher's cycle-start flow/opening scope and latest-report closing
   scope. Do not treat the first report's later start date as the date of cycle-
   opening cash or as evidence of inactive earlier days. Retain blocked operands.
4. Emit summary-minus-window exact signed deltas only for compatible fields.
   Preserve field/scope conflicts, duplicate/fanout membership, and both sources'
   arithmetic discrepancies. Neither balanced arithmetic nor a matching value
   selects a preferred variant or supplies a missing amount.
5. Keep financial and terminal guards false, and preserve older readiness-v1
   behavior. Source-backed prefix coverage, financial account/report membership,
   summary-versus-detail reconciliation, compact publication, and automation remain open.

## 2026-09-10 — Separate declared reported spans from full-cycle financial coverage

The user accepted investigation of early-cycle coverage. The
[source gate](./audit/cycle-prefix-2026-09-10.md) found no proof of earlier
inactivity, but showed that v1 required that proof for a narrower reported
comparison that did not need it.

1. Interpret exact summary coverage dates as the declared span for a reported
   flow comparison, supported by the first-report date definition and the FEC's
   own dated summary presentation. This is not a claim that the source's nominal
   two-year population changed or that constituent report membership is proven.
2. Ship the additive [v2 comparison](./design/summary-reported-span.md), preserving
   v1 byte-for-byte. Require exact matching start/end for flows and all existing
   field/membership checks. Keep opening and closing stock rules unchanged.
3. Expose each assertion's reported span and outside-cycle-envelope intervals
   with conserving dates/day counts. Outside activity remains unknown, not zero;
   envelope dates do not bridge internal report gaps or establish financial use.
4. Do not use registration as first financial activity. Do not reinterpret a
   candidate's election-cycle-to-date column as a two-calendar-year accumulator.
   Preserve all source values and cash discrepancies; no named exceptions,
   source correction, additional transaction fetch, or graph change is authorized.
5. Apply the next comparison to source-aligned receipt detail with use-specific
   membership and field qualification. Full-cycle financial coverage and terminal
   allocation remain separate; neither blocks an otherwise qualified narrower
   reported-observation comparison.

## 2026-09-10 — Compare retained receipt occurrences over bound reported windows

The user accepted the source-aligned detail comparison. The
[retained gate](./audit/receipt-reported-window-2026-09-10.md) matches the reviewed
itemized-individual line to bound report values and the exact-span summary.

1. Revalidate a pinned complete v2 profile, including every group, conservation,
   content identity, and exact verified summary/release ancestry. Reuse the
   accepted physical scan; do not rescan a bulk corpus for a small diagnostic.
2. Compare exact committee/file/form/type/year occurrence populations through the
   existing reviewed non-memo line policy. Retain memo and other-line groups;
   preserve unknowns and duplicate physical rows. Do not filter by individual
   flags or clip receipt dates to reconstruct report membership.
3. Qualify the empty-report zero only with no profile groups, a complete qualified
   original containing no Schedule A records, and explicit bound zero amounts.
   Missing evidence or a nonzero/blank cover never becomes a zero detail value.
4. Keep per-report and whole-window differences separate from financial acceptance.
   Preserve every summary assertion ID, original scope/amount warning, and cash
   discrepancy. Group equality does not establish v4 transaction uniqueness;
   older complete row/original witnesses keep their own source ancestry.
5. Add no graph dependency, correction policy, publication, or Dagster asset.
   Remaining receipt families, complete financial funding bases, and terminal
   allocation need separate contracts; this field's success does not accept them.

## 2026-09-11 — Pin receipt-family meanings before expanding monetary consumers

The user accepted review of the remaining receipt families. The
[source gate](./audit/receipt-families-2026-09-11.md) pins a machine-readable F3/F3X
period-field map with complete workbook checks and conserved saved-profile evidence.

1. Keep source categories, detail coverage and cash role as separate dimensions.
   Loans, transfers, repayments, refunds, offsets and other receipts do not all
   mean new donations. Candidate-guaranteed does not mean candidate-funded.
2. Preserve thresholded detail separately from cover totals and explicit
   unitemized fields. Positive cover amounts without detail remain source
   observations with missing detail, not zeroes, inferred donors or named residuals.
3. Separate leaves from nested totals and account transfers. Exact F3X `11D`
   cannot use the F3 candidate meaning. Do not strip unknown suffixes, mix H3/H5
   transfers with underlying receipts, or add a loan balance as another receipt.
4. Preserve F3P/F4/F9 and unrecognized source keys outside this map. F3P needs
   its own presidential-field and complete-original gate; source storage is unchanged.
5. Accept this as a reviewed field contract, not runtime financial selection.
   Extend Go comparisons additively next, preserving existing outputs. Funding
   acceptance, cash continuity, complete form coverage and terminal allocation
   remain separate. Python is an independent test oracle only.

## 2026-09-11 — Add report-level family comparisons without inferred zeroes

The user accepted the Go comparison step after the source-map review. The
[retained gate](./audit/receipt-family-comparison-2026-09-11.md) verifies an
additive command; existing seven-field and itemized-window results remain unchanged.

1. Bind the reviewed contribution/transfer/loan subset through shared original,
   metadata and observed-chain verification. Check compiled field positions and
   endpoint field types against the pinned contracts. Keep field failures local
   within valid source scope; malformed capture schemas still fail verification.
2. Revalidate and reuse the complete exact-source occurrence profile. Route raw
   form/schedule/line keys, not old `11AI` dispositions. Preserve every report
   group, raw memo state, unknown amount, receipt-date diagnostic and occurrence.
3. Do not infer zero from an absent family, even with a bound zero cover and a
   complete original. Accept numeric differences only for qualified nonempty
   occurrence populations; do not relabel a loan's principal origin from its cover.
4. Keep this report-scoped. New family window/summary differences need their own
   absence/completeness gate. Real F3X and additional positive role witnesses,
   financial report selection, cash availability and terminal allocation remain
   separate. No graph, Dagster or source publication changes are authorized here.

## 2026-09-11 — Qualify reported zero without replacing absent family detail

The user accepted the absence/completeness gate before family-window totals.
The [retained gate](./audit/receipt-family-absence-2026-09-11.md) verifies an
additive Go reviewer over the existing contribution/transfer/loan subset.

1. Require an explicit bound original/metadata zero, complete original record
   census, exact full-report Schedule A line-count agreement with the verified
   profile, and no occurrence on the exact family line in either inventory.
   Memo-only, zero-valued and cancelling populations are present, not absent.
2. Keep source format, filer, period and observed-chain qualification unchanged.
   Unknown Schedule A lines, other record families, malformed records, scope
   conflicts or incomplete sources block this negative-evidence use. Non-SA
   census layouts identify record families, not their financial validity.
3. Emit a separate qualified reported-zero observation. Preserve the older
   comparison and null detail byte for byte; do not create a receipt or fill a
   detail gap. Count agreement is not transaction identity or cash proof.
4. Keep this per-report. Family-window aggregation needs field/date coverage;
   missing covers, unknown dates and unsupported categories remain explicit.
   No source publication, graph change, Dagster activation, financial eligibility
   or terminal attribution follows from this qualification.

## 2026-09-11 — Aggregate receipt families over exact reported date windows

The user accepted date-window aggregation after the reported-zero gate. The
[retained window gate](./audit/receipt-family-window-2026-09-11.md) verifies
an additive Go consumer with unchanged prior outputs.

1. Use every intersecting observed chain candidate for each exact endpoint/form
   family. Preserve missing-document references, unresolved cohorts and superseded
   alternatives. Do not choose report membership from available amounts.
2. Measure reported-field and comparison-operand coverage separately, using the
   existing calendar sweep. Require a qualified partition, every relevant
   candidate and each requested day exactly once for a full-window value.
   Cross-boundary report amounts are not clipped or prorated.
3. Accept qualified nonempty occurrence subtotals and separate qualified reported
   zeroes as distinct comparison bases. Retain per-report mismatches, including
   differences that cancel in a window. Missing detail remains null upstream.
4. Sum exact signed integer cents without int64 overflow. Partial sums describe
   their own memberships, not full-window totals or bounds; do not subtract sums
   with unequal scopes. Keep families, forms and nested totals separate.
5. Add no summary-field mappings, publication, graph/Dagster change, financial
   eligibility or terminal attribution. Exact family-summary meaning/date gates,
   wider positive source witnesses and financial funding acceptance remain open.

## 2026-09-11 — Compare receipt-family windows with exact summary fields

The user accepted the next family-summary gate. The additive
[Go consumer](./design/receipt-family-summary.md) and
[retained evidence](./audit/receipt-family-summary-2026-09-11.md) now pass.

1. Use the reviewed F3/F3X contribution, transfer and loan field mappings only.
   Preserve their distinct form-specific meanings. Candidate-guaranteed loans
   do not establish candidate-funded principal; affiliated loans can be transfers.
   Do not substitute aggregate loans, balances, repayments or another form.
2. Read new raw/typed scalars from the same reverified summary publication and
   representative fact. Preserve all original assertion IDs, members and variants;
   do not add fan-out rows or alter existing diagnostic equations.
3. Require exact declared summary and requested window boundaries, independent
   family coverage and compatible type/designation/form. Scope conflicts block
   all fields; amount conflicts block that field across every variant.
4. Compare reported and qualified detail windows separately in exact signed
   cents. A blank remains null; a narrower report cannot replace a wider summary.
   Keep the distinction between nonempty detail and qualified reported zero.
5. Preserve older outputs and independent-snapshot status. Numeric agreement
   proves neither identical report membership nor a cash/terminal funding basis.
   No graph, Dagster, publication or financial-selection boundary changes.
   Remaining positive source witnesses and financial coverage stay separate work.

## 2026-09-11 — Verify the remaining initial positive receipt-family shapes

The user accepted bounded positive source verification before expanding funding
calculations. The [source gate](./audit/positive-receipt-families-2026-09-11.md)
adds tests and captured evidence without changing the application binary.

1. Reuse retained originals and rank missing examples by saved-profile row cost.
   Four small public captures fill the remaining initial loan/party shapes; one
   existing report supplies F3 transfers and other-committee contributions.
2. Compare original covers and complete original Schedule A groups with the
   pinned processed profile. Preserve all raw groups, dates and known differences.
   This establishes bounded reported-source observations, not metadata binding,
   effective amendment selection or transaction-level identity equivalence.
3. Keep the reused report's eleven unreviewed debt records and two unknown
   processed amounts explicit. Do not relax the census guard, import debt balances
   as receipts or fill processed nulls. An EOF record without a newline remains
   exact byte evidence when the HTTP body is verified complete.
4. Positive examples cover the initial mapped form/family shapes, not all source
   populations or financial uses. The next implementation boundary is the
   remaining mapped receipt categories with their distinct itemization scopes.

## 2026-09-11 — Preserve detail relationships in remaining receipt comparisons

The user accepted the remaining categories and required general rules rather
than entity-specific exceptions. The [shared v2 comparator](./design/receipt-family-comparison.md)
and [retained gate](./audit/receipt-families-v2-2026-09-11.md) implement that boundary.

1. Map all remaining reviewed non-individual F3/F3X Schedule A leaves by exact
   form, line, period field and source-defined detail relationship. No committee,
   candidate, file, year, amount or desired-result branch chooses the rules.
2. Keep required-itemized comparisons distinct from thresholded detail components.
   Thresholded detail has no reported-total difference, even when numbers match.
   Missing detail stays null; no residual unitemized or lower-bound claim is made.
3. Bind repeated F3 offset cover positions independently and retain conflicts.
   Use exact pinned metadata names and permitted string types, not alias fallback
   or substring-based numeric coercion.
4. Share the verifier, scope binder and comparison engine. Preserve the v1 default
   and its absence/window/summary consumers; opt-in v2 is report-level evidence,
   not a silent change to existing published meanings.
5. Keep source and graph state unchanged. Real retained cases and general fixture
   counterexamples do not establish population-wide financial membership. Wider
   positive witnesses, new-family window/summary scope, F3P, cash availability and
   terminal allocation remain explicit gates.

## 2026-09-11 — Verify real remaining-family sources without bypassing header scope

The user accepted real positive witnesses before extending window totals. The
[retained gate](./audit/positive-receipt-families-v2-2026-09-11.md) now covers every
new form/category combination with originals, saved-profile groups and real metadata.

1. Prefer existing originals and rank missing captures by saved-profile row cost,
   not agreement with a desired total. Preserve separate cover/detail expectations.
   Recapture a retained body only where transport evidence is missing; verify identity.
2. Use bounded public-demo metadata captures through the existing Go boundary.
   Preserve API ancestry separately from the bulk snapshot, including explicit
   history and financial-selection limits. No private credentials were needed.
3. Share test helpers and replay older source results exactly. The application
   binary stays unchanged; matching source examples do not alter counting policy.
4. Keep the three zero-numbered original-header binding failures explicit. Their
   cover/detail/metadata amounts agree, but the current blank-only header rule
   blocks scope. Review this shared source shape before changing qualification;
   never grant per-file, committee or filing-software exemptions.
5. Keep partial-detail, debt-census, unknown-amount and financial/terminal guards
   intact. New-family window/summary totals remain a separate step.

## 2026-09-11 — Integrate reproducible evidence before selecting terminal policies

The user confirmed that reproducibility and preserved graph evidence come first;
terminal classification and dollar-allocation methods can be selected later.
The [candidate evidence view](./design/candidate-evidence-view.md) implements the
next integrated product result rather than making every financial-source audit
a prerequisite for useful observation queries.

1. Join exact source-backed receipt populations, upstream connectivity and
   optional candidate-linked summary context. Preserve source versions and local
   coverage blockers; never fill receipt detail from summaries.
2. Keep terminal classification and allocation separate and unselected. Missing
   adjacency is not a terminal definition; connected paths alone do not determine
   pooled-dollar attribution. Do not introduce proportional or other allocation
   policy merely to populate a headline total.
3. Bind each result to the executable digest, existing component policies and
   exact inputs. Reproduce both JSON and readable reports from retained evidence;
   changing a policy must not overwrite the underlying facts or older answers.
4. Keep receipts for each committee distinct. Repeated money along a path is
   not extra candidate funding. Preserve the accepted candidate-boundary accounting
   and the complete selected upstream membership without copying the whole ledger.
5. Use source investigations where a concrete financial use needs them. The
   original-header numbering question and incomplete cash basis remain open but
   do not prevent this integrated evidence result. Publication/serving, other-cycle
   rollout and unattended operation remain separate from this read-only consumer.

## 2026-09-11 — Present source names and paths without changing evidence

The accepted next step is a more inspectable
[candidate evidence report](./design/candidate-evidence-view.md), not a new money
calculation. `--view-version v2` wraps the unchanged v1 result with name assertions
and deterministic complete witness paths. The default v1 command stays compatible.

Committee names must match the trace's exact master source and fact identities.
Optional candidate-name facts are same-cycle display context only, with exact
source lineage even when releases differ. Neither a label nor a missing name
changes authorization, identity resolution or terminal status.

Path examples are selected by hop distance and committee ID, never money or a
preferred conclusion. Preserve every hop's amount and reported date; explain
date reversals and missing dates. Do not infer same-dollar provenance or compute
a path total. Keep memo populations separate in the readable report, with exact
underlying codes and all financial blockers preserved in JSON.

## 2026-09-11 — Drill into exact source rows without recalculating the report

The [connection drilldown](./design/candidate-evidence-view.md#source-row-drilldown)
reuses the existing pinned source reader. A caller supplies the exact v2 report ID
and a concrete connection ordinal; content hashes and closed JSON decoding guard
the parent snapshot. The source reader independently verifies calculation/input
identities, selected membership, every observation field and the complete row.

Keep the two verification scopes explicit. A content hash is not authenticated
authorship or a new full-report calculation. Names and candidate context remain
parent assertions; the selected source observation is checked independently.
Do not promote authorization, memo interpretation, amendment selection, cash
availability or terminal attribution merely because a row matches.

The CLI opens a reader per invocation and verifies complete source backing at
startup. Only requested rows are decoded; a long-lived consumer can reuse the
existing reader lifecycle. Do not download sources, reaggregate receipts or
introduce a new database/index just to inspect a selected connection.

## 2026-09-11 — Complete core graph connections before a GUI

The user clarified that missing donor, conduit and corporate connections are
unfinished original-product scope, not optional coverage improvements. The
[connected funding-graph plan](./design/connected-funding-graph.md) now owns the
priority and acceptance gates. The completed report/drilldown remains useful,
but does not establish a complete funding graph.

Next, publish cycle-wide 2024 reported contributor and supported conduit
connections into committee ancestry and candidate authorization. Preserve source
appearances without requiring resolved person identities. Benchmark the indexing
and graph representation; do not repeat bounded per-report reviews at cycle scale.
Then integrate typed relationship families and finish remaining A/B cycle coverage,
person/organization resolution and coordinated operational gates. Keep ledger,
identity, employment and payment meanings separate.

Python reached broader product behavior; Go has stronger verified source lineage
and replay for its implemented slices. Neither is proof of complete attribution.
Continue the Go implementation without automatically copying legacy heuristics.
Terminal classification and allocation remain independent, unselected policies;
financial blockers apply to the uses they affect, not all evidence connectivity.

## 2026-09-11 — Treat Python as research, not the rebuild's guide

The user reaffirmed that Python was an exploration. Product requirements,
reviewed source evidence and independently justified contracts guide the rebuild;
Python supplies questions, counterexamples and prior learning. Restoring its
totals, thresholds, schemas, identity rules or feature list is not the acceptance
criterion. Parity applies only to an explicitly justified `KEEP` behavior.

The [participant publication contract](./design/receipt-participant-publication.md)
therefore starts with source-backed appearances, not legacy donor hashes or a
high-dollar filter. Person/organization resolution stays additive. Report-reference
membership must include every occurrence in scope before checking uniqueness or
absence; a bounded sample cannot resolve those questions for the whole cycle.

The bounded Go sort-run benchmark passes its real source, field-mapping and replay
gates. Its measured layouts inform the external merge/join implementation; they
do not lock the full-cycle graph representation or establish global completeness.
No source selection, financial policy, graph publication or Dagster behavior
changed through this benchmark.

## 2026-09-11 — Join report references over complete cycle membership

The [cycle reference join](./design/receipt-reference-join.md) uses exact report
and transaction keys from the retained Schedule A facts. Both source scans include
all occurrences; unreferenced rows can be targets or duplicate-ID counterexamples.
A fixed-size filter only reduces candidate sorting. False positives cannot create
a match because exact keys and complete requested-key multiplicities decide it.

Share the reference classifier with the bounded reviewer. Preserve unsupported,
absent, duplicate and ambiguous references rather than repairing IDs or selecting
a preferred occurrence. Retain reverse incidence and distinct-peer evidence, but
do not treat exact references as payments, resolved donors or qualified conduits.
Conduit qualification must also consider invalid incident evidence and role rules.

Bound source readers, buffered batches, sort runs, merge fan-in and live workspace
bytes. Publish a completion manifest only after full readback and conservation.
Execution geometry stays outside logical decision identity; source, executable,
policy and decision digests remain bound. Failed attempts retain their files and
restart from immutable input. This is not yet production reuse or checkpointing.

The real complete-cycle gate and layout-varied replay must pass before accepting
this publication. Participant graph publication remains the next separate gate;
existing source pointers, financial calculations and Arango graphs stay unchanged.

## 2026-09-11 — Parallelize complete report work under shared budgets

The user requested use of available CPU capacity after the serial join became a
measured bottleneck. The [parallel reference execution](./design/receipt-reference-parallelism.md)
assigns each exact recipient/report scope to one of 1–8 workers. This keeps every
transaction-key duplicate, requested target and reverse incidence together while
reusing the accepted classifier. Hashes choose buckets; they do not resolve IDs.

Workers share one physical workspace cap and split one fixed filter budget. Bound
concurrent merge-input decoders, acknowledge borrowed source buffers before reuse,
and cancel/drain siblings on failure. Different artifact families can assemble
concurrently, while each final stream retains its deterministic global ordering.
No raw source, domain predicate, graph or Dagster change is part of this work.

Eight workers are the initial measured processing budget, not automatic use of
all host CPUs or RAM. The complete million-row synthetic pipeline improved from
23.025 to 5.368 seconds under the same 4 GiB cap, with identical canonical digests.
It excludes Parquet decoding; real report skew and source throughput still need
the full-corpus comparison. Per-partition counts and stage times make that visible.

The serial full-cycle gate and layout-varied replay are accepted and retained.
The parallel build must pass its own complete source/artifact check and canonical
evidence comparison before adoption. Its executable changes calculation identity;
filter-only extra lookup candidates can change with partitioning. Neither permits
changed source identity, reference decisions, state totals or reverse connectivity.

Acceptance update: the real parallel run, full canonical comparison and separate
source/artifact verification pass and are durably retained in the
[parallel audit](./audit/receipt-reference-parallelism-2026-09-11.md). Runtime fell
from 46m 20s to 22m 19s, but the old run already used four source readers and a
four-CPU quota. Do not describe this as one-CPU/eight-CPU scaling. The two source
passes now consume 13m 45s; profile that remaining cost under equal budgets before
selecting another concurrency change. No participant graph is published by this
acceptance.

## 2026-09-12 — Read narrow receipt columns without source-width temporary rows

The [real-source profile](./audit/receipt-source-scan-profile-2026-09-12.md) found
substantial generic Parquet row-copy work and an independently measured reader
limit. The [narrow reader](./audit/receipt-narrow-reader-2026-09-12.md) uses the
pinned library's converted column chunks directly, keeping its typed reconstruction
and exact source-type/optionality checks. Full-file hashes and the entire physical
schema remain mandatory. No retained fields or domain rules change.

Permit/default to eight source readers under the existing shared CPU, memory and
workspace budgets. Keep source batch acknowledgments and bounded report workers;
do not introduce a second dispatcher design as part of this change. The old reader
is a test-only differential oracle, not an error fallback. Every selected sample
value must match, followed by full-cycle artifact/source equivalence and resource
measurement before accepting the new build. The eight-million-row sample gate and
Go tests/race/vet pass; full-cycle acceptance is recorded in the linked audit.

The complete 2024 gate is now accepted and retained: 916.890 seconds versus
1,338.845 seconds, all four artifact hashes and reference states unchanged,
complete readback/source checks passed. Source passes fell from 825.384 to
406.483 seconds under the same eight-CPU/4 GiB budget. Return to contributor
publication; further dispatcher overlap remains a separate performance follow-up.

## 2026-09-12 — Share association semantics and publish incident-safe reference topology

The [endpoint topology consumer](./design/receipt-reference-topology.md) uses the
accepted complete-cycle reference join as immutable input. Propagate non-exact
references to their source and every matching target occurrence; exact peer
degree alone cannot establish a safe association. Recheck both reference
directions against the retained neighbor publication and keep reciprocal pairs
distinct from shared targets. No report or duplicate-key group must fit in memory.

Extract the bounded report reviewer's existing role/conflict/amount rules into
one pure Go policy before using them at cycle scale. Preserve its policy version,
wire output and source fixtures. The topology artifact carries no money or source
roles; it is a checked input for the next participant/association publisher, not
that publisher or its Arango projection.

An omitted endpoint establishes no relevant reference incident, not uniqueness of
every transaction key. Keep unrelated unreferenced duplicates unassessed unless
a later consumer obtains their complete membership. Do not turn filter-positive
lookup evidence into a claimed full transaction-ID census.

The first full topology calculation passes in 4m 5s with a roughly 280 MiB output
and 248 MiB peak RSS under the existing temporary budget. Its
[audit](./audit/receipt-reference-topology-2026-09-12.md) owns replay, corpus and
retention acceptance. Existing source facts, graph generations, monetary rules
and Dagster activation remain unchanged.

Acceptance update: the full endpoint cross-check, same-build layout-varied replay
and both retained report-policy comparisons pass. The output identity and both
artifact hashes are identical on replay. The audit and all copied files are now
durably retained and checksum-verified; participant/graph publication remains next.

## 2026-09-12 — Preserve every reported contributor appearance in a compact access index

Implement the [participant index](./design/receipt-participant-index.md) before
selecting the Arango physical layout. Every source occurrence has its own
fact-set/ordinal/role identity, including unresolved descriptions and excluded
financial populations. Retain the full original record in the existing immutable
fact set and prove that an exact appearance opens it; do not copy all raw fields
into another store or merge people by name/employer.

Reuse the existing receipt inventory and source-role policy for each occurrence.
Keep raw IDs, policy decisions, structured conduit assertions and report-reference
annotations distinct. This index neither resolves identities nor qualifies memo
references, counts another payment or selects terminal/allocation policy.

Use independent source-to-readback shard workers and the checked narrow-column
reader. Pin source/build/policy identities, conserve every row, enforce one shared
data budget plus a hard manifest-size reserve, and publish only after full shard
readback. Worker-varied replay must preserve indexed values and file identities.
No mutable publication pointer, Dagster activation or graph cutover is included.

The [participant gate](./audit/receipt-participant-index-2026-09-12.md) owns corpus
results and resource measurements. Source-role-qualified conduit associations
and typed connections into committee/candidate ancestry remain the next step;
the sparse reference topology does not prove uniqueness for omitted occurrences.

Acceptance update: the complete 2024 index conserves 264,085,606 appearances in
868,576,646 bytes. Both full publications, independent full readback, sampled
old-reader equivalence, exact artifact replay and full-fact inspection pass.
The final build enforces the reserved manifest size before publication. Its
artifacts, binaries, sources and checks are retained without changing source or
graph pointers. The linked audit owns exact identities and measurements.

## 2026-09-12 — Qualify conduit observations from complete participant and reference evidence

The [cycle-wide conduit publication](./design/receipt-conduit-publication.md)
joins the exact compact participant and reference-topology publications. Use the
existing reviewed role/conflict/amount policy unchanged. Preserve every applicable
disposition and all other source appearances; do not promote raw references into
conduit links or use matching names, amounts or ID suffixes as replacements.

An omitted sparse endpoint receives an explicit transaction-uniqueness-unassessed
state. It cannot inherit a stronger duplicate-specific or no-related-memo claim
from the bounded complete-report reviewer. Keep the original and supporting memo
as separate source facts. Qualified associations add no money, resolve no real
identity and select no terminal policy.

Use independent shard jobs and bounded concurrent merge groups over verified
external-sort streams. Enforce one shared workspace budget, a hard metadata cap,
complete source/disposition membership and full output readback. The
[2024 gate](./audit/receipt-conduit-publication-2026-09-12.md) owns exact population,
replay, performance and retention evidence. Arango integration remains a typed,
streaming import/readback step, not an in-memory expansion of this result.

Acceptance update: the complete 2024 run publishes 33,262,189 dispositions and
14,143,626 qualified reported associations. Full membership, independent corpus
checks, earlier report expectations and same-build varied-layout replay pass.
The 124,747,339-byte output, code and checks are retained and checksum-verified.
No graph or financial policy changed; streaming Arango integration is next.

## 2026-09-12 — Stream source-grain Arango samples before complete-cycle publication

Use the [bounded receipt-participant importer](./design/arango-receipt-participants.md)
for the next connected-graph boundary. Keep source appearances, reported receipt
edges and qualified non-monetary conduit associations distinct. Bind exact
participant/conduit bytes and the original source release's immutable candidate,
committee and linkage context. Reuse the existing authorization and conduit
policies; do not merge contributors by names or introduce amount thresholds.

Encode borrowed source values before handing batches to parallel workers. Bound
batch bytes/rows, worker count, queue depth, context populations and queries.
Verify every stored field plus complete submitted-key/count membership. Keep
completion last and completed replay read-only. Reuse a scoped source inspector
to verify complete backing once per invocation, retaining full source drilldown.

The [100,000/million-row gate](./audit/arango-receipt-participants-2026-09-12.md)
passes and is retained. It validates real graph connections, not full-cycle
coverage, integrated generation readiness or terminal amounts. Sample JSON byte
measurements justify testing a leaner graph layout; source and participant grain
must remain intact. The full-cycle physical layout is not locked by this sample.
Do not infer production storage or linear runtime from these measurements.

## 2026-09-12 — Keep duplicate participant fields outside compact Arango appearances

Use the [compact appearance layout](./design/arango-receipt-participants.md) for
the next full-cycle publisher. Preserve source-grain keys, exact fact-set/ordinal
locators, queryable dispositions and full conduit decisions. Receipt/conduit
edges and candidate/master context remain unchanged. Keep all participant fields
in the verified index and every original source field in immutable facts; this
is physical deduplication, not early aggregation or identity resolution.

Require each compact graph document to reconstruct its complete former appearance
using its exact verified participant row. Bind physical and expanded-source
digests separately in completion. Both payload and temporary proof share the
existing bounded worker buffers. Queries for fields removed from the graph must
follow the exact participant/fact lookup rather than rely on `participant.*`.

Version the physical definition and create a separate isolated database. Keep
the existing expanded benchmark default for compatibility. An optional pinned
v1 result adds complete live comparison without writing the old graph. It is
acceptance evidence, not a permanent input dependency: standalone compact replay
must still verify every field and source reconstruction without the v1 graph.

The [retained real gate](./audit/arango-receipt-participants-compact-2026-09-12.md)
passes both sample sizes, full live equivalence, varied-layout replay and
standalone replay. The million-row encoded payload falls 45.26%; engine figures
do not establish an equivalent disk saving. Full-cycle storage admission,
progress/retry, publication and connected-generation integration remain open.
Do not lift the one-million-row sample cap or activate weekly publication on
the strength of this layout gate alone.

## 2026-09-12 — Publish complete receipt cycles with disk guards and verified shard checkpoints

The [cycle publisher](./design/arango-receipt-participant-cycle.md) is a separate
command with no sample-range override. Derive the whole population from exact
participant/conduit manifests and retain the existing compact graph model and
source-release context. Use a separate versioned, isolated database; do not
promote a bounded sample by relabeling its completion.

Checkpoint only at complete source-shard boundaries after durable batch imports,
all-field readback and full source reconstruction. Keep the prefix's canonical
digests and counts independent of worker/batch layout. Resume must regenerate
and verify completed evidence without rewriting it, then retry only unfinished
work. Accept repeated prefix verification as the cost of failing closed on
changed database state; this first resume design does not promise fast skipping.

Require an operator-verified read-only server data mount, an explicit free-space
reserve, a net filesystem-growth allowance and a complete encoded-payload cap.
Persist the first filesystem baseline across retries. Check capacity before
writes, not only at startup. Shared filesystem growth is not exact per-database
usage; these guards are circuit breakers, not a disk quota or reservation.

Write graph completion only after complete source, membership, reconstruction,
count and query gates, then publish a non-overwriting local manifest after
completion readback. In-progress checkpoints are not publication success. Keep
existing graphs, source artifacts and current pointers unchanged. No Dagster
activation, terminal policy or A/B/E integration is implied.

Go, race and static gates pass. The actual full-cycle interruption preserves the
first two verified shards without publishing completion. The same build and
identity are now being resumed with a different worker/batch layout; the
[dated audit](./audit/arango-receipt-participant-cycle-2026-09-12.md) owns the live
acceptance state and retained evidence. Complete-cycle readback/replay and
connected-generation integration remain open until their gates pass.

## 2026-09-12 — Connect existing graph families through exact read-only evidence

The [receipt-to-candidate consumer](./design/receipt-candidate-connection.md)
joins a pinned completed receipt graph to the existing selected receiver graph
and candidate authorization. Do not copy the receipt corpus into another database
to establish this connection. Reuse the existing upstream witness and source-byte
compatibility rules; a newer coordinated release may select unchanged older
facts, but matching cycle names alone never establish compatibility.

Require exact shared fact/master/linkage identities, manifest bytes, completed
graph evidence and selected-document readback. Keep receipt, conduit, committee
observation and authorization types distinct. A path is not a payment sum or
terminal allocation. Preserve missing-route and unresolved-identity states.
The generic Go consumer derives cycle from its manifest and pins its own build
separately from the publisher. Fixture/race checks pass; real cross-graph replay
and population-wide integration remain acceptance work, not implied completion.

## 2026-09-12 — Automate graph acceptance rather than hand-select investigations

Use [Go connection validation](./design/receipt-candidate-connection.md) to choose
test scope and witnesses from pinned publications. No named candidate, donor,
cycle or source ordinal belongs in a validation rule. Require a complete conserving
input census, exact selected-source/document readback, deterministic replay and
explicit absent-case states. A single JSON result and nonzero failure exit must
support future machine orchestration without an agent interpreting examples.

Keep the publisher build and consumer build distinct and preserve failed attempts.
Reuse exact retained source arguments rather than transcribing long identities.
No embedding or LLM is needed for source identity, graph connectivity or this gate.
Models are not a substitute for missing evidence or a default identity-merging rule.

This command's success is narrower than unattended production: it verifies one
data-selected candidate scope against complete index membership, not all candidate
paths, terminal amounts or integrated A/B/E coverage. Weekly orchestration remains
gated by generation readiness, other-cycle verification, invalidation and retention.

## 2026-09-13 — Prove reference content without replacing acquisition ancestry

Use an explicit [CM/CCL content-equivalence proof](./design/reference-content-equivalence.md)
when existing graph families retain different archive identities. Verify complete
ZIPs, selected members, supported schemas and every fact through normalization
replay. Preserve both original release/archive/occurrence/fact-set identities.
Matching content does not make those provenance IDs interchangeable.

Bind the proof to the exact flow bundle through a freshly verified in-process
context; never accept a serialized approval flag or silently weaken the ordinary
exact-source loader. Reuse the receipt graph's original reference facts through
that context, with proofs included in downstream calculation identity. No new
receipt import, reference relabeling, canonical-person inference or money rule
is needed for archive-only repackaging.

Resource ceilings fail closed. Actual content/schema drift still blocks reuse.
The reference proofs, full 2024 index census, selected cross-graph witnesses and
byte-identical fresh replay pass on retained inputs; the linked live audit owns
that scoped acceptance. This is not every candidate/path or an integrated A/B/E
generation. Weekly activation remains separate work.

## 2026-09-13 — Bind typed graph families without merging their ledgers

The [generation verifier](./design/funding-evidence-generation.md) requires the
completed receipt, selected A/B and resolved Schedule E projections. Bind exact
source/calculation/graph identities under the A/B source-release target; preserve
original publication ancestry when unchanged sources are reused. Prove exact
Schedule E source-version membership and complete CN/CM/CCL reference content.
Actual source or reference changes fail this reuse boundary.

Declare typed relationship families, source grain, overlap and endpoint namespaces.
Receipt and receiver Schedule A views overlap; reconciliation candidates, conduit
associations and authorization are not extra payment ledgers. Keep outside support,
opposition and unprojectable decisions separate. Do not emit a combined total.

Use read-only readers. The receipt reader verifies its published completion and
live counts; A/B and resolved E readers also verify all selected model fields.
The generation ID includes exact inputs, methods and consumer executable identity.
No importer, latest-pointer fallback or serialized approval flag participates.
CN proof has a separate policy ID, preserving existing CM/CCL proof identities.

The retained 2024 generation and byte-identical fresh replay pass. This accepts
the declared-family binding, not every candidate path, a serving API, terminal
allocation or unattended publication. Generation-bound typed queries are next.

## 2026-09-13 — Read typed neighborhoods through the pinned generation

The [neighborhood reader](./design/funding-neighborhoods.md) consumes an exact
generation file and byte checksum. Derive immutable source/bundle paths from its
identities; accept physical receipt locators only when their backing matches.
Reconstruct and compare the complete generation before exposing its readers.
Keep its original producer identity and record the current consumer build
separately. Reverification does not claim execution of the old binary.

Expose bounded one-hop family pages and source-backed entity facets. Preserve
available-empty, absent, missing-master, inapplicable and unrequested states.
Keep receipt/receiver overlap, sender observations, reconciliation context,
authorization and outside stances separate; add no combined amount field.
Label source-occurrence, linkage-membership and resolved-group evidence by their
actual grain. Group references are not full individual-source membership.

Use deterministic family ordering and generation/entity/family-scoped continuation
tokens. Recheck all completion boundaries before returning. This relies on
immutable publications, not a cross-database transaction. Output bounds do not
bound backing-verification or adjacency scan cost; serving performance requires
its own gate. Automated acceptance selects evidence witnesses from the data
and supports exact fresh-process replay, not operator-curated cases.

No person/corporation resolution, terminal policy, graph import, GUI or weekly
activation is introduced. Typed multi-hop access is the next query boundary.

The retained 2024 query gate and byte-identical fresh-process replay pass for
all declared family witnesses, cross-family endpoints and a missing-master case.
This accepts one-hop evidence access, not all-path or serving readiness.

## 2026-09-13 — Keep multi-hop evidence paths distinct from terminal rules

The [typed path reader](./design/funding-paths.md) extends exact-generation access
with an optional source-occurrence receipt/conduit entry, one selected A/B
committee ledger and an explicit authorized/support/opposition candidate ending.
Preserve edge direction, parallel occurrences and source evidence. Do not mix
receiver and sender observations within the chain, traverse reconciliation
components as payments or infer that the same dollars followed a connected path.

Search deterministic simple committee paths over the generation opener's fully
verified selected topology. Bound committee hops, returned paths and examined
links. Expose hop frontiers, skipped cycle-closing edges and unknown results from
budget exhaustion; these are search states, not source classifications. Reject
an empty committee self-path rather than manufacturing evidence from a typed ID.
No new graph import or AQL serving index is required for this reader.

The connected evidence will inform later terminal-source definitions. Preserve
the distinction between observed endpoints, identity/source gaps, time coverage
and cyclic components while evaluating those definitions. Do not bake traversal
limits into terminal status. Keep terminal classification and subsequent dollar
allocation as separate, explicit versioned decisions. Neither is selected here.

The guarded final reader passes all ten data-selected 2024 cases and
byte-identical fresh-process replay. Both ledger/candidate-ending combinations,
source entries and explicit hop/work boundaries are exercised. This does not
complete all-entity coverage, terminal classification or serving readiness.

## 2026-09-13 — Compare terminal hypotheses without classifying financial origins

The [terminal-source assessment](./design/terminal-source-assessment.md) compares
selected-inbound frontiers, same-cycle-master frontiers and root SCC membership.
These are provisional topology predicates, not adopted terminal policies.
Assess the entire endpoint union of the generation's selected A/B committee
ledgers. Preserve each ledger's source membership; never sum them as money or
interpret absence from one as a zero-inbound frontier.

Use full iterative SCC analysis rather than bounded path-query stopping states.
Share the existing candidate-upstream SCC engine. Retain every node's outcome,
component membership, identity evidence and explicit origin blockers. Validate
observation/endpoint conservation and preserve signed, zero and parallel source
observations without a new amount filter. Cyclic roots are component boundaries,
not individual financial origins. Same-cycle master presence does not establish
activity, person/corporation identity or terminal status.

Pin the generation and consumer executable; select illustrative source witnesses
from the verified data and support exact fresh-process replay. The selected
topology, not a witness edge, establishes the frontier and component census.
Do not add a graph import, scheduled service or LLM dependency for this calculation.
Reported participant roles and separately evidenced identity associations are the
next inputs. Choosing a terminal policy and allocating dollars remain separate
contracts; this assessment emits no allocation and promotes no eligibility.

The retained 2024 assessment passes both complete selected populations, automatic
source witnesses and byte-identical fresh replay. This accepts the comparison
method only; none of its hypotheses becomes an adopted terminal-source rule.

## 2026-09-13 — Join reported roles without merging source appearances

The [receipt-role profiler](./design/terminal-receipt-roles.md) joins the complete
compact participant index to every committee in the selected A/B boundary scope.
Keep the source-role policy unchanged. Partition all occurrences into scoped,
outside-scope and unresolved recipients; retain memo, conflict, overlap, null and
amount-sign evidence. Derived profiles do not replace the original source grain.

Join routed committee IDs only to exact facts in the full pinned same-cycle
master input. Do not promote raw IDs, entity labels, names, employer text or
reported organization attributes into resolved identities or corporate payments.
Keep historical registration and person/corporation resolution separate.

Use bounded parallel compact-shard readers with worker-local profile maps,
owned retained strings, whole-file/canonical readback and complete census checks.
Exceeding the shared state cap fails without truncation. Worker count does not
enter semantic identity; fresh worker-varied replay must reproduce every byte.
Select source witnesses automatically and verify complete source rows and their
receipt edges. All three graph completion boundaries remain required.

The same Schedule A role profile informs both topology views; it is not a second
Schedule B receipt ledger and cannot be added twice. Annotation distributions are
separate marginals, not joint predicates. No terminal policy, dollar allocation,
source fetch, graph import, new LLM dependency or weekly activation is introduced.
Source-grain employer/organization assertions and separately evidenced identity
resolution are the next implementation boundary.

The retained 2024 gate passes the full participant corpus, automatic source
witnesses and byte-identical fresh replay with eight and four readers. This
accepts the reported-role/identity evidence join, not person/corporation
resolution, a terminal definition or dollar attribution.

## 2026-09-13 — Preserve reported identity text and require an interpretation review

Expose source-grain receipt names, addresses, employer/occupation and receipt
date fields, plus reported committee organization fields, through a
[verified source-backed view](./design/reported-identity-assertions.md). Reuse
the existing columnar/source artifacts rather than copying the text corpus.
Preserve nulls, blanks, whitespace, unknown values and separate appearances.
Keep raw fields distinct from publisher-cleaned IDs and natural text distinct
from search vectors. Do not infer employment intervals from receipt dates.

Every receipt assertion remains tied to one exact source ordinal/fact set.
Every committee assertion remains tied to one published fact and occurrence;
the existing classic fact-selection boundary is explicit, not silently described
as one-to-one with all original occurrences. Pin the two independent source
histories and verify complete field/count conservation with bounded parallel
reads and worker-independent replay. Sharing a cycle does not reconcile them.

This adds no inferred identity, employment, ownership, corporate-payment or
terminal link. A source-backed view is neither a standalone backup nor a
name-search index. Source retention remains required.

The user requires an [interpretation review and reproducible evidence checkpoint](./design/pre-attribution-review.md)
before choosing terminal definitions or dollar-allocation methods. Review all
non-literal transformations, exclusions and linkage policies, not merely new
identity work. Pin source, occurrence, fact, calculation and graph boundaries;
Arango alone is not the raw evidence. The review remains open and must be
accepted by the user; implementation or a passing technical gate cannot accept
that review on their behalf.

The retained 2024 view now passes complete source verification, exact
worker-varied fresh replay, independent first/middle/last full-width reader
comparison and regression/race/static checks. This accepts field preservation
and the source-backed view only; the interpretation review remains open.

## 2026-09-13 — Cycle partitions are reusable inputs, not universal calculation boundaries

Clarify [PD-002](./design/investigative-questions.md#pd-002--reuse-cycle-partitions-declare-calculation-windows)
and the [time contract](./design/product-contract.md#time-versions-and-change):
retain independently reproducible cycle publications, but remove the blanket
requirement to calculate every cycle independently before any combined result.
The existing fine-grained evidence model already supports preserving inputs for
later regrouping; the old cycle wording was too restrictive, not a reason to
rebuild the source data.

The [cycle/window contract](./design/cycle-calculation-windows.md) separates output
period from required evidence partitions. Compatible additive subtotals may be
summed only with disjoint accepted membership and consistent definitions/units.
Distinct identities, ratios, rankings, graph analytics and temporal attribution
need their own composition rules and sufficient detailed inputs. Balances and
overlapping ledgers cannot be summed as independent period flows.

Pin source, identity and method versions across the declared input window.
Earlier evidence can affect a later-period result through explicit dependencies,
never by silently rewriting prior results or applying current relationships
throughout history. Missing prior context remains a coverage boundary, not a
terminal-source classification. No allocation method is adopted here.

Continue 2024 evidence/identity groundwork without requiring every target A/B
cycle first. Multi-cycle consumers get bounded fixtures and then real-input gates
when needed; the full four-cycle rollout remains a production-coverage milestone.
The user-requested interpretation review and evidence checkpoint still precede
terminal definitions and allocation. Existing cycle-checking readers, calculation
versions, source publications and graph data are unchanged by this clarification.

## 2026-09-13 — Test cross-partition composition before extending the application

The user requested a [bounded test](./design/cycle-calculation-windows.md#bounded-composition-test)
before implementing partition-independent application queries. Synthetic fixtures
now exercise the actual stored observation/master models and existing path-search
code: detailed evidence from two cycles creates routes absent from either cycle
alone, with separate occurrences, historical assertions, dates and source references.
Other-year fixtures, input-order replay and duplicate rejection pass. Existing
single-cycle compatibility checks remain intact.

The experiment's date selector and provenance routing are test scaffolding, not
production query behavior. Reverse-date observations can form a topology path
without establishing chronological funding. A time-filter boundary cannot define
a terminal source, and a cycle-specific master cannot supply invented day-level
relationship validity. No new identity or dollar-allocation rule was adopted.

Package, race and static checks pass. This supports extending a separate verified
multi-publication consumer; it is not acceptance of live cross-cycle integration,
non-FEC adapters, temporal attribution or a new physical graph layout. No source
data, production Go code, Arango graph or Dagster activation was changed.

## 2026-09-13 — Add verified publication-independent committee date windows

Implement the first [multi-publication reader](./design/funding-window-reader.md)
over selected Schedule A or Schedule B committee observations. Open each pinned
generation through its existing verification boundary. Preserve source-cycle
fields, original graph keys, historical master facets and source-qualified
drilldown. Exact FEC committee IDs connect supplied inputs without combining
their source profiles. Reject repeated fact sets and multiple versions of one
source cycle to prevent accidental snapshot overlap.

Inclusive reported calendar dates select observations independently of source
partitions. A bounded query excludes unknown dates with explicit conserved counts;
an unfiltered query includes all supplied observations, not all history. Do not
fill dates from cycle labels or infer chronological funding, relationship validity,
terminal identity or dollar allocation from a path. No date policy modifies raw
or normalized evidence. Keep the two reported ledgers separate.

Operational input/observation caps bound the first in-memory implementation; they
are not a four-cycle policy. Real two-cycle capacity and general cross-source
semantics remain separate gates. The [2024 gate](./audit/funding-window-2026-09-13.md)
passes complete selected-ledger date counts, automatic query witnesses and exact
fresh-process replay. Synthetic fixtures cover two-cycle composition and unknown
dates; they do not establish real multi-cycle acceptance.

Full regression, targeted race and static checks pass. This adds a Go reader and
CLI, not a graph import, source download, Dagster activation or a serving API.
Extend receipt/candidate connections next while preserving source intervals and
unknown validity. The user-requested interpretation review remains required before
terminal definitions and allocation.

## 2026-09-13 — Exercise publication composition through the complete loader

Close the review's stub-only test gap with a
[synthetic two-publication integration fixture](./design/funding-window-reader.md#verification-and-next-boundary).
Use the normal fact/calculation/graph publishers and the public window loader,
with real ArangoDB in a disposable isolated Compose stack. Do not add a runtime
verification bypass or rewrite retained real data into a fake second cycle.
Synthetic acquisition metadata and fixed external Schedule B extraction output
are explicit test boundaries; downstream source, Parquet and graph checks remain.

`make test-window-integration` covers both ledgers, input-order replay, source
routing, historical facets, dates, CLI replay and failure of the second input's
source/graph/completion checks. It retains success/failure logs and tears down
only its unique test project. Race, full regression and static checks pass.
This changes test infrastructure, not production data rules or source coverage.
Real multi-cycle capacity and corpus acceptance remain separate gates.

## 2026-09-13 — Keep dated receipts and undated authorization context distinct

Add the [window connection interface](./design/funding-window-reader.md#receipt-and-candidate-connections)
without changing the existing committee-only command. Require an exact supplied
generation ID and source ordinal for receipt starts. Filter the underlying receipt
date and one selected A/B ledger; preserve excluded entry evidence and unknown dates.
Do not derive observation dates or relationship validity from cycle labels.

Keep candidate authorization as publication-qualified context with unknown
day-level validity. Its graph key identifies endpoint pairs, so different source
publications can share that key. Qualify returned connection link IDs by generation
and retain the original graph topology and source membership separately. Multiple
authorization assertions are evidence variants, not additional payments.

Do not expose grouped Schedule E support/opposition amounts as date-filtered
endings until dated source membership is defined. The generation-bound interface
remains unchanged. No terminal definition, identity resolution, allocation, graph
import or source acquisition is added. Full regression, targeted race/static
checks and real-loader synthetic two-publication integration/CLI replay pass.
The new interface's retained real-source gate and real second-cycle acceptance
remain distinct follow-ups.

## 2026-09-13 — Accept retained 2024 date-window connection evidence

The [window connection gate](./audit/funding-window-connections-2026-09-13.md)
now passes automatic receipt, committee/candidate and conduit cases with exact
source routing, independent complete selected-ledger date counts, unknown
authorization validity and byte-identical fresh-process replay. Full regression,
targeted race/static checks and independent artifact/source-snapshot readback pass.

Retain the executable, source snapshot, inputs, runner, results and success markers.
Select witnesses in Go from pinned evidence rather than coding identities or
ordinals into the gate. This adds acceptance tests, not production classification,
amount changes, data loading or graph writes. CLI equivalence remains covered by
the separate full-chain synthetic fixture; this live gate binds the test executable.

Continue with date-selected Schedule E source membership before exposing arbitrary
date-window spending endings. Real second-cycle acceptance, source identity
resolution and the user-requested pre-attribution review remain separate gates.

## 2026-09-14 — Select Schedule E dates at source-member grain

Extend the [window connection reader](./design/funding-window-reader.md#source-grain-schedule-e-connections)
with source-grain support/opposition endings. Require the caller to select
expenditure or dissemination date, even for an unbounded query. Never fill a
missing date from another field or a cycle label. Keep the receipt/authorization
v1 contract and the existing generation-bound aggregate APIs unchanged; spending
queries use a separate v2 response policy.

Reuse the existing effective-record and candidate-resolution implementation.
Replay every published decision against the pinned source facts, retain policy
exceptions, and reconcile source members to every stored aggregate's count,
signed amount, sign counts and resolution breakdown. Return the selected source
fact and resolution decision with the unchanged aggregate as parent evidence.
These are read-time source-member relationships, not new physical graph edges.

Batch selected source drilldown and keep full facts only for returned witnesses.
Expose source/date/stance/decision coverage over all supplied Schedule E facts,
including unprojectable records, rather than treating a missing path as complete
coverage. Preserve both native dates and unknown dates. Do not change attribution,
financial eligibility, terminal policy, source acquisition or existing graphs.

Synthetic two-publication full-chain parsing, publication, Arango readback,
window selection and CLI replay pass with race detection. The retained 2024
source census, automatic witnesses, fresh-process replay and runtime/memory
acceptance remain the next gate. A second real cycle remains separate work.

## 2026-09-14 — Accept the retained Schedule E source-member window gate

The [retained 2024 spending gate](./audit/funding-window-spending-2026-09-14.md)
passes complete independent source/date coverage, automatic source-member and
upstream/receipt witnesses, unchanged parent evidence and byte-identical
fresh-process replay. Full regression, targeted race checks, static analysis and
independent artifact/source-snapshot verification also pass.

Retain the executable, source snapshot, exact inputs, scripts, outputs, metrics
and success markers together. Witness selection stays in Go and depends on
source shape, not hardcoded real identities. This accepts the existing reader
against retained evidence; it introduces no source rule, identity inference,
terminal policy, amount change, data download or graph import.

Measured full-verification queries are not an interactive-serving latency claim.
The capped run also reached memory-reclaim pressure without OOM; no larger-window
headroom is established. Record optimization as a separate measured follow-up.
Keep real second-cycle acceptance separate and prepare the user-requested
interpretation review and reproducible baseline before choosing terminal or
allocation rules.

## 2026-09-14 — Prepare the interpretation review without accepting a recovery baseline

The [first code-backed review packet](./audit/pre-attribution-review-2026-09-14.md)
records the current 2024 generation's selections, routing, candidate matching,
reference associations, graph/time semantics and explicit unresolved populations.
Added resolver tests characterize normalized-name collisions, reported-ID
precedence and replacement of a present ID without changing runtime behavior.
The review is not user acceptance of these interpretations or terminal rules.

The [checkpoint proposal](./audit/pre-attribution-checkpoint-2026-09-14.md)
pins selected manifests, source/build snapshots and replay results. All listed
checksums and retained generation/window success evidence pass fresh read-only
verification. The exact historical A/B/E raw archives are present by file-stat
checks; their large bodies were not rehashed by this review.

Keep the distinction explicit: these pins preserve the subject of a review,
not a complete recovery dependency closure or enforced retention. Complete
typed dependency inventory, build/runtime requirements and a scoped isolated
rebuild remain work before claiming raw-to-graph recovery. No current pointer,
graph, financial membership, source artifact or orchestration setting changed.

## 2026-09-14 — Explain shared-reference exclusions before expanding associations

Keep the detailed A/B/E evidence and focus on a concrete graph-coverage boundary.
The [shared-reference profile](./design/shared-reference-profile.md) reuses the
complete compact participant/topology join, with role inspection separate from
association qualification. No candidate, committee, source ordinal, amount
threshold or transaction-name exception selects a link or witness.

The [retained 2024 result](./audit/shared-reference-profile-2026-09-14.md) conserves
the entire rejected population and reproduces the accepted association artifact
byte-for-byte. It distinguishes complete peer coverage, uncharacterized peers,
role/ID conflicts and amount comparisons. Role compatibility and sum equality
are observations, not new links or inferred payments.

Use automatic original-source witnesses to specify any broader group rule;
retain unsupported shapes and version the eventual association publication.
This diagnostic does not change Arango, Dagster, financial selection, terminal
attribution or the pending raw-to-graph recovery checkpoint.

## 2026-09-14 — Add a separate complete shared-memo association rule

The [automatic original-source review](./audit/shared-reference-source-review-2026-09-14.md)
confirms a supported one-to-many reporting shape and counterexamples that must
remain explicit. Implement the [complete shared-memo group rule](./design/shared-reference-group-rule.md)
as a separate policy, without changing the accepted one-to-one publication.

Require complete exact-peer membership, safe endpoints, sole-peer originals and
compatible source roles/IDs across the whole group. Retain unresolved partial,
mixed, conflicting and non-leaf groups. This is a limited group assertion, not a
claim that unsupported occurrences are invalid or unrelated. Amount equality,
sign, dates, memo phrases, names and known IDs are not qualification gates.

Keep full source fields and dates. Group annotations add no money, determine no
terminal source and do not resolve people or legal organizations. The review
is bounded diagnostic work; the new pure rule has group-size-independent state.
Complete streaming publication, old/new conservation and a separate graph
generation remain the next implementation boundary.

## 2026-09-14 — Publish shared-conduit evidence as an additive graph extension

The [complete group publication](./design/shared-reference-group-rule.md) must
reproduce the accepted one-to-one baseline and retain all unchanged decisions.
Only complete, safe, compatible shared groups receive the separately versioned
association state. Missing peers remain uncharacterized, not implicitly safe.

Use a [small isolated graph extension](./design/shared-conduit-generation.md)
instead of copying the full receipt graph. Store exact source-occurrence
references, new non-monetary conduit edges and source-backed committee context.
Bind it to the existing A/B/E generation by exact manifest identities. Keep old
graphs and old generation readers unchanged; extension-aware query support is
explicit follow-up work. No current pointer or Dagster schedule is advanced.

Require complete old/new decision comparison, group member conservation, exact
prior appearance checks, full new-document readback, persistent storage limits
and read-only replay of completed evidence. This does not select effective
money, resolve identities or determine terminal-source dollars.

## 2026-09-14 — Keep shared-conduit query provenance explicit

The [path/neighborhood consumer](./design/shared-conduit-generation.md#consumer-boundary)
uses a separate `shared_conduit_association` family, not a merged base collection
or an overwritten old disposition. Require the outer generation, exact original
generation file and exact extension graph/calculation locators. Original-generation
queries retain their old meaning. Bind cursors and result identities to the outer
generation and keep the extension's physical provenance in each family descriptor.

Return original/new decisions, complete-group evidence and both source occurrences.
Verify complete compact membership and expected publication hashes on open, then
source-check selected live fields and lookahead on query. Do not describe that
as another full live-graph field scan. Leave date-window integration separate;
no money selection, identity resolution, terminal policy or current pointer changes.

## 2026-09-14 — Bind shared-conduit windows without changing source time

The [window extension](./design/funding-window-reader.md#shared-conduit-generation-inputs)
requires an explicit v2 input specification with the exact original generation,
extension graph and extension calculation locators. Base-only v1 inputs remain
unchanged. Reuse the source-backed query opener; reject overlapping base/extended
inputs during metadata admission. Qualify entries, links, coverage and facets by
the outer generation while preserving unchanged base metadata and physical ancestry.

Use the original receipt's nullable typed date, not its related memo's date, a
cycle label or a publication timestamp. Retain unknown dates in unbounded queries
and expose their exclusion in bounded queries. Source-check excluded entries too.
Keep the prior/new decisions, both original rows and the separate extension facet;
do not turn the association into additional money or alter the original family.

Two-cycle integration and the [retained 2024 gate](./audit/shared-conduit-windows-2026-09-14.md)
pass, including byte-identical fresh replay. Real multi-cycle capacity, low-latency
serving and production build/recovery guarantees remain separate acceptance work.

## 2026-09-14 — Pin new Go executable builds independently of historical generations

Use the [Go release build contract](./go-build.md) for new accepted executable
identities: digest-pinned Linux/amd64 compiler, CGO disabled, explicit architecture,
clean environment, trimmed paths, no VCS stamping and no implicit PGO. The release
runner and both Docker Go stages call one compiler script. Development/race-test
executables are not release identities.

Require two offline builds with independent caches and different source paths,
locked verified modules, full source tests, exact binary comparison and explicit
success markers. Retain source/module archives and build evidence. The
[accepted gate](./audit/go-build-2026-09-14.md) also reconstructs from only those
archives and the pinned image, preserving identical executable and archive bytes.

Do not rewrite earlier build hashes or republish graph/calculation generations.
Compiler-image retention, complete historical producer/runtime dependencies,
source-data closure and a scoped raw-to-graph rebuild remain recovery work.
No current pointer, data selection, graph or service deployment changes here.

## 2026-09-15 — Inventory typed recovery dependencies without declaring restoration

Use the [read-only Go inspector](./design/funding-recovery-inventory.md) to follow
explicit manifest types from a pinned funding generation. Resolve storage roots
from publisher contracts; require exact locators where publications do not own
a content-addressed layout. Do not search arbitrary JSON, current pointers or
directories to guess dependencies. Reject unsupported versions, conflicting
identities and unsafe paths.

Separate fresh manifest hashes, artifact presence/size, optional complete blob
hashes and prior publisher attestations. Missing backing produces a partial JSON
inventory and nonzero exit. Replay agreement does not clear a missing-input state.
Build/runtime contracts and live graphs remain unverified requirements; no
inventory result alone establishes recovery readiness or enforced retention.

The [first retained inventory](./audit/funding-recovery-inventory-2026-09-15.md)
locates the declared data artifacts but cannot open one exact historical staging
manifest. Preserve that gap; a run-named file with different bytes cannot replace
the release pin. No source download, graph write or destructive cleanup follows.

## 2026-09-15 — Compare reconstructed stage metadata without rewriting history

The [historical-stage review](./audit/release-stage-evidence-review-2026-09-15.md)
identifies a documented manual-output overwrite. An independently regenerated
record remains separately pinned. The new bounded Go review checks the exact
release/plan/acquisition chain, complete source descriptors and every staged-output
field; it never aliases a different byte digest to the release's original pin.

Record descriptor agreement and exact-byte identity as separate results. The
review cannot infer unavailable original execution times, disk observations or
checks, and does not verify source bodies or graph contents. Keep the strict
inventory incomplete and preserve its original result. A future recovery gate
must declare its starting layer and retained runtime inputs; choosing or accepting
that gate is not part of a passing metadata comparison.

## 2026-09-15 — Define fact-start recovery separately from historical byte recovery

The [next checkpoint](./design/funding-recovery-checkpoint.md) starts from pinned,
fully verified normalized facts and every source input required by the existing
validators. Recompute downstream evidence and build empty isolated graphs through
the normal Go publishers. Old outputs are comparison evidence, never rebuild seeds.
The first real subject remains the pinned 2024 shared-conduit generation; the
recipe must derive its scope rather than hardcode that cycle or any entity.

Keep execution inputs, comparison evidence and historical provenance explicit.
The strict inventory remains incomplete for the original missing stage record.
A new fact-start recovery can establish its own narrower claim without recovering
that record or proving raw parsing. Raw-to-fact recovery and user interpretation
acceptance remain separate requirements before a complete baseline claim.

Require exact retained input bytes, explicit per-schema logical comparison for
new derived identities, protected dependencies, retained offline runtimes and an
admitted isolated rebuild. No blanket ignored fields or missing-input waivers.
Implement the metadata-only recipe planner first. This entry records the chosen
boundary and acceptance requirements, not a completed recovery, new retention
enforcement, data copy, source fetch, graph write or production activation.

## 2026-09-15 — Plan fact-start reconstruction without reusing historical outputs

The [Go recovery planner](./design/funding-recovery-checkpoint.md#implemented-metadata-planner)
reuses the typed inspector's exact metadata bytes and preserves its full inventory.
Explicit publisher adapters distinguish retained execution inputs, rebuilt outputs
and historical-only provenance. Unknown dependency roles, unsupported producers,
ambiguous selections and missing required backing block the dependency plan.
Classic reference-proof archives/members are selected from exact source metadata;
no cycle, entity or digest exception is introduced.

Keep graph construction explicit when old projection identities are embedded in
generation metadata. Pin expected policy/output metadata, but do not treat it as
an executable input or replace normal producer validation with the planner.
Actual runtime/build bindings, input hashes, cleanup protection, workspace limits,
isolated execution and per-schema comparison remain execution blockers. The Go
command returns nonzero for a blocked plan even when deterministic replay passes.

The [retained-generation gate](./audit/funding-recovery-plan-2026-09-15.md) accounts
for the selected dependency layout and preserves the missing historical stage
record without a waiver. Full source tests, targeted race/static checks, two
offline builds, real planner replay and independent inventory comparison pass.
Only new build/audit evidence was retained; source data, graphs, current pointers,
financial/identity rules and terminal policy remain unchanged.

## 2026-09-15 — Verify selected recovery bytes without widening the recovery claim

The [scoped file verifier](./design/funding-recovery-checkpoint.md#implemented-scoped-file-verification)
derives its execution/comparison closure from a fresh typed plan, not a saved
recipe supplied as authority. Require complete dependency planning, exact byte
pins, a positive total-byte ceiling and bounded worker count before body hashing.
Verify expected comparison artifacts as well as execution inputs; old passing
checks cannot substitute for their content hashes.

Hash distinct physical paths even when digests match, reject unsafe/changed files,
and make output independent of worker completion order. Keep the historical
inventory unchanged. A successful command establishes selected-file byte
integrity, not normal source decoding, permanent retention or reconstruction.

The [real gate](./audit/funding-recovery-files-2026-09-15.md) passes all selected
files, exact fresh replay, offline builds and live over-budget rejection.
Current Go cleanup targets temporary/workspace files, not published generations;
permanent checkpoint protection needs an enforced storage/cleanup boundary.
Do not label a checksum inventory or an unused cleanup guard as that protection.
Runtime retention, restricted reconstruction and full comparisons remain open.

## 2026-09-15 — Protect recovery inputs through independent copies and restricted consumers

The [input retention store](./design/funding-recovery-retention.md) creates new
independent copies from a freshly derived plan. Execution and comparison files
live in separate trees; their original identities are preserved. Require explicit
copy/free-space bounds, full source hashing and destination readback, and a pinned
closed seal before reporting a completed capture. Existing targets are never
overwritten or silently resumed.

Supported consumers mount sealed inputs read-only. Capture cleanup can remove
only owned scratch after validating the exact seal and owner; missing or corrupt
metadata fails closed. There is no sealed-checkpoint deletion operation or claim
of protection against administrator access, writable out-of-contract mounts or
hardware loss. This is an enforced storage boundary, not just a checksum list.

The [fixture gate](./audit/funding-recovery-retention-2026-09-15.md) proves independent
copies, kernel-enforced mutation rejection and guarded cleanup. It also verifies
that test resources are gone, rather than trusting Compose's teardown exit alone.
Full source/race/static checks, reproducible builds and unchanged real-file
verification pass. No real generation is captured yet; runtime retention,
production bindings and the isolated publisher/graph rebuild remain open.

## 2026-09-15 — Return to graph assumptions; defer the recovery expansion

The user clarified that the requested checkpoint concerned assumptions that
determine graph nodes, edges and counted amounts. The assistant expanded that
into a recovery project. Correct the active scope: the
[interpretation review](./design/pre-attribution-review.md) comes next; an isolated
fact-start rebuild is not a prerequisite. This supersedes the earlier entries'
sequencing of recovery as the next milestone, not their historical test results.

Review actual candidate-reference precedence, selected committee-flow membership,
pair/shared conduit associations, authorization states and A/B candidate matching.
The refreshed map includes the shared extension omitted from the first packet.
Keep source assertions, calculation selections and identity/allocation inferences
distinct. No graph or financial rule is changed or accepted by this scope correction.

Keep immutable data, graph generations, historical audit evidence, reproducible
builds and useful read-only verification tools. Defer further recovery/runtime
engineering. The unused input-copying command and its dedicated fixture harness
have no domain or Dagster consumers; their removal is proposed separately for
user confirmation. Do not delete data or weaken source-integrity checks under
the heading of simplification. The exact historical staging gap remains recorded.

## 2026-09-15 — Remove the unused recovery-copy feature after approval

The user approved the scoped cleanup. Remove `capture-funding-recovery-inputs`,
its capture/seal/scratch-cleanup implementation, dedicated tests and Compose runner,
and its CLI/Make/release-source bindings. Simplify the surviving input reader and
file verifier by removing the helper paths used only for copying. Preserve their
strict decoding, exact hashes, path confinement and concurrent-change checks.

All nine removed source files matched the existing archive before deletion; the
[historical design](./design/funding-recovery-retention.md#retained-source-and-verification)
records its exact identity and location. No data, graphs, published results, archives
or audit records were deleted. The read-only inventory/planner/verifier and
reproducible build tooling remain. The full Go regression suite passes after removal.
Static checks and targeted recovery/candidate-resolution race tests also pass;
release-source allowlist paths and local documentation links were checked.

The [candidate-ID review](./design/pre-attribution-review.md#candidate-id-resolution-review--2026-09-15)
documents current inferred reassignment and unverified routing, including their
effect on edge totals. This cleanup neither changes nor accepts those policies;
financial/graph semantics remain unchanged and user interpretation review stays open.

## 2026-09-15 — Start organization resolution with saved candidate evidence

The user approved continuing donor/corporate connections and Wikipedia/Wikidata
integration, treating Python as an experiment rather than target authority.
Implement a [bounded Go capture/replay slice](./design/organization-resolution.md)
over immutable FEC reported employer and connected-organization strings. Wikipedia
discovers pages; linked Wikidata items retain labels, aliases, revision metadata
and original claims. Raw captures and source references support offline replay.

The first proposal rule normalizes case/punctuation only and keeps ambiguity,
missing identity evidence and source failures explicit. A singleton exact name
within the saved search window is not a verified organization. No names are
silently merged, no personal employment/ownership facts are inferred and no graph
or financial attribution policy changes. Python's entity thresholds, property
direction assumptions and primary-company selection are not inherited.

Keep the external source contract draft pending broader source/matching acceptance.
The small real capture and byte-identical offline replay validate the acquisition
path, not matching accuracy. Next define source-grounded evaluation and corroboration;
do not turn this into another recovery, whole-corpus scan or terminal-allocation project.

## 2026-09-15 — Evaluate organization proposals without feeding labels into resolution

The user approved the next source-grounded matching/corroboration step. Add a
[reusable offline evaluator and retained corpus](./design/organization-evaluation.md).
Bind review annotations to exact captures and primary-source snapshot bytes.
Keep reviewed positive referents, explicit counterexamples and unreviewed alternatives
separate. An annotation is a development review assertion, not user acceptance of
canonical identity, employment, ownership or financial attribution.

The evaluator runs the unchanged proposal policy before comparing annotations.
Report source failures, retrieval misses, missing entity metadata, matching misses
and unreviewed proposals separately; never turn absent labels into negatives.
The first expanded capture hit a real HTTP 429. Preserve that failure and the
unattempted queries rather than claiming negative matches or complete coverage.

The first two reviewed positives were retrieved but not proposed. This justifies
working on candidate generation/matching, not silently loosening identity approval.
The corroboration requirements distinguish identifiers from name lookup, independent
sources from repeated assertions, and organizations from brands/parents/subsidiaries.
No runtime matching, graph, financial or terminal policy changes in this step.

## 2026-09-15 — Broaden organization name proposals without approving identities

The user approved continuing candidate matching. Add opt-in
`organization-name-proposals.v2`, with explicit letter/number boundaries and a
bounded trailing legal-designator vocabulary. Retain original text, matched source
names, exact transformations and the v1 outcome. Default commands remain v1;
unsupported policy names fail, with no mutable `latest` alias. Source acquisition
and physical parsing are unchanged.

Collect all matching QIDs before selecting a singleton. Broader rivals block even
an exact v1 winner. Preserve missing-source/type guards; never rank competing
entities or treat aliases as independent confirmations. No benchmark names, QIDs,
source positions, spelling model or score enters the production rules.

The unchanged [diagnostic corpus](./design/organization-evaluation.md#v2-replay-on-the-same-evidence)
now yields both reviewed positive proposals and neither annotated counterexample.
Retain failures and unknown alternatives. The corpus motivated the rules and is not
held-out validation or population-wide acceptance. Next expand harder-case evidence
and implement source-backed corroboration before any identity graph publication.
No employment, ownership, money selection or terminal-allocation rule changes.

## 2026-09-15 — Add exact-identifier registry evidence without a forced name fallback

The user approved independent corroboration and harder cases after the versioned
name proposer. Implement [bounded GLEIF capture and offline assessment](./design/organization-corroboration.md)
for exact LEIs asserted in the saved Wikimedia evidence. Derive requests in Go
from all usable query/QID candidates, preserve every claim occurrence and deduplicate
only identical network lookups. Pin source bytes, request membership and policy
versions. Support GLEIF's JSON:API media type in the shared source metadata schema;
the new source contract remains draft.

Keep legal names, previous names, identifiers, statuses, qualifiers and observation
dates separate. A fetched record verifies an observation of that identifier; it
does not independently bind FEC text to a legal entity. No currentness or historical
validity follows from a successful HTTP request. No shared website, parent lookup,
name search, benchmark label or named exception can fill a missing identifier.

The retained source-backed gate observes one LEI on a nonmatching candidate. The
two reviewed name proposals lack LEIs. Record both facts without approving any
identity, employment, ownership or financial edge. Synthetic hard cases exercise
positive correspondence and invalid/ambiguous/historical evidence without changing
the reviewed real labels. Next handle missing identifiers through independently
reviewed sources and broaden real evidence; an LEI must not become a mandatory
eligibility condition for all organizations. No graph, money or terminal policy changes.

## 2026-09-15 — Add independent bulk issuer discovery for missing identifiers

Continue missing-identifier work with a separate [SEC issuer directory adapter](./design/organization-issuer-discovery.md).
One pinned bulk snapshot supplies title/ticker/CIK evidence for the original FEC
query strings. Wikipedia success, a QID and an LEI are not prerequisites. This is
not a fallback inside the existing GLEIF policy and does not rewrite earlier outcomes.
Source and candidate contracts remain draft pending live capture and broader review.

Preserve exact rows and provenance. Reuse the unchanged explained v2 name rules;
group matches by CIK while retaining every ticker occurrence. All competing CIKs
survive, including broader rivals to exact matches. No named exceptions, scores,
parent inference, historical-name interpolation or graph writes are introduced.
Directory absence is scoped evidence, not entity nonexistence. Every candidate
retains missing independent FEC binding and temporal-validity blockers.

Go implementation and synthetic/replay tests are complete. Live SEC acquisition
awaits an operator-supplied reachable contact for its declared user agent; do not
claim a retained SEC corpus or real identity result before that gate runs.
Nonissuer sources, independent corroboration, historical validity and identity
publication remain separate work. No financial or terminal-allocation policy changes.

## 2026-09-15 — Retain the SEC live gate without committing operator contact

The user supplied a reachable project contact for the declared SEC request header.
Keep its value in gitignored `.env` as `SEC_USER_AGENT`; explicit `--user-agent`
overrides the exported variable. Do not print the configured value in CLI help,
load unrelated `.env` credentials or change other service identities.

The normal Go fetcher captured one complete public directory. Exact pinned-query
discovery then passed twice in fresh processes with byte-identical results. Retain
the unchanged public body as a regression fixture; keep full contact-bearing
capture metadata and discovery results private. The
[live gate](./design/organization-issuer-discovery.md#retained-live-gate) records
the counts, source/result pins and remaining identity boundaries.

The fixed policy found one directory CIK candidate on the existing twenty-name
diagnostic set. This demonstrates an additional identifier-discovery path, not
population-wide accuracy or an independent FEC identity binding. Existing matching
rules, reviewed labels, graph edges and financial interpretations are unchanged.

## 2026-09-15 — Preserve tagged filing identity without promoting a name match

Continue the approved candidate verification with the
[bounded filed registrant checker](./design/organization-filed-identity.md).
Go captures an explicit SEC filing and reads only selected DEI name/CIK facts and
their referenced contexts. Retain exact source bytes and occurrence locators;
unsupported transformations, conflicting identifiers and context limits remain
explicit. Reuse the existing SEC HTTP boundary rather than adding another client
or an unbounded source crawler. The source contract remains draft.

An additive offline comparison retains original FEC references and all directory
candidates. Format-only correspondence with a filed registrant supports a candidate;
it does not independently establish FEC identity, employment, corporate ownership
or money attribution. The real capture and fresh-process replay pass, with no
identity approval, reviewed-label changes, new name exceptions or graph writes.

The separately reviewed FEC mailing fields belong to the committee. Their match
with an SEC mailing address is supporting context, not a legal-entity address join.
The FEC registration-page request returned 403; no document contents or absence
were inferred. Keep that evidence gap explicit. Next review the identity-edge
evidence policy. Automatic filing selection/amendment handling and broader source
coverage remain deferred, not silently claimed by the reusable parser.

## 2026-09-15 — Test person affiliations before identity or money publication

The user requested executable tests for the proposed individual-to-corporation
relationships and their separation from terminal-source attribution. Add the
[bounded person-affiliation evaluator and tests](./design/person-affiliation-testing.md)
over synthetic source-qualified appearances and role assertions. This is a draft
screening policy, not accepted real person resolution or an identity graph rollout.

Preserve namesakes, source IDs/occurrences, unknown validity and multiple affiliations.
Name/employer correspondence selects candidates only. Do not use donation size,
source cycle, a reported occupation string, founder status or ordinary shareholding
to manufacture executive/control status. Keep roles and dates explicit; an as-of
observation is not a lifetime appointment. Do not choose a primary company.

The actual funding-path query boundary rejects affiliation families. All new
identity/publication/terminal/financial approval flags remain false. No source
adapter, network acquisition, graph write or monetary calculation was added.
The next bounded gate is real source-backed person/role evidence and acceptance
rules, not a claim that synthetic tests establish real identity accuracy.

## 2026-09-15 — Separate retained affiliation evidence from automatic role extraction

The approved [real-source diagnostic replay](./design/person-affiliation-corpus.md)
uses exact retained FEC occurrences and company pages. Corporate role meanings,
subject grouping and historical interpretation remain explicit reviewed test
annotations. A checksum or excerpt match verifies bytes, not that interpretation.
The audit runner calls the unchanged production screening evaluator; annotation
prose and expected outcomes cannot affect matching. Review IDs never become a
runtime identity directory.

Preserve the observed matching failures and missing historical validity. Do not
introduce person-specific name aliases, infer precise role dates from retrieval
time or year-only prose, or classify an unsearched engineer as a verified ordinary
employee. No financial interpretation or graph publication changes. The next
implementation is bounded automatic evidence extraction tested against this corpus,
not deployment of the reviewed role annotations as operational truth.

## 2026-09-15 — Extract structured role statements before automatic identity joins

The [bounded Wikidata role reader](./design/wikidata-role-extraction.md) automates
source interpretation without company-specific HTML rules or reviewed-label lookup.
Use explicit property direction; preserve statement occurrences, ranks, references,
unsupported qualifiers and date precision. Employment does not imply executive
authority, ordinary ownership does not imply control, and a general chairperson or
position-held statement does not imply a corporate board role.

Keep item type uncertainty explicit. A holder is not necessarily a person and a
related object is not necessarily a corporation. Year/month dates remain year/month
values; interval semantics and the screening bridge require a separate contract.
No identity, graph or financial approval follows from extraction.

Existing retained snapshots pass replay, but fresh review-item requests returned
upstream `maxlag` errors. Preserve those failures and leave the company-page corpus
comparison pending. Do not bypass backoff, substitute source failure for absence or
relabel reviewed annotations as automatically extracted evidence. Automatic person
and inverse discovery remain future work; the current CLI is offline and explicit.

## 2026-09-15 — Separate interactive source review from background backoff

The user approved one small interactive Wikidata request under the publisher's
[documented maxlag exception](https://www.mediawiki.org/wiki/Manual:Maxlag_parameter).
The request succeeded; earlier background-mode backoff responses did not establish
a general outage. Preserve both modes as separate observations. This does not add
a background fallback, raise a lag limit, or authorize omission of maxlag during
scheduled ingestion. The existing capture code is unchanged.

The [retained source comparison](./design/wikidata-role-extraction.md) replays all
selected statements through the unchanged extractor and keeps the original reviewed
corpus. Agreement on a historical role and year is distinct from identity approval;
unfetched organizations and alternatives are incomplete discovery, not negative
affiliation evidence. Next reuse the Wikipedia-first discovery pattern for people
and inspect relevant organization items for inverse role assertions. Do not add a
named-person directory, infer current authority from undated employment, substitute
one company for another, or change financial policy.

## 2026-09-15 — Keep automatic affiliation discovery exploratory

The user confirmed that this work remains exploration. The
[bounded discovery slice](./design/affiliation-discovery.md) implements repeatable
Go search planning, capture and offline replay, not accepted identity rules.
Use reported name and employer in separate windows; retain every source appearance
and every returned page, including rivals and unrelated results. A linked QID,
human-type assertion or singleton search window never approves a donor match.

The diagnostic source adapter reads the existing verified FEC corpus. Reviewed role
annotations and benchmark entity IDs do not choose requests. Query formatting is
an explicit experimental policy, not a change to stored names or financial rules.
Reuse the bounded transport under a distinct source contract; background maxlag
and stop-on-failure behavior remain unchanged. Preserve the first live baseline
before testing retrieval variants. Recursive inverse discovery, identity relevance/
corroboration, historical screening, affiliation publication and dollars remain open.

## 2026-09-15 — Separate retrieval variants from affiliation evidence

The [affiliation discovery experiment](./design/affiliation-discovery.md) now has
opt-in, source-derived employer formatting variants. Reuse the existing bounded
organization text rules; preserve v1 and the raw source values. Record derivations
per appearance and fail excess query fanout instead of truncating the population.
The live experiment did not recover the missing company items. Keep v2 opt-in.

The separate offline relevance calculation checks saved labels, aliases and titles
and preserves all rivals, including real namesakes and a vehicle alias matching an
employer stem. A shared name or search-context term is not identity or employment.
Inspect statement endpoint names only within the same retained response; preserve
unloaded endpoints, all selected statement occurrences and unassessed date validity.
No rank, singleton result or repeated observation becomes a confidence score or
accepted relationship. Company discovery and independent corroboration remain the
next evidence gaps, not reasons to broaden name matching into forced resolution.
The original corpus, screening policy, graph and financial rules stay unchanged.

## 2026-09-15 — Discover registry candidates without requiring a Wikidata item

The [registry-name discovery slice](./design/organization-registry-discovery.md)
uses verified reported organization fields directly. Requiring a Wikidata item or
preexisting LEI would make community coverage an unnecessary prerequisite for
company discovery. Preserve the old exact-identifier corroborator separately.

Bound each name query to the first five GLEIF records and retain pagination limits,
source failures, all result records and independent source occurrences. Name matching
does not choose identities: the real capture has two distinct corresponding-name
LEIs. Keep both, retain historical names/statuses and require independent binding
before affiliation publication. Empty search windows do not exclude companies.
Registry presence also establishes no person role or historical employment.

This remains exploratory Go code, not a new identity policy. No graph, dollar,
screening, corpus annotation or Dagster schedule changes. Next automate extraction
from the retained company sources and test identity/role corroboration; do not
substitute another round of broad name-only queries for the missing evidence.

## 2026-09-15 — Extract company-page evidence without promoting prose to identity

The user approved the next retained-source step. Here, company pages are company-
owned team/leadership/biography pages, not Wikidata items. The
[Go reader](./design/company-page-evidence.md) preserves lexical text, metadata,
JSON-LD and exact byte evidence from the already saved pages. Use the pinned HTML5
tokenizer, not company-specific selectors or a named-person runtime lookup table.
Keep source selection and observation provenance explicit; the reader does not fetch
pages or verify domain ownership.

Document-order headings, source titles and structured-data blocks are evidence, not
accepted person/organization relationships. Preserve current/historical wording,
near names, repeated content and invalid JSON without inventing role dates or
independent corroboration. All three pages pass fresh offline replay. The original
reviewed corpus and screening/financial rules remain unchanged. Prose-role
interpretation, independent identity bindings and dated affiliation publication
remain separate work; this adds no graph edges or terminal allocation.

## 2026-09-16 — Explore reusable relationship data before further implementation

The user wants relationships between people and organizations, organizations and
organizations, and people and people as useful data independently of donor-dollar
attribution. They explicitly requested exploration before implementation. The
[relationship note](./design/relationship-exploration.md) records a read-only review
of existing source examples, candidate meanings and open questions. It is not an
accepted graph schema or an authorization to add the proposed source/Arango pipeline.
Company-site parsing and LLM extraction are optional avenues, not settled architecture.

Keep source assertions, cross-source identity decisions, derived connections and
financial interpretations distinct. Unknown donor identity must not conceptually
prevent retaining a useful organizational claim. Query/presentation and acceptance
rules still need review; no runtime rule or graph was changed for this decision.

The user also reaffirmed that the project is still being built. The unused
input-copying subsystem was already removed; the read-only recovery tools and
reproducible-build machinery remain. Do not claim all of that code was deleted.
Use ordinary development builds/tests; park new archive/checkpoint work, exact-build
campaigns and full recovery acceptance until explicitly needed. Retain existing
source provenance, tests, dependency locks and publication integrity checks.
No further deletion, data copy or build/runtime change was performed in this turn.

## 2026-09-16 — Query retained relationship claims without accepting identities

The user approved the finite read path recommended by the
[evidence comparison](./audit/relationship-evidence-comparison-2026-09-16.md).
The [implemented query](./design/relationship-query.md) reuses the Go role reader
and adds parent/child claims under a separate source contract. Given a source entity,
it returns incident claims from one retained response, not a complete neighborhood.

Preserve original direction, raw statements, qualifiers, date precision and missing
endpoint states. Display inverse parent/child properties consistently without
merging their assertions or inferring legal ownership/control. Observation time is
optional caller metadata, never role validity. Existing role/discovery consumers
keep their original selection; names and test QIDs do not enter runtime rules.

This is not FEC-person resolution, accepted corporate affiliation, an Arango
publication or financial attribution. No service, dependency, acquisition pipeline,
LLM or recovery work is added. Identity and transaction-time affiliation binding
remain separate follow-ups; broad relationship graph design remains open.

## 2026-09-16 — Test identity context and dated roles separately

The user approved testing FEC-to-person and dated-affiliation binding. The
[binding diagnostic](./design/person-binding-diagnostic.md) adds pure Go comparison
and retained-source tests, not identity acceptance. It reuses existing name and
employer rules; namesakes remain candidates even when their role dates differ.

Compare receipt dates at the source qualifiers' precision. Do not fabricate day
bounds, infer current service from missing ends, or treat a different as-of period
as proof of non-service. These are provisional diagnostic comparisons; the existing
day-only screening and relationship-query contracts are unchanged. No graph,
financial, acquisition or production-resolution policy changes. The tests identify
dated private-company evidence and independent identity corroboration as remaining
gaps; company-page annotations remain reviewed rather than automatically extracted.

## 2026-09-16 — Keep dated corporate evidence separate from continuity inference

The user approved the [dated first-party evidence check](./audit/dated-person-role-evidence-2026-09-16.md).
Retained-source tests reuse existing Go readers and evaluators; source selection,
prose-role interpretation and point-date assignment remain reviewed. Page/update,
document, event and acquisition dates must not substitute for one another.

The new examples strengthen candidate context but do not select an identity or
continuous-tenure rule. Review corroboration and temporal inference separately
before implementing acceptance; no age cutoff, alias exception, source pipeline,
graph publication or money rule is added by this research step.

## 2026-09-16 — Implement bounded affiliation evidence evaluation, not identity acceptance

The user approved the next step after the acceptance proposal. The additive
[Go evaluator](./design/person-affiliation-acceptance.md#implemented-scope) implements
its concrete candidate and role-time rules. The multi-signal identity route was
not specified sufficiently to implement; it remains closed rather than becoming
an arbitrary confidence score or caller-supplied acceptance flag.

Keep explicit role assertions and denials, original source references and separate
person/organization/specific-role timelines. Continuity is opt-in, requires known
distinct observation origins and nearest bracketing points, and remains inferred.
Unassessed constraints, term breaks, reused/unknown origins and unscoped role
evidence must not be discarded to manufacture a bridge. Broad executive categories
cannot equate different offices. These checks consume caller-authenticated claims;
they do not authenticate sources or interpret prose automatically.

No existing screening policy, capture, graph, terminal or financial behavior changes.
Real-source tests retain earlier role points and unresolved identities. Next work
is a concrete independently corroborated identity rule and its evaluation, not
another source pipeline, recovery campaign or automatic affiliation publisher.

## 2026-09-16 — Reject the exact identity conjunction after feasibility review

The user approved specifying and evaluating independent corroboration. A bounded
experimental rule tested exact structured name and occupation text, mandatory
personal locality, receipt-day coverage and blanket rival blocking. The subsequent
[real-source review](./audit/person-identity-rule-feasibility-2026-09-16.md) rejects
that shape: it couples person identity to affiliation time, mistakes address
contexts for stable locality, ignores semantic role equivalence and blocks rivals
even when distinguishing evidence exists.

Remove the experimental evaluator, its synthetic tests and trial-only city/state
audit output. Preserve the implemented candidate/role-time evaluator and all source
bytes. A replacement must return identity evidence separately from role meaning
and date applicability, treat locality as typed optional evidence, and block only
rivals still plausible under the same distinguishing evidence. Evaluate Chambers,
Duffield, Catsimatidis and ordinary-employee contrasts before publication. No
canonical merge, graph edge, terminal decision or money allocation is authorized.

## 2026-09-16 — Implement a separated identity-evidence classifier without acceptance

The user approved the replacement after the rejected exact-field conjunction. The
[v2 evaluation](./audit/person-identity-evidence-v2-2026-09-16.md) adds a pure Go
classifier over exact appearances and bounded caller-authenticated observations.

Keep structured name components, organization text correspondence, reported-role
meaning, source-role timing, provenance and optional typed locality as separate
outputs. Missing middle/suffix components preserve a rival; a conflicting supplied
distinguishing component does not. An unattested first-name variant remains an
explicit noncandidate gap. Add only the bounded `CO`/`COMPANY` legal-designator
correspondence locally; it is not legal-entity resolution.

Never infer source independence from hashes or URLs. Multiple origins remain
independence-unassessed. Preserve explicit role support, denial and conflict without
letting them decide person identity. Locality context never gates identity or becomes
residence evidence. Emit no score or winner and keep identity, graph and financial
approval false.

The next decision is a source bridge, not a publisher. It must provide source-backed
structured names and variants, organization-identity evidence, role semantics,
origin/dependence and discovery scope with offline replay and abstention. No canonical
merge, affiliation edge, terminal rule or money allocation is authorized.

## 2026-09-16 — Automate appearance enrichment with existing parsed-source rules

The user approved an end-to-end FEC appearance enrichment run and evaluation on the
reviewed cases plus an additional code-selected sample. The
[implemented command](./design/affiliation-enrichment.md) reads verified FEC rows,
plans and captures Wikipedia/Wikidata searches, and produces a per-appearance report.
Offline replay binds the exact source references and reported text to the capture.

Reuse existing full-name/alias comparisons and source-precision role dates. The v2
review classifier requires manually interpreted components/day-only claims and is
not the automatic source interface. Do not manufacture those inputs to wire it in.
Preserve all retrieved candidates, including unmatched names, no-employer rivals,
missing items and failures. Keep source responses separate; repeated QIDs are an
inventory, not independent identity proof. Retain statements that cannot attach to a
usable candidate and preserve role and source issues.

Additional sample selection hashes exact name/employer text pairs from the supported
filing profile, excluding reviewed pairs. It is an explicit diagnostic sampling rule,
not donor deduplication or population coverage measurement. The fresh three-case run
and saved replay work without per-person annotation; they expose missing evidence
coverage. Independent identity corroboration, production bulk selection and graph
publication remain unimplemented. No money interpretation changes.

## 2026-09-16 — Keep prose syntax proposals separate from accepted assertions

The user approved the bounded extraction experiment following supplementary-source
research. The [implemented baseline](./design/prose-relationship-prototype.md)
uses a generic English grammar over verified retained HTML text. It recognizes
explicit role-clause prefixes and parenthetical name forms, with original wording,
entry-level raw HTML citations and distinct offsets in projected text. No person,
company or website exception controls extraction.

Expose it only through `extract-company-page --propose-relationships`; preserve
default lexical output and do not wire it into automatic donor matching. A syntax
candidate is not a verified positive assertion: context, entity types, website
ownership and publisher reliability are unassessed. Preserve explicit negation
and time wording without converting page dates or present tense into tenure.
Keep duplicate occurrences and unsupported layouts visible. All identity, graph
and financial approvals remain false.

The seven retained development pages provide regression evidence, not held-out
precision/recall. Evaluate unseen pages before widening the grammar or considering
a model-assisted comparison. Automatic source discovery and independent donor
binding remain separate work. No crawler, model call, source adapter or publisher
is added by this prototype.

## 2026-09-16 — Do not promote the prose grammar after out-of-development evaluation

The user approved evaluating the unchanged baseline before connecting it to donor
matching. The [source-bound replay](./audit/prose-extraction-evaluation-2026-09-16.md)
finds one of six reviewed role witnesses across five readable bodies and records
one selected page's retrieval failure separately. The reader retains all reviewed
text. Compound punctuation, quote attribution, pronouns and separate cards explain
the missed roles. No source or grammar implementation changed.

Keep this explicit grammar as a baseline, not the general extraction method. Passing
regression tests preserves the measured misses; it is not a quality-acceptance gate.
The manually selected, post-output-reviewed witnesses do not establish population
accuracy. The single supported output does not establish precision, and no positive
alias witness supports an alias-accuracy claim. Preserve acquisition failures rather
than replacing them with search snippets or treating them as empty role sets.

Recommend a bounded source-cited local-model comparison next, with subject/organization,
negation, date and unsupported-claim controls. No model call, deployment, crawler,
identity acceptance, graph publication or money policy is authorized by this result.

## 2026-09-16 — Evaluate local-model proposals without promoting citation validity to truth

The user approved and asked about a local-model comparison. The
[first trial](./audit/local-prose-model-comparison-2026-09-16.md) uses the existing
self-hosted gateway and a live-discovered model ID, with fourteen bounded sequential
requests and retained responses. No runtime inference client or deployment is added.

Keep model output separate from source facts. Require exact entry references and
literal quoted fields, retain invalid answers and reject incomplete generations.
Nine Gemma answers pass those literal checks and five fail, while semantic review
still finds wrong date meaning, unsupported role/alias scope and absent card context.
A real quotation does not prove a correct relationship. Passing replay tests means
the recorded trial, including its failures, remains reproducible.

This is reviewer-selected excerpt interpretation, not autonomous page discovery or
whole-page accuracy. Next compare the same task with the stronger available local
model using supported controls before changing the prompt or relaxing constraints.
Keep old captures intact; no identity acceptance, graph edge or monetary rule follows.

## 2026-09-16 — Preserve the same task across the stronger-model comparison

The user approved the [stronger local comparison](./audit/stronger-prose-model-comparison-2026-09-16.md).
Keep the source windows, prompt, schema and validator unchanged. Only explicit
model-supported controls differ: GPT-OSS uses medium reasoning and 4,096 output
tokens; Gemma used no reasoning and 2,048. Advertised sampler defaults also differ.
This is a configuration comparison, not equal-compute or population accuracy.

All fourteen calls complete. Eleven answers pass literal checks and three fail;
manual review still finds omitted context and wrong role meaning. Preserve all
answers, whole-answer failures and the original Gemma run in offline replay.
Neither model is accepted for production extraction. No source, identity, graph
or financial behavior changed. Code-owned full-entry evidence attachment is the
recommended next experiment, not an implemented or approved production design.

## 2026-09-16 — Attach original evidence in code without treating references as truth

The user approved the [entry-attachment experiment](./audit/prose-evidence-attachment-2026-09-16.md).
In a separate test-only contract, the model supplies literal fields and entry IDs;
Go supplies complete selected text, source metadata and full input context with raw
HTML spans. Reuse literal validation and whole-answer rejection. Do not repair or
normalize returned fields, silently add missing references, or overwrite old trials.

Six known real cases and nine fresh controls completed with eleven literal passes
and four rejections. Correct evidence attachment does not establish pronoun binding,
role scope, alias meaning or retraction semantics. Preserve those failures in offline
replay and manual review; do not promote the experiment to accepted affiliations.
The next proposed design/test boundary separates literal mentions from derived
interpretation and contextual status. Source selection, production identity, graph
publication and financial attribution remain separate, unchanged work.

## 2026-09-17 — Keep literal mentions separate from supplied interpretations

The user approved the [offline mention/interpretation proof](./design/prose-mention-interpretation.md).
Literal source occurrences retain exact reader text, text offsets and raw HTML spans.
Normalized labels, role/activity/name-form classifications, pronoun bindings and
assertion statuses live in separate proposals. Explicit context links target local
interpretation IDs; corrections do not overwrite the original positive statement,
and conflicting statements do not gain a winner by ordering.

Go validates references and applies those supplied context links; it does not prove
their semantic correctness or discover missing corrections. The six retained
synthetic cases use reviewed annotations, not repaired model outputs or production
whitelists. No model call, new prompt, runtime pipeline, accepted affiliation or
financial behavior changed. A fresh automatic-producer experiment remains the next
test before any acceptance decision.

## 2026-09-17 — Preserve failed automatic interpretations without promoting them

The approved [fresh producer trial](./audit/prose-interpretation-model-2026-09-17.md)
uses the separated contract in test-only code. Model output cannot claim reviewed
origin or approvals; Go stamps its provenance and retains full supplied context.
Nine fixed attempts preserve five literal/reference passes and four rejections,
including every real-source input. No response is repaired or retried.

Retain semantic review separately from structural checks. Successful synthetic
context handling does not establish adequate role evidence, real-page accuracy or
affiliation acceptance. Do not deploy this producer. The proposed next step is an
offline test of code-owned occurrence/full-entry grounding; semantic interpretation,
source discovery and donor identity remain separate unresolved boundaries.

## 2026-09-17 — Test source-owned citation coordinates without repairing model answers

The user approved the [offline citation-selection proof](./design/prose-citation-selection.md).
Go owns lexical token IDs, exact text spans and complete selected-entry attachment.
Supplied selections cannot contain quotes or byte offsets. Exact-match diagnostics
enumerate ambiguous occurrences and preserve missing text, never choosing a first
match or normalizing a failed quote into a pass.

Retain prior trial outcomes unchanged. New reviewed selections are contract tests,
not improved model accuracy. Valid citations still do not establish role meaning,
person binding, source truth or affiliation acceptance. The next proposed trial
must measure model selection and cost; no new live call or runtime behavior is added
by this proof.

## 2026-09-17 — Retain token-ID trial limits without claiming an extraction upgrade

The user approved the [bounded token-ID trial](./audit/prose-token-selection-model-2026-09-17.md).
Reuse the prior nine excerpts and model controls, retaining all outcomes without
retries or repairs. Four answers pass structural checks, four fail required reference
checks and one receives a capacity error. Exact source ranges coexist with wrong
endpoint selection and unresolved pronouns; higher input usage is not a reliability gain.

Do not promote this research producer. Keep infrastructure failure separate from
semantic failure and old captures unchanged. Next review whether Go should derive
redundant required-reference unions from explicit selections; that proposed offline
change must not be presented as repairing old answers or solving semantic binding.

## 2026-09-17 — Derive redundant citation references without inferring meaning

The user approved the small [offline reference-contract change](./design/prose-citation-selection.md#go-derived-required-references).
Go derives a stable evidence-reference union from explicit clause, subject and
organization selections plus explicit additional evidence. Keep the supplied proposal
separate from the derived review, and reuse existing range, binding and context-link
validation. Do not infer endpoints, corrections, identity or acceptance.

Independent synthetic annotations prove the new shape. Old prompts, captures,
producer and failure results stay unchanged; no model call or runtime behavior changed.
Next test selection and binding on fresh inputs with this simpler shape before
claiming any extraction improvement. Valid references alone are not correct meaning.

## 2026-09-17 — Retain fresh reference-trial failures without promoting extraction

The user approved the [six-case fresh trial](./audit/prose-derived-reference-model-2026-09-17.md).
The simplified reference producer uses fixed inputs, a fixed prompt and the existing
Go validator. Retain all outcomes: four structural passes, one generation truncation
and one joint-activity contract rejection. No retries, response repairs or runtime
changes. Old producer results remain unchanged.

Separate wrong source ranges from valid range coordinates, semantic successes from
whole-answer rejection, and the joint-activity representation limit from genuine
identity ambiguity. Do not promote the producer: passing references still include
wrong person/company endpoints and an instruction-derived unclear role. The next
proposed experiment isolates name-span selection; it is not a production architecture
decision or permission to add a new source pipeline.

## 2026-09-17 — Measure name selection separately from relationship meaning

The user approved the [isolated names-only trial](./audit/prose-name-selection-model-2026-09-17.md).
Select exact literal person/organization spellings without roles, normalization or
identity links. Keep pre-run expectations out of model input and compare exact
source-selected text and proposed kinds; report missing, unexpected and duplicate
selections separately from coordinate validity.

Eight attempts match all 22 known-case spellings and 13 of 15 fresh spellings, with
two extra fresh selections. Retain the fresh surname, punctuation and institutional-
scope errors; do not trim, relabel or repair old outcomes. This narrowed task does
not establish full relationship extraction or production identity acceptance.
Next test role binding using uncorrected candidates and full source context, with
separate ambiguity/abstention review. No runtime or graph behavior changed.

## 2026-09-17 — Keep two-stage role binding as unverified research

The user approved the [bounded role-binding experiment](./audit/prose-role-binding-model-2026-09-17.md).
Use original first-stage name captures without repairs; restrict second-stage
endpoints to those candidates and retain full source context. Keep original proposals
separate from Go-derived evidence. Reference validity never approves identity or meaning.

All ten second-stage answers pass structural checks. The two fresh controls preserve
ambiguity, hypothetical status and missing endpoints, while known cases expose wrong
binding labels, omitted selected context and role/institution-scope gaps. Retain the
no-name control's disagreement with its frozen empty-output expectation; it exposes
a task-scope tension, not an accepted named relationship. No responses are repaired.

Do not promote this producer or claim general accuracy improvement. Resolve the
documented interpretation-contract gaps before another fresh check. This decision adds
no production model client, crawler, source pipeline, affiliation or financial rule.

## 2026-09-17 — Separate role highlights, complete context and endpoint surfaces

The user approved the [role-grounding follow-up](./design/prose-role-grounding.md).
Keep one literal role focus separate from the complete Go-attached statement entry.
Keep actual name/pronoun surfaces separate from uncorrected candidate endpoints;
claimed direct binding must meet exact surface equality, without approving referent
identity. Preserve anonymous roles as unresolved evidence, and use unknown coarse
classes when advisory-body scope does not justify a corporate directorship.

The [five-case fresh trial](./audit/prose-role-grounding-model-2026-09-17.md) retains
four structural passes and one reserved-ID rejection. The surname counterexample
proves literal equality is insufficient for ambiguity; a recognized correction still
lacks its retraction link. Keep these failures and old trials unchanged. Next review
referent resolution and explicit correction targets offline; no producer promotion,
identity acceptance or financial-rule change follows this experiment.

## 2026-09-17 — Require separate referent and correction-target assessments

The user approved continuation of the
[offline contract review](./design/prose-referents-and-corrections.md).
Keep literal name correspondence separate from unassessed, unresolved, ambiguous
or proposed referents. Candidate IDs remain source mentions, never accepted people
or organizations. Require each supplied correction to state proposed, ambiguous
or unresolved targets; only explicit proposed targets derive retraction links.

Retain the original proposal and distinguish caller-owned grounding and assessment
origins. Reviewed test overlays must not masquerade as model output or repair old
answers. Independent and retained-source tests prove representation and conditional
rules, not better automatic interpretation. No inference, model deployment, graph
publication or financial policy changes. Next evaluate an opt-in producer against
this shape after verifying its actual served model and controls.

## 2026-09-18 — Prepare the referent trial without model-node load

The user approved the [next experimental step](./design/prose-referents-and-corrections.md#offline-producer-preparation--2026-09-18)
and requested notice before a trial. Add only a test-side request/response adapter
and fixed regression set now. Keep review checks out of requests, reuse uncorrected
automatic names, and preserve old producers and captures. Candidate/model-specific
exceptions, production integration and output repair remain out of scope.

The eight prepared cases are known regressions, not held-out accuracy evidence.
Offline tests validate structure and provenance, not model quality. A separate
explicit trial opt-in prevents older generic settings from launching this producer.
No discovery, inference or deployment ran. Await approval before the bounded trial;
it needs eight sequential generation calls per model and served-ID/control checks.

## 2026-09-19 — Reject the combined grounding and assessment producer

The user authorized the complete
[referent/target comparison](./audit/prose-referent-model-comparison-2026-09-19.md).
Retain the explicit referent and correction-target representation, strict validator,
raw captures and manual review. Do not promote the producer that asks one response
to recreate lexical grounding, role interpretations, referents and correction targets.

Gemma 31B and GPT-OSS each pass structure on four of eight fixed cases. Only three
and two synthetic cases, respectively, also meet pre-run semantic checks; neither
model produces a usable answer on a real-source case. Rejected raw answers confirm
that the representation can express ambiguity and correction targets, but no partial
answer is accepted. A fixed seed does not make output deterministic.

The next experiment may isolate assessment from extraction: consume immutable,
already validated grounding and return only referent and target states. Keep fresh
evaluation, provenance and abstention gates. No model deployment, accepted identity,
graph edge or financial rule follows this decision.

## 2026-09-19 — Keep grounding-bound assessment test-only

The user authorized the
[narrow referent assessment comparison](./audit/prose-referent-assessment-model-2026-09-19.md).
Supply validated grounding as caller-owned immutable input, omit grounding and role
fields from the response schema, require explicit assessment rather than `unassessed`,
and join only in Go. Preserve separate grounding and assessment origins and reject
invalid answers whole.

Both models pass nine of ten structural checks and six of ten semantic reviews, with
one usable result among three retained real-source excerpts. This supports the task
split as a research boundary, not production promotion. Both models still confuse
short mention occurrences with supported full-name antecedents. Address that as an
offline representation problem separating occurrences, proposed antecedent groups
and canonical entities. Do not add named-person/company prompt patches, accept an
identity, publish graph edges or change financial attribution.

## 2026-09-19 — Separate occurrences, mention groups and canonical candidates

The user approved the next offline contract step in
[prose referents and corrections](./design/prose-referents-and-corrections.md#offline-occurrence-group-and-canonical-candidate-boundary--2026-09-19).
Keep literal source occurrences immutable. Represent discourse equivalence only as
explicit, unverified, same-kind mention groups with a proposed antecedent. Role
referents point to group IDs; ambiguous alternatives remain separate groups and do
not absorb the ambiguous short occurrence.

Keep canonical entity candidates in a later input with an independent namespace,
record identifier and pinned source. Bind that input to the exact mention-group
report digest. A source candidate or proposed mapping does not accept identity or
authorize graph publication or money attribution. Reject cross-layer identifiers,
overlapping groups, changed digests and model-supplied canonical source candidates.

Retained person-surname, organization-short-name and two-person surname cases prove
the representation offline. They are fixture annotations, not runtime exceptions.
No prompt, model call, source adapter or production package changed.

## 2026-09-20 — Accept separate pre-attribution evidence families

The user accepted the four boundaries supported by the
[focused 2024 validation](./audit/pre-attribution-interpretation-validation-2026-09-20.md).
The prior implementation was deterministic but mixed reported candidate assertions
with inferred replacements and exposed transitive Schedule A/B comparison
components that hid direct ambiguity.

Decision:

1. Preserve the filer-reported Schedule E candidate assertion separately from an
   exact-context alternative. Call a reported ID and exact context agreement
   `confirmed`; call a unique alternative for an absent reported master ID
   `inferred`; and call a unique alternative that disagrees with a present reported
   ID `conflicting`. Keep unverified, ambiguous and unresolved states explicit.
   Only confirmed agreement supplies a safe default endpoint. An alternative does
   not create another expenditure amount.
2. Retain strict raw/clean counterparty-ID agreement for countable Schedule A and
   Schedule B committee flows. The complete inspected one-sided populations are
   raw-only self references. Preserve them as evidence; do not manufacture fallback
   self-flow edges.
3. Retain exact-reference conduit pair/star associations with zero financial
   effect. Do not require amount or date equality. A conduit association is
   reported context, not another payment, origin proof or allocation.
4. Publish direct Schedule A/B comparison pairs before transitive component union.
   Preserve both independent observations, exact signals, signed and absolute date
   gaps, evidence bands and both candidate degrees. Comparison carries no money and
   performs no cross-ledger deduplication. A later product may apply an adjustable
   date filter, but a date threshold is not payment identity.

Implement these as additive, immutable calculations. Do not rewrite the retained
candidate-resolution decisions, reconciliation components, graph generations or
financial totals. The strict committee-flow and conduit rules already satisfy their
accepted boundaries; they need no replacement calculation. The new
candidate-interpretation and direct-pair calculations pass their
[complete 2024 publication gate](./audit/pre-attribution-interpretation-publications-2026-09-20.md).

Still open: the product-shaped candidate view, graph/presentation consumers,
Dagster wiring, additional-cycle publication, terminal-source definitions and any
dollar-allocation method.

## 2026-09-20 — Present candidate evidence without cross-domain collapse

The user authorized the first product-shaped candidate slice after accepting the
pre-attribution evidence families. Implement the slice as a versioned read-only
dossier over one exact candidate-evidence v2 report and one complete published
candidate-interpretation set.

Decision:

1. Keep candidate-linked receipt evidence separate from independent
   expenditures. They are different reporting domains and are never added.
2. Within independent expenditures, publish reported endpoint, corroborated safe
   default, inferred alternative and conflicting alternative as overlapping,
   explicitly non-additive views. Store each source interpretation once and list
   every role it supports.
3. Preserve support and opposition separately. Preserve signed, positive,
   negative and zero measures in integer minor units.
4. Require cycle agreement and exact immutable input identities. Do not require
   different evidence domains to carry one source-release ID. Record the exact
   Schedule A, committee-flow and Schedule E release IDs and disclose their
   alignment state.
5. Keep terminal, allocation, person/corporation, lobbying and legislative-action
   policy outside this presentation contract.

The [two-candidate 2024 gate](./audit/candidate-dossier-2026-09-20.md) passes. The
view changes no source fact, calculation, graph, current pointer or prior report.
Terminal definitions and allocation methods remain the next product decision.

## 2026-09-20 — Accept partial direct and earmarked source-appearance attribution

The user accepted the narrow boundary recommended by the
[terminal-policy comparison](./design/terminal-policy-comparison.md). Implement a
separate production calculation over the complete receipt-participant publication;
do not promote dossier diagnostic output.

Decision:

1. Attribute a known nonmemo `itemized_individual_only` occurrence directly to
   its reported source appearance. Keep an explicit earmark in a separate,
   exclusive bucket.
2. Treat this endpoint as one reported source appearance, not a resolved or
   deduplicated person or organization. Employer and occupation text do not
   establish corporate affiliation.
3. Leave every committee-chain amount unresolved. Do not use a topology
   frontier, immediate committee stop, pooled pro-rata, FIFO or path replication
   as the upstream economic origin.
4. Exclude memo subtotal appearances from counted money while preserving them as
   evidence. A conduit association adds no second amount.
5. Route occurrences to a candidate only through one uniquely accepted same-cycle
   candidate–committee authorization. Shared and unresolved authorization carries
   no candidate money.
6. Preserve exact rows, signs, unknown amounts, immutable input identities and a
   versioned source-membership predicate. Require candidate and cycle-level
   conservation and deterministic replay.

The [complete 2024 gate](./audit/direct-source-attribution-2026-09-20.md)
passes all 264,085,606 participant occurrences and byte-identical eight/four-worker
replay. The result contains 7,470 candidate aggregates. It does not publish a
durable artifact, update ArangoDB, resolve entities, allocate committee chains or
combine candidate receipts with independent expenditures. Those remain separate
boundaries.

## 2026-09-20 — Do not make derived publication the automatic next step

The high-level inventory found that the accepted direct source-appearance
calculation is deterministic, schema-validated and byte-identical across worker
counts even though its complete result currently lives under `/tmp`. Reproducible
calculation and durable operational publication are separate concerns.

Decision:

1. Keep immutable acquired sources, normalized facts, lineage, conservation gates
   and existing calculation publications. They are the justified evidence core.
2. Do not add a content-addressed publisher, Dagster asset or ArangoDB projection
   to every accepted derived result by default.
3. Add operational publication only when a concrete downstream graph, API,
   schedule or product query needs durable discovery and lifecycle management.
4. Review and land or remove the current worktree before starting another large
   feature slice. Then use the candidate-shaped evidence to choose the next domain
   boundary; identity resolution and committee-chain allocation remain separate.

This corrects sequencing. It does not weaken source/fact immutability, provenance,
dependency locks, replay tests or the integrity of publications that already have
real consumers. See the
[2026-09-20 inventory](./audit/go-rewrite-inventory-2026-09-20.md).
