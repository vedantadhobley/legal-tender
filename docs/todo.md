# TODO

The persistent, project-scoped scratchpad. Cross-references @audit/ for source of each item and @plan.md for phase context.

This file is editable by both humans and the agent during sessions. Append-friendly; use markdown checkboxes; reference file:line where helpful.

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

- [ ] **Remaining hit-rate residuals:**
  - Some corporate families that should be merged remain split: ADELSON DRUG CLINIC vs ADELSON CLINIC (different Q-ids in Wikidata or one not_found), Pan Am Systems vs Pan Am Railways (different real entities owned by Mellon). Need follow-up: SPARQL parent-resolution would catch most of these via P749 once SPARQL endpoint is healthy.
  - KKR HOLDINGS suffix-strips to KKR which still ranks Kolkata Knight Riders (cricket team Q1156894, P31=Q12973014) above Kohlberg Kravis Roberts (Q1570773, real KKR private equity). Add Q12973014 (cricket team) and other sport-team P31 Q-ids to `_NON_CORPORATE_P31` so the cricket team gets skipped and the real KKR is picked from top-N.
  - FEC name format vs Wikidata search ranking: "SIMONS, JAMES H" → "James Simons" → top hit is 19th-century lawyer Confederate general (Q109713137), not Jim Simons hedge fund mathematician (Q560847). "Jim Simons" gets the right hit but the FEC normalization doesn't always produce that form. Hard to fix without per-name disambiguation hints.
  - Moskovitz-shaped gaps: founders are listed on the company side (P112) not the person side, so REST path can't follow that edge cheaply. Acceptable miss; deferred until SPARQL is healthy enough to do the inverse query.

### Configuration centralization

- [ ] **Single `ACTIVE_CYCLES` constant.** Currently hardcoded as `["2020", "2022", "2024", "2026"]` in **17 files**. Move to `src/config.py` (new module) and import everywhere. Source: @audit/codebase-inventory.md "Configuration sprawl".
- [ ] **`PER_ELECTION_LIMITS` constant.** Currently lives in `src/assets/graph/donors.py:47-50`, referenced via comments in 4 other places. Move to `src/config.py`, import where needed.
- [ ] **`TERMINAL_TYPES`, `PASSTHROUGH_TYPES`, `CONDUIT_PATTERNS`** in `candidate_upstream.py:67-74` — duplicated (with subtle divergence!) in `pies_v3.py:21-28`. Move to `src/config.py`.
- [ ] **Progress logging interval constants.** `pas2.py:130` (% 250000), `contributed_to.py:272` (% 50000), `arango_dump.py:248` (% 500000) all use different intervals. Pick one (`LOG_PROGRESS_EVERY = 100_000`), use everywhere.

### Cache + data hygiene

- [x] ~~**Move repo-root caches to `cache/`.**~~ Done in commit `9ac4321`. `wikidata_cache.json` migrated to `~/workspace/data/legal-tender/cache/wikidata.json`. Both `/wikidata_cache.json` and `/corporate_families.json` removed from tracking and added to `.gitignore` so they can't be re-committed.

### Logging hygiene

- [ ] **Replace 158 `print()` calls with logger / context.log.** Concentrations: `src/utils/preflight.py`, `src/utils/arango_schema.py`, scattered through assets. Critical impact: many warnings (including the wikidata 429s) hit stdout but not Dagster's UI. Source: @audit/code-quality-findings.md §5a.
- [ ] **Audit 6 bare `except Exception:` clauses.** Three are around metadata-emit in `*_summaries.py`; should at minimum log the swallowed exception. Two are reasonable fallbacks. One in `canonical_employers.py:243` needs investigation. Source: @audit/code-quality-findings.md §5b.

### Dead code removal (verify before deleting)

- [ ] **Delete `src/cli/pies_v3.py`** (679 LOC). No callers; predates funding_channels. Verify no local workflow depends on it first. Source: @audit/code-quality-findings.md §2a.
- [ ] **Delete `src/cli/check_funding.py`** (74 LOC) OR move to `scripts/`. No callers; CLI helper. Source: @audit/code-quality-findings.md §2b.
- [ ] **Move root-level dev scripts.** `test_download.py`, `test_fec_schema.py`, `validate_schemas.py` are not pytest tests; they're dev utilities. Move to `scripts/` (currently empty) and rename without `test_` prefix.
- [ ] **Verify no orphan imports** for the 4 deleted enrichment files (`committee_financials.py`, `corporate_hierarchy.py`, `employer_cluster_integration.py`, `employer_clustering.py`). Source: @audit/code-quality-findings.md §2c.

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
- [ ] **`DEMOCRACY ENGINE, INC., PAC` ($46M) classified as `corporation`.** Functionally a payment-processor conduit for Dem small-dollar money. Single-entity edge case. Manual override in committee_classification or `CONDUIT_PATTERNS` extension would handle it.
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
