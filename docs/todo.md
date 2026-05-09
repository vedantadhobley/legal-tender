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
- [ ] **End-to-end live validation.** Pending — Wikidata's public SPARQL endpoint is currently returning 502/timeouts (verified with direct curl 2026-05-09). Re-run `wikidata_corporate_resolution` when their service recovers and confirm the new batched path actually pulls corporate-family data at scale. Cache file already at `~/workspace/data/legal-tender/cache/wikidata.json` with 27 employer entries from prior run as starting point.

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

- [ ] **Refactor `candidate_upstream.py`.** 1,150 LOC with one ~1,050-line function. Break into ~10 focused helpers. Validate by byte-comparing `candidate_funding` output before and after. Big scope; do AFTER the wikidata + cycles wins so you can iterate without bouncing. Source: @audit/code-quality-findings.md §3.
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

## Performance / iteration speed (2026-05-09)

- [x] ~~**Cycle-level parallelism via threads.**~~ Done in commit `2542b86`. Added `src/utils/parallel.py` with `parallel_cycles` (threads, I/O-bound) and `parallel_map` (processes, CPU-bound, reserved for future use). Wired into `committee_receipts` Phases 3+3.5, `donors`, `transferred_to`. `committee_receipts` ~12min → ~6min (2× — Amdahl-limited because Phases 1, 2, 4 are still sequential single AQL queries / single UPSERT loop). `indiv`, `pas2`, `oth` were already parallel.
- [x] ~~**Phase 4 of `committee_receipts` parallel UPSERT.**~~ Done in commit `662bd96`. 8 thread workers, each owning a chunk of disjoint cmte_ids and its own batched UPSERT stream. Speedup turned out modest (~30s saved on a ~6min run) — ArangoDB UPSERT is server-side bottlenecked; more client workers just queue at the server. Worth keeping for slightly faster runs and as a pattern, but Phase 3+3.5 cycle parallelism remains the bigger win.
- [ ] **`candidate_funding` ProcessPool.** The big remaining win (15-25× expected). Deferred to the `candidate_upstream.py` monolith refactor (1,150 LOC into ~10 helpers — already a Phase 3 task). The refactor lifts the nested closures to module level, which is the prerequisite for ProcessPool picklability. Don't do these as separate efforts.
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
