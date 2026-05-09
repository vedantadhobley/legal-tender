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

- [ ] **Persistent negative cache.** `wikidata_cache.json` should store sentinel entries for "tried, no Wikidata match" so subsequent runs skip them. Currently dead-end queries get re-fired forever. Source: @audit/code-quality-findings.md §1b.
- [ ] **Batched VALUES queries.** `_execute_sparql` issues one request per company name. Rewrite calling code to batch 50 names per request via SPARQL `VALUES ?label { ... }`. 5,000 sequential queries → ~100 batched queries. Source: @audit/code-quality-findings.md §1b.
- [ ] **Exponential backoff + circuit breaker.** On 429/502, retry with `2^attempt` delay, cap at 60s, give up after 3 consecutive global failures. Prevents future 14-hour grinds.
- [ ] **Incremental cache flush in `wikidata_resolution.py`.** Save cache every N queries, not only at end of run. Saves work if the asset crashes.
- [ ] **Make `WIKIDATA_CACHE_PATH` configurable via storage helpers.** Currently hardcoded `/workspace/wikidata_cache.json`. Should use `get_cache_dir() / "wikidata.json"`.

### Configuration centralization

- [ ] **Single `ACTIVE_CYCLES` constant.** Currently hardcoded as `["2020", "2022", "2024", "2026"]` in **17 files**. Move to `src/config.py` (new module) and import everywhere. Source: @audit/codebase-inventory.md "Configuration sprawl".
- [ ] **`PER_ELECTION_LIMITS` constant.** Currently lives in `src/assets/graph/donors.py:47-50`, referenced via comments in 4 other places. Move to `src/config.py`, import where needed.
- [ ] **`TERMINAL_TYPES`, `PASSTHROUGH_TYPES`, `CONDUIT_PATTERNS`** in `candidate_upstream.py:67-74` — duplicated (with subtle divergence!) in `pies_v3.py:21-28`. Move to `src/config.py`.
- [ ] **Progress logging interval constants.** `pas2.py:130` (% 250000), `contributed_to.py:272` (% 50000), `arango_dump.py:248` (% 500000) all use different intervals. Pick one (`LOG_PROGRESS_EVERY = 100_000`), use everywhere.

### Cache + data hygiene

- [ ] **Move repo-root caches to `cache/`.** `wikidata_cache.json` (13KB) and `corporate_families.json` are cache files tracked in git. Move to `~/workspace/data/legal-tender/cache/`, gitignore them, update path references. Source: @audit/code-quality-findings.md §11.

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

- [ ] **Self-funder gap.** Candidates who poured their own money in (David Trone -99%, Jim Lamon -94%, Michael Gibbons -95%, Christina Clement -100%) show massive undercounts because **`CAND_CONTRIB` isn't folded into our individuals total**. FEC counts a candidate's self-funding as a receipt; we don't. Two options: fold into `individuals.total` (treat self-funding like an individual contribution), OR add a new channel/sub-bucket for self-funding. Lean toward the former — it's an individual giving to a campaign, just one with a special FEC code. Source: `validation_report.py` 2026-05-09.

- [ ] **JFC / leadership-PAC scope mismatch.** Some candidates with multiple affiliated committees (Scalise +81%, Sanders +35%, Haley +45%) show *over*-attribution. Our `direct_funding` aggregates across ALL affiliated committees while `weball.TTL_RECEIPTS` reports only the principal campaign committee. This is a *definitional* difference, not strictly a bug. Decide: do we restrict trace to principal committee only (matches weball), or keep the broader scope (more accurate "total money raised in this candidate's name") and document the difference? Likely keep + document.

- [ ] **Possible whale/grassroots double-count for unitemized-heavy candidates.** Sanders 2020 went from -58% to +35% — direction flipped. The +35% suggests we may be counting whales twice: once via graph attribution (in `individuals.whale.corporate_connected`/`independent`) AND once via the grassroots residual (`weball.TTL_INDIV_CONTRIB - whale_total` may not perfectly subtract). Investigate: is the whale_total we subtract truly equal to the whale donations within TTL_INDIV_CONTRIB? Or is there a definitional gap?

- [ ] **Bulk validation — automate as part of Phase 4 production-readiness.** Add `validation_report.py` to a CI step or scheduled job. Set thresholds: median should stay <15%, within-±10% rate should stay >40%. Alert if regressions.

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
