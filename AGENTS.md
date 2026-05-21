# Legal Tender — Agent Context

Political campaign finance tracing pipeline. Dagster + ArangoDB. Traces money from source (corporations, unions, individuals) to candidates through PAC transfer chains, organized into 5 funding channels.

This file is your front door. Read it first; follow the imports below for deeper detail.

## Run

```bash
docker compose -f docker-compose.dev.yml up -d
```

- Dagster UI: http://localhost:4300
- ArangoDB UI: http://localhost:4301 (root / ltpass)

## Stack

- **Orchestrator**: Dagster (assets, schedules, jobs)
- **Database**: ArangoDB (multi-model: documents + named graph)
- **External LLMs**: `Qwen3.5-122B-A10B` (chat) + `Qwen3-Embedding-8B-Q4_K_M` served from `joi` over Tailscale, OpenAI-compatible endpoints. Configured via `EMBEDDING_HOST` in `.env`. Live model IDs may shift; check `curl http://joi.<tailnet>:3101/v1/models` before relying on them.
- **Anthropic API**: for me (Claude Code). Independent of `joi`.

## Where to look first

- @docs/architecture.md — high-level system design, layer diagram
- @docs/pipeline.md — the 5-layer ETL detail (sync → parse → graph → enrich → aggregate)
- @docs/funding-channels.md — the 5-channel model and the trace algorithm
- @docs/fec-data.md — FEC bulk file shapes and reference
- @docs/storage.md — `~/workspace/data/legal-tender/{raw,dumps,cache}/` layout
- @docs/operations.md — runbook (how to do common ops tasks)
- @docs/decisions.md — historical decision log (formerly PIPELINE_FIXES.md)
- @docs/todo.md — active TODOs, open issues, deferred work
- @docs/validation.md — validation methodology, four-gate contract, current numbers
- @docs/data-quality.md — how to read `individuals.data_quality` (detail_coverage, primary_source) and what it catches
- @docs/second-brain.md — self-hosted second brain stack design
- @docs/setup-currency.md — meta-tooling currency tracking
- @docs/plan.md — the master plan for this professionalization effort
- @docs/audit/ — point-in-time codebase + doc + production audits (Phase 0)

## Conventions

- **Commits**: no `Co-Authored-By: Claude` trailer. Lowercase prefix style (`chore:`, `fix:`, `feat:`, `docs:`, `perf:`).
- **Active election cycles**: `["2020", "2022", "2024", "2026"]`. Currently hardcoded across 21 files; centralize when touching this area (see @docs/todo.md).
- **Per-election limits** (FEC): `{2020: 2800, 2022: 2900, 2024: 3300, 2026: 3500}`. Lives in `src/assets/graph/donors.py:47-50`.
- **Funding channel terminology**: "funding channels" — NOT "Five Pies" (deprecated terminology, occasional residual references in deleted/orphan code).
- **Storage paths**: bind-mounted at `/storage` inside containers, mapped from `~/workspace/data/legal-tender/`. Subdirs `raw/` (FEC zips), `dumps/` (Arango JSONL), `cache/` (regeneratable).
- **Tailnet identifier**: do NOT commit it to public docs or example files. `.env` is gitignored; `.env.example` uses `<host>.<your-tailnet>.ts.net` placeholder.
- **Print vs log**: use `context.log.<level>` inside Dagster assets, `logger.<level>` (stdlib `logging`) elsewhere. Avoid bare `print()` — many existing call sites are anti-patterns to fix during Phase 3.

## Things to check before doing X

- **Editing FEC parsers** (`src/assets/fec/*.py`): read @docs/fec-data.md first. The parsers reference `~/workspace/data/legal-tender/raw/headers/` for FEC's official column headers, never hardcode field positions.
- **Editing graph assets** (`src/assets/graph/*.py`): read @docs/funding-channels.md to understand which edges feed what. Especially check `donors.py` for the per-election whale threshold.
- **Editing the funding-channels algorithm** (`src/assets/aggregation/candidate_upstream.py`): 1,263 LOC after the May 2026 refactor (module-level helpers + ProcessPool worker). The orchestration function is ~100 LOC; the trace/attribution helpers are extracted. Don't expand the orchestration body — add new module-level helpers and call them.
- **Editing corporate resolution** (`src/rag/wikidata_resolver.py`, `src/rag/whale_resolver.py`, `src/rag/name_match.py`, `src/assets/enrichment/wikidata_resolution.py`): the resolver pipeline is reconci.link → GLEIF → not_found, with multi-signal corroboration for low-confidence matches. No description-filter chain. Cache lives at `~/workspace/data/legal-tender/cache/wikidata.json`. Pytest in `tests/test_name_match.py`, `tests/test_wikidata_resolver.py`, `tests/test_whale_resolver.py` (~68 tests, 25s).
- **Editing the cycles list**: 17 files have it hardcoded. Use `grep -rn '"2020", "2022", "2024", "2026"' src/` before editing one site to ensure consistency.
- **Adding a Dagster asset**: register it in `src/assets/__init__.py` AND in `src/__init__.py`'s `Definitions(...)`. Pick the right `group_name=` (sync, fec, graph, enrichment, aggregation, mapping). The Stop hook will prompt to update docs.
- **Bulk ArangoDB writes**: prefer `collection.import_bulk(batch, on_duplicate="replace")` over per-doc inserts. Batch sizes 10K-50K depending on doc shape. Memory limits in compose were tuned for this in commit `fb34a44`.

## Active state

- **Branch**: `feature/professionalization`. Master plan at @docs/plan.md.
- **Phase 0 audit**: complete. See @docs/audit/.
- **Last full data sync**: 2026-05-16 (resync after the 2026-05-07 baseline; pulled Q1 2026 itemized records — `fec_2026.indiv` grew from 21M→27M). Weekly schedule (`weekly_fec_refresh`, default RUNNING since 2026-05-16) re-fires Sundays 2 AM Eastern.
- **Last bulk validation** (post-resync 2026-05-16): median |Δ| vs FEC `weball.TTL_RECEIPTS` = **2.3%**; 66% within ±5%; 78% within ±10%; 89% within ±25%. ⚠ Tautological for fec_summary-fallback candidates — see @docs/validation.md "validation methodology caveat" and @docs/data-quality.md.
- **Inspect a candidate**: `docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py "<name or CAND_ID>"`. Flags: `--cycle 2024`, `--top 15`. Renders all 5 channels + by_organization + `data_quality` banner (when detail coverage < 100%).
- **Deeper analysis scripts**:
  - `scripts/donor_network_overlap.py <CAND_ID>` — surfaces top-K committees the candidate's whale pool also funds; cluster-naming is the reader's job. See @docs/data-quality.md.
  - `scripts/race_signature.py --state NJ --district 12 --year 2026 --party DEM` — field-wide donor signatures for a race.
- **Four-gate validation contract** (every fix touching data flow runs all four before commit):
  1. Bulk median ≤5% — `scripts/validation_report.py`
  2. Named-candidate diff for Cruz/Trump/Harris/Bacon/Sanders — `scripts/view_candidate.py "<id>"` vs `docs/audit/baseline-2026-05-12.md`
  3. Target case — explicit per-fix pass/fail
  4. Pytest 65/65 — `pytest tests/ -q`
  See @docs/validation.md for thresholds and how-to.
- **Recent completed work** (May 14-16):
  - **Same-entity merge** for corporate_families (Phase 3.5 in `wikidata_corporate_resolution`) — Pan Am Systems + Railways → $923M one Mellon entity. Plus Bloomberg, Marvel trio (union-find), DreamWorks, Rocket, Hilton, Coca-Cola, Capitol. Commit `fd2c7d1`.
  - **`donor_detail_coverage` + `primary_source`** in candidate_funding output. Surfaces when `fec_summary` fallback fired so consumers can't silently lie about whale/grassroots splits. Commit `160c1eb`.
  - **Weekly schedule default-ON** (was STOPPED for 5+ weeks; gitignore was also hiding the schedule file from review). Commit `160c1eb`.
  - **Resync 2026-05-16** — fresh Q1 2026 indiv records, esp. for newly-filed candidates like NJ-12's Hamawy who went from 0 records to 513.
  - **Memory caps** on legal-tender-dev-arango (`32 GiB`) + RocksDB block-cache trim (`32→12 GiB`) — companion to long-exposure 347c8cd; the host had OOM-killed arangod at 40 GB on 2026-05-16. Commits `9b1fc65` + `a03ea8b`.
  - **Centralized constants**: `ACTIVE_CYCLES` + `PER_ELECTION_LIMITS` → `src/config.py` (was duplicated across 21 files). Commit `2302859`.
  - **Reusable scripts**: `donor_network_overlap.py` + `race_signature.py`.
  - **Dead code removed**: `pies_v3.py` + `check_funding.py` (753 LOC, commit `576bee0`); `compute_normalized_key` + `find_potential_matches` (commit `7d6ae79`).
- **Next session targets** (see @docs/todo.md "This week"):
  - **EARMARKED-memo-share conduit detection** — replace the `CONDUIT_PATTERNS` substring list with a structural rule (committee's incoming indiv records >80% `EARMARKED FOR` → passthrough). Kills the last donor-name substring classifier. ~75-90 min including re-materialization.
  - **Phase 1 rule for IE-only Super PACs without ORG_TP** → `super_pac_unclassified` regardless of name-cluster inheritance. Catches JDPAC which currently gets `passthrough`.
  - **Audit script** for "same-shape committee classification disagreements" (committees with identical CMTE_TP / ORG_TP / connected-org but different terminal_types).
  - **Generic-string rejection in `name_match.py`** for "TARGETED VICTORY" / "PRESIDENT" / federal-agency leakage in `by_organization`.
- **UI work** queued for this/next weekend — web view consuming `funding_channels.aggregate`; the two CLI scripts above are the structural prototype.
- **Known dead/orphan still in tree**:
  - `src/api/lobbying_api.py` (63 LOC) — aspirational; keep if the lobbying-integration plan is live, delete otherwise. (pies_v3.py + check_funding.py + embedding.py + election_api.py all gone.)

See @docs/todo.md for the full priority list, @docs/validation.md for current validation state, @docs/data-quality.md for the data-coverage interpretation rules, and @docs/audit/hardcodes-2026-05-11-pm.md for the latest hardcode/dead-code verdicts.

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
