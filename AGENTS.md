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
- **Last full data sync**: 2026-05-07 (17.2 GB, 4 cycles, 215M individual contributions parsed).
- **Last bulk validation**: median |delta| vs FEC `weball.TTL_RECEIPTS` = 2.5%; 62% within ±5%; 73% within ±10%. Run via `docker exec ... python scripts/validation_report.py`.
- **Next session plan** (queued, see @docs/todo.md "Next session"):
  1. Capture baseline at `docs/audit/baseline-2026-05-12.md`
  2. Extend May-9 parent-org inheritance: when N committees share `CONNECTED_ORG_NM`, most-specific terminal_type wins. Fixes NAR Congressional Fund (super_pac_unclassified → trade_association), NRA ILA, Club for Growth Action splits.
  3. Stop treating `super_pac_unclassified` as terminal — trace through their `contributed_to` / `transferred_to` edges. Target case: Club for Growth Action's $263.5M moves from unaccounted to attributed.
  4. Then: thinnest CLI `python view_candidate.py "CRUZ, TED"` rendering `funding_channels.by_organization` + trace path.
- **Known dead/orphan still in tree**:
  - `src/cli/pies_v3.py` (679 LOC) — confirmed no callers; delete in dedicated commit
  - `src/cli/check_funding.py` (74 LOC) — no callers; delete or move to `scripts/`
  - `src/api/lobbying_api.py` (63 LOC) — aspirational; keep if the lobbying-integration plan is live, delete otherwise

See @docs/todo.md for the full priority list and @docs/audit/hardcodes-2026-05-11-pm.md for the latest hardcode/dead-code verdicts.

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
