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
- **External LLMs**: Qwen3.5-35B-A3B (chat) + Qwen3-Embedding-8B served from `joi` over Tailscale, OpenAI-compatible endpoints. Configured via `EMBEDDING_HOST` in `.env`.
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
- **Active election cycles**: `["2020", "2022", "2024", "2026"]`. Currently hardcoded across 17 files; centralize when touching this area (see @docs/todo.md).
- **Per-election limits** (FEC): `{2020: 2800, 2022: 2900, 2024: 3300, 2026: 3500}`. Lives in `src/assets/graph/donors.py:47-50`.
- **Funding channel terminology**: "funding channels" — NOT "Five Pies" (deprecated terminology, occasional residual references in deleted/orphan code).
- **Storage paths**: bind-mounted at `/storage` inside containers, mapped from `~/workspace/data/legal-tender/`. Subdirs `raw/` (FEC zips), `dumps/` (Arango JSONL), `cache/` (regeneratable).
- **Tailnet identifier**: do NOT commit it to public docs or example files. `.env` is gitignored; `.env.example` uses `<host>.<your-tailnet>.ts.net` placeholder.
- **Print vs log**: use `context.log.<level>` inside Dagster assets, `logger.<level>` (stdlib `logging`) elsewhere. Avoid bare `print()` — many existing call sites are anti-patterns to fix during Phase 3.

## Things to check before doing X

- **Editing FEC parsers** (`src/assets/fec/*.py`): read @docs/fec-data.md first. The parsers reference `~/workspace/data/legal-tender/raw/headers/` for FEC's official column headers, never hardcode field positions.
- **Editing graph assets** (`src/assets/graph/*.py`): read @docs/funding-channels.md to understand which edges feed what. Especially check `donors.py` for the per-election whale threshold.
- **Editing the funding-channels algorithm** (`src/assets/aggregation/candidate_upstream.py`): this is a 1,150-line monolith with one ~1,050-line function. Phase 3 candidate for refactoring into ~10 helpers. Don't expand it further; if adding logic, extract first.
- **Editing wikidata code** (`src/rag/wikidata_client.py`, `src/assets/enrichment/wikidata_resolution.py`): the client lacks negative caching, batched VALUES queries, and exponential backoff. **Don't trigger live runs at full scale until those are fixed** (see @docs/todo.md, "Wikidata client refactor"). Cache lives at repo-root `wikidata_cache.json` (intended to move to `cache/` in Phase 3).
- **Editing the cycles list**: 17 files have it hardcoded. Use `grep -rn '"2020", "2022", "2024", "2026"' src/` before editing one site to ensure consistency.
- **Adding a Dagster asset**: register it in `src/assets/__init__.py` AND in `src/__init__.py`'s `Definitions(...)`. Pick the right `group_name=` (sync, fec, graph, enrichment, aggregation, mapping). The Stop hook will prompt to update docs.
- **Bulk ArangoDB writes**: prefer `collection.import_bulk(batch, on_duplicate="replace")` over per-doc inserts. Batch sizes 10K-50K depending on doc shape. Memory limits in compose were tuned for this in commit `fb34a44`.

## Active state

- **Branch**: `feature/professionalization`. Master plan at @docs/plan.md.
- **Phase 0 audit**: complete. See @docs/audit/.
- **Last full data sync**: 2026-05-07 (17.2 GB, 4 cycles, 215M individual contributions parsed).
- **Known broken**:
  - `src/assets/graph/spent_on.py:202-203` — `MetadataValue.float()` receives an int. Crashes aggregation. One-line fix.
  - `wikidata_client._execute_sparql` — see above. Re-running wikidata at full scale will re-trigger the 14-hour grind we just escaped.
  - `candidate_funding`'s declared dep on `wikidata_corporate_resolution` — Dagster enforces it but the code is graceful. Easy to drop.
- **Known dead/orphan**:
  - `src/cli/pies_v3.py` (679 LOC) — predates funding_channels, no callers
  - `src/cli/check_funding.py` (74 LOC) — no callers

See @docs/todo.md for the full priority list.

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
