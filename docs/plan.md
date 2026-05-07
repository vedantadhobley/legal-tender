# Plan: Professionalize legal-tender for production + agent-first operation

This is the master plan for transforming legal-tender from "personal project iteratively built with various AI models" into a production-ready codebase where Claude (or any agent) can operate confidently with the right context every session.

The Karpathy framing is the philosophical north star: **the docs in the repo are the LLM's working memory**, and the discipline is keeping them current alongside the code. Obsidian, Khoj, basic-memory MCP, etc. are tools that serve this — not the goal itself.

## Why a plan, not just "start fixing"

Three failure modes to avoid:

1. **Doc churn** — rewrite docs, fix code, rewrite docs again to reflect the fixes
2. **Lost context** — make changes, the brain doesn't capture them, next session starts cold
3. **Cargo cult** — apply Karpathy/Khoj patterns without grounding them in what's actually true about *this* codebase

Five phases, each with read-only or write-only operations on a specific layer. No phase undoes another's work.

## Phase 0 — Audit (read-only)

Catalog reality before changing anything. Deliverables under `docs/audit/`:

| File | Answers |
|---|---|
| `codebase-inventory.md` | File tree, line counts per module, full asset+job inventory, dep graph between assets, what's actually used vs dead code |
| `doc-accuracy.md` | For each existing doc: accurate / stale / missing-detail / contradicted-by-code |
| `code-quality-findings.md` | Things to refactor with current judgment (cycle list, wikidata gaps, hardcoded constants, ad-hoc patterns) |
| `production-gaps.md` | What blocks prod deploy: CI/CD, monitoring, secret handling, backup strategy, etc. |

**Phase 0 changes zero code and zero non-audit docs.** Read-only.

## Phase 1 — Shape the brain (write-only on structure)

Lock in the final doc structure. The reorg decision happens *once*, here.

- Final `docs/` layout (lowercase kebab-case, RAG-chunkable, ~200-400 lines per file with title + summary up top)
- `AGENTS.md` written as the agent's terse front door — imports topic docs via `@docs/...`, lists conventions
- `CLAUDE.md → AGENTS.md` symlink (Claude Code compat)
- All target doc files **stubbed** with title + 1-paragraph description. Empty bodies. Container committed; content fills in during Phase 3 alongside code changes
- `docs/todo.md` populated with Phase 0 findings as actionable items
- Claude Code Stop hook in `.claude/settings.json`: prompt if `src/` changed but `docs/` and `todo.md` didn't
- `README.md` rewritten as human-facing project pitch

**Phase 1 doesn't rewrite content.** Existing accurate content moves intact; stale content gets stubbed for Phase 3 to rewrite alongside the matching code fix. This is the key anti-churn move.

## Phase 2 — Integrate the brain

Wire up the brain stack so Phases 3+ happen *with the brain operational*.

- `~/workspace/obsidian/` stack live: Khoj + Open WebUI + brain-postgres + basic-memory MCP. Khoj configured with joi LLM endpoints
- Vault structure: `~/workspace/obsidian/brain/projects/legal-tender → ~/workspace/dev/legal-tender/docs/`
- `~/.claude/mcp.json` updated: `basic-memory` MCP server (write notes during sessions), `khoj` MCP server (search vault during sessions)
- Verification: in a session, search Khoj for an existing fact, write a new fact via basic-memory, confirm it appears in vault
- Optional: laptop-side native Obsidian + Syncthing

**Why Phase 2 before code fixes**: once the brain is wired, every Phase 3 change writes its own decision-log entry in real time. Otherwise decisions only exist in commit messages.

**Phase 2 changes zero code in legal-tender.**

## Phase 3 — Execute fixes with brain operational

Code work prioritized from Phase 0's findings. Each change pattern: *fix code → update matching topic doc (stubbed in Phase 1) → write a basic-memory note*.

Likely order:
1. **Wikidata client**: negative cache + batched SPARQL (VALUES clause) + backoff/circuit breaker. Then re-run wikidata_corporate_resolution with sane ceiling, populate cache
2. **`spent_on` int/float type bug** (discovered during last aggregation run — quick fix)
3. **Cycle list centralization**: single `ACTIVE_CYCLES` constant, replace 17 hardcoded copies
4. **Whatever else Phase 0 finds**, prioritized by impact

By end of Phase 3, the docs are real, current, and grounded in what the code actually does.

## Phase 4 — Production-readiness

From Phase 0's `production-gaps.md`. Likely includes:

- CI/CD: GitHub Actions for `~/workspace/prod/legal-tender/` pull-on-main + image-build flow
- Monitoring: integrate with `~/workspace/monitor/` stack (Prom metrics from Dagster, Loki for logs, Grafana dashboard for funding-channel quality)
- Backup: arango dump retention policy
- Secrets: proper handling for prod (.env handoff, sops/doppler, env vars from CI)
- Healthchecks tuning
- Versioning: semver tags, decisions.md as changelog source

## Phase 5 — Continuous operation

The brain is self-maintaining. Workflow:

- Every session: agent auto-reads AGENTS.md, has Khoj for search and basic-memory for note-writing
- Every code change: matching doc update (hook-enforced)
- todo.md is the persistent agent scratchpad
- Cross-project: found-footy, spin-cycle add their `docs/` to the same vault when ready

## Anti-churn invariants

| Phase | Reads | Writes | Doesn't |
|---|---|---|---|
| 0 Audit | code, docs | `docs/audit/*` only | change code or non-audit docs |
| 1 Shape | audit | `AGENTS.md`, stubbed `docs/*`, `todo.md`, hook config | rewrite doc content; fix code |
| 2 Integrate | nothing in legal-tender | `~/workspace/obsidian/`, `~/.claude/mcp.json` | touch legal-tender code |
| 3 Execute | audit, brain | `src/`, fills stubbed docs, brain notes | revise Phase 1's structure |
| 4 Prod-ready | everything | CI configs, monitoring, secrets | revise core code unless required |
| 5 Operate | continuous | continuous | n/a |

The key invariant: **structural decisions happen in Phases 0+1 and are not revisited.** Content fills in during Phase 3+ but doesn't change structure.

## Decisions agreed up front

- **Test coverage**: include as a Phase 0 audit finding; only write tests in Phase 3 if blocking a fix
- **Phase 3 aggression**: meaningful refactors are authorized when warranted (not just patches). The codebase was built incrementally with smaller models; current judgment may differ
- **Doc shape**: lowercase kebab-case file names, ~200-400 lines per topic file, RAG-friendly chunking
- **Branch**: `feature/professionalization` — all this work lands here, merges to main when production-ready

## Status

- **Phase 0**: in progress
- Phases 1-5: pending
