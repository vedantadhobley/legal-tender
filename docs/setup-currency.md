# Setup Currency Audit

> **Status**: stub. Phase 5 (continuous operation) owns the recurring audit. Phase 1 establishes the file.

This file tracks the *meta*-stack — the agentic-coding tooling we depend on — and whether our setup is current with state-of-practice. The agentic-coding ecosystem evolves fast (AGENTS.md spec, MCP servers, RAG patterns, Claude Code features); a periodic re-audit catches drift before our setup goes stale.

## Versions tracked

| Component | Version | Last verified | Source |
|---|---|---|---|
| AGENTS.md spec | (verify) | — | https://github.com/agentsmd/agents.md |
| Khoj | (verify) | — | https://github.com/khoj-ai/khoj/releases |
| basic-memory | (verify) | — | https://github.com/basicmachines-co/basic-memory/releases |
| Claude Code | opus-4-7 | 2026-05-07 | https://code.claude.com |
| obsidian-livesync | (verify) | — | https://github.com/vrtmrz/obsidian-livesync/releases |
| Quartz | (verify, optional) | — | https://github.com/jackyzha0/quartz/releases |

## Subscribed-to changes (manual or RSS-driven)

Once the brain stack is up, an RSS Feed Parser MCP can ingest these into the vault for me to surface in periodic digests. Until then, manual quarterly check.

- `github.com/agentsmd/agents.md` — releases + commits
- `github.com/khoj-ai/khoj` — releases
- `github.com/basicmachines-co/basic-memory` — releases
- `github.com/blackwell-systems/claudewatch` — drift-detection tool, integration candidate
- Anthropic Claude Code release notes
- Karpathy on X — for shifts in agentic-coding practice

## Audit cadence

- Quarterly minimum
- Triggered ad-hoc if any of the above ships a major release
- The audit is *not* Claude auditing Claude (rubber-stamping risk per published "audit-fix-loop" pattern). Use a different model — local Qwen on `joi`, a different Claude version, or a deliberate human pass — as the auditor.

## Last audit

**Not yet performed.** First audit due 2026-08-07 (quarterly cadence from Phase 1 establishment).

## Audit format

Each audit produces:
1. **Versions** — what's the current upstream version of each component, do we match?
2. **Spec changes** — any breaking or important changes in AGENTS.md / MCP / Claude Code that we should adopt or migrate against?
3. **New tools** — what shipped that's worth evaluating? (e.g., claudewatch arrived in 2026; we should adopt it)
4. **Drift findings** — if we have claudewatch wired, run `claudewatch scan` and review `gaps`/`suggest`/`drift_signal` output
5. **Action items** — append to @todo.md
6. **Next audit date** — set on a 90-day cadence

## Anti-patterns to avoid

From research (see @audit/ for full report):

- **Context rot**: AGENTS.md / docs grow large and dense, attention dilutes, the agent gets *worse* with more context. Pruning > adding. If a doc grows past ~500 lines, split it.
- **Rubber-stamp audit**: same model audits and fixes its own work. Always use a separate auditor.
- **Skill bloat**: adding new skill/tool/doc instead of fixing broken ones. The default failure mode.
- **Headline-number trust**: don't adopt a tool because its README claims great benchmarks. MemPalace was a 2026 case study — viral, then the benchmarks were shown to be inflated. Validate before integrating.
- **Cargo-cult AGENTS.md from viral templates**: e.g., "the Karpathy CLAUDE.md" floating around is Forrest Chang's distillation, not Karpathy's. Use viral examples as menus, not templates.
