# Doc Accuracy Assessment

**Date**: 2026-05-07
**Method**: read each `.md`, compare claims to current code/runtime state

Score key:
- 🟢 **Accurate** — matches current state
- 🟡 **Stale** — was true once, no longer reflects reality
- 🔴 **Wrong / contradicted** — actively misleading vs. code
- ⚪ **Aspirational** — describes a target, not current state, but not labeled as such
- ➕ **Missing** — important content the doc should have but doesn't

## Summary table

| Doc | Lines | Score breakdown |
|---|---|---|
| `README.md` | 177 | 🟢 architecture intent; 🟡 collection counts, 🔴 jobs list, ⚪ query CLI, ➕ AGENTS.md ref |
| `docs/PIPELINE.md` | 433 | 🟢 channel definitions + algorithm; 🔴 jobs list, 🟡 validation results, 🟡 schema counts, 🔴 db list missing 2026 |
| `docs/FEC.md` | 328 | 🟢 FEC schema reference; 🔴 directory structure; ➕ no link from README |
| `docs/PIPELINE_FIXES.md` | 402 | 🟢 historical accuracy; 🟡 "current" framing now stale; 🟡 branch reference |
| `docs/SECOND_BRAIN.md` | 510 | 🟢 just written today |
| `docs/PLAN.md` | 113 | 🟢 just written today |

## README.md (177 lines)

### 🟢 Accurate
- Mission statement / funding-channels overview (lines 25-37)
- Mermaid architecture diagram structure (lines 41-66) — flow is right, see issue below
- Quick start commands (lines 14-23) — port 4300 / 4301 are correct

### 🔴 Wrong
- **Lines 70-77 — Jobs table claims 4 jobs.** Reality: only `fec_pipeline_job` exists. `enrichment_job`, `aggregation_job`, `upstream_job` are documented but never defined in `src/jobs/asset_jobs.py`. This is the single most-misleading claim in the docs and appears in 3 places (README, PIPELINE.md, src/__init__.py docstring).
- **Line 82-89 CLI example** — `dagster job execute -m src -j enrichment_job` won't work because that job doesn't exist.

### 🟡 Stale
- **Line 56 Mermaid arch:** `fec_2020 / fec_2022 / fec_2024` — missing fec_2026 (we now ingest 4 cycles)
- **Lines 95-103 Vertex collection counts:**
  - `donors ~5M` → actual after fresh data: **~1M** (per-election max-out gate is in effect)
  - `committees ~30K` → actual: **~78K** across 4 cycles (rough sum)
  - `corporate_families ~1K` → actual: **533** (close enough)
  - `canonical_employers ~75K` → actual: **100K**
- **Lines 105-113 Edge collection counts:**
  - `contributed_to ~5M` → actual: **6.4M** (good — grew with fresh data)
  - `transferred_to ~2M` → actual: **800K** (claimed too high)
  - `spent_on ~200K` → actual: **21K** (off by 10x — was probably aspirational or pre-bug)
  - `subsidiary_of ~500` → actual: **34** (pre-wikidata-population, way off)

### ⚪ Aspirational (not labeled)
- **Lines 144-152 Query CLI**: `./query.sh --candidate "Ted Cruz" --funding` — `query.sh` exists but I haven't verified the flag parsing works as advertised. This needs a runtime test.
- **Lines 117-127 AQL example**: References `donors/MUSK_ELON` as a key. Real donor keys are MD5 hashes of normalized name+employer. The example wouldn't run.

### ➕ Missing
- No reference to `AGENTS.md` (because it doesn't exist yet — Phase 1)
- No reference to `docs/PLAN.md` or `docs/audit/*` (just-created this session)
- No mention of the storage location (`~/workspace/data/legal-tender/`) or the `raw/dumps/cache` layout
- No mention of joi LLM dependency

## docs/PIPELINE.md (433 lines)

### 🟢 Accurate
- Lines 1-37 mission statement + funding channels: still the right model
- Lines 41-78 layer overview Mermaid: correct flow
- Lines 80-128 asset reference tables: accurate (assets exist with these names)
- Lines 160-212 funding channels algorithm: matches the candidate_upstream.py implementation
- Lines 218-266 output structure: matches what candidate_funding produces

### 🔴 Wrong
- **Lines 132-156 Jobs section.** Lists `enrichment_job`, `aggregation_job`, `upstream_job` with bash examples. **None of these exist.** Only `fec_pipeline_job` is defined.
- **Lines 336-341 Database list** — only lists `fec_2020 / fec_2022 / fec_2024`. We have `fec_2026` too.

### 🟡 Stale
- **Lines 269-280 Validation Results (dated "Feb 7, 2026")** — Harris/Trump/Cruz funding totals from a specific past run. Numbers may have shifted with fresh data. Should either be re-run or labeled as a historical snapshot.
- **Lines 165-185 Terminal/Passthrough type counts** (corporations 2,048, etc.) — these specific numbers depend on `committee_classification` runs. Claims feel dated. Worth re-counting after any classification re-run.
- **Lines 347-353 Vertex collection counts:**
  - `candidates 11,796` → actual: **18,823** in current state (or **34K** counting all four cycles deduplicated; verify)
  - `committees 30,840 classified` → actual: **78K total** but many unclassified
  - `donors 955,137` → actual: **1,023,215** (close, fresh data grew it)
- **Lines 357-364 Edge collection counts:**
  - `contributed_to 5,680,106` → actual: **6,466,955**
  - `transferred_to 654,530` → actual: **799,669**
  - `affiliated_with 22,808` → actual: **30,070**
  - `employed_by 147,810` → actual: **584,282** (off by 4x — significantly grown)
  - `spent_on 20,373` → actual: **21,462** (close)
- **Lines 376-389 Directory structure:** misses `~/workspace/data/legal-tender/{raw,dumps,cache}/` layout we just established

### ➕ Missing
- No reference to `wikidata_corporate_resolution` cache (`wikidata_cache.json`) or its current state
- No mention of FIX 12 / FIX 13 outcomes from `PIPELINE_FIXES.md` (cross-link)
- No discussion of the FEC 302→S3 redirect issue and how `should_download_file` handles it now (post-fix)

## docs/FEC.md (328 lines)

### 🟢 Accurate
- Lines 1-22 file purposes table — these are FEC's canonical descriptions, won't change
- Lines 52+ raw schema reference for cn / cm / ccl / pas2 / oth / indiv (commented-out csv tables) — accurate FEC bulk format

### 🔴 Wrong
- **Lines 28-46 Directory structure** — shows `legal-tender/data/fec/20xx/` (outdated). Reality is `~/workspace/data/legal-tender/raw/<cycle>/` (post-storage relocation, commit `284a84b`). This block predates the last week's reorg.

### 🟡 Stale
- **Line 4 download URL** points at the S3-Gov direct URL. While this works, the documented FEC entry point is `https://www.fec.gov/files/bulk-downloads/`. This is the URL the parser uses (with redirect-following — fix from `ddebc02`).

### ➕ Missing
- No mention that FEC URLs return 302 → S3 (the bug we just fixed in `should_download_file`)
- No documentation of the per-cycle structure inside ZIPs vs flat
- No discussion of the FEC re-publish daily cadence (FEC bulk files get re-published, so `Last-Modified` is reset even when content didn't change — affects sync logic)

## docs/PIPELINE_FIXES.md (402 lines)

### 🟢 Accurate (as a historical record)
- Documents fixes 1-13 with rationale and outcomes
- Captures architectural decisions made during the funding-channels-V3 work

### 🟡 Stale (as "current state" framing)
- **Line 4 "Last Updated: February 7, 2026"** — predates everything in this session (storage relocation, FEC sync fix, memory tuning, brain doc, audit start). Should be either updated or repurposed
- **Line 5 "Branch: feature/employer-enrichment"** — that branch is now merged into main
- The doc reads like a *current* status doc but is actually a *historical* one. Repurpose semantics: rename to `decisions.md` (per Phase 1 plan) and frame each entry as a dated decision

### Recommended action
This doc is valuable as a historical decision log. In Phase 1 it gets renamed to `decisions.md` with a clear "this is a historical record, ordered by date" framing — and new decisions get appended in real time during Phase 3+.

## docs/SECOND_BRAIN.md (510 lines)

🟢 Written this session, accurate at time of writing. Worth confirming during Phase 1 that the architecture diagram (lines 73-110) and the docker-compose YAML still match what we end up actually running in Phase 2.

## docs/PLAN.md (113 lines)

🟢 Written this session, accurate. The plan IS the current truth.

## Cross-cutting findings

1. **The "4 jobs" misclaim is documented in 3 places:** `README.md:70-77`, `docs/PIPELINE.md:132-156`, `src/__init__.py:7-12` (docstring). Single source-of-truth fix: define the missing 3 jobs (or remove the claims). Phase 1 stub-and-fix opportunity.

2. **Schema counts in 2 places** (`README.md` and `PIPELINE.md`) are both stale, and they don't even agree with each other (e.g., README says donors ~5M, PIPELINE.md says 955,137 — both wrong now, and the second was at least precise about what it was claiming). Phase 3 should compute counts dynamically and inject them into docs (or remove the counts entirely and link to the live ArangoDB UI).

3. **Storage location is documented inconsistently.** README doesn't mention it. PIPELINE.md shows old `data/fec/` paths. FEC.md shows old `legal-tender/data/fec/` paths. The new canonical location (`~/workspace/data/legal-tender/{raw,dumps,cache}/`) is documented only in `docker-compose.dev.yml` comments and `src/utils/storage.py` docstring. Phase 1 should consolidate this into a single `docs/storage.md` and link from everywhere.

4. **No top-level entry point for an agent.** AGENTS.md and CLAUDE.md don't exist. An agent (me) starting cold has to read README → PIPELINE → FEC → PIPELINE_FIXES → SECOND_BRAIN to get oriented (1,963 lines total). Phase 1 fixes this with a terse AGENTS.md that imports the others via `@docs/...`.

5. **Validation results in PIPELINE.md** are point-in-time snapshots presented as truth. Better pattern: a `docs/validation.md` that stores dated snapshots, plus a script that regenerates the current snapshot.

## Recommendations for Phase 1

| Existing doc | Phase 1 action |
|---|---|
| `README.md` | Rewrite as human-facing project pitch (flux-studio style). Strip jobs/collection counts (move counts to live UI; jobs come from a single source of truth in Phase 3). |
| `docs/PIPELINE.md` | Keep the funding-channels algorithm content. Strip the jobs list, fix database list, drop validation snapshots. Rename to `docs/pipeline.md`. |
| `docs/FEC.md` | Fix directory structure block. Add 302→S3 redirect note. Rename to `docs/fec-data.md`. |
| `docs/PIPELINE_FIXES.md` | Rename to `docs/decisions.md`. Reframe as dated decision log. Phase 3+ appends new entries. |
| `docs/SECOND_BRAIN.md` | Rename to `docs/second-brain.md`. Verify against Phase 2 actual deployment. |
| `docs/PLAN.md` | Stays as `docs/PLAN.md` (or rename `docs/plan.md` for consistency). |
| **NEW `AGENTS.md`** | Terse front door. Imports topic docs. The agent-first entry point. |
| **NEW `CLAUDE.md` → `AGENTS.md`** | Symlink for Claude Code compat (until Anthropic supports AGENTS.md natively). |
| **NEW `docs/architecture.md`** | High-level system view (extracted from PIPELINE.md introduction + README arch diagram). |
| **NEW `docs/funding-channels.md`** | The 5-channel model + algorithm (extracted from PIPELINE.md sections 4-5). |
| **NEW `docs/storage.md`** | The `~/workspace/data/legal-tender/{raw,dumps,cache}/` layout, env vars, dump strategy. |
| **NEW `docs/todo.md`** | Phase 0 findings as actionable items. |
