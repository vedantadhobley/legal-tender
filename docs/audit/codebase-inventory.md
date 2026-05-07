# Codebase Inventory

**Date**: 2026-05-07
**Branch**: feature/professionalization
**Method**: structural scan via `find`, `grep`, `wc`. No code modification.

## Top-line numbers

| Metric | Count |
|---|---|
| Python files in `src/` | 60 |
| Python LOC in `src/` | 13,126 |
| Dagster assets registered | 25 |
| Dagster jobs | **1** (not 4 — see "stale claims" below) |
| Dagster schedules | 1 (`weekly_fec_refresh`, default STOPPED) |
| Test files | 0 (`tests/` directory does not exist) |
| Root-level dev scripts | 3 (`test_download.py`, `test_fec_schema.py`, `validate_schemas.py` — NOT pytest tests) |

## Source tree (`src/`)

| Module | Files | Lines | Purpose |
|---|---|---|---|
| `src/api/` | 5 | 456 | External API clients (Congress, FEC OpenFEC, Lobbying) |
| `src/assets/` | 1 | 90 | Asset registration (`__init__.py` only) |
| `src/assets/aggregation/` | 5 | **2,093** | candidate_funding (974), candidate_summaries, committee_summaries, donor_summaries |
| `src/assets/enrichment/` | 6 | 1,416 | classifications + wikidata_corporate_resolution |
| `src/assets/fec/` | 7 | 1,382 | Per-file parsers: cn, cm, ccl, pas2, oth, indiv |
| `src/assets/graph/` | 9 | 1,636 | Vertex + edge builders + political_money_graph |
| `src/assets/mapping/` | 2 | 364 | `member_fec_mapping` only |
| `src/assets/sync/` | 2 | 521 | `data_sync` only (one large asset, 7 phases inline) |
| `src/cli/` | 3 | **754** | `check_funding.py` (74), `pies_v3.py` (679) — see "dead/orphan code" |
| `src/data/` | 2 | 482 | `repository.py` |
| `src/jobs/` | 2 | 194 | `asset_jobs.py` defines 1 job |
| `src/models/` | 3 | 856 | `vertices.py`, `edges.py` |
| `src/rag/` | 3 | 690 | `wikidata_client.py`, `employer_normalization.py` |
| `src/resources/` | 3 | 591 | ArangoDBResource, EmbeddingResource |
| `src/schedules/` | 1 | 16 | One schedule, STOPPED by default |
| `src/utils/` | 7 | 1,459 | storage, fec_schema, arango_dump, arango_schema, memory, preflight |

**Largest files worth flagging:**
- `src/assets/aggregation/candidate_upstream.py` — **974 lines**. Contains the `candidate_funding` asset. This is the core funding-channels logic and needs Phase 0 deeper review for code quality.
- `src/cli/pies_v3.py` — **679 lines**. See "dead code" below.

## Asset inventory by group

| Group | Count | Assets |
|---|---|---|
| `sync` | 1 | data_sync |
| `fec` | 6 | cn, cm, ccl, pas2, oth, indiv |
| `graph` | 8 | donors, employers, contributed_to, transferred_to, affiliated_with, employed_by, spent_on, political_money_graph |
| `enrichment` | 5 | committee_classification, donor_classification, committee_receipts, canonical_employers, wikidata_corporate_resolution |
| `aggregation` | 4 | candidate_funding, candidate_summaries, committee_summaries, donor_summaries |
| `mapping` | 1 | member_fec_mapping |
| **Total** | **25** | |

## Dependency graph (from asset deps declarations)

```
data_sync
   ├── cn, cm, ccl, pas2, oth, indiv (parallel, depend on data_sync)
   │
   ├── donors ←── indiv
   ├── employers ←── donors
   ├── contributed_to ←── donors, cm, indiv
   ├── transferred_to ←── pas2, oth
   ├── affiliated_with ←── ccl
   ├── employed_by ←── donors, employers
   ├── spent_on ←── pas2  (broken — int/float type bug found in last run)
   ├── political_money_graph ← all graph edges
   │
   ├── canonical_employers ←── employers
   ├── committee_classification ←── contributed_to
   ├── donor_classification ←── donors
   ├── committee_receipts ←── indiv, pas2, oth, contributed_to
   ├── wikidata_corporate_resolution ←── canonical_employers, donors  (broken — see code-quality-findings)
   │
   ├── candidate_funding ←── committee_classification, committee_receipts,
   │                          affiliated_with, transferred_to, spent_on,
   │                          contributed_to, wikidata_corporate_resolution
   ├── candidate_summaries ←── committee_classification, donor_classification,
   │                            committee_receipts, affiliated_with,
   │                            contributed_to, transferred_to
   ├── committee_summaries ←── committee_classification, committee_receipts,
   │                            donor_classification, contributed_to, transferred_to
   └── donor_summaries ←── donor_classification, contributed_to, affiliated_with

member_fec_mapping  (mapping group, standalone — connects congress API to FEC IDs)
```

The dep graph has one issue worth flagging: `candidate_funding` has `wikidata_corporate_resolution` as a hard dep, but the code uses `db.has_collection()` defensively — meaning the dep is *enforced* by Dagster but the code is *graceful*. This is what made the last aggregation run pull in the broken Wikidata step even when we tried to skip it. **Action for Phase 3**: either remove the dep declaration, or stub the wikidata asset to fail gracefully.

## Jobs and schedules

**Jobs** (`src/jobs/asset_jobs.py`):
- `fec_pipeline_job` — uses `AssetSelection.all()`, runs every asset

**Schedules** (`src/schedules/__init__.py`):
- `weekly_fec_refresh` — Sundays 2 AM, `default_status=STOPPED` (manual start required)

## Stale claims found

- `src/__init__.py` docstring lists 4 jobs (`fec_pipeline_job`, `enrichment_job`, `aggregation_job`, `upstream_job`). **Only `fec_pipeline_job` actually exists.** The other 3 are documented but don't exist.
- Auto-memory's `MEMORY.md` says "33 Dagster assets, 6 jobs" — actual is **25 assets, 1 job**.

## Dead / orphan code candidates

These files have **no callers anywhere in `src/`**:

| File | LOC | Status |
|---|---|---|
| `src/cli/pies_v3.py` | 679 | Not imported anywhere. CLI runnable via `python -m src.cli.pies_v3`? Naming hints at Five-Pies V3 — predecessor to current `funding_channels`. **Likely dead** but verify before deletion. |
| `src/cli/check_funding.py` | 74 | Not imported anywhere. CLI utility, possibly dev-time only. Verify if used. |

Other suspicious patterns:
- `src/assets/__init__.py` line `_system` listed as an asset name in grep — likely a Dagster internal artifact, not user-defined.
- `src/cli/__init__.py` is just a comment (`# CLI modules`) — no exports, suggests CLI is meant to be invoked by file path not as a package.

## Root-level files

```
corporate_families.json    ← cache, possibly belongs in data/legal-tender/cache/
wikidata_cache.json        ← cache, possibly belongs in data/legal-tender/cache/
                              (currently TRACKED in git, contains 27 employers + 20 whales)
test_download.py           ← ad-hoc dev script, NOT pytest
test_fec_schema.py         ← ad-hoc dev script, NOT pytest
validate_schemas.py        ← ad-hoc dev script
query.sh                   ← arango query helper
start.sh                   ← stack startup helper
dagster.yaml               ← Dagster config, correct location
docker-compose.dev.yml     ← correct
docker-compose.yml         ← correct
requirements.txt           ← correct
workspace.yaml             ← Dagster workspace, correct
```

**Action items for later phases**:
- Move caches into `~/workspace/data/legal-tender/cache/` and gitignore them (Phase 3)
- Convert dev scripts to pytest tests OR move to `scripts/` (Phase 3 or 4)
- `scripts/` directory exists but is empty

## Test infrastructure: nonexistent

There is no `tests/` directory and no pytest configuration. The three `test_*.py` files at root are dev-time validation scripts (e.g., "run this to verify FEC headers parse correctly"), not unit/integration tests. **Phase 4 (production-readiness) should add a real test layer**; Phase 3 should write tests only when fixing a bug requires one to lock the fix in place.

## Configuration sprawl

Things that should ideally be centralized:

- **Active cycles list** — hardcoded as `["2020", "2022", "2024", "2026"]` in **17 files**. Should be one constant.
- **FEC per-election limits** — currently in `donors.py:7` as a comment block; not a typed constant. AQL queries hardcode the values.
- **Conduit patterns** — `["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]` in `candidate_upstream.py:74`. Reasonable place but could move to a config module.
- **Terminal types / passthrough types** — in `candidate_upstream.py:68-71`. Same comment.

## What this inventory does NOT cover

The next audit docs:
- `doc-accuracy.md` — content of each doc compared to current code
- `code-quality-findings.md` — pattern-level issues, idioms, refactoring opportunities
- `production-gaps.md` — CI/CD, secrets, monitoring, backup, etc.

Plus:
- AGENTS.md / CLAUDE.md status: **neither exists.** Adding it is a Phase 1 priority.
- `.claude/settings.local.json` exists (gitignored). Project-level Claude Code settings.
