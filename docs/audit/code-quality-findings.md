# Code Quality Findings

**Date**: 2026-05-07
**Scope**: pattern-level issues, refactoring opportunities, code smells, and known-broken behavior across `src/`. Read-only audit; no fixes applied.

Findings are tagged by severity and grouped by theme. Each item names the file(s) and lines so Phase 3 can target it precisely.

Severity legend:
- 🔴 **Bug or guaranteed-stale code** — must fix
- 🟠 **Maintainability cliff** — meaningful refactor opportunity
- 🟡 **Smell** — fix opportunistically
- 🔵 **Convention drift** — style/consistency

## 1. Known broken behavior (🔴)

### 1a. `spent_on` int/float type error
**File**: `src/assets/graph/spent_on.py:202-203`

```python
"support_amount": MetadataValue.float(stats['support_amount']),
"oppose_amount": MetadataValue.float(stats['oppose_amount']),
```

`stats['support_amount']` is summed with integer cents (we saw `3787599698` int in the runtime crash). `MetadataValue.float()` raises a Dagster type-check error if passed an int. Fix: wrap with `float(...)` explicitly. Single-line fix; this is what crashed the aggregation run.

### 1b. Wikidata client has no negative cache, no batched queries, no backoff
**File**: `src/rag/wikidata_client.py:52-69` (`_execute_sparql`)

Three compounding issues:
- One SPARQL request per company name. With 5K target companies, this is 5,000 sequential requests. **Batched VALUES queries** (50 names per request) would reduce that to ~100.
- No persistent negative cache. Companies that returned no result get re-queried every run forever.
- No exponential backoff. On 429s/502s, we just sleep `RATE_LIMIT_DELAY=1.0` and continue. Three consecutive failures should trigger circuit breaker.

This is what made the 24-hour grind happen. See `wikidata_cache.json` (27 employer entries, 20 whales — should be ~5K + ~2K).

### 1c. `wikidata_corporate_resolution` is a hard dep of `candidate_funding`
**File**: `src/assets/aggregation/candidate_upstream.py:103`

```python
deps=["committee_classification", "committee_receipts", "affiliated_with",
      "transferred_to", "spent_on", "contributed_to", "wikidata_corporate_resolution"],
```

But the code is graceful (`db.has_collection('corporate_families')`). Dagster's enforcement of the dep means we can't run candidate_funding without first running wikidata_corporate_resolution. Remove the dep declaration (the code already handles missing data).

### 1d. The "4 jobs" misclaim
**Files**: `README.md:70-77`, `docs/PIPELINE.md:132-156`, `src/__init__.py` docstring lines 7-12

Three documents claim 4 jobs (`fec_pipeline_job`, `enrichment_job`, `aggregation_job`, `upstream_job`). Only `fec_pipeline_job` is actually defined in `src/jobs/asset_jobs.py`. Either define the missing 3 or remove the claims.

## 2. Dead code (🔴 if removed; ⚪ if kept)

### 2a. `src/cli/pies_v3.py` — 679 LOC
- No callers in `src/`
- Imports `arango.ArangoClient` directly, bypassing `src/resources/arango.py`
- Duplicates `TERMINAL_TYPES`, `PASSTHROUGH_TYPES`, `CONDUIT_PATTERNS` constants from `candidate_upstream.py:67-74`
- Predates the funding_channels asset model
- Has its own arg parsing, runs as `python -m src.cli.pies_v3`

**Action**: delete. Verify nothing in your local workflow depends on it first; if you do, document it before deletion.

### 2b. `src/cli/check_funding.py` — 74 LOC
- No callers in `src/`
- Looks like a quick CLI helper for inspecting candidates by FEC ID

**Action**: either move to `scripts/` (as a known dev utility) or delete. Verify before deleting.

### 2c. Removed assets that may still have leftover imports
**File**: `src/assets/__init__.py`

The branch merged into main (`feature/employer-enrichment`) **deleted** these enrichment files:
- `src/assets/enrichment/committee_financials.py`
- `src/assets/enrichment/corporate_hierarchy.py`
- `src/assets/enrichment/employer_cluster_integration.py`
- `src/assets/enrichment/employer_clustering.py`

Worth a `grep -r "committee_financials\|corporate_hierarchy\|employer_cluster" src/` to verify no orphan imports remain.

## 3. The `candidate_upstream.py` monolith (🟠)

**File**: `src/assets/aggregation/candidate_upstream.py` — **1,150 LOC**

Structure of the file:
- `CandidateFundingConfig` class (16 lines)
- `candidate_funding_asset` function — **~1,050 LOC of procedural code in a single function**

This is the largest single maintainability issue in the codebase. The function loads many collections, computes intermediate state, runs the two-phase trace algorithm, computes per-channel attribution, formats output — all inline.

Inside that function, you'd want extracted helpers:
- `_load_committee_classification_map(db)` — pulls committees + terminal_type into a dict
- `_load_corporate_families(db)` — pulls corporate_families with totals
- `_load_whale_corporate_links(db)` — pulls whale_to_company map
- `_load_donor_info(db, min_amount)` — loads donors above threshold
- `_phase_1_propagate_multipliers(...)` — the multi-hop trace
- `_phase_2_attribute_individuals(...)` — second pass
- `_compute_ie_channels(...)` — IE Support / IE Oppose
- `_compute_unaccounted(...)` — residual calculation
- `_format_output(...)` — final structure

**Phase 3 refactor candidate** (after the easier wins): break into ~10 focused functions, each ~50-100 lines. Test the boundary by running `candidate_funding` before and after, comparing outputs byte-for-byte. The algorithm doesn't change; just the shape.

## 4. Configuration sprawl (🟠)

### 4a. Active cycles list — 17 hardcoded copies
Mentioned in `codebase-inventory.md`. The literal `["2020", "2022", "2024", "2026"]` appears in 17 files. Single `ACTIVE_CYCLES` constant in `src/utils/storage.py` or a new `src/config.py` would consolidate.

### 4b. Per-election limits live in `donors.py:47-50`
```python
PER_ELECTION_LIMITS = {
    "2020": 2800,
    "2022": 2900,
    "2024": 3300,
    "2026": 3500,
}
```

Referenced via comments in 4 other files (`candidate_upstream.py`, `committee_receipts.py`, `PIPELINE.md`, `PIPELINE_FIXES.md`) but not imported. The constant should live in `src/config.py` and be imported wherever needed.

### 4c. Terminal/passthrough types
**File**: `src/assets/aggregation/candidate_upstream.py:67-74`

```python
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}
PASSTHROUGH_TYPES = {"passthrough", "unknown", "super_pac_unclassified"}
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]
```

Duplicated in `pies_v3.py:21-28` (with a slightly different `PASSTHROUGH_TYPES` — pies_v3 lacks `super_pac_unclassified`). If pies_v3 is kept, this divergence is a real bug. Move to `src/config.py`.

### 4d. Inconsistent progress-logging intervals
**Files**: `pas2.py:130` (% 250000), `contributed_to.py:272` (% 50000), `arango_dump.py:248` (% 500000), `cli/pies_v3.py:187` (max_iterations=10000)

Each asset picks its own log interval. Should be a single `LOG_PROGRESS_EVERY = 100_000` (or whatever) constant, used consistently.

## 5. Logging and observability (🟠)

### 5a. 158 `print()` calls in `src/`
Should be `logger.<level>` or `context.log.<level>`. Significant clusters:
- `src/utils/preflight.py` — uses `print()` exclusively. No Dagster context here, but should use `logging` module for structured output
- `src/utils/arango_schema.py` — uses `print()` instead of logger
- Various assets use `print()` mid-loop, mixed with `context.log.info()` calls

This means many warnings/errors are invisible in Dagster's UI logs and sent to stdout instead. **Concrete impact**: the 14-hour Wikidata loop's per-call warnings showed up in the docker exec stdout but not as Dagster step logs.

### 5b. Bare `except Exception:` patterns swallow errors
**Files**: 6 locations
- `src/utils/storage.py:209` — disk_usage check, OK to swallow
- `src/data/repository.py:212` — JSON load fallback, OK
- `src/assets/aggregation/committee_summaries.py:221` — likely metadata-emit; should be `except DagsterLogException` or specific
- `src/assets/aggregation/donor_summaries.py:214` — same
- `src/assets/aggregation/candidate_summaries.py:412` — same
- `src/assets/enrichment/canonical_employers.py:243` — needs investigation

The three `*_summaries.py` files all swallow exceptions around their final metadata writes. If those calls fail, the asset reports success but metadata is missing. Should at minimum log the swallowed exception.

### 5c. 51 total `except` clauses
Most are reasonable but worth a Phase 3 sweep with this question: *"is each one specific to a known exception, or is it catching too broadly?"*

## 6. ArangoDB resource boilerplate (🟠)

### 6a. 31 `arango.get_client()` / `get_database()` / `get_collection()` calls scattered
Pattern repeated across 17 files:

```python
with arango.get_client() as client:
    db = arango.get_database(client, f"fec_{cycle}")
    collection = arango.get_collection(db, "cn")
    # do work
```

A context manager helper would DRY this:

```python
with arango.scoped("fec_2024", ["cn", "cm"]) as (db, cn, cm):
    # do work
```

Estimated 100+ lines saved across the codebase, plus consistent error handling.

### 6b. 75 collection-management calls
`collection.truncate()`, `db.create_collection()`, `collection.add_persistent_index()` appear 75 times across asset files. Same idea — these patterns are mechanical and repeat. A schema-aware helper could:
- Create the collection with the right vertex/edge type
- Apply standard indexes
- Optionally truncate before write

### 6c. 88 AQL queries inline
Most assets embed AQL as string literals. Some are simple lookups (good to keep inline); some are 30-50 line procedural traversals (should live in a query library).

**Phase 3 candidate**: a `src/queries/` module with named, parameterized AQL — `queries.get_committee_receipts(db, cycle)`, `queries.find_whale_donors(db, cmte_id, cycle)`, etc. Centralizes query maintenance and makes it possible to optimize them in one place.

## 7. The `wikidata_resolution.py` asset (🟠)

**File**: `src/assets/enrichment/wikidata_resolution.py` — 433 LOC

Beyond the client-side issues (1b above), the asset itself has issues:

- `WIKIDATA_CACHE_PATH = os.environ.get("WIKIDATA_CACHE_PATH", "/workspace/wikidata_cache.json")` — hardcoded `/workspace` path. Won't work outside the container without env override.
- No incremental update — re-runs always start from `_load_cache()` and append. No way to "process only the next 500 employers."
- Phase 1 (employers) and Phase 2 (whales) are sequential within the asset. Could parallelize.
- Saves cache only at the end of the run. If the run crashes (as it did), all the in-flight cache work is lost.

**Phase 3 candidate** alongside the wikidata_client fixes: incremental save (every N queries flush to cache), checkpoint-resume, parallel phase execution.

## 8. Type hints / typing (🔵)

Spot-check shows mixed type hint coverage:
- Most asset functions have parameter types
- Many internal helpers don't
- `Optional[]` used inconsistently with `| None` (newer Python style)
- No `mypy` or `pyright` configuration in repo

**Phase 4 candidate**: add `mypy` config + CI check. Don't enforce in Phase 3.

## 9. Tests (🟠)

There is no `tests/` directory. The three `test_*.py` files at root are dev-time validation scripts, not unit tests:

- `test_download.py` — hits FEC live to verify download works
- `test_fec_schema.py` — verifies header CSV parses correctly
- `validate_schemas.py` — checks parser fields match FEC headers

These should either:
- Be moved to `scripts/` and renamed (they're dev utilities, not tests), OR
- Be converted to real pytest tests with mocked FEC fixtures

**Phase 4 candidate** (production-readiness): build out a real test layer. Phase 3 should write tests *only when* they're needed to lock in a fix.

## 10. The 0 TODO comments observation (🟡)

Surprisingly: zero `TODO`, `FIXME`, `HACK`, `XXX` comments in `src/`. Either the code is unusually clean, or — more likely given other patterns — the convention is to NOT leave breadcrumbs in code.

This is a culture change for Phase 3 onwards: when something is hacky-but-shipping, leave a `# TODO(person/issue): why this is here` comment so it can be searched and tracked. Pairs with `docs/todo.md` for the bigger items.

## 11. Caches in repo root (🔵)

**Files**: `wikidata_cache.json` (13KB, tracked in git), `corporate_families.json` (size unknown, tracked in git)

These are caches. They:
- Don't belong in git (every run modifies them, creating noise commits)
- Should live in `~/workspace/data/legal-tender/cache/` (the new layout we just established)
- Currently have a hardcoded `/workspace/wikidata_cache.json` path inside the container

**Phase 3 action**: move to cache dir, update env var paths, gitignore them in a single commit.

## 12. Models module (🔵)

**Files**: `src/models/edges.py` (451 LOC), `src/models/vertices.py` (~400 LOC)

Haven't deeply audited but: 856 LOC of models is large for the domain (5 vertex types, 6 edge types). Some of this is likely schema definitions that could move to a leaner shape (TypedDict, dataclass, or pydantic). Phase 4 candidate, not urgent.

## Summary: Phase 3 priority list

In rough order of impact-per-effort:

1. ⭐ **Wikidata fixes** (1b, 7) — unblocks corporate attribution; needed for full funding-channels output
2. ⭐ **`spent_on` int/float fix** (1a) — single line, unblocks aggregation
3. ⭐ **Remove `wikidata_corporate_resolution` from candidate_funding deps** (1c) — single line
4. **Centralize cycles + per-election limits** (4a, 4b) — `src/config.py`, ~17 file edits
5. **Move caches out of repo** (11) — gitignore + path updates
6. **Replace `print()` calls with logging** (5a) — bulk find-replace
7. **Delete `pies_v3.py` after verifying** (2a)
8. **Refactor `candidate_upstream.py` monolith** (3) — biggest maintainability win, but also biggest scope
9. **Fix the "4 jobs" misclaim** — define the 3 missing jobs OR remove the claims everywhere (1d)
10. **Bare `except` audit** (5b) — opportunistic
11. **ArangoDB scoped helper** (6a) — DRY win across 31 sites
12. **Tests + types + models cleanup** — Phase 4

Items 1-3 are bug fixes that unblock the pipeline. Items 4-7 are clean wins (small effort, broad benefit). Items 8-12 are scope choices.
