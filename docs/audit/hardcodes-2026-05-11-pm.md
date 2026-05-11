# Hardcode + dead-code audit, afternoon of 2026-05-11

Follow-up to the morning audit (`hardcodes-2026-05-11.md`). That one was
written before the resolver simplification shipped. This one is the
post-simplification picture: what's left, what's still dead, what's
new since.

Scope: `src/rag/` and direct consumers; broader codebase only where
something stands out. Skips items already on `docs/todo.md` unless their
status changed.

Verdict legend:
- **Keep**: legitimate reference data / parameter / interface
- **Trim**: stale entries inside an otherwise-legit constant
- **Delete**: dead code or band-aid that can come out today
- **Defer**: real issue but bigger refactor, leave a TODO

---

## 1. Recently-shipped resolver layer

### `src/rag/whale_resolver.py` (new this session)

| Item | LOC | Verdict | Note |
|---|---|---|---|
| `_PERSON_TO_COMPANY_PROPS` (7 entries) | 90-99 | **Keep** | Wikidata property → relationship-name mapping. Each entry is principled (subject→object schema). Adding properties when discovered is the right shape. |
| `_RELATIONSHIP_PRIORITY` (7 entries) | 102-110 | **Keep** | Sort key for "which relationship wins for primary_company". Founder > CEO > owns > board > employed. |
| `_CEO_POSITION_QIDS` (2 entries) | 92-95 | **Keep** | Distinct CEO position Q-ids on Wikidata. Reference data, not a band-aid. |
| `_FOUNDER_POSITION_QIDS` (empty body) | 98-103 | **Delete** | I added a placeholder set with only comments — no values used anywhere. Pure dead code I left behind. |

### `src/rag/wikidata_resolver.py`

| Item | Verdict | Note |
|---|---|---|
| `HIGH_CONFIDENCE_THRESHOLD = 70.0`, `LOW_CONFIDENCE_THRESHOLD = 40.0` | **Keep** | Calibrated parameters with rationale documented inline. |
| Accept-decision logic | **Keep** | Principled (high-score + short-input guard, low-score + corroboration). |

### `src/rag/name_match.py`

| Item | Verdict | Note |
|---|---|---|
| `_STOPWORDS` (35-ish English/legal stopwords) | **Keep** | Generic linguistic data. Documented why each kind is included. |
| `acronym_match` tries with-and-without-stopwords | **Keep** | Catches both "WH" and "BCG" with one rule, no per-case hardcoding. |

---

## 2. `src/rag/wikidata_client.py` — leftovers

`wikidata_client.py` is now 150 LOC (down from 416). Remaining items:

| Function | Verdict | Note |
|---|---|---|
| `_execute_rest` | **Keep** | HTTP transport with retry+circuit breaker. Used by `_entity_data`. |
| `_entity_data` | **Keep** | Used by `whale_resolver._enrich_companies` and `_resolve_one`. |
| `_claim_qid` | **Keep** | Used by `whale_resolver._extract_company_qids`. |
| `_wbsearchentities` | **Delete** | NO callers in current code. Used to back the legacy whale path that just got deleted. |
| `WikidataCircuitOpen`, `reset_circuit_breaker` | **Keep** | Asset (`wikidata_resolution.py`) calls `reset_circuit_breaker()` before each run. |
| Docstring references to whale path in `wikidata_client.py` line 5 | **Keep** | Accurate description of what the module is now used for. |

### Dead in `wikidata_reconci.py`

| Item | Verdict | Note |
|---|---|---|
| `ORG_TYPE_QID = "Q43229"` (line 70) | **Delete** | No references after removing the type filter from default callers. The 25-line comment justifying it (lines 52-69) refers to a `wikidata_ontology` module that no longer exists. |
| Docstring `default Q43229 (organization)` claim line 169 | **Trim** | The default is `None`, not Q43229. |
| Type-hint comment `[{"id": "Q43229", "name": "organization"}, ...]` line 86 | **Keep** | Accurate example of the API's payload shape. |

---

## 3. `src/rag/employer_normalization.py`

| Item | LOC | Verdict | Note |
|---|---|---|---|
| `LEGAL_SUFFIXES` (~28 regexes) | 20-49 | **Keep** | Legitimate reference data: legal-form suffixes worldwide. |
| `ABBREVIATIONS` (~17 entries) | 52-69 | **Keep** | Generic abbreviation expansion. Same shape as a real dictionary. |
| `NON_EMPLOYERS` (~40 entries) | 78-94 | **Keep** | Reference data (RETIRED, SELF-EMPLOYED, INFORMATION REQUESTED, etc.). |
| `EMPLOYER_FAMILY_ALIASES` (2 entries) | 110-115 | **Defer** | Earlier audit said "move to YAML". Today only 2 entries (ADELSON CLINIC, ULINE INDUSTRIES). Below the noise threshold for now — but worth either deleting (the new resolver may handle these via reconci) or moving to a data file if we're going to keep growing it. |
| `CAMPAIGN_COMMITTEE_MARKERS` (8 patterns) | 123-127 | **Keep** | Reference data: campaign-committee text patterns leaking into FEC employer field. |
| `compute_normalized_key`, `find_potential_matches` | 219-291 | **Defer** | Exported from `src/rag/__init__.py` but no callers anywhere in `src/`. Either dead or held for a planned consumer. Recommend grepping for external imports; if zero, delete. |

---

## 4. Asset-level cache migration leftovers

`src/assets/enrichment/wikidata_resolution.py`:

| Item | Verdict | Note |
|---|---|---|
| `LEGACY_CACHE_PATH = "/workspace/wikidata_cache.json"` + fallback-read block (lines 82-118) | **Defer** | Migration helper from May 2026. Real one-time use during the storage relocation; now everyone's cache is at the new path. Safe to delete after one more clean run on every dev machine. Risk: if anyone reverts to a pre-migration checkout the cache reload silently empties. Trim with a 1-month sunset. |

---

## 5. Configuration sprawl (unchanged from morning audit)

Still in place, still not fixed:

| Item | Sites | Verdict |
|---|---|---|
| `ACTIVE_CYCLES = ["2020", "2022", "2024", "2026"]` literal | 21 files | **Defer** — `todo.md` already tracks this. Move to `src/config.py`. |
| `TERMINAL_TYPES` / `PASSTHROUGH_TYPES` / `CONDUIT_PATTERNS` | duplicated in `pies_v3.py` and `candidate_upstream.py` | **Delete `pies_v3.py` copies** by deleting pies_v3.py (next item) |
| `PER_ELECTION_LIMITS` | `donors.py:47-50`, referenced via comments elsewhere | **Defer** — move to `src/config.py` |

---

## 6. Confirmed dead modules (per earlier audits, still present)

| Module | LOC | Verdict | Confidence |
|---|---|---|---|
| `src/cli/pies_v3.py` | 679 | **Delete** | High — no callers found anywhere. Predates funding_channels. |
| `src/cli/check_funding.py` | 74 | **Delete** | High — no callers. Could move to `scripts/` if you want it for debugging. |
| `test_download.py`, `test_fec_schema.py`, `validate_schemas.py` (repo root) | small | **Move** to `scripts/` | These aren't pytest tests. |

### New finding: `EmbeddingResource` is dead

`src/resources/embedding.py` (320 LOC) defines an `EmbeddingResource`
Dagster resource. It's wired into `Definitions(...)` in `src/__init__.py:119`
as `"embedding": EmbeddingResource()`. But:

```bash
grep -rn "embedding" src/assets/ src/jobs/
# (no output)
```

**No asset declares the resource as a dependency. No asset calls
`embedding.embed_*`.** The whole module is wired but unused. Costs:

- 320 LOC of code that runs at import time (initializes an HTTP client at
  `joi.<tailnet>:3102` or similar)
- A Dagster resource slot consumed for nothing
- A stale facade that suggests the project has embedding capabilities
  when it actually doesn't use them

Verdict: **Delete** `src/resources/embedding.py`, drop the import +
`"embedding": EmbeddingResource()` line from `src/__init__.py`, drop
the `EmbeddingResource` re-export from `src/resources/__init__.py`.
Net ~-330 LOC. The morning audit (`code-quality-findings.md`) didn't
catch this — it's an addition.

If embeddings come back (e.g. Khoj integration writes assets that need
to embed text), reintroduce via the brain stack which already runs an
embedding endpoint.

### New finding: `src/api/election_api.py`, `src/api/lobbying_api.py`

| Module | LOC | External refs | Verdict |
|---|---|---|---|
| `election_api.py` | 67 | 0 | **Delete** — also documented as TODO in `src/api/__init__.py` ("for additional donor/campaign finance and committee data"). Aspirational, never wired. |
| `lobbying_api.py` | 63 | 0 (referenced only in `docs/lobbying-integration.md` and `docs/decisions.md` aspirationally) | **Defer** — there's a documented lobbying-integration plan. If that plan is still on the roadmap, keep the file; if not, delete. |

---

## 7. Quick concrete deletions for today

Safe to ship in one commit, ~700 LOC removed:

1. **`src/rag/whale_resolver.py`**: drop the empty `_FOUNDER_POSITION_QIDS` set (5 LOC).
2. **`src/rag/wikidata_client.py`**: delete `_wbsearchentities` (~20 LOC, no callers).
3. **`src/rag/wikidata_reconci.py`**: delete `ORG_TYPE_QID` constant and the 25-line dead comment about `wikidata_ontology`. Update the docstring `default Q43229` claim to `default None`.
4. **`src/resources/embedding.py`**: delete the file. Update `src/__init__.py` and `src/resources/__init__.py` to drop the import + registration. (~330 LOC + 3 line changes)
5. **`src/api/election_api.py`**: delete (no callers).

For these the verification is the same: no grep hits in `src/` or `tests/` for the symbol being deleted, and the test suite stays green.

## 8. Bigger asks (separate commits)

- **Delete `pies_v3.py` (679 LOC)** — same pattern, but big and someone might have a saved invocation of it. Single commit, easy to revert.
- **Sunset `LEGACY_CACHE_PATH` block** — 35 LOC removal once we're confident no one needs the migration path.
- **`ACTIVE_CYCLES` / `TERMINAL_TYPES` / `PER_ELECTION_LIMITS` centralization** — already in `todo.md`. ~25 file edits. Worth doing as one focused commit so the diff is reviewable.
- **`compute_normalized_key` / `find_potential_matches`** — confirm zero external callers, then delete plus their exports.

---

## Summary table

| Action | LOC delta | Risk |
|---|---|---|
| Drop `_FOUNDER_POSITION_QIDS` | -5 | None — never referenced |
| Drop `_wbsearchentities` | -20 | None — never referenced |
| Drop `ORG_TYPE_QID` + dead comment | -30 | None |
| Drop `embedding.py` + wiring | -330 | Low — no asset uses it |
| Drop `election_api.py` | -67 | None |
| Drop `pies_v3.py` | -679 | Low — single CLI file, easy revert |
| **Total quick wins** | **-1,131** | |
