# Storage Layout

> **Status**: stub. Phase 3 fills in the full reference.

Persistent state lives at `~/workspace/data/legal-tender/` on the host, bind-mounted as `/storage` inside containers. Subdirectory split:

- `raw/` — FEC zips, headers/, legislators/. Re-downloadable from FEC. Source of truth for input data.
- `dumps/` — ArangoDB JSONL dumps for fast reload. Subdirs: `fec/{cycle}/`, `enriched/{cycle}/`, `aggregation/`, `graphs/`. Regeneratable from `raw/` but skip the parse cost.
- `cache/` — Regeneratable API caches: `congress_api/`, `wikidata/`, etc. Should NOT be in repo (currently `wikidata_cache.json` and `corporate_families.json` are at repo root — Phase 3 fix).

Env vars (set in compose):
- `LEGAL_TENDER_STORAGE` → `/storage`
- `LEGAL_TENDER_RAW_DIR` → `/storage/raw`
- `LEGAL_TENDER_DUMPS_DIR` → `/storage/dumps`
- `LEGAL_TENDER_CACHE_DIR` → `/storage/cache`

Helpers in `src/utils/storage.py`: `get_raw_dir()`, `get_dumps_dir()`, `get_cache_dir()`, `get_cycle_raw_dir(cycle)`, `get_fec_dumps_dir(cycle)`, `get_enriched_dumps_dir(cycle)`, `get_aggregation_dumps_dir()`, `get_graph_dumps_dir()`.

Phase 3 should fill in:
- The dump format (gzipped JSONL, header line + records, index defs embedded)
- The dump-vs-reparse decision logic in `arango_dump.py`
- The Arango runtime memory tuning (block cache, write buffers — see commit `fb34a44`)
- The bind-mount semantics for dev vs prod (same source, separate runtime named volumes)
