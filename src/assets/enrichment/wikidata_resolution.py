"""Wikidata-based canonical employer + whale resolution.

Resolves:
1. Employer name → parent company (Google → Alphabet)
2. Whale donor → corporate origin (Jan Koum → WhatsApp)

Uses the batched Wikidata client (`resolve_companies` / `resolve_people`)
so 5,000 employers go from ~5,000 sequential HTTP round-trips down to
~100 batched ones — minutes instead of hours.

Cache strategy
--------------
Persistent negative cache: every name we look up gets stored, regardless
of whether Wikidata had a match. A miss is recorded as
`{'source': 'not_found'}`; on subsequent runs we skip those names entirely
rather than re-querying. Errors (request failed) are NOT cached so they
get retried.

Cache file lives at `<cache_dir>/wikidata.json` (per `src.utils.storage`)
rather than the legacy `/workspace/wikidata_cache.json`. The asset reads
the legacy path as a one-time migration when the new path doesn't exist
yet.

Incremental flush: the cache is written to disk after every batch,
not just at the end of the run. If the asset is interrupted, the next
run starts where it left off.

Pipeline integration
--------------------
- Depends on: canonical_employers, donors
- Creates/refreshes: corporate_families, employer_canonical_mapping,
  whale_corporate_links
"""

import hashlib
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dagster import asset, AssetExecutionContext, Config, MetadataValue, Output
from pydantic import Field

from src.rag.employer_normalization import NON_EMPLOYERS, normalize_employer_name
from src.rag.wikidata_client import (
    reset_circuit_breaker,
    resolve_companies,
    resolve_companies_rest,
    resolve_people,
    resolve_people_rest,
)
from src.resources.arango import ArangoDBResource
from src.utils.storage import get_cache_dir

logger = logging.getLogger(__name__)


# Cache schema:
#   {
#     "employer_resolutions": { "<name>": { ...company-result-dict... }, ... },
#     "whale_resolutions":    { "<name>": { ...person-result-dict... }, ... },
#     "updated_at":           "<ISO timestamp>"
#   }
#
# Each name's value is the result dict from the Wikidata client, augmented
# with a 'cached_at' timestamp. Names with source='not_found' are cached;
# names with source='error' are NOT (so we retry next run).

LEGACY_CACHE_PATH = "/workspace/wikidata_cache.json"


def _cache_path() -> Path:
    """Cache file location: <cache_dir>/wikidata.json. Override with
    WIKIDATA_CACHE_PATH env var."""
    env = os.environ.get("WIKIDATA_CACHE_PATH")
    if env:
        return Path(env)
    return get_cache_dir() / "wikidata.json"


def _load_cache() -> Dict[str, Any]:
    """Load cache. Falls back to legacy /workspace/wikidata_cache.json on
    first run after migration if the new path doesn't exist yet."""
    target = _cache_path()
    sources_to_try = [target]
    if target != Path(LEGACY_CACHE_PATH):
        sources_to_try.append(Path(LEGACY_CACHE_PATH))

    for path in sources_to_try:
        if not path.exists():
            continue
        try:
            with open(path, 'r') as f:
                cache = json.load(f)
            logger.info(
                f"Loaded wikidata cache from {path}: "
                f"{len(cache.get('employer_resolutions', {}))} employers, "
                f"{len(cache.get('whale_resolutions', {}))} whales"
            )
            return cache
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load cache from {path}: {e}")

    return {"employer_resolutions": {}, "whale_resolutions": {}}


def _save_cache(cache: Dict[str, Any]) -> None:
    """Atomic write: serialize to .tmp, then rename. Avoids leaving a
    truncated/corrupt JSON file if the process is killed mid-write."""
    target = _cache_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    cache["updated_at"] = datetime.utcnow().isoformat()
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        with open(tmp, 'w') as f:
            json.dump(cache, f, indent=2)
        os.replace(tmp, target)
    except IOError as e:
        logger.warning(f"Failed to save cache to {target}: {e}")
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _is_cache_hit(entry: Optional[Dict[str, Any]]) -> bool:
    """A cache entry is a hit if it has a definitive source — either a
    successful resolution OR a confirmed not_found. 'error' results are
    intentionally NOT cached, so they'll never be cache hits."""
    if not entry:
        return False
    return entry.get('source') in ('wikidata', 'not_found')


# Convert "MELLON, TIMOTHY" -> "Timothy Mellon" for Wikidata search.
def _whale_name_to_search(name: str) -> str:
    parts = name.split(',', 1)
    if len(parts) == 2:
        last = parts[0].strip().title()
        first_parts = parts[1].strip().split()
        first = first_parts[0].title() if first_parts else ''
        return f"{first} {last}".strip()
    return name.title()


class WikidataResolutionConfig(Config):
    min_whale_amount: int = Field(
        default=250_000,
        description="Minimum donation amount to consider for whale resolution",
    )
    min_employer_amount: int = Field(
        default=50_000,
        description="Minimum total from employees to resolve employer",
    )
    max_employers: int = Field(
        default=5_000,
        description="Maximum employers to process (top by amount)",
    )
    max_whales: int = Field(
        default=2_000,
        description="Maximum whales to process (top by amount)",
    )
    resolve_whales: bool = Field(
        default=True,
        description="Whether to resolve retired/self-employed whales",
    )
    use_cache: bool = Field(
        default=True,
        description="Whether to use cached Wikidata resolutions",
    )
    live_queries: bool = Field(
        default=True,
        description="Whether to make live Wikidata queries for cache misses",
    )
    resolution_path: str = Field(
        default="rest",
        description="'rest' (per-name MediaWiki REST API — reliable, ~0.2s/name) "
                    "or 'sparql' (SPARQL VALUES batches — faster when working "
                    "but the public endpoint has been intermittently flaky).",
    )
    employer_chunk_size: int = Field(
        default=20,
        description="Names per SPARQL VALUES batch for employers. Smaller = "
                    "more requests but each completes faster on Wikidata's "
                    "public endpoint. 20 keeps each batch under ~30s.",
    )
    whale_chunk_size: int = Field(
        default=15,
        description="Names per SPARQL VALUES batch for people. Smaller than "
                    "employer because the UNION-of-5-properties query is "
                    "heavier per name.",
    )


@asset(
    deps=["canonical_employers", "donors"],
    description="Resolve employers and whales to canonical corporate entities via Wikidata (batched + cached).",
    group_name="enrichment",
    compute_kind="external_api",
)
def wikidata_corporate_resolution(
    context: AssetExecutionContext,
    config: WikidataResolutionConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Resolve employers and whales to corporate entities via Wikidata.

    Two-phase: identify all uncached names, batch-query in one pass, then
    write the corporate_families / employer_canonical_mapping /
    whale_corporate_links collections.
    """
    # Re-arm the circuit breaker at the start of every asset run. If a
    # previous run tripped it, this run gets a fresh chance.
    reset_circuit_breaker()

    cache = _load_cache() if config.use_cache else {
        "employer_resolutions": {}, "whale_resolutions": {}
    }
    employer_cache: Dict[str, Any] = cache.setdefault("employer_resolutions", {})
    whale_cache: Dict[str, Any] = cache.setdefault("whale_resolutions", {})

    context.log.info(
        f"📦 Cache: {len(employer_cache):,} employers, {len(whale_cache):,} whales (hits + negative)"
    )

    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)

        for coll in ('corporate_families', 'employer_canonical_mapping', 'whale_corporate_links'):
            if not db.has_collection(coll):
                db.create_collection(coll)
                context.log.info(f"Created collection: {coll}")

        stats: Dict[str, Any] = {
            'employers_processed': 0,
            'employers_from_cache_hit': 0,
            'employers_from_cache_miss': 0,
            'employers_live_resolved': 0,
            'employers_live_not_found': 0,
            'whales_processed': 0,
            'whales_from_cache_hit': 0,
            'whales_from_cache_miss': 0,
            'whales_live_resolved': 0,
            'whales_live_not_found': 0,
            'corporate_families_created': 0,
            'total_whale_money_attributed': 0,
        }

        corporate_families: Dict[str, Dict[str, Any]] = {}
        employer_mappings: List[Dict[str, Any]] = []

        # ================================================================
        # Phase 1: collect employer names + identify which need live queries
        # ================================================================
        context.log.info("Phase 1: Loading canonical employers + identifying uncached names...")
        employers = list(db.aql.execute("""
            FOR ce IN canonical_employers
            FILTER ce.total_from_employees >= @min_amount
            SORT ce.total_from_employees DESC
            LIMIT @max_employers
            RETURN {
                _key: ce._key,
                name: ce.canonical_name,
                total: ce.total_from_employees,
                donor_count: ce.employee_donor_count,
                aliases: ce.aliases
            }
        """, bind_vars={
            "min_amount": config.min_employer_amount,
            "max_employers": config.max_employers,
        }))
        context.log.info(f"  {len(employers):,} canonical employers eligible for resolution")

        # Filter to actual employers, then split cache hits / misses.
        eligible_employers: List[Dict[str, Any]] = []
        names_to_query: List[str] = []
        for emp in employers:
            normalized, meta = normalize_employer_name(emp['name'])
            if meta.get('is_non_employer'):
                continue
            eligible_employers.append({**emp, 'normalized': normalized})
            stats['employers_processed'] += 1

            if _is_cache_hit(employer_cache.get(emp['name'])):
                stats['employers_from_cache_hit'] += 1
            else:
                stats['employers_from_cache_miss'] += 1
                # Query against the *normalized* name; cache is keyed by the raw
                # employer name so result lookup later still works.
                if normalized not in names_to_query:
                    names_to_query.append(normalized)

        context.log.info(
            f"  hits={stats['employers_from_cache_hit']:,}  "
            f"misses={stats['employers_from_cache_miss']:,}  "
            f"unique-to-query={len(names_to_query):,}"
        )

        if config.live_queries and names_to_query:
            use_rest = config.resolution_path == "rest"
            if use_rest:
                # REST path: per-name lookups, batch into "report-progress
                # groups" of 50 for cache-flush cadence. Each name takes
                # ~0.2-0.5s.
                progress_chunk = 50
                context.log.info(
                    f"Phase 1b: REST Wikidata resolution "
                    f"({len(names_to_query):,} names, ~{len(names_to_query)//4:,}s estimated)..."
                )
            else:
                progress_chunk = config.employer_chunk_size
                context.log.info(
                    f"Phase 1b: Batched SPARQL Wikidata resolution "
                    f"({len(names_to_query):,} names, chunk={config.employer_chunk_size})..."
                )
            n_done = 0
            n_chunks = 0
            for start in range(0, len(names_to_query), progress_chunk):
                chunk = names_to_query[start:start + progress_chunk]
                if use_rest:
                    results = resolve_companies_rest(chunk)
                else:
                    results = resolve_companies(chunk, chunk_size=config.employer_chunk_size)
                n_done += len(chunk)
                n_chunks += 1
                if n_chunks % 5 == 0 or n_done == len(names_to_query):
                    context.log.info(f"  resolved {n_done:,}/{len(names_to_query):,}")
                # Persist incrementally — cache by raw employer name.
                ts = datetime.utcnow().isoformat()
                for emp in eligible_employers:
                    raw = emp['name']
                    norm = emp['normalized']
                    if norm not in results:
                        continue
                    r = results[norm]
                    if r.get('source') == 'error':
                        continue
                    employer_cache[raw] = {**r, 'cached_at': ts}
                    if r['source'] == 'wikidata':
                        stats['employers_live_resolved'] += 1
                    else:
                        stats['employers_live_not_found'] += 1
                if n_chunks % 5 == 0:
                    _save_cache(cache)
            _save_cache(cache)

        # ================================================================
        # Phase 2: assemble employer_mappings + corporate_families
        # ================================================================
        context.log.info("Phase 2: Assembling employer mappings + corporate families...")
        for emp in eligible_employers:
            name = emp['name']
            normalized = emp['normalized']
            cached = employer_cache.get(name)
            if cached and cached.get('source') == 'wikidata':
                canonical = cached.get('canonical', normalized)
                relationship = cached.get('relationship', 'self')
                wikidata_id = cached.get('wikidata_id')
            else:
                # not_found OR cache miss + live disabled OR error
                canonical = normalized
                relationship = 'self'
                wikidata_id = None

            family = corporate_families.setdefault(canonical, {
                'canonical_name': canonical,
                'wikidata_id': wikidata_id,
                'member_employers': [],
                'linked_whales': [],
                'total_from_employees': 0,
                'total_from_whales': 0,
            })
            family['member_employers'].append(name)
            family['total_from_employees'] += emp['total']

            employer_mappings.append({
                '_key': hashlib.md5(name.encode()).hexdigest()[:16],
                'employer_key': emp['_key'],
                'employer_name': name,
                'canonical_name': canonical,
                'relationship': relationship,
                'wikidata_id': wikidata_id,
                'amount': emp['total'],
            })

        # ================================================================
        # Phase 3: whale resolution
        # ================================================================
        whale_links: List[Dict[str, Any]] = []
        if config.resolve_whales:
            context.log.info("Phase 3: Loading whales + identifying uncached names...")
            non_employer_values = list(NON_EMPLOYERS) + [
                'RETIRED', 'SELF-EMPLOYED', 'SELF EMPLOYED', 'N/A', 'NONE', ''
            ]
            whales = list(db.aql.execute("""
                FOR d IN donors
                FILTER d.total_amount >= @min_amount
                FILTER d.donor_type IN ["individual", "likely_individual"]
                FILTER d.canonical_employer IN @non_employers
                    OR d.canonical_employer LIKE "%RETIRED%"
                    OR d.canonical_employer LIKE "%SELF%EMPLOY%"
                    OR d.canonical_employer == null
                SORT d.total_amount DESC
                LIMIT @max_whales
                RETURN {
                    _key: d._key,
                    name: d.canonical_name,
                    employer: d.canonical_employer,
                    total: d.total_amount
                }
            """, bind_vars={
                "min_amount": config.min_whale_amount,
                "non_employers": non_employer_values,
                "max_whales": config.max_whales,
            }))
            context.log.info(f"  {len(whales):,} retired/self-employed whales above threshold")

            person_pattern = re.compile(r'^[A-Z][A-Z\'-]+,\s+[A-Z]')
            eligible_whales: List[Dict[str, Any]] = []
            whale_names_to_query: List[str] = []
            for whale in whales:
                if not person_pattern.match(whale['name']):
                    continue
                eligible_whales.append({**whale, 'search_name': _whale_name_to_search(whale['name'])})
                stats['whales_processed'] += 1

                if _is_cache_hit(whale_cache.get(whale['name'])):
                    stats['whales_from_cache_hit'] += 1
                else:
                    stats['whales_from_cache_miss'] += 1
                    sn = eligible_whales[-1]['search_name']
                    if sn not in whale_names_to_query:
                        whale_names_to_query.append(sn)

            context.log.info(
                f"  hits={stats['whales_from_cache_hit']:,}  "
                f"misses={stats['whales_from_cache_miss']:,}  "
                f"unique-to-query={len(whale_names_to_query):,}"
            )

            if config.live_queries and whale_names_to_query:
                use_rest_w = config.resolution_path == "rest"
                progress_chunk = 25 if use_rest_w else config.whale_chunk_size
                if use_rest_w:
                    context.log.info(
                        f"Phase 3b: REST Wikidata resolution "
                        f"({len(whale_names_to_query):,} names, ~{len(whale_names_to_query)//3:,}s estimated)..."
                    )
                else:
                    context.log.info(
                        f"Phase 3b: Batched SPARQL Wikidata resolution "
                        f"({len(whale_names_to_query):,} names, chunk={config.whale_chunk_size})..."
                    )
                n_done = 0
                n_chunks = 0
                for start in range(0, len(whale_names_to_query), progress_chunk):
                    chunk = whale_names_to_query[start:start + progress_chunk]
                    if use_rest_w:
                        results = resolve_people_rest(chunk)
                    else:
                        results = resolve_people(chunk, chunk_size=config.whale_chunk_size)
                    n_done += len(chunk)
                    n_chunks += 1
                    if n_chunks % 4 == 0 or n_done == len(whale_names_to_query):
                        context.log.info(f"  resolved {n_done:,}/{len(whale_names_to_query):,}")
                    ts = datetime.utcnow().isoformat()
                    for whale in eligible_whales:
                        raw = whale['name']
                        sn = whale['search_name']
                        if sn not in results:
                            continue
                        r = results[sn]
                        if r.get('source') == 'error':
                            continue
                        whale_cache[raw] = {**r, 'cached_at': ts}
                        if r['source'] == 'wikidata':
                            stats['whales_live_resolved'] += 1
                        else:
                            stats['whales_live_not_found'] += 1
                    if n_chunks % 4 == 0:
                        _save_cache(cache)
                _save_cache(cache)

            # Assemble whale_links + corporate_families
            for whale in eligible_whales:
                cached = whale_cache.get(whale['name'])
                if not cached or cached.get('source') != 'wikidata':
                    continue
                companies = cached.get('companies', [])
                if not companies:
                    continue
                stats['total_whale_money_attributed'] += whale['total']
                for company in companies:
                    canonical = company['name']
                    whale_links.append({
                        '_key': hashlib.md5(f"{whale['name']}_{canonical}".encode()).hexdigest()[:16],
                        'donor_key': whale['_key'],
                        'donor_name': whale['name'],
                        'company_name': canonical,
                        'canonical_name': canonical,
                        'relationship': company.get('relationship', 'unknown'),
                        'wikidata_id': company.get('wikidata_id'),
                        'amount': whale['total'],
                    })
                    family = corporate_families.setdefault(canonical, {
                        'canonical_name': canonical,
                        'wikidata_id': company.get('wikidata_id'),
                        'member_employers': [],
                        'linked_whales': [],
                        'total_from_employees': 0,
                        'total_from_whales': 0,
                    })
                    family['linked_whales'].append(whale['name'])
                    family['total_from_whales'] += whale['total']

        # ================================================================
        # Phase 4: write to ArangoDB
        # ================================================================
        context.log.info("Phase 4: Writing results to ArangoDB...")
        corp_coll = db.collection('corporate_families')
        corp_coll.truncate()
        family_docs: List[Dict[str, Any]] = []
        for canonical, info in corporate_families.items():
            total_influence = info['total_from_employees'] + info['total_from_whales']
            family_docs.append({
                '_key': hashlib.md5(canonical.encode()).hexdigest()[:16],
                'canonical_name': canonical,
                'wikidata_id': info.get('wikidata_id'),
                'member_employers': sorted(set(info['member_employers'])),
                'linked_whales': sorted(set(info['linked_whales'])),
                'total_from_employees': info['total_from_employees'],
                'total_from_whales': info['total_from_whales'],
                'total_influence': total_influence,
                'created_at': datetime.utcnow().isoformat(),
            })
        corp_coll.import_bulk(family_docs, on_duplicate='replace')
        stats['corporate_families_created'] = len(family_docs)
        context.log.info(f"  {len(family_docs):,} corporate families")

        mapping_coll = db.collection('employer_canonical_mapping')
        mapping_coll.truncate()
        mapping_coll.import_bulk(employer_mappings, on_duplicate='replace')
        context.log.info(f"  {len(employer_mappings):,} employer mappings")

        if whale_links:
            whale_coll = db.collection('whale_corporate_links')
            whale_coll.truncate()
            whale_coll.import_bulk(whale_links, on_duplicate='replace')
            context.log.info(f"  {len(whale_links):,} whale-corporate links")

        # Final cache flush
        if config.use_cache:
            _save_cache(cache)

        context.log.info("=" * 60)
        context.log.info("WIKIDATA RESOLUTION SUMMARY")
        context.log.info("=" * 60)
        context.log.info(
            f"Employers: processed={stats['employers_processed']:,} "
            f"hits={stats['employers_from_cache_hit']:,} "
            f"misses={stats['employers_from_cache_miss']:,} "
            f"live-resolved={stats['employers_live_resolved']:,} "
            f"live-not-found={stats['employers_live_not_found']:,}"
        )
        context.log.info(
            f"Whales: processed={stats['whales_processed']:,} "
            f"hits={stats['whales_from_cache_hit']:,} "
            f"misses={stats['whales_from_cache_miss']:,} "
            f"live-resolved={stats['whales_live_resolved']:,} "
            f"live-not-found={stats['whales_live_not_found']:,}"
        )
        context.log.info(f"Corporate families: {stats['corporate_families_created']:,}")
        context.log.info(
            f"Whale money attributed: ${stats['total_whale_money_attributed']/1e6:.1f}M"
        )

        return Output(
            stats,
            metadata={
                "employers_processed": MetadataValue.int(stats['employers_processed']),
                "employers_live_resolved": MetadataValue.int(stats['employers_live_resolved']),
                "employers_live_not_found": MetadataValue.int(stats['employers_live_not_found']),
                "whales_processed": MetadataValue.int(stats['whales_processed']),
                "whales_live_resolved": MetadataValue.int(stats['whales_live_resolved']),
                "corporate_families": MetadataValue.int(stats['corporate_families_created']),
                "whale_money_attributed": MetadataValue.float(float(stats['total_whale_money_attributed'])),
                "cache_path": MetadataValue.text(str(_cache_path())),
            }
        )
