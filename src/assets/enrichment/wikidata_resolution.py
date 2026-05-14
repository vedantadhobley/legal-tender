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

from src.rag.employer_normalization import (
    EMPLOYER_FAMILY_ALIASES,
    NON_EMPLOYERS,
    normalize_employer_name,
)


def _apply_family_alias(canonical: str) -> str:
    """Map a Wikidata-canonical or raw-FEC canonical name through the
    EMPLOYER_FAMILY_ALIASES table. Returns the unified canonical when
    matched, or the input unchanged. Case-insensitive lookup."""
    if not canonical:
        return canonical
    return EMPLOYER_FAMILY_ALIASES.get(canonical.upper(), canonical)
from src.rag.wikidata_client import reset_circuit_breaker
from src.rag.wikidata_resolver import resolve_batch as resolver_resolve_batch
from src.rag.whale_resolver import resolve_people_batch
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
    return entry.get('source') in ('wikidata', 'gleif', 'not_found')


# ---------------------------------------------------------------------------
# Same-entity merge — Wikidata upstream Q-id shared between corporate_families
# ---------------------------------------------------------------------------
# Some corporate_families resolve to distinct Wikidata Q-ids but represent
# the same real-world entity in FEC context: Pan Am Systems (Q7129582) and
# Pan Am Railways (Q2048811) both have P112 (founder) = Q7807399 (Mellon).
# Without merging, the data shows them as $616M + $308M instead of $923M
# one Mellon entity.
#
# Mechanism: after corporate_families is built, fetch each family's
# upstream Q-ids (P112 founder / P127 owned by / P749 parent org). Two
# families that share any upstream Q-id are candidates to merge — but
# only with guardrails. Without guardrails this fires false positives:
#   - "located in USA" (Q30) puts 64 unrelated megacaps in one cluster
#   - "founded by Elon Musk" (Q317521) clusters OpenAI/SpaceX/X/PayPal/Tesla
#   - "founded by Bill Bain" (Q4908004) clusters Bain Capital (PE) with
#     Bain & Company (consulting) — same founder, different companies.
#
# Guardrails that survived dry-run evaluation against 146 candidate clusters:
#
#   1. n=2 only. Clusters of ≥3 are almost always "famous founder /
#      common HQ" megaclusters, not same-entity.
#   2. Shared name prefix ≥6 chars after legal-suffix strip.
#      "Pan Am" / "Pan Am" = 7 ✓. "Citadel" / "Citadel" = 8 ✓.
#      "Bain " / "Bain " = 5 ✗ (intentionally rejects Bain false positive).
#   3. Educational-institution exclusion: clusters where both names
#      contain University/College/School are different campuses of one
#      system (e.g. UIUC + UIC under "University of Illinois system")
#      and remain distinct FEC employers.

_EDU_NAME_PATTERN = re.compile(r"\b(University|College|School)\b", re.IGNORECASE)

_LEGAL_SUFFIX_PATTERN = re.compile(
    r"\s+(INC|LLC|LP|LLP|LTD|CORP|CORPORATION|COMPANY|CO|GROUP|HOLDINGS|"
    r"PARTNERS|GMBH|SA|AG)\.?$",
    re.IGNORECASE,
)


def _upstream_cache_path() -> Path:
    """Separate cache file from name-resolution cache. Different data shape
    (qid → upstream qids), different write cadence (fetched once per qid),
    avoids bloating the name-keyed cache for every incremental flush."""
    env = os.environ.get("WIKIDATA_UPSTREAM_CACHE_PATH")
    if env:
        return Path(env)
    return get_cache_dir() / "wikidata_upstream.json"


def _load_upstream_cache() -> Dict[str, Any]:
    path = _upstream_cache_path()
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load upstream cache from {path}: {e}")
        return {}


def _save_upstream_cache(cache: Dict[str, Any]) -> None:
    path = _upstream_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(cache, f, indent=2)
        os.replace(tmp, path)
    except IOError as e:
        logger.warning(f"Failed to save upstream cache to {path}: {e}")
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _normalize_for_prefix(name: str) -> str:
    return _LEGAL_SUFFIX_PATTERN.sub("", name.strip()).upper()


def _shared_prefix_chars(a: str, b: str) -> int:
    na, nb = _normalize_for_prefix(a), _normalize_for_prefix(b)
    n = 0
    for ca, cb in zip(na, nb):
        if ca != cb:
            break
        n += 1
    return n


def _compute_same_entity_remap(
    context,
    corporate_families: Dict[str, Dict[str, Any]],
    upstream_cache: Dict[str, Any],
    live_queries: bool,
) -> Dict[str, str]:
    """Return {loser_canonical_name: winner_canonical_name} for families
    that should be merged. See module-level comment above for the rule
    set. May trigger Wikidata REST fetches to populate upstream_cache
    for Q-ids we haven't seen before; that cache persists to disk."""
    # Each family's Wikidata Q-id, in dict-iteration (= insertion) order.
    qid_to_family: Dict[str, str] = {}
    for canonical, info in corporate_families.items():
        qid = info.get("wikidata_id")
        if qid:
            qid_to_family[qid] = canonical

    needs_fetch = [q for q in qid_to_family if q not in upstream_cache]
    if needs_fetch:
        if not live_queries:
            context.log.info(
                f"  {len(needs_fetch)} families lack cached upstream data "
                f"(live_queries disabled — skipping merge phase)"
            )
            return {}
        from src.rag.wikidata_client import fetch_upstream_qids
        context.log.info(
            f"  Fetching upstream Q-ids for {len(needs_fetch):,} families "
            f"(P112/P127/P749, ~{len(needs_fetch)//4:,}s estimated)..."
        )
        ts = datetime.utcnow().isoformat()
        for i, qid in enumerate(needs_fetch, 1):
            data = fetch_upstream_qids(qid)
            if data is None:
                # Transient fetch failure — skip, retry next run.
                continue
            upstream_cache[qid] = {**data, "fetched_at": ts}
            if i % 200 == 0:
                context.log.info(f"    upstream fetch {i:,}/{len(needs_fetch):,}")
                _save_upstream_cache(upstream_cache)
        _save_upstream_cache(upstream_cache)

    # Build {upstream_qid: [family_canonical_name, ...]} from cache.
    upstream_to_families: Dict[str, List[str]] = defaultdict(list)
    for qid, canonical in qid_to_family.items():
        u = upstream_cache.get(qid)
        if not u:
            continue
        for prop in ("p112", "p127", "p749"):
            for target in u.get(prop, []):
                upstream_to_families[target].append(canonical)

    # Apply guardrails to derive merge pairs.
    merge_pairs: List[tuple] = []
    for upstream_qid, families in upstream_to_families.items():
        unique = list(dict.fromkeys(families))  # preserve order, dedup
        if len(unique) != 2:
            continue
        a, b = unique
        if _shared_prefix_chars(a, b) < 6:
            continue
        if _EDU_NAME_PATTERN.search(a) and _EDU_NAME_PATTERN.search(b):
            continue
        merge_pairs.append((a, b))

    if not merge_pairs:
        return {}

    # Union-find over merge pairs — handles transitively-connected
    # clusters (Marvel Comics ↔ Marvel Entertainment ↔ Marvel Games).
    parent: Dict[str, str] = {}

    def find(x: str) -> str:
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        # Path compression
        while parent.get(x, x) != root:
            parent[x], x = root, parent[x]
        return root

    def union(x: str, y: str) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    for a, b in merge_pairs:
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        union(a, b)

    groups: Dict[str, List[str]] = defaultdict(list)
    for name in list(parent):
        groups[find(name)].append(name)

    def influence(n: str) -> int:
        f = corporate_families.get(n, {})
        return f.get("total_from_employees", 0) + f.get("total_from_whales", 0)

    remap: Dict[str, str] = {}
    for root, members in groups.items():
        members = [m for m in members if m in corporate_families]
        if len(members) < 2:
            continue
        target = max(members, key=influence)
        for m in members:
            if m != target:
                remap[m] = target
    return remap


def _apply_same_entity_remap(
    corporate_families: Dict[str, Dict[str, Any]],
    employer_mappings: List[Dict[str, Any]],
    whale_links: List[Dict[str, Any]],
    remap: Dict[str, str],
) -> None:
    """Apply merge remap in-place: fold loser families into winners,
    rewrite mapping records to point at winners."""
    for loser, winner in remap.items():
        if loser not in corporate_families or winner not in corporate_families:
            continue
        l_info = corporate_families.pop(loser)
        w_info = corporate_families[winner]
        w_info["member_employers"].extend(l_info.get("member_employers", []))
        w_info["linked_whales"].extend(l_info.get("linked_whales", []))
        w_info["total_from_employees"] += l_info.get("total_from_employees", 0)
        w_info["total_from_whales"] += l_info.get("total_from_whales", 0)
        merged_from = w_info.setdefault("merged_from", [])
        merged_from.append({
            "canonical_name": loser,
            "wikidata_id": l_info.get("wikidata_id"),
        })

    for mapping in employer_mappings:
        cn = mapping.get("canonical_name")
        if cn in remap:
            winner = remap[cn]
            mapping["canonical_name"] = winner
            w_info = corporate_families.get(winner)
            if w_info:
                mapping["wikidata_id"] = w_info.get("wikidata_id")

    for link in whale_links:
        cn = link.get("canonical_name")
        if cn in remap:
            winner = remap[cn]
            link["canonical_name"] = winner
            link["company_name"] = winner
            w_info = corporate_families.get(winner)
            if w_info:
                link["wikidata_id"] = w_info.get("wikidata_id")


# Suffix tokens to strip from FEC-format names ("MR.", "JR.", "II", etc).
# Lower-cased here for case-insensitive matching against `.lower()` parts.
_NAME_SUFFIX_TOKENS = {
    'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.', 'dr', 'dr.', 'sr', 'sr.',
    'jr', 'jr.', 'ii', 'iii', 'iv', 'esq', 'esq.', 'phd', 'phd.', 'md', 'md.',
}


def _is_initial(token: str) -> bool:
    """Whether a token looks like a single-letter initial (e.g. 'W.', 'A',
    'J.J.'). Multi-character tokens with a trailing period like 'JR.' are
    NOT initials (single-letter base check)."""
    bare = token.replace('.', '')
    return len(bare) == 1 and bare.isalpha()


def _whale_name_to_search(name: str) -> str:
    """Convert FEC-format "LASTNAME, FIRSTNAME [MIDDLE...] [SUFFIX]" into
    a search-friendly "Firstname Lastname" form for Wikidata.

    Handles:
    - Skip leading initials when extracting first name. "BROWN, W. L. LYONS JR."
      → uses "Lyons" not "W." as the first name → "Lyons Brown".
    - Strip trailing suffixes (JR./SR./II/III/IV/MR./DR./PHD/etc).
    - Fall back to title-casing the full name when there's no comma.
    """
    parts = name.split(',', 1)
    if len(parts) != 2:
        return name.title()

    last = parts[0].strip().title()

    raw_tokens = parts[1].strip().split()
    # Strip trailing suffix tokens
    while raw_tokens and raw_tokens[-1].lower() in _NAME_SUFFIX_TOKENS:
        raw_tokens.pop()
    if not raw_tokens:
        return last  # all-suffix first names — degenerate, return last only

    # Skip leading initials. Keep going until we find a non-initial token.
    # Example: ['W.', 'L.', 'LYONS'] → use 'LYONS' as first name.
    first_idx = 0
    while first_idx < len(raw_tokens) and _is_initial(raw_tokens[first_idx]):
        first_idx += 1
    if first_idx >= len(raw_tokens):
        # All initials — degenerate, fall back to first initial
        first = raw_tokens[0]
    else:
        first = raw_tokens[first_idx]

    # Strip trailing punctuation (e.g. an embedded comma like "LYONS,")
    first = first.rstrip(',.;:')
    return f"{first.title()} {last}".strip()


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
    employer_chunk_size: int = Field(
        default=50,
        description="Names per reconci.link batch for employers. 50 is the "
                    "default batch size in wikidata_reconci; this knob lets "
                    "the asset shape progress-logging granularity.",
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
            'families_merged': 0,
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
            # Resolver pipeline: reconci.link top-hit at confidence ≥ 70,
            # GLEIF strict-match fallback for not-founds. See
            # `docs/corporate-resolution.md` for the architecture.
            progress_chunk = config.employer_chunk_size
            context.log.info(
                f"Phase 1b: Resolver pipeline (reconci.link + GLEIF) "
                f"({len(names_to_query):,} names)..."
            )
            n_done = 0
            n_chunks = 0
            for start in range(0, len(names_to_query), progress_chunk):
                chunk = names_to_query[start:start + progress_chunk]
                rr_dict = resolver_resolve_batch(chunk)
                # Normalize ResolutionResult shape to the existing
                # cache schema (canonical, wikidata_id, source, etc.).
                results = {name: rr.to_cache_dict() for name, rr in rr_dict.items()}
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
                    # Both 'wikidata' (Layer 1: reconci.link) and 'gleif'
                    # (Layer 2: LEI registry fallback) count as resolved.
                    if r['source'] in ('wikidata', 'gleif'):
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
        n_skipped_p31 = 0
        for emp in eligible_employers:
            name = emp['name']
            normalized = emp['normalized']
            cached = employer_cache.get(name)
            # Skip canonical_employers that the resolver actively rejected
            # (vs simply "not in Wikidata"). Rejection methods come from
            # the resolver's `_accept_candidate` rules — they signal that
            # the top reconci candidate matched a non-employer entity
            # (state, country, office, occupation, TV episode), OR that
            # the input was too short/ambiguous to trust (USA → United
            # States at 100 score but only 3 chars).
            #
            # Dropping these means donor donations under those employer
            # strings fall through to `by_individual` in the trace,
            # rather than aggregating under "STATE OF ILLINOIS" / "USA" /
            # "PRESIDENT" as if those were corporate sources.
            #
            # Distinction: `no_candidates` and `gleif_no_match` mean
            # "Wikidata + GLEIF both didn't have this entity" — those
            # are KEPT (canonical_employer preserved for legit small
            # corps like PRATT INDUSTRIES not in Wikidata).
            method = cached.get('method') if cached else None
            rejection_prefixes = (
                'reject_p31_',                  # P31 class blacklist hit
                'short_input_no_corroboration', # short input, no signal
                'below_low_threshold_',         # reconci score < 40
                'low_confidence_no_signal_',    # 40-69 score, no signal
                'empty_types_no_classification',# no P31 → not a classified org
            )
            if cached and cached.get('source') == 'not_found' and method and \
                    any(method.startswith(p) for p in rejection_prefixes):
                n_skipped_p31 += 1
                continue
            if cached and cached.get('source') in ('wikidata', 'gleif'):
                canonical = cached.get('canonical', normalized)
                relationship = cached.get('relationship', 'self')
                # wikidata_id is None for source='gleif' results; that's
                # expected. The LEI from GLEIF lives in cached['external_id'];
                # downstream Phase 4 doesn't currently consult it but it's
                # preserved in cache for future use.
                wikidata_id = cached.get('wikidata_id')
            else:
                # not_found OR cache miss + live disabled OR error
                canonical = normalized
                relationship = 'self'
                wikidata_id = None

            # Apply alias merges: same physical entity that surfaced as
            # multiple canonical groups (different FEC name variants
            # without Wikidata, or Wikidata returning sibling Q-ids).
            canonical = _apply_family_alias(canonical)

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
        if n_skipped_p31:
            context.log.info(
                f"  Skipped {n_skipped_p31:,} canonical employers categorically "
                f"rejected by Wikidata P31 (state, federal department, occupation, etc.)"
            )

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
                progress_chunk = 25
                context.log.info(
                    f"Phase 3b: REST Wikidata resolution "
                    f"({len(whale_names_to_query):,} names, "
                    f"~{len(whale_names_to_query)//3:,}s estimated)..."
                )
                n_done = 0
                n_chunks = 0
                for start in range(0, len(whale_names_to_query), progress_chunk):
                    chunk = whale_names_to_query[start:start + progress_chunk]
                    results = resolve_people_batch(chunk)
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
                    canonical = _apply_family_alias(company['name'])
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
        # Phase 3.5: Same-entity merge via shared Wikidata upstream Q-id
        # ================================================================
        # See module-level comment on `_compute_same_entity_remap` for the
        # rule set and why each guardrail exists. Runs before Phase 4
        # so the writes reflect the merged shape.
        context.log.info("Phase 3.5: Same-entity merge (shared upstream Q-id)...")
        upstream_cache = _load_upstream_cache()
        remap = _compute_same_entity_remap(
            context, corporate_families, upstream_cache, config.live_queries
        )
        if remap:
            _apply_same_entity_remap(
                corporate_families, employer_mappings, whale_links, remap
            )
            context.log.info(f"  Merged {len(remap)} families into same-entity targets")
            for loser, winner in list(remap.items())[:20]:
                context.log.info(f"    {loser!r} → {winner!r}")
        else:
            context.log.info("  No same-entity merges (no qualifying clusters)")
        stats['families_merged'] = len(remap)

        # ================================================================
        # Phase 4: write to ArangoDB
        # ================================================================
        context.log.info("Phase 4: Writing results to ArangoDB...")
        corp_coll = db.collection('corporate_families')
        corp_coll.truncate()
        family_docs: List[Dict[str, Any]] = []
        for canonical, info in corporate_families.items():
            total_influence = info['total_from_employees'] + info['total_from_whales']
            doc = {
                '_key': hashlib.md5(canonical.encode()).hexdigest()[:16],
                'canonical_name': canonical,
                'wikidata_id': info.get('wikidata_id'),
                'member_employers': sorted(set(info['member_employers'])),
                'linked_whales': sorted(set(info['linked_whales'])),
                'total_from_employees': info['total_from_employees'],
                'total_from_whales': info['total_from_whales'],
                'total_influence': total_influence,
                'created_at': datetime.utcnow().isoformat(),
            }
            if info.get('merged_from'):
                doc['merged_from'] = info['merged_from']
            family_docs.append(doc)
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
        context.log.info(
            f"Corporate families: {stats['corporate_families_created']:,} "
            f"(merged {stats['families_merged']:,} same-entity dupes)"
        )
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
                "families_merged": MetadataValue.int(stats['families_merged']),
                "whale_money_attributed": MetadataValue.float(float(stats['total_whale_money_attributed'])),
                "cache_path": MetadataValue.text(str(_cache_path())),
            }
        )
