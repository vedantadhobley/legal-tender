"""Wikidata ontology walker.

Answers the question "is this Wikidata entity an employer-shaped
organization?" by walking Wikidata's own subclass-of (P279) chain
upward from a given type Q-id. No hardcoded Q-id whitelists, no
keyword guesses — the answer comes from Wikidata's own ontology.

Why a walk and not a SPARQL one-shot:
- The canonical query `SELECT ?subclass WHERE { ?subclass wdt:P279* wd:Q43229 }`
  needs the WDQS SPARQL endpoint, which is currently in migration weather
  (8 of 16 health-check queries timed out as of 2026-05-10).
- The MediaWiki REST API is healthy. We can walk P279 upward per Q-id
  via `Special:EntityData` lookups and reach the same answer.
- Bounded cost: P279 chains are typically 3-7 hops. Cached after first
  walk per Q-id. Cold-start total ~1-2K unique Q-ids × ~3-7 fetches each
  with parallel workers = a few minutes; warm cache is free.

Two small lists of subtree-root Q-ids define the answer:
  ORG_ROOTS         — reaching one of these means "yes, employer-shaped"
  NON_EMPLOYER_ROOTS — reaching one of these means "no, not employer-shaped"

These are NOT growing whitelists. They're a small set of well-known
top-level subtree roots in the Wikidata ontology. The exhaustive
membership comes from Wikidata's own data, derived at walk time.

Per docs/decisions.md 2026-05-10, this replaces the band-aid
`_STRICT_CORPORATE_QIDS` (~40 hand-curated Q-ids) and
`_CORPORATE_TYPE_NAME_SUBSTRINGS` / `_NON_EMPLOYER_TYPE_NAME_SUBSTRINGS`
(~55 English-keyword strings) in `wikidata_resolver.py`.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Dict, Optional, Set

from src.rag.wikidata_client import _claim_qid, _entity_data

logger = logging.getLogger(__name__)


# Subtree roots: reaching one of these via P279 (subclass-of) walk
# means the starting Q-id IS a kind of organization / employer.
#
# Q43229 is the "organization" root in Wikidata's class hierarchy. Its
# subtree includes business, university, hospital, government agency,
# trade union, association, nonprofit, religious organization, sports
# club, and ~5-10K other organization-shaped Q-ids.
ORG_ROOTS: Set[str] = {
    "Q43229",  # organization
}

# Subtree roots: reaching one of these means the entity is NOT an
# employer in our model, even if it also descends from an org root.
#
# Wikidata's organization tree is broad — a country IS technically an
# organization in the ontology (sovereign state → political entity →
# organization). But for FEC-employer attribution, countries,
# municipalities, regions, video games, films, etc. don't count.
#
# These are subtree ROOTS, not exhaustive enumerations. Each one
# anchors a sub-tree of thousands of leaves; we only have to
# recognize the root and the walk terminates.
NON_EMPLOYER_ROOTS: Set[str] = {
    # Geography / sovereignty
    "Q6256",  # country
    "Q3624078",  # sovereign state
    "Q15642541",  # administrative territorial entity (covers all
                   # municipalities, cities, towns, villages, districts)
    "Q56061",  # administrative territorial entity (alternate)
    "Q486972",  # human settlement
    "Q23397",  # lake
    "Q4022",  # river
    # Creative works
    "Q7889",  # video game
    "Q11424",  # film
    "Q571",  # book
    "Q7725634",  # literary work
    "Q47461344",  # written work
    "Q5398426",  # television series
    "Q482994",  # album
    "Q7366",  # song
    "Q386724",  # work of art
    "Q386724",  # work of art (alt)
    # Concepts / categorical
    "Q34770",  # language
    "Q11879003",  # given name
    "Q101352",  # family name
    "Q133327",  # taxon (life form classification — covers all genus/species)
    # Substances / abstract objects
    "Q11173",  # chemical compound
    "Q12140",  # medication
    "Q134808",  # vaccine
    "Q7187",  # gene
    "Q8054",  # protein
    # Wikimedia administrivia
    "Q4167410",  # Wikimedia disambiguation page
    "Q4167836",  # Wikimedia category
    "Q13406463",  # Wikimedia list article
    # Direct human (P31=Q5 — for completeness even though humans aren't
    # in the organization subtree under normal walks)
    "Q5",
    # Family / kin-group structures. Wikidata's "family" subtree
    # eventually reaches the organization root via "agent" / "social
    # group" subclasses, but extended families (Goldman-Sachs family,
    # Adelson family) aren't employers in our model.
    "Q8436",  # family
    "Q721790",  # extended family
    # Sport teams — same rationale; "team" subtree reaches org root
    # but FEC donors don't list "Kolkata Knight Riders" as employer.
    "Q12973014",  # cricket team
    "Q15944511",  # sports team
    "Q476028",  # association football club
    "Q14435",  # baseball team
    "Q15873",  # basketball team
}

# Cache: Q-id → True (org), False (non-employer), None (unknown / no
# resolution within walk depth). Persisted to disk so subsequent runs
# don't re-walk.
_CACHE_PATH = os.environ.get(
    "WIKIDATA_ONTOLOGY_CACHE_PATH",
    "/storage/cache/wikidata_ontology.json",
)
_LEGACY_CACHE_PATHS = [
    "/workspace/wikidata_ontology.json",
]

_cache: Dict[str, Optional[bool]] = {}
_cache_lock = threading.Lock()
_cache_loaded = False
_cache_dirty = False

# Per-Q-id locking for in-progress walks. Two threads asking for the
# same Q-id should not both walk it — the second should wait for the
# first to complete and then read from cache. Without this, parallel
# prewarm produces inconsistent results because transient REST
# failures (429, timeout) cause different threads to follow truncated
# walks and write divergent answers.
_qid_walk_locks: Dict[str, threading.Lock] = {}
_qid_walk_locks_mutex = threading.Lock()


def _get_walk_lock(qid: str) -> threading.Lock:
    """Return the per-Q-id lock, creating it on first access."""
    with _qid_walk_locks_mutex:
        lock = _qid_walk_locks.get(qid)
        if lock is None:
            lock = threading.Lock()
            _qid_walk_locks[qid] = lock
        return lock

# Bounded depth — P279 chains rarely exceed this. Caps cost on cycles.
MAX_WALK_DEPTH = 12


def _load_cache() -> None:
    global _cache, _cache_loaded
    if _cache_loaded:
        return
    paths = [_CACHE_PATH] + _LEGACY_CACHE_PATHS
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path) as f:
                data = json.load(f)
            with _cache_lock:
                _cache.update(data)
            logger.info("Loaded wikidata ontology cache: %d entries from %s", len(data), path)
            break
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Could not load ontology cache %s: %s", path, e)
    _cache_loaded = True


def save_cache() -> None:
    """Persist cache to disk. Idempotent; safe to call any time."""
    global _cache_dirty
    if not _cache_dirty:
        return
    try:
        os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
        tmp = _CACHE_PATH + ".tmp"
        with _cache_lock:
            snapshot = dict(_cache)
        with open(tmp, "w") as f:
            json.dump(snapshot, f)
        os.replace(tmp, _CACHE_PATH)
        _cache_dirty = False
        logger.info("Saved wikidata ontology cache: %d entries", len(snapshot))
    except OSError as e:
        logger.warning("Could not save ontology cache: %s", e)


def _classify_walk(start_qid: str) -> Optional[bool]:
    """Level-parallel BFS up the P279 chain from `start_qid`. First
    root hit wins (BFS depth-order).

    Within a single BFS level, fetch all parents' entity data in
    parallel via ThreadPool — typically 5-10x faster than sequential
    per-node fetches. Different LEVELS are still sequential (we need
    level N's results before exploring level N+1), but within a
    level the requests fly concurrently.

    Returns: True / False / None per the docstring above.
    """
    from concurrent.futures import ThreadPoolExecutor

    if start_qid in NON_EMPLOYER_ROOTS:
        return False
    if start_qid in ORG_ROOTS:
        return True

    visited: Set[str] = {start_qid}
    current_level: List[str] = [start_qid]
    depth = 0

    while current_level and depth < MAX_WALK_DEPTH:
        # Check root membership for current level (BFS shallowest wins).
        for qid in current_level:
            if qid in ORG_ROOTS:
                return True
            if qid in NON_EMPLOYER_ROOTS:
                return False

        # Fetch entity data for the current level in parallel.
        # is_org_subclass cache hits within the level are also useful —
        # if any cached Q-id is True/False, we can early-return.
        cached_results: Dict[str, Optional[bool]] = {}
        uncached: List[str] = []
        with _cache_lock:
            for qid in current_level:
                if qid in _cache:
                    cached_results[qid] = _cache[qid]
                else:
                    uncached.append(qid)
        # Cache-hit shortcuts: if any current-level Q-id is already
        # classified True, we can return True (it's at this depth).
        for qid, result in cached_results.items():
            if result is True:
                return True
            if result is False:
                # Don't immediately return False — another sibling at
                # this level might be True. But mark this branch dead.
                pass
        # If ALL of the current level is cached and all False, we're
        # done — no org root reachable from here.
        if not uncached and all(r is False for r in cached_results.values()):
            return False

        # Fetch entity data for uncached nodes in parallel.
        if uncached:
            with ThreadPoolExecutor(max_workers=4) as ex:
                entities = dict(zip(uncached, ex.map(_entity_data, uncached)))
        else:
            entities = {}

        # Build the next level from P279 parents.
        next_level: List[str] = []
        for qid in current_level:
            ent = entities.get(qid)
            if ent is None:
                continue
            for claim in ent.get("claims", {}).get("P279", []):
                parent_qid = _claim_qid(claim)
                if parent_qid and parent_qid not in visited:
                    visited.add(parent_qid)
                    next_level.append(parent_qid)

        current_level = next_level
        depth += 1

    # No root reached within depth cap.
    return False


def is_org_subclass(qid: str) -> Optional[bool]:
    """Cached classification: is this Q-id an employer-shaped subclass
    of organization in Wikidata's ontology?

    Thread-safe: per-Q-id locking ensures that concurrent calls for
    the same Q-id don't both walk; the second blocks until the first
    finishes and then reads the cached result. Without this, parallel
    prewarm produces inconsistent classifications because transient
    REST failures cause different threads to follow truncated walks.

    Returns:
        True  — first root reached in P279 BFS is in ORG_ROOTS
        False — first root reached is in NON_EMPLOYER_ROOTS, or no
                root reached within depth cap
        None  — only on input validation failure
    """
    if not qid:
        return None
    _load_cache()
    # Fast path: cache hit without taking the per-Q-id lock.
    with _cache_lock:
        if qid in _cache:
            return _cache[qid]

    # Slow path: take the per-Q-id lock so we don't double-walk.
    walk_lock = _get_walk_lock(qid)
    with walk_lock:
        # Re-check cache: another thread may have just finished walking
        # this Q-id while we were waiting on the lock.
        with _cache_lock:
            if qid in _cache:
                return _cache[qid]
        result = _classify_walk(qid)
        with _cache_lock:
            _cache[qid] = result
        global _cache_dirty
        _cache_dirty = True
        return result


def is_employer_type(type_ids: list) -> bool:
    """Decide if a candidate's P31 type list represents an employer.

    Accept iff ANY type's P279 chain reaches an ORG_ROOT (with
    NON_EMPLOYER_ROOTS treated as walls — see `_classify_walk`).

    A candidate with multiple types where one reaches org and another
    is purely territorial gets accepted on the strength of the org
    type. Steyr's types are all purely territorial (no org-reaching
    chain) and stay rejected. Goldman-Sachs family's 'extended family'
    can only reach org via the family→social-group chain, which is
    walled, so stays rejected. Goldman Sachs the bank has multiple
    org-reaching types and is accepted.
    """
    return any(is_org_subclass(tid) is True for tid in type_ids)


# Common Q-ids that appear as P31 (instance-of) values for the most
# frequent employer shapes. Pre-walking these once at startup means the
# vast majority of in-chunk type filters become cache hits without any
# REST traffic. Each subsequent walk also primes intermediate Q-ids in
# the chain, so the cache builds even broader.
_COMMON_PREWALK_QIDS = (
    # Top-level
    "Q43229",  # organization
    "Q24229398",  # being / agent (broader than org)
    # Corporate generics
    "Q4830453",  # business
    "Q6881511",  # enterprise
    "Q167037",  # corporation
    "Q891723",  # public company
    "Q161726",  # multinational corporation
    "Q740752",  # limited liability company
    "Q3558581",  # joint-stock company
    "Q7258079",  # company (general)
    "Q783794",  # company (alt)
    "Q45776",  # holding company
    "Q21980538",  # commercial organization
    "Q155076",  # juridical person
    "Q3778211",  # legal person
    # Finance
    "Q22687",  # bank
    "Q319845",  # investment bank
    "Q1331793",  # financial institution / media company
    "Q7257717",  # financial services company
    "Q11691",  # stock exchange
    "Q5621421",  # hedge fund
    "Q1137319",  # capital markets firm
    "Q837171",  # private equity firm
    "Q10689397",  # asset management company
    "Q730038",  # credit institution
    # Education / health / nonprofit
    "Q3918",  # university
    "Q38723",  # higher education institution
    "Q16917",  # hospital
    "Q4287745",  # medical organization
    "Q15911314",  # association
    "Q163740",  # nonprofit organization
    "Q178790",  # trade union
    "Q1361353",  # consulting firm
    "Q122229703",  # management consulting company
    # Government
    "Q327333",  # government agency
    # Media / tech
    "Q15265344",  # broadcasting company
    "Q11707",  # restaurant
    "Q210167",  # video game developer
    "Q860572",  # photo agency
    "Q192283",  # press agency
    "Q41691",  # food manufacturer
    # Industry
    "Q249556",  # railway company
    "Q46970",  # airline
    "Q740752",  # limited liability company
    "Q2089936",  # consulting company
    "Q3591545",  # holding company variant
    # Common non-employer types we want pre-classified as False so
    # the filter pays no walk cost when these appear:
    "Q6256",  # country
    "Q15642541",  # admin territorial entity
    "Q486972",  # human settlement
    "Q5",  # human
    "Q11879003",  # given name
    "Q101352",  # family name
    "Q7889",  # video game
    "Q8436",  # family
    "Q721790",  # extended family
    "Q12973014",  # cricket team
    "Q15944511",  # sports team
)


def prewarm(qids: list, max_workers: int = 8) -> None:
    """Resolve `is_org_subclass` for many Q-ids in parallel. Used to
    populate the cache before per-candidate filtering so the filter
    itself doesn't pay sequential network latency on cache misses.

    Each Q-id triggers a P279 walk. Walks for different start Q-ids
    are independent and parallelizable; per-Q-id locking ensures
    no double-walking for the same Q-id.

    8 workers is the sweet spot empirically — more triggers
    intermittent 429s on Wikidata's REST endpoint.
    """
    from concurrent.futures import ThreadPoolExecutor

    _load_cache()
    # Identify Q-ids not yet cached. Avoid spawning threads for cache
    # hits — they'd just acquire the lock and return.
    with _cache_lock:
        missing = [q for q in qids if q and q not in _cache]
    if not missing:
        return
    # Dedupe.
    missing = list(set(missing))
    logger.info("Pre-warming ontology cache for %d unique Q-ids", len(missing))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        list(ex.map(is_org_subclass, missing))


def prewalk_common_types() -> None:
    """One-time pre-walk of the ~50 most common employer-type Q-ids.
    Idempotent — already-cached Q-ids are skipped. Run once at
    asset / resolver startup to amortize the cold-cache cost.

    After this, the vast majority of in-chunk type filters become
    cache hits with zero REST traffic. The remaining walks are for
    long-tail types not in this list, which still trigger walks but
    the chain hits cached intermediate roots quickly.
    """
    prewarm(list(_COMMON_PREWALK_QIDS))
    save_cache()


if __name__ == "__main__":
    # Smoke test. Walks a handful of representative Q-ids and reports
    # the classification.
    cases = [
        ("Q4830453", "business — should be org"),
        ("Q22687", "bank — should be org"),
        ("Q3918", "university — should be org"),
        ("Q178790", "trade union — should be org"),
        ("Q163740", "nonprofit organization — should be org"),
        ("Q43229", "organization itself — should be org"),
        ("Q122229703", "management consulting company — should be org (BCG case)"),
        # Non-employers
        ("Q6256", "country — should be non-employer"),
        ("Q15642541", "admin territorial entity — should be non-employer"),
        ("Q116457956", "German municipality without town privileges — non-employer"),
        ("Q260320", "Steyr (city instance) — non-employer"),
        ("Q7889", "video game — non-employer"),
        ("Q5", "human — non-employer"),
        ("Q101352", "family name — non-employer"),
        # Mixed / interesting
        ("Q12973014", "cricket team — depends; expect org via sport-team subtree"),
        ("Q15145537", "transit line — expected non-employer (territorial-related)"),
    ]
    for qid, label in cases:
        cls = is_org_subclass(qid)
        emoji = {True: "✓ org", False: "✗ non-employer", None: "? unknown"}[cls]
        print(f"  {qid:14} {emoji:18} ({label})")
    save_cache()
