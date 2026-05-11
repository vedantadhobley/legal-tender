"""Wikidata SPARQL client for corporate entity resolution.

Resolves:
1. Company → canonical parent (Google → Alphabet)
2. Person → companies they founded/own/lead

Designed for *bulk* resolution. The original implementation issued one
SPARQL request per name with a 1-second sleep — fine for 10 lookups,
catastrophic for 5,000+ employers (the 14-hour grind that prompted this
rewrite). The current API batches names via SPARQL VALUES so one HTTP
round-trip resolves up to ~50 names at once. Per-name single-shot
helpers are retained as thin wrappers for backward compatibility.

Reliability:
- Exponential backoff on transient failures (429/5xx). Capped at 60s.
- Circuit breaker after 3 consecutive global failures — caller sees
  empty results rather than hanging the asset for hours on an
  unreachable endpoint.
- Distinguishes "Wikidata responded but the name has no match"
  (source='not_found', suitable for negative caching) from "request
  failed entirely" (source='error', should NOT be cached so we retry
  next run).
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_API_ENDPOINT = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_ENDPOINT = "https://www.wikidata.org/wiki/Special:EntityData"
USER_AGENT = "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"
REQUEST_TIMEOUT = 180  # batched queries with P279* ontology walks can be slow
                       # on Wikidata's public endpoint, especially under load
REST_TIMEOUT = 15      # MediaWiki REST API is consistently fast; tight budget
RATE_LIMIT_DELAY = 1.0  # base inter-request sleep
REST_RATE_LIMIT_DELAY = 0.05  # MediaWiki REST quota for anonymous use is
                               # ~200 req/min documented (≈3.3 req/s steady-
                               # state per process). With 4 prewarm workers
                               # doing sequential walks, 0.05s/call = ~20-30
                               # combined req/s. We're bursty (cold-cache
                               # walks) but quickly hit cache once warm.
                               # MAX_RETRIES=5 absorbs occasional 429s
                               # (9s of retry budget per request) so the
                               # circuit breaker doesn't trip on short bursts.
                               # Previously 0.3s — conservative from the era
                               # of 8 workers + 3 retries when bursts
                               # corrupted ontology walks.
REST_PARALLEL_WORKERS = 4  # ThreadPool size for resolve_*_rest. Lowered from
                            # 8 after 429-storm tripped the circuit on a real
                            # run. 4 workers × 0.3s delay keeps us safely
                            # below MediaWiki's anonymous rate limits.
MAX_BACKOFF = 60.0
# Bumped from 3 to 5 (2026-05-10): the per-call retry budget needs to
# survive 429-bursts during the ontology prewarm phase. Three retries
# at 0.3s/0.6s/1.2s back off totals to ~2s — not enough when several
# threads hit Wikidata's per-IP limit simultaneously. Five retries
# (0.3/0.6/1.2/2.4/4.8s = ~9s) buys enough breathing room that
# transient 429-storms don't truncate ontology walks and write
# spurious False classifications (which is how Q22687 (bank) was
# corrupted in the prior run).
MAX_RETRIES = 5
CIRCUIT_BREAKER_THRESHOLD = 10  # consecutive non-429 failures before tripping
                                 # (raised from 3 — false trips on parallel
                                 #  prewarms were corrupting ontology walks)

# Module-level state for the circuit breaker. Reset on each successful
# request. When tripped, _execute_sparql returns None immediately without
# making another request.
_consecutive_failures = 0
_circuit_open = False


class WikidataCircuitOpen(RuntimeError):
    """Raised when the circuit breaker has tripped — Wikidata unreachable."""


def reset_circuit_breaker() -> None:
    """Re-arm the circuit. Call between asset runs, or after fixing
    whatever caused the failures (network issue, etc)."""
    global _consecutive_failures, _circuit_open
    _consecutive_failures = 0
    _circuit_open = False


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _escape_sparql_literal(s: str) -> str:
    """Escape a Python string for use as a SPARQL string literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _execute_rest(url: str, params: Dict[str, Any], timeout: float = REST_TIMEOUT) -> Optional[Dict[str, Any]]:
    """Issue a request to MediaWiki's REST endpoints with the same backoff
    + circuit breaker as SPARQL. The REST endpoints (wbsearchentities,
    Special:EntityData) are on different infrastructure than the SPARQL
    query service and stay reliable when SPARQL is overloaded.

    429 (rate-limit) responses are handled separately from request
    failures: a 429 means "slow down", not "service is down". Repeated
    429s back off this thread but do NOT count toward the circuit
    breaker. Only RequestException (5xx, connection errors, timeouts)
    increment the failure counter."""
    global _consecutive_failures, _circuit_open
    if _circuit_open:
        return None
    last_exc: Optional[BaseException] = None
    last_was_429 = False
    for attempt in range(MAX_RETRIES):
        delay = min(REST_RATE_LIMIT_DELAY * (2 ** attempt), MAX_BACKOFF)
        time.sleep(delay)
        try:
            response = requests.get(
                url,
                params=params,
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
                timeout=timeout,
            )
            if response.status_code == 429:
                last_was_429 = True
                last_exc = requests.HTTPError("429")
                continue
            response.raise_for_status()
            _consecutive_failures = 0
            return response.json()
        except requests.RequestException as e:
            last_was_429 = False
            last_exc = e
            logger.warning(f"Wikidata REST {url} failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
    # Only count non-429 failures toward the circuit breaker. A 429-only
    # failure pattern indicates client should slow down, not that the
    # endpoint is down.
    if not last_was_429:
        _consecutive_failures += 1
        if _consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            _circuit_open = True
            logger.error(f"Wikidata circuit breaker tripped: {last_exc}")
    return None


def _wbsearchentities(name: str, type_filter: str = "item", limit: int = 1) -> Optional[List[Dict[str, Any]]]:
    """Look up a name via MediaWiki's wbsearchentities API. Returns a list
    of match dicts ({id, label, description, ...}) up to `limit`, or None
    on request failure.

    `type_filter` can narrow to 'property', 'item' (default — entities,
    which includes companies, people, and everything else), 'lexeme',
    'form', 'sense'.

    Top-N (limit > 1) lets callers filter generic-concept and
    government-entity matches that often outrank the real corporation
    for short or ambiguous names (e.g. "SIG", "ATT")."""
    response = _execute_rest(
        WIKIDATA_API_ENDPOINT,
        {
            "action": "wbsearchentities",
            "search": name,
            "language": "en",
            "format": "json",
            "limit": limit,
            "type": type_filter,
        },
    )
    if not response:
        return None
    return response.get("search", []) or []


def _entity_data(qid: str) -> Optional[Dict[str, Any]]:
    """Fetch full entity data (claims, labels, descriptions, etc.) for a
    Q-id via Special:EntityData. Returns the entity dict or None."""
    response = _execute_rest(
        f"{WIKIDATA_ENTITY_ENDPOINT}/{qid}.json",
        params={},
    )
    if not response:
        return None
    return response.get("entities", {}).get(qid)


def _claim_qid(claim: Dict[str, Any]) -> Optional[str]:
    """Extract the target Q-id from a Wikidata claim, or None if not a
    Q-id-typed claim (e.g. dates, strings, etc)."""
    try:
        return claim["mainsnak"]["datavalue"]["value"]["id"]
    except (KeyError, TypeError):
        return None


def _execute_sparql(query: str) -> Optional[Dict[str, Any]]:
    """Execute a SPARQL query with exponential backoff + circuit breaker.

    Returns:
        - dict on success
        - None when the circuit is open OR all retries exhausted
    """
    global _consecutive_failures, _circuit_open

    if _circuit_open:
        return None

    last_exc: Optional[BaseException] = None
    for attempt in range(MAX_RETRIES):
        delay = min(RATE_LIMIT_DELAY * (2 ** attempt), MAX_BACKOFF)
        time.sleep(delay)
        try:
            response = requests.get(
                WIKIDATA_SPARQL_ENDPOINT,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT,
            )
            # 429 is rate-limit; treat as retryable
            if response.status_code == 429:
                logger.warning(f"Wikidata 429 (attempt {attempt + 1}); backing off")
                last_exc = requests.HTTPError(f"429 rate limited")
                continue
            response.raise_for_status()
            _consecutive_failures = 0
            return response.json()
        except requests.RequestException as e:
            last_exc = e
            logger.warning(f"Wikidata query failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")

    # All retries exhausted
    _consecutive_failures += 1
    if _consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
        _circuit_open = True
        logger.error(
            f"Wikidata circuit breaker tripped after {_consecutive_failures} "
            f"consecutive failures. Last error: {last_exc}. Returning None for "
            f"all subsequent queries this session — call reset_circuit_breaker() "
            f"to re-arm."
        )
    return None


# ---------------------------------------------------------------------------
# Batched company resolution
# ---------------------------------------------------------------------------


def _build_company_batch_query(names: List[str]) -> str:
    """SPARQL query to resolve N company names in one round-trip.

    For each input name, returns any entity with a matching English label
    plus its parent organization (P749) if any. Names with no match simply
    don't appear in the result rows.

    Originally constrained to business entities via `wdt:P31/wdt:P279*
    wd:Q4830453` (instance-of-or-subclass-of business). Dropped because
    that transitive ontology walk made batched queries time out on
    Wikidata's public endpoint. False-positive matches (e.g. people who
    happen to share a corporate name) won't have a P749 parent and are
    effectively ignored downstream — `_resolve_company` only consumes
    canonical/parent fields.
    """
    values = " ".join(f'"{_escape_sparql_literal(n)}"' for n in names)
    return f"""
    SELECT ?inputName ?company ?companyLabel ?parent ?parentLabel WHERE {{
      VALUES ?inputName {{ {values} }}
      ?company rdfs:label ?label .
      FILTER(LANG(?label) = "en" && LCASE(STR(?label)) = LCASE(STR(?inputName)))
      OPTIONAL {{ ?company wdt:P749 ?parent . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 5000
    """


# ---------------------------------------------------------------------------
# REST-based resolution (preferred — SPARQL endpoint is intermittently flaky)
# ---------------------------------------------------------------------------
#
# The MediaWiki REST API (wbsearchentities + Special:EntityData) is on
# different infrastructure than the Blazegraph SPARQL query service and
# stays reliable when SPARQL is overloaded. Per-call instead of batched
# (slower throughput) but consistently fast (~0.2s per call).


# Wikidata properties used for parent-organization resolution.
P_PARENT_ORGANIZATION = "P749"
P_OWNED_BY = "P127"
P_SUBSIDIARY_OF = "P361"  # part-of (broader than just corporate parent)


# Description-keyword blacklist for filtering generic-concept matches.
#
# Abbreviation employer names (ACS, ADM, ATT, BCG, SIG, BIO, etc.) often
# hit Wikidata's generic-concept Q-ids ("acute coronary syndrome",
# "automated decision-making", "lawyer", etc.) instead of the actual
# corporation. These patterns flag the wrong matches so we don't
# attribute corporate money to abstract concepts.
#
# Conservative — false negatives (rejecting a legit company whose
# description happens to contain "type of") are acceptable; false
# positives ("advocacy group" in by_organization) are not.
_GENERIC_DESCRIPTION_PATTERNS = (
    "type of ",
    "given name",
    "family name",
    "surname",
    "wikimedia category",
    "wikimedia disambiguation",
    "wikimedia list article",
    "branch of",
    "field of",
    "scientific theory",
    "concept of",
    "fictional",
    "abbreviation",
    "language",
    "may refer to",
    "groups using advocacy",  # specifically catches Q431603 = "advocacy group"
    # Description-only patterns for hits whose P31 wasn't blacklist-matched:
    "vaccine ",
    "creative commons license",
    "chemical compound",
    "biographical database",  # "Biografisch Portaal"-style aggregators
    "biographical work",
    "online database",
    "british overseas territory",
    "commune in",
    "administrative territorial",
    "academic discipline",
    "scholarly database",
    "social science",
)


def _is_generic_match(label: Optional[str], description: Optional[str]) -> bool:
    """Heuristic: reject Wikidata matches that look like generic concepts
    rather than specific entities (companies/people).

    Rules:
    1. label starts with a lowercase letter — e.g. "advocacy group",
       "business", "lawyer", "asset management". Real entities are
       title-cased ("Apple Inc.", "Pan Am Railways").
    2. description contains a known generic-concept phrase from
       _GENERIC_DESCRIPTION_PATTERNS.

    False-negative tradeoff: a legit company whose label happens to
    start lowercase (e.g. "iPhone" — but iPhone isn't a company) might
    be wrongly rejected. Acceptable because our use case is corporate
    attribution; we'd rather miss than misattribute.
    """
    if not label:
        return False
    if label[0].islower():
        return True
    if description:
        d = description.lower()
        for pattern in _GENERIC_DESCRIPTION_PATTERNS:
            if pattern in d:
                return True
    return False


# Description-keyword patterns for government / sovereign-entity filtering.
#
# Surfaced via the Kelly Craft whale case: her Wikidata P108 (employer)
# resolves to "United States federal government", which is a real entity
# but not a corporation. Including it as a corporate-family poisons the
# `by_organization` cross-cut with non-corporate dollars.
#
# Conservative — we filter only entities described as governments,
# countries, or sovereign-entity instances. Universities, NGOs, and
# trade unions are NOT filtered (they're legit donor employers with a
# "company-like" relationship for attribution purposes).
_GOVERNMENT_DESCRIPTION_PATTERNS = (
    "federal government",
    "government of the",
    "government of a",
    "national government",
    "sovereign state",
    "country in ",
    "country located",
    "country bordering",
    "head of state",
    "head of government",
    "ministry of",
)


def _is_government_entity(label: Optional[str], description: Optional[str]) -> bool:
    """Heuristic: reject Wikidata matches that resolve to government /
    sovereign entities rather than corporations.

    Triggered when description matches a known government-pattern phrase.
    Label-based detection is intentionally avoided — many legit company
    names contain the word "America" / "United" / "National".
    """
    if not description:
        return False
    d = description.lower()
    for pattern in _GOVERNMENT_DESCRIPTION_PATTERNS:
        if pattern in d:
            return True
    return False


def _should_reject_match(label: Optional[str], description: Optional[str]) -> bool:
    """Combined filter: reject if generic-concept OR government entity."""
    return _is_generic_match(label, description) or _is_government_entity(label, description)


# P31 (instance of) Q-ids that disqualify a Wikidata entity from being
# treated as a corporation/employer. Walking the ontology to derive this
# automatically (via P279* subclass) is too expensive for our REST-only
# code path, so we maintain an explicit blacklist of common bad matches.
#
# Surfaced via abbreviation resolution: 3-4-letter employer names like
# "SIG", "ATT", "BCG", "BIO", "GS" rank a song / license / vaccine / book /
# country above the real corporation. Filtering by P31 (instance-of)
# keeps the corporate hit when it exists in the top-N.
_NON_CORPORATE_P31 = {
    "Q5",  # human
    "Q4167410",  # Wikimedia disambiguation page
    "Q4167836",  # Wikimedia category
    "Q13406463",  # Wikimedia list article
    # Written works
    "Q571",  # book
    "Q11424",  # film
    "Q482994",  # album
    "Q7725634",  # literary work
    "Q15239622",  # literary work (alt)
    "Q47461344",  # written work
    "Q5398426",  # television series
    "Q24856",  # film series
    "Q277759",  # book series
    "Q41298",  # magazine
    "Q108381",  # book series (alt)
    "Q19479619",  # bibliographic database
    # Music
    "Q134556",  # single (music)
    "Q7366",  # song
    "Q386724",  # work of art
    # Geography / sovereignty
    "Q6256",  # country
    "Q3624078",  # sovereign state
    "Q15634554",  # state with limited recognition
    "Q56061",  # administrative territorial entity
    "Q46395",  # British Overseas Territory
    "Q484170",  # commune of France
    "Q727",  # capital
    "Q515",  # city
    "Q23397",  # lake
    "Q4022",  # river
    "Q486972",  # human settlement
    "Q3957",  # town
    "Q532",  # village
    # Languages / categorical
    "Q34770",  # language
    "Q33829",  # natural language
    "Q133327",  # life form
    "Q284465",  # ethnic group
    "Q41710",  # ethnic group (alt)
    # Chemistry / medicine
    "Q134808",  # vaccine
    "Q105967696",  # vaccine subclass
    "Q12140",  # medication
    "Q11173",  # chemical compound
    "Q113145171",  # chemical compound (alt)
    # Names / labels
    "Q11879003",  # given name
    "Q11879590",  # female given name
    "Q12308941",  # male given name
    "Q3409032",  # unisex given name
    "Q101352",  # family name
    # Concepts / abstracts
    "Q205663",  # process
    "Q1190554",  # occurrence
    "Q11862829",  # academic discipline
    # Vehicles (frequently match abbreviations)
    "Q2811",  # submarine
    "Q11446",  # ship
    "Q170382",  # warship
    # Licenses
    "Q177682",  # license
    "Q207621",  # software license
    "Q284742",  # Creative Commons license
    # Roles / professions (job titles match abbreviations)
    "Q15987302",  # legal profession
    "Q189533",  # academic discipline (used for "lawyer" too)
    "Q4611891",  # association football position
    # Astronomy
    "Q17444909",  # galaxy classification
    "Q850950",  # astronomical catalog
    # Sport teams (3-4 letter abbreviations frequently match these)
    "Q12973014",  # cricket team (Kolkata Knight Riders for "KKR")
    "Q476028",  # association football club
    "Q847017",  # sports club
    "Q4438121",  # sports organization
    "Q15944511",  # sports team
    "Q53538476",  # sports team season
    "Q14435",  # baseball team
    "Q15873",  # basketball team
    "Q15976457",  # American football team
    "Q1078541",  # ice hockey club
    # Transit / rail lines (catches "ULINE" → "U Line" light rail noise)
    "Q15145537",  # transit line / metro line
    "Q107343049",  # transit subline
    "Q124130104",  # railway service
    # Biology / genes (catches "LFG" → FAIM2 protein-coding gene)
    "Q7187",  # gene
    "Q8054",  # protein
    "Q11173",  # chemical compound (already in but listed here for clarity)
    "Q16521",  # taxon (insects, plants)
    "Q713623",  # clade
    # Other generic-noise types surfaced via FEC abbreviations
    "Q936518",  # aerospace manufacturer (caught a defunct German aircraft co for "LFG")
    # Concept / media abstraction (catches "CORPORATION" → "Corporation"
    # the 1988 video game)
    "Q7889",  # video game
    # Municipality variants (catches "FAHR" → "Fahrenzhausen", a German
    # municipality, when looking up donor employers)
    "Q116457956",  # municipality without town privileges in Germany
    "Q15284",  # municipality
    "Q1799794",  # administrative division of Bavaria
    "Q4790",  # municipality of the Czech Republic
    "Q5124673",  # commune of Italy
    # Cities and settlements (catches "STEYER" → "Steyr", "GS" → other cities)
    "Q667509",  # district of Austria
    "Q13539802",  # statutory city of Austria
    "Q262882",  # city in Austria
    "Q871419",  # statutory city
    "Q41176",  # building (some entities mis-classified)
    "Q3957",  # town
    "Q532",  # village
    "Q486972",  # human settlement
    # Units of measurement (catches "FAHR" → degree Fahrenheit)
    "Q68723978",  # unit of temperature
    "Q82047057",  # unit derived from a base SI unit (?)
    "Q21684377",  # imperial unit
    "Q47574",  # unit of measurement
    # Tractor brands and equipment classes
    "Q98579904",  # tractor brand
}


# Strict corporate-only P31 set used as a positive whitelist for the
# alternate-form retry path. Suffix-stripping is aggressive ("PRATT
# INDUSTRIES" → "PRATT") and it's easy to land on a real entity that
# isn't the intended employer (Pratt Institute the school, Pratt &
# Whitney the engine maker). Requiring the retry result's P31 to
# overlap with this strict set prevents non-corporate matches from
# winning a retry — at the cost of missing some legit corporate
# matches whose P31 isn't in this set. Trade-off chosen toward
# precision over recall on the retry path.
_STRICT_CORPORATE_P31 = {
    "Q4830453",  # business
    "Q6881511",  # enterprise
    "Q43229",  # organization
    "Q167037",  # corporation
    "Q891723",  # public company
    "Q161726",  # multinational corporation
    "Q740752",  # limited liability company
    "Q3558581",  # joint-stock company
    "Q22687",  # bank
    "Q1331793",  # financial institution
    "Q7257717",  # financial services company
    "Q1361353",  # consulting firm
    "Q11691",  # stock exchange
    "Q15911314",  # association
    "Q163740",  # nonprofit organization
    "Q178790",  # trade union
    "Q11707",  # restaurant
    "Q210167",  # video game developer
    "Q319845",  # investment bank
    "Q15265344",  # broadcasting company
    "Q45776",  # holding company
    "Q43501",  # zoo (sometimes employer)
    "Q188509",  # suburb (no — remove)
}
_STRICT_CORPORATE_P31.discard("Q188509")


def _entity_has_strict_corporate_p31(entity: Dict[str, Any]) -> bool:
    """True iff at least one P31 (instance of) on the entity is in the
    strict corporate whitelist. Used to gate the alternate-form retry
    path so suffix-stripped names can't accidentally win against
    universities, government agencies, etc."""
    claims = entity.get('claims', {})
    for claim in claims.get('P31', []):
        if _claim_qid(claim) in _STRICT_CORPORATE_P31:
            return True
    return False


def _entity_has_non_corporate_p31(entity: Dict[str, Any]) -> bool:
    """True if any P31 (instance of) on the entity is in the non-corporate
    blacklist. Used to reject Wikidata hits that aren't companies — books,
    films, vaccines, countries, songs, people — for employer resolution.
    """
    claims = entity.get('claims', {})
    for claim in claims.get('P31', []):
        if _claim_qid(claim) in _NON_CORPORATE_P31:
            return True
    return False


# Suffix tokens that often appear on FEC employer names but are absent
# (or differently-cased) on the canonical Wikidata entry. If the full
# name fails to resolve, retry with these stripped — captures cases like
# "BLACKSTONE GROUP" → "Blackstone Inc." (Wikidata) and
# "CITADEL INVESTMENT GROUP" → "Citadel LLC".
#
# Conservative: we only retry the longest single suffix-stripped form
# once, not every combinatorial variation. Risk of a false positive
# match (stripping "PARTNERS" turns a unique name into a common one) is
# the reason for the conservative single-retry approach.
_RETRY_SUFFIX_TOKENS = (
    "GROUP",
    "HOLDINGS",
    "MANAGEMENT",
    "PARTNERS",
    "INVESTMENTS",
    "INVESTMENT",
    "CAPITAL",
    "ENTERPRISES",
    "ASSOCIATES",
    "COMPANIES",
    "INDUSTRIES",
    "VENTURES",
    "ADVISORS",
)


def _alternate_employer_forms(name: str) -> List[str]:
    """Generate up to a small number of alternate forms of an employer
    name to retry against Wikidata when the literal name didn't resolve.

    Currently emits at most one alternate: the input with one of the
    well-known generic suffixes stripped (e.g. "BLACKSTONE GROUP" →
    "BLACKSTONE"). Returns [] if the name has none of the suffixes or
    if stripping would leave fewer than two tokens.
    """
    tokens = name.split()
    if len(tokens) < 2:
        return []
    last_upper = tokens[-1].upper().rstrip(",.;:")
    if last_upper in _RETRY_SUFFIX_TOKENS:
        candidate = " ".join(tokens[:-1])
        # Don't return a single-token alternate that's too generic.
        if len(candidate.split()) >= 1 and len(candidate) >= 3:
            return [candidate]
    return []


# Hardcoded overrides for FEC employer name → Wikidata Q-id when the
# default search ranking gets the wrong answer.
#
# Use sparingly. Each entry should:
# 1. Be a real Wikidata entity (verified Q-id)
# 2. Be the *intended* corporate identity for the FEC string in context,
#    not just a literal-match
# 3. Be common enough to justify a hardcode (significant $ at stake)
#
# Surfaced via spot-checks: "NEA" wbsearchentities ranks the defunct
# Newspaper Enterprise Association above the National Education
# Association teachers union, even though the union is the
# overwhelming intent for FEC donors listing "NEA" as employer.
_EMPLOYER_OVERRIDES: Dict[str, str] = {
    # FEC name (normalized, uppercase)  →  Wikidata Q-id
    "NEA": "Q3111510",  # National Education Association (teachers union)
    # Citadel — wbsearchentities ranks "Citadel" the 1985 video game
    # ahead of Citadel LLC (Ken Griffin's hedge fund) when searching
    # the bare "CITADEL" string. Force the corporate parent. Also
    # used for sub-entities and a typo we observed in FEC data.
    "CITADEL": "Q2974366",  # Citadel Enterprise Americas LLC
    "CITADEL ASSET MANAGEMENT": "Q2974366",
    "CITADELL": "Q2974366",  # FEC typo
    "CITADEL LLC": "Q2974366",
}


def _resolve_company_one_query(name: str, search_limit: int = 5) -> Dict[str, Any]:
    """Single-shot REST lookup for one literal name. Pulls top-N candidates
    from wbsearchentities, filters generic + government matches, then
    fetches entity data for the first surviving hit and walks P749 to
    find its parent.

    Returns a result dict shaped like `_resolve_company_rest`'s output.
    Source is 'not_found' when search returns nothing OR all hits were
    filtered; 'error' on circuit-open / network failure.
    """
    base = {
        'canonical': name,
        'original': name,
        'relationship': 'self',
        'wikidata_id': None,
        'parent_id': None,
        'source': 'not_found',
    }

    # Hardcoded override path. Skips the wbsearchentities top-N step
    # and directly fetches the intended entity. Returns the override's
    # canonical label (or input name if entity-data fetch failed).
    override_qid = _EMPLOYER_OVERRIDES.get(name.upper().strip())
    if override_qid:
        ent = _entity_data(override_qid)
        if ent is not None:
            label = ent.get('labels', {}).get('en', {}).get('value') or name
            return {
                'canonical': label,
                'original': name,
                'relationship': 'parent',
                'wikidata_id': override_qid,
                'parent_id': None,
                'source': 'wikidata',
            }
        # Override fetch failed (network blip etc.) — fall through to
        # normal search rather than returning error, so the override
        # doesn't silently turn into not_found.

    hits = _wbsearchentities(name, limit=search_limit)
    if hits is None:
        # Distinguish 'not_found' (search returned nothing) from 'error'
        # (request failed). _execute_rest returns None on both, so we
        # check circuit state to disambiguate.
        if _circuit_open:
            return {**base, 'source': 'error'}
        return base

    if not hits:
        return base

    # Walk through hits in rank order. Reject generic concepts /
    # governments by description, then fetch entity data and reject any
    # P31=Q5 (human) — abbreviation employer names like "SIG" rank
    # Sigmund Freud above Susquehanna International Group; humans
    # aren't employers, so we keep going.
    chosen_qid: Optional[str] = None
    chosen_label: Optional[str] = None
    chosen_entity: Optional[Dict[str, Any]] = None
    for hit in hits:
        if _should_reject_match(hit.get('label'), hit.get('description')):
            continue
        qid_h = hit.get('id')
        if not qid_h:
            continue
        ent = _entity_data(qid_h)
        if ent is None:
            # Couldn't fetch full entity. Accept the hit (cacheable
            # partial-success) without further P31 inspection rather
            # than skipping entirely — circuit may have just opened.
            return {
                'canonical': hit.get('label') or name,
                'original': name,
                'relationship': 'parent',
                'wikidata_id': qid_h,
                'parent_id': None,
                'source': 'wikidata',
            }
        if _entity_has_non_corporate_p31(ent):
            continue  # human / book / film / country / vaccine — try next hit
        chosen_qid = qid_h
        chosen_label = hit.get('label') or name
        chosen_entity = ent
        break

    if chosen_qid is None or chosen_entity is None:
        return base  # all hits filtered → not_found

    qid = chosen_qid
    label = chosen_label or name
    entity = chosen_entity
    claims = entity.get('claims', {})
    parent_qid = None
    # Only P749 (parent organization). P127 (owned by) was tempting as
    # fallback but it captures shareholder relationships — Microsoft → BlackRock
    # because BlackRock is a major shareholder. Wrong semantically.
    for claim in claims.get(P_PARENT_ORGANIZATION, []):
        qid_p = _claim_qid(claim)
        if qid_p:
            parent_qid = qid_p
            break

    if not parent_qid:
        return {
            'canonical': label,
            'original': name,
            'relationship': 'parent',
            'wikidata_id': qid,
            'parent_id': None,
            'source': 'wikidata',
        }

    # Resolve parent's label
    parent_entity = _entity_data(parent_qid)
    parent_label = None
    parent_desc = None
    if parent_entity:
        parent_label = parent_entity.get('labels', {}).get('en', {}).get('value')
        parent_desc = parent_entity.get('descriptions', {}).get('en', {}).get('value')
    if not parent_label:
        # Couldn't get parent label — keep canonical as self with parent_id
        # for future re-resolution.
        return {
            'canonical': label,
            'original': name,
            'relationship': 'parent',
            'wikidata_id': qid,
            'parent_id': parent_qid,
            'source': 'wikidata',
        }

    # If the parent itself is a government / generic concept, don't roll
    # the subsidiary up — keep the subsidiary's own label as canonical.
    # Example: a subsidiary whose P749 points at a government ministry.
    if _should_reject_match(parent_label, parent_desc):
        return {
            'canonical': label,
            'original': name,
            'relationship': 'parent',
            'wikidata_id': qid,
            'parent_id': None,
            'source': 'wikidata',
        }

    return {
        'canonical': parent_label,
        'original': name,
        'relationship': 'subsidiary_of',
        'wikidata_id': qid,
        'parent_id': parent_qid,
        'source': 'wikidata',
    }


def _resolve_company_rest(name: str) -> Dict[str, Any]:
    """Look up one company name via REST. Returns the same dict shape as
    `resolve_companies` per-name results.

    First tries the literal name with top-5 candidate filtering. If that
    returns not_found, generates a small set of alternate forms (suffix
    stripping) and retries each once. The first form that resolves AND
    passes the strict corporate-P31 check wins. The strict check on the
    retry path prevents suffix-stripping from landing on a wrong entity
    (e.g. PRATT INDUSTRIES → PRATT → Pratt Institute the school).
    """
    result = _resolve_company_one_query(name)
    if result['source'] in ('wikidata', 'error'):
        return result

    # Retry with alternate forms (e.g. "BLACKSTONE GROUP" → "BLACKSTONE").
    for alt in _alternate_employer_forms(name):
        retry = _resolve_company_one_query(alt)
        if retry['source'] == 'error':
            return retry  # circuit just opened — don't keep retrying
        if retry['source'] != 'wikidata':
            continue
        # Strict corporate-P31 gate: only accept the alternate form if
        # the matched entity is unambiguously corporate. Fetches entity
        # data once to inspect P31; cheap relative to the search itself.
        retry_qid = retry.get('wikidata_id')
        if retry_qid:
            ent = _entity_data(retry_qid)
            if ent is not None and not _entity_has_strict_corporate_p31(ent):
                continue
        # Preserve the original query name in `original`; the canonical
        # / wikidata_id come from the alternate-form match.
        retry['original'] = name
        return retry

    return result  # all forms returned not_found / failed strict check


def _resolve_company_safe(name: str) -> Dict[str, Any]:
    """ThreadPool worker: handles circuit-open shortcut + dispatches to
    _resolve_company_rest. Kept separate so the executor can map cleanly
    over names without lambda-in-pool surprises."""
    if _circuit_open:
        return {
            'canonical': name, 'original': name, 'relationship': 'self',
            'wikidata_id': None, 'parent_id': None, 'source': 'error',
        }
    return _resolve_company_rest(name)


def resolve_companies_rest(names: List[str]) -> Dict[str, Dict[str, Any]]:
    """REST-based per-name company resolution. Parallelized via ThreadPool
    (REST_PARALLEL_WORKERS workers). MediaWiki's REST endpoints handle
    concurrent requests well, and the per-call REST_RATE_LIMIT_DELAY
    naturally interleaves across threads.

    Stops early if the circuit breaker trips — names submitted after the
    trip get `source='error'` (not cached, will retry next run).
    """
    if not names:
        return {}
    with ThreadPoolExecutor(max_workers=REST_PARALLEL_WORKERS) as ex:
        # Preserve input ordering by using map() so result indices align.
        return {name: r for name, r in zip(names, ex.map(_resolve_company_safe, names))}


def _resolve_person_rest(name: str) -> Dict[str, Any]:
    """Look up one person name via REST. Returns same shape as
    resolve_people per-name results.

    Two-step: wbsearchentities for the person → Special:EntityData for
    their company-relationships. Captures founders (P112 inverse —
    people who founded a co), employer (P108), CEO_of (P169 inverse),
    etc. With per-call cost we collect a focused subset rather than the
    full UNION-of-5 the SPARQL path uses.
    """
    base = {'person': name, 'companies': [], 'primary_company': None, 'source': 'not_found'}

    hits = _wbsearchentities(name, limit=5)
    if hits is None:
        if _circuit_open:
            return {**base, 'source': 'error'}
        return base
    if not hits:
        return base

    # Pick the first hit that isn't a generic concept / government /
    # disambiguation page. Without top-N, short whale surnames like "WALL"
    # or "BANK" often hit a Wikimedia disambiguation page and never
    # resolve a real person.
    hit: Optional[Dict[str, Any]] = None
    for h in hits:
        if _should_reject_match(h.get('label'), h.get('description')):
            continue
        if h.get('id'):
            hit = h
            break
    if hit is None:
        return base
    qid = hit['id']

    entity = _entity_data(qid)
    if entity is None:
        return {**base, 'source': 'wikidata'}

    claims = entity.get('claims', {})
    found_companies: List[Dict[str, Any]] = []

    # P108 = employer (person → company they work at)
    for claim in claims.get('P108', []):
        cqid = _claim_qid(claim)
        if cqid:
            found_companies.append({'wikidata_id': cqid, 'relationship': 'employed_by'})

    # Companies founded — Wikidata stores this as P112 from the company's
    # side (founder = this person), not on the person. Skip without
    # adding a separate query for now (would double per-name cost).

    # P39 = position held — could include CEO/director roles. The 'of'
    # is in a qualifier (P642) that points to the company. Parse that
    # for ceo_of / manages.
    for claim in claims.get('P39', []):
        # qualifier P642 = of
        quals = claim.get('qualifiers', {})
        for q in quals.get('P642', []):
            try:
                cqid = q['datavalue']['value']['id']
                # Approximate: any P39 with a P642-of qualifier becomes
                # 'manages' unless we have evidence it's a CEO role.
                rel = 'manages'
                # Look at the position itself for hints.
                pos_qid = _claim_qid(claim)
                if pos_qid in ('Q484876', 'Q3387717'):  # Q484876=CEO, Q3387717=chief executive officer
                    rel = 'ceo_of'
                found_companies.append({'wikidata_id': cqid, 'relationship': rel})
            except (KeyError, TypeError):
                continue

    if not found_companies:
        return {**base, 'source': 'wikidata'}  # person exists in Wikidata but no co. links found

    # Resolve each company's English label
    seen = set()
    enriched = []
    for c in found_companies:
        cqid = c['wikidata_id']
        if cqid in seen:
            continue
        seen.add(cqid)
        ce = _entity_data(cqid)
        if ce is None:
            continue
        clabel = ce.get('labels', {}).get('en', {}).get('value')
        if not clabel:
            continue
        # Filter government / generic-concept entities at the
        # employer-target step. Catches cases like a public official
        # whose P108 (employer) is "United States federal government"
        # or "Government of the United Kingdom" — real Wikidata data
        # but not corporate attribution.
        cdesc = ce.get('descriptions', {}).get('en', {}).get('value')
        if _should_reject_match(clabel, cdesc):
            continue
        enriched.append({'name': clabel, 'wikidata_id': cqid, 'relationship': c['relationship']})

    if not enriched:
        return {**base, 'source': 'wikidata'}

    enriched.sort(key=lambda c: _RELATIONSHIP_PRIORITY.get(c['relationship'], 99))
    return {
        'person': name,
        'companies': enriched,
        'primary_company': enriched[0]['name'],
        'source': 'wikidata',
    }


def _resolve_person_safe(name: str) -> Dict[str, Any]:
    """ThreadPool worker for resolve_people_rest."""
    if _circuit_open:
        return {
            'person': name, 'companies': [], 'primary_company': None,
            'source': 'error',
        }
    return _resolve_person_rest(name)


def resolve_people_rest(names: List[str]) -> Dict[str, Dict[str, Any]]:
    """REST-based per-name person resolution. Parallelized via ThreadPool;
    see resolve_companies_rest for rationale."""
    if not names:
        return {}
    with ThreadPoolExecutor(max_workers=REST_PARALLEL_WORKERS) as ex:
        return {name: r for name, r in zip(names, ex.map(_resolve_person_safe, names))}


# ---------------------------------------------------------------------------
# SPARQL-based resolution (kept as fallback for when SPARQL is healthy)
# ---------------------------------------------------------------------------


def resolve_companies(names: List[str], chunk_size: int = 50) -> Dict[str, Dict[str, Any]]:
    """Batch-resolve company names to their canonical parent (or self).

    Returns a dict keyed by input name. Every input name appears in the
    output. Result schema:

        {
          'canonical':    str,      # parent name if subsidiary, else self
          'original':     str,      # the input name
          'relationship': str,      # 'parent' | 'subsidiary_of' | 'self'
          'wikidata_id':  str|None, # the matched company's Q-id
          'parent_id':    str|None, # the parent company's Q-id (if subsidiary_of)
          'source':       str,      # 'wikidata' | 'not_found' | 'error'
        }

    'not_found' is suitable for negative-cache storage (Wikidata responded
    but no match). 'error' means the request failed entirely — caller may
    want to retry next run rather than caching.
    """
    # Pre-populate every name as not_found; matched ones get overwritten.
    results: Dict[str, Dict[str, Any]] = {
        name: {
            'canonical': name,
            'original': name,
            'relationship': 'self',
            'wikidata_id': None,
            'parent_id': None,
            'source': 'not_found',
        }
        for name in names
    }

    if not names:
        return results

    for chunk in _chunked(names, chunk_size):
        query = _build_company_batch_query(chunk)
        response = _execute_sparql(query)
        if response is None:
            # Circuit open or retries exhausted. Mark this chunk as 'error'
            # so caller knows to retry next run rather than caching as
            # 'not_found'.
            for name in chunk:
                if results[name]['source'] == 'not_found':
                    results[name] = {**results[name], 'source': 'error'}
            continue

        for binding in response.get('results', {}).get('bindings', []):
            input_name = binding['inputName']['value']
            if input_name not in results:
                continue
            company_uri = binding['company']['value']
            company_id = company_uri.rsplit('/', 1)[-1]
            company_label = binding.get('companyLabel', {}).get('value', input_name)
            # Skip rows where the label resolved as a Q-id (entity has no
            # English label). Don't overwrite a previous valid match.
            if company_label.startswith('Q') and results[input_name]['source'] == 'wikidata':
                continue

            parent_id = None
            parent_label = None
            if 'parent' in binding:
                parent_uri = binding['parent']['value']
                parent_id = parent_uri.rsplit('/', 1)[-1]
                parent_label = binding.get('parentLabel', {}).get('value')
                if parent_label and parent_label.startswith('Q'):
                    parent_label = None  # unresolved entity label

            if parent_label:
                results[input_name] = {
                    'canonical': parent_label,
                    'original': input_name,
                    'relationship': 'subsidiary_of',
                    'wikidata_id': company_id,
                    'parent_id': parent_id,
                    'source': 'wikidata',
                }
            else:
                results[input_name] = {
                    'canonical': company_label,
                    'original': input_name,
                    'relationship': 'parent',
                    'wikidata_id': company_id,
                    'parent_id': None,
                    'source': 'wikidata',
                }

    return results


# ---------------------------------------------------------------------------
# Batched person resolution
# ---------------------------------------------------------------------------


def _build_person_batch_query(names: List[str]) -> str:
    """SPARQL query to find companies linked to N people in one round-trip.

    Captures founders (P112), employees (P108), owners (P127), managers
    (P1037), and CEOs (P169). The relationship is bound per-row so the
    Python side can rank by priority.
    """
    values = " ".join(f'"{_escape_sparql_literal(n)}"' for n in names)
    return f"""
    SELECT DISTINCT ?inputName ?company ?companyLabel ?role WHERE {{
      VALUES ?inputName {{ {values} }}
      ?person wdt:P31 wd:Q5 ;
              rdfs:label ?personLabel .
      FILTER(LANG(?personLabel) = "en" && LCASE(STR(?personLabel)) = LCASE(STR(?inputName)))
      {{
        ?company wdt:P112 ?person .
        BIND("founded" as ?role)
      }} UNION {{
        ?person wdt:P108 ?company .
        ?company wdt:P31/wdt:P279* wd:Q4830453 .
        BIND("employed_by" as ?role)
      }} UNION {{
        ?company wdt:P127 ?person .
        BIND("owns" as ?role)
      }} UNION {{
        ?person wdt:P1037 ?company .
        BIND("manages" as ?role)
      }} UNION {{
        ?company wdt:P169 ?person .
        BIND("ceo_of" as ?role)
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1000
    """


_RELATIONSHIP_PRIORITY = {
    'founded': 0,
    'ceo_of': 1,
    'owns': 2,
    'manages': 3,
    'employed_by': 4,
}


def resolve_people(names: List[str], chunk_size: int = 25) -> Dict[str, Dict[str, Any]]:
    """Batch-resolve people to their corporate connections.

    Returns dict keyed by input name. Every input name appears. Result schema:

        {
          'person':          str,              # input name
          'companies':       List[Dict],       # [{name, relationship, wikidata_id}]
          'primary_company': str|None,         # ranked by relationship priority
          'source':          'wikidata' | 'not_found' | 'error',
        }

    People are batched smaller (25 default) than companies because the
    UNION-of-5-properties query is heavier per name.
    """
    results: Dict[str, Dict[str, Any]] = {
        name: {
            'person': name,
            'companies': [],
            'primary_company': None,
            'source': 'not_found',
        }
        for name in names
    }
    if not names:
        return results

    for chunk in _chunked(names, chunk_size):
        query = _build_person_batch_query(chunk)
        response = _execute_sparql(query)
        if response is None:
            for name in chunk:
                if results[name]['source'] == 'not_found':
                    results[name] = {**results[name], 'source': 'error'}
            continue

        # Group rows by input name; each name gets a list of (company, role)
        per_name: Dict[str, List[Dict[str, Any]]] = {n: [] for n in chunk}
        for binding in response.get('results', {}).get('bindings', []):
            input_name = binding['inputName']['value']
            if input_name not in per_name:
                continue
            company_label = binding.get('companyLabel', {}).get('value', '')
            if not company_label or company_label.startswith('Q'):
                continue
            company_id = binding['company']['value'].rsplit('/', 1)[-1]
            role = binding['role']['value']
            per_name[input_name].append({
                'name': company_label,
                'relationship': role,
                'wikidata_id': company_id,
            })

        for name, companies in per_name.items():
            if not companies:
                continue
            # Dedupe by (name, role)
            seen = set()
            unique = []
            for c in companies:
                key = (c['name'], c['relationship'])
                if key in seen:
                    continue
                seen.add(key)
                unique.append(c)
            unique.sort(key=lambda c: _RELATIONSHIP_PRIORITY.get(c['relationship'], 99))
            results[name] = {
                'person': name,
                'companies': unique,
                'primary_company': unique[0]['name'] if unique else None,
                'source': 'wikidata',
            }

    return results


# ---------------------------------------------------------------------------
# Backward-compatible single-name shims
# ---------------------------------------------------------------------------


def resolve_company_to_canonical(company_name: str) -> Dict[str, Any]:
    """Single-name shim. Prefer resolve_companies() for bulk work."""
    return resolve_companies([company_name])[company_name]


def resolve_person_to_companies(person_name: str) -> Dict[str, Any]:
    """Single-name shim. Prefer resolve_people() for bulk work."""
    return resolve_people([person_name])[person_name]


# Legacy dataclasses retained for any external callers; not used in the
# bulk path. Internal asset code should use the dict-based API above.

@dataclass
class CompanyInfo:
    wikidata_id: str
    name: str
    parent: Optional[str] = None
    parent_id: Optional[str] = None
    subsidiaries: Optional[List[str]] = None
    aliases: Optional[List[str]] = None


@dataclass
class PersonCompanyLink:
    person_name: str
    company_name: str
    company_id: str
    relationship: str


if __name__ == '__main__':
    print("=" * 70)
    print("WIKIDATA CLIENT — BATCHED API SMOKE TEST")
    print("=" * 70)
    companies = resolve_companies(['Google', 'YouTube', 'WhatsApp', 'LinkedIn', 'Tesla'])
    print("\n--- COMPANIES ---")
    for name, r in companies.items():
        print(f"{name} → {r['canonical']} ({r['relationship']}, source={r['source']})")
    people = resolve_people(['Elon Musk', 'Timothy Mellon', 'Jan Koum'])
    print("\n--- PEOPLE ---")
    for name, r in people.items():
        print(f"{name}: source={r['source']}, primary={r['primary_company']}")
        for c in r['companies'][:3]:
            print(f"  - {c['name']} ({c['relationship']})")
