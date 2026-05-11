"""Low-level Wikidata MediaWiki REST client.

Thin transport layer used by:
  - `wikidata_reconci.py` indirectly (it talks to reconci.link, not these
    endpoints, but reuses the User-Agent string and shares retry/circuit
    breaker patterns)
  - The whale (person → company) resolution path in
    `assets/enrichment/wikidata_resolution.py`, which still uses the
    pre-simplification filter chain (`_should_reject_match`,
    `_entity_has_non_corporate_p31`, etc.). When the whale path gets the
    same simplification employer resolution received in May 2026, most of
    this file becomes deletable too.

Wikidata REST endpoints we hit:
  - https://www.wikidata.org/w/api.php  (wbsearchentities)
  - https://www.wikidata.org/wiki/Special:EntityData/<Q-id>.json

SPARQL query service (`query.wikidata.org/sparql`) was removed
2026-05-11 — see decisions log. The Blazegraph endpoint had been
unreliable for months and we have better alternatives (the
reconciliation API + GLEIF) for the legal-tender use cases.

Reliability:
- Exponential backoff on transient failures (429/5xx); 429s do NOT
  trip the circuit breaker (they're "slow down", not "service down")
- Circuit breaker opens after CIRCUIT_BREAKER_THRESHOLD consecutive
  non-429 failures; caller sees `None` rather than blocking
- Per-Q-id locking elsewhere (e.g. ontology walker, when present) is
  the caller's responsibility — this module's state is process-global
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

WIKIDATA_API_ENDPOINT = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_ENDPOINT = "https://www.wikidata.org/wiki/Special:EntityData"
USER_AGENT = "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"

REST_TIMEOUT = 15
REST_RATE_LIMIT_DELAY = 0.05
REST_PARALLEL_WORKERS = 4
MAX_BACKOFF = 60.0
MAX_RETRIES = 5
CIRCUIT_BREAKER_THRESHOLD = 10

# Module-level circuit-breaker state. Reset on each successful request;
# tripped after CIRCUIT_BREAKER_THRESHOLD consecutive non-429 failures.
_consecutive_failures = 0
_circuit_open = False


class WikidataCircuitOpen(RuntimeError):
    """Raised when the circuit breaker has tripped — Wikidata unreachable."""


def reset_circuit_breaker() -> None:
    """Re-arm the circuit. Call between asset runs."""
    global _consecutive_failures, _circuit_open
    _consecutive_failures = 0
    _circuit_open = False


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _execute_rest(url: str, params: Dict[str, Any], timeout: float = REST_TIMEOUT) -> Optional[Dict[str, Any]]:
    """Issue a request to a Wikidata REST endpoint with backoff + circuit
    breaker. 429 responses don't trip the circuit (they're rate-limit
    signals, not outages); only RequestException (5xx, connection errors,
    timeouts) increment the failure counter."""
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
            logger.warning(
                "Wikidata REST %s failed (attempt %d/%d): %s",
                url, attempt + 1, MAX_RETRIES, e,
            )
    if not last_was_429:
        _consecutive_failures += 1
        if _consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            _circuit_open = True
            logger.error("Wikidata circuit breaker tripped: %s", last_exc)
    return None


def _wbsearchentities(name: str, type_filter: str = "item", limit: int = 1) -> Optional[List[Dict[str, Any]]]:
    """Look up a name via MediaWiki's wbsearchentities. Returns up to
    `limit` candidate dicts ({id, label, description, ...}), or None
    on request failure."""
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
    """Fetch full entity data (claims, labels, descriptions, sitelinks)
    for a Q-id. Returns the entity dict or None."""
    response = _execute_rest(
        f"{WIKIDATA_ENTITY_ENDPOINT}/{qid}.json",
        params={},
    )
    if not response:
        return None
    return response.get("entities", {}).get(qid)


def _claim_qid(claim: Dict[str, Any]) -> Optional[str]:
    """Extract the target Q-id from a Wikidata claim, or None if not a
    Q-id-typed claim."""
    try:
        return claim["mainsnak"]["datavalue"]["value"]["id"]
    except (KeyError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Whale (person → company) resolution
#
# Used by the wikidata_corporate_resolution Dagster asset to map whale
# donors (people who maxed out) to their corporate connections via
# Wikidata's P108 (employer) and P39 (position held) properties.
#
# Still uses the pre-simplification filter chain — when the whale path
# gets the same minimal-resolver treatment that employer resolution
# received in May 2026, most of this section becomes deletable.
# ---------------------------------------------------------------------------


P_PARENT_ORGANIZATION = "P749"

# Description-keyword patterns that flag generic-concept Wikidata
# matches as not-real-entities. Used by the whale path's filter chain.
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
    "groups using advocacy",
    "vaccine ",
    "creative commons license",
    "chemical compound",
    "biographical database",
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
    """Reject Wikidata matches that look like generic concepts rather
    than specific entities. Used by the whale path."""
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
    """Reject government / sovereign entities from being treated as
    employers. Used by the whale path."""
    if not description:
        return False
    d = description.lower()
    for pattern in _GOVERNMENT_DESCRIPTION_PATTERNS:
        if pattern in d:
            return True
    return False


def _should_reject_match(label: Optional[str], description: Optional[str]) -> bool:
    """Combined filter: generic-concept OR government entity. Whale path."""
    return _is_generic_match(label, description) or _is_government_entity(label, description)


# P31 (instance-of) Q-ids that disqualify a candidate from being treated
# as a corporation. Used by the whale path's filter chain.
_NON_CORPORATE_P31 = {
    "Q5",  # human
    "Q4167410",  # Wikimedia disambiguation page
    "Q4167836",  # Wikimedia category
    "Q13406463",  # Wikimedia list article
    "Q571", "Q11424", "Q482994", "Q7725634", "Q15239622",
    "Q47461344", "Q5398426", "Q24856", "Q277759", "Q41298",
    "Q108381", "Q19479619",
    "Q134556", "Q7366", "Q386724",
    "Q6256", "Q3624078", "Q15634554", "Q56061", "Q46395",
    "Q484170", "Q727", "Q515", "Q23397", "Q4022", "Q486972",
    "Q3957", "Q532",
    "Q34770", "Q33829", "Q133327", "Q284465", "Q41710",
    "Q134808", "Q105967696", "Q12140", "Q11173", "Q113145171",
    "Q11879003", "Q11879590", "Q12308941", "Q3409032", "Q101352",
    "Q205663", "Q1190554", "Q11862829",
    "Q2811", "Q11446", "Q170382",
    "Q177682", "Q207621", "Q284742",
    "Q15987302", "Q108300140", "Q189533", "Q4611891",
    "Q17444909", "Q850950",
    "Q12973014", "Q476028", "Q847017", "Q4438121", "Q15944511",
    "Q53538476", "Q14435", "Q15873", "Q15976457", "Q1078541",
    "Q15145537", "Q107343049", "Q124130104",
    "Q7187", "Q8054", "Q16521", "Q713623",
    "Q936518",
    "Q7889",
    "Q116457956", "Q15284", "Q1799794", "Q4790", "Q5124673",
    "Q667509", "Q13539802", "Q262882", "Q871419", "Q41176",
    "Q68723978", "Q82047057", "Q21684377", "Q47574",
    "Q98579904",
}


def _entity_has_non_corporate_p31(entity: Dict[str, Any]) -> bool:
    """True if any P31 (instance of) on the entity is in the non-corporate
    blacklist. Used by the whale path to reject Wikidata hits that aren't
    companies — books, films, vaccines, countries, songs, people."""
    claims = entity.get("claims", {})
    for claim in claims.get("P31", []):
        if _claim_qid(claim) in _NON_CORPORATE_P31:
            return True
    return False


_RELATIONSHIP_PRIORITY = {
    "founded": 0,
    "ceo_of": 1,
    "owns": 2,
    "manages": 3,
    "employed_by": 4,
}


def _resolve_person_rest(name: str) -> Dict[str, Any]:
    """Look up one person via REST. Returns same shape as resolve_people
    per-name results.

    Two-step: wbsearchentities for the person → Special:EntityData for
    their company-relationships. Captures founders (P112), employer
    (P108), CEO_of (P169), manages (P1037), owns (P127)."""
    base = {"person": name, "companies": [], "primary_company": None, "source": "not_found"}

    hits = _wbsearchentities(name, limit=5)
    if hits is None:
        if _circuit_open:
            return {**base, "source": "error"}
        return base
    if not hits:
        return base

    # Pick the first hit that isn't a generic concept / government /
    # disambiguation page.
    hit: Optional[Dict[str, Any]] = None
    for h in hits:
        if _should_reject_match(h.get("label"), h.get("description")):
            continue
        if h.get("id"):
            hit = h
            break
    if hit is None:
        return base
    qid = hit["id"]

    entity = _entity_data(qid)
    if entity is None:
        return {**base, "source": "wikidata"}

    claims = entity.get("claims", {})
    found_companies: List[Dict[str, Any]] = []

    # P108 = employer
    for claim in claims.get("P108", []):
        cqid = _claim_qid(claim)
        if cqid:
            found_companies.append({"wikidata_id": cqid, "relationship": "employed_by"})

    # P39 = position held; qualifier P642 ("of") points at the org
    for claim in claims.get("P39", []):
        quals = claim.get("qualifiers", {})
        for q in quals.get("P642", []):
            try:
                cqid = q["datavalue"]["value"]["id"]
                rel = "manages"
                pos_qid = _claim_qid(claim)
                if pos_qid in ("Q484876", "Q3387717"):  # CEO Q-ids
                    rel = "ceo_of"
                found_companies.append({"wikidata_id": cqid, "relationship": rel})
            except (KeyError, TypeError):
                continue

    if not found_companies:
        return {**base, "source": "wikidata"}

    # Resolve each company's English label + filter non-employer types
    seen = set()
    enriched = []
    for c in found_companies:
        cqid = c["wikidata_id"]
        if cqid in seen:
            continue
        seen.add(cqid)
        ce = _entity_data(cqid)
        if ce is None:
            continue
        clabel = ce.get("labels", {}).get("en", {}).get("value")
        if not clabel:
            continue
        cdesc = ce.get("descriptions", {}).get("en", {}).get("value")
        if _should_reject_match(clabel, cdesc):
            continue
        enriched.append({"name": clabel, "wikidata_id": cqid, "relationship": c["relationship"]})

    if not enriched:
        return {**base, "source": "wikidata"}

    enriched.sort(key=lambda c: _RELATIONSHIP_PRIORITY.get(c["relationship"], 99))
    return {
        "person": name,
        "companies": enriched,
        "primary_company": enriched[0]["name"],
        "source": "wikidata",
    }


def _resolve_person_safe(name: str) -> Dict[str, Any]:
    """ThreadPool worker for resolve_people_rest."""
    if _circuit_open:
        return {
            "person": name, "companies": [], "primary_company": None,
            "source": "error",
        }
    return _resolve_person_rest(name)


def resolve_people_rest(names: List[str]) -> Dict[str, Dict[str, Any]]:
    """Resolve many people in parallel. Used by the whale path."""
    if not names:
        return {}
    with ThreadPoolExecutor(max_workers=REST_PARALLEL_WORKERS) as ex:
        return {name: r for name, r in zip(names, ex.map(_resolve_person_safe, names))}
