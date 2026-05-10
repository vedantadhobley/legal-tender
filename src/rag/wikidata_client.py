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
REST_RATE_LIMIT_DELAY = 0.1  # MediaWiki REST is more lenient than SPARQL
MAX_BACKOFF = 60.0
MAX_RETRIES = 3
CIRCUIT_BREAKER_THRESHOLD = 3  # consecutive failures before tripping

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
    query service and stay reliable when SPARQL is overloaded."""
    global _consecutive_failures, _circuit_open
    if _circuit_open:
        return None
    last_exc: Optional[BaseException] = None
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
                last_exc = requests.HTTPError("429")
                continue
            response.raise_for_status()
            _consecutive_failures = 0
            return response.json()
        except requests.RequestException as e:
            last_exc = e
            logger.warning(f"Wikidata REST {url} failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
    _consecutive_failures += 1
    if _consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
        _circuit_open = True
        logger.error(f"Wikidata circuit breaker tripped: {last_exc}")
    return None


def _wbsearchentities(name: str, type_filter: str = "item") -> Optional[Dict[str, Any]]:
    """Look up a name via MediaWiki's wbsearchentities API. Returns the
    top match dict ({id, label, description, ...}) or None.

    `type_filter` can narrow to 'property', 'item' (default — entities,
    which includes companies, people, and everything else), 'lexeme',
    'form', 'sense'."""
    response = _execute_rest(
        WIKIDATA_API_ENDPOINT,
        {
            "action": "wbsearchentities",
            "search": name,
            "language": "en",
            "format": "json",
            "limit": 1,
            "type": type_filter,
        },
    )
    if not response:
        return None
    results = response.get("search", [])
    return results[0] if results else None


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


def _resolve_company_rest(name: str) -> Dict[str, Any]:
    """Look up one company name via REST. Returns the same dict shape as
    `resolve_companies` per-name results.

    Two-step: wbsearchentities for label match → Special:EntityData for
    parent claims. If parent (P749 or P127) exists, resolve to parent's
    label; else canonical = the entity itself.
    """
    base = {
        'canonical': name,
        'original': name,
        'relationship': 'self',
        'wikidata_id': None,
        'parent_id': None,
        'source': 'not_found',
    }
    hit = _wbsearchentities(name)
    if hit is None:
        # Distinguish 'not_found' (search returned nothing) from 'error'
        # (request failed). _execute_rest returns None on both, so we
        # check circuit state to disambiguate.
        if _circuit_open:
            return {**base, 'source': 'error'}
        return base

    qid = hit.get('id')
    if not qid:
        return base
    label = hit.get('label') or name

    # Reject generic-concept matches (e.g. "SIG" → "advocacy group",
    # "ATT" → "lawyer"). Better to mark not_found than to attribute
    # corporate money to an abstract concept.
    if _is_generic_match(label, hit.get('description')):
        return base

    entity = _entity_data(qid)
    if entity is None:
        # Got the search hit but couldn't fetch full entity. Mark as
        # wikidata-found but with no parent — partial-success so it's
        # still cacheable.
        return {
            'canonical': label,
            'original': name,
            'relationship': 'parent',
            'wikidata_id': qid,
            'parent_id': None,
            'source': 'wikidata',
        }

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
    if parent_entity:
        parent_label = parent_entity.get('labels', {}).get('en', {}).get('value')
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

    return {
        'canonical': parent_label,
        'original': name,
        'relationship': 'subsidiary_of',
        'wikidata_id': qid,
        'parent_id': parent_qid,
        'source': 'wikidata',
    }


def resolve_companies_rest(names: List[str]) -> Dict[str, Dict[str, Any]]:
    """REST-based per-name company resolution. Slower per-name than
    `resolve_companies` (the SPARQL batched version) but reliable when
    the SPARQL endpoint is overloaded. ~0.2s per name.

    Stops early if the circuit breaker trips — remaining names get
    `source='error'` (not cached, will retry next run).
    """
    results: Dict[str, Dict[str, Any]] = {}
    for name in names:
        if _circuit_open:
            results[name] = {
                'canonical': name, 'original': name, 'relationship': 'self',
                'wikidata_id': None, 'parent_id': None, 'source': 'error',
            }
            continue
        results[name] = _resolve_company_rest(name)
    return results


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

    hit = _wbsearchentities(name)
    if hit is None:
        if _circuit_open:
            return {**base, 'source': 'error'}
        return base
    qid = hit.get('id')
    if not qid:
        return base

    # Reject generic-concept matches for people too — e.g. some surnames
    # match "Wikimedia disambiguation page" or generic-concept Q-ids.
    if _is_generic_match(hit.get('label'), hit.get('description')):
        return base

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


def resolve_people_rest(names: List[str]) -> Dict[str, Dict[str, Any]]:
    """REST-based per-name person resolution. See resolve_companies_rest."""
    results: Dict[str, Dict[str, Any]] = {}
    for name in names:
        if _circuit_open:
            results[name] = {
                'person': name, 'companies': [], 'primary_company': None,
                'source': 'error',
            }
            continue
        results[name] = _resolve_person_rest(name)
    return results


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
