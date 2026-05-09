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
USER_AGENT = "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"
REQUEST_TIMEOUT = 60  # batched queries are heavier than single-name queries
RATE_LIMIT_DELAY = 1.0  # base inter-request sleep
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

    For each input name, returns the matched company entity (case-insensitive
    label match against the business-entity ontology) plus its parent (P749)
    if any. Names with no match simply don't appear in the result rows.
    """
    values = " ".join(f'"{_escape_sparql_literal(n)}"' for n in names)
    return f"""
    SELECT ?inputName ?company ?companyLabel ?parent ?parentLabel WHERE {{
      VALUES ?inputName {{ {values} }}
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      ?company rdfs:label ?label .
      FILTER(LANG(?label) = "en" && LCASE(STR(?label)) = LCASE(STR(?inputName)))
      OPTIONAL {{ ?company wdt:P749 ?parent . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """


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
