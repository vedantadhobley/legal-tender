"""Low-level Wikidata MediaWiki REST client.

Thin transport layer. Provides:
  - HTTP transport with retry/backoff and a process-global circuit breaker
  - `_entity_data`: Q-id → full entity JSON (used by `whale_resolver` to
    follow corporate-relationship properties on person entities)
  - `_claim_qid`: extract a Q-id target from a Wikidata claim

Used by:
  - `whale_resolver` for entity-data fetches when following P108/P1830/P39

Endpoint:
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
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

WIKIDATA_ENTITY_ENDPOINT = "https://www.wikidata.org/wiki/Special:EntityData"
USER_AGENT = "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"

REST_TIMEOUT = 15
REST_RATE_LIMIT_DELAY = 0.05
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


def fetch_upstream_qids(qid: str) -> Optional[Dict[str, List[str]]]:
    """Fetch upstream relationship Q-ids for an entity:
      - P112 founder
      - P127 owned by
      - P749 parent organization

    Returns dict with keys 'p112', 'p127', 'p749' (each a list of Q-ids,
    possibly empty). Returns None on fetch failure so the caller can
    retry next run without poisoning the cache."""
    entity = _entity_data(qid)
    if not entity:
        return None
    claims = entity.get("claims", {})
    out: Dict[str, List[str]] = {"p112": [], "p127": [], "p749": []}
    for prop_key, out_key in (("P112", "p112"), ("P127", "p127"), ("P749", "p749")):
        for claim in claims.get(prop_key, []):
            target = _claim_qid(claim)
            if target:
                out[out_key].append(target)
    return out

