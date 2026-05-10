"""GLEIF (Global Legal Entity Identifier) client.

Fallback corporate-identity resolver for entities that don't appear in
Wikidata. GLEIF is the official registry behind the LEI (Legal Entity
Identifier) standard, mandated by financial regulators worldwide. Free,
no auth required, authoritative for legal entity data.

Coverage characteristics vs Wikidata:
- BETTER for: small US private companies with LEIs (Pratt Industries Inc.,
  Uline Inc., regional banks, professional services firms). These
  routinely don't have Wikipedia articles, so Wikidata's coverage is
  sparse. LEI registration is required for many financial transactions
  so US private companies often have one.
- WORSE for: famous corporations whose Wikipedia articles dominate
  search ranking but whose LEI search returns subsidiary funds (Goldman
  Sachs returns 1700 sub-funds; Wikidata returns the parent cleanly).
- NEITHER for: very small entities below the LEI registration threshold
  (Adelson Drug Clinic, ad-hoc LLCs, certain private foundations).

Use case here: ONLY as a fallback after `wikidata.reconci.link` returns
not_found. Strictly filtered to high-confidence matches:
  - At least one ACTIVE US-jurisdiction record
  - Legal name post-suffix-strip is an exact or near-exact match to
    the FEC employer string (post-suffix-strip)
  - Single unambiguous match (multiple equally-good US matches → defer
    to not_found rather than guess)

These constraints are tight on purpose. GLEIF returns lots of noise on
short ambiguous queries (KKR returns 1094 mostly-Indian-company hits;
CITADEL returns 429 mostly Dutch B.V.s). For our use case it's better
to miss than to misattribute corporate money.

Rate limit: GLEIF doesn't publish a strict numeric limit but asks for
reasonable use. We self-throttle to ~3 req/s with parallel workers.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

GLEIF_ENDPOINT = "https://api.gleif.org/api/v1/lei-records"
USER_AGENT = "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"

REQUEST_TIMEOUT = 15
PARALLEL_WORKERS = 3  # ~3 req/s steady-state across workers
RATE_LIMIT_DELAY = 0.3
MAX_RETRIES = 3


# Legal-suffix tokens to strip from BOTH the input FEC name and the
# GLEIF legal_name before exact-equality comparison. Wider than the
# employer_normalization set because GLEIF carries jurisdiction-specific
# suffixes ("INC.", "L.L.C.", "S.à r.l.", "B.V.", "PLC", etc.).
_STRIP_SUFFIX_TOKENS = (
    "inc", "incorporated", "corp", "corporation", "co", "company",
    "ltd", "limited", "lp", "llp", "llc", "plc", "pllc", "pa", "pc",
    "na", "gmbh", "ag", "sa", "nv", "bv", "kg", "pty", "lc",
    "lc.", "inc.", "corp.", "co.", "ltd.", "lp.", "llp.", "llc.",
)


def _strip_legal_suffixes(name: str) -> str:
    """Remove trailing legal-form suffixes and trailing punctuation.
    Lower-cases for comparison."""
    s = (name or "").strip().lower()
    s = re.sub(r"[.,;:]+$", "", s)
    # Iteratively strip trailing tokens that are legal forms.
    changed = True
    while changed:
        changed = False
        for suf in _STRIP_SUFFIX_TOKENS:
            pat = r"[\s,]+" + re.escape(suf) + r"[.,]?$"
            new_s = re.sub(pat, "", s)
            if new_s != s:
                s = new_s
                changed = True
    return s.strip()


@dataclass
class GleifRecord:
    """One LEI record."""
    lei: str
    legal_name: str
    country: Optional[str]
    status: str  # 'ACTIVE' | 'INACTIVE' | 'NULL' | other
    parent_lei: Optional[str] = None
    company_type: Optional[str] = None


@dataclass
class GleifResolution:
    """Result of resolving one FEC name via GLEIF."""
    original: str
    canonical: str
    lei: Optional[str]
    country: Optional[str]
    source: str  # 'gleif' | 'not_found' | 'ambiguous' | 'error'
    method: str = ""
    alternatives: List[Dict[str, Any]] = field(default_factory=list)


def _gleif_get(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    backoff = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_LIMIT_DELAY)
            r = requests.get(
                GLEIF_ENDPOINT,
                params=params,
                headers={
                    "Accept": "application/vnd.api+json",
                    "User-Agent": USER_AGENT,
                },
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 429:
                logger.warning("gleif 429 (attempt %d), backing off %.1fs", attempt + 1, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.warning("gleif request failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    return None


def _parse_record(raw: Dict[str, Any]) -> GleifRecord:
    attrs = raw.get("attributes", {}) or {}
    entity = attrs.get("entity", {}) or {}
    legal = entity.get("legalName", {}) or {}
    addr = entity.get("legalAddress", {}) or {}
    rels = raw.get("relationships", {}) or {}
    parent_rel = rels.get("direct-parent", {}).get("data") or {}
    return GleifRecord(
        lei=raw.get("id", ""),
        legal_name=legal.get("name", ""),
        country=addr.get("country"),
        status=entity.get("status", ""),
        parent_lei=parent_rel.get("id") if parent_rel else None,
        company_type=entity.get("category"),
    )


def search(name: str, page_size: int = 10) -> List[GleifRecord]:
    """Full-text search GLEIF for `name`. Returns up to `page_size`
    records. Empty list on no results or transient failure."""
    if not name or not name.strip():
        return []
    response = _gleif_get({"filter[fulltext]": name, "page[size]": page_size})
    if response is None:
        return []
    return [_parse_record(r) for r in (response.get("data") or [])]


# ---------------------------------------------------------------------------
# Strict-match resolver
# ---------------------------------------------------------------------------


def _names_match(fec_name: str, gleif_legal_name: str) -> bool:
    """Two strings represent the same entity, after stripping legal
    suffixes and punctuation, ignoring case. Strict — no fuzzy/Jaccard.
    """
    a = _strip_legal_suffixes(fec_name)
    b = _strip_legal_suffixes(gleif_legal_name)
    if not a or not b:
        return False
    return a == b


def resolve_one(fec_name: str) -> GleifResolution:
    """Resolve one FEC employer name via GLEIF with strict acceptance:
    (1) at least one ACTIVE record exists, (2) its legal_name matches
    the FEC name post-suffix-strip, (3) US jurisdiction preferred but
    not required, (4) ambiguity (multiple distinct match candidates
    after strict match) → return ambiguous rather than guess."""
    records = search(fec_name)
    alts = [
        {"lei": r.lei, "name": r.legal_name, "country": r.country, "status": r.status}
        for r in records[:5]
    ]
    if not records:
        return GleifResolution(
            original=fec_name, canonical=fec_name, lei=None, country=None,
            source="not_found", method="empty_results",
            alternatives=alts,
        )

    # Filter: strict name match + ACTIVE.
    matches = [r for r in records if r.status == "ACTIVE" and _names_match(fec_name, r.legal_name)]

    if not matches:
        return GleifResolution(
            original=fec_name, canonical=fec_name, lei=None, country=None,
            source="not_found", method="no_strict_match",
            alternatives=alts,
        )

    # Prefer US jurisdiction; if multiple candidates, we want exactly one.
    us_matches = [r for r in matches if r.country == "US"]
    if len(us_matches) == 1:
        chosen = us_matches[0]
        return GleifResolution(
            original=fec_name,
            canonical=chosen.legal_name,
            lei=chosen.lei,
            country=chosen.country,
            source="gleif",
            method="strict_match_us",
            alternatives=alts,
        )
    if len(us_matches) > 1:
        # Multiple US ACTIVE strict matches → ambiguous (e.g., a name like
        # "ACME CORP" registered in DE, NV, and CA). Don't guess.
        return GleifResolution(
            original=fec_name, canonical=fec_name, lei=None, country=None,
            source="ambiguous", method=f"{len(us_matches)}_us_active_matches",
            alternatives=alts,
        )

    # No US match but at least one strict match elsewhere — accept only
    # if exactly one. Foreign matches for FEC employers are unusual but
    # legitimate (e.g., a foreign-domiciled investment fund's US donors).
    if len(matches) == 1:
        chosen = matches[0]
        return GleifResolution(
            original=fec_name,
            canonical=chosen.legal_name,
            lei=chosen.lei,
            country=chosen.country,
            source="gleif",
            method=f"strict_match_{chosen.country or 'unk'}",
            alternatives=alts,
        )

    return GleifResolution(
        original=fec_name, canonical=fec_name, lei=None, country=None,
        source="ambiguous", method=f"{len(matches)}_active_matches",
        alternatives=alts,
    )


def resolve_batch(names: List[str]) -> Dict[str, GleifResolution]:
    """Parallelize GLEIF lookups across `PARALLEL_WORKERS` threads.
    Output keyed by input name."""
    if not names:
        return {}
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        results = list(ex.map(resolve_one, names))
    return {names[i]: results[i] for i in range(len(names))}


if __name__ == "__main__":
    cases = [
        "ULINE",
        "PRATT INDUSTRIES",
        "ADELSON DRUG CLINIC",
        "FAHR LLC",
        "BRIDGEWATER ASSOCIATES",
        "CITADEL",  # known-ambiguous via GLEIF
        "GOLDMAN SACHS",  # known-ambiguous (1700 hits)
        "KKR",  # known-ambiguous
    ]
    t = time.time()
    results = resolve_batch(cases)
    dt = time.time() - t
    print(f"Resolved {len(cases)} names in {dt:.2f}s\n")
    for name in cases:
        r = results[name]
        flag = {
            "gleif": "✓",
            "not_found": " ",
            "ambiguous": "?",
            "error": "✗",
        }.get(r.source, "?")
        print(f"  {flag} {name!r:25} -> source={r.source:11} method={r.method:30} canonical={r.canonical!r}")
        if r.lei:
            print(f"     LEI={r.lei}  country={r.country}")
