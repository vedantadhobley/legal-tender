"""Whale (person → company) resolver.

For each whale donor name, find the Wikidata person entity and follow
corporate-relationship properties (founded, CEO of, employer, etc.) to
the companies they're connected to. Output feeds `whale_corporate_links`
and the `corporate_families` aggregation.

Pipeline:
    1. Reconcile name via reconci.link (no type filter — person names are
       distinctive enough that the top hit is reliable on its own)
    2. Accept top candidate via the same HIGH/LOW threshold + corroboration
       rule used for employers (`wikidata_resolver._accept_candidate`)
    3. Fetch person entity data via REST `Special:EntityData`
    4. Extract companies via a fixed set of corporate-relationship
       properties (P112 founder, P169 CEO, P108 employer, etc.) — these
       are by definition person→organization edges in Wikidata's schema,
       so no candidate-side filtering needed
    5. Fetch each company's English label

No description-keyword filter, no government-entity blacklist, no
non-corporate P31 set. Same minimalist trade-off as the employer path:
trust the source, surface provenance, accept occasional bogus matches
(visible in `by_organization` cross-cuts) over false negatives.

Output shape is backward-compatible with the previous
`resolve_people_rest` for drop-in replacement in
`wikidata_resolution.py`:

    {
        "person": str,           # input name
        "companies": [{"name": str, "wikidata_id": str, "relationship": str}, ...],
        "primary_company": str | None,
        "source": "wikidata" | "not_found",
        # Additional provenance fields (ignored by the asset, kept for
        # cache + debugging):
        "person_qid": str | None,
        "confidence": float,
        "method": str,
        "alternatives": [{"qid": str, "name": str, "score": float}, ...],
    }
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.rag.wikidata_reconci import ReconciCandidate, reconcile_batch
from src.rag.wikidata_client import _entity_data, _claim_qid
from src.rag.wikidata_resolver import _accept_candidate

logger = logging.getLogger(__name__)

# Wikidata properties that link a person to an organization. Each is
# unambiguously a "person X has corporate relationship Y to org Z" edge
# in the schema, so the target Q-id is reliably an organization and we
# don't need to filter the company side.
#
# Order in this dict doesn't matter — we use _RELATIONSHIP_PRIORITY
# below to sort the final output.
_PERSON_TO_COMPANY_PROPS: Dict[str, str] = {
    "P1830": "owns",             # owner of — person→company (right direction)
    "P112": "founded",           # founder of
    "P169": "ceo_of",            # chief executive officer
    "P488": "chair_of",          # chairperson
    "P1037": "manages",          # director / manager
    "P3320": "board_member_of",  # board member of
    "P108": "employed_by",       # employer
    # Deliberately NOT included: P127 ("owned by") — that's company→person,
    # not the direction we want when starting from a person entity.
}

# Determines which relationship "wins" if a person has multiple links to
# the same company (founder beats employee for the primary_company field).
_RELATIONSHIP_PRIORITY = {
    "founded": 0,
    "ceo_of": 1,
    "chair_of": 2,
    "owns": 3,
    "board_member_of": 4,
    "manages": 5,
    "employed_by": 6,
}


# Position Q-ids that we treat as "CEO of" when they appear as the value
# of a P39 (position held) claim. The org being held-CEO-of is the P642
# ("of") qualifier on the same claim. Wikidata stores executive roles
# this way rather than as direct P169-inverse, so without walking P39 we
# miss founders/CEOs like Kenneth Griffin (Citadel) and John Arnold
# (Centaurus Capital). Anything else in a P39+P642 pair defaults to
# the generic "manages" relationship.
_CEO_POSITION_QIDS = {
    "Q484876",   # chief executive officer
    "Q3387717",  # CEO (alt)
}


def _extract_company_qids(entity: Dict[str, Any]) -> List[Dict[str, str]]:
    """Read corporate-relationship claims from a person entity. Returns
    list of {wikidata_id, relationship} dicts (companies may repeat
    across relationships — deduped later).

    Two extraction modes:
      - Direct: P108/P112/P169/etc claim → target Q-id IS the company
      - Positional: P39 (position held) claim → P642 qualifier IS the
        company (Wikidata's idiom for "X holds position Y at org Z")
    """
    claims = entity.get("claims", {})
    out: List[Dict[str, str]] = []

    # Direct corporate-relationship claims.
    for prop, rel in _PERSON_TO_COMPANY_PROPS.items():
        for claim in claims.get(prop, []):
            cqid = _claim_qid(claim)
            if cqid:
                out.append({"wikidata_id": cqid, "relationship": rel})

    # P39 (position held) with P642 ("of") qualifier. Catches "Kenneth
    # Griffin → founder/CEO of Citadel" cases where the person entity
    # stores the role as a position-with-qualifier rather than a direct
    # P108 link.
    for claim in claims.get("P39", []):
        position_qid = _claim_qid(claim)
        quals = claim.get("qualifiers", {}) or {}
        for q in quals.get("P642", []):
            try:
                cqid = q["datavalue"]["value"]["id"]
            except (KeyError, TypeError):
                continue
            rel = "ceo_of" if position_qid in _CEO_POSITION_QIDS else "manages"
            out.append({"wikidata_id": cqid, "relationship": rel})

    return out


def _enrich_companies(raw: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """For each Q-id, fetch the entity and pull its English label.
    Deduplicates by Q-id, keeping the highest-priority relationship."""
    by_qid: Dict[str, Dict[str, Any]] = {}
    for c in raw:
        cqid = c["wikidata_id"]
        existing = by_qid.get(cqid)
        if existing is not None:
            # Keep the higher-priority relationship.
            old_prio = _RELATIONSHIP_PRIORITY.get(existing["relationship"], 99)
            new_prio = _RELATIONSHIP_PRIORITY.get(c["relationship"], 99)
            if new_prio < old_prio:
                existing["relationship"] = c["relationship"]
            continue
        ent = _entity_data(cqid)
        if ent is None:
            continue
        label = ent.get("labels", {}).get("en", {}).get("value")
        if not label:
            continue
        by_qid[cqid] = {
            "name": label,
            "wikidata_id": cqid,
            "relationship": c["relationship"],
        }
    enriched = list(by_qid.values())
    enriched.sort(key=lambda c: _RELATIONSHIP_PRIORITY.get(c["relationship"], 99))
    return enriched


def _empty(name: str, source: str, method: str, alternatives: List[Dict[str, Any]],
           person_qid: str | None = None, confidence: float = 0.0) -> Dict[str, Any]:
    return {
        "person": name,
        "companies": [],
        "primary_company": None,
        "source": source,
        "person_qid": person_qid,
        "confidence": confidence,
        "method": method,
        "alternatives": alternatives,
    }


def _resolve_one(name: str, candidates: List[ReconciCandidate]) -> Dict[str, Any]:
    """Resolve one person name given their reconci candidates."""
    alts = [
        {"qid": c.qid, "name": c.name, "score": c.score}
        for c in candidates[:3]
    ]
    if not candidates:
        return _empty(name, "not_found", "no_candidates", alts)

    top = candidates[0]
    accept, reason = _accept_candidate(name, top)
    if not accept:
        return _empty(name, "not_found", reason, alts, confidence=top.score)

    entity = _entity_data(top.qid)
    if entity is None:
        # Person found but entity fetch failed. Mark as wikidata (we know
        # the Q-id) but with no companies.
        return _empty(
            name, "wikidata", f"reconci_{reason}_no_entity_data",
            alts, person_qid=top.qid, confidence=top.score,
        )

    raw = _extract_company_qids(entity)
    enriched = _enrich_companies(raw)

    return {
        "person": name,
        "companies": enriched,
        "primary_company": enriched[0]["name"] if enriched else None,
        "source": "wikidata",
        "person_qid": top.qid,
        "confidence": top.score,
        "method": f"reconci_{reason}",
        "alternatives": alts,
    }


def resolve_people_batch(names: List[str]) -> Dict[str, Dict[str, Any]]:
    """Resolve many person names to corporate connections. Batched
    reconci call then per-person entity fetches.

    Drop-in replacement for `wikidata_client.resolve_people_rest`."""
    if not names:
        return {}
    candidates_by_name = reconcile_batch(names)
    return {
        name: _resolve_one(name, candidates_by_name.get(name, []))
        for name in names
    }


if __name__ == "__main__":
    import time
    cases = [
        "ELON MUSK", "SHELDON ADELSON", "KENNETH GRIFFIN",
        "JAN KOUM", "TIMOTHY MELLON", "CHARLES KOCH",
        "JOHN ARNOLD", "PAUL SINGER",
    ]
    t = time.time()
    results = resolve_people_batch(cases)
    dt = time.time() - t
    print(f"{len(cases)} names in {dt:.1f}s\n")
    for name in cases:
        r = results[name]
        print(f"  {name!r:20} -> source={r['source']:10} "
              f"primary={r['primary_company']!r} "
              f"companies={len(r['companies'])}")
        for c in r["companies"][:5]:
            print(f"      {c['relationship']:18} -> {c['name']!r} ({c['wikidata_id']})")
