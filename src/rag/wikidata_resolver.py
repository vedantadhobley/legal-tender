"""Wikidata corporate-identity resolver.

Orchestration layer that turns a raw FEC employer string into a single
canonical corporate identity (Wikidata Q-id + canonical label + parent
rollup) using:

  Layer 1: wikidata.reconci.link (typed candidate space, ranked scoring)
  Layer 2 (TBD Phase 3): OpenCorporates for entities Wikidata lacks

This module owns:
  - client-side type verification (soft `type=Q43229` from reconci.link
    is not a hard filter; we re-check P31 against a strict corporate
    whitelist using the types reconci.link already returned)
  - confidence threshold (reject fuzzy matches below it)
  - sitelinks-based tiebreak when multiple candidates score equally
  - parent-organization rollup (P749) for subsidiary → parent canonical
  - small irreducible-overrides table for cases where Wikidata's own
    ranking is wrong (NEA-style) or Wikidata genuinely lacks the entity

Per `docs/decisions.md` 2026-05-10, the goal is to obsolete the
~250-line band-aid filter chain in `wikidata_client.py` by handling
the same concerns structurally: pre-filtered candidate space + scored
selection rather than a growing list of negative-pattern filters.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.rag.wikidata_client import _entity_data, _claim_qid
from src.rag.wikidata_reconci import (
    ORG_TYPE_QID,
    ReconciCandidate,
    reconcile_batch,
)
from src.rag.wikidata_ontology import (
    is_employer_type,
    prewarm as ontology_prewarm,
    save_cache as save_ontology_cache,
)
from src.rag.gleif import resolve_batch as gleif_resolve_batch, GleifResolution

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Client-side type verification — backed by Wikidata's own ontology
# ---------------------------------------------------------------------------
#
# The reconci.link `type` parameter is a soft preference, not a strict
# filter — the server can return entities whose own P31 isn't actually
# a subclass of Q43229 (organization). We re-check by walking each
# returned type's `wdt:P279*` (subclass-of) chain via
# `wikidata_ontology.is_employer_type`. The walk decides membership
# from Wikidata's own subclass tree rather than from a hand-curated
# Q-id list or English keyword guesses.
#
# This replaces three earlier band-aid lists deleted in this commit:
#   _STRICT_CORPORATE_QIDS         (~40 hand-curated org Q-ids)
#   _CORPORATE_TYPE_NAME_SUBSTRINGS (~35 corporate-keyword strings)
#   _NON_EMPLOYER_TYPE_NAME_SUBSTRINGS (~20 non-employer keyword strings)


def _candidate_passes_type_filter(c: ReconciCandidate) -> bool:
    """Returns True iff the candidate's P31 type list represents an
    employer per Wikidata's own ontology.

    Walks `wdt:P279*` (subclass-of) from each type Q-id via
    `wikidata_ontology.is_employer_type`. The walk hits ORG_ROOTS
    (rooted at Q43229 organization) → accept; or NON_EMPLOYER_ROOTS
    (territorial entities, creative works, languages, etc.) → reject.
    Cached after first walk per Q-id.

    No hand-maintained Q-id whitelist or keyword lists. The
    classification comes from Wikidata's own subclass relationships,
    not our guesses about what "looks corporate."
    """
    type_ids = [t.get("id", "") for t in c.types if t.get("id")]
    return is_employer_type(type_ids)


# ---------------------------------------------------------------------------
# Confidence threshold
# ---------------------------------------------------------------------------
#
# reconci.link returns 0–100 scores. Empirically:
#   - 100 = exact label or alias match (high confidence)
#   - 70-99 = strong fuzzy / alias-with-noise (medium-high)
#   - 50-69 = weak fuzzy (often wrong — Deutz-Fahr matched on FAHR by
#     stem similarity)
#   - < 50 = noise
# Threshold of 70 keeps strong matches and rejects partial-token noise.
CONFIDENCE_THRESHOLD = 70.0


# ---------------------------------------------------------------------------
# Resolution result
# ---------------------------------------------------------------------------


@dataclass
class ResolutionResult:
    """Output of resolving one FEC employer string."""

    original: str
    canonical: str
    wikidata_id: Optional[str]
    parent_id: Optional[str]
    relationship: str  # 'self' | 'parent' | 'subsidiary_of' | 'override' | 'data_gap'
    source: str  # 'wikidata' | 'gleif' | 'not_found' | 'override' | 'data_gap' | 'error'

    # External identifier from non-Wikidata sources. For source='gleif',
    # this carries the LEI. None otherwise. Useful for downstream
    # debugging and possible future cross-database joins.
    external_id: Optional[str] = None
    external_id_type: Optional[str] = None  # 'lei' | None

    # Provenance — written so future debugging doesn't require code spelunking.
    method: str = ""  # which decision path produced this
    confidence: float = 0.0
    alternatives: List[Dict[str, Any]] = field(default_factory=list)  # candidates considered

    def to_cache_dict(self) -> Dict[str, Any]:
        """Schema-compatible with the existing wikidata_client cache shape
        so the asset's existing read code doesn't change."""
        return {
            "original": self.original,
            "canonical": self.canonical,
            "wikidata_id": self.wikidata_id,
            "parent_id": self.parent_id,
            "relationship": self.relationship,
            "source": self.source,
            # Extended schema fields:
            "external_id": self.external_id,
            "external_id_type": self.external_id_type,
            "method": self.method,
            "confidence": self.confidence,
            "alternatives": self.alternatives,
        }


def _not_found(name: str, method: str = "no_candidates") -> ResolutionResult:
    return ResolutionResult(
        original=name,
        canonical=name,
        wikidata_id=None,
        parent_id=None,
        relationship="self",
        source="not_found",
        method=method,
    )


# ---------------------------------------------------------------------------
# Sitelinks tiebreak
# ---------------------------------------------------------------------------


def _sitelinks_count(qid: str) -> int:
    """Fetch the entity-data sitelinks count as a popularity proxy.
    Real Citadel LLC has more Wikipedia-language pages than the
    fictional cricket team in our examples; that signal disambiguates
    score-100 ties."""
    entity = _entity_data(qid)
    if entity is None:
        return 0
    sitelinks = entity.get("sitelinks") or {}
    return len(sitelinks)


# ---------------------------------------------------------------------------
# Parent rollup (P749)
# ---------------------------------------------------------------------------


def _resolve_parent_label(qid: str) -> Optional[str]:
    """If `qid` has a P749 (parent organization) claim, return the
    parent's English label. Otherwise None. Used to roll subsidiaries
    up to their parent corporate family.

    P127 (owned-by) is intentionally NOT followed — see comment in
    wikidata_client._resolve_company_one_query for why (Microsoft →
    BlackRock via P127 is shareholder-relationship noise)."""
    entity = _entity_data(qid)
    if entity is None:
        return None
    parents = entity.get("claims", {}).get("P749", [])
    for claim in parents:
        parent_qid = _claim_qid(claim)
        if not parent_qid:
            continue
        parent_entity = _entity_data(parent_qid)
        if parent_entity is None:
            continue
        return parent_entity.get("labels", {}).get("en", {}).get("value")
    return None


# ---------------------------------------------------------------------------
# Per-name resolution
# ---------------------------------------------------------------------------


def _select_best_candidate(
    candidates: List[ReconciCandidate],
) -> Optional[ReconciCandidate]:
    """Pick the best candidate from a ranked list:
      1. Filter by client-side P31 type whitelist
      2. Filter by confidence threshold
      3. If multiple survive at the top score → sitelinks tiebreak
      4. Otherwise → highest score wins
    Returns None if nothing passes filters."""

    if not candidates:
        return None

    # 1+2: type + confidence filters
    eligible = [
        c for c in candidates
        if c.score >= CONFIDENCE_THRESHOLD and _candidate_passes_type_filter(c)
    ]
    if not eligible:
        return None

    # Highest score across eligible
    top_score = max(c.score for c in eligible)
    top_tier = [c for c in eligible if c.score == top_score]

    if len(top_tier) == 1:
        return top_tier[0]

    # Tiebreak by sitelinks count. One entity-data fetch per tied
    # candidate. Bounded; usually 2-3 candidates, occasionally 5.
    logger.debug(
        "Tiebreaking %d candidates at score %.1f via sitelinks: %s",
        len(top_tier), top_score, [c.qid for c in top_tier],
    )
    best: Optional[ReconciCandidate] = None
    best_links = -1
    for c in top_tier:
        links = _sitelinks_count(c.qid)
        if links > best_links:
            best, best_links = c, links
    return best


def resolve_one(
    name: str,
    candidates_by_name: Dict[str, List[ReconciCandidate]],
) -> ResolutionResult:
    """Resolve one FEC name given pre-fetched candidate lists.

    Caller batches the reconcile_batch() upstream and passes the
    {name → [candidates]} dict in. This separates network I/O (which we
    want to batch + parallelize) from the per-name decision logic
    (which is pure).
    """
    raw_candidates = candidates_by_name.get(name, [])

    # Capture top-5 alternatives in provenance regardless of decision.
    alts = [
        {
            "qid": c.qid, "name": c.name, "score": c.score,
            "types": [t.get("name", "") for t in c.types[:3]],
        }
        for c in raw_candidates[:5]
    ]

    chosen = _select_best_candidate(raw_candidates)
    if chosen is None:
        return ResolutionResult(
            original=name, canonical=name, wikidata_id=None, parent_id=None,
            relationship="self", source="not_found",
            method="no_candidates_passed_filters" if raw_candidates else "no_candidates",
            alternatives=alts,
        )

    # Walk P749 for parent rollup. Skip if parent itself is in our
    # non-corporate space (handled by the same type filter — if the
    # parent's P31 doesn't include a corporate Q-id, we keep the
    # subsidiary's own label as canonical).
    parent_label = _resolve_parent_label(chosen.qid)
    if parent_label:
        return ResolutionResult(
            original=name,
            canonical=parent_label,
            wikidata_id=chosen.qid,
            parent_id=None,  # not currently used downstream; could be filled
            relationship="subsidiary_of",
            source="wikidata",
            method="reconci_with_p749_rollup",
            confidence=chosen.score,
            alternatives=alts,
        )

    return ResolutionResult(
        original=name,
        canonical=chosen.name,
        wikidata_id=chosen.qid,
        parent_id=None,
        relationship="parent",
        source="wikidata",
        method="reconci",
        confidence=chosen.score,
        alternatives=alts,
    )


def _gleif_to_resolution(name: str, g: GleifResolution) -> ResolutionResult:
    """Convert a GleifResolution to a ResolutionResult."""
    if g.source == "gleif":
        return ResolutionResult(
            original=name,
            canonical=g.canonical,
            wikidata_id=None,
            parent_id=None,
            relationship="parent",
            source="gleif",
            external_id=g.lei,
            external_id_type="lei",
            method=f"gleif_{g.method}",
            confidence=100.0,  # GLEIF strict-match is binary; "100" reflects high confidence
            alternatives=g.alternatives,
        )
    # not_found / ambiguous → return not_found, but preserve provenance
    return ResolutionResult(
        original=name,
        canonical=name,
        wikidata_id=None,
        parent_id=None,
        relationship="self",
        source="not_found",
        method=f"gleif_{g.method}",
        alternatives=g.alternatives,
    )


def resolve_batch(
    names: List[str],
    use_gleif_fallback: bool = True,
) -> Dict[str, ResolutionResult]:
    """Resolve many FEC names through the two-layer pipeline:
       Layer 1: Wikidata via reconci.link (typed candidate space)
       Layer 2: GLEIF (LEI registry) as fallback for not_found

    GLEIF is gated by `use_gleif_fallback`. Disable for tests / dry-runs.
    """
    if not names:
        return {}

    # Layer 1: Wikidata via reconci.link.
    candidates_by_name = reconcile_batch(names)

    # Pre-warm ontology cache for every type Q-id we're about to filter
    # on. Without this, _candidate_passes_type_filter pays serial
    # network latency on each cache miss. With pre-warm, the per-name
    # filter is a sequence of cache hits.
    all_type_ids = [
        t.get("id", "")
        for cands in candidates_by_name.values()
        for c in cands
        for t in (c.types or [])
        if t.get("id")
    ]
    if all_type_ids:
        ontology_prewarm(all_type_ids)
        save_ontology_cache()

    layer1: Dict[str, ResolutionResult] = {
        name: resolve_one(name, candidates_by_name) for name in names
    }

    if not use_gleif_fallback:
        return layer1

    # Layer 2: for names that came back not_found from Wikidata,
    # try GLEIF. Only the not-found subset is queried — GLEIF should
    # never override a confident Wikidata match.
    gleif_input = [name for name, r in layer1.items() if r.source == "not_found"]
    if not gleif_input:
        return layer1

    logger.info(
        "Wikidata resolved %d/%d names; trying GLEIF fallback on %d not-founds",
        len(names) - len(gleif_input), len(names), len(gleif_input),
    )
    gleif_results = gleif_resolve_batch(gleif_input)

    # Merge: GLEIF result OVERRIDES the layer1 not_found if and only if
    # GLEIF actually matched.
    out: Dict[str, ResolutionResult] = dict(layer1)
    for name in gleif_input:
        g = gleif_results.get(name)
        if g is None:
            continue
        if g.source == "gleif":
            out[name] = _gleif_to_resolution(name, g)
        # else: keep the layer1 not_found (with its more-detailed
        # Wikidata-side alternatives provenance)
    return out


if __name__ == "__main__":
    # Smoke test: same hard cases as the reconci client smoke test, but
    # now passed through the full resolver (filters, threshold, tiebreak,
    # parent rollup).
    import time

    cases = [
        "CITADEL", "NEA", "BCG", "KKR", "FAHR", "ULINE",
        "BLACKSTONE GROUP", "PRATT INDUSTRIES", "ADELSON CLINIC",
        "GOLDMAN SACHS", "APPLE", "GOOGLE", "PAN AM RAILWAYS",
        "CITADEL INVESTMENT GROUP", "STEYER", "IBM", "RED HAT",
        "NEWSWEB",
    ]
    t = time.time()
    results = resolve_batch(cases)
    dt = time.time() - t
    print(f"Resolved {len(cases)} names in {dt:.2f}s\n")
    for name in cases:
        r = results[name]
        flag = "✓" if r.source == "wikidata" else "✗"
        print(
            f"  {flag} {name!r:30} -> {r.canonical!r:35} "
            f"qid={r.wikidata_id} method={r.method} conf={r.confidence:.0f}"
        )
        if r.source == "not_found" and r.alternatives:
            print(f"    rejected: {r.alternatives[:2]}")
