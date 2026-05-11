"""Minimal corporate-identity resolver.

Pipeline:
    1. Wikidata reconci.link top hit at score >= CONFIDENCE_THRESHOLD
    2. GLEIF strict-match fallback for not-founds
    3. Otherwise not_found (raw FEC name preserved)

No ontology walking. No type filtering. No subtree-root sets. No YAML
overrides. We trust reconci.link's relevance ranking — if a candidate
scores 100 it's almost always the right match.

Trade-off accepted: a few Wikidata-data-quality issues will produce
bogus matches (e.g. "RDV" → "Rice dwarf virus" Q7323079). Those show
up in the top-30 by_organization cross-cut; review there and add a
narrow override at the call-site if the dollars are material.

For FEC corporate-attribution, missing a corporation (false negative)
is worse than misattributing one (false positive — visible and
reviewable). Earlier elaborate filtering tuned for the opposite
trade-off; this is the corrected direction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.rag.wikidata_reconci import ReconciCandidate, reconcile_batch
from src.rag.gleif import GleifResolution, resolve_batch as gleif_resolve_batch
from src.rag import name_match

logger = logging.getLogger(__name__)

# Reconci.link returns scores 0-100. Decision rule:
#
#   score >= HIGH_CONFIDENCE_THRESHOLD (70)
#       → accept, unless input is short (≤4 chars) AND no corroborating
#         signal fires. (Short-input rule catches "RDV → North Vietnam"
#         class: 3-char input fuzz-matched a distant entity at score 100.)
#
#   LOW_CONFIDENCE_THRESHOLD (40) <= score < 70
#       → accept iff at least one corroborating signal fires (acronym,
#         portmanteau, edit-distance ≤ 2, or strong token containment).
#         These catch real matches that the raw score under-rates because
#         the canonical name is much longer than the FEC abbreviation
#         ("WILMERHALE" → "Wilmer Cutler Pickering Hale and Dorr" at
#         reconci 49; portmanteau corroboration kicks in).
#
#   score < 40
#       → reject. Below this even corroborating signals are unreliable.
HIGH_CONFIDENCE_THRESHOLD = 70.0
LOW_CONFIDENCE_THRESHOLD = 40.0


@dataclass
class ResolutionResult:
    """Output of resolving one FEC employer string. Cache-schema
    compatible with the existing wikidata_resolution asset reader."""

    original: str
    canonical: str
    wikidata_id: Optional[str]
    parent_id: Optional[str]
    relationship: str  # 'self' | 'parent'
    source: str  # 'wikidata' | 'gleif' | 'not_found'

    # External ID for non-Wikidata sources (LEI for GLEIF).
    external_id: Optional[str] = None
    external_id_type: Optional[str] = None

    # Provenance — top-3 alternatives reconci returned, for review when
    # a resolution looks wrong.
    method: str = ""
    confidence: float = 0.0
    alternatives: List[Dict[str, Any]] = field(default_factory=list)

    def to_cache_dict(self) -> Dict[str, Any]:
        return {
            "original": self.original,
            "canonical": self.canonical,
            "wikidata_id": self.wikidata_id,
            "parent_id": self.parent_id,
            "relationship": self.relationship,
            "source": self.source,
            "external_id": self.external_id,
            "external_id_type": self.external_id_type,
            "method": self.method,
            "confidence": self.confidence,
            "alternatives": self.alternatives,
        }


def _accept_candidate(input_name: str, candidate: ReconciCandidate) -> tuple[bool, str]:
    """Decide whether to accept this candidate. Returns (accept, reason).
    `reason` is a short label that goes into the resolution's `method`
    field for provenance — e.g., "high_confidence",
    "corroborated_acronym", "rejected_short_input_no_signal"."""
    score = candidate.score
    label = candidate.name

    if score < LOW_CONFIDENCE_THRESHOLD:
        return False, f"below_low_threshold_{int(score)}"

    short_input = name_match.is_short_input(input_name)
    has_signal = name_match.any_corroborating_signal(input_name, label)

    if score >= HIGH_CONFIDENCE_THRESHOLD:
        # High raw score. Trust it unless the input is short AND no
        # corroborating signal — that pattern is the RDV→North Vietnam
        # class of fuzz-match-on-distant-entity.
        if short_input and not has_signal:
            return False, "short_input_no_corroboration"
        return True, "high_confidence"

    # Low band (40-69): require at least one corroborating signal.
    if has_signal:
        return True, "low_confidence_corroborated"
    return False, f"low_confidence_no_signal_{int(score)}"


def _from_reconci(name: str, candidates: List[ReconciCandidate]) -> ResolutionResult:
    """Pick the top candidate that passes the accept rule."""
    alts = [
        {
            "qid": c.qid, "name": c.name, "score": c.score,
            "types": [t.get("name", "") for t in c.types[:3]],
        }
        for c in candidates[:3]
    ]
    if not candidates:
        return ResolutionResult(
            original=name, canonical=name,
            wikidata_id=None, parent_id=None,
            relationship="self", source="not_found",
            method="no_candidates", alternatives=alts,
        )
    top = candidates[0]
    accept, reason = _accept_candidate(name, top)
    if not accept:
        return ResolutionResult(
            original=name, canonical=name,
            wikidata_id=None, parent_id=None,
            relationship="self", source="not_found",
            method=reason, alternatives=alts,
        )
    return ResolutionResult(
        original=name,
        canonical=top.name,
        wikidata_id=top.qid,
        parent_id=None,
        relationship="parent",
        source="wikidata",
        method=f"reconci_{reason}",
        confidence=top.score,
        alternatives=alts,
    )


def _from_gleif(name: str, g: GleifResolution) -> ResolutionResult:
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
            confidence=100.0,
            alternatives=g.alternatives,
        )
    return ResolutionResult(
        original=name, canonical=name,
        wikidata_id=None, parent_id=None,
        relationship="self", source="not_found",
        method=f"gleif_{g.method}",
        alternatives=g.alternatives,
    )


def resolve_batch(
    names: List[str],
    use_gleif_fallback: bool = True,
) -> Dict[str, ResolutionResult]:
    """Two-layer resolution: reconci.link (top hit at threshold) →
    GLEIF strict-match fallback for not-founds."""
    if not names:
        return {}

    # Layer 1: Wikidata via reconci.link.
    candidates_by_name = reconcile_batch(names)
    out: Dict[str, ResolutionResult] = {
        name: _from_reconci(name, candidates_by_name.get(name, []))
        for name in names
    }

    if not use_gleif_fallback:
        return out

    # Layer 2: GLEIF strict-match for not-founds.
    gleif_input = [name for name, r in out.items() if r.source == "not_found"]
    if not gleif_input:
        return out

    logger.info(
        "Wikidata resolved %d/%d; trying GLEIF on %d not-founds",
        len(names) - len(gleif_input), len(names), len(gleif_input),
    )
    gleif_results = gleif_resolve_batch(gleif_input)
    for name in gleif_input:
        g = gleif_results.get(name)
        if g and g.source == "gleif":
            out[name] = _from_gleif(name, g)

    return out


if __name__ == "__main__":
    import time
    cases = [
        "GOLDMAN SACHS", "APPLE", "MICROSOFT", "GOOGLE", "IBM",
        "CITADEL", "BAUPOST GROUP", "BALLMER GROUP", "BEAL BANK",
        "LINKEDIN", "PRATT INDUSTRIES", "MOUNTAIRE",
        "STEYER", "FAHR", "RDV",  # known weird cases
    ]
    t = time.time()
    results = resolve_batch(cases)
    dt = time.time() - t
    print(f"{len(cases)} names in {dt:.1f}s\n")
    for name in cases:
        r = results[name]
        flag = {"wikidata": "✓", "gleif": "·", "not_found": " "}.get(r.source, "?")
        print(f"  {flag} {name!r:25} -> {r.source:10} {r.canonical!r} (conf={r.confidence:.0f})")
