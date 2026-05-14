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


# Wikidata P31 classes that disqualify a candidate from being treated as
# an employer. Applied in `_accept_candidate` to reject top hits that
# reconci returned at high score but that aren't organization-shaped.
#
# Same reference-data shape as `_TRADE_CLASS_QIDS` (in
# committee_classification.py) but in the rejection direction. Each
# entry maps to a Wikidata class empirically observed in our corpus as
# a problematic top-hit for FEC employer strings. The set is finite
# and stable: Wikidata's taxonomy of "thing that isn't an organization
# that pays salaries to donors" doesn't grow per-case.
#
# Categories:
#   - Government entities: state employees, federal department
#     employees DO donate, but the entity isn't a corporate-money
#     source in the funding-channels sense
#   - Offices / positions / occupations: a donor typing a job title
#     in the employer field shouldn't resolve to a "corporate source"
#   - Mismatch entity types: a TV episode / person / book that
#     reconci surfaced as a top hit for a corporate-looking name
_NON_EMPLOYER_QIDS = frozenset({
    # Government entities (observed: STATE OF X, UNITED STATES DEPT OF X)
    "Q35657",        # U.S. state
    "Q910252",       # United States federal executive department
    "Q327333",       # government agency
    "Q2366457",      # department (generic government department class)
    # Sovereign-state / country (observed: "USA" → United States Q30)
    "Q6256",         # country
    "Q3624078",      # sovereign state
    # Offices, positions, occupations (observed: PRESIDENT, CEO, CONSULTANT)
    "Q17279032",     # elective office (President of the US, etc.)
    "Q4164871",      # position (generic job position)
    "Q12737077",     # occupation
    "Q28640",        # profession
    "Q11488158",     # corporate title (CEO, CFO, etc.)
    # Entity-type mismatches (observed misresolutions)
    "Q21191270",     # television series episode  (TARGETED VICTORY case)
    "Q13442814",     # scholarly article  (SOUTHERN WASTE SYSTEMS case)
    # Person — a human isn't an employer. Whale resolver is unaffected
    # because it passes its own empty `reject_types` (it explicitly
    # WANTS persons). Catches cases like PRATT INDUSTRIES resolving
    # to Anthony Pratt the founder rather than Pratt Industries Inc.
    "Q5",            # human
})


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


def _accept_candidate(
    input_name: str,
    candidate: ReconciCandidate,
    reject_types: "frozenset[str]" = frozenset(),
) -> tuple[bool, str]:
    """Decide whether to accept this candidate. Returns (accept, reason).
    `reason` is a short label that goes into the resolution's `method`
    field for provenance — e.g., "high_confidence",
    "corroborated_acronym", "rejected_short_input_no_signal".

    `reject_types` is a per-caller set of Wikidata P31 Q-ids that
    disqualify the candidate regardless of score. The employer path
    passes `_NON_EMPLOYER_QIDS` (rejects governments, occupations,
    persons, etc.); the whale path passes `frozenset()` because it
    expects persons and shouldn't reject Q5=human.
    """
    score = candidate.score
    label = candidate.name

    # P31 rejection: reject regardless of score if the candidate's
    # Wikidata classification is in the caller's reject set. Runs
    # before score/corroboration because some of these (PRESIDENT →
    # "President of the United States" at score 100; STATE OF ILLINOIS
    # → "Illinois" at score 100) would otherwise be accepted by the
    # high-confidence branch.
    if reject_types:
        type_ids = {t.get("id") for t in candidate.types if t.get("id")}
        blocking = type_ids & reject_types
        if blocking:
            return False, f"reject_p31_{sorted(blocking)[0]}"

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
    """Accept the top candidate if it passes `_accept_candidate`.
    Returns `not_found` if rejected.

    Earlier iteration tried falling through to next candidates when top
    was rejected, but the next hits in reconci's ranking were usually
    just-as-wrong (CEO → 'Clinical and Experimental Otorhinolaryngology';
    Dept of Army → 'badges of the United States Army'). Honest
    not_found is better than weird fallback."""
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
    accept, reason = _accept_candidate(name, top, reject_types=_NON_EMPLOYER_QIDS)
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

    # Layer 2: GLEIF strict-match for not-founds. Skip names the resolver
    # actively rejected — those are either categorically not-employers
    # (P31 in reject set) OR input too short/ambiguous to trust. Even
    # if GLEIF has an LEI for them, we don't want a "rescue" because
    # the reason reconci rejected applies regardless of source.
    #
    # Only names that genuinely weren't found (no candidates, score
    # below low threshold) flow to GLEIF as legit "Wikidata doesn't
    # know about this entity" cases.
    _REJECTION_PREFIXES = (
        "reject_p31_",
        "short_input_no_corroboration",
        "below_low_threshold_",
        "low_confidence_no_signal_",
    )
    gleif_input = [
        name for name, r in out.items()
        if r.source == "not_found"
        and not any(r.method.startswith(p) for p in _REJECTION_PREFIXES)
    ]
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
