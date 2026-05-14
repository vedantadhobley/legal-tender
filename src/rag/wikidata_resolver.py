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


# Wikidata P31 classes that disqualify a candidate from being treated
# as an employer. Used by `_accept_candidate` to reject top hits that
# reconci returned at high score but that are categorically not
# employer-shaped per Wikidata's own classification.
#
# Reference data, same shape as `LEGAL_SUFFIXES` / `_TRADE_CLASS_QIDS`
# / `_PERSON_TO_COMPANY_PROPS` elsewhere in the codebase. Built
# comprehensively up-front (not grown reactively per-case) by
# enumerating the *categories* of Wikidata entities that FEC employer
# strings might fuzz-match to. Each Q-id is a category root or
# well-known instance; growth happens only when a new whole category
# of misresolution is identified, not per-PAC.
#
# Decision rule: complete the categorization once; resist adding
# entries one-at-a-time as new wrong matches surface. If a new entry
# is requested, ask whether it represents a new category (add) or
# just a new leaf in an existing category root (probably already
# covered by a broader root; no add).
_NON_EMPLOYER_QIDS = frozenset({
    # ── Government / political entities ─────────────────────────
    # FEC employer strings like "STATE OF X", "UNITED STATES DEPT
    # OF X", "U.S. SENATE", "DEPARTMENT OF Y" reach these.
    "Q35657",        # U.S. state
    "Q910252",       # United States federal executive department
    "Q327333",       # government agency
    "Q2366457",      # department (generic government department class)
    "Q11204",        # legislature (generic)
    "Q110315658",    # elected legislative house (catches Senate, House)
    "Q2570643",      # senate (upper house)
    "Q189445",       # bicameral legislature (catches Congress)
    "Q4358176",      # council (catches "City Council of X")
    "Q41487",        # parliament
    "Q1752346",      # ministry (covers "Ministry of X")
    # ── Sovereign-state / country ──────────────────────────────
    # FEC employer strings like "USA", "MEXICO", "JAPAN" reach these.
    "Q6256",         # country
    "Q3624078",      # sovereign state
    # ── Administrative-territorial / settlement ────────────────
    # Donors typing the name of a city/town/county as their employer.
    "Q15642541",     # administrative territorial entity
    "Q486972",       # human settlement
    "Q515",          # city
    "Q3957",         # town
    "Q532",          # village
    "Q15284",        # municipality (generic)
    # ── Offices, positions, occupations ────────────────────────
    # FEC employer strings like "PRESIDENT", "CEO", "ATTORNEY",
    # "CONSULTANT" reach these — donors typed their job title in the
    # employer field.
    "Q17279032",     # elective office (President of the US, etc.)
    "Q4164871",      # position (generic job position)
    "Q12737077",     # occupation
    "Q28640",        # profession
    "Q11488158",     # corporate title (CEO, CFO, etc.)
    # ── Creative works ─────────────────────────────────────────
    # Reconci sometimes fuzz-matches to film/book/song/TV titles
    # that happen to share an FEC employer name.
    "Q11424",        # film
    "Q571",          # book
    "Q7725634",      # literary work
    "Q47461344",     # written work
    "Q5398426",      # television series
    "Q21191270",     # television series episode (TARGETED VICTORY case)
    "Q482994",       # album
    "Q7366",         # song
    "Q386724",       # work of art
    "Q7889",         # video game
    "Q13442814",     # scholarly article (SOUTHERN WASTE SYSTEMS case)
    "Q5633421",      # scientific journal
    # ── Concepts / abstract ────────────────────────────────────
    "Q34770",        # language
    "Q11879003",     # given name
    "Q101352",       # family name
    "Q133327",       # taxon (biological classification)
    "Q11173",        # chemical compound
    "Q12140",        # medication
    "Q134808",       # vaccine
    # ── Geographic features ────────────────────────────────────
    "Q23397",        # lake
    "Q4022",         # river
    "Q8502",         # mountain
    # ── Wikimedia administrivia ────────────────────────────────
    "Q4167410",      # Wikimedia disambiguation page
    "Q4167836",      # Wikimedia category
    "Q13406463",     # Wikimedia list article
    # ── People ─────────────────────────────────────────────────
    # Q5 (human). The whale resolver expects persons and passes its
    # own empty `reject_types`; only the employer path applies this.
    # Catches PRATT INDUSTRIES → Anthony Pratt, TESLA → Nikola
    # Tesla, etc.
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
    require_typed: bool = False,
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

    `require_typed` enforces "candidate must have non-empty P31".
    The structural fact: Wikidata-classified organizations have P31
    claims; entities with empty P31 in reconci's response are usually
    concepts (yoga poses, fruits, scholarly articles, government
    commissions) — not organizations. The employer path passes True
    because we only want org-shaped entities as employers. The whale
    path passes False (default) because persons sometimes have
    minimal P31 data.
    """
    score = candidate.score
    label = candidate.name
    type_ids = {t.get("id") for t in candidate.types if t.get("id")}

    # P31 rejection: reject regardless of score if the candidate's
    # Wikidata classification is in the caller's reject set. Runs
    # before score/corroboration because some of these (PRESIDENT →
    # "President of the United States" at score 100; STATE OF ILLINOIS
    # → "Illinois" at score 100) would otherwise be accepted by the
    # high-confidence branch.
    if reject_types:
        blocking = type_ids & reject_types
        if blocking:
            return False, f"reject_p31_{sorted(blocking)[0]}"

    # Empty-types rejection: when the caller cares about org-shaped
    # entities (employer path), require the candidate to have at least
    # one P31 claim. Empty types is a strong "not a classified org"
    # signal — catches yoga poses, fruits, scholarly articles,
    # uncategorized Wikidata entries that happen to share a name with
    # FEC employer strings (ASANA → yoga pose Q466797; CITADEL →
    # fortification Q1764; "Afghanistan War Commission" Q111915137).
    #
    # Single rule, not a list. Replaces what would otherwise be an
    # ever-growing enumeration of "concept" / "article" / "building"
    # / "yoga pose" / etc. P31 classes.
    if require_typed and not type_ids:
        return False, "empty_types_no_classification"

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
    accept, reason = _accept_candidate(
        name, top,
        reject_types=_NON_EMPLOYER_QIDS,
        require_typed=True,
    )
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
    # NOTE: empty_types_no_classification is deliberately NOT in this
    # list. Its semantic is "Wikidata didn't classify this entity" —
    # NOT "Wikidata classified as not-an-employer". GLEIF should still
    # get a chance to find a legit LEI for the name (Apple Inc has
    # an LEI even when reconci's top hit was the fruit).
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
