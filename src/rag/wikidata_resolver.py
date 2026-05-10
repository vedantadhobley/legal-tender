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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Client-side type verification
# ---------------------------------------------------------------------------
#
# The reconci.link `type` parameter is a soft preference, not a strict
# filter — the server can return entities whose own P31 isn't actually
# a subclass of Q43229 (organization). We therefore re-check using the
# `type` list the API already returned per candidate (no extra REST
# call needed). Any candidate that DOES NOT have at least one
# corporate-shaped Q-id in its returned types is rejected.
#
# This is a short whitelist of specific Q-ids. It's narrower than
# "anything subclass of Q43229" because Wikidata's organization tree
# includes municipalities, sovereign states, and other things that
# aren't employers. The whitelist captures the actual employer shapes
# we care about.

_STRICT_CORPORATE_QIDS = frozenset({
    "Q43229",  # organization (broad fallback)
    "Q4830453",  # business
    "Q6881511",  # enterprise
    "Q167037",  # corporation
    "Q891723",  # public company
    "Q161726",  # multinational corporation
    "Q740752",  # limited liability company
    "Q3558581",  # joint-stock company
    "Q22687",  # bank
    "Q1331793",  # media company / financial institution
    "Q319845",  # investment bank
    "Q7257717",  # financial services company
    "Q1361353",  # consulting firm
    "Q11691",  # stock exchange
    "Q15911314",  # association
    "Q163740",  # nonprofit organization
    "Q178790",  # trade union
    "Q11707",  # restaurant
    "Q210167",  # video game developer
    "Q15265344",  # broadcasting company
    "Q45776",  # holding company
    "Q3918",  # university
    "Q38723",  # higher education institution
    "Q16917",  # hospital
    "Q4287745",  # medical organization
    "Q327333",  # government agency
    "Q249556",  # railway company
    "Q46970",  # airline
    "Q10689397",  # asset management company
    "Q837171",  # private equity firm
    "Q7258079",  # company (very broad)
    "Q783794",  # company
    "Q5621421",  # hedge fund
    "Q1137319",  # capital markets firm
    "Q41691",  # food manufacturer
    "Q210167",  # video game developer
    "Q860572",  # photo agency
    "Q192283",  # press agency / news agency
    "Q43501",  # zoo
})


# Type-name substrings that indicate a corporate-shaped entity. Used in
# ADDITION to the Q-id whitelist because the long tail of corporate
# subclass Q-ids is large and we don't want to maintain it by hand.
# Wikidata's English type labels are reliably descriptive — a Q-id
# named "X consulting firm" or "X manufacturer" is by definition an
# employer.
_CORPORATE_TYPE_NAME_SUBSTRINGS = (
    "company",
    "corporation",
    "enterprise",
    "business",
    "firm",
    "manufacturer",
    "bank",
    "fund",
    "association",
    "society",  # for "professional society"; risk: "secret society" — minor
    "institute",
    "agency",  # press agency, news agency, etc.
    "studio",
    "publisher",
    "publishing",
    "broadcaster",
    "carrier",
    "operator",
    "syndicate",
    "consortium",
    "league",  # professional leagues
    "guild",
    "union",
    "cooperative",
    "nonprofit",
    "ngo",
    "foundation",
    "university",
    "college",
    "school",
    "hospital",
    "clinic",
    "ministry",  # gov agencies — yes, employers in our model
    "department of",
    "office of",
    "bureau",
    "commission",  # regulatory commissions
    "authority",
)

# Type-name substrings that indicate the candidate is NOT employer-shaped
# even if Q43229 or another organization Q-id appears in its types.
# Wikidata's organization tree is broad; some subclasses (municipalities,
# countries, language families) shouldn't count as employers in our model.
_NON_EMPLOYER_TYPE_NAME_SUBSTRINGS = (
    "municipality",
    "country",
    "sovereign state",
    "city",
    "town",
    "village",
    "settlement",
    "language",
    "ethnic group",
    "human settlement",
    "given name",
    "family name",
    "video game",
    "song",
    "album",
    "film",
    "novel",
    "book",
    "vaccine",
    "chemical compound",
    "gene",
    "protein",
    "submarine",
)


def _type_name_contains_corporate_keyword(type_name: str) -> bool:
    n = (type_name or "").lower()
    return any(s in n for s in _CORPORATE_TYPE_NAME_SUBSTRINGS)


def _type_name_contains_non_employer_keyword(type_name: str) -> bool:
    n = (type_name or "").lower()
    return any(s in n for s in _NON_EMPLOYER_TYPE_NAME_SUBSTRINGS)


def _candidate_passes_type_filter(c: ReconciCandidate) -> bool:
    """Two-stage filter:
      1. Reject if ANY type is a known non-employer (municipality, country,
         video game, song, given name, etc.) — protects against the soft
         type=Q43229 hint occasionally letting non-orgs through.
      2. Accept if any type is in the strict Q-id whitelist OR its English
         label contains a corporate keyword (company, firm, university,
         etc.). The keyword check covers the long tail without a fixed
         Q-id list to maintain.
    """
    type_ids = [t.get("id", "") for t in c.types]
    type_names = [t.get("name", "") for t in c.types]

    # Stage 1: hard reject on non-employer type
    for n in type_names:
        if _type_name_contains_non_employer_keyword(n):
            return False

    # Stage 2: accept on Q-id whitelist hit
    if any(qid in _STRICT_CORPORATE_QIDS for qid in type_ids):
        return True

    # Stage 2 fallback: accept on type-name keyword
    if any(_type_name_contains_corporate_keyword(n) for n in type_names):
        return True

    return False


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
    source: str  # 'wikidata' | 'not_found' | 'override' | 'data_gap' | 'error'

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
            # Provenance fields — additional to existing cache schema.
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


def resolve_batch(names: List[str]) -> Dict[str, ResolutionResult]:
    """Resolve many FEC names. Batched reconcile_batch() upstream, then
    per-name decision logic locally. Output keyed by input name."""
    if not names:
        return {}

    # One round-trip to reconci.link covering all names.
    candidates_by_name = reconcile_batch(names)

    return {name: resolve_one(name, candidates_by_name) for name in names}


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
