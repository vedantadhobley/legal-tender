"""Committee Classification - Add terminal_type field to committees.

Classifies committees based on FEC type codes to determine upstream traversal behavior:
- TERMINAL: Stop here - this entity is the true funding source
- PASSTHROUGH: Trace upstream - this is just a conduit

Classification priority (CMTE_TP checked FIRST for certain types):

1. FIRST: Committee type overrides (always applies regardless of ORG_TP):
   X, Y, Z = Party committee → terminal_type: "passthrough"
   V = Conduit (WinRed/ActBlue) → terminal_type: "passthrough"
   H, S, P = Campaign committee → terminal_type: "campaign"

2. SECOND: ORG_TP (Organization Type) - if present and not overridden:
   C = Corporation → terminal_type: "corporation"
   T = Trade association → terminal_type: "trade_association"
   L = Labor union → terminal_type: "labor_union"
   M = Membership organization → terminal_type: "ideological"
   W = Corp without stock → terminal_type: "corporation"
   V = Cooperative → terminal_type: "cooperative"

3. THIRD: Remaining CMTE_TP (when no ORG_TP):
   O, U = Super PAC (no ORG_TP) → terminal_type: "super_pac_unclassified"
   I = Independent Expenditure Filer (people/groups filing 24-/48-hr IE
       notices, e.g. HOFFMAN REID, AFL-CIO COPE Treasury, SEIU PEAF) →
       terminal_type: "super_pac_unclassified" (IE-only spenders, route
       through trace as passthroughs to find their funders)
   E = Electioneering Communications Filer → terminal_type:
       "super_pac_unclassified" (similar shape to type I)
   N, Q = PAC (no ORG_TP) → terminal_type: "passthrough" (likely JFC/leadership)

Note: Party committees (X, Y, Z) are always passthrough even if they incorrectly
report an ORG_TP value in their FEC filings.

Source: aggregation.committees
Target: aggregation.committees (adds terminal_type field)
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, List

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


# Wikidata class Q-ids that signal a "trade / professional association"
# rather than ideological advocacy. Used in Phase 1b to refine committees
# that the raw ORG_TP=M default classified as `ideological`.
#
# FEC's ORG_TP=M = "Membership organization" is a catch-all that
# conflates two structurally distinct things:
#   - Single-issue advocacy ("ideological") — NRA, AIPAC, Club for
#     Growth, Sierra Club, J Street, Planned Parenthood, Citizens United
#   - Trade / professional associations — NAR (realtors), ADA (dentists),
#     AICPA (accountants), AVMA (vets), Restaurant Assoc, Gas Assoc, etc.
#
# Refinement strategy: for each Phase-1-classified `ideological` M-org-tp
# committee, ask Wikidata (via reconci.link) what it thinks the org is.
# If the top candidate's P31 (instance-of) values include any class
# below, reclassify as trade_association. Otherwise stay ideological.
#
# This set is reference data tied to Wikidata's own ontology. Each Q-id
# was either (a) seen empirically in M-org PAC reconciliation results
# or (b) a top-level class in Wikidata's professional-organization
# taxonomy. Growth shape: rare — only changes when Wikidata adds a new
# top-level class. Not for per-PAC overrides.
_TRADE_CLASS_QIDS = frozenset({
    # Verified empirically against current FEC corpus (2026-05-13).
    # Each Q-id was observed as a P31 value on a real PAC's parent
    # organization. Add new entries only after verifying the Q-id's
    # actual Wikidata label and that it semantically maps to
    # "trade/professional/industry membership organization."
    "Q2178147",     # trade association — NAR, Restaurant, Gas, IT Council
    "Q829080",      # professional association — AICPA, AVMA, NATA
    "Q10729872",    # medical association — ADA, AMA, Cardiology, Pathologists
    "Q4287745",     # medical organization — ADA secondary
    "Q1865205",     # bar association — ABA
    "Q897399",      # chamber of commerce and industry — US Chamber
    "Q18325460",    # 501(c)(6) organization — US tax code for business leagues
    "Q16904718",    # agricultural organization — NMPF
    "Q114301854",   # veterinary medical association (specialty)
    "Q70363673",    # pharmaceutical societies (specialty)
    # NOT included (deliberately):
    # Q484652 "international organization" — formal-political-agreement
    #   entity, NOT trade-shaped (ASIS / IFW edge cases).
    # Q170691 "learned society", Q1059232 / Q7257717 (alternates) —
    #   not observed in current corpus; add when first seen.
})

# Wikidata classes that *exclude* trade — if a candidate's top P31 set
# is dominated by one of these without ANY trade-class hit, we don't flip.
# Currently advisory only; the absence-of-trade-class check is the
# primary mechanism, this is here for documentation of seen P31 values.
_NON_TRADE_HINT_QIDS = frozenset({
    "Q431603",      # advocacy group
    "Q7210356",     # political organization
    "Q18325483",    # 501(c)(4) organization (US social welfare / advocacy)
    "Q1666019",     # pressure group
    "Q1899015",     # conservation organization
    "Q115197642",   # political donor
})


# Suffixes stripped (longest-first within each group) from CMTE_NM when
# computing the cluster root_name for Phase 2b. Each entry is the form
# typical FEC committees use to distinguish affiliates of the same parent
# organization (PAC vs Super PAC arm vs lobbying arm vs membership entity).
# Keep this list conservative — over-eager stripping clusters unrelated
# committees.
_CMTE_NAME_SUFFIXES = (
    " SEPARATE SEGREGATED FUND",  # FEC's legal term for the PAC arm of a corp/union
    " POLITICAL ACTION COMMITTEE",
    " POLITICAL VICTORY FUND",
    " POLITICAL FUND COMMITTEE",
    " POLITICAL FUND",
    " CONGRESSIONAL FUND",
    " VICTORY FUND",
    " FEDERAL FUND",
    " INSTITUTE FOR LEGISLATIVE ACTION",
    " ACTION COMMITTEE",
    " ACTION INC",
    " ACTION",
    " INC.",
    " INC",
    " LLC",
    " LTD",
    " PAC",
    " OF AMERICA",
    " OF THE UNITED STATES",
    " OF THE U.S.A.",
    " OF U.S.A.",
)

# "Loose" types that may be upgraded by inheritance — these were either
# wrong (corporation often is) or definitionally missing classification.
_LOOSE_TYPES = ("corporation", "super_pac_unclassified", "unknown")

# "Specific" types that may be inherited — never demote toward these
# from a more-specific type.
_SPECIFIC_TYPES = ("labor_union", "trade_association", "ideological", "cooperative")

# Priority order for picking which specific type wins inside a cluster
# that has multiple specific-type members (rare but possible). Earlier =
# more specific.
_SPECIFIC_TYPE_PRIORITY = {
    "trade_association": 0,
    "labor_union": 1,
    "cooperative": 2,
    "ideological": 3,
}

_MIN_ROOT_LENGTH = 8  # don't cluster on anything shorter than this


def _strip_suffixes(name: str) -> str:
    """Repeatedly strip known committee-form suffixes until none apply.
    Used to compute a cluster key for inheriting terminal_type across
    siblings of the same parent organization."""
    if not name:
        return ""
    upper = name.upper().strip()
    # Strip trailing punctuation/whitespace
    upper = upper.rstrip(" .,;:-")
    changed = True
    while changed:
        changed = False
        for suffix in _CMTE_NAME_SUFFIXES:
            if upper.endswith(suffix):
                upper = upper[:-len(suffix)].rstrip(" .,;:-")
                changed = True
                break  # restart the suffix scan (longest-first by group)
    return upper


def _pac_search_name(cmte_nm: str) -> str:
    """Derive a Wikidata-search-friendly name from a FEC CMTE_NM by
    stripping PAC suffixes, prefixes, parenthetical annotations, and
    alternate-name appendages.

    Handles three FEC naming patterns symmetrically:
      - "[parent] POLITICAL ACTION COMMITTEE" → "[parent]"  (suffix form)
      - "POLITICAL ACTION COMMITTEE OF [parent]" → "[parent]"  (prefix form)
      - "[parent]--PAC OF X" → "[parent]"  (alternate-name appendage)

    Examples:
      'NATIONAL ASSOCIATION OF REALTORS POLITICAL ACTION COMMITTEE'
        → 'NATIONAL ASSOCIATION OF REALTORS'
      'NATIONAL RESTAURANT ASSOCIATION PAC (RESTAURANT PAC)'
        → 'NATIONAL RESTAURANT ASSOCIATION'
      'NATIONAL FEDERATION OF INDEPENDENT BUSINESS FEDERAL POLITICAL ACTION COMMITTEE'
        → 'NATIONAL FEDERATION OF INDEPENDENT BUSINESS'
      'POLITICAL ACTION COMMITTEE OF THE AMERICAN ASSOCIATION OF ORTHOPAEDIC SURGEONS--PAC OF AAOS'
        → 'AMERICAN ASSOCIATION OF ORTHOPAEDIC SURGEONS'
    """
    if not cmte_nm:
        return ""
    import re
    # Strip alternate-name appendage: "X--PAC OF Y" → "X" (FEC uses
    # double-dash as a name/acronym separator in a handful of cases)
    s = re.sub(r"--.*$", "", cmte_nm).strip()
    # Strip parenthetical content
    s = re.sub(r"\s*\([^)]*\)\s*", " ", s).strip()
    # Strip leading "POLITICAL ACTION COMMITTEE OF (THE )?" or "PAC OF (THE )?"
    # — symmetric to the suffix forms in _CMTE_NAME_SUFFIXES
    s = re.sub(
        r"^(POLITICAL ACTION COMMITTEE|PAC) OF (THE )?",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()
    # Strip known committee-form suffixes
    s = _strip_suffixes(s)
    # Strip trailing 'FEDERAL' / 'FEDERAL FUND' / 'AGFUND' (left over
    # after a PAC suffix is removed in some FEC namings)
    s = re.sub(r"\s+(FEDERAL FUND|FEDERAL|AGFUND)$", "", s, flags=re.IGNORECASE).strip()
    return s


def _trade_classify_via_wikidata(
    names: List[str],
    cache: Dict[str, Dict[str, Any]],
    confidence_threshold: float = 70.0,
) -> Dict[str, Dict[str, Any]]:
    """Classify a list of (search_name) strings as trade vs ideological
    via Wikidata reconci.link's P31 (instance-of) types.

    Cache schema (one entry per search_name):
        {
            'qid':              Q-id of the top candidate, or None,
            'wikidata_label':   English label of the top candidate,
            'reconci_score':    float [0, 100],
            'types':            list of {'id', 'name'} — the entity's P31 values,
            'cached_at':        ISO timestamp,
        }

    is_trade is *derived at read time* from the live `_TRADE_CLASS_QIDS`
    set, so changes to that set re-evaluate cached entries without
    requiring cache invalidation.

    Conservative: only flips to trade if the top reconci candidate scores
    >= threshold AND has a P31 in _TRADE_CLASS_QIDS. False default
    (stays ideological) for: not-in-Wikidata, low-confidence match,
    no trade-class P31.
    """
    from datetime import datetime
    from src.rag.wikidata_reconci import reconcile_batch

    # Cache hit only if the entry uses the current schema (has 'types' key).
    # Old-schema entries (pre-2026-05-13) are re-queried so they migrate.
    to_query = [n for n in names if n and ("types" not in cache.get(n, {}))]
    now = datetime.utcnow().isoformat()
    if to_query:
        candidates_by_name = reconcile_batch(to_query)
        for name in to_query:
            cands = candidates_by_name.get(name) or []
            if not cands:
                cache[name] = {
                    "qid": None, "wikidata_label": None,
                    "reconci_score": 0.0, "types": [],
                    "cached_at": now,
                }
                continue
            top = cands[0]
            cache[name] = {
                "qid": top.qid,
                "wikidata_label": top.name,
                "reconci_score": top.score,
                "types": [{"id": t.get("id"), "name": t.get("name")} for t in top.types],
                "cached_at": now,
            }
    # Derive per-name classification from the live trade-class set
    out: Dict[str, Dict[str, Any]] = {}
    for n in names:
        entry = cache.get(n)
        if not entry:
            continue
        score = entry.get("reconci_score", 0.0) or 0.0
        types = entry.get("types") or []
        matched = None
        if score >= confidence_threshold:
            for t in types:
                if t.get("id") in _TRADE_CLASS_QIDS:
                    matched = t
                    break
        out[n] = {
            **entry,
            "is_trade": matched is not None,
            "matched_qid": (matched or {}).get("id"),
            "matched_type": (matched or {}).get("name"),
        }
    return out


def _load_trade_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, IOError):
        return {}


def _save_trade_cache(path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(cache, indent=2, sort_keys=True))
        os.replace(tmp, path)
    except IOError:
        pass


def _apply_m_org_refinement(context, db) -> int:
    """Phase 1b: reclassify ORG_TP=M committees from `ideological` to
    `trade_association` when Wikidata's P31 says so.

    For each M-org-tp committee currently classified `ideological`:
      1. Derive a Wikidata-search-friendly name from CMTE_NM
      2. Reconcile via reconci.link (cached at
         `<cache_dir>/trade_assoc_classification.json`)
      3. If top candidate's P31 includes any _TRADE_CLASS_QIDS → flip

    Provenance stored on each refined committee:
      - terminal_type_refined_from_m_org_wikidata = True
      - terminal_type_wikidata_qid = Q-id of the matched entity
      - terminal_type_wikidata_class = matched P31 class name
    """
    from src.utils.storage import get_cache_dir

    cache_path = get_cache_dir() / "trade_assoc_classification.json"
    cache = _load_trade_cache(cache_path)

    # Clear stale provenance flags from any prior run. When
    # _TRADE_CLASS_QIDS changes, some committees previously flipped may
    # no longer qualify; their terminal_type gets reverted by Phase 1
    # but the provenance fields persist on the document. Wipe them
    # before recomputing so only this run's flips carry the flags.
    cleared = list(db.aql.execute(
        """
        FOR c IN committees
            FILTER c.terminal_type_refined_from_m_org_wikidata != null
                OR c.terminal_type_wikidata_qid != null
                OR c.terminal_type_wikidata_class != null
            UPDATE c WITH {
                terminal_type_refined_from_m_org_wikidata: null,
                terminal_type_wikidata_qid: null,
                terminal_type_wikidata_class: null
            } IN committees OPTIONS { keepNull: false }
            COLLECT WITH COUNT INTO n
            RETURN n
        """
    ))
    if cleared and cleared[0]:
        context.log.info(f"  Phase 1b: cleared stale provenance from {cleared[0]:,} committees")

    all_ideo = list(db.aql.execute(
        "FOR c IN committees FILTER c.terminal_type == 'ideological' AND c.ORG_TP == 'M' "
        "RETURN { key: c._key, name: c.CMTE_NM, receipts: c.total_receipts }"
    ))
    if not all_ideo:
        context.log.info("  Phase 1b: no ORG_TP=M ideological committees to refine")
        return 0

    # Build (cmte → search_name) mapping; dedupe search_names for batched lookup
    cmte_search = [(c, _pac_search_name(c["name"])) for c in all_ideo]
    cmte_search = [(c, s) for c, s in cmte_search if len(s) >= 5]
    unique_search = sorted({s for _, s in cmte_search})

    n_before = len(cache)
    classifications = _trade_classify_via_wikidata(unique_search, cache)
    n_new = len(cache) - n_before
    context.log.info(
        f"  Phase 1b: reconci queries — {n_before} cached + {n_new} new = {len(cache)} total"
    )

    # Determine updates
    updates: List[Dict[str, Any]] = []
    total_receipts_flipped = 0.0
    for c, s in cmte_search:
        res = classifications.get(s) or {}
        if res.get("is_trade"):
            updates.append({
                "_key": c["key"],
                "terminal_type": "trade_association",
                "terminal_type_refined_from_m_org_wikidata": True,
                "terminal_type_wikidata_qid": res.get("qid"),
                "terminal_type_wikidata_class": res.get("matched_type"),
            })
            total_receipts_flipped += (c.get("receipts") or 0)

    # Persist cache regardless of whether we found flips (so next run skips)
    _save_trade_cache(cache_path, cache)

    if not updates:
        context.log.info("  Phase 1b: no M-ORG_TP refinements needed this run")
        return 0
    db.collection("committees").import_bulk(updates, on_duplicate="update")
    context.log.info(
        f"  Phase 1b refined {len(updates):,} ORG_TP=M committees from ideological → trade_association "
        f"(${total_receipts_flipped / 1e6:.1f}M of receipts reclassified via Wikidata P31)"
    )
    return len(updates)


def _apply_name_cluster_inheritance(context, db) -> int:
    """Phase 2b: cluster committees by stripped-name root, inherit the
    most-specific terminal_type within each cluster. Returns the number
    of committees updated."""
    all_cmtes = list(db.aql.execute(
        "FOR c IN committees RETURN { key: c._key, name: c.CMTE_NM, type: c.terminal_type }"
    ))

    # Cluster by stripped root name
    clusters: Dict[str, List[Dict[str, Any]]] = {}
    for c in all_cmtes:
        root = _strip_suffixes(c.get("name") or "")
        if len(root) < _MIN_ROOT_LENGTH:
            continue
        clusters.setdefault(root, []).append(c)

    updates: List[Dict[str, Any]] = []
    type_counts: Dict[str, int] = {}

    for root, members in clusters.items():
        if len(members) < 2:
            continue
        # Find specific-type members; pick the highest-priority type.
        specific_members = [m for m in members if m["type"] in _SPECIFIC_TYPES]
        if not specific_members:
            continue
        winning_type = min(
            (m["type"] for m in specific_members),
            key=lambda t: _SPECIFIC_TYPE_PRIORITY.get(t, 99),
        )
        # Upgrade loose members to the winning type
        for m in members:
            if m["type"] in _LOOSE_TYPES:
                updates.append({
                    "_key": m["key"],
                    "terminal_type": winning_type,
                    "terminal_type_inherited_from_name_cluster": True,
                    "terminal_type_cluster_root": root,
                })
                type_counts[winning_type] = type_counts.get(winning_type, 0) + 1

    if not updates:
        context.log.info("  Phase 2b: no name-cluster corrections needed this run")
        return 0

    # Batch-update via UPDATE...IN with on_duplicate=replace style; the
    # simplest path is import_bulk on_duplicate=update
    coll = db.collection("committees")
    coll.import_bulk(updates, on_duplicate="update")
    for t, n in type_counts.items():
        context.log.info(f"  Phase 2b inherited {n:,} → {t}")
    return len(updates)


@asset(
    name="committee_classification",
    description="Enriches committees with terminal_type for upstream traversal control.",
    group_name="enrichment",
    compute_kind="enrichment",
    deps=["contributed_to"],  # committees are created by contributed_to
)
def committee_classification_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Add terminal_type field to all committees.
    
    Uses server-side AQL UPDATE for efficiency.
    """
    
    with arango.get_client() as client:
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("committees"):
            raise RuntimeError("committees collection missing - run contributed_to asset first")
        
        # Get counts by type before classification
        count_query = """
        FOR c IN committees
            COLLECT cmte_tp = c.CMTE_TP, org_tp = c.ORG_TP WITH COUNT INTO cnt
            SORT cnt DESC
            RETURN {cmte_tp, org_tp, cnt}
        """
        type_counts = list(agg_db.aql.execute(count_query))
        context.log.info(f"📊 Found {len(type_counts)} unique type combinations")
        
        # Build classification mapping for logging
        classifications = {}
        for tc in type_counts:
            cmte_tp = (tc['cmte_tp'] or '').upper().strip()
            org_tp = (tc['org_tp'] or '').upper().strip()
            key = (cmte_tp, org_tp)
            # Mirror the AQL classification logic for logging
            if cmte_tp in ('X', 'Y', 'Z', 'V'):
                terminal_type = 'passthrough'
            elif cmte_tp in ('H', 'S', 'P'):
                terminal_type = 'campaign'
            elif org_tp == 'C':
                terminal_type = 'corporation'
            elif org_tp == 'T':
                terminal_type = 'trade_association'
            elif org_tp == 'L':
                terminal_type = 'labor_union'
            elif org_tp == 'M':
                terminal_type = 'ideological'
            elif org_tp == 'W':
                terminal_type = 'corporation'
            elif org_tp == 'V':
                terminal_type = 'cooperative'
            elif cmte_tp in ('O', 'U', 'I', 'E'):
                terminal_type = 'super_pac_unclassified'
            elif cmte_tp in ('N', 'Q', 'W'):
                terminal_type = 'passthrough'
            else:
                terminal_type = 'unknown'
            classifications[key] = terminal_type
            context.log.info(f"  CMTE_TP={tc['cmte_tp'] or '(empty)'}, ORG_TP={tc['org_tp'] or '(empty)'} → {terminal_type} ({tc['cnt']:,} committees)")
        
        # Update all committees with terminal_type using server-side AQL
        # This is much faster than fetching and updating individually
        # IMPORTANT: Check CMTE_TP first for party/campaign committees, then ORG_TP
        update_aql = """
        FOR c IN committees
            LET terminal_type = (
                // FIRST: Party and campaign committees (regardless of ORG_TP)
                c.CMTE_TP IN ["X", "Y", "Z"] ? "passthrough" :
                c.CMTE_TP == "V" ? "passthrough" :
                c.CMTE_TP IN ["H", "S", "P"] ? "campaign" :
                // SECOND: Check ORG_TP for organization type
                c.ORG_TP == "C" ? "corporation" :
                c.ORG_TP == "T" ? "trade_association" :
                c.ORG_TP == "L" ? "labor_union" :
                c.ORG_TP == "M" ? "ideological" :
                c.ORG_TP == "W" ? "corporation" :
                c.ORG_TP == "V" ? "cooperative" :
                // THIRD: Remaining CMTE_TP (no ORG_TP)
                // I = Independent Expenditure Filer (24-/48-hr IE notices,
                //     e.g. HOFFMAN REID, AFL-CIO COPE Treasury, SEIU PEAF)
                // E = Electioneering Communications Filer
                // Both functionally IE-only spenders → route through as
                // passthroughs so the trace finds their funders.
                c.CMTE_TP IN ["O", "U", "I", "E"] ? "super_pac_unclassified" :
                c.CMTE_TP IN ["N", "Q", "W"] ? "passthrough" :
                "unknown"
            )
            UPDATE c WITH { terminal_type: terminal_type } IN committees
            COLLECT type = terminal_type WITH COUNT INTO cnt
            RETURN {type, cnt}
        """
        
        context.log.info("🔧 Updating committees with terminal_type...")
        result = list(agg_db.aql.execute(update_aql))

        # Phase 1b: M-ORG_TP refinement (ideological → trade_association
        # for committees whose CMTE_NM contains a profession/industry
        # token from `_TRADE_PROFESSION_TOKENS`).
        #
        # FEC's ORG_TP=M ("Membership organization") catch-all conflates
        # single-issue advocacy ("ideological": NRA, AIPAC, Sierra Club,
        # Club for Growth) with trade/professional associations (NAR
        # realtors, ADA dentists, AAJ trial lawyers, AOPA pilots, AICPA
        # accountants, etc.). The former is correctly "ideological"; the
        # latter is structurally a "trade_association" but landed in the
        # ideological bucket because Phase 1 sees only ORG_TP=M.
        #
        # Refinement: if a Phase-1-classified `ideological` committee's
        # CMTE_NM contains any token in _TRADE_PROFESSION_TOKENS, flip
        # to `trade_association`. Conservative — only acts on M-default
        # `ideological`, never demotes truly-ideological committees.
        context.log.info("🔧 Phase 1b: M-ORG_TP refinement (ideological → trade_association)...")
        refine_count = _apply_m_org_refinement(context, agg_db)

        # Phase 2a: parent-organization inheritance via CONNECTED_ORG_NM.
        #
        # FEC bulk data sometimes mis-labels a committee's ORG_TP — e.g.
        # NEA Fund's ORG_TP=C (corporation) when the connected parent NEA
        # is clearly a labor union. Same shape applies to many SuperPAC-
        # shaped affiliates: NAR Congressional Fund (super_pac_unclassified,
        # $60M) and NAR PAC (ideological, $63M) both have
        # CONNECTED_ORG_NM='NATIONAL ASSOCIATION OF REALTORS' — the
        # Congressional Fund inherits from the PAC sibling via the
        # shared parent committee.
        #
        # Source filter includes super_pac_unclassified and unknown — these
        # are "definitionally missing classification" rather than explicit
        # mis-classifications, so they're safe to upgrade.
        context.log.info("🔧 Phase 2a: parent-organization inheritance via CONNECTED_ORG_NM...")
        # Two-step match:
        #   (1) exact: parent.CMTE_NM == CONNECTED_ORG_NM
        #   (2) prefix: parent.CMTE_NM starts with CONNECTED_ORG_NM
        #       (with ≥10-char guardrail to avoid over-matching short
        #       prefixes; covers cases like a Super PAC's CONNECTED
        #       being 'UNITED FOOD AND COMMERCIAL WORKERS INTERNATIONAL
        #       UNION' matching the parent labor cmte
        #       'UNITED FOOD AND COMMERCIAL WORKERS INTERNATIONAL UNION
        #       ACTIVE BALLOT CLUB')
        inheritance_aql = """
        FOR c IN committees
            FILTER c.terminal_type IN ["corporation", "super_pac_unclassified", "unknown"]
            FILTER c.CONNECTED_ORG_NM != null
                AND c.CONNECTED_ORG_NM != ""
                AND UPPER(c.CONNECTED_ORG_NM) != "NONE"
            LET conn = UPPER(c.CONNECTED_ORG_NM)
            LET parent = FIRST(
                FOR p IN committees
                    FILTER p._key != c._key
                    FILTER p.terminal_type IN ["labor_union", "trade_association", "ideological", "cooperative"]
                    FILTER UPPER(p.CMTE_NM) == conn
                       OR (LENGTH(conn) >= 10 AND STARTS_WITH(UPPER(p.CMTE_NM), conn))
                    LIMIT 1
                    RETURN p.terminal_type
            )
            FILTER parent != null
            UPDATE c WITH { terminal_type: parent, terminal_type_inherited_from_connected: true } IN committees
            COLLECT type = parent WITH COUNT INTO cnt
            RETURN { type, cnt }
        """
        inh_a_result = list(agg_db.aql.execute(inheritance_aql))
        if inh_a_result:
            for r in inh_a_result:
                context.log.info(f"  Phase 2a inherited {r['cnt']:,} → {r['type']}")
        else:
            context.log.info("  Phase 2a: no inheritance corrections needed this run")

        # Phase 2b: name-cluster inheritance.
        #
        # Some affiliated committees lack a useful CONNECTED_ORG_NM (it's
        # empty, or the literal string "NONE" — FEC's stand-in for "no
        # connected org"). Catch them by stripping known committee-form
        # suffixes from CMTE_NM and clustering by the resulting root.
        # Examples this catches that Phase 2a misses:
        #   CLUB FOR GROWTH ACTION       (super_pac_unclassified, $263.5M)
        #     → root 'CLUB FOR GROWTH', cluster with CLUB FOR GROWTH PAC (ideological)
        #   NATIONAL RIFLE ASSOCIATION INSTITUTE FOR LEGISLATIVE ACTION
        #     → root 'NATIONAL RIFLE ASSOCIATION', cluster with NRA Victory Fund
        #
        # Conservative: minimum root length 8 chars, skip clusters with no
        # specific-type member, never demote an already-specific type.
        context.log.info("🔧 Phase 2b: name-cluster inheritance via stripped CMTE_NM...")
        inh_b_count = _apply_name_cluster_inheritance(context, agg_db)

        # Refresh stats since some counts moved in Phase 2a/2b
        if inh_a_result or inh_b_count:
            refresh = list(agg_db.aql.execute(
                "FOR c IN committees COLLECT t = c.terminal_type WITH COUNT INTO cnt RETURN { type: t, cnt }"
            ))
            stats = {r['type']: r['cnt'] for r in refresh}
        else:
            stats = {r['type']: r['cnt'] for r in result}

        total = sum(stats.values())

        context.log.info(f"✅ Classified {total:,} committees:")
        for terminal_type, count in sorted(stats.items(), key=lambda x: -x[1]):
            pct = count / total * 100
            action = "STOP" if terminal_type not in ["passthrough", "unknown"] else "TRACE UPSTREAM"
            context.log.info(f"  {terminal_type}: {count:,} ({pct:.1f}%) → {action}")

        # Create index on terminal_type for fast filtering
        collection = agg_db.collection("committees")
        collection.add_persistent_index(fields=["terminal_type"], unique=False, sparse=False)
        context.log.info("📇 Created index on terminal_type")
        
        return Output(
            value={
                "total_classified": total,
                "by_type": stats,
            },
            metadata={
                "total_committees": MetadataValue.int(total),
                "terminal_types": MetadataValue.json(stats),
                "passthrough_count": MetadataValue.int(stats.get("passthrough", 0)),
                "terminal_count": MetadataValue.int(total - stats.get("passthrough", 0) - stats.get("unknown", 0)),
            }
        )
