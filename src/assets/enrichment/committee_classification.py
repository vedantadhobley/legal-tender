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

from typing import Dict, Any, List

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


# Suffixes stripped (longest-first within each group) from CMTE_NM when
# computing the cluster root_name for Phase 2b. Each entry is the form
# typical FEC committees use to distinguish affiliates of the same parent
# organization (PAC vs Super PAC arm vs lobbying arm vs membership entity).
# Keep this list conservative — over-eager stripping clusters unrelated
# committees.
_CMTE_NAME_SUFFIXES = (
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
        inheritance_aql = """
        FOR c IN committees
            FILTER c.terminal_type IN ["corporation", "super_pac_unclassified", "unknown"]
            FILTER c.CONNECTED_ORG_NM != null
                AND c.CONNECTED_ORG_NM != ""
                AND UPPER(c.CONNECTED_ORG_NM) != "NONE"
            LET parent = FIRST(
                FOR p IN committees
                    FILTER UPPER(p.CMTE_NM) == UPPER(c.CONNECTED_ORG_NM)
                    FILTER p._key != c._key
                    FILTER p.terminal_type IN ["labor_union", "trade_association", "ideological", "cooperative"]
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
