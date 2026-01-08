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
   N, Q = PAC (no ORG_TP) → terminal_type: "passthrough" (likely JFC/leadership)

Note: Party committees (X, Y, Z) are always passthrough even if they incorrectly
report an ORG_TP value in their FEC filings.

Source: aggregation.committees
Target: aggregation.committees (adds terminal_type field)
"""

from typing import Dict, Any, List

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


def classify_committee(cmte_tp: str, org_tp: str) -> str:
    """Classify a committee based on FEC type codes.
    
    Returns the terminal_type for the committee.
    """
    # Normalize inputs
    cmte_tp = (cmte_tp or "").upper().strip()
    org_tp = (org_tp or "").upper().strip()
    
    # FIRST: Check CMTE_TP for committee types that are ALWAYS passthrough
    # regardless of ORG_TP (party committees can have incorrect ORG_TP values)
    if cmte_tp:
        # Party committees - ALWAYS passthrough (even if ORG_TP says otherwise)
        if cmte_tp in ["X", "Y", "Z"]:
            return "passthrough"
        
        # Conduits (WinRed, ActBlue, etc.) - always passthrough
        if cmte_tp == "V":
            return "passthrough"
        
        # Campaign committees - terminal but represent the candidate, not a funding source
        if cmte_tp in ["H", "S", "P"]:
            return "campaign"
    
    # SECOND: Check ORG_TP - if present, this tells us the sponsoring organization type
    if org_tp:
        if org_tp == "C":
            return "corporation"
        elif org_tp == "T":
            return "trade_association"
        elif org_tp == "L":
            return "labor_union"
        elif org_tp == "M":
            return "ideological"
        elif org_tp == "W":
            return "corporation"  # Corp without stock
        elif org_tp == "V":
            return "cooperative"
    
    # THIRD: Remaining CMTE_TP classifications (no ORG_TP)
    if cmte_tp:
        # Super PACs without ORG_TP - need further classification (LLM in future)
        if cmte_tp in ["O", "U"]:
            return "super_pac_unclassified"
        
        # Regular PACs without ORG_TP - likely JFCs, victory funds, leadership PACs
        if cmte_tp in ["N", "Q", "W"]:
            return "passthrough"
    
    return "unknown"


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
        
        # Build classification mapping
        classifications = {}
        for tc in type_counts:
            key = (tc['cmte_tp'] or '', tc['org_tp'] or '')
            terminal_type = classify_committee(tc['cmte_tp'], tc['org_tp'])
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
                c.CMTE_TP IN ["O", "U"] ? "super_pac_unclassified" :
                c.CMTE_TP IN ["N", "Q", "W"] ? "passthrough" :
                "unknown"
            )
            UPDATE c WITH { terminal_type: terminal_type } IN committees
            COLLECT type = terminal_type WITH COUNT INTO cnt
            RETURN {type, cnt}
        """
        
        context.log.info("🔧 Updating committees with terminal_type...")
        result = list(agg_db.aql.execute(update_aql))
        
        # Log results
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
