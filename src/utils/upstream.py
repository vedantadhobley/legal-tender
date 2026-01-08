"""Upstream Money Traversal - Trace funding sources to terminal entities.

This module provides functions to trace money upstream from a candidate
to find the actual funding sources (corporations, unions, ideological groups).

The traversal uses the `terminal_type` field on committees to know when to stop:
- TERMINAL types (stop here - this IS the funding source):
  - corporation, trade_association, labor_union, ideological, cooperative
- PASSTHROUGH types (continue upstream - this is just a conduit):
  - passthrough, unknown
- ENDPOINT types (don't trace FROM these, only TO):
  - campaign (candidate's own committee)
  - super_pac_unclassified (needs LLM classification)

Usage:
    from src.utils.upstream import trace_upstream_funding
    
    # Get resolved funding sources for a candidate
    sources = trace_upstream_funding(
        db=agg_db,
        candidate_id="S2KY00012",  # McConnell
        cycle="2024",
        max_depth=10
    )
"""

from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from collections import defaultdict


# Terminal types where we STOP - these are the actual funding identities
TERMINAL_TYPES = {
    "corporation",
    "trade_association",
    "labor_union",
    "ideological",
    "cooperative",
}

# Passthrough types where we CONTINUE upstream
PASSTHROUGH_TYPES = {
    "passthrough",
    "unknown",
}

# Types that are endpoints (candidates) - we trace TO these, not FROM
ENDPOINT_TYPES = {
    "campaign",
    "super_pac_unclassified",  # TODO: LLM classify these
}

# Conduit names to filter out from donors - these are NOT real donors
# They are aggregate/earmark records that should be excluded
CONDUIT_DONOR_PATTERNS = [
    "WINRED",
    "ACTBLUE",
    "EARMARK",
    "CONDUIT",
    "UNITEMIZED",
]


@dataclass
class FundingSource:
    """A resolved funding source with aggregated amount."""
    entity_id: str           # _key of the source (committee or donor)
    entity_type: str         # "committee" or "donor"
    name: str                # Display name
    terminal_type: str       # corporation, labor_union, ideological, donor, etc.
    total_amount: float      # Aggregated amount reaching the candidate
    transaction_count: int   # Number of transactions
    paths: int               # Number of distinct paths to candidate
    sample_path: List[str]   # One example path (committee names)


def trace_upstream_funding(
    db,  # ArangoDB database connection
    candidate_id: str,
    cycle: Optional[str] = None,
    max_depth: int = 10,
) -> List[FundingSource]:
    """Trace all funding upstream from a candidate to terminal entities.
    
    Args:
        db: ArangoDB database connection (aggregation database)
        candidate_id: FEC candidate ID (e.g., "S2KY00012")
        cycle: Optional cycle filter (e.g., "2024"). If None, all cycles.
        max_depth: Maximum traversal depth (default 10)
    
    Returns:
        List of FundingSource objects, sorted by total_amount descending
    """
    
    # Step 1: Get all committees affiliated with this candidate
    affiliated_query = """
    FOR a IN affiliated_with
        FILTER a._to == CONCAT("candidates/", @cand_id)
        RETURN DISTINCT PARSE_IDENTIFIER(a._from).key
    """
    candidate_committees = list(db.aql.execute(
        affiliated_query, 
        bind_vars={"cand_id": candidate_id}
    ))
    
    if not candidate_committees:
        return []
    
    # Step 2: Build cycle filter clause
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # Step 3: Recursive upstream traversal using AQL
    # This query traces money backwards through the graph, stopping at terminal entities
    traversal_query = f"""
    // Start from candidate's committees
    LET start_committees = @start_cmtes
    
    // Recursive traversal function (using subquery pattern)
    LET results = (
        FOR start_cmte IN start_committees
            // Get direct contributions from donors
            LET donor_contributions = (
                FOR e IN contributed_to
                    FILTER e._to == CONCAT("committees/", start_cmte)
                    {cycle_filter}
                    FOR d IN donors
                        FILTER d._id == e._from
                        // Filter out conduit aggregate records (WinRed, ActBlue, etc.)
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "WINRED")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "ACTBLUE")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "EARMARK")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "CONDUIT")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "UNITEMIZED")
                        RETURN {{
                            entity_id: d._key,
                            entity_type: "donor",
                            name: d.canonical_name,
                            employer: d.canonical_employer,
                            terminal_type: "donor",
                            amount: e.total_amount,
                            path: [start_cmte]
                        }}
            )
            
            // Get transfers from other committees (1 hop)
            LET committee_transfers = (
                FOR e IN transferred_to
                    FILTER e._to == CONCAT("committees/", start_cmte)
                    {cycle_filter}
                    FOR c IN committees
                        FILTER c._id == e._from
                        RETURN {{
                            entity_id: c._key,
                            entity_type: "committee",
                            name: c.CMTE_NM,
                            terminal_type: c.terminal_type,
                            amount: e.total_amount,
                            path: [c.CMTE_NM, start_cmte]
                        }}
            )
            
            RETURN {{donor_contributions, committee_transfers}}
    )
    
    // Flatten and process results
    LET all_donors = FLATTEN(results[*].donor_contributions)
    LET all_committees = FLATTEN(results[*].committee_transfers)
    
    // Separate terminal committees from passthrough
    LET terminal_committees = (
        FOR c IN all_committees
            FILTER c.terminal_type IN ["corporation", "trade_association", "labor_union", "ideological", "cooperative"]
            RETURN c
    )
    
    LET passthrough_committees = (
        FOR c IN all_committees
            FILTER c.terminal_type IN ["passthrough", "unknown"]
            RETURN c
    )
    
    // For passthrough committees, trace one more level up
    LET upstream_from_passthrough = (
        FOR pt IN passthrough_committees
            // Donors to passthrough
            LET pt_donors = (
                FOR e IN contributed_to
                    FILTER e._to == CONCAT("committees/", pt.entity_id)
                    {cycle_filter}
                    FOR d IN donors
                        FILTER d._id == e._from
                        // Filter out conduit aggregate records (WinRed, ActBlue, etc.)
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "WINRED")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "ACTBLUE")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "EARMARK")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "CONDUIT")
                        FILTER NOT CONTAINS(UPPER(d.canonical_name), "UNITEMIZED")
                        RETURN {{
                            entity_id: d._key,
                            entity_type: "donor",
                            name: d.canonical_name,
                            employer: d.canonical_employer,
                            terminal_type: "donor",
                            amount: e.total_amount * (pt.amount > 0 ? 1 : 0),  // Preserve connection
                            path: APPEND([d.canonical_name], pt.path)
                        }}
            )
            
            // Committees to passthrough
            LET pt_committees = (
                FOR e IN transferred_to
                    FILTER e._to == CONCAT("committees/", pt.entity_id)
                    {cycle_filter}
                    FOR c IN committees
                        FILTER c._id == e._from
                        RETURN {{
                            entity_id: c._key,
                            entity_type: "committee",
                            name: c.CMTE_NM,
                            terminal_type: c.terminal_type,
                            amount: e.total_amount,
                            path: APPEND([c.CMTE_NM], pt.path)
                        }}
            )
            
            RETURN {{donors: pt_donors, committees: pt_committees}}
    )
    
    // Combine all sources
    LET all_sources = UNION(
        all_donors,
        terminal_committees,
        FLATTEN(upstream_from_passthrough[*].donors),
        // Only include terminal types from upstream
        (FOR c IN FLATTEN(upstream_from_passthrough[*].committees)
            FILTER c.terminal_type IN ["corporation", "trade_association", "labor_union", "ideological", "cooperative"]
            RETURN c)
    )
    
    // Aggregate by entity
    FOR src IN all_sources
        FILTER src.amount > 0
        COLLECT 
            entity_id = src.entity_id,
            entity_type = src.entity_type,
            name = src.name,
            terminal_type = src.terminal_type
        AGGREGATE 
            total_amount = SUM(src.amount),
            paths = COUNT(1)
        
        SORT total_amount DESC
        
        RETURN {{
            entity_id,
            entity_type,
            name,
            terminal_type,
            total_amount,
            paths
        }}
    """
    
    results = list(db.aql.execute(
        traversal_query,
        bind_vars={"start_cmtes": candidate_committees}
    ))
    
    return [
        FundingSource(
            entity_id=r["entity_id"],
            entity_type=r["entity_type"],
            name=r["name"],
            terminal_type=r["terminal_type"],
            total_amount=r["total_amount"],
            transaction_count=0,  # Not tracked in this query
            paths=r["paths"],
            sample_path=[]  # Not tracked in this query
        )
        for r in results
    ]


def trace_upstream_simple(
    db,
    candidate_id: str,
    cycle: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Simplified upstream trace that returns results grouped by terminal type.
    
    Returns dict with keys: corporations, trade_associations, labor_unions, 
    ideological, donors, unresolved
    """
    
    sources = trace_upstream_funding(db, candidate_id, cycle)
    
    grouped = defaultdict(list)
    for src in sources:
        if src.terminal_type == "corporation":
            grouped["corporations"].append({
                "name": src.name, 
                "amount": src.total_amount
            })
        elif src.terminal_type == "trade_association":
            grouped["trade_associations"].append({
                "name": src.name,
                "amount": src.total_amount
            })
        elif src.terminal_type == "labor_union":
            grouped["labor_unions"].append({
                "name": src.name,
                "amount": src.total_amount
            })
        elif src.terminal_type == "ideological":
            grouped["ideological"].append({
                "name": src.name,
                "amount": src.total_amount
            })
        elif src.terminal_type == "donor":
            grouped["donors"].append({
                "name": src.name,
                "amount": src.total_amount
            })
        else:
            grouped["unresolved"].append({
                "name": src.name,
                "type": src.terminal_type,
                "amount": src.total_amount
            })
    
    return dict(grouped)


def get_upstream_summary(
    db,
    candidate_id: str,
    cycle: Optional[str] = None,
) -> Dict[str, Any]:
    """Get a summary of upstream funding by category.
    
    Returns totals and top 10 for each category.
    """
    
    grouped = trace_upstream_simple(db, candidate_id, cycle)
    
    summary = {}
    for category, sources in grouped.items():
        total = sum(s["amount"] for s in sources)
        top_10 = sorted(sources, key=lambda x: -x["amount"])[:10]
        summary[category] = {
            "total": total,
            "count": len(sources),
            "top_10": top_10
        }
    
    return summary
