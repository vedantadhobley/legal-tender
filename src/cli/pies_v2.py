#!/usr/bin/env python3
"""Five Pies Analysis with Proportional Upstream Attribution.

This script shows WHERE money actually comes from using proportional attribution.
When $10M flows through a passthrough committee, we calculate what percentage
of that committee's funding came from each source and attribute proportionally.

Example:
    Cruz gets $10M from NRSC
    NRSC received $100M total
    AIPAC gave NRSC $20M (20% of NRSC funding)
    
    Cruz's AIPAC-attributed funding via NRSC = $10M × 20% = $2M

Usage:
    python -m src.cli.pies_v2 --candidate "Ted Cruz"
    python -m src.cli.pies_v2 --fec S2TX00312
"""

import argparse
import sys
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple

from arango import ArangoClient

# Terminal types
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}
PASSTHROUGH_TYPES = {"passthrough", "unknown"}

# Conduit patterns to filter from donors
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]


def format_money(amount: float) -> str:
    """Format amount as human-readable money."""
    if abs(amount) >= 1_000_000_000:
        return f"${amount/1_000_000_000:.2f}B"
    elif abs(amount) >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    elif abs(amount) >= 1_000:
        return f"${amount/1_000:.1f}K"
    else:
        return f"${amount:.0f}"


def resolve_candidate(db, candidate: str = None, fec_id: str = None, bioguide: str = None) -> Dict[str, Any]:
    """Resolve candidate to FEC IDs and name."""
    
    if bioguide:
        member = list(db.aql.execute('''
            FOR m IN member_fec_mapping
                FILTER m._key == @bioguide
                RETURN m
        ''', bind_vars={"bioguide": bioguide}))
        
        if member:
            m = member[0]
            return {
                "name": f"{m['name']['first']} {m['name']['last']}",
                "candidate_ids": m['fec']['candidate_ids'],
                "committee_ids": m['fec'].get('committee_ids', []),
                "bioguide": bioguide
            }
        else:
            print(f"ERROR: Bioguide ID not found: {bioguide}", file=sys.stderr)
            sys.exit(1)
    
    elif fec_id:
        cand = list(db.aql.execute('''
            FOR c IN candidates
                FILTER c._key == @fec_id
                RETURN c
        ''', bind_vars={"fec_id": fec_id}))
        
        name = cand[0]['CAND_NAME'] if cand else fec_id
        return {
            "name": name,
            "candidate_ids": [fec_id],
            "committee_ids": []
        }
    
    elif candidate:
        search = candidate.upper()
        member = list(db.aql.execute('''
            FOR m IN member_fec_mapping
                LET fullName = CONCAT(UPPER(m.name.first), " ", UPPER(m.name.last))
                FILTER CONTAINS(fullName, @name) OR CONTAINS(@name, UPPER(m.name.last))
                RETURN m
        ''', bind_vars={"name": search}))
        
        if member:
            m = member[0]
            return {
                "name": f"{m['name']['first']} {m['name']['last']}",
                "candidate_ids": m['fec']['candidate_ids'],
                "committee_ids": m['fec'].get('committee_ids', []),
                "bioguide": m['_key']
            }
        
        cand = list(db.aql.execute('''
            FOR c IN candidates
                FILTER CONTAINS(UPPER(c.CAND_NAME), @name)
                RETURN c
        ''', bind_vars={"name": search}))
        
        if cand:
            return {
                "name": cand[0]['CAND_NAME'],
                "candidate_ids": [cand[0]['_key']],
                "committee_ids": []
            }
        
        print(f"ERROR: Candidate not found: {candidate}", file=sys.stderr)
        sys.exit(1)
    
    else:
        print("ERROR: Must specify --candidate, --fec, or --bioguide", file=sys.stderr)
        sys.exit(1)


def get_affiliated_committees(db, candidate_ids: List[str], known_committees: List[str]) -> List[str]:
    """Get all committees affiliated with the candidate."""
    
    affiliated = list(db.aql.execute('''
        FOR cand_id IN @cands
            FOR a IN affiliated_with
                FILTER a._to == CONCAT("candidates/", cand_id)
                RETURN DISTINCT PARSE_IDENTIFIER(a._from).key
    ''', bind_vars={"cands": candidate_ids}))
    
    all_cmtes = set(affiliated)
    all_cmtes.update(known_committees)
    
    return list(all_cmtes)


def get_committee_total_receipts(db, cmte_id: str) -> float:
    """Get total receipts for a committee (donations + transfers in)."""
    
    result = list(db.aql.execute('''
        LET donations = (
            FOR e IN contributed_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                RETURN e.total_amount
        )
        LET transfers = (
            FOR e IN transferred_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                RETURN e.total_amount
        )
        RETURN SUM(APPEND(donations, transfers))
    ''', bind_vars={"cmte_id": cmte_id}))
    
    return result[0] if result and result[0] else 0


def get_proportional_upstream_funding(
    db, 
    committees: List[str], 
    cycle: Optional[str] = None,
    max_depth: int = 3
) -> Dict[str, List[Dict]]:
    """Get upstream funding with PROPORTIONAL attribution.
    
    When money flows through a passthrough committee, we attribute it
    proportionally based on that committee's funding sources.
    
    Returns funding grouped by terminal type.
    """
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # Build conduit filter
    conduit_filter_clauses = " AND ".join([
        f'NOT CONTAINS(UPPER(d.canonical_name), "{pattern}")'
        for pattern in CONDUIT_PATTERNS
    ])
    
    # Aggregated results: {(entity_id, terminal_type, name): attributed_amount}
    attributed_funding = defaultdict(float)
    
    # Track committees we've already processed to avoid cycles
    processed_committees = set()
    
    # Queue: [(committee_id, attribution_multiplier, depth)]
    # Start with candidate's committees at 100% attribution
    queue = [(cmte_id, 1.0, 0) for cmte_id in committees]
    
    while queue:
        cmte_id, multiplier, depth = queue.pop(0)
        
        if cmte_id in processed_committees:
            continue
        if depth > max_depth:
            continue
        if multiplier < 0.001:  # Skip if less than 0.1% attribution
            continue
            
        processed_committees.add(cmte_id)
        
        # Get total receipts for this committee (for calculating upstream proportions)
        total_receipts = get_committee_total_receipts(db, cmte_id)
        if total_receipts <= 0:
            continue
        
        # Get direct contributions from donors to this committee
        donors = list(db.aql.execute(f'''
            FOR e IN contributed_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                {cycle_filter}
                FOR d IN donors
                    FILTER d._id == e._from
                    FILTER {conduit_filter_clauses}
                    RETURN {{
                        entity_id: d._key,
                        name: d.canonical_name,
                        terminal_type: "individual",
                        amount: e.total_amount
                    }}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        # Attribute donors proportionally
        for d in donors:
            key = (d['entity_id'], d['terminal_type'], d['name'])
            attributed_amount = d['amount'] * multiplier
            attributed_funding[key] += attributed_amount
        
        # Get transfers from other committees
        transfers = list(db.aql.execute(f'''
            FOR e IN transferred_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                {cycle_filter}
                FOR c IN committees
                    FILTER c._id == e._from
                    RETURN {{
                        entity_id: c._key,
                        name: c.CMTE_NM,
                        terminal_type: c.terminal_type,
                        amount: e.total_amount
                    }}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for t in transfers:
            term_type = t['terminal_type'] or 'unknown'
            
            if term_type in TERMINAL_TYPES:
                # Terminal committee - attribute directly
                key = (t['entity_id'], term_type, t['name'])
                attributed_amount = t['amount'] * multiplier
                attributed_funding[key] += attributed_amount
            
            elif term_type in PASSTHROUGH_TYPES:
                # Passthrough - trace upstream with proportional attribution
                # 
                # The question: "Of the $X this passthrough gave Cruz, how much
                # is attributable to each of the passthrough's funding sources?"
                #
                # Answer: For each upstream source, multiply their contribution
                # by (transfer_to_cruz / passthrough_total_receipts)
                #
                # Example:
                #   Middleman gave Cruz $10M
                #   Middleman received $500M total
                #   AIPAC gave Middleman $100M (20% of Middleman's funding)
                #   AIPAC attribution to Cruz = $100M × ($10M / $500M) = $2M
                #
                transfer_amount = t['amount']
                passthrough_total = get_committee_total_receipts(db, t['entity_id'])
                
                if passthrough_total > 0:
                    # Flow ratio: what fraction of the passthrough's total receipts
                    # went to Cruz (or to the committee we're currently processing)
                    flow_ratio = transfer_amount / passthrough_total
                    new_multiplier = multiplier * flow_ratio
                    
                    if t['entity_id'] not in processed_committees:
                        queue.append((t['entity_id'], new_multiplier, depth + 1))
    
    # Group results by terminal type
    grouped = defaultdict(list)
    for (entity_id, terminal_type, name), amount in attributed_funding.items():
        if amount > 0:
            grouped[terminal_type].append({
                "entity_id": entity_id,
                "name": name,
                "amount": amount
            })
    
    # Sort each group by amount
    for term_type in grouped:
        grouped[term_type] = sorted(grouped[term_type], key=lambda x: -x['amount'])
    
    return dict(grouped)


def get_ie_spending_proportional(
    db, 
    candidate_ids: List[str], 
    support_oppose: str, 
    cycle: Optional[str] = None
) -> Tuple[Dict[str, List[Dict]], float]:
    """Get IE spending with proportional upstream attribution.
    
    For each Super PAC spending on the candidate, trace upstream proportionally.
    """
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # Get Super PACs and their spending
    ie_committees = list(db.aql.execute(f'''
        FOR cand_id IN @cands
            FOR e IN spent_on
                FILTER e._to == CONCAT("candidates/", cand_id)
                FILTER e.support_oppose == @sup_opp
                {cycle_filter}
                FOR src IN committees
                    FILTER src._id == e._from
                    COLLECT 
                        cmte_id = src._key,
                        cmte_name = src.CMTE_NM
                    AGGREGATE total = SUM(e.total_amount)
                    SORT total DESC
                    RETURN {{cmte_id, cmte_name, total}}
    ''', bind_vars={
        "cands": candidate_ids,
        "sup_opp": support_oppose
    }))
    
    ie_total = sum(c['total'] for c in ie_committees)
    
    # For each IE committee, get proportional upstream funding
    all_attributed = defaultdict(float)
    
    conduit_filter_clauses = " AND ".join([
        f'NOT CONTAINS(UPPER(d.canonical_name), "{pattern}")'
        for pattern in CONDUIT_PATTERNS
    ])
    
    for ie_cmte in ie_committees:
        cmte_id = ie_cmte['cmte_id']
        ie_amount = ie_cmte['total']
        
        # Get total receipts for this Super PAC
        total_receipts = get_committee_total_receipts(db, cmte_id)
        if total_receipts <= 0:
            continue
        
        # Calculate what proportion of their spending went to this candidate
        # and use that to attribute their funding sources
        attribution_ratio = ie_amount / total_receipts if total_receipts > 0 else 0
        
        # Get donors to this Super PAC
        donors = list(db.aql.execute(f'''
            FOR e IN contributed_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                FOR d IN donors
                    FILTER d._id == e._from
                    FILTER {conduit_filter_clauses}
                    COLLECT name = d.canonical_name
                    AGGREGATE total = SUM(e.total_amount)
                    RETURN {{name, total, type: "individual"}}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for d in donors:
            # Attribute proportionally based on how much of SuperPAC's spending went to this candidate
            attributed = d['total'] * attribution_ratio
            all_attributed[f"individual:{d['name']}"] += attributed
        
        # Get committee transfers to this Super PAC
        committees = list(db.aql.execute('''
            FOR e IN transferred_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                FOR c IN committees
                    FILTER c._id == e._from
                    COLLECT name = c.CMTE_NM, terminal_type = c.terminal_type
                    AGGREGATE total = SUM(e.total_amount)
                    RETURN {name, total, type: terminal_type}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for c in committees:
            term_type = c['type'] or 'unknown'
            attributed = c['total'] * attribution_ratio
            all_attributed[f"{term_type}:{c['name']}"] += attributed
    
    # Group results
    grouped = defaultdict(list)
    for key, amount in all_attributed.items():
        if amount > 0:
            term_type, name = key.split(":", 1)
            grouped[term_type].append({"name": name, "amount": amount})
    
    # Sort each group
    for term_type in grouped:
        grouped[term_type] = sorted(grouped[term_type], key=lambda x: -x['amount'])
    
    return dict(grouped), ie_total


def print_category(name: str, sources: List[Dict], total: float, limit: int = 10):
    """Print a funding category."""
    if not sources:
        return
    
    cat_total = sum(s['amount'] for s in sources)
    pct = (cat_total / total * 100) if total > 0 else 0
    
    print(f"\n  {name}: {format_money(cat_total)} ({pct:.1f}%)")
    for s in sources[:limit]:
        print(f"    • {s['name']}: {format_money(s['amount'])}")
    if len(sources) > limit:
        print(f"    ... and {len(sources) - limit} more")


def main():
    parser = argparse.ArgumentParser(description="Five Pies with Proportional Attribution")
    parser.add_argument("--candidate", "-c", help="Search by candidate name")
    parser.add_argument("--fec", "-f", help="Search by FEC candidate ID")
    parser.add_argument("--bioguide", "-b", help="Search by bioguide ID")
    parser.add_argument("--cycle", help="Filter by election cycle (e.g., 2024)")
    parser.add_argument("--host", default="arangodb", help="ArangoDB host")
    parser.add_argument("--port", default="8529", help="ArangoDB port")
    parser.add_argument("--depth", type=int, default=3, help="Max upstream depth")
    
    args = parser.parse_args()
    
    # Connect to ArangoDB
    client = ArangoClient(hosts=f"http://{args.host}:{args.port}")
    db = client.db("aggregation", username="root", password="ltpass")
    
    # Resolve candidate
    cand_info = resolve_candidate(db, args.candidate, args.fec, args.bioguide)
    print(f"Found: {cand_info['name']}")
    print(f"FEC IDs: {', '.join(cand_info['candidate_ids'])}")
    
    # Get affiliated committees
    committees = get_affiliated_committees(
        db, 
        cand_info['candidate_ids'],
        cand_info.get('committee_ids', [])
    )
    print(f"Affiliated committees: {len(committees)}")
    
    print("\n" + "=" * 70)
    print(f"     FIVE PIES: {cand_info['name']}")
    print("     (Proportional upstream attribution)")
    print("=" * 70)
    
    # PIES 1 & 2: Direct + PAC with proportional upstream
    print("\n" + "-" * 70)
    print("PIES 1 & 2: DIRECT FUNDING (proportionally attributed)")
    print("-" * 70)
    
    funding = get_proportional_upstream_funding(db, committees, args.cycle, args.depth)
    
    # Calculate total
    direct_total = sum(
        sum(s['amount'] for s in sources)
        for sources in funding.values()
    )
    
    print(f"\nTotal Direct Funding: {format_money(direct_total)}")
    
    # Print each category
    print_category("CORPORATIONS", funding.get('corporation', []), direct_total)
    print_category("TRADE ASSOCIATIONS", funding.get('trade_association', []), direct_total)
    print_category("LABOR UNIONS", funding.get('labor_union', []), direct_total)
    print_category("IDEOLOGICAL GROUPS", funding.get('ideological', []), direct_total)
    print_category("INDIVIDUALS", funding.get('individual', []), direct_total, limit=15)
    
    other = funding.get('unknown', []) + funding.get('cooperative', [])
    if other:
        print_category("OTHER/UNCLASSIFIED", other, direct_total)
    
    # PIE 3: IE Support
    print("\n" + "-" * 70)
    print("PIE 3: INDEPENDENT EXPENDITURE SUPPORT")
    print("-" * 70)
    
    ie_support, ie_support_total = get_ie_spending_proportional(
        db, cand_info['candidate_ids'], "S", args.cycle
    )
    
    print(f"\nTotal IE Support: {format_money(ie_support_total)}")
    print("(Funders of Super PACs - proportionally attributed)")
    
    if ie_support:
        print_category("CORPORATIONS", ie_support.get('corporation', []), ie_support_total)
        print_category("TRADE ASSOCIATIONS", ie_support.get('trade_association', []), ie_support_total)
        print_category("IDEOLOGICAL", ie_support.get('ideological', []), ie_support_total)
        print_category("INDIVIDUALS", ie_support.get('individual', []), ie_support_total)
    
    # PIE 4: IE Opposition
    print("\n" + "-" * 70)
    print("PIE 4: INDEPENDENT EXPENDITURE OPPOSITION")
    print("-" * 70)
    
    ie_oppose, ie_oppose_total = get_ie_spending_proportional(
        db, cand_info['candidate_ids'], "O", args.cycle
    )
    
    print(f"\nTotal IE Opposition: {format_money(ie_oppose_total)}")
    print("(Funders of opposing Super PACs - proportionally attributed)")
    
    if ie_oppose:
        print_category("CORPORATIONS", ie_oppose.get('corporation', []), ie_oppose_total)
        print_category("TRADE ASSOCIATIONS", ie_oppose.get('trade_association', []), ie_oppose_total)
        print_category("IDEOLOGICAL", ie_oppose.get('ideological', []), ie_oppose_total)
        print_category("INDIVIDUALS", ie_oppose.get('individual', []), ie_oppose_total)
    
    # Summary
    print("\n" + "=" * 70)
    print("                           SUMMARY")
    print("=" * 70)
    
    pro_total = direct_total + ie_support_total
    last_name = cand_info['name'].split()[-1].upper()
    
    print(f"\nPRO-{last_name} TOTAL: {format_money(pro_total)}")
    print(f"  ├── Direct Funding:  {format_money(direct_total)}")
    print(f"  └── IE Support:      {format_money(ie_support_total)}")
    
    print(f"\nANTI-{last_name} TOTAL: {format_money(ie_oppose_total)}")
    print(f"  └── IE Opposition:   {format_money(ie_oppose_total)}")
    print()


if __name__ == "__main__":
    main()
