#!/usr/bin/env python3
"""Five Pies Analysis with Upstream Resolution.

This script shows WHERE money actually comes from - not pass-through committees.

Usage:
    python -m src.cli.pies --candidate "Ted Cruz"
    python -m src.cli.pies --fec S2TX00312
    python -m src.cli.pies --bioguide C001098
"""

import argparse
import sys
from collections import defaultdict
from typing import Optional, Dict, Any, List

from arango import ArangoClient

# Terminal types
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}
PASSTHROUGH_TYPES = {"passthrough", "unknown"}

# Conduit patterns to filter from donors
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]


def format_money(amount: float) -> str:
    """Format amount as human-readable money."""
    if amount >= 1_000_000_000:
        return f"${amount/1_000_000_000:.2f}B"
    elif amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    else:
        return f"${amount:.0f}"


def resolve_candidate(db, candidate: str = None, fec_id: str = None, bioguide: str = None) -> Dict[str, Any]:
    """Resolve candidate to FEC IDs and name."""
    
    if bioguide:
        # Look up by bioguide ID
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
        # Direct FEC ID lookup
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
        # Search by name - first try member_fec_mapping
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
        
        # Fall back to candidates collection
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
    
    # Merge with known committees
    all_cmtes = set(affiliated)
    all_cmtes.update(known_committees)
    
    return list(all_cmtes)


def get_upstream_funding(db, committees: List[str], cycle: Optional[str] = None) -> Dict[str, List[Dict]]:
    """Get upstream-resolved funding sources.
    
    Returns funding grouped by terminal type:
    - corporations: Corporate PACs
    - trade_associations: Trade/industry PACs  
    - labor_unions: Union PACs
    - ideological: Membership/ideological PACs (AIPAC, NRA, etc.)
    - individuals: Individual donors (not through a PAC)
    """
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # Build conduit filter
    conduit_filters = "\n".join([
        f'        FILTER NOT CONTAINS(UPPER(d.canonical_name), "{pattern}")'
        for pattern in CONDUIT_PATTERNS
    ])
    
    query = f'''
    // Start from candidate's committees
    LET start_committees = @start_cmtes
    
    // Get all funding sources with upstream resolution
    LET results = (
        FOR start_cmte IN start_committees
            // Get direct contributions from donors
            LET donor_contributions = (
                FOR e IN contributed_to
                    FILTER e._to == CONCAT("committees/", start_cmte)
                    {cycle_filter}
                    FOR d IN donors
                        FILTER d._id == e._from
{conduit_filters}
                        RETURN {{
                            entity_id: d._key,
                            entity_type: "donor",
                            name: d.canonical_name,
                            employer: d.canonical_employer,
                            terminal_type: "individual",
                            amount: e.total_amount
                        }}
            )
            
            // Get transfers from other committees
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
                            amount: e.total_amount
                        }}
            )
            
            RETURN {{donor_contributions, committee_transfers}}
    )
    
    // Flatten results
    LET all_donors = FLATTEN(results[*].donor_contributions)
    LET all_committees = FLATTEN(results[*].committee_transfers)
    
    // Separate terminal from passthrough committees
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
    
    // Trace one level upstream from passthrough committees
    LET upstream_from_passthrough = (
        FOR pt IN passthrough_committees
            LET pt_donors = (
                FOR e IN contributed_to
                    FILTER e._to == CONCAT("committees/", pt.entity_id)
                    {cycle_filter}
                    FOR d IN donors
                        FILTER d._id == e._from
{conduit_filters}
                        RETURN {{
                            entity_id: d._key,
                            entity_type: "donor",
                            name: d.canonical_name,
                            employer: d.canonical_employer,
                            terminal_type: "individual",
                            amount: e.total_amount
                        }}
            )
            
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
                            amount: e.total_amount
                        }}
            )
            
            RETURN {{donors: pt_donors, committees: pt_committees}}
    )
    
    // Combine all terminal sources
    LET all_sources = UNION(
        all_donors,
        terminal_committees,
        FLATTEN(upstream_from_passthrough[*].donors),
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
            total_amount = SUM(src.amount)
        
        SORT total_amount DESC
        
        RETURN {{
            entity_id,
            entity_type,
            name,
            terminal_type,
            total_amount
        }}
    '''
    
    results = list(db.aql.execute(query, bind_vars={"start_cmtes": committees}))
    
    # Group by terminal type
    grouped = defaultdict(list)
    for r in results:
        term_type = r['terminal_type'] or 'unknown'
        grouped[term_type].append({
            "name": r['name'],
            "amount": r['total_amount']
        })
    
    return dict(grouped)


def get_ie_spending(db, candidate_ids: List[str], support_oppose: str, cycle: Optional[str] = None) -> Dict[str, List[Dict]]:
    """Get independent expenditure spending with upstream resolution.
    
    For each Super PAC spending on the candidate, trace upstream to find
    who funds that Super PAC.
    """
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # First get the Super PACs and their spending
    ie_query = f'''
        FOR cand_id IN @cands
            FOR e IN spent_on
                FILTER e._to == CONCAT("candidates/", cand_id)
                FILTER e.support_oppose == @sup_opp
                {cycle_filter}
                FOR src IN committees
                    FILTER src._id == e._from
                    COLLECT 
                        cmte_id = src._key,
                        cmte_name = src.CMTE_NM,
                        terminal_type = src.terminal_type
                    AGGREGATE total = SUM(e.total_amount)
                    SORT total DESC
                    RETURN {{cmte_id, cmte_name, terminal_type, total}}
    '''
    
    ie_committees = list(db.aql.execute(ie_query, bind_vars={
        "cands": candidate_ids,
        "sup_opp": support_oppose
    }))
    
    # For each IE committee, trace upstream to find funders
    conduit_filters = "\n".join([
        f'        FILTER NOT CONTAINS(UPPER(d.canonical_name), "{pattern}")'
        for pattern in CONDUIT_PATTERNS
    ])
    
    all_funders = defaultdict(float)
    
    for ie_cmte in ie_committees:
        # Get donors to this Super PAC
        donors = list(db.aql.execute(f'''
            FOR e IN contributed_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                FOR d IN donors
                    FILTER d._id == e._from
{conduit_filters}
                    COLLECT name = d.canonical_name
                    AGGREGATE total = SUM(e.total_amount)
                    SORT total DESC
                    LIMIT 50
                    RETURN {{name, total, type: "individual"}}
        ''', bind_vars={"cmte_id": ie_cmte['cmte_id']}))
        
        # Get committee transfers to this Super PAC
        committees = list(db.aql.execute('''
            FOR e IN transferred_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                FOR c IN committees
                    FILTER c._id == e._from
                    COLLECT name = c.CMTE_NM, terminal_type = c.terminal_type
                    AGGREGATE total = SUM(e.total_amount)
                    SORT total DESC
                    LIMIT 50
                    RETURN {name, total, type: terminal_type}
        ''', bind_vars={"cmte_id": ie_cmte['cmte_id']}))
        
        # Weight by the proportion of IE spending
        for d in donors:
            all_funders[f"individual:{d['name']}"] += d['total']
        for c in committees:
            term_type = c['type'] or 'unknown'
            all_funders[f"{term_type}:{c['name']}"] += c['total']
    
    # Group results
    grouped = defaultdict(list)
    for key, amount in all_funders.items():
        term_type, name = key.split(":", 1)
        grouped[term_type].append({"name": name, "amount": amount})
    
    # Sort each group
    for term_type in grouped:
        grouped[term_type] = sorted(grouped[term_type], key=lambda x: -x['amount'])
    
    return dict(grouped), sum(c['total'] for c in ie_committees)


def print_category(name: str, sources: List[Dict], total: float, limit: int = 10):
    """Print a funding category."""
    if not sources:
        print(f"  {name}: $0")
        return
    
    cat_total = sum(s['amount'] for s in sources)
    pct = (cat_total / total * 100) if total > 0 else 0
    
    print(f"\n  {name}: {format_money(cat_total)} ({pct:.1f}%)")
    for s in sources[:limit]:
        print(f"    • {s['name']}: {format_money(s['amount'])}")
    if len(sources) > limit:
        print(f"    ... and {len(sources) - limit} more")


def main():
    parser = argparse.ArgumentParser(description="Five Pies Analysis with Upstream Resolution")
    parser.add_argument("--candidate", "-c", help="Search by candidate name")
    parser.add_argument("--fec", "-f", help="Search by FEC candidate ID")
    parser.add_argument("--bioguide", "-b", help="Search by bioguide ID")
    parser.add_argument("--cycle", help="Filter by election cycle (e.g., 2024)")
    parser.add_argument("--host", default="arangodb", help="ArangoDB host")
    parser.add_argument("--port", default="8529", help="ArangoDB port")
    
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
    print(f"          FIVE PIES ANALYSIS: {cand_info['name']}")
    print("          (Upstream-resolved to terminal entities)")
    print("=" * 70)
    
    # PIE 1 & 2: Direct + PAC funding (combined via upstream resolution)
    print("\n" + "-" * 70)
    print("PIES 1 & 2: DIRECT FUNDING (who actually funds this candidate)")
    print("-" * 70)
    
    funding = get_upstream_funding(db, committees, args.cycle)
    
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
    
    if funding.get('unknown') or funding.get('cooperative'):
        other = funding.get('unknown', []) + funding.get('cooperative', [])
        print_category("OTHER/UNCLASSIFIED", other, direct_total)
    
    # PIE 3: IE Support
    print("\n" + "-" * 70)
    print("PIE 3: INDEPENDENT EXPENDITURE SUPPORT (Super PACs spending FOR)")
    print("-" * 70)
    
    ie_support, ie_support_total = get_ie_spending(db, cand_info['candidate_ids'], "S", args.cycle)
    
    print(f"\nTotal IE Support: {format_money(ie_support_total)}")
    print("(Funders of Super PACs supporting this candidate)")
    
    if ie_support:
        print_category("CORPORATIONS", ie_support.get('corporation', []), ie_support_total)
        print_category("TRADE ASSOCIATIONS", ie_support.get('trade_association', []), ie_support_total)
        print_category("IDEOLOGICAL", ie_support.get('ideological', []), ie_support_total)
        print_category("INDIVIDUALS", ie_support.get('individual', []), ie_support_total)
    
    # PIE 4: IE Opposition
    print("\n" + "-" * 70)
    print("PIE 4: INDEPENDENT EXPENDITURE OPPOSITION (Super PACs spending AGAINST)")
    print("-" * 70)
    
    ie_oppose, ie_oppose_total = get_ie_spending(db, cand_info['candidate_ids'], "O", args.cycle)
    
    print(f"\nTotal IE Opposition: {format_money(ie_oppose_total)}")
    print("(Funders of Super PACs opposing this candidate)")
    
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
    print(f"\nPRO-{cand_info['name'].split()[-1].upper()} TOTAL: {format_money(pro_total)}")
    print(f"  ├── Direct Funding:  {format_money(direct_total)}")
    print(f"  └── IE Support:      {format_money(ie_support_total)}")
    
    print(f"\nANTI-{cand_info['name'].split()[-1].upper()} TOTAL: {format_money(ie_oppose_total)}")
    print(f"  └── IE Opposition:   {format_money(ie_oppose_total)}")
    print()


if __name__ == "__main__":
    main()
