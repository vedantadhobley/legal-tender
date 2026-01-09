#!/usr/bin/env python3
"""Five Pies Analysis with Proper Graph Traversal.

Uses ArangoDB's native graph traversal to trace money flows backwards
from a candidate to terminal funding sources, handling cycles correctly.

The key insight: We're not following money forward through circular JFCs.
We're asking: "For each dollar that reached Cruz's campaign, where did it
originally come from?"

This uses edge-based traversal to avoid double-counting while still
finding all valid upstream paths.
"""

import argparse
import sys
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple

from arango import ArangoClient

# Terminal types - these are the REAL funding sources
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}

# Passthrough types - trace through these to find real sources
PASSTHROUGH_TYPES = {"passthrough", "unknown"}

# Conduit patterns to filter from donors
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]


def format_money(amount: float) -> str:
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
            FOR m IN member_fec_mapping FILTER m._key == @bioguide RETURN m
        ''', bind_vars={"bioguide": bioguide}))
        if member:
            m = member[0]
            return {
                "name": f"{m['name']['first']} {m['name']['last']}",
                "candidate_ids": m['fec']['candidate_ids'],
                "committee_ids": m['fec'].get('committee_ids', [])
            }
        print(f"ERROR: Bioguide not found: {bioguide}", file=sys.stderr)
        sys.exit(1)
    
    elif fec_id:
        cand = list(db.aql.execute('''
            FOR c IN candidates FILTER c._key == @fec_id RETURN c
        ''', bind_vars={"fec_id": fec_id}))
        return {
            "name": cand[0]['CAND_NAME'] if cand else fec_id,
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
                "committee_ids": m['fec'].get('committee_ids', [])
            }
        
        cand = list(db.aql.execute('''
            FOR c IN candidates FILTER CONTAINS(UPPER(c.CAND_NAME), @name) RETURN c
        ''', bind_vars={"name": search}))
        
        if cand:
            return {
                "name": cand[0]['CAND_NAME'],
                "candidate_ids": [cand[0]['_key']],
                "committee_ids": []
            }
        
        print(f"ERROR: Candidate not found: {candidate}", file=sys.stderr)
        sys.exit(1)
    
    print("ERROR: Must specify --candidate, --fec, or --bioguide", file=sys.stderr)
    sys.exit(1)


def get_candidate_committees(db, candidate_ids: List[str], known_committees: List[str]) -> List[str]:
    """Get the candidate's DIRECT campaign committees (not JFCs)."""
    
    affiliated = list(db.aql.execute('''
        FOR cand_id IN @cands
            FOR a IN affiliated_with
                FILTER a._to == CONCAT("candidates/", cand_id)
                LET cmte = DOCUMENT(a._from)
                // Only include actual campaign committees (H, S, P types)
                FILTER cmte.CMTE_TP IN ["H", "S", "P"]
                RETURN DISTINCT cmte._key
    ''', bind_vars={"cands": candidate_ids}))
    
    # If no H/S/P committees found, fall back to all affiliated
    if not affiliated:
        affiliated = list(db.aql.execute('''
            FOR cand_id IN @cands
                FOR a IN affiliated_with
                    FILTER a._to == CONCAT("candidates/", cand_id)
                    RETURN DISTINCT PARSE_IDENTIFIER(a._from).key
        ''', bind_vars={"cands": candidate_ids}))
    
    all_cmtes = set(affiliated)
    all_cmtes.update(known_committees)
    
    return list(all_cmtes)


def get_upstream_funding_graph(db, start_committees: List[str], cycle: Optional[str] = None, max_depth: int = 5) -> Dict[str, List[Dict]]:
    """
    Trace money BACKWARDS from candidate's campaign to find ultimate sources.
    
    Uses proper graph traversal that:
    1. Follows edges (transfers) backwards through the graph
    2. Tracks which edges we've used to avoid double-counting
    3. Calculates proportional attribution at each hop
    4. Handles cycles by tracking edge visits, not node visits
    
    The math:
    - If Committee A received $100 total and gave Cruz $40, flow_ratio = 0.4
    - If AIPAC gave Committee A $20, AIPAC's attribution to Cruz = $20 × 0.4 = $8
    - This correctly handles the case where Committee A also gives to other candidates
    """
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # Build conduit filter for donors
    conduit_checks = " AND ".join([
        f'NOT CONTAINS(UPPER(d.canonical_name), "{p}")' for p in CONDUIT_PATTERNS
    ])
    
    # Results: {(entity_id, terminal_type, name): total_attributed_amount}
    attributed = defaultdict(float)
    
    # Track edges we've traversed to avoid double-counting in cycles
    # Key: (from_id, to_id, edge_type)
    visited_edges = set()
    
    # BFS queue: (committee_id, multiplier, path_for_debug)
    # multiplier = "For every $1 this committee received, how much reaches Cruz?"
    queue = []
    
    # Calculate initial multipliers for candidate's direct committees
    # These committees send 100% of their spending "to Cruz" (they ARE Cruz's committees)
    for cmte_id in start_committees:
        queue.append((cmte_id, 1.0, [cmte_id]))
    
    # Cache for committee total receipts (now pre-computed by committee_financials enrichment)
    receipt_cache = {}
    
    def get_total_receipts(cmte_id: str) -> float:
        """Get pre-computed total_receipts from committee record."""
        if cmte_id not in receipt_cache:
            result = list(db.aql.execute('''
                FOR c IN committees
                    FILTER c.CMTE_ID == @cmte
                    RETURN c.total_receipts
            ''', bind_vars={"cmte": cmte_id}))
            receipt_cache[cmte_id] = result[0] if result and result[0] else 0
        return receipt_cache[cmte_id]
    
    iterations = 0
    max_iterations = 10000  # Safety limit
    
    while queue and iterations < max_iterations:
        iterations += 1
        cmte_id, multiplier, path = queue.pop(0)
        
        if multiplier < 0.0001:  # Stop if less than 0.01% attribution
            continue
        
        if len(path) > max_depth:
            continue
        
        # Get total receipts for this committee
        total_receipts = get_total_receipts(cmte_id)
        if total_receipts <= 0:
            continue
        
        # TRACE 1: Individual donors to this committee
        donors = list(db.aql.execute(f'''
            FOR e IN contributed_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                {cycle_filter}
                FOR d IN donors
                    FILTER d._id == e._from
                    FILTER {conduit_checks}
                    RETURN {{
                        entity_id: d._key,
                        name: d.canonical_name,
                        donor_type: d.donor_type,
                        employer: d.canonical_employer,
                        amount: e.total_amount,
                        edge_key: e._key
                    }}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for d in donors:
            edge_key = ("donor", d['entity_id'], cmte_id)
            if edge_key in visited_edges:
                continue
            visited_edges.add(edge_key)
            
            # Attribute proportionally
            # This donor's contribution × multiplier = how much of their money reached Cruz
            attr_amount = d['amount'] * multiplier
            
            # Classify donor type - organization = dark money, else individual
            donor_type = d.get('donor_type', 'individual')
            if donor_type == 'organization':
                term_type = "dark_money"
            else:
                term_type = "individual"
            
            key = (d['entity_id'], term_type, d['name'])
            attributed[key] += attr_amount
        
        # TRACE 2: Committee transfers TO this committee
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
                        amount: e.total_amount,
                        edge_key: e._key
                    }}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for t in transfers:
            edge_key = ("transfer", t['entity_id'], cmte_id)
            if edge_key in visited_edges:
                continue
            visited_edges.add(edge_key)
            
            term_type = t['terminal_type'] or 'unknown'
            attr_amount = t['amount'] * multiplier
            
            if term_type in TERMINAL_TYPES:
                # Terminal source - attribute directly
                key = (t['entity_id'], term_type, t['name'])
                attributed[key] += attr_amount
            
            elif term_type in PASSTHROUGH_TYPES:
                # Passthrough - trace further upstream
                # New multiplier = current_multiplier × (this_transfer / passthrough_total_receipts)
                upstream_receipts = get_total_receipts(t['entity_id'])
                if upstream_receipts > 0:
                    new_multiplier = multiplier * (t['amount'] / upstream_receipts)
                    if new_multiplier >= 0.0001:
                        new_path = path + [t['entity_id']]
                        queue.append((t['entity_id'], new_multiplier, new_path))
    
    # Group results by terminal type
    grouped = defaultdict(list)
    for (entity_id, term_type, name), amount in attributed.items():
        if amount > 0.01:  # Filter out dust
            grouped[term_type].append({
                "entity_id": entity_id,
                "name": name,
                "amount": amount
            })
    
    # Sort each group
    for term_type in grouped:
        grouped[term_type] = sorted(grouped[term_type], key=lambda x: -x['amount'])
    
    return dict(grouped)


def get_ie_funding_graph(db, candidate_ids: List[str], support_oppose: str, cycle: Optional[str] = None) -> Tuple[Dict[str, List[Dict]], float]:
    """Get IE spending and trace funders of those Super PACs."""
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    
    # Get Super PACs spending on this candidate
    ie_committees = list(db.aql.execute(f'''
        FOR cand_id IN @cands
            FOR e IN spent_on
                FILTER e._to == CONCAT("candidates/", cand_id)
                FILTER e.support_oppose == @sup_opp
                {cycle_filter}
                FOR src IN committees
                    FILTER src._id == e._from
                    COLLECT cmte_id = src._key, cmte_name = src.CMTE_NM
                    AGGREGATE total = SUM(e.total_amount)
                    SORT total DESC
                    RETURN {{cmte_id, cmte_name, total}}
    ''', bind_vars={"cands": candidate_ids, "sup_opp": support_oppose}))
    
    ie_total = sum(c['total'] for c in ie_committees)
    
    if not ie_committees:
        return {}, 0
    
    # For each Super PAC, trace their funding sources
    # Use the same graph traversal logic
    all_attributed = defaultdict(float)
    
    conduit_checks = " AND ".join([
        f'NOT CONTAINS(UPPER(d.canonical_name), "{p}")' for p in CONDUIT_PATTERNS
    ])
    
    for ie_cmte in ie_committees:
        cmte_id = ie_cmte['cmte_id']
        ie_amount = ie_cmte['total']
        
        # Get total receipts
        total_receipts = list(db.aql.execute('''
            LET donations = (FOR e IN contributed_to FILTER e._to == CONCAT("committees/", @cmte) RETURN e.total_amount)
            LET transfers = (FOR e IN transferred_to FILTER e._to == CONCAT("committees/", @cmte) RETURN e.total_amount)
            RETURN SUM(APPEND(donations, transfers))
        ''', bind_vars={"cmte": cmte_id}))[0] or 0
        
        if total_receipts <= 0:
            continue
        
        # Multiplier: what fraction of this Super PAC's spending went to this candidate
        multiplier = ie_amount / total_receipts
        
        # Get donors with their type classification
        donors = list(db.aql.execute(f'''
            FOR e IN contributed_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                FOR d IN donors
                    FILTER d._id == e._from
                    FILTER {conduit_checks}
                    COLLECT 
                        name = d.canonical_name, 
                        donor_type = d.donor_type,
                        employer = d.canonical_employer
                    AGGREGATE total = SUM(e.total_amount)
                    RETURN {{name, total, donor_type, employer}}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for d in donors:
            attr_amount = d['total'] * multiplier
            # Classify: organization = dark_money, else individual
            dtype = d.get('donor_type', 'individual')
            if dtype == 'organization':
                all_attributed[f"dark_money:{d['name']}"] += attr_amount
            else:
                # Include employer for whales
                emp = d.get('employer') or ''
                display_name = d['name']
                if emp and emp.upper() not in ['RETIRED', 'SELF', 'SELF-EMPLOYED', 'NOT EMPLOYED', 'NONE', 'N/A', 'HOMEMAKER']:
                    display_name = f"{d['name']} ({emp})"
                all_attributed[f"individual:{display_name}"] += attr_amount
        
        # Get committee transfers
        committees = list(db.aql.execute('''
            FOR e IN transferred_to
                FILTER e._to == CONCAT("committees/", @cmte_id)
                FOR c IN committees
                    FILTER c._id == e._from
                    COLLECT name = c.CMTE_NM, terminal_type = c.terminal_type
                    AGGREGATE total = SUM(e.total_amount)
                    RETURN {name, total, terminal_type}
        ''', bind_vars={"cmte_id": cmte_id}))
        
        for c in committees:
            attr_amount = c['total'] * multiplier
            term_type = c['terminal_type'] or 'unknown'
            all_attributed[f"{term_type}:{c['name']}"] += attr_amount
    
    # Group results
    grouped = defaultdict(list)
    for key, amount in all_attributed.items():
        if amount > 0.01:
            term_type, name = key.split(":", 1)
            grouped[term_type].append({"name": name, "amount": amount})
    
    for term_type in grouped:
        grouped[term_type] = sorted(grouped[term_type], key=lambda x: -x['amount'])
    
    return dict(grouped), ie_total


def get_corporate_influence(db, start_committees: List[str], cycle: Optional[str] = None) -> Dict[str, Dict]:
    """
    Calculate corporate influence via both PAC and employee donations.
    
    Returns dict of {company_name: {'pac': amount, 'employees': amount}}
    """
    from collections import defaultdict
    
    cycle_filter = f'FILTER e.cycle == "{cycle}"' if cycle else ""
    cmte_ids = [f"committees/{c}" for c in start_committees]
    
    # 1. Get passthrough committees that gave to candidate
    intermediaries = list(db.aql.execute(f'''
        FOR c IN committees
            FILTER c.terminal_type == "passthrough"
            LET to_cand = SUM(
                FOR t IN transferred_to
                    FILTER t._from == c._id
                    FILTER t._to IN @cmtes
                    {cycle_filter}
                    RETURN t.total_amount
            )
            FILTER to_cand > 0
            LET total_out = SUM(
                FOR t IN transferred_to
                    FILTER t._from == c._id
                    RETURN t.total_amount
            )
            RETURN {{ 
                id: c._id, 
                name: c.CMTE_NM, 
                to_cand: to_cand,
                total_out: total_out
            }}
    ''', bind_vars={"cmtes": cmte_ids}))
    
    # 2. For each intermediary, get corporate PAC donors and attribute proportionally
    corp_pac = defaultdict(float)
    
    for inter in intermediaries:
        if inter['total_out'] and inter['total_out'] > 0:
            cand_pct = min(inter['to_cand'] / inter['total_out'], 1.0)
            
            corp_donors = list(db.aql.execute(f'''
                FOR t IN transferred_to
                    FILTER t._to == @inter_id
                    {cycle_filter}
                    FOR corp IN committees
                        FILTER corp._id == t._from
                        FILTER corp.ORG_TP == "C"
                        RETURN {{ name: corp.CMTE_NM, amount: t.total_amount }}
            ''', bind_vars={"inter_id": inter['id']}))
            
            for cd in corp_donors:
                corp_pac[cd['name']] += cd['amount'] * cand_pct
    
    # 3. Direct corporate PAC donations
    direct = list(db.aql.execute(f'''
        FOR t IN transferred_to
            FILTER t._to IN @cmtes
            {cycle_filter}
            FOR corp IN committees
                FILTER corp._id == t._from
                FILTER corp.ORG_TP == "C"
                RETURN {{ name: corp.CMTE_NM, amount: t.total_amount }}
    ''', bind_vars={"cmtes": cmte_ids}))
    
    for d in direct:
        corp_pac[d['name']] += d['amount']
    
    # 4. Employee donations (individuals with employers)
    # Exclude self-employed, retired, etc.
    excluded = ["RETIRED", "SELF", "SELF-EMPLOYED", "NOT EMPLOYED", "NONE", "N/A", 
                "HOMEMAKER", "SELF EMPLOYED", "NA", "UNEMPLOYED", "OWNER", 
                "INVESTOR", "BUSINESS OWNER", ""]
    
    emp_donations = list(db.aql.execute(f'''
        FOR c IN contributed_to
            FILTER c._to IN @cmtes
            {cycle_filter}
            FOR d IN donors
                FILTER d._id == c._from
                FILTER d.donor_type IN ["individual", "likely_individual"]
                FILTER d.canonical_employer != null
                FILTER d.canonical_employer NOT IN @excluded
                RETURN {{ employer: UPPER(d.canonical_employer), amount: c.total_amount }}
    ''', bind_vars={"cmtes": cmte_ids, "excluded": excluded}))
    
    corp_emp = defaultdict(float)
    for e in emp_donations:
        corp_emp[e['employer'].strip()] += e['amount']
    
    # 5. Combine PAC and employee data
    # Try to match company names from PAC names
    combined = defaultdict(lambda: {'pac': 0, 'employees': 0})
    
    # Extract company name from PAC name
    def extract_company(pac_name):
        name = pac_name
        for suffix in [' POLITICAL ACTION COMMITTEE', ' PAC', ' EMPLOYEE FEDERAL POLITICAL ACTION COMMITTEE', 
                       ' EMPLOYEES POLITICAL ACTION COMMITTEE', ' GOOD GOVERNMENT FUND', " EMPLOYEES' PAC",
                       ', INC.', ' INC.', ' LLC', ' CORPORATION', ' CORP', ' COMPANY', ' CO.',
                       ' EMPLOYEES OF', ' EMPLOYEES']:
            name = name.replace(suffix, '')
        return name.strip().upper()
    
    for pac_name, amount in corp_pac.items():
        company = extract_company(pac_name)
        combined[company]['pac'] += amount
    
    for employer, amount in corp_emp.items():
        combined[employer]['employees'] += amount
    
    # Calculate totals
    for c in combined:
        combined[c]['total'] = combined[c]['pac'] + combined[c]['employees']
    
    return dict(combined)


def print_category(name: str, sources: List[Dict], total: float, limit: int = 10):
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
    parser = argparse.ArgumentParser(description="Five Pies with Graph Traversal")
    parser.add_argument("--candidate", "-c", help="Search by candidate name")
    parser.add_argument("--fec", "-f", help="Search by FEC candidate ID")
    parser.add_argument("--bioguide", "-b", help="Search by bioguide ID")
    parser.add_argument("--cycle", help="Filter by election cycle")
    parser.add_argument("--host", default="arangodb", help="ArangoDB host")
    parser.add_argument("--port", default="8529", help="ArangoDB port")
    parser.add_argument("--depth", type=int, default=5, help="Max traversal depth")
    
    args = parser.parse_args()
    
    client = ArangoClient(hosts=f"http://{args.host}:{args.port}")
    db = client.db("aggregation", username="root", password="ltpass")
    
    # Resolve candidate
    cand_info = resolve_candidate(db, args.candidate, args.fec, args.bioguide)
    print(f"Candidate: {cand_info['name']}")
    print(f"FEC IDs: {', '.join(cand_info['candidate_ids'])}")
    
    # Get DIRECT campaign committees only
    committees = get_candidate_committees(
        db, 
        cand_info['candidate_ids'],
        cand_info.get('committee_ids', [])
    )
    print(f"Campaign committees: {len(committees)}")
    
    print("\n" + "=" * 70)
    print(f"     FIVE PIES: {cand_info['name']}")
    print("     (Graph traversal with cycle detection)")
    print("=" * 70)
    
    # PIES 1 & 2: Direct funding traced to sources
    print("\n" + "-" * 70)
    print("PIES 1 & 2: FUNDING SOURCES (traced through graph)")
    print("-" * 70)
    
    funding = get_upstream_funding_graph(db, committees, args.cycle, args.depth)
    
    direct_total = sum(
        sum(s['amount'] for s in sources)
        for sources in funding.values()
    )
    
    print(f"\nTotal Attributed Funding: {format_money(direct_total)}")
    
    print_category("CORPORATIONS", funding.get('corporation', []), direct_total)
    print_category("TRADE ASSOCIATIONS", funding.get('trade_association', []), direct_total)
    print_category("LABOR UNIONS", funding.get('labor_union', []), direct_total)
    print_category("IDEOLOGICAL GROUPS", funding.get('ideological', []), direct_total)
    print_category("DARK MONEY ORGS", funding.get('dark_money', []), direct_total, limit=15)
    print_category("INDIVIDUALS", funding.get('individual', []), direct_total, limit=15)
    
    other = funding.get('unknown', []) + funding.get('cooperative', [])
    if other:
        print_category("OTHER", other, direct_total)
    
    # PIE 3: IE Support
    print("\n" + "-" * 70)
    print("PIE 3: INDEPENDENT EXPENDITURE SUPPORT (Super PACs)")
    print("-" * 70)
    
    ie_support, ie_support_total = get_ie_funding_graph(db, cand_info['candidate_ids'], "S", args.cycle)
    print(f"\nTotal IE Support: {format_money(ie_support_total)}")
    
    if ie_support:
        print_category("CORPORATIONS", ie_support.get('corporation', []), ie_support_total)
        print_category("TRADE ASSOCIATIONS", ie_support.get('trade_association', []), ie_support_total)
        print_category("IDEOLOGICAL", ie_support.get('ideological', []), ie_support_total)
        print_category("DARK MONEY ORGS", ie_support.get('dark_money', []), ie_support_total, limit=15)
        print_category("BILLIONAIRES/WHALES", ie_support.get('individual', []), ie_support_total, limit=15)
    
    # PIE 4: IE Opposition
    print("\n" + "-" * 70)
    print("PIE 4: INDEPENDENT EXPENDITURE OPPOSITION (Super PACs)")
    print("-" * 70)
    
    ie_oppose, ie_oppose_total = get_ie_funding_graph(db, cand_info['candidate_ids'], "O", args.cycle)
    print(f"\nTotal IE Opposition: {format_money(ie_oppose_total)}")
    
    if ie_oppose:
        print_category("CORPORATIONS", ie_oppose.get('corporation', []), ie_oppose_total)
        print_category("TRADE ASSOCIATIONS", ie_oppose.get('trade_association', []), ie_oppose_total)
        print_category("IDEOLOGICAL", ie_oppose.get('ideological', []), ie_oppose_total)
        print_category("DARK MONEY ORGS", ie_oppose.get('dark_money', []), ie_oppose_total, limit=15)
        print_category("BILLIONAIRES/WHALES", ie_oppose.get('individual', []), ie_oppose_total, limit=15)
    
    # PIE 5: Corporate Influence (PAC + Employees)
    print("\n" + "-" * 70)
    print("PIE 5: CORPORATE INFLUENCE (PAC + Employee Donations)")
    print("-" * 70)
    
    corp_influence = get_corporate_influence(db, committees, args.cycle)
    
    # Sort by total influence and display top
    sorted_corps = sorted(corp_influence.items(), key=lambda x: -x[1]['total'])
    
    total_pac = sum(d['pac'] for d in corp_influence.values())
    total_emp = sum(d['employees'] for d in corp_influence.values())
    corp_total = total_pac + total_emp
    
    print(f"\nTotal Corporate Influence: {format_money(corp_total)}")
    print(f"  ├── Corporate PAC (attributed): {format_money(total_pac)}")
    print(f"  └── Employee Donations: {format_money(total_emp)}")
    
    print(f"\n  TOP CORPORATE INFLUENCERS:")
    for company, data in sorted_corps[:20]:
        if data['total'] >= 1000:
            pac_str = format_money(data['pac']) if data['pac'] > 0 else "-"
            emp_str = format_money(data['employees']) if data['employees'] > 0 else "-"
            print(f"    • {company[:40]:<42} PAC: {pac_str:>10}  EMP: {emp_str:>10}  = {format_money(data['total'])}")
    
    if len([c for c in sorted_corps if c[1]['total'] >= 1000]) > 20:
        remaining = len([c for c in sorted_corps if c[1]['total'] >= 1000]) - 20
        print(f"    ... and {remaining} more companies")
    
    # Summary
    print("\n" + "=" * 70)
    print("                           SUMMARY")
    print("=" * 70)
    
    pro_total = direct_total + ie_support_total
    last_name = cand_info['name'].split()[-1].upper()
    
    print(f"\nPRO-{last_name}: {format_money(pro_total)}")
    print(f"  ├── Direct Funding:  {format_money(direct_total)}")
    print(f"  └── IE Support:      {format_money(ie_support_total)}")
    
    print(f"\nANTI-{last_name}: {format_money(ie_oppose_total)}")
    print(f"  └── IE Opposition:   {format_money(ie_oppose_total)}")
    
    print(f"\nCORPORATE INFLUENCE: {format_money(corp_total)}")
    print(f"  (Shows same money traced to corporate sources)")
    print()


if __name__ == "__main__":
    main()
