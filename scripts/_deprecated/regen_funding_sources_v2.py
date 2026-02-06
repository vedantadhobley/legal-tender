#!/usr/bin/env python3
"""Regenerate candidate funding_sources (Five Pies) data - FIXED VERSION.

The key fix: When tracing through a passthrough committee, we weight the
upstream sources by the proportion of money that flowed to our target candidate.

Example: If PAC A receives $100K from Corp X and $50K from individuals,
and PAC A gives $10K to Candidate 1 and $5K to Candidate 2:
- Candidate 1 gets 10/15 = 66.7% of PAC A's sources
- Candidate 2 gets 5/15 = 33.3% of PAC A's sources
"""
import sys
sys.path.insert(0, '/workspace')

from collections import defaultdict
from arango import ArangoClient

print("Running candidate_upstream logic (FIXED VERSION)...", flush=True)

client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
db = client.db('aggregation', username='root', password='ltpass')

# Terminal types
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]

# Load lookup data
print("Loading lookup data...", flush=True)

cmte_info = {}
for c in db.aql.execute("""
    FOR c IN committees
    RETURN {_key: c._key, name: c.CMTE_NM, terminal_type: c.terminal_type, total_receipts: c.total_receipts || 0}
"""):
    cmte_info[c['_key']] = c
print(f"  Committees: {len(cmte_info):,}", flush=True)

employer_to_company = {}
if db.has_collection('employer_canonical_mapping'):
    for m in db.aql.execute("FOR m IN employer_canonical_mapping RETURN {employer: m.employer_name, company: m.canonical_name}"):
        employer_to_company[m['employer']] = m['company']
print(f"  Employer mappings: {len(employer_to_company):,}", flush=True)

whale_to_company = {}
if db.has_collection('whale_corporate_links'):
    for l in db.aql.execute("FOR l IN whale_corporate_links RETURN {donor: l.donor_name, company: l.canonical_name}"):
        whale_to_company[l['donor']] = l['company']
print(f"  Whale-corporate links: {len(whale_to_company):,}", flush=True)

donor_info = {}
for d in db.aql.execute("FOR d IN donors FILTER d.total_amount >= 10000 RETURN {_key: d._key, name: d.canonical_name, employer: d.canonical_employer, total: d.total_amount}"):
    donor_info[d['_key']] = d
print(f"  Whale donors: {len(donor_info):,}", flush=True)

# Transfer edges (who sent money to this committee, and how much)
transfer_edges = defaultdict(list)  # cmte_id -> [(from_cmte, amount), ...]
for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
    to_cmte = e['_to'].split('/')[1]
    from_cmte = e['_from'].split('/')[1]
    transfer_edges[to_cmte].append((from_cmte, e.get('total_amount', 0) or 0))
print(f"  Transfer edges: {sum(len(v) for v in transfer_edges.values()):,}", flush=True)

# Contribution edges (who contributed to this committee)
contrib_edges = defaultdict(list)  # cmte_id -> [(donor_key, amount), ...]
for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
    to_cmte = e['_to'].split('/')[1]
    donor_key = e['_from'].split('/')[1]
    contrib_edges[to_cmte].append((donor_key, e.get('total_amount', 0) or 0))
print(f"  Contribution edges: {sum(len(v) for v in contrib_edges.values()):,}", flush=True)

# Pre-compute committee total inflows (for weighting)
print("Pre-computing committee inflows...", flush=True)
cmte_total_inflow = {}
for cmte_id in cmte_info:
    # Total = transfers in + contributions in
    transfer_in = sum(amt for _, amt in transfer_edges.get(cmte_id, []))
    contrib_in = sum(amt for _, amt in contrib_edges.get(cmte_id, []))
    cmte_total_inflow[cmte_id] = transfer_in + contrib_in
print(f"  Computed inflows for {len(cmte_total_inflow):,} committees", flush=True)

def is_conduit(name):
    if not name:
        return False
    return any(p in name.upper() for p in CONDUIT_PATTERNS)

def trace_committee_sources(start_cmte_ids):
    """Trace backwards from committees to find terminal sources.
    
    KEY FIX: We track a 'weight' for each committee in the queue.
    The weight represents what fraction of that committee's sources
    should be attributed to the candidate we're tracing for.
    
    Example: If we trace from PCC and find that Super PAC gave us $10K,
    but Super PAC received $100K total, we give weight 10K/100K = 10% to
    all of Super PAC's upstream sources.
    """
    sources = {
        'corporations': defaultdict(float),
        'trade_associations': defaultdict(float),
        'labor_unions': defaultdict(float),
        'ideological': defaultdict(float),
        'individuals': {'total': 0, 'corporate_connected': defaultdict(float), 'independent': 0, 'whale_connected': defaultdict(float)},
    }
    
    visited = {}  # cmte_id -> total weight already attributed
    
    # For each starting PCC, process direct contributions and queue upstream transfers
    for pcc_id in start_cmte_ids:
        # Direct contributions to PCC (these are 100% attributable to this candidate)
        for donor_key, amt in contrib_edges.get(pcc_id, []):
            donor = donor_info.get(donor_key)
            if not donor or is_conduit(donor.get('name', '')):
                continue
            
            sources['individuals']['total'] += amt
            dname = donor.get('name', '')
            employer = donor.get('employer', '')
            
            if dname in whale_to_company:
                sources['individuals']['whale_connected'][whale_to_company[dname]] += amt
            elif employer in employer_to_company:
                sources['individuals']['corporate_connected'][employer_to_company[employer]] += amt
            else:
                sources['individuals']['independent'] += amt
    
    # BFS queue: (cmte_id, weight) where weight is the fraction of sources to attribute
    queue = []
    for pcc_id in start_cmte_ids:
        for from_cmte, transfer_amt in transfer_edges.get(pcc_id, []):
            if transfer_amt > 0:
                # Weight = amount transferred / total inflow of source committee
                total_inflow = cmte_total_inflow.get(from_cmte, 0)
                if total_inflow > 0:
                    weight = transfer_amt / total_inflow
                    # Cap weight at 1.0 (can happen with data issues)
                    weight = min(weight, 1.0)
                    queue.append((from_cmte, weight, transfer_amt))
    
    while queue:
        cmte_id, weight, direct_amt = queue.pop(0)
        
        if weight < 0.001:  # Skip negligible weights
            continue
        
        # Track total weight attributed to avoid double-counting
        prev_weight = visited.get(cmte_id, 0)
        if prev_weight >= 1.0:
            continue
        new_weight = min(weight, 1.0 - prev_weight)
        visited[cmte_id] = prev_weight + new_weight
        
        info = cmte_info.get(cmte_id, {})
        ttype = info.get('terminal_type', 'unknown')
        name = info.get('name', cmte_id)
        
        # Terminal committee - attribute directly with weight
        if ttype in TERMINAL_TYPES:
            # Use the direct transfer amount, not weighted
            # (the transfer amount IS the amount that came to our candidate)
            if ttype == 'corporation':
                sources['corporations'][name] += direct_amt
            elif ttype == 'trade_association':
                sources['trade_associations'][name] += direct_amt
            elif ttype == 'labor_union':
                sources['labor_unions'][name] += direct_amt
            elif ttype == 'ideological':
                sources['ideological'][name] += direct_amt
            continue
        
        # Passthrough - attribute upstream sources with weight
        # Process individual contributions to this passthrough
        for donor_key, contrib_amt in contrib_edges.get(cmte_id, []):
            donor = donor_info.get(donor_key)
            if not donor or is_conduit(donor.get('name', '')):
                continue
            
            # Weighted contribution
            weighted_amt = contrib_amt * new_weight
            sources['individuals']['total'] += weighted_amt
            
            dname = donor.get('name', '')
            employer = donor.get('employer', '')
            
            if dname in whale_to_company:
                sources['individuals']['whale_connected'][whale_to_company[dname]] += weighted_amt
            elif employer in employer_to_company:
                sources['individuals']['corporate_connected'][employer_to_company[employer]] += weighted_amt
            else:
                sources['individuals']['independent'] += weighted_amt
        
        # Queue upstream transfers with propagated weight
        for from_cmte, transfer_amt in transfer_edges.get(cmte_id, []):
            if transfer_amt > 0:
                total_inflow = cmte_total_inflow.get(from_cmte, 0)
                if total_inflow > 0:
                    upstream_weight = new_weight * (transfer_amt / total_inflow)
                    upstream_weight = min(upstream_weight, 1.0)
                    if upstream_weight >= 0.001:
                        # Direct amount for terminal calculation
                        upstream_direct = transfer_amt * new_weight
                        queue.append((from_cmte, upstream_weight, upstream_direct))
    
    return sources

# Get candidates with their PCCs
print("\nLoading candidates with principal campaign committees...", flush=True)
cand_to_pcc = {}
for e in db.aql.execute("FOR e IN affiliated_with RETURN e"):
    cand_id = e['_to'].split('/')[1]
    cmte_id = e['_from'].split('/')[1]
    if cand_id not in cand_to_pcc:
        cand_to_pcc[cand_id] = []
    cand_to_pcc[cand_id].append(cmte_id)
print(f"  Candidates with PCCs: {len(cand_to_pcc):,}", flush=True)

# Process candidates
print("\nProcessing candidates...", flush=True)
processed = 0
batch = []

for cand_id, pcc_ids in cand_to_pcc.items():
    sources = trace_committee_sources(pcc_ids)
    
    # Build funding_sources object
    funding = {
        'corporations': {
            'amount': sum(sources['corporations'].values()),
            'top': sorted([(k, v) for k, v in sources['corporations'].items() if v > 0], key=lambda x: -x[1])[:10]
        },
        'trade_associations': {
            'amount': sum(sources['trade_associations'].values()),
            'top': sorted([(k, v) for k, v in sources['trade_associations'].items() if v > 0], key=lambda x: -x[1])[:10]
        },
        'labor_unions': {
            'amount': sum(sources['labor_unions'].values()),
            'top': sorted([(k, v) for k, v in sources['labor_unions'].items() if v > 0], key=lambda x: -x[1])[:10]
        },
        'ideological': {
            'amount': sum(sources['ideological'].values()),
            'top': sorted([(k, v) for k, v in sources['ideological'].items() if v > 0], key=lambda x: -x[1])[:10]
        },
        'individuals': {
            'amount': sources['individuals']['total'],
            'corporate_connected': {
                'amount': sum(sources['individuals']['corporate_connected'].values()) + sum(sources['individuals']['whale_connected'].values()),
                'top': sorted(
                    [(k, v) for k, v in {**sources['individuals']['corporate_connected'], **sources['individuals']['whale_connected']}.items() if v > 0],
                    key=lambda x: -x[1]
                )[:10]
            },
            'independent': sources['individuals']['independent']
        },
    }
    
    batch.append({'_key': cand_id, 'funding_sources': funding})
    processed += 1
    
    if len(batch) >= 500:
        db.aql.execute("""
            FOR doc IN @batch
            UPDATE doc._key WITH {funding_sources: doc.funding_sources} IN candidates
            OPTIONS {ignoreErrors: true}
        """, bind_vars={'batch': batch})
        print(f"  Processed {processed:,} candidates...", flush=True)
        batch = []

# Final batch
if batch:
    db.aql.execute("""
        FOR doc IN @batch
        UPDATE doc._key WITH {funding_sources: doc.funding_sources} IN candidates
        OPTIONS {ignoreErrors: true}
    """, bind_vars={'batch': batch})

print(f"\nDone! Processed {processed:,} candidates with funding_sources", flush=True)

# Quick verification
result = list(db.aql.execute("RETURN COUNT(FOR c IN candidates FILTER c.funding_sources != null RETURN 1)"))
print(f"Candidates with funding_sources: {result[0]:,}", flush=True)
