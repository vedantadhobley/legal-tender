#!/usr/bin/env python3
"""Regenerate candidate funding_sources (Five Pies) data."""
import sys
sys.path.insert(0, '/workspace')

from collections import defaultdict
from arango import ArangoClient

print("Running candidate_upstream logic directly...")

client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
db = client.db('aggregation', username='root', password='ltpass')

# Terminal types
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}
PASSTHROUGH_TYPES = {"passthrough", "unknown", "super_pac_unclassified"}
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]

# Load lookup data
print("Loading lookup data...")

cmte_info = {}
for c in db.aql.execute("""
    FOR c IN committees
    RETURN {_key: c._key, name: c.CMTE_NM, terminal_type: c.terminal_type, total_receipts: c.total_receipts || 0}
"""):
    cmte_info[c['_key']] = c
print(f"  Committees: {len(cmte_info):,}")

employer_to_company = {}
if db.has_collection('employer_canonical_mapping'):
    for m in db.aql.execute("FOR m IN employer_canonical_mapping RETURN {employer: m.employer_name, company: m.canonical_name}"):
        employer_to_company[m['employer']] = m['company']
print(f"  Employer mappings: {len(employer_to_company):,}")

corporate_families = {}
if db.has_collection('corporate_families'):
    for f in db.aql.execute("FOR f IN corporate_families RETURN {name: f.canonical_name, employee_total: f.total_from_employees, whale_total: f.total_from_whales}"):
        corporate_families[f['name']] = f
print(f"  Corporate families: {len(corporate_families):,}")

whale_to_company = {}
if db.has_collection('whale_corporate_links'):
    for l in db.aql.execute("FOR l IN whale_corporate_links RETURN {donor: l.donor_name, company: l.canonical_name}"):
        whale_to_company[l['donor']] = l['company']
print(f"  Whale-corporate links: {len(whale_to_company):,}")

donor_info = {}
for d in db.aql.execute("FOR d IN donors FILTER d.total_amount >= 10000 RETURN {_key: d._key, name: d.canonical_name, employer: d.canonical_employer, total: d.total_amount}"):
    donor_info[d['_key']] = d
print(f"  Whale donors: {len(donor_info):,}")

# Transfer edges
transfer_edges = defaultdict(list)
for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
    to_cmte = e['_to'].split('/')[1]
    from_cmte = e['_from'].split('/')[1]
    transfer_edges[to_cmte].append((from_cmte, e.get('total_amount', 0) or 0))
print(f"  Transfer edges: {sum(len(v) for v in transfer_edges.values()):,}")

# Contribution edges
contrib_edges = defaultdict(list)
for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
    to_cmte = e['_to'].split('/')[1]
    donor_key = e['_from'].split('/')[1]
    contrib_edges[to_cmte].append((donor_key, e.get('total_amount', 0) or 0))
print(f"  Contribution edges: {sum(len(v) for v in contrib_edges.values()):,}")

# IE spending
ie_by_candidate = defaultdict(lambda: {'support': [], 'oppose': []})
for e in db.aql.execute("FOR e IN spent_on RETURN {cand_id: SPLIT(e._to, '/')[1], cmte_id: SPLIT(e._from, '/')[1], amount: e.total_amount, support_oppose: e.support_oppose}"):
    cand_id = e['cand_id']
    cmte_id = e['cmte_id']
    amount = e['amount'] or 0
    if e['support_oppose'] == 'S':
        ie_by_candidate[cand_id]['support'].append((cmte_id, amount))
    else:
        ie_by_candidate[cand_id]['oppose'].append((cmte_id, amount))
print(f"  IE spending for {len(ie_by_candidate):,} candidates")

def is_conduit(name):
    if not name:
        return False
    return any(p in name.upper() for p in CONDUIT_PATTERNS)

def trace_committee_sources(start_cmte_ids):
    """Trace backwards from committees to find terminal sources.
    
    For each starting committee (campaign committee), we trace all incoming
    money to find where it originated from (terminal sources).
    """
    sources = {
        'corporations': defaultdict(float),
        'trade_associations': defaultdict(float),
        'labor_unions': defaultdict(float),
        'ideological': defaultdict(float),
        'individuals': {'total': 0, 'corporate_connected': defaultdict(float), 'independent': 0, 'whale_connected': defaultdict(float)},
    }
    
    visited = set()
    
    # For each starting PCC, add all incoming transfers to the queue
    queue = []
    for pcc_id in start_cmte_ids:
        # Add transfers into this PCC
        for from_cmte, amt in transfer_edges.get(pcc_id, []):
            if amt > 0:
                queue.append((from_cmte, amt))
        
        # Add direct contributions to this PCC (whale donors)
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
    
    while queue:
        cmte_id, amount = queue.pop(0)
        
        if cmte_id in visited or amount < 1:  # Skip if already visited or tiny amount
            continue
        visited.add(cmte_id)
        
        info = cmte_info.get(cmte_id, {})
        ttype = info.get('terminal_type', 'unknown')
        name = info.get('name', cmte_id)
        
        # Terminal committee - this is a REAL funding source
        # The amount represents how much money flowed TO this committee that eventually went to the candidate
        if ttype in TERMINAL_TYPES:
            if ttype == 'corporation':
                sources['corporations'][name] += amount
            elif ttype == 'trade_association':
                sources['trade_associations'][name] += amount
            elif ttype == 'labor_union':
                sources['labor_unions'][name] += amount
            elif ttype == 'ideological':
                sources['ideological'][name] += amount
            continue
        
        # Passthrough - trace upstream transfers
        # For each upstream transfer, add to queue with the transfer amount
        for from_cmte, transfer_amt in transfer_edges.get(cmte_id, []):
            if transfer_amt > 0:
                queue.append((from_cmte, transfer_amt))
        
        # Process individual contributions to this passthrough
        for donor_key, contrib_amt in contrib_edges.get(cmte_id, []):
            donor = donor_info.get(donor_key)
            if not donor or is_conduit(donor.get('name', '')):
                continue
            
            sources['individuals']['total'] += contrib_amt
            
            # Check corporate connection
            dname = donor.get('name', '')
            employer = donor.get('employer', '')
            
            # Whale link?
            if dname in whale_to_company:
                company = whale_to_company[dname]
                sources['individuals']['whale_connected'][company] += contrib_amt
            # Employer mapping?
            elif employer in employer_to_company:
                company = employer_to_company[employer]
                sources['individuals']['corporate_connected'][company] += contrib_amt
            else:
                sources['individuals']['independent'] += contrib_amt
    
    return sources

# Get candidates with their PCCs
print("\nLoading candidates with principal campaign committees...")
cand_to_pcc = {}
for e in db.aql.execute("FOR e IN affiliated_with RETURN e"):
    cand_id = e['_to'].split('/')[1]    # Candidate is _to
    cmte_id = e['_from'].split('/')[1]  # Committee is _from
    if cand_id not in cand_to_pcc:
        cand_to_pcc[cand_id] = []
    cand_to_pcc[cand_id].append(cmte_id)
print(f"  Candidates with PCCs: {len(cand_to_pcc):,}")

# Process candidates
print("\nProcessing candidates...")
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
        print(f"  Processed {processed:,} candidates...")
        batch = []

# Final batch
if batch:
    db.aql.execute("""
        FOR doc IN @batch
        UPDATE doc._key WITH {funding_sources: doc.funding_sources} IN candidates
        OPTIONS {ignoreErrors: true}
    """, bind_vars={'batch': batch})

print(f"\nDone! Processed {processed:,} candidates with funding_sources")

# Quick verification
result = list(db.aql.execute("RETURN COUNT(FOR c IN candidates FILTER c.funding_sources != null RETURN 1)"))
print(f"Candidates with funding_sources: {result[0]:,}")
