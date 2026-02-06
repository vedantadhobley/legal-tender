#!/usr/bin/env python3
"""Fix the enrichment data - recompute totals and run Wikidata.

This script:
1. Recomputes canonical_employers.total_amount from employer_alias_of edges
2. Runs Wikidata queries for top canonical employers to find parent companies
3. Runs Wikidata queries for whale donors to find corporate links
4. Updates corporate_families with the results

Run: docker compose exec dagster-webserver python3 -u /workspace/scripts/fix_enrichment.py
"""
import sys
sys.path.insert(0, '/workspace')

import hashlib
import time
import re
from collections import defaultdict
from datetime import datetime
from arango import ArangoClient

print("=" * 70, flush=True)
print("FIX ENRICHMENT DATA", flush=True)
print("=" * 70, flush=True)

client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
db = client.db('aggregation', username='root', password='ltpass')

# ============================================================================
# PHASE 1: Recompute canonical_employers totals
# ============================================================================
print("\n📊 PHASE 1: Recomputing canonical_employers totals...", flush=True)

# Compute totals via employer_alias_of edges
result = list(db.aql.execute('''
    FOR ce IN canonical_employers
    LET total = SUM(
        FOR alias_edge IN employer_alias_of
        FILTER alias_edge._to == ce._id
        LET emp = DOCUMENT(alias_edge._from)
        RETURN emp.total_from_employees || 0
    )
    LET donor_count = SUM(
        FOR alias_edge IN employer_alias_of
        FILTER alias_edge._to == ce._id
        LET emp = DOCUMENT(alias_edge._from)
        RETURN emp.employee_donor_count || 0
    )
    FILTER total > 0
    RETURN {_key: ce._key, canonical_name: ce.canonical_name, total_amount: total, donor_count: donor_count}
'''))

print(f"   Found {len(result)} canonical employers with non-zero totals", flush=True)

# Update in batches
batch_size = 500
for i in range(0, len(result), batch_size):
    batch = result[i:i+batch_size]
    db.aql.execute('''
        FOR doc IN @batch
        UPDATE doc._key WITH {
            total_amount: doc.total_amount,
            donor_count: doc.donor_count,
            updated_at: @now
        } IN canonical_employers
    ''', bind_vars={'batch': batch, 'now': datetime.utcnow().isoformat()})
    print(f"   Updated {min(i+batch_size, len(result))}/{len(result)}...", flush=True)

# Verify
top5 = list(db.aql.execute('''
    FOR ce IN canonical_employers
    SORT ce.total_amount DESC
    LIMIT 5
    RETURN {name: ce.canonical_name, total: ce.total_amount}
'''))
print("   Top 5 canonical employers:", flush=True)
for t in top5:
    print(f"      {t['name']}: ${t['total']:,.0f}", flush=True)

# ============================================================================
# PHASE 2: Wikidata parent company resolution for top employers
# ============================================================================
print("\n📊 PHASE 2: Wikidata parent company resolution...", flush=True)

from src.rag.wikidata_client import search_company, get_person_companies

# Get top canonical employers
top_employers = list(db.aql.execute('''
    FOR ce IN canonical_employers
    FILTER ce.total_amount >= 1000000
    SORT ce.total_amount DESC
    LIMIT 200
    RETURN {_key: ce._key, name: ce.canonical_name, total: ce.total_amount}
'''))

print(f"   Processing {len(top_employers)} employers with $1M+ donations", flush=True)

corporate_parents = []
resolved_count = 0

for i, emp in enumerate(top_employers):
    name = emp['name']
    
    # Skip non-company names
    if any(skip in name.upper() for skip in ['RETIRED', 'SELF', 'NOT EMPLOYED', 'HOMEMAKER', 'N/A', 'NONE']):
        continue
    
    # Clean for search
    clean = re.sub(r'\s+(INC|LLC|LLP|CORP|CO|LTD)\.?$', '', name, flags=re.I).strip()
    
    print(f"   [{i+1}/{len(top_employers)}] {name}...", end=' ', flush=True)
    
    try:
        result = search_company(clean)
        if result:
            parent = result.parent or result.name
            print(f"-> {parent}", flush=True)
            resolved_count += 1
            
            corporate_parents.append({
                '_key': emp['_key'],
                'name': emp['name'],
                'parent_name': parent,
                'parent_wikidata_id': result.parent_id,
                'wikidata_id': result.wikidata_id,
                'subsidiaries': result.subsidiaries[:20] if result.subsidiaries else [],
            })
        else:
            print("(not in wikidata)", flush=True)
    except Exception as e:
        print(f"(error: {e})", flush=True)
    
    time.sleep(0.3)  # Rate limit

print(f"\n   Resolved {resolved_count}/{len(top_employers)} via Wikidata", flush=True)

# Update corporate_parents collection
if corporate_parents:
    cp_coll = db.collection('corporate_parents')
    cp_coll.truncate()
    cp_coll.import_bulk(corporate_parents, on_duplicate='replace')
    print(f"   Updated corporate_parents collection: {len(corporate_parents)} records", flush=True)

# ============================================================================
# PHASE 3: Wikidata whale -> corporate links
# ============================================================================
print("\n📊 PHASE 3: Wikidata whale -> corporate links...", flush=True)

# Get whale donors who are retired/self-employed
NON_EMPLOYERS = {'RETIRED', 'SELF-EMPLOYED', 'SELF EMPLOYED', 'SELF', 'NOT EMPLOYED', 'HOMEMAKER', 'N/A', 'NONE'}

whales = list(db.aql.execute("""
    FOR d IN donors
    FILTER d.total_amount >= 500000
    FILTER d.donor_type IN ["individual", "likely_individual"]
    FILTER d.canonical_employer IN @non_employers
        OR REGEX_TEST(UPPER(d.canonical_employer || ""), "RETIRED|SELF.?EMPLOY|NOT EMPLOY")
        OR d.canonical_employer == null OR d.canonical_employer == ""
    SORT d.total_amount DESC
    LIMIT 150
    RETURN {
        _key: d._key,
        name: d.canonical_name,
        employer: d.canonical_employer,
        total: d.total_amount
    }
""", bind_vars={"non_employers": list(NON_EMPLOYERS)}))

print(f"   Found {len(whales)} retired/self-employed whale donors ($500K+)", flush=True)

# Person name pattern
person_pattern = re.compile(r'^[A-Z][A-Z\'\-]+,\s+[A-Z]')

whale_links = []
resolved_whales = 0

for i, whale in enumerate(whales):
    name = whale['name']
    
    if not person_pattern.match(name):
        continue  # Skip orgs
    
    # Convert "MELLON, TIMOTHY" to "Timothy Mellon"
    parts = name.split(',', 1)
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().split()[0].title()
        search_name = f"{first} {last}"
    else:
        search_name = name.title()
    
    print(f"   [{i+1}/{len(whales)}] {name} ({search_name})...", end=' ', flush=True)
    
    try:
        links = get_person_companies(search_name)
        if links:
            companies = [l.company_name for l in links[:3]]
            print(f"-> {companies}", flush=True)
            resolved_whales += 1
            
            for link in links:
                whale_links.append({
                    '_key': hashlib.md5(f"{name}_{link.company_name}".encode()).hexdigest()[:16],
                    'donor_key': whale['_key'],
                    'donor_name': name,
                    'canonical_name': link.company_name,
                    'relationship': link.relationship,
                    'wikidata_id': link.company_id,
                    'amount': whale['total'],
                })
        else:
            print("(not found)", flush=True)
    except Exception as e:
        print(f"(error: {e})", flush=True)
    
    time.sleep(0.3)

print(f"\n   Resolved {resolved_whales} whale donors via Wikidata", flush=True)
print(f"   Created {len(whale_links)} whale-corporate links", flush=True)

# Update whale_corporate_links
if whale_links:
    wcl = db.collection('whale_corporate_links')
    wcl.truncate()
    wcl.import_bulk(whale_links, on_duplicate='replace')
    print(f"   Updated whale_corporate_links: {len(whale_links)} records", flush=True)

# ============================================================================
# PHASE 4: Build corporate_families from all sources
# ============================================================================
print("\n📊 PHASE 4: Building corporate_families...", flush=True)

corporate_families = defaultdict(lambda: {
    'member_employers': [],
    'linked_whales': [],
    'total_from_employees': 0,
    'total_from_whales': 0,
    'wikidata_id': None,
    'subsidiaries': [],
})

# From corporate_parents (employer->parent relationships)
for cp in corporate_parents:
    parent = cp['parent_name']
    corporate_families[parent]['member_employers'].append(cp['name'])
    corporate_families[parent]['wikidata_id'] = cp.get('parent_wikidata_id') or cp.get('wikidata_id')
    corporate_families[parent]['subsidiaries'].extend(cp.get('subsidiaries', []))
    
    # Get the employer total
    emp_total = list(db.aql.execute('''
        FOR ce IN canonical_employers
        FILTER ce._key == @key
        RETURN ce.total_amount || 0
    ''', bind_vars={'key': cp['_key']}))
    if emp_total:
        corporate_families[parent]['total_from_employees'] += emp_total[0]

# From whale_links
for wl in whale_links:
    company = wl['canonical_name']
    corporate_families[company]['linked_whales'].append(wl['donor_name'])
    corporate_families[company]['total_from_whales'] += wl['amount']
    if wl.get('wikidata_id'):
        corporate_families[company]['wikidata_id'] = wl['wikidata_id']

# Write to collection
cf_coll = db.collection('corporate_families')
cf_coll.truncate()

cf_docs = []
for name, info in corporate_families.items():
    total = info['total_from_employees'] + info['total_from_whales']
    if total > 0:
        cf_docs.append({
            '_key': hashlib.md5(name.encode()).hexdigest()[:16],
            'canonical_name': name,
            'wikidata_id': info['wikidata_id'],
            'member_employers': list(set(info['member_employers'])),
            'linked_whales': list(set(info['linked_whales'])),
            'subsidiaries': list(set(info['subsidiaries']))[:20],
            'total_from_employees': info['total_from_employees'],
            'total_from_whales': info['total_from_whales'],
            'total_influence': total,
            'updated_at': datetime.utcnow().isoformat(),
        })

cf_coll.import_bulk(cf_docs, on_duplicate='replace')
print(f"   Created {len(cf_docs)} corporate families", flush=True)

# Show top families
top_fams = sorted(cf_docs, key=lambda x: -x['total_influence'])[:10]
print("\n   Top 10 Corporate Families:", flush=True)
for f in top_fams:
    print(f"      {f['canonical_name']}: ${f['total_influence']:,.0f}", flush=True)
    if f['linked_whales']:
        print(f"         Whales: {f['linked_whales'][:3]}", flush=True)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 70, flush=True)
print("SUMMARY", flush=True)
print("=" * 70, flush=True)
print(f"✅ canonical_employers totals updated: {len(result)} with non-zero totals", flush=True)
print(f"✅ corporate_parents resolved: {len(corporate_parents)} via Wikidata", flush=True)
print(f"✅ whale_corporate_links created: {len(whale_links)}", flush=True)
print(f"✅ corporate_families built: {len(cf_docs)}", flush=True)
print("\nDone!", flush=True)
