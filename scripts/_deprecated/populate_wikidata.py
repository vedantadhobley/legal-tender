#!/usr/bin/env python3
"""Properly populate Wikidata corporate resolution data.

This script:
1. Gets top employers from donors and resolves them via Wikidata
2. Gets whale donors (retired/self-employed) and finds their corporate links
3. Builds corporate families with subsidiaries
4. Stores everything in ArangoDB

Run with: docker compose exec dagster-webserver python3 /workspace/scripts/populate_wikidata.py
"""
import sys
sys.path.insert(0, '/workspace')

import hashlib
import time
import re
from collections import defaultdict
from datetime import datetime
from arango import ArangoClient

from src.rag.wikidata_client import (
    search_company, 
    get_person_companies, 
    get_parent_company,
    get_subsidiaries,
)

print("=" * 70, flush=True)
print("WIKIDATA CORPORATE RESOLUTION", flush=True)
print("=" * 70, flush=True)

# Connect to ArangoDB
client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
db = client.db('aggregation', username='root', password='ltpass')

# Ensure collections exist
for coll in ['corporate_families', 'employer_canonical_mapping', 'whale_corporate_links']:
    if not db.has_collection(coll):
        db.create_collection(coll)

# Non-employer patterns to skip
NON_EMPLOYERS = {
    'RETIRED', 'NOT EMPLOYED', 'SELF-EMPLOYED', 'SELF EMPLOYED', 'SELF',
    'HOMEMAKER', 'N/A', 'NONE', 'UNEMPLOYED', 'STUDENT', 'NOT-EMPLOYED',
    'INFORMATION REQUESTED', 'INFORMATION REQUESTED PER BEST EFFORTS',
    'REQUESTED', 'REFUSED', 'ENTREPRENEUR', 'INVESTOR', 'PRIVATE INVESTOR'
}

# Track results
corporate_families = {}  # canonical_name -> info
employer_mappings = []
whale_links = []

# ============================================================================
# PHASE 1: Resolve top employers to parent companies
# ============================================================================
print("\n📊 PHASE 1: Resolving employers to parent companies...", flush=True)

# Get top employers (excluding non-employers)
employers = list(db.aql.execute("""
    FOR d IN donors
    FILTER d.total_amount >= 50000
    FILTER d.canonical_employer != null AND d.canonical_employer != ""
    LET emp = UPPER(d.canonical_employer)
    FILTER emp NOT IN @non_employers
    FILTER NOT REGEX_TEST(emp, "RETIRED|SELF.?EMPLOY|HOMEMAKER|NOT EMPLOY")
    COLLECT employer = d.canonical_employer 
    AGGREGATE total = SUM(d.total_amount), cnt = COUNT(d)
    SORT total DESC
    LIMIT 300
    RETURN {employer, total, cnt}
""", bind_vars={"non_employers": list(NON_EMPLOYERS)}))

print(f"   Found {len(employers)} employers to resolve", flush=True)

resolved = 0
for i, emp in enumerate(employers):
    name = emp['employer']
    
    # Clean name for search
    clean_name = re.sub(r'\s+(INC|LLC|LLP|CORP|CORPORATION|CO\.?)\.?$', '', name, flags=re.IGNORECASE).strip()
    
    print(f"   [{i+1}/{len(employers)}] {name}...", end=' ', flush=True)
    
    # Try Wikidata search
    company_info = search_company(clean_name)
    
    if company_info:
        canonical = company_info.parent or company_info.name
        print(f"-> {canonical}", flush=True)
        resolved += 1
        
        # Add to corporate families
        if canonical not in corporate_families:
            corporate_families[canonical] = {
                'canonical_name': canonical,
                'wikidata_id': company_info.wikidata_id,
                'member_employers': [],
                'linked_whales': [],
                'total_from_employees': 0,
                'total_from_whales': 0,
                'subsidiaries': company_info.subsidiaries or [],
            }
        
        corporate_families[canonical]['member_employers'].append(name)
        corporate_families[canonical]['total_from_employees'] += emp['total']
        
        # Add employer mapping
        employer_mappings.append({
            '_key': hashlib.md5(name.encode()).hexdigest()[:16],
            'employer_name': name,
            'canonical_name': canonical,
            'wikidata_id': company_info.wikidata_id,
            'relationship': 'subsidiary' if company_info.parent else 'self',
            'amount': emp['total'],
        })
    else:
        print("(not found)", flush=True)
        # Still create a self-mapping
        employer_mappings.append({
            '_key': hashlib.md5(name.encode()).hexdigest()[:16],
            'employer_name': name,
            'canonical_name': clean_name,
            'wikidata_id': None,
            'relationship': 'self',
            'amount': emp['total'],
        })
    
    # Rate limiting
    time.sleep(0.5)

print(f"\n   Resolved {resolved}/{len(employers)} employers via Wikidata", flush=True)

# ============================================================================
# PHASE 2: Resolve whale donors to their corporate origins
# ============================================================================
print("\n📊 PHASE 2: Resolving whale donors to corporate origins...", flush=True)

# Get whale donors who are retired/self-employed
whales = list(db.aql.execute("""
    FOR d IN donors
    FILTER d.total_amount >= 500000
    FILTER d.donor_type IN ["individual", "likely_individual"]
    FILTER d.canonical_employer IN @non_employers
        OR REGEX_TEST(UPPER(d.canonical_employer || ""), "RETIRED|SELF.?EMPLOY|NOT EMPLOY")
        OR d.canonical_employer == null OR d.canonical_employer == ""
    SORT d.total_amount DESC
    LIMIT 200
    RETURN {
        _key: d._key,
        name: d.canonical_name,
        employer: d.canonical_employer,
        total: d.total_amount
    }
""", bind_vars={"non_employers": list(NON_EMPLOYERS)}))

print(f"   Found {len(whales)} retired/self-employed whale donors", flush=True)

# Pattern to identify person names (LASTNAME, FIRSTNAME)
person_pattern = re.compile(r'^[A-Z][A-Z\'\-]+,\s+[A-Z]')

resolved_whales = 0
for i, whale in enumerate(whales):
    name = whale['name']
    
    # Skip if not a person name pattern
    if not person_pattern.match(name):
        continue
    
    # Convert "MELLON, TIMOTHY" to "Timothy Mellon" for Wikidata search
    parts = name.split(',', 1)
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().split()[0].title()
        search_name = f"{first} {last}"
    else:
        search_name = name.title()
    
    print(f"   [{i+1}/{len(whales)}] {name} ({search_name})...", end=' ', flush=True)
    
    # Query Wikidata for person's companies
    links = get_person_companies(search_name)
    
    if links:
        print(f"-> {[l.company_name for l in links[:3]]}", flush=True)
        resolved_whales += 1
        
        for link in links:
            company = link.company_name
            
            # Add whale link
            whale_links.append({
                '_key': hashlib.md5(f"{name}_{company}".encode()).hexdigest()[:16],
                'donor_key': whale['_key'],
                'donor_name': name,
                'canonical_name': company,
                'relationship': link.relationship,
                'wikidata_id': link.company_id,
                'amount': whale['total'],
            })
            
            # Add to corporate families
            if company not in corporate_families:
                corporate_families[company] = {
                    'canonical_name': company,
                    'wikidata_id': link.company_id,
                    'member_employers': [],
                    'linked_whales': [],
                    'total_from_employees': 0,
                    'total_from_whales': 0,
                    'subsidiaries': [],
                }
            
            corporate_families[company]['linked_whales'].append(name)
            corporate_families[company]['total_from_whales'] += whale['total']
    else:
        print("(not found)", flush=True)
    
    time.sleep(0.5)

print(f"\n   Resolved {resolved_whales} whale donors via Wikidata", flush=True)
print(f"   Created {len(whale_links)} whale-corporate links", flush=True)

# ============================================================================
# PHASE 3: Write results to ArangoDB
# ============================================================================
print("\n📊 PHASE 3: Writing results to ArangoDB...", flush=True)

# Write corporate families
corp_coll = db.collection('corporate_families')
corp_coll.truncate()

family_docs = []
for canonical, info in corporate_families.items():
    total_influence = info['total_from_employees'] + info['total_from_whales']
    doc = {
        '_key': hashlib.md5(canonical.encode()).hexdigest()[:16],
        'canonical_name': canonical,
        'wikidata_id': info.get('wikidata_id'),
        'member_employers': list(set(info['member_employers'])),
        'linked_whales': list(set(info['linked_whales'])),
        'subsidiaries': info.get('subsidiaries', []),
        'total_from_employees': info['total_from_employees'],
        'total_from_whales': info['total_from_whales'],
        'total_influence': total_influence,
        'created_at': datetime.utcnow().isoformat(),
    }
    family_docs.append(doc)

corp_coll.import_bulk(family_docs, on_duplicate='replace')
print(f"   Created {len(family_docs)} corporate families", flush=True)

# Write employer mappings
mapping_coll = db.collection('employer_canonical_mapping')
mapping_coll.truncate()
mapping_coll.import_bulk(employer_mappings, on_duplicate='replace')
print(f"   Created {len(employer_mappings)} employer mappings", flush=True)

# Write whale links
whale_coll = db.collection('whale_corporate_links')
whale_coll.truncate()
if whale_links:
    whale_coll.import_bulk(whale_links, on_duplicate='replace')
print(f"   Created {len(whale_links)} whale-corporate links", flush=True)

# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 70, flush=True)
print("SUMMARY", flush=True)
print("=" * 70, flush=True)

# Top corporate families by influence
top_corps = sorted(family_docs, key=lambda x: -x['total_influence'])[:10]
print("\nTop 10 Corporate Families by Total Influence:", flush=True)
for corp in top_corps:
    print(f"   {corp['canonical_name']}: ${corp['total_influence']:,.0f}", flush=True)
    print(f"      Employees: ${corp['total_from_employees']:,.0f}, Whales: ${corp['total_from_whales']:,.0f}", flush=True)
    if corp['linked_whales']:
        print(f"      Whales: {corp['linked_whales'][:3]}", flush=True)

print("\n✅ Done!", flush=True)
