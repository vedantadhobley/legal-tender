#!/usr/bin/env python3
"""Pre-fetch Wikidata resolutions for whales and employers.

This script runs from the HOST machine (better network connectivity)
and saves results to a JSON cache file. The Dagster assets can then
use this cache instead of hitting Wikidata directly.

Run this periodically (e.g., weekly) to refresh the cache:
    python scripts/prefetch_wikidata.py

The cache is saved to: wikidata_cache.json
"""

import json
import time
import re
import sys
from datetime import datetime
from pathlib import Path

import requests
from arango import ArangoClient


WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
RATE_LIMIT_DELAY = 2.5  # Be nice to Wikidata
OUTPUT_FILE = Path(__file__).parent.parent / "wikidata_cache.json"


def sparql_query(query: str) -> dict | None:
    """Execute SPARQL query against Wikidata."""
    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query},
            headers={
                "Accept": "application/json",
                "User-Agent": "LegalTender/1.0 (FEC analysis tool)"
            },
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  ⚠️ Query failed: {e}")
        return None


def resolve_company(name: str) -> dict:
    """Resolve a company to its parent/canonical form."""
    # Clean name for search
    clean = name.upper().strip()
    clean = re.sub(r'\s+(LLC|INC|CORP|CO|LTD|LP|L\.P\.)\.?$', '', clean)
    clean = re.sub(r'[.,]+$', '', clean)
    
    query = f'''
    SELECT ?company ?companyLabel ?parent ?parentLabel WHERE {{
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      ?company rdfs:label ?label .
      FILTER(CONTAINS(UCASE(?label), "{clean}"))
      OPTIONAL {{ ?company wdt:P749 ?parent . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 5
    '''
    
    result = sparql_query(query)
    if not result or not result.get('results', {}).get('bindings'):
        return {"name": name, "canonical": name, "relationship": "self", "source": "not_found"}
    
    bindings = result['results']['bindings']
    company = bindings[0]
    
    wikidata_id = company['company']['value'].split('/')[-1]
    company_name = company['companyLabel']['value']
    
    if 'parent' in company and 'parentLabel' in company:
        parent_name = company['parentLabel']['value']
        parent_id = company['parent']['value'].split('/')[-1]
        return {
            "name": company_name,
            "canonical": parent_name,
            "relationship": "subsidiary",
            "wikidata_id": wikidata_id,
            "parent_id": parent_id,
            "source": "wikidata"
        }
    
    return {
        "name": company_name,
        "canonical": company_name,
        "relationship": "parent",
        "wikidata_id": wikidata_id,
        "source": "wikidata"
    }


def resolve_person(name: str) -> dict:
    """Resolve a person to their corporate connections."""
    # Convert "MELLON, TIMOTHY" to "Timothy Mellon"
    if ',' in name:
        parts = name.split(',', 1)
        last = parts[0].strip().title()
        first = parts[1].strip().split()[0].title()
        search_name = f"{first} {last}"
    else:
        search_name = name.title()
    
    query = f'''
    SELECT DISTINCT ?person ?personLabel ?company ?companyLabel ?relation WHERE {{
      ?person wdt:P31 wd:Q5 .
      ?person rdfs:label ?label .
      FILTER(CONTAINS(LCASE(?label), "{search_name.lower()}"))
      
      {{
        ?person wdt:P112 ?company .
        BIND("founded" AS ?relation)
      }} UNION {{
        ?company wdt:P112 ?person .
        BIND("founded" AS ?relation)
      }} UNION {{
        ?person wdt:P108 ?company .
        BIND("employed_by" AS ?relation)
      }} UNION {{
        ?person wdt:P127 ?company .
        BIND("owns" AS ?relation)
      }} UNION {{
        ?company wdt:P127 ?person .
        BIND("owns" AS ?relation)
      }}
      
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 10
    '''
    
    result = sparql_query(query)
    if not result:
        return {"name": name, "companies": [], "source": "not_found"}
    
    bindings = result.get('results', {}).get('bindings', [])
    if not bindings:
        return {"name": name, "companies": [], "source": "not_found"}
    
    companies = []
    seen = set()
    
    for binding in bindings:
        company_name = binding.get('companyLabel', {}).get('value', '')
        if company_name and company_name not in seen:
            seen.add(company_name)
            companies.append({
                "name": company_name,
                "wikidata_id": binding.get('company', {}).get('value', '').split('/')[-1],
                "relationship": binding.get('relation', {}).get('value', 'unknown')
            })
    
    person_id = bindings[0].get('person', {}).get('value', '').split('/')[-1] if bindings else None
    
    return {
        "name": name,
        "wikidata_id": person_id,
        "companies": companies,
        "source": "wikidata" if companies else "not_found"
    }


def main():
    print("=" * 70)
    print("Wikidata Pre-fetch for Legal Tender")
    print("=" * 70)
    
    # Connect to ArangoDB
    print("\n📊 Connecting to ArangoDB...")
    client = ArangoClient(hosts='http://localhost:4201')
    db = client.db('aggregation', username='root', password='ltpass')
    
    cache = {
        "employer_resolutions": {},
        "whale_resolutions": {},
        "generated_at": datetime.now().isoformat()
    }
    
    # =========================================================================
    # Get top employers to resolve
    # =========================================================================
    print("\n📊 Fetching top employers...")
    
    employers = list(db.aql.execute('''
        FOR e IN employers
        FILTER e.total_from_employees >= 1000000
        SORT e.total_from_employees DESC
        LIMIT 100
        RETURN {name: e.name, total: e.total_from_employees}
    '''))
    
    print(f"   Found {len(employers)} employers with $1M+ donations")
    
    # Skip non-corporate entries
    skip_patterns = ['RETIRED', 'SELF', 'HOMEMAKER', 'NOT EMPLOYED', 'STUDENT', 'NONE', 'N/A']
    
    print("\n🔍 Resolving employers via Wikidata...")
    for i, emp in enumerate(employers):
        name = emp['name']
        if any(p in name.upper() for p in skip_patterns):
            continue
        
        print(f"   [{i+1}/{len(employers)}] {name}...")
        result = resolve_company(name)
        cache["employer_resolutions"][name] = result
        
        if result.get('source') == 'wikidata':
            print(f"      ✓ → {result.get('canonical')} ({result.get('relationship')})")
    
    # =========================================================================
    # Get whale donors to resolve
    # =========================================================================
    print("\n📊 Fetching whale donors (RETIRED/SELF-EMPLOYED)...")
    
    whales = list(db.aql.execute('''
        FOR d IN donors
        FILTER d.total_amount >= 1000000
        FILTER d.donor_type IN ["individual", "likely_individual"]
        FILTER d.canonical_employer IN ["RETIRED", "SELF-EMPLOYED", "SELF", "HOMEMAKER", "NOT EMPLOYED", null, ""]
            OR d.canonical_employer LIKE "%RETIRED%"
            OR d.canonical_employer LIKE "%SELF%"
        SORT d.total_amount DESC
        LIMIT 100
        RETURN {name: d.canonical_name, total: d.total_amount, employer: d.canonical_employer}
    '''))
    
    print(f"   Found {len(whales)} whale donors")
    
    # Filter to actual person names
    person_pattern = re.compile(r'^[A-Z][A-Z\'-]+,\s+[A-Z]')
    person_whales = [w for w in whales if person_pattern.match(w['name'])]
    
    print(f"   {len(person_whales)} are individual persons (vs organizations)")
    
    print("\n🔍 Resolving whales via Wikidata...")
    resolved_total = 0
    
    for i, whale in enumerate(person_whales):
        name = whale['name']
        print(f"   [{i+1}/{len(person_whales)}] {name} (${whale['total']/1e6:.1f}M)...")
        
        result = resolve_person(name)
        cache["whale_resolutions"][name] = result
        
        if result.get('companies'):
            corps = ', '.join([c['name'] for c in result['companies'][:2]])
            print(f"      ✓ → {corps}")
            resolved_total += whale['total']
    
    # =========================================================================
    # Save cache
    # =========================================================================
    print(f"\n💾 Saving cache to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(cache, f, indent=2)
    
    # Summary
    emp_resolved = sum(1 for r in cache["employer_resolutions"].values() if r.get('source') == 'wikidata')
    whale_resolved = sum(1 for r in cache["whale_resolutions"].values() if r.get('companies'))
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Employers resolved: {emp_resolved}/{len(cache['employer_resolutions'])}")
    print(f"Whales resolved: {whale_resolved}/{len(cache['whale_resolutions'])}")
    print(f"Total whale money attributed: ${resolved_total/1e6:.1f}M")
    print(f"Cache saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
