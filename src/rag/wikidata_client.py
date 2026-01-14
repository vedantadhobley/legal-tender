"""Wikidata SPARQL client for corporate entity resolution.

This module queries Wikidata to resolve:
1. Company → Parent/Subsidiary relationships
2. Person → Company relationships (founders, executives, owners)
3. Company name variations and aliases

No hardcoding - all corporate knowledge comes from Wikidata.
"""

import requests
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from functools import lru_cache
import time

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1.0  # Be nice to Wikidata


@dataclass
class CompanyInfo:
    """Information about a company from Wikidata."""
    wikidata_id: str
    name: str
    parent: Optional[str] = None
    parent_id: Optional[str] = None
    subsidiaries: List[str] = None
    aliases: List[str] = None
    industry: Optional[str] = None
    
    def __post_init__(self):
        if self.subsidiaries is None:
            self.subsidiaries = []
        if self.aliases is None:
            self.aliases = []


@dataclass
class PersonCompanyLink:
    """Link between a person and a company."""
    person_name: str
    company_name: str
    company_id: str
    relationship: str  # founded, employed_by, owns, manages, ceo_of


def _execute_sparql(query: str) -> Optional[Dict[str, Any]]:
    """Execute a SPARQL query against Wikidata."""
    try:
        time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
        response = requests.get(
            WIKIDATA_SPARQL_ENDPOINT,
            params={"query": query},
            headers={
                "Accept": "application/json",
                "User-Agent": "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"
            },
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.warning(f"Wikidata query failed: {e}")
        return None


def search_company(name: str) -> Optional[CompanyInfo]:
    """
    Search for a company by name and get its corporate family info.
    
    Returns parent company, subsidiaries, and aliases.
    """
    # First, find the company entity
    search_query = f'''
    SELECT ?company ?companyLabel ?parentLabel ?parent WHERE {{
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      ?company rdfs:label "{name}"@en .
      OPTIONAL {{ ?company wdt:P749 ?parent . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    '''
    
    result = _execute_sparql(search_query)
    if not result or not result.get('results', {}).get('bindings'):
        # Try case-insensitive search
        search_query = f'''
        SELECT ?company ?companyLabel ?parentLabel ?parent WHERE {{
          ?company wdt:P31/wdt:P279* wd:Q4830453 .
          ?company rdfs:label ?label .
          FILTER(LCASE(?label) = LCASE("{name}"))
          OPTIONAL {{ ?company wdt:P749 ?parent . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1
        '''
        result = _execute_sparql(search_query)
    
    if not result or not result.get('results', {}).get('bindings'):
        return None
    
    row = result['results']['bindings'][0]
    company_id = row['company']['value'].split('/')[-1]
    company_name = row['companyLabel']['value']
    
    parent_name = None
    parent_id = None
    if 'parent' in row:
        parent_id = row['parent']['value'].split('/')[-1]
        parent_name = row.get('parentLabel', {}).get('value')
    
    # Get subsidiaries
    subsidiaries = get_subsidiaries(company_id)
    
    # Get aliases
    aliases = get_company_aliases(company_id)
    
    return CompanyInfo(
        wikidata_id=company_id,
        name=company_name,
        parent=parent_name,
        parent_id=parent_id,
        subsidiaries=subsidiaries,
        aliases=aliases
    )


def get_subsidiaries(company_id: str) -> List[str]:
    """Get all subsidiaries of a company by Wikidata ID."""
    query = f'''
    SELECT ?subsidiaryLabel WHERE {{
      wd:{company_id} wdt:P355 ?subsidiary .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 50
    '''
    
    result = _execute_sparql(query)
    if not result:
        return []
    
    subsidiaries = []
    for row in result.get('results', {}).get('bindings', []):
        name = row['subsidiaryLabel']['value']
        if not name.startswith('Q'):  # Skip unresolved entities
            subsidiaries.append(name)
    
    return subsidiaries


def get_company_aliases(company_id: str) -> List[str]:
    """Get alternative names/aliases for a company."""
    query = f'''
    SELECT ?alias WHERE {{
      wd:{company_id} skos:altLabel ?alias .
      FILTER(LANG(?alias) = "en")
    }}
    LIMIT 20
    '''
    
    result = _execute_sparql(query)
    if not result:
        return []
    
    return [row['alias']['value'] for row in result.get('results', {}).get('bindings', [])]


def get_parent_company(company_name: str) -> Optional[str]:
    """Get the parent company name for a given company."""
    query = f'''
    SELECT ?parentLabel WHERE {{
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      ?company rdfs:label "{company_name}"@en .
      ?company wdt:P749 ?parent .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    '''
    
    result = _execute_sparql(query)
    if not result or not result.get('results', {}).get('bindings'):
        return None
    
    parent = result['results']['bindings'][0]['parentLabel']['value']
    return parent if not parent.startswith('Q') else None


def get_person_companies(person_name: str) -> List[PersonCompanyLink]:
    """
    Get all companies associated with a person.
    
    Looks for:
    - Companies they founded (P112)
    - Companies they worked at (P108)  
    - Companies they own (P127)
    - Companies they manage/direct (P1037)
    - Companies where they are CEO (P169)
    """
    query = f'''
    SELECT DISTINCT ?companyLabel ?company ?role WHERE {{
      ?person wdt:P31 wd:Q5 .
      ?person rdfs:label "{person_name}"@en .
      {{
        ?company wdt:P112 ?person .
        BIND("founded" as ?role)
      }} UNION {{
        ?person wdt:P108 ?company .
        ?company wdt:P31/wdt:P279* wd:Q4830453 .
        BIND("employed_by" as ?role)
      }} UNION {{
        ?company wdt:P127 ?person .
        BIND("owns" as ?role)
      }} UNION {{
        ?person wdt:P1037 ?company .
        BIND("manages" as ?role)
      }} UNION {{
        ?company wdt:P169 ?person .
        BIND("ceo_of" as ?role)
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 20
    '''
    
    result = _execute_sparql(query)
    if not result:
        return []
    
    links = []
    seen = set()
    for row in result.get('results', {}).get('bindings', []):
        company_name = row['companyLabel']['value']
        if company_name.startswith('Q'):  # Skip unresolved
            continue
        
        company_id = row['company']['value'].split('/')[-1]
        role = row['role']['value']
        
        # Dedupe
        key = (company_name, role)
        if key in seen:
            continue
        seen.add(key)
        
        links.append(PersonCompanyLink(
            person_name=person_name,
            company_name=company_name,
            company_id=company_id,
            relationship=role
        ))
    
    return links


def get_companies_by_owner(owner_name: str) -> List[str]:
    """Get all companies owned/controlled by a person (like Elon Musk)."""
    query = f'''
    SELECT DISTINCT ?companyLabel WHERE {{
      ?owner wdt:P31 wd:Q5 .
      ?owner rdfs:label "{owner_name}"@en .
      {{
        ?company wdt:P112 ?owner .
      }} UNION {{
        ?company wdt:P127 ?owner .
      }} UNION {{
        ?owner wdt:P1037 ?company .
      }} UNION {{
        ?company wdt:P169 ?owner .
      }}
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    '''
    
    result = _execute_sparql(query)
    if not result:
        return []
    
    companies = []
    for row in result.get('results', {}).get('bindings', []):
        name = row['companyLabel']['value']
        if not name.startswith('Q'):
            companies.append(name)
    
    return companies


def resolve_company_to_canonical(company_name: str) -> Dict[str, Any]:
    """
    Resolve a company name to its canonical parent and get full corporate family.
    
    Returns:
        {
            'canonical': 'ALPHABET INC',
            'original': 'GOOGLE',
            'relationship': 'subsidiary_of',
            'family': ['Google', 'YouTube', 'Waymo', ...],
            'source': 'wikidata'
        }
    """
    info = search_company(company_name)
    if not info:
        return {
            'canonical': company_name,
            'original': company_name,
            'relationship': 'self',
            'family': [],
            'source': 'not_found'
        }
    
    # If has parent, the canonical is the parent
    if info.parent:
        # Get parent's subsidiaries for full family
        parent_info = search_company(info.parent)
        family = parent_info.subsidiaries if parent_info else []
        
        return {
            'canonical': info.parent,
            'original': company_name,
            'relationship': 'subsidiary_of',
            'family': family,
            'wikidata_id': info.wikidata_id,
            'parent_id': info.parent_id,
            'source': 'wikidata'
        }
    
    # No parent - this might BE the parent
    return {
        'canonical': info.name,
        'original': company_name,
        'relationship': 'parent',
        'family': info.subsidiaries,
        'wikidata_id': info.wikidata_id,
        'source': 'wikidata'
    }


def resolve_person_to_companies(person_name: str) -> Dict[str, Any]:
    """
    Resolve a person (retired whale) to their corporate connections.
    
    Returns:
        {
            'person': 'Timothy Mellon',
            'companies': [
                {'name': 'Pan Am Systems', 'relationship': 'founded'},
                ...
            ],
            'primary_company': 'Pan Am Systems',
            'source': 'wikidata'
        }
    """
    links = get_person_companies(person_name)
    
    if not links:
        return {
            'person': person_name,
            'companies': [],
            'primary_company': None,
            'source': 'not_found'
        }
    
    companies = [
        {'name': link.company_name, 'relationship': link.relationship, 'wikidata_id': link.company_id}
        for link in links
    ]
    
    # Primary company: prefer founded > ceo_of > owns > manages > employed_by
    priority = {'founded': 0, 'ceo_of': 1, 'owns': 2, 'manages': 3, 'employed_by': 4}
    sorted_companies = sorted(companies, key=lambda x: priority.get(x['relationship'], 5))
    primary = sorted_companies[0]['name'] if sorted_companies else None
    
    return {
        'person': person_name,
        'companies': companies,
        'primary_company': primary,
        'source': 'wikidata'
    }


if __name__ == '__main__':
    # Test cases
    print("=" * 70)
    print("WIKIDATA CLIENT TESTS")
    print("=" * 70)
    
    # Test company resolution
    print("\n--- COMPANY RESOLUTION ---")
    for company in ['Google', 'YouTube', 'WhatsApp', 'LinkedIn', 'Tesla']:
        result = resolve_company_to_canonical(company)
        print(f"{company} → {result['canonical']} ({result['relationship']})")
        if result['family']:
            print(f"  Family: {result['family'][:5]}...")
    
    # Test person resolution
    print("\n--- PERSON RESOLUTION ---")
    for person in ['Elon Musk', 'Timothy Mellon', 'Jan Koum']:
        result = resolve_person_to_companies(person)
        print(f"{person}:")
        for c in result['companies'][:3]:
            print(f"  - {c['name']} ({c['relationship']})")
