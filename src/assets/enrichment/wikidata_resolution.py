"""Wikidata-based canonical employer resolution.

This asset uses Wikidata to:
1. Resolve employer names to their parent companies
2. Link retired/self-employed whale donors to their corporate origins
3. Build corporate family trees (subsidiaries)

No hardcoding - all corporate knowledge comes from Wikidata.

Cache Strategy:
- Checks for wikidata_cache.json first (pre-fetched resolutions)
- Falls back to live Wikidata queries for missing entries
- Caches new resolutions for future runs

Pipeline Integration:
- Depends on: employers, donors (from graph layer)
- Creates: corporate_families, employer_canonical_mapping, whale_corporate_links
- Runs automatically after graph rebuild
"""

import hashlib
import json
import os
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Optional
import logging

from dagster import asset, AssetExecutionContext, Config, Output, MetadataValue
from pydantic import Field

from src.resources.arango import ArangoDBResource

logger = logging.getLogger(__name__)

# Cache file location (mounted volume in container)
WIKIDATA_CACHE_PATH = os.environ.get("WIKIDATA_CACHE_PATH", "/workspace/wikidata_cache.json")


class WikidataResolutionConfig(Config):
    """Configuration for Wikidata resolution."""
    min_whale_amount: int = Field(
        default=1_000_000,
        description="Minimum donation amount to consider for whale resolution"
    )
    min_employer_amount: int = Field(
        default=100_000,
        description="Minimum total from employees to resolve employer"
    )
    max_employers: int = Field(
        default=500,
        description="Maximum employers to process (top by amount)"
    )
    max_whales: int = Field(
        default=200,
        description="Maximum whales to process (top by amount)"
    )
    resolve_whales: bool = Field(
        default=True,
        description="Whether to resolve retired/self-employed whales"
    )
    use_cache: bool = Field(
        default=True,
        description="Whether to use cached Wikidata resolutions"
    )
    live_queries: bool = Field(
        default=False,
        description="Whether to make live Wikidata queries for cache misses (slow, rate-limited)"
    )


def _load_cache() -> Dict[str, Any]:
    """Load Wikidata cache from file."""
    if os.path.exists(WIKIDATA_CACHE_PATH):
        try:
            with open(WIKIDATA_CACHE_PATH, 'r') as f:
                cache = json.load(f)
                logger.info(f"Loaded cache with {len(cache.get('employer_resolutions', {}))} employers, "
                          f"{len(cache.get('whale_resolutions', {}))} whales")
                return cache
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load cache: {e}")
    return {"employer_resolutions": {}, "whale_resolutions": {}}


def _save_cache(cache: Dict[str, Any]) -> None:
    """Save Wikidata cache to file."""
    try:
        cache["updated_at"] = datetime.utcnow().isoformat()
        with open(WIKIDATA_CACHE_PATH, 'w') as f:
            json.dump(cache, f, indent=2)
        logger.info(f"Saved cache to {WIKIDATA_CACHE_PATH}")
    except IOError as e:
        logger.warning(f"Failed to save cache: {e}")


@asset(
    deps=["employers", "donors"],
    description="Resolve employers and whales to canonical corporate entities using Wikidata",
    group_name="enrichment",
    compute_kind="external_api",
)
def wikidata_corporate_resolution(
    context: AssetExecutionContext,
    config: WikidataResolutionConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """
    Use Wikidata to resolve:
    1. Employer → Parent company (Google → Alphabet)
    2. Whale donor → Corporate origin (Jan Koum → WhatsApp → Meta)
    
    Creates/updates collections:
    - corporate_families: Canonical corporate entities with total influence
    - employer_canonical_mapping: Maps employers to canonical
    - whale_corporate_links: Links whale donors to their corporate origins
    
    Cache Strategy:
    - First checks wikidata_cache.json for pre-fetched resolutions
    - Only makes live Wikidata queries if live_queries=True
    - Updates cache with any new resolutions
    """
    from src.rag.employer_normalization import normalize_employer_name, NON_EMPLOYERS
    
    # Load cache
    cache = _load_cache() if config.use_cache else {"employer_resolutions": {}, "whale_resolutions": {}}
    employer_cache = cache.get("employer_resolutions", {})
    whale_cache = cache.get("whale_resolutions", {})
    
    context.log.info(f"📦 Cache loaded: {len(employer_cache)} employers, {len(whale_cache)} whales")
    
    # Connect to ArangoDB
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Ensure collections exist
        for coll_name in ['corporate_families', 'employer_canonical_mapping', 'whale_corporate_links']:
            if not db.has_collection(coll_name):
                db.create_collection(coll_name)
                context.log.info(f"Created collection: {coll_name}")
        
        stats = {
            'employers_processed': 0,
            'employers_from_cache': 0,
            'employers_from_live': 0,
            'whales_processed': 0,
            'whales_from_cache': 0,
            'whales_from_live': 0,
            'corporate_families_created': 0,
            'total_whale_money_attributed': 0,
        }
        
        # Track corporate families
        corporate_families = {}  # canonical_name -> {info}
        employer_mappings = []
        
        # =====================================================================
        # PHASE 1: Resolve employers to parent companies
        # =====================================================================
        context.log.info("Phase 1: Resolving employers to parent companies...")
        
        employers = list(db.aql.execute("""
            FOR e IN employers
            FILTER e.total_from_employees >= @min_amount
            SORT e.total_from_employees DESC
            LIMIT @max_employers
            RETURN {
                _key: e._key,
                name: e.name,
                total: e.total_from_employees,
                donor_count: e.employee_donor_count
            }
        """, bind_vars={
            "min_amount": config.min_employer_amount,
            "max_employers": config.max_employers
        }))
        
        context.log.info(f"  Found {len(employers)} employers with ${config.min_employer_amount:,}+ donations")
        
        for emp in employers:
            name = emp['name']
            normalized, meta = normalize_employer_name(name)
            
            # Skip non-employers
            if meta.get('is_non_employer'):
                continue
            
            stats['employers_processed'] += 1
            
            # Check cache first
            cached = employer_cache.get(name)
            if cached and cached.get('source') == 'wikidata':
                stats['employers_from_cache'] += 1
                canonical = cached.get('canonical', normalized)
                relationship = cached.get('relationship', 'self')
                wikidata_id = cached.get('wikidata_id')
            elif config.live_queries:
                # Make live query (slow, rate-limited)
                from src.rag.wikidata_client import resolve_company_to_canonical
                result = resolve_company_to_canonical(normalized)
                
                if result.get('source') == 'wikidata':
                    stats['employers_from_live'] += 1
                    canonical = result['canonical']
                    relationship = result['relationship']
                    wikidata_id = result.get('wikidata_id')
                    
                    # Update cache
                    employer_cache[name] = result
                else:
                    canonical = normalized
                    relationship = 'self'
                    wikidata_id = None
            else:
                # No cache hit and live queries disabled
                canonical = normalized
                relationship = 'self'
                wikidata_id = None
            
            # Build corporate families
            if canonical not in corporate_families:
                corporate_families[canonical] = {
                    'canonical_name': canonical,
                    'wikidata_id': wikidata_id,
                    'member_employers': [],
                    'linked_whales': [],
                    'total_from_employees': 0,
                    'total_from_whales': 0,
                }
            
            corporate_families[canonical]['member_employers'].append(name)
            corporate_families[canonical]['total_from_employees'] += emp['total']
            
            employer_mappings.append({
                '_key': hashlib.md5(name.encode()).hexdigest()[:16],
                'employer_key': emp['_key'],
                'employer_name': name,
                'canonical_name': canonical,
                'relationship': relationship,
                'wikidata_id': wikidata_id,
                'amount': emp['total'],
            })
        
        context.log.info(f"  Processed {stats['employers_processed']} employers")
        context.log.info(f"  From cache: {stats['employers_from_cache']}, Live: {stats['employers_from_live']}")
        
        # =====================================================================
        # PHASE 2: Resolve retired/self-employed whale donors
        # =====================================================================
        whale_links = []
        
        if config.resolve_whales:
            context.log.info("Phase 2: Resolving retired/self-employed whales...")
            
            # Find whale donors with non-employer values
            non_employer_values = list(NON_EMPLOYERS) + ['RETIRED', 'SELF-EMPLOYED', 'SELF EMPLOYED', 'N/A', 'NONE', '']
            
            whales = list(db.aql.execute("""
                FOR d IN donors
                FILTER d.total_amount >= @min_amount
                FILTER d.donor_type IN ["individual", "likely_individual"]
                FILTER d.canonical_employer IN @non_employers
                    OR d.canonical_employer LIKE "%RETIRED%"
                    OR d.canonical_employer LIKE "%SELF%EMPLOY%"
                    OR d.canonical_employer == null
                SORT d.total_amount DESC
                LIMIT @max_whales
                RETURN {
                    _key: d._key,
                    name: d.canonical_name,
                    employer: d.canonical_employer,
                    total: d.total_amount
                }
            """, bind_vars={
                "min_amount": config.min_whale_amount,
                "non_employers": non_employer_values,
                "max_whales": config.max_whales
            }))
            
            context.log.info(f"  Found {len(whales)} retired/self-employed whales with ${config.min_whale_amount/1e6:.1f}M+ donations")
            
            # Filter to actual person names (LASTNAME, FIRSTNAME pattern)
            person_pattern = re.compile(r'^[A-Z][A-Z\'-]+,\s+[A-Z]')
            
            for whale in whales:
                name = whale['name']
                if not person_pattern.match(name):
                    continue  # Skip organizations misclassified as individuals
                
                stats['whales_processed'] += 1
                
                # Check cache first
                cached = whale_cache.get(name)
                if cached and cached.get('companies'):
                    stats['whales_from_cache'] += 1
                    companies = cached['companies']
                elif config.live_queries:
                    # Make live query
                    from src.rag.wikidata_client import resolve_person_to_companies
                    
                    # Convert "MELLON, TIMOTHY" to "Timothy Mellon"
                    parts = name.split(',', 1)
                    if len(parts) == 2:
                        last = parts[0].strip().title()
                        first = parts[1].strip().split()[0].title()
                        search_name = f"{first} {last}"
                    else:
                        search_name = name.title()
                    
                    result = resolve_person_to_companies(search_name)
                    companies = result.get('companies', [])
                    
                    if companies:
                        stats['whales_from_live'] += 1
                        whale_cache[name] = result
                else:
                    companies = []
                
                # Process companies
                if companies:
                    stats['total_whale_money_attributed'] += whale['total']
                    
                    for company in companies:
                        whale_links.append({
                            '_key': hashlib.md5(f"{name}_{company['name']}".encode()).hexdigest()[:16],
                            'donor_key': whale['_key'],
                            'donor_name': name,
                            'company_name': company['name'],
                            'relationship': company.get('relationship', 'unknown'),
                            'wikidata_id': company.get('wikidata_id'),
                            'amount': whale['total'],
                        })
                        
                        # Add to corporate families
                        canonical = company['name']
                        if canonical not in corporate_families:
                            corporate_families[canonical] = {
                                'canonical_name': canonical,
                                'wikidata_id': company.get('wikidata_id'),
                                'member_employers': [],
                                'linked_whales': [],
                                'total_from_employees': 0,
                                'total_from_whales': 0,
                            }
                        
                        corporate_families[canonical]['linked_whales'].append(name)
                        corporate_families[canonical]['total_from_whales'] += whale['total']
            
            context.log.info(f"  Processed {stats['whales_processed']} whales")
            context.log.info(f"  From cache: {stats['whales_from_cache']}, Live: {stats['whales_from_live']}")
            context.log.info(f"  Total whale money attributed: ${stats['total_whale_money_attributed']/1e6:.1f}M")
        
        # =====================================================================
        # PHASE 3: Write results to ArangoDB
        # =====================================================================
        context.log.info("Phase 3: Writing results to ArangoDB...")
        
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
                'total_from_employees': info['total_from_employees'],
                'total_from_whales': info['total_from_whales'],
                'total_influence': total_influence,
                'created_at': datetime.utcnow().isoformat(),
            }
            family_docs.append(doc)
        
        corp_coll.import_bulk(family_docs, on_duplicate='replace')
        stats['corporate_families_created'] = len(family_docs)
        context.log.info(f"  Created {len(family_docs)} corporate families")
        
        # Write employer mappings
        mapping_coll = db.collection('employer_canonical_mapping')
        mapping_coll.truncate()
        mapping_coll.import_bulk(employer_mappings, on_duplicate='replace')
        context.log.info(f"  Created {len(employer_mappings)} employer mappings")
        
        # Write whale links
        if whale_links:
            whale_coll = db.collection('whale_corporate_links')
            whale_coll.truncate()
            whale_coll.import_bulk(whale_links, on_duplicate='replace')
            context.log.info(f"  Created {len(whale_links)} whale-corporate links")
        
        # Save updated cache
        if config.use_cache:
            cache['employer_resolutions'] = employer_cache
            cache['whale_resolutions'] = whale_cache
            _save_cache(cache)
        
        # Log summary
        context.log.info("=" * 60)
        context.log.info("WIKIDATA RESOLUTION SUMMARY")
        context.log.info("=" * 60)
        context.log.info(f"Employers processed: {stats['employers_processed']}")
        context.log.info(f"  From cache: {stats['employers_from_cache']}")
        context.log.info(f"  From live queries: {stats['employers_from_live']}")
        context.log.info(f"Whales processed: {stats['whales_processed']}")
        context.log.info(f"  From cache: {stats['whales_from_cache']}")
        context.log.info(f"  From live queries: {stats['whales_from_live']}")
        context.log.info(f"Corporate families created: {stats['corporate_families_created']}")
        context.log.info(f"Total whale money attributed: ${stats['total_whale_money_attributed']/1e6:.1f}M")
        
        return Output(
            stats,
            metadata={
                "employers_processed": MetadataValue.int(stats['employers_processed']),
                "employers_from_cache": MetadataValue.int(stats['employers_from_cache']),
                "whales_processed": MetadataValue.int(stats['whales_processed']),
                "whales_from_cache": MetadataValue.int(stats['whales_from_cache']),
                "corporate_families": MetadataValue.int(stats['corporate_families_created']),
                "whale_money_attributed": MetadataValue.float(stats['total_whale_money_attributed']),
            }
        )
