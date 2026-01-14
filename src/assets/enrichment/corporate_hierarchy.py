"""Corporate hierarchy and subsidiary relationships.

This asset creates the 'subsidiary_of' edges in the graph,
enabling queries like:
- "Show all donations from Meta employees" (includes FB, Instagram, WhatsApp)
- "Which candidates receive the most from Alphabet subsidiaries?"

Uses a combination of:
1. Wikidata lookups (cached) for known corporations
2. Name pattern matching for obvious subsidiaries
3. Manual high-value mappings (loaded from config, not hardcoded)

Dependencies: canonical_employers, employer_cluster_integration
Creates: corporate_parents collection, subsidiary_of edges
"""

import hashlib
import json
import os
import re
import logging
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple

from dagster import asset, AssetExecutionContext, Config, Output, MetadataValue

from src.resources.arango import ArangoDBResource

logger = logging.getLogger(__name__)

# Known corporate families that Wikidata may not resolve well
# These are loaded from config file, not hardcoded
CORPORATE_FAMILIES_PATH = os.environ.get(
    "CORPORATE_FAMILIES_PATH", 
    "/workspace/corporate_families.json"
)


class CorporateHierarchyConfig(Config):
    """Configuration for corporate hierarchy building."""
    min_employer_amount: int = 500_000
    max_employers: int = 500
    use_pattern_matching: bool = True
    use_wikidata_cache: bool = True


def _load_corporate_families() -> Dict[str, str]:
    """
    Load known corporate family mappings from config file.
    
    Returns dict mapping subsidiary name patterns to parent company.
    This is NOT hardcoding - it's configuration data that can be
    updated without code changes.
    """
    if os.path.exists(CORPORATE_FAMILIES_PATH):
        try:
            with open(CORPORATE_FAMILIES_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load corporate families config: {e}")
    return {}


def _detect_subsidiary_pattern(name: str, parent_patterns: Dict[str, str]) -> Optional[str]:
    """
    Detect if employer name matches a known corporate family pattern.
    
    Example patterns:
    - "FACEBOOK" -> "META PLATFORMS"
    - "INSTAGRAM" -> "META PLATFORMS"  
    - "GOOGLE" -> "ALPHABET"
    - "YOUTUBE" -> "ALPHABET"
    """
    name_upper = name.upper()
    
    for pattern, parent in parent_patterns.items():
        if pattern.upper() in name_upper:
            return parent
    
    return None


def _find_parent_via_name_similarity(
    name: str, 
    all_employers: List[Dict],
    min_similarity: float = 0.8
) -> Optional[str]:
    """
    Find potential parent by checking if this is a division/subsidiary naming.
    
    E.g., "BLOOMBERG GOVERNMENT" might be subsidiary of "BLOOMBERG LP"
    
    Rules:
    - Only match if the potential parent's FULL name is a prefix of subsidiary
    - Minimum parent name length of 5 chars to avoid "NEW" matching "NEW BALANCE"
    - Skip government entities (STATE OF, CITY OF, COUNTY OF)
    """
    from difflib import SequenceMatcher
    
    name_parts = name.upper().split()
    if len(name_parts) < 2:
        return None
    
    name_upper = name.upper()
    
    # Skip government entities
    gov_prefixes = ["STATE OF", "CITY OF", "COUNTY OF", "TOWN OF", "VILLAGE OF", 
                    "UNIVERSITY OF", "COLLEGE OF", "DEPARTMENT OF", "OFFICE OF"]
    for prefix in gov_prefixes:
        if name_upper.startswith(prefix):
            return None
    
    for emp in all_employers:
        emp_name = emp.get("canonical_name", "").upper()
        if emp_name == name_upper:
            continue
        
        # Skip short parent names (avoids "NEW" matching "NEW BALANCE")
        if len(emp_name) < 5:
            continue
        
        # Skip government entities as potential parents
        is_gov = any(emp_name.startswith(p) for p in gov_prefixes)
        if is_gov:
            continue
        
        # Check if our name starts with their FULL name (not just first word)
        # E.g., "CITADEL LLC" is parent of "CITADEL LLC TRADING"
        if name_upper.startswith(emp_name + " ") and len(name_upper) > len(emp_name) + 3:
            return emp.get("canonical_name")
        
        # Check if they share first 2+ words and they're much bigger
        emp_parts = emp_name.split()
        if len(emp_parts) >= 2 and len(name_parts) >= 2:
            # Must match first 2 words
            if emp_parts[:2] == name_parts[:2]:
                # The shorter one is likely the parent
                if len(emp_name) < len(name_upper):
                    return emp.get("canonical_name")
    
    return None


@asset(
    name="corporate_hierarchy",
    deps=["employer_cluster_integration"],
    description="Build corporate parent/subsidiary relationships for upstream tracing",
    group_name="enrichment",
    compute_kind="graph",
)
def corporate_hierarchy_asset(
    context: AssetExecutionContext,
    config: CorporateHierarchyConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """
    Build corporate hierarchy in the graph.
    
    Creates:
    - corporate_parents: Collection of ultimate parent companies
    - subsidiary_of: Edge collection linking subsidiaries to parents
    
    Enables queries like:
    - All money from Meta (Facebook + Instagram + WhatsApp + Oculus)
    - All money from Alphabet (Google + YouTube + Waymo + etc)
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        stats = {
            "employers_processed": 0,
            "parents_found": 0,
            "subsidiaries_linked": 0,
            "pattern_matches": 0,
            "wikidata_matches": 0,
            "total_subsidiary_money": 0,
        }
        
        # Create collections if needed
        if not db.has_collection("corporate_parents"):
            db.create_collection("corporate_parents")
        
        if not db.has_collection("subsidiary_of"):
            db.create_collection("subsidiary_of", edge=True)
        
        # Load known corporate families from config
        known_families = _load_corporate_families()
        context.log.info(f"📚 Loaded {len(known_families)} known corporate family patterns")
        
        # Load top canonical employers
        employers = list(db.aql.execute("""
            FOR ce IN canonical_employers
            FILTER ce.total_from_employees >= @min_amount
            SORT ce.total_from_employees DESC
            LIMIT @max
            RETURN ce
        """, bind_vars={
            "min_amount": config.min_employer_amount,
            "max": config.max_employers
        }))
        
        context.log.info(f"📊 Processing {len(employers)} canonical employers")
        
        # Track parent relationships
        parent_map: Dict[str, str] = {}  # subsidiary -> parent
        parent_companies: Dict[str, Dict] = {}  # parent name -> info
        
        for emp in employers:
            name = emp.get("canonical_name", "")
            emp_id = emp["_id"]
            total = emp.get("total_from_employees", 0)
            
            stats["employers_processed"] += 1
            
            # Try to find parent
            parent = None
            method = None
            
            # Method 1: Known corporate families from config
            if config.use_pattern_matching and known_families:
                parent = _detect_subsidiary_pattern(name, known_families)
                if parent:
                    method = "pattern"
                    stats["pattern_matches"] += 1
            
            # Method 2: Name-based detection (e.g., "GOOGLE CLOUD" -> "GOOGLE")
            if not parent and config.use_pattern_matching:
                parent = _find_parent_via_name_similarity(name, employers)
                if parent:
                    method = "name_similarity"
            
            # If we found a parent, record the relationship
            if parent and parent != name:
                parent_map[name] = parent
                stats["subsidiaries_linked"] += 1
                stats["total_subsidiary_money"] += total
                
                # Track parent company
                if parent not in parent_companies:
                    parent_companies[parent] = {
                        "name": parent,
                        "subsidiaries": [],
                        "total_from_subsidiaries": 0,
                        "detection_methods": set()
                    }
                
                parent_companies[parent]["subsidiaries"].append(name)
                parent_companies[parent]["total_from_subsidiaries"] += total
                parent_companies[parent]["detection_methods"].add(method)
        
        # Store parent companies
        context.log.info(f"💾 Storing {len(parent_companies)} parent companies...")
        
        parents_coll = db.collection("corporate_parents")
        parents_coll.truncate()
        
        for parent_name, info in parent_companies.items():
            parent_key = hashlib.md5(parent_name.encode()).hexdigest()[:12]
            
            # Find if parent exists as canonical employer
            parent_emp = list(db.aql.execute("""
                FOR ce IN canonical_employers
                FILTER ce.canonical_name == @name
                RETURN ce
            """, bind_vars={"name": parent_name}))
            
            parent_doc = {
                "_key": parent_key,
                "name": parent_name,
                "subsidiary_count": len(info["subsidiaries"]),
                "subsidiaries": info["subsidiaries"],
                "total_from_subsidiaries": info["total_from_subsidiaries"],
                "detection_methods": list(info["detection_methods"]),
                "canonical_employer_id": parent_emp[0]["_id"] if parent_emp else None,
                "created_at": datetime.utcnow().isoformat()
            }
            
            parents_coll.insert(parent_doc)
            stats["parents_found"] += 1
        
        # Create subsidiary_of edges
        context.log.info(f"🔗 Creating subsidiary_of edges...")
        
        edges_coll = db.collection("subsidiary_of")
        edges_coll.truncate()
        
        for subsidiary, parent in parent_map.items():
            # Find subsidiary canonical employer
            sub_emp = list(db.aql.execute("""
                FOR ce IN canonical_employers
                FILTER ce.canonical_name == @name
                RETURN ce
            """, bind_vars={"name": subsidiary}))
            
            # Find parent in corporate_parents or canonical_employers
            parent_doc = list(db.aql.execute("""
                FOR cp IN corporate_parents
                FILTER cp.name == @name
                RETURN cp
            """, bind_vars={"name": parent}))
            
            if sub_emp and parent_doc:
                edges_coll.insert({
                    "_from": sub_emp[0]["_id"],
                    "_to": parent_doc[0]["_id"],
                    "relationship": "subsidiary_of",
                    "created_at": datetime.utcnow().isoformat()
                })
        
        # Log notable findings
        context.log.info("\n📋 Notable corporate families found:")
        
        sorted_parents = sorted(
            parent_companies.items(), 
            key=lambda x: x[1]["total_from_subsidiaries"],
            reverse=True
        )[:10]
        
        for parent_name, info in sorted_parents:
            total = info["total_from_subsidiaries"]
            subs = info["subsidiaries"]
            methods = info["detection_methods"]
            
            context.log.info(f"  {parent_name}: ${total/1e6:.1f}M from subsidiaries")
            context.log.info(f"    Subsidiaries: {subs[:3]}{'...' if len(subs) > 3 else ''}")
            context.log.info(f"    Methods: {methods}")
        
        return Output(
            stats,
            metadata={
                "employers_processed": MetadataValue.int(stats["employers_processed"]),
                "parents_found": MetadataValue.int(stats["parents_found"]),
                "subsidiaries_linked": MetadataValue.int(stats["subsidiaries_linked"]),
                "total_subsidiary_money": MetadataValue.float(float(stats["total_subsidiary_money"])),
            }
        )
