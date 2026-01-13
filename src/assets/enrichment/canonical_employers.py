"""Canonical Employers Asset - Normalized employer entities with alias resolution.

This asset creates a `canonical_employers` collection that:
1. Normalizes employer name variations using rule-based preprocessing
2. Groups similar names using token-based clustering
3. Creates canonical entities with all known aliases
4. Tracks parent/subsidiary relationships

The result enables accurate corporate influence analysis by aggregating
all donations from the same actual company, even when recorded differently.

Example:
  Before: "GOOGLE" ($29M), "ALPHABET" ($340K), "GOOGLE INC" ($552K) = 3 separate entities
  After:  "ALPHABET INC" ($30M total) with aliases = 1 entity

Dependencies: employers (from graph layer)
Target: Creates canonical_employers collection in aggregation database
"""

from typing import Dict, Any, List, Set
from datetime import datetime
from collections import defaultdict
import hashlib

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource
from src.rag.employer_normalization import (
    normalize_employer_name,
    get_canonical_mapping,
    NON_EMPLOYERS,
)


class CanonicalEmployersConfig(Config):
    """Configuration for canonical employers asset."""
    min_total_amount: float = 10000.0  # Minimum total donations to include
    similarity_threshold: float = 0.7  # Jaccard similarity for clustering
    batch_size: int = 100


@asset(
    name="canonical_employers",
    description="Normalized employer entities with alias resolution for accurate corporate influence tracking.",
    group_name="enrichment",
    compute_kind="rag",
    deps=["employers"],
)
def canonical_employers_asset(
    context: AssetExecutionContext,
    config: CanonicalEmployersConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """
    Create canonical employer entities by normalizing and grouping variations.
    
    Pipeline:
    1. Load all employers from graph layer
    2. Apply rule-based normalization (Tier 1)
    3. Apply canonical mappings for known companies
    4. Cluster remaining by token similarity (Tier 2)
    5. Create canonical_employers collection with aliases
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("📊 Creating canonical employers from normalized variations...")
        
        # ============================================================
        # Phase 1: Load all employers
        # ============================================================
        context.log.info("  Phase 1: Loading employers...")
        
        employers = list(db.aql.execute("""
            FOR e IN employers
            FILTER e.total_from_employees >= @min_amount
            RETURN {
                _key: e._key,
                name: e.name,
                total: e.total_from_employees,
                donor_count: e.employee_donor_count,
                transactions: e.total_transactions
            }
        """, bind_vars={"min_amount": config.min_total_amount}))
        
        context.log.info(f"    Loaded {len(employers):,} employers with ${config.min_total_amount:,.0f}+ in donations")
        
        # ============================================================
        # Phase 2: Normalize and group employers
        # ============================================================
        context.log.info("  Phase 2: Normalizing employer names...")
        
        # Track canonical groupings
        # canonical_key -> list of employer records
        canonical_groups: Dict[str, List[dict]] = defaultdict(list)
        non_employers_found = []
        
        for emp in employers:
            name = emp['name']
            normalized, meta = normalize_employer_name(name)
            
            # Skip non-employers (retired, self-employed, etc.)
            if meta.get('is_non_employer'):
                non_employers_found.append({
                    'name': name,
                    'category': meta.get('category'),
                    'total': emp['total']
                })
                continue
            
            # Check for pre-defined canonical mapping
            canonical_info = get_canonical_mapping(name)
            
            if canonical_info:
                canonical_key = canonical_info['canonical']
            else:
                # Use normalized name as canonical key
                canonical_key = normalized
            
            emp['normalized'] = normalized
            emp['canonical_key'] = canonical_key
            emp['canonical_info'] = canonical_info
            
            canonical_groups[canonical_key].append(emp)
        
        context.log.info(f"    Found {len(canonical_groups):,} canonical groups")
        context.log.info(f"    Found {len(non_employers_found):,} non-employer entries")
        
        # ============================================================
        # Phase 3: Cluster similar normalized names (Tier 2)
        # ============================================================
        context.log.info("  Phase 3: Clustering similar names...")
        
        # For groups with only normalized names (no canonical mapping),
        # try to merge based on token similarity
        merge_candidates = _find_merge_candidates(canonical_groups, config.similarity_threshold)
        
        context.log.info(f"    Found {len(merge_candidates)} potential merges")
        
        # Apply merges
        for target, source in merge_candidates:
            if source in canonical_groups and target in canonical_groups:
                canonical_groups[target].extend(canonical_groups[source])
                del canonical_groups[source]
        
        context.log.info(f"    After merging: {len(canonical_groups):,} canonical groups")
        
        # ============================================================
        # Phase 4: Create canonical_employers collection
        # ============================================================
        context.log.info("  Phase 4: Creating canonical_employers collection...")
        
        # Ensure collection exists
        if not db.has_collection("canonical_employers"):
            db.create_collection("canonical_employers")
        else:
            db.collection("canonical_employers").truncate()
        
        # Ensure edge collection for aliases
        if not db.has_collection("employer_alias_of"):
            db.create_collection("employer_alias_of", edge=True)
        else:
            db.collection("employer_alias_of").truncate()
        
        canonical_docs = []
        alias_edges = []
        
        stats = {
            'canonical_employers': 0,
            'total_aliases': 0,
            'total_amount': 0,
            'with_predefined_mapping': 0,
        }
        
        for canonical_key, members in canonical_groups.items():
            # Aggregate totals
            total_amount = sum(m['total'] for m in members)
            total_donors = sum(m['donor_count'] for m in members)
            total_transactions = sum(m['transactions'] for m in members)
            
            # Collect aliases
            aliases = list(set(m['name'] for m in members))
            original_keys = [m['_key'] for m in members]
            
            # Check if any member had canonical info
            canonical_info = next((m.get('canonical_info') for m in members if m.get('canonical_info')), None)
            
            # Create canonical document
            canonical_key_safe = hashlib.md5(canonical_key.encode()).hexdigest()[:16]
            
            doc = {
                '_key': canonical_key_safe,
                'canonical_name': canonical_key,
                'display_name': canonical_key,  # Can be overridden
                'aliases': sorted(aliases),
                'original_employer_keys': original_keys,
                'total_from_employees': total_amount,
                'employee_donor_count': total_donors,
                'total_transactions': total_transactions,
                'alias_count': len(aliases),
                'resolution_method': 'predefined' if canonical_info else 'normalized',
                'created_at': datetime.now().isoformat(),
            }
            
            # Add subsidiary info if present
            if canonical_info:
                doc['is_predefined_mapping'] = True
                if 'subsidiaries' in canonical_info:
                    doc['known_subsidiaries'] = canonical_info['subsidiaries']
                stats['with_predefined_mapping'] += 1
            
            canonical_docs.append(doc)
            
            # Create alias edges (original employer → canonical)
            for orig_key in original_keys:
                alias_edges.append({
                    '_from': f'employers/{orig_key}',
                    '_to': f'canonical_employers/{canonical_key_safe}',
                    'alias_name': next(m['name'] for m in members if m['_key'] == orig_key),
                })
            
            stats['canonical_employers'] += 1
            stats['total_aliases'] += len(aliases)
            stats['total_amount'] += total_amount
        
        # Batch insert
        context.log.info(f"    Inserting {len(canonical_docs):,} canonical employers...")
        
        for i in range(0, len(canonical_docs), config.batch_size):
            batch = canonical_docs[i:i + config.batch_size]
            db.collection("canonical_employers").insert_many(batch)
        
        context.log.info(f"    Inserting {len(alias_edges):,} alias edges...")
        
        for i in range(0, len(alias_edges), config.batch_size):
            batch = alias_edges[i:i + config.batch_size]
            db.collection("employer_alias_of").insert_many(batch)
        
        # Create indexes
        try:
            db.collection("canonical_employers").add_persistent_index(
                fields=["total_from_employees"],
                sparse=False
            )
            db.collection("canonical_employers").add_persistent_index(
                fields=["canonical_name"],
                sparse=False
            )
        except Exception:
            pass
        
        # ============================================================
        # Phase 5: Store non-employer summary
        # ============================================================
        # Store non-employer stats for reference
        non_employer_summary = defaultdict(lambda: {'count': 0, 'total': 0})
        for ne in non_employers_found:
            cat = ne['category']
            non_employer_summary[cat]['count'] += 1
            non_employer_summary[cat]['total'] += ne['total']
        
        context.log.info(f"\n✅ Created canonical employers:")
        context.log.info(f"   Canonical entities: {stats['canonical_employers']:,}")
        context.log.info(f"   Total aliases merged: {stats['total_aliases']:,}")
        context.log.info(f"   With predefined mappings: {stats['with_predefined_mapping']:,}")
        context.log.info(f"   Total amount tracked: ${stats['total_amount']:,.0f}")
        
        context.log.info(f"\n   Non-employer breakdown:")
        for cat, data in sorted(non_employer_summary.items(), key=lambda x: -x[1]['total']):
            context.log.info(f"     {cat}: {data['count']:,} entries, ${data['total']:,.0f}")
        
        return Output(
            value=stats,
            metadata={
                "canonical_employers": MetadataValue.int(stats['canonical_employers']),
                "total_aliases": MetadataValue.int(stats['total_aliases']),
                "with_predefined_mappings": MetadataValue.int(stats['with_predefined_mapping']),
                "total_amount_tracked": MetadataValue.float(float(stats['total_amount'])),
                "non_employers_found": MetadataValue.int(len(non_employers_found)),
            }
        )


def _find_merge_candidates(
    groups: Dict[str, List[dict]], 
    threshold: float
) -> List[tuple]:
    """
    Find groups that should be merged based on token similarity.
    
    Uses Jaccard similarity on word tokens.
    Returns list of (target, source) tuples where source should merge into target.
    """
    merges = []
    keys = list(groups.keys())
    
    for i, key1 in enumerate(keys):
        tokens1 = set(key1.split())
        if len(tokens1) < 2:
            continue  # Skip single-word names
            
        total1 = sum(m['total'] for m in groups[key1])
        
        for key2 in keys[i+1:]:
            tokens2 = set(key2.split())
            if len(tokens2) < 2:
                continue
                
            # Jaccard similarity
            intersection = tokens1 & tokens2
            union = tokens1 | tokens2
            similarity = len(intersection) / len(union) if union else 0
            
            if similarity >= threshold:
                # Merge smaller into larger
                total2 = sum(m['total'] for m in groups[key2])
                if total1 >= total2:
                    merges.append((key1, key2))
                else:
                    merges.append((key2, key1))
    
    return merges
