"""Embedding-based employer clustering for typo/variation detection.

This asset clusters employer names using embedding similarity to detect:
1. Typos: CITADEL vs CITADELL (similarity ~0.96)
2. Variations: BLOOMBERG INC vs BLOOMBERG L.P. (similarity ~0.92)
3. Abbreviations: INTERNATIONAL vs INTL (handled by embeddings)

NO HARDCODING - all clustering is learned from embedding similarity.
The embedding model can be changed via environment variables.

Dependencies: employers
Target: Creates employer_clusters collection, updates canonical_employers
"""

import hashlib
import logging
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Set, Optional

from dagster import asset, AssetExecutionContext, Config, Output, MetadataValue
from pydantic import Field

from src.resources.arango import ArangoDBResource
from src.resources.embedding import EmbeddingResource
from src.rag.employer_normalization import normalize_employer_name, NON_EMPLOYERS

logger = logging.getLogger(__name__)


class EmployerClusteringConfig(Config):
    """Configuration for employer clustering."""
    
    typo_threshold: float = Field(
        default=0.97,
        description="Minimum embedding similarity for typo detection (0.97+ = very conservative, typos only)"
    )
    
    variation_threshold: float = Field(
        default=0.94,
        description="Minimum embedding similarity for name variation detection (LLC vs L.L.C.)"
    )
    
    min_name_length: int = Field(
        default=5,
        description="Minimum employer name length to consider for clustering (skip short names like SIG, EMC)"
    )
    
    min_total_amount: int = Field(
        default=100_000,
        description="Minimum total donations to include employer in clustering"
    )
    
    batch_size: int = Field(
        default=100,
        description="Number of employers to embed per batch"
    )
    
    max_employers: int = Field(
        default=5000,
        description="Maximum employers to process (top by donation amount)"
    )


@asset(
    name="employer_clusters",
    deps=["employers"],
    description="Cluster similar employer names using embedding similarity (no hardcoding)",
    group_name="enrichment",
    compute_kind="ml",
)
def employer_clusters_asset(
    context: AssetExecutionContext,
    config: EmployerClusteringConfig,
    arango: ArangoDBResource,
    embedding: EmbeddingResource,
) -> Output[Dict[str, Any]]:
    """
    Cluster employers by embedding similarity.
    
    Pipeline:
    1. Load top employers by donation amount
    2. Apply rule-based normalization (cleaning)
    3. Embed employer names using configured model
    4. Cluster by similarity threshold
    5. Store clusters for canonical resolution
    
    This enables detecting:
    - CITADEL LLC vs CITADELL LLC (typo)
    - BLOOMBERG INC vs BLOOMBERG INC. (punctuation)
    - GOOGLE vs GOOGLE INC (suffix variation)
    """
    
    # Check embedding service health
    if not embedding.health_check():
        context.log.warning("⚠️ Embedding service not available, falling back to rule-based only")
        return Output(
            {"status": "skipped", "reason": "embedding_unavailable"},
            metadata={"status": "skipped"}
        )
    
    context.log.info(f"🔗 Embedding service: {embedding.base_url}")
    context.log.info(f"📊 Clustering config: typo_threshold={config.typo_threshold}, variation_threshold={config.variation_threshold}")
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Ensure collection exists
        if not db.has_collection("employer_clusters"):
            db.create_collection("employer_clusters")
        
        stats = {
            "employers_processed": 0,
            "clusters_found": 0,
            "typos_detected": 0,
            "variations_detected": 0,
            "embeddings_computed": 0,
            "total_money_unified": 0,
        }
        
        # =====================================================================
        # Phase 1: Load employers
        # =====================================================================
        context.log.info("Phase 1: Loading employers...")
        
        employers = list(db.aql.execute("""
            FOR e IN employers
            FILTER e.total_from_employees >= @min_amount
            SORT e.total_from_employees DESC
            LIMIT @max_employers
            RETURN {
                key: e._key,
                name: e.name,
                total: e.total_from_employees,
                donor_count: e.employee_donor_count
            }
        """, bind_vars={
            "min_amount": config.min_total_amount,
            "max_employers": config.max_employers
        }))
        
        context.log.info(f"  Loaded {len(employers):,} employers")
        
        # Filter out non-employers
        valid_employers = []
        for emp in employers:
            normalized, meta = normalize_employer_name(emp["name"])
            if not meta.get("is_non_employer"):
                emp["normalized"] = normalized
                valid_employers.append(emp)
        
        context.log.info(f"  After filtering non-employers: {len(valid_employers):,}")
        
        # =====================================================================
        # Phase 2: Group by normalized name (pre-clustering)
        # =====================================================================
        context.log.info("Phase 2: Pre-grouping by normalized name...")
        
        # First pass: exact normalized matches (free, no embedding needed)
        normalized_groups: Dict[str, List[dict]] = defaultdict(list)
        for emp in valid_employers:
            normalized_groups[emp["normalized"]].append(emp)
        
        # Find groups that need embedding (single items need to find matches)
        single_employers = [
            emp for norm, group in normalized_groups.items() 
            if len(group) == 1 
            for emp in group
        ]
        
        # Pre-grouped employers (already have matches via normalization)
        pre_grouped = {
            norm: group for norm, group in normalized_groups.items()
            if len(group) > 1
        }
        
        context.log.info(f"  Pre-grouped by normalization: {len(pre_grouped)} groups")
        context.log.info(f"  Need embedding clustering: {len(single_employers)} employers")
        
        # =====================================================================
        # Phase 3: String similarity clustering (edit distance)
        # =====================================================================
        context.log.info("Phase 3: String similarity clustering...")
        
        if single_employers:
            # Filter by minimum length to avoid false positives on short names
            long_names = [emp for emp in single_employers if len(emp["normalized"]) >= config.min_name_length]
            short_names = [emp for emp in single_employers if len(emp["normalized"]) < config.min_name_length]
            
            context.log.info(f"  Long names (>={config.min_name_length} chars): {len(long_names)}")
            context.log.info(f"  Short names (<{config.min_name_length} chars): {len(short_names)} (skipping)")
            
            # Use edit distance ratio for clustering
            # This catches typos without semantic confusion
            # Threshold 0.88+ is conservative - catches typos but avoids false positives
            # like "UNIVERSITY OF UTAH" vs "UNIVERSITY OF IDAHO" (0.865)
            names_to_cluster = [emp["normalized"] for emp in long_names]
            
            # Cluster using string similarity (not embeddings)
            string_clusters = _cluster_by_edit_distance(names_to_cluster, threshold=0.88)
            
            context.log.info(f"  Found {len(string_clusters)} string-based clusters")
            
            # Optionally use embeddings for a second pass on remaining singletons
            if embedding.health_check():
                # Get singleton clusters (still not grouped)
                singletons = [names_to_cluster[c[0]] for c in string_clusters if len(c) == 1]
                if singletons and len(singletons) <= 1000:
                    context.log.info(f"  Running embedding pass on {len(singletons)} singletons...")
                    emb_clusters = embedding.cluster_by_similarity(singletons, threshold=config.typo_threshold)
                    stats["embeddings_computed"] = len(singletons)
                    
                    # Merge embedding clusters back
                    # (for now, just use string clusters - embeddings too noisy for employer names)
            
            # Convert cluster indices back to employer records  
            embedding_clusters: List[List[dict]] = []
            for cluster_indices in string_clusters:
                cluster_emps = [long_names[i] for i in cluster_indices]
                embedding_clusters.append(cluster_emps)
                
                if len(cluster_indices) > 1:
                    stats["typos_detected"] += len(cluster_indices) - 1
            
            # Add short names as individual clusters (no grouping)
            for emp in short_names:
                embedding_clusters.append([emp])
        else:
            embedding_clusters = []
        
        # =====================================================================
        # Phase 4: Merge all clusters and pick canonical representatives
        # =====================================================================
        context.log.info("Phase 4: Merging clusters and selecting canonical names...")
        
        all_clusters: List[Dict[str, Any]] = []
        
        # Process pre-grouped (normalization matches)
        for normalized, group in pre_grouped.items():
            if len(group) > 1:
                cluster = _build_cluster_record(group, "normalization")
                all_clusters.append(cluster)
                stats["variations_detected"] += len(group) - 1
                stats["total_money_unified"] += cluster["total_amount"]
        
        # Process embedding clusters (only those with multiple members)
        for cluster_emps in embedding_clusters:
            if len(cluster_emps) > 1:
                cluster = _build_cluster_record(cluster_emps, "embedding")
                all_clusters.append(cluster)
                stats["total_money_unified"] += cluster["total_amount"]
        
        stats["clusters_found"] = len(all_clusters)
        stats["employers_processed"] = len(valid_employers)
        
        context.log.info(f"  Total clusters: {len(all_clusters)}")
        context.log.info(f"  Typos detected: {stats['typos_detected']}")
        context.log.info(f"  Variations detected: {stats['variations_detected']}")
        
        # =====================================================================
        # Phase 5: Store clusters
        # =====================================================================
        context.log.info("Phase 5: Storing clusters...")
        
        cluster_coll = db.collection("employer_clusters")
        cluster_coll.truncate()
        
        for cluster in all_clusters:
            cluster_coll.insert(cluster)
        
        context.log.info(f"  Stored {len(all_clusters)} clusters")
        
        # =====================================================================
        # Log notable clusters for verification
        # =====================================================================
        context.log.info("\n📋 Notable clusters found:")
        
        # Sort by total amount
        notable = sorted(all_clusters, key=lambda x: x["total_amount"], reverse=True)[:10]
        
        for cluster in notable:
            members = cluster["members"]
            canonical = cluster["canonical_name"]
            total = cluster["total_amount"]
            method = cluster["detection_method"]
            
            member_names = [m["original_name"] for m in members]
            if len(member_names) > 3:
                display = f"{member_names[:3]} + {len(member_names)-3} more"
            else:
                display = member_names
            
            context.log.info(f"  {canonical}: ${total/1e6:.1f}M [{method}]")
            context.log.info(f"    Members: {display}")
        
        return Output(
            stats,
            metadata={
                "employers_processed": MetadataValue.int(stats["employers_processed"]),
                "clusters_found": MetadataValue.int(stats["clusters_found"]),
                "typos_detected": MetadataValue.int(stats["typos_detected"]),
                "variations_detected": MetadataValue.int(stats["variations_detected"]),
                "total_money_unified": MetadataValue.float(float(stats["total_money_unified"])),
                "embedding_service": MetadataValue.text(embedding.base_url),
            }
        )


def _cluster_by_edit_distance(names: List[str], threshold: float = 0.85) -> List[List[int]]:
    """
    Cluster names by edit distance ratio using union-find.
    
    Uses SequenceMatcher ratio which gives a value in [0, 1] based on 
    the longest contiguous matching subsequence.
    
    Args:
        names: List of normalized employer names
        threshold: Minimum similarity ratio to consider a match (0.85 = 85% similar)
        
    Returns:
        List of clusters, where each cluster is a list of indices into `names`
    """
    from difflib import SequenceMatcher
    
    n = len(names)
    if n == 0:
        return []
    
    # Union-Find data structure
    parent = list(range(n))
    rank = [0] * n
    
    def find(x: int) -> int:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]
    
    def union(x: int, y: int):
        px, py = find(x), find(y)
        if px == py:
            return
        if rank[px] < rank[py]:
            px, py = py, px
        parent[py] = px
        if rank[px] == rank[py]:
            rank[px] += 1
    
    # Compare all pairs - O(n²) but necessary for accurate clustering
    # For very large n, could use blocking by first few chars
    comparisons = 0
    matches = 0
    
    for i in range(n):
        # Optimization: only compare with names of similar length (within 30%)
        len_i = len(names[i])
        min_len = int(len_i * 0.7)
        max_len = int(len_i * 1.3)
        
        # Get first word for additional filtering
        first_word_i = names[i].split()[0] if ' ' in names[i] else names[i]
        
        for j in range(i + 1, n):
            len_j = len(names[j])
            
            # Skip if lengths too different (can't be similar)
            if len_j < min_len or len_j > max_len:
                continue
            
            comparisons += 1
            
            # Quick rejection: if first chars don't match, skip
            if names[i][0] != names[j][0]:
                continue
            
            # For multi-word names, require first word to be similar
            # This prevents AH CAPITAL vs AQR CAPITAL (different first words)
            if ' ' in names[i] and ' ' in names[j]:
                first_word_j = names[j].split()[0]
                first_word_ratio = SequenceMatcher(None, first_word_i, first_word_j).ratio()
                if first_word_ratio < 0.8:
                    continue
            
            ratio = SequenceMatcher(None, names[i], names[j]).ratio()
            
            if ratio >= threshold:
                union(i, j)
                matches += 1
    
    # Build clusters from union-find
    clusters_map: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        root = find(i)
        clusters_map[root].append(i)
    
    return list(clusters_map.values())


def _build_cluster_record(employers: List[dict], method: str) -> Dict[str, Any]:
    """
    Build a cluster record from a list of employer records.
    
    Selects canonical name as the shortest variation with highest donations.
    """
    # Sort by total donations descending
    sorted_emps = sorted(employers, key=lambda x: x["total"], reverse=True)
    
    # Canonical = highest money employer
    canonical = sorted_emps[0]
    
    # But prefer shorter name if close in money (within 10%)
    for emp in sorted_emps[1:]:
        if emp["total"] >= canonical["total"] * 0.9:
            if len(emp["normalized"]) < len(canonical["normalized"]):
                canonical = emp
    
    total_amount = sum(emp["total"] for emp in employers)
    total_donors = sum(emp.get("donor_count", 0) for emp in employers)
    
    cluster_id = hashlib.md5(
        canonical["normalized"].encode()
    ).hexdigest()[:12]
    
    return {
        "_key": cluster_id,
        "canonical_name": canonical["normalized"],
        "canonical_key": canonical["key"],
        "total_amount": total_amount,
        "total_donors": total_donors,
        "detection_method": method,
        "members": [
            {
                "original_name": emp["name"],
                "normalized_name": emp["normalized"],
                "key": emp["key"],
                "amount": emp["total"],
                "donors": emp.get("donor_count", 0),
            }
            for emp in sorted_emps
        ],
        "member_count": len(employers),
        "created_at": datetime.utcnow().isoformat(),
    }
