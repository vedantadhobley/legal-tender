"""Integrate employer clusters into the graph.

This asset takes the typo/variation clusters detected by employer_clustering
and merges them into the canonical_employers system.

Flow:
1. Read employer_clusters (67 docs with typo detections)
2. For each cluster, ensure all members point to same canonical
3. Update employer_alias_of edges
4. Recalculate canonical_employer totals

This runs AFTER employer_clusters and BEFORE wikidata_resolution.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any

from dagster import asset, AssetExecutionContext, Config, Output, MetadataValue

from src.resources.arango import ArangoDBResource

logger = logging.getLogger(__name__)


@asset(
    name="employer_cluster_integration",
    deps=["employer_clusters"],
    description="Integrate typo/variation clusters into canonical_employers",
    group_name="enrichment",
    compute_kind="graph",
)
def employer_cluster_integration_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """
    Merge employer clusters into the canonical employer graph.
    
    For each cluster detected by edit-distance/embedding:
    1. Find or create the canonical employer
    2. Point all cluster members to that canonical
    3. Update totals
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        stats = {
            "clusters_processed": 0,
            "aliases_created": 0,
            "aliases_updated": 0,
            "canonical_updated": 0,
            "total_money_unified": 0,
        }
        
        # Check if cluster collection exists
        if not db.has_collection("employer_clusters"):
            context.log.warning("⚠️ No employer_clusters collection - run employer_clusters asset first")
            return Output(
                {"status": "skipped", "reason": "no_clusters"},
                metadata={"status": "skipped"}
            )
        
        clusters = list(db.aql.execute("""
            FOR c IN employer_clusters
            SORT c.total_amount DESC
            RETURN c
        """))
        
        context.log.info(f"📦 Processing {len(clusters)} employer clusters")
        
        for cluster in clusters:
            canonical_name = cluster["canonical_name"]
            canonical_key = cluster["canonical_key"]
            members = cluster["members"]
            
            if len(members) <= 1:
                continue
            
            stats["clusters_processed"] += 1
            stats["total_money_unified"] += cluster["total_amount"]
            
            # Find or create canonical employer
            canonical_emp = list(db.aql.execute("""
                FOR ce IN canonical_employers
                FILTER ce.canonical_name == @name
                LIMIT 1
                RETURN ce
            """, bind_vars={"name": canonical_name}))
            
            if not canonical_emp:
                # Find the employer record
                emp_record = list(db.aql.execute("""
                    FOR e IN employers
                    FILTER e._key == @key
                    RETURN e
                """, bind_vars={"key": canonical_key}))
                
                if emp_record:
                    emp = emp_record[0]
                    # Use existing canonical or create new
                    existing_canonical = list(db.aql.execute("""
                        FOR e IN employer_alias_of
                        FILTER e._from == @emp_id
                        FOR ce IN canonical_employers
                        FILTER ce._id == e._to
                        RETURN ce
                    """, bind_vars={"emp_id": emp["_id"]}))
                    
                    if existing_canonical:
                        canonical_emp = existing_canonical
            
            if canonical_emp:
                canonical_id = canonical_emp[0]["_id"]
                
                # Update all cluster members to point to this canonical
                for member in members:
                    if member["key"] == canonical_key:
                        continue
                    
                    emp_id = f"employers/{member['key']}"
                    
                    # Check if alias edge exists
                    existing = list(db.aql.execute("""
                        FOR e IN employer_alias_of
                        FILTER e._from == @from
                        RETURN e
                    """, bind_vars={"from": emp_id}))
                    
                    if existing:
                        # Update to point to cluster's canonical
                        if existing[0]["_to"] != canonical_id:
                            db.aql.execute("""
                                UPDATE @key WITH {_to: @to, cluster_merged: true}
                                IN employer_alias_of
                            """, bind_vars={
                                "key": existing[0]["_key"],
                                "to": canonical_id
                            })
                            stats["aliases_updated"] += 1
                    else:
                        # Create new alias edge
                        db.collection("employer_alias_of").insert({
                            "_from": emp_id,
                            "_to": canonical_id,
                            "cluster_merged": True,
                            "updated_at": datetime.utcnow().isoformat()
                        })
                        stats["aliases_created"] += 1
                
                stats["canonical_updated"] += 1
        
        # Recalculate canonical employer totals
        context.log.info("📊 Recalculating canonical employer totals...")
        
        db.aql.execute("""
            FOR ce IN canonical_employers
                LET aliases = (
                    FOR e IN employer_alias_of
                    FILTER e._to == ce._id
                    FOR emp IN employers
                    FILTER emp._id == e._from
                    RETURN emp
                )
                LET total = SUM(aliases[*].total_from_employees)
                LET donors = SUM(aliases[*].employee_donor_count)
                UPDATE ce WITH {
                    total_from_employees: total,
                    total_donor_count: donors,
                    alias_count: LENGTH(aliases),
                    cluster_integrated: true,
                    updated_at: @now
                } IN canonical_employers
        """, bind_vars={"now": datetime.utcnow().isoformat()})
        
        context.log.info(f"✅ Cluster integration complete:")
        context.log.info(f"   Clusters processed: {stats['clusters_processed']}")
        context.log.info(f"   Aliases created: {stats['aliases_created']}")
        context.log.info(f"   Aliases updated: {stats['aliases_updated']}")
        context.log.info(f"   Money unified: ${stats['total_money_unified']/1e6:.1f}M")
        
        return Output(
            stats,
            metadata={
                "clusters_processed": MetadataValue.int(stats["clusters_processed"]),
                "aliases_created": MetadataValue.int(stats["aliases_created"]),
                "aliases_updated": MetadataValue.int(stats["aliases_updated"]),
                "total_money_unified": MetadataValue.float(float(stats["total_money_unified"])),
            }
        )
