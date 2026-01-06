"""Affiliated With Edge Collection - Committee-candidate linkages.

Creates edges between committees and the candidates they support.
This is from the CCL (candidate-committee linkage) file.

Source: fec_{cycle}.ccl
Target: aggregation.affiliated_with (edge collection)
"""

from typing import Dict, Any, List
from datetime import datetime
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class AffiliatedWithConfig(Config):
    """Configuration for affiliated_with edge asset."""
    cycles: List[str] = ["2020", "2022", "2024"]
    batch_size: int = 5000


@asset(
    name="affiliated_with",
    description="Edges from committees to candidates - official FEC linkages.",
    group_name="graph",
    compute_kind="graph_edge",
    deps=["ccl", "contributed_to"],
)
def affiliated_with_asset(
    context: AssetExecutionContext,
    config: AffiliatedWithConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create affiliated_with edge collection from CCL data.
    
    CCL contains official FEC linkages between committees and candidates.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Create/truncate edge collection
        if agg_db.has_collection("affiliated_with"):
            agg_db.collection("affiliated_with").truncate()
            context.log.info("Truncated affiliated_with")
        else:
            agg_db.create_collection("affiliated_with", edge=True)
            context.log.info("Created affiliated_with edge collection")
        
        edges_coll = agg_db.collection("affiliated_with")
        
        # Get valid committees and candidates
        valid_committees = set()
        cursor = agg_db.aql.execute("FOR c IN committees RETURN c._key", ttl=3600)
        for key in cursor:
            valid_committees.add(key)
        
        valid_candidates = set()
        cursor = agg_db.aql.execute("FOR c IN candidates RETURN c._key", ttl=3600)
        for key in cursor:
            valid_candidates.add(key)
        
        context.log.info(f"📋 {len(valid_committees):,} committees, {len(valid_candidates):,} candidates")
        
        stats = {'edges_created': 0, 'by_cycle': {}}
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            
            if not cycle_db.has_collection("ccl"):
                context.log.warning(f"{db_name}.ccl missing, skipping")
                continue
            
            context.log.info(f"📊 Processing {cycle} CCL...")
            
            cursor = cycle_db.aql.execute(
                "FOR doc IN ccl RETURN doc",
                ttl=3600, batch_size=config.batch_size, stream=True
            )
            
            cycle_edges = 0
            batch = []
            
            for record in cursor:
                cmte_id = record.get('CMTE_ID')
                cand_id = record.get('CAND_ID')
                
                if not cmte_id or not cand_id:
                    continue
                
                if cmte_id not in valid_committees or cand_id not in valid_candidates:
                    continue
                
                edge = {
                    '_key': f"{cmte_id}_{cand_id}_{cycle}",
                    '_from': f"committees/{cmte_id}",
                    '_to': f"candidates/{cand_id}",
                    'linkage_id': record.get('LINKAGE_ID'),
                    'cmte_designation': record.get('CMTE_DSGN'),
                    'cmte_type': record.get('CMTE_TP'),
                    'cycle': cycle,
                    'updated_at': datetime.now().isoformat()
                }
                
                batch.append(edge)
                cycle_edges += 1
                
                if len(batch) >= config.batch_size:
                    edges_coll.import_bulk(batch, on_duplicate="replace")
                    stats['edges_created'] += len(batch)
                    batch = []
            
            if batch:
                edges_coll.import_bulk(batch, on_duplicate="replace")
                stats['edges_created'] += len(batch)
            
            stats['by_cycle'][cycle] = cycle_edges
            context.log.info(f"✅ {cycle}: {cycle_edges:,} linkages")
            gc.collect()
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        edges_coll.add_persistent_index(fields=["_from"])
        edges_coll.add_persistent_index(fields=["_to"])
        
        context.log.info(f"🎉 Complete: {stats['edges_created']:,} edges")
        
        return Output(
            value=stats,
            metadata={
                "edges_count": stats['edges_created'],
                "by_cycle": MetadataValue.json(stats['by_cycle']),
            }
        )
