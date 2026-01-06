"""Spent On Edge Collection - Independent Expenditures (IE) for/against candidates.

Creates edges from committees to candidates representing independent expenditures.
This captures Super PAC spending FOR or AGAINST candidates.

Transaction Types:
- 24E: Independent expenditure advocating election of candidate (SUPPORT)
- 24A: Independent expenditure against candidate (OPPOSE)

Source: fec_{cycle}.pas2 (TRANSACTION_TP IN ['24E', '24A'])
Target: aggregation.spent_on (edge collection)
"""

from typing import Dict, Any, List
from datetime import datetime
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class SpentOnConfig(Config):
    """Configuration for spent_on edge asset."""
    cycles: List[str] = ["2020", "2022", "2024"]
    batch_size: int = 5000


@asset(
    name="spent_on",
    description="Edges from committees to candidates - independent expenditures FOR/AGAINST.",
    group_name="graph",
    compute_kind="graph_edge",
    deps=["pas2", "contributed_to"],  # contributed_to ensures committees/candidates exist
)
def spent_on_asset(
    context: AssetExecutionContext,
    config: SpentOnConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create spent_on edge collection for Independent Expenditures.
    
    24E = Support (independent expenditure FOR a candidate)
    24A = Oppose (independent expenditure AGAINST a candidate)
    
    These are NOT direct contributions - they're spending by Super PACs
    on ads, mailers, etc. advocating for/against candidates.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Create/truncate edge collection
        if agg_db.has_collection("spent_on"):
            agg_db.collection("spent_on").truncate()
            context.log.info("Truncated spent_on")
        else:
            agg_db.create_collection("spent_on", edge=True)
            context.log.info("Created spent_on edge collection")
        
        edges_coll = agg_db.collection("spent_on")
        
        # Get valid committee IDs
        valid_committees = set()
        cursor = agg_db.aql.execute("FOR c IN committees RETURN c._key", ttl=3600)
        for key in cursor:
            valid_committees.add(key)
        context.log.info(f"📋 {len(valid_committees):,} valid committees")
        
        # Get valid candidate IDs
        valid_candidates = set()
        cursor = agg_db.aql.execute("FOR c IN candidates RETURN c._key", ttl=3600)
        for key in cursor:
            valid_candidates.add(key)
        context.log.info(f"🎯 {len(valid_candidates):,} valid candidates")
        
        stats = {
            'edges_created': 0,
            'by_cycle': {},
            'support_edges': 0,
            'oppose_edges': 0,
            'support_amount': 0,
            'oppose_amount': 0,
            'skipped_invalid_committee': 0,
            'skipped_invalid_candidate': 0,
        }
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            cycle_edges = 0
            
            if not cycle_db.has_collection("pas2"):
                continue
            
            context.log.info(f"📊 Processing {cycle} independent expenditures (24E/24A)...")
            
            # Aggregate IE records by committee + candidate + support/oppose
            aql = """
            FOR doc IN pas2
                FILTER doc.TRANSACTION_TP IN ["24E", "24A"]
                FILTER doc.CMTE_ID != null AND doc.CAND_ID != null
                FILTER doc.TRANSACTION_AMT != null
                
                LET support_oppose = (doc.TRANSACTION_TP == "24E") ? "S" : "O"
                
                COLLECT 
                    cmte_id = doc.CMTE_ID,
                    cand_id = doc.CAND_ID,
                    sup_opp = support_oppose
                AGGREGATE 
                    total_amount = SUM(TO_NUMBER(doc.TRANSACTION_AMT)),
                    transaction_count = COUNT(1)
                
                FILTER total_amount > 0
                
                RETURN {
                    cmte_id: cmte_id,
                    cand_id: cand_id,
                    support_oppose: sup_opp,
                    total_amount: total_amount,
                    transaction_count: transaction_count
                }
            """
            
            cursor = cycle_db.aql.execute(
                aql, ttl=3600, batch_size=config.batch_size, stream=True
            )
            
            batch = []
            for record in cursor:
                cmte_id = record['cmte_id']
                cand_id = record['cand_id']
                
                # Validate both ends exist
                if cmte_id not in valid_committees:
                    stats['skipped_invalid_committee'] += 1
                    continue
                    
                if cand_id not in valid_candidates:
                    stats['skipped_invalid_candidate'] += 1
                    continue
                
                support_oppose = record['support_oppose']
                
                edge = {
                    '_key': f"{cmte_id}_{cand_id}_{support_oppose}_{cycle}",
                    '_from': f"committees/{cmte_id}",
                    '_to': f"candidates/{cand_id}",
                    'total_amount': record['total_amount'],
                    'transaction_count': record['transaction_count'],
                    'support_oppose': support_oppose,  # 'S' = Support, 'O' = Oppose
                    'cycle': cycle,
                    'updated_at': datetime.now().isoformat()
                }
                
                batch.append(edge)
                cycle_edges += 1
                
                if support_oppose == 'S':
                    stats['support_edges'] += 1
                    stats['support_amount'] += record['total_amount']
                else:
                    stats['oppose_edges'] += 1
                    stats['oppose_amount'] += record['total_amount']
                
                if len(batch) >= config.batch_size:
                    edges_coll.import_bulk(batch, on_duplicate="replace")
                    stats['edges_created'] += len(batch)
                    batch = []
            
            if batch:
                edges_coll.import_bulk(batch, on_duplicate="replace")
                stats['edges_created'] += len(batch)
                batch = []
            
            stats['by_cycle'][cycle] = cycle_edges
            context.log.info(f"  ✅ {cycle}: {cycle_edges:,} IE edges")
            gc.collect()
        
        # Create indexes
        context.log.info("📇 Creating indexes...")
        edges_coll.add_hash_index(fields=['_from'], unique=False)
        edges_coll.add_hash_index(fields=['_to'], unique=False)
        edges_coll.add_hash_index(fields=['support_oppose'], unique=False)
        edges_coll.add_hash_index(fields=['cycle'], unique=False)
        
        context.log.info(f"✅ Created {stats['edges_created']:,} spent_on edges")
        context.log.info(f"   Support (24E): {stats['support_edges']:,} edges, ${stats['support_amount']:,.0f}")
        context.log.info(f"   Oppose (24A): {stats['oppose_edges']:,} edges, ${stats['oppose_amount']:,.0f}")
        
        return Output(
            value=stats,
            metadata={
                "edges_created": MetadataValue.int(stats['edges_created']),
                "support_edges": MetadataValue.int(stats['support_edges']),
                "oppose_edges": MetadataValue.int(stats['oppose_edges']),
                "support_amount": MetadataValue.float(stats['support_amount']),
                "oppose_amount": MetadataValue.float(stats['oppose_amount']),
                "skipped_invalid_committee": MetadataValue.int(stats['skipped_invalid_committee']),
                "skipped_invalid_candidate": MetadataValue.int(stats['skipped_invalid_candidate']),
                "cycles_processed": MetadataValue.text(str(config.cycles)),
            }
        )
