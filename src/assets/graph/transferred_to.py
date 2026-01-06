"""Transferred To Edge Collection - Committee-to-committee transfers (dark money paths).

Creates edges from committees to committees representing PAC-to-PAC transfers.
This is the "dark money" path before reaching candidates.

MEMORY OPTIMIZATION: Streaming processing with immediate writes.

Source: fec_{cycle}.pas2, fec_{cycle}.oth
Target: aggregation.transferred_to (edge collection)
"""

from typing import Dict, Any, List
from datetime import datetime
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class TransferredToConfig(Config):
    """Configuration for transferred_to edge asset."""
    cycles: List[str] = ["2020", "2022", "2024"]
    batch_size: int = 5000


@asset(
    name="transferred_to",
    description="Edges from committees to committees - PAC-to-PAC transfers.",
    group_name="graph",
    compute_kind="graph_edge",
    deps=["pas2", "oth", "contributed_to"],
)
def transferred_to_asset(
    context: AssetExecutionContext,
    config: TransferredToConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create transferred_to edge collection - MEMORY EFFICIENT.
    
    Streams aggregated data per cycle/source and writes immediately.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Create/truncate edge collection
        if agg_db.has_collection("transferred_to"):
            agg_db.collection("transferred_to").truncate()
            context.log.info("Truncated transferred_to")
        else:
            agg_db.create_collection("transferred_to", edge=True)
            context.log.info("Created transferred_to edge collection")
        
        edges_coll = agg_db.collection("transferred_to")
        
        # Get valid committee IDs
        valid_committees = set()
        cursor = agg_db.aql.execute("FOR c IN committees RETURN c._key", ttl=3600)
        for key in cursor:
            valid_committees.add(key)
        context.log.info(f"📋 {len(valid_committees):,} valid committees")
        
        stats = {'edges_created': 0, 'by_cycle': {}, 'from_pas2': 0, 'from_oth': 0}
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            cycle_edges = 0
            
            # Process pas2 - committee contributions
            # PAS2 is Schedule B (disbursements): CMTE_ID = giver, OTHER_ID = recipient
            if cycle_db.has_collection("pas2"):
                context.log.info(f"📊 Processing {cycle} pas2...")
                
                aql = """
                FOR doc IN pas2
                    FILTER doc.CMTE_ID != null AND doc.OTHER_ID != null
                    FILTER doc.TRANSACTION_AMT != null
                    FILTER doc.ENTITY_TP IN ["COM", "PAC", "PTY", "CCM", "ORG"]
                    
                    COLLECT 
                        source_cmte = doc.CMTE_ID,
                        dest_cmte = doc.OTHER_ID
                    AGGREGATE 
                        total_amount = SUM(TO_NUMBER(doc.TRANSACTION_AMT)),
                        transaction_count = COUNT(1)
                    
                    FILTER total_amount > 0
                    FILTER source_cmte != dest_cmte
                    
                    RETURN {
                        source_cmte: source_cmte,
                        dest_cmte: dest_cmte,
                        total_amount: total_amount,
                        transaction_count: transaction_count
                    }
                """
                
                cursor = cycle_db.aql.execute(
                    aql, ttl=3600, batch_size=config.batch_size, stream=True
                )
                
                batch = []
                for record in cursor:
                    source = record['source_cmte']
                    dest = record['dest_cmte']
                    
                    if source not in valid_committees or dest not in valid_committees:
                        continue
                    
                    edge = {
                        '_key': f"{source}_{dest}_{cycle}_pas2",
                        '_from': f"committees/{source}",
                        '_to': f"committees/{dest}",
                        'total_amount': record['total_amount'],
                        'transaction_count': record['transaction_count'],
                        'cycle': cycle,
                        'source': 'pas2',
                        'updated_at': datetime.now().isoformat()
                    }
                    
                    batch.append(edge)
                    cycle_edges += 1
                    stats['from_pas2'] += 1
                    
                    if len(batch) >= config.batch_size:
                        edges_coll.import_bulk(batch, on_duplicate="replace")
                        stats['edges_created'] += len(batch)
                        batch = []
                
                if batch:
                    edges_coll.import_bulk(batch, on_duplicate="replace")
                    stats['edges_created'] += len(batch)
                    batch = []
                
                gc.collect()
            
            # Process oth - other receipts
            # OTH is Schedule A (receipts): CMTE_ID = receiver, OTHER_ID = giver
            if cycle_db.has_collection("oth"):
                context.log.info(f"📊 Processing {cycle} oth...")
                
                aql = """
                FOR doc IN oth
                    FILTER doc.CMTE_ID != null AND doc.OTHER_ID != null
                    FILTER doc.TRANSACTION_AMT != null
                    
                    COLLECT 
                        source_cmte = doc.OTHER_ID,
                        dest_cmte = doc.CMTE_ID
                    AGGREGATE 
                        total_amount = SUM(TO_NUMBER(doc.TRANSACTION_AMT)),
                        transaction_count = COUNT(1)
                    
                    FILTER total_amount > 0
                    FILTER source_cmte != dest_cmte
                    
                    RETURN {
                        source_cmte: source_cmte,
                        dest_cmte: dest_cmte,
                        total_amount: total_amount,
                        transaction_count: transaction_count
                    }
                """
                
                cursor = cycle_db.aql.execute(
                    aql, ttl=3600, batch_size=config.batch_size, stream=True
                )
                
                batch = []
                for record in cursor:
                    source = record['source_cmte']
                    dest = record['dest_cmte']
                    
                    if source not in valid_committees or dest not in valid_committees:
                        continue
                    
                    edge = {
                        '_key': f"{source}_{dest}_{cycle}_oth",
                        '_from': f"committees/{source}",
                        '_to': f"committees/{dest}",
                        'total_amount': record['total_amount'],
                        'transaction_count': record['transaction_count'],
                        'cycle': cycle,
                        'source': 'oth',
                        'updated_at': datetime.now().isoformat()
                    }
                    
                    batch.append(edge)
                    cycle_edges += 1
                    stats['from_oth'] += 1
                    
                    if len(batch) >= config.batch_size:
                        edges_coll.import_bulk(batch, on_duplicate="replace")
                        stats['edges_created'] += len(batch)
                        batch = []
                
                if batch:
                    edges_coll.import_bulk(batch, on_duplicate="replace")
                    stats['edges_created'] += len(batch)
                
                gc.collect()
            
            stats['by_cycle'][cycle] = cycle_edges
            context.log.info(f"✅ {cycle}: {cycle_edges:,} transfers")
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        edges_coll.add_persistent_index(fields=["_from"])
        edges_coll.add_persistent_index(fields=["_to"])
        edges_coll.add_persistent_index(fields=["total_amount"])
        
        context.log.info(f"🎉 Complete: {stats['edges_created']:,} edges")
        
        return Output(
            value=stats,
            metadata={
                "edges_count": stats['edges_created'],
                "from_pas2": stats['from_pas2'],
                "from_oth": stats['from_oth'],
                "by_cycle": MetadataValue.json(stats['by_cycle']),
            }
        )
