"""Contributed To Edge Collection - Individual donations to committees.

Creates edges from donors to committees representing individual contributions.
Only includes donations from donors above the $10K threshold.

MEMORY OPTIMIZATION: Streaming processing with immediate writes.

Source: fec_{cycle}.indiv + aggregation.donors
Target: aggregation.contributed_to (edge collection)
"""

from typing import Dict, Any, List
from datetime import datetime
from hashlib import sha256
import re
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class ContributedToConfig(Config):
    """Configuration for contributed_to edge asset."""
    cycles: List[str] = ["2020", "2022", "2024"]
    batch_size: int = 5000


def normalize_name(name: str) -> str:
    """Normalize donor name."""
    if not name:
        return ""
    name = re.sub(r'\s*,\s*', ' ', name.upper().strip())
    name = re.sub(r'\s+', ' ', name)
    return name


def normalize_employer(employer: str) -> str:
    """Normalize employer name."""
    if not employer:
        return ""
    employer = employer.upper().strip()
    employer = re.sub(r'[,.]', '', employer)
    employer = re.sub(r'\s+', ' ', employer)
    return employer


def make_donor_key(name: str, employer: str) -> str:
    """Create donor _key from normalized name + employer."""
    normalized = f"{normalize_name(name)}|{normalize_employer(employer)}"
    return sha256(normalized.encode()).hexdigest()[:16]


@asset(
    name="contributed_to",
    description="Edges from donors to committees - individual contributions.",
    group_name="graph",
    compute_kind="graph_edge",
    deps=["donors", "indiv", "cm", "cn"],
)
def contributed_to_asset(
    context: AssetExecutionContext,
    config: ContributedToConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create contributed_to edge collection - MEMORY EFFICIENT.
    
    Streams aggregated data and writes directly to ArangoDB using UPSERT.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("donors"):
            raise RuntimeError("donors collection missing - run donors asset first")
        
        # Create/truncate edge collection
        if agg_db.has_collection("contributed_to"):
            agg_db.collection("contributed_to").truncate()
            context.log.info("Truncated contributed_to")
        else:
            agg_db.create_collection("contributed_to", edge=True)
            context.log.info("Created contributed_to edge collection")
        
        # Ensure committees exists
        if not agg_db.has_collection("committees"):
            agg_db.create_collection("committees")
        else:
            agg_db.collection("committees").truncate()
        
        # Ensure candidates exists  
        if not agg_db.has_collection("candidates"):
            agg_db.create_collection("candidates")
        else:
            agg_db.collection("candidates").truncate()
        
        # Copy committees and candidates from FEC data
        context.log.info("📋 Copying committees & candidates...")
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            
            # Copy committees
            if cycle_db.has_collection("cm"):
                cursor = cycle_db.aql.execute(
                    "FOR doc IN cm RETURN doc",
                    ttl=3600, batch_size=5000, stream=True
                )
                batch = []
                for doc in cursor:
                    doc['_key'] = doc['CMTE_ID']
                    batch.append(doc)
                    if len(batch) >= 5000:
                        agg_db.collection("committees").import_bulk(batch, on_duplicate="update")
                        batch = []
                if batch:
                    agg_db.collection("committees").import_bulk(batch, on_duplicate="update")
                gc.collect()
            
            # Copy candidates
            if cycle_db.has_collection("cn"):
                cursor = cycle_db.aql.execute(
                    "FOR doc IN cn RETURN doc",
                    ttl=3600, batch_size=5000, stream=True
                )
                batch = []
                for doc in cursor:
                    doc['_key'] = doc['CAND_ID']
                    batch.append(doc)
                    if len(batch) >= 5000:
                        agg_db.collection("candidates").import_bulk(batch, on_duplicate="update")
                        batch = []
                if batch:
                    agg_db.collection("candidates").import_bulk(batch, on_duplicate="update")
                gc.collect()
            
            context.log.info(f"  Copied from {cycle}")
        
        # Load donor keys into memory (this should be small - only $10K+ donors)
        context.log.info("📋 Loading donor keys...")
        donor_keys = set()
        cursor = agg_db.aql.execute("FOR d IN donors RETURN d._key", ttl=3600)
        for key in cursor:
            donor_keys.add(key)
        context.log.info(f"  {len(donor_keys):,} donors to match")
        
        stats = {'edges_created': 0, 'by_cycle': {}}
        
        # Process each cycle
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            if not cycle_db.has_collection("indiv"):
                continue
            
            context.log.info(f"📊 Processing {cycle} contributions...")
            
            # Aggregate by donor-committee on server
            aql = """
            FOR doc IN indiv
                FILTER doc.NAME != null AND doc.NAME != ""
                FILTER doc.CMTE_ID != null
                FILTER doc.TRANSACTION_AMT != null
                
                COLLECT 
                    name = doc.NAME,
                    employer = (doc.EMPLOYER == null OR doc.EMPLOYER == "") ? "NOT EMPLOYED" : doc.EMPLOYER,
                    cmte_id = doc.CMTE_ID
                AGGREGATE 
                    total_amount = SUM(TO_NUMBER(doc.TRANSACTION_AMT)),
                    transaction_count = COUNT(1)
                
                FILTER total_amount > 0
                
                RETURN {
                    name: name,
                    employer: employer,
                    cmte_id: cmte_id,
                    total_amount: total_amount,
                    transaction_count: transaction_count
                }
            """
            
            cursor = cycle_db.aql.execute(
                aql, ttl=7200, batch_size=config.batch_size, stream=True
            )
            
            cycle_edges = 0
            batch = []
            
            for record in cursor:
                donor_key = make_donor_key(record['name'], record['employer'])
                
                # Skip if donor not in our filtered list
                if donor_key not in donor_keys:
                    continue
                
                edge_key = f"{donor_key}_{record['cmte_id']}_{cycle}"
                
                edge = {
                    '_key': edge_key,
                    '_from': f"donors/{donor_key}",
                    '_to': f"committees/{record['cmte_id']}",
                    'total_amount': record['total_amount'],
                    'transaction_count': record['transaction_count'],
                    'cycle': cycle,
                    'updated_at': datetime.now().isoformat()
                }
                
                batch.append(edge)
                cycle_edges += 1
                
                if len(batch) >= config.batch_size:
                    agg_db.collection("contributed_to").import_bulk(batch, on_duplicate="replace")
                    stats['edges_created'] += len(batch)
                    batch = []
                    
                    if cycle_edges % 50000 == 0:
                        context.log.info(f"  [{cycle}] {cycle_edges:,} edges...")
                        gc.collect()
            
            if batch:
                agg_db.collection("contributed_to").import_bulk(batch, on_duplicate="replace")
                stats['edges_created'] += len(batch)
            
            stats['by_cycle'][cycle] = cycle_edges
            context.log.info(f"✅ {cycle}: {cycle_edges:,} edges")
            gc.collect()
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        edges_coll = agg_db.collection("contributed_to")
        edges_coll.add_persistent_index(fields=["_from"])
        edges_coll.add_persistent_index(fields=["_to"])
        edges_coll.add_persistent_index(fields=["total_amount"])
        
        context.log.info(f"🎉 Complete: {stats['edges_created']:,} edges")
        
        return Output(
            value=stats,
            metadata={
                "edges_count": stats['edges_created'],
                "by_cycle": MetadataValue.json(stats['by_cycle']),
            }
        )
