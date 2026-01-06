"""Donors Vertex Collection - Normalized individual donors for graph traversal.

Threshold: $10,000+ total contributions in ANY single cycle.

This aggregates raw indiv records by (NAME, EMPLOYER) and creates deduplicated
donor vertices. Only donors exceeding the threshold are included.

MEMORY OPTIMIZATION: Uses server-side AQL aggregation and streaming inserts.
Never holds more than one batch in Python memory at a time.

Source: fec_{cycle}.indiv collections
Target: aggregation.donors (vertex collection)
"""

from typing import Dict, Any, List
from datetime import datetime
from hashlib import sha256
import re
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class DonorsConfig(Config):
    """Configuration for donors vertex asset."""
    cycles: List[str] = ["2020", "2022", "2024"]
    threshold_amount: float = 10000.0
    batch_size: int = 5000  # Smaller batch for memory


def normalize_name(name: str) -> str:
    """Normalize donor name for deduplication."""
    if not name:
        return ""
    name = re.sub(r'\s*,\s*', ' ', name.upper().strip())
    name = re.sub(r'\s+', ' ', name)
    return name


def normalize_employer(employer: str) -> str:
    """Normalize employer name for deduplication."""
    if not employer:
        return ""
    employer = employer.upper().strip()
    employer = re.sub(r'[,.]', '', employer)
    employer = re.sub(r'\s+', ' ', employer)
    return employer


def make_donor_key(name: str, employer: str) -> str:
    """Create unique _key for donor from normalized name + employer."""
    normalized = f"{normalize_name(name)}|{normalize_employer(employer)}"
    return sha256(normalized.encode()).hexdigest()[:16]


def upsert_donors_batch(db, batch: List[Dict], cycle: str):
    """Upsert a batch of donors, merging cycle data for existing donors."""
    
    aql = """
    FOR doc IN @batch
        UPSERT { _key: doc._key }
        INSERT doc
        UPDATE {
            total_amount: OLD.total_amount + doc.total_amount,
            transaction_count: OLD.transaction_count + doc.transaction_count,
            cycles: APPEND(OLD.cycles, doc.cycles, true),
            updated_at: doc.updated_at
        }
        IN donors
    """
    
    db.aql.execute(aql, bind_vars={"batch": batch})


@asset(
    name="donors",
    description="Normalized donor vertices - individuals who contributed $10K+ in any cycle.",
    group_name="graph",
    compute_kind="graph_vertex",
    deps=["indiv"],
)
def donors_asset(
    context: AssetExecutionContext,
    config: DonorsConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create donors vertex collection - MEMORY EFFICIENT.
    
    1. Process one cycle at a time
    2. Server-side AQL COLLECT aggregation (no Python memory)
    3. Stream results directly to donors collection using UPSERT
    4. Force GC between cycles
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        
        # Ensure aggregation database exists
        if not sys_db.has_database("aggregation"):
            sys_db.create_database("aggregation")
            context.log.info("Created 'aggregation' database")
        
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Create/truncate donors collection
        if agg_db.has_collection("donors"):
            agg_db.collection("donors").truncate()
            context.log.info("Truncated existing donors collection")
        else:
            agg_db.create_collection("donors")
            context.log.info("Created donors collection")
        
        stats = {
            'cycles_processed': 0,
            'total_donors_inserted': 0,
            'by_cycle': {}
        }
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            
            if not sys_db.has_database(db_name):
                context.log.warning(f"Database {db_name} does not exist, skipping")
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            
            if not cycle_db.has_collection("indiv"):
                context.log.warning(f"{db_name}.indiv does not exist, skipping")
                continue
            
            context.log.info(f"📊 Processing {cycle} - server-side aggregation...")
            
            # Server-side aggregation - minimal memory footprint
            aql = """
            FOR doc IN indiv
                FILTER doc.NAME != null AND doc.NAME != ""
                FILTER doc.TRANSACTION_AMT != null
                COLLECT 
                    name = doc.NAME,
                    employer = (doc.EMPLOYER == null OR doc.EMPLOYER == "") ? "NOT EMPLOYED" : doc.EMPLOYER
                AGGREGATE 
                    total_amount = SUM(TO_NUMBER(doc.TRANSACTION_AMT)),
                    transaction_count = COUNT(1)
                FILTER total_amount >= @threshold
                RETURN {
                    name: name,
                    employer: employer,
                    total_amount: total_amount,
                    transaction_count: transaction_count
                }
            """
            
            cursor = cycle_db.aql.execute(
                aql,
                bind_vars={"threshold": config.threshold_amount},
                ttl=7200,
                batch_size=config.batch_size,
                stream=True
            )
            
            cycle_donors = 0
            cycle_total = 0.0
            batch = []
            
            for record in cursor:
                donor_key = make_donor_key(record['name'], record['employer'])
                
                doc = {
                    '_key': donor_key,
                    'canonical_name': record['name'],
                    'canonical_employer': record['employer'],
                    'total_amount': record['total_amount'],
                    'transaction_count': record['transaction_count'],
                    'cycles': [cycle],
                    'updated_at': datetime.now().isoformat()
                }
                
                batch.append(doc)
                cycle_donors += 1
                cycle_total += record['total_amount']
                
                if len(batch) >= config.batch_size:
                    upsert_donors_batch(agg_db, batch, cycle)
                    stats['total_donors_inserted'] += len(batch)
                    batch = []
                    
                    if cycle_donors % 25000 == 0:
                        context.log.info(f"  [{cycle}] {cycle_donors:,} donors...")
                        gc.collect()  # Periodic GC
            
            if batch:
                upsert_donors_batch(agg_db, batch, cycle)
                stats['total_donors_inserted'] += len(batch)
            
            stats['by_cycle'][cycle] = {
                'donors_above_threshold': cycle_donors,
                'total_contributed': cycle_total
            }
            stats['cycles_processed'] += 1
            
            context.log.info(f"✅ {cycle}: {cycle_donors:,} donors, ${cycle_total:,.0f}")
            
            # Force GC between cycles
            gc.collect()
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        donors_coll = agg_db.collection("donors")
        donors_coll.add_persistent_index(fields=["canonical_name"])
        donors_coll.add_persistent_index(fields=["canonical_employer"])
        donors_coll.add_persistent_index(fields=["total_amount"])
        
        final_count = donors_coll.count()
        
        context.log.info(f"🎉 Complete: {final_count:,} unique donors")
        
        return Output(
            value=stats,
            metadata={
                "donors_count": final_count,
                "threshold_amount": config.threshold_amount,
                "cycles_processed": stats['cycles_processed'],
                "by_cycle": MetadataValue.json(stats['by_cycle']),
            }
        )
