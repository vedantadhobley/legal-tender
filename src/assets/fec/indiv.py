"""Individual Contributions Asset - Parse FEC individual contributions (indiv.zip) using raw FEC field names

This file contains Schedule A itemized individual contributions >$200.

RAW DATA STRATEGY (November 2025):
- indiv.zip contains ~40M records per cycle (2-4GB compressed)
- We load ALL records into MongoDB raw (no filtering at parse time)
- PARALLEL PROCESSING: All 4 cycles processed simultaneously using ThreadPoolExecutor
- Processing time: ~5 minutes with parallel processing (was 15-20 min sequential)
- Dump/restore: 30 seconds to reload complete dataset

What we load:
- ALL individual contributions from indiv.zip
- Complete raw dataset for flexible enrichment/aggregation

Filtering happens in ENRICHMENT layer:
- Apply $10K threshold per donor per cycle
- Aggregate donations by (NAME, EMPLOYER, CMTE_ID)
- Filter in enrichment allows changing thresholds without re-parsing

CRITICAL for Super PAC transparency:
- 44.9% of Super PACs have ZERO visible upstream funding without this
- Example: RED SENATE spent $914K, only $1K visible (need individual donors)
- Enables tracing: "MUSK, ELON → Super PAC → Candidate" influence chains
"""

from typing import Dict, Any, List, Tuple
from datetime import datetime
import zipfile
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema
from src.utils.memory import get_processing_config


class IndividualContributionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


def _process_indiv_cycle(
    cycle: str,
    mongo_uri: str,
    batch_size: int,
    tier: str,
) -> Tuple[str, Dict[str, Any]]:
    """Process a single cycle's indiv.zip file. Designed for parallel execution.
    
    Returns:
        Tuple of (cycle, result_dict) where result_dict contains count or error
    """
    from pymongo import MongoClient
    from src.data import get_repository
    from src.utils.fec_schema import FECSchema
    from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
    
    repo = get_repository()
    result = {'cycle': cycle, 'count': 0, 'error': None}
    
    try:
        # Connect to MongoDB (each thread gets its own connection)
        client = MongoClient(mongo_uri)
        db = client[f"fec_{cycle}"]
        collection = db["indiv"]
        
        zip_path = repo.fec_indiv_path(cycle)
        
        if not zip_path.exists():
            result['error'] = f"File not found: {zip_path}"
            client.close()
            return (cycle, result)
        
        # Check if we can restore from dump
        if should_restore_from_dump(cycle, "indiv", zip_path):
            print(f"[{cycle}] 🚀 Restoring from dump...")
            
            if restore_collection_from_dump(
                cycle=cycle,
                collection="indiv",
                mongo_uri=mongo_uri,
                context=None  # No context in thread
            ):
                result['count'] = collection.count_documents({})
                result['restored'] = True
                client.close()
                return (cycle, result)
            else:
                print(f"[{cycle}] ⚠️ Restore failed, falling back to parsing...")
        
        # Parse from ZIP - NO FILTERING, load ALL records
        print(f"[{cycle}] 📂 Parsing indiv.zip (batch_size={batch_size:,}, tier={tier})...")
        collection.delete_many({})
        
        batch = []
        total_records = 0
        schema = FECSchema()
        
        with zipfile.ZipFile(zip_path) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                result['error'] = "No .txt file in ZIP"
                client.close()
                return (cycle, result)
            
            with zf.open(txt_files[0]) as f:
                for line in f:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if not decoded:
                        continue
                    
                    # Parse using official FEC schema - NO FILTERING
                    record = schema.parse_line('indiv', decoded)
                    if not record:
                        continue
                    
                    total_records += 1
                    
                    # Log progress every 1M records
                    if total_records % 1000000 == 0:
                        print(f"[{cycle}] Parsed {total_records:,} records...")
                    
                    # Add timestamp
                    record['updated_at'] = datetime.now()
                    
                    # Type conversion for transaction amount
                    if record.get('TRANSACTION_AMT'):
                        try:
                            record['TRANSACTION_AMT'] = float(record['TRANSACTION_AMT'])
                        except (ValueError, TypeError):
                            record['TRANSACTION_AMT'] = None
                    
                    batch.append(record)
                    
                    # Batch insert for performance (dynamic sizing)
                    if len(batch) >= batch_size:
                        collection.insert_many(batch, ordered=False)
                        batch = []
        
        # Insert remaining
        if batch:
            collection.insert_many(batch, ordered=False)
        
        print(f"[{cycle}] ✅ {total_records:,} records loaded")
        
        result['count'] = total_records
        
        # Create indexes
        print(f"[{cycle}] 🔧 Creating indexes...")
        collection.create_index([("CMTE_ID", 1)])
        collection.create_index([("NAME", 1)])
        collection.create_index([("EMPLOYER", 1)])
        collection.create_index([("ENTITY_TP", 1)])
        collection.create_index([("TRANSACTION_AMT", -1)])
        collection.create_index([("TRANSACTION_DT", -1)])
        collection.create_index([("NAME", 1), ("EMPLOYER", 1)])
        
        # Create dump for next time (includes indexes)
        create_collection_dump(
            cycle=cycle,
            collection="indiv",
            mongo_uri=mongo_uri,
            source_file=zip_path,
            record_count=total_records,
            context=None  # No context in thread
        )
        
        client.close()
        
    except Exception as e:
        result['error'] = str(e)
    
    return (cycle, result)


@asset(
    name="indiv",
    description="FEC individual contributions file (indiv.zip) - ALL individual contributions, unfiltered raw data. PARALLEL processing across cycles.",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def indiv_asset(
    context: AssetExecutionContext,
    config: IndividualContributionsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse indiv.zip files IN PARALLEL and store ALL individual contributions in fec_{cycle}.indiv collections."""
    
    stats = {
        'total_contributions': 0,
        'by_cycle': {}
    }
    
    mem_config = get_processing_config()
    batch_size = mem_config['batch_size']
    tier = mem_config['tier']
    
    # Use number of cycles as max workers (each cycle is independent)
    max_workers = min(len(config.cycles), os.cpu_count() or 4, 4)  # Cap at 4 (one per cycle)
    
    context.log.info(f"🚀 PARALLEL PROCESSING: {len(config.cycles)} cycles with {max_workers} workers")
    context.log.info(f"   Memory tier: {tier}, Batch size: {batch_size:,}")
    
    # Process all cycles in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_indiv_cycle,
                cycle,
                mongo.connection_string,
                batch_size,
                tier,
            ): cycle
            for cycle in config.cycles
        }
        
        for future in as_completed(futures):
            cycle = futures[future]
            try:
                cycle_name, result = future.result()
                
                if result.get('error'):
                    context.log.error(f"❌ {cycle_name}: {result['error']}")
                else:
                    count = result['count']
                    restored = result.get('restored', False)
                    status = "restored from dump" if restored else "parsed"
                    context.log.info(f"✅ {cycle_name}: {count:,} records ({status})")
                    
                    stats['by_cycle'][cycle_name] = count
                    stats['total_contributions'] += count
                    
            except Exception as e:
                context.log.error(f"❌ {cycle} failed: {e}")
    
    context.log.info(f"🎉 COMPLETE: {stats['total_contributions']:,} total records across {len(stats['by_cycle'])} cycles")
    
    return Output(
        value=stats,
        metadata={
            "total_contributions": stats['total_contributions'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "parallel_workers": max_workers,
            "batch_size": batch_size,
            "memory_tier": tier,
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "indiv",
        }
    )
