"""Individual Contributions Asset - Parse FEC individual contributions (indiv.zip)

This file contains Schedule A itemized individual contributions >$200.

RAW DATA STRATEGY (November 2025):
- indiv.zip contains ~40M records per cycle (2-4GB compressed)
- We load ALL records into ArangoDB raw (no filtering at parse time)
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
from src.resources.arango import ArangoDBResource
from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema
from src.utils.memory import get_processing_config


class IndividualContributionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


def _process_indiv_cycle(
    cycle: str,
    arango_host: str,
    arango_port: int,
    arango_user: str,
    arango_password: str,
    batch_size: int,
    tier: str,
    force_refresh: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    """Process a single cycle's indiv.zip file. Designed for parallel execution.
    
    Returns:
        Tuple of (cycle, result_dict) where result_dict contains count or error
    """
    from arango import ArangoClient
    from src.data import get_repository
    from src.utils.fec_schema import FECSchema
    from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
    
    repo = get_repository()
    result = {'cycle': cycle, 'count': 0, 'error': None}
    
    try:
        # Connect to ArangoDB (each thread gets its own connection)
        client = ArangoClient(hosts=f"http://{arango_host}:{arango_port}")
        sys_db = client.db("_system", username=arango_user, password=arango_password)
        
        # Ensure database exists
        db_name = f"fec_{cycle}"
        if not sys_db.has_database(db_name):
            sys_db.create_database(db_name)
        
        db = client.db(db_name, username=arango_user, password=arango_password)
        
        # Ensure collection exists
        if not db.has_collection("indiv"):
            db.create_collection("indiv")
        collection = db.collection("indiv")
        
        zip_path = repo.fec_indiv_path(cycle)
        
        if not zip_path.exists():
            result['error'] = f"File not found: {zip_path}"
            return (cycle, result)
        
        # Check if we can restore from dump
        if not force_refresh and should_restore_from_dump("indiv", zip_path, "fec", cycle):
            print(f"[{cycle}] 🚀 Restoring from dump...")
            
            restore_result = restore_collection_from_dump(
                db=db,
                collection_name="indiv",
                dump_type="fec",
                cycle=cycle,
                context=None
            )
            if restore_result:
                result['count'] = restore_result['record_count']
                result['restored'] = True
                return (cycle, result)
            else:
                print(f"[{cycle}] ⚠️ Restore failed, falling back to parsing...")
        
        # Parse from ZIP - NO FILTERING, load ALL records
        print(f"[{cycle}] 📂 Parsing indiv.zip (batch_size={batch_size:,}, tier={tier})...")
        collection.truncate()
        
        batch = []
        total_records = 0
        schema = FECSchema()
        
        with zipfile.ZipFile(zip_path) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                result['error'] = "No .txt file in ZIP"
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
                    
                    # Add timestamp and _key for ArangoDB
                    record['updated_at'] = datetime.now().isoformat()
                    
                    # Create unique _key from SUB_ID (FEC's unique transaction ID)
                    if record.get('SUB_ID'):
                        record['_key'] = str(record['SUB_ID'])
                    
                    # Type conversion for transaction amount
                    if record.get('TRANSACTION_AMT'):
                        try:
                            record['TRANSACTION_AMT'] = float(record['TRANSACTION_AMT'])
                        except (ValueError, TypeError):
                            record['TRANSACTION_AMT'] = None
                    
                    batch.append(record)
                    
                    # Batch insert for performance (dynamic sizing)
                    if len(batch) >= batch_size:
                        db.collection("indiv").import_bulk(batch, on_duplicate="replace")
                        batch = []
        
        # Insert remaining
        if batch:
            db.collection("indiv").import_bulk(batch, on_duplicate="replace")
        
        print(f"[{cycle}] ✅ {total_records:,} records loaded")
        
        result['count'] = total_records
        
        # Create indexes
        print(f"[{cycle}] 🔧 Creating indexes...")
        collection.add_persistent_index(fields=["CMTE_ID"])
        collection.add_persistent_index(fields=["NAME"])
        collection.add_persistent_index(fields=["EMPLOYER"])
        collection.add_persistent_index(fields=["ENTITY_TP"])
        collection.add_persistent_index(fields=["TRANSACTION_AMT"])
        collection.add_persistent_index(fields=["TRANSACTION_DT"])
        collection.add_persistent_index(fields=["NAME", "EMPLOYER"])
        
        # Apply JSON schema for documentation
        print(f"[{cycle}] 📋 Applying schema...")
        from src.utils.arango_schema import apply_fec_schemas
        apply_fec_schemas(db, collections=["indiv"], level="none")
        
        # Create dump for fast restore (with extended timeout for large datasets)
        print(f"[{cycle}] 📦 Creating dump for {total_records:,} records...")
        create_collection_dump(
            db=db,
            collection_name="indiv",
            source_file=zip_path,
            dump_type="fec",
            cycle=cycle,
            context=None
        )
        
    except Exception as e:
        import traceback
        result['error'] = f"{str(e)}\n{traceback.format_exc()}"
    
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
    arango: ArangoDBResource,
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
                arango.host,
                arango.port,
                arango.username,
                arango.password,
                batch_size,
                tier,
                config.force_refresh,
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
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "indiv",
        }
    )
