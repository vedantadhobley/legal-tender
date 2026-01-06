"""Itemized Transactions Asset - Parse FEC itemized transactions (pas2.zip)

NOTE: This file contains ALL itemized transactions (Schedule A receipts and Schedule B disbursements),
not just committee-to-committee transfers! Transaction types include 24A, 24C, 24E, 24F, 24H, 24K, 
24N, 24P, 24R, 24Z. Entity types include CCM, ORG, IND, PAC, COM, PTY.

PARALLEL PROCESSING: All 4 cycles processed simultaneously using ThreadPoolExecutor.
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


class ItemizedTransactionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


def _process_pas2_cycle(
    cycle: str,
    arango_host: str,
    arango_port: int,
    arango_user: str,
    arango_password: str,
    batch_size: int,
    tier: str,
    force_refresh: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    """Process a single cycle's pas2.zip file. Designed for parallel execution.
    
    Returns:
        Tuple of (cycle, result_dict) where result_dict contains stats or error
    """
    from arango import ArangoClient
    from src.data import get_repository
    from src.utils.fec_schema import FECSchema
    from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
    
    repo = get_repository()
    result = {
        'cycle': cycle,
        'count': 0,
        'error': None,
        'transaction_types': {},
        'entity_types': {},
    }
    
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
        if not db.has_collection("pas2"):
            db.create_collection("pas2")
        collection = db.collection("pas2")
        
        zip_path = repo.fec_pas2_path(cycle)
        
        if not zip_path.exists():
            result['error'] = f"File not found: {zip_path}"
            return (cycle, result)
        
        # Check if we can restore from dump
        if not force_refresh and should_restore_from_dump("pas2", zip_path, "fec", cycle):
            print(f"[{cycle}] 🚀 Restoring from dump...")
            
            restore_result = restore_collection_from_dump(
                db=db,
                collection_name="pas2",
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
        
        # Parse from ZIP
        print(f"[{cycle}] 📂 Parsing pas2.zip (batch_size={batch_size:,}, tier={tier})...")
        collection.truncate()
        
        batch = []
        cycle_transaction_types = {}
        cycle_entity_types = {}
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
                    
                    # Parse using official FEC schema
                    record = schema.parse_line('pas2', decoded)
                    if not record:
                        continue
                    
                    total_records += 1
                    
                    # Log progress every 250K records
                    if total_records % 250000 == 0:
                        print(f"[{cycle}] Parsed {total_records:,} records...")
                    
                    # Add timestamp and _key for ArangoDB
                    record['updated_at'] = datetime.now().isoformat()
                    
                    # Create unique _key from SUB_ID (FEC's unique transaction ID)
                    if record.get('SUB_ID'):
                        record['_key'] = str(record['SUB_ID'])
                    
                    # Track transaction and entity types
                    transaction_tp = record.get('TRANSACTION_TP', '')
                    entity_tp = record.get('ENTITY_TP', '')
                    cycle_transaction_types[transaction_tp] = cycle_transaction_types.get(transaction_tp, 0) + 1
                    cycle_entity_types[entity_tp] = cycle_entity_types.get(entity_tp, 0) + 1
                    
                    # Type conversion for transaction amount
                    if record.get('TRANSACTION_AMT'):
                        try:
                            record['TRANSACTION_AMT'] = float(record['TRANSACTION_AMT'])
                        except (ValueError, TypeError):
                            record['TRANSACTION_AMT'] = None
                    
                    batch.append(record)
                    
                    # Batch insert for performance
                    if len(batch) >= batch_size:
                        db.collection("pas2").import_bulk(batch, on_duplicate="replace")
                        batch = []
        
        # Insert remaining
        if batch:
            db.collection("pas2").import_bulk(batch, on_duplicate="replace")
        
        total_cycle = sum(cycle_transaction_types.values())
        print(f"[{cycle}] ✅ {total_cycle:,} transactions")
        
        result['count'] = total_cycle
        result['transaction_types'] = cycle_transaction_types
        result['entity_types'] = cycle_entity_types
        
        # Create indexes
        print(f"[{cycle}] 🔧 Creating indexes...")
        collection.add_persistent_index(fields=["CMTE_ID"])
        collection.add_persistent_index(fields=["CAND_ID"])
        collection.add_persistent_index(fields=["OTHER_ID"])
        collection.add_persistent_index(fields=["TRANSACTION_TP"])
        collection.add_persistent_index(fields=["ENTITY_TP"])
        collection.add_persistent_index(fields=["TRANSACTION_DT"])
        collection.add_persistent_index(fields=["TRANSACTION_AMT"])
        collection.add_persistent_index(fields=["TRANSACTION_TP", "ENTITY_TP"])
        
        # Apply JSON schema for documentation
        print(f"[{cycle}] 📋 Applying schema...")
        from src.utils.arango_schema import apply_fec_schemas
        apply_fec_schemas(db, collections=["pas2"], level="none")
        
        # Create dump for next time
        create_collection_dump(
            db=db,
            collection_name="pas2",
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
    name="pas2",
    description="FEC itemized transactions file (pas2.zip) - ALL transaction types. PARALLEL processing across cycles.",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def pas2_asset(
    context: AssetExecutionContext,
    config: ItemizedTransactionsConfig,
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse pas2.zip files IN PARALLEL and store in fec_{cycle}.pas2 collections."""
    
    stats = {'total_transactions': 0, 'by_cycle': {}, 'by_transaction_type': {}, 'by_entity_type': {}}
    
    mem_config = get_processing_config()
    batch_size = mem_config['batch_size']
    tier = mem_config['tier']
    
    # Use number of cycles as max workers
    max_workers = min(len(config.cycles), os.cpu_count() or 4, 4)
    
    context.log.info(f"🚀 PARALLEL PROCESSING: {len(config.cycles)} cycles with {max_workers} workers")
    context.log.info(f"   Memory tier: {tier}, Batch size: {batch_size:,}")
    
    # Process all cycles in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_pas2_cycle,
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
                    
                    stats['by_cycle'][cycle_name] = {
                        'total': count,
                        'transaction_types': result.get('transaction_types', {}),
                        'entity_types': result.get('entity_types', {}),
                    }
                    stats['total_transactions'] += count
                    
                    # Merge into global stats
                    for tt, c in result.get('transaction_types', {}).items():
                        stats['by_transaction_type'][tt] = stats['by_transaction_type'].get(tt, 0) + c
                    for et, c in result.get('entity_types', {}).items():
                        stats['by_entity_type'][et] = stats['by_entity_type'].get(et, 0) + c
                    
            except Exception as e:
                context.log.error(f"❌ {cycle} failed: {e}")
    
    context.log.info(f"🎉 COMPLETE: {stats['total_transactions']:,} total transactions across {len(stats['by_cycle'])} cycles")
    
    return Output(
        value=stats,
        metadata={
            "total_transactions": stats['total_transactions'],
            "transaction_types": MetadataValue.json(dict(sorted(stats['by_transaction_type'].items(), key=lambda x: x[1], reverse=True)[:10])),
            "entity_types": MetadataValue.json(dict(sorted(stats['by_entity_type'].items(), key=lambda x: x[1], reverse=True))),
            "cycles_processed": MetadataValue.json(config.cycles),
            "parallel_workers": max_workers,
            "batch_size": batch_size,
            "memory_tier": tier,
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "pas2",
        }
    )
