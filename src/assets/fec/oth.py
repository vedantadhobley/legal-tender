"""Other Receipts Asset - Parse FEC other receipts (oth.zip)

This file contains Schedule A receipts that don't fit other categories.

CRITICAL FILTERING STRATEGY (November 2025):
- oth.zip contains 18.7M records (2024 cycle)
- 91% are individual donations (IND entity type) - mostly late filings (form 15J)
- We ONLY need committee-to-committee transfers for upstream tracing
- Filter at parse time: ENTITY_TP IN ["PAC", "COM", "PTY", "ORG"] only
- Result: 18.7M → 291K records (98.4% reduction)
- PARALLEL PROCESSING: All 4 cycles processed simultaneously

What we load:
- PAC-to-PAC transfers (156K records, $2.4B) - CRITICAL for dark money tracing
- Committee transfers (24K records, $2.4B)
- Party committee transfers (112K records, $2.4B)
- Organization donations (85K records, $5.4B)

What we SKIP:
- Individual donations (17.1M records) - Use indiv (indiv.zip) for this in Phase 2
- Candidate committee transfers (1.2M records) - Mostly refunds, not upstream influence

CRITICAL for upstream tracing: Shows WHO gave money TO committees (CMTE_ID = recipient)
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


class OtherReceiptsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


def _process_oth_cycle(
    cycle: str,
    arango_host: str,
    arango_port: int,
    arango_user: str,
    arango_password: str,
    batch_size: int,
    tier: str,
    force_refresh: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    """Process a single cycle's oth.zip file. Designed for parallel execution.
    
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
        'total_parsed': 0,
        'filtered_out': 0,
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
        if not db.has_collection("oth"):
            db.create_collection("oth")
        collection = db.collection("oth")
        
        zip_path = repo.fec_oth_path(cycle)
        
        if not zip_path.exists():
            result['error'] = f"File not found: {zip_path}"
            return (cycle, result)
        
        # Check if we can restore from dump
        if not force_refresh and should_restore_from_dump("oth", zip_path, "fec", cycle):
            print(f"[{cycle}] 🚀 Restoring from dump...")
            
            restore_result = restore_collection_from_dump(
                db=db,
                collection_name="oth",
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
        print(f"[{cycle}] 📂 Parsing oth.zip (batch_size={batch_size:,}, tier={tier})...")
        collection.truncate()
        
        batch = []
        cycle_transaction_types = {}
        cycle_entity_types = {}
        total_parsed = 0
        filtered_out = 0
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
                    record = schema.parse_line('oth', decoded)
                    if not record:
                        continue
                    
                    total_parsed += 1
                    
                    # CRITICAL FILTER: Only load committee-to-committee transfers
                    entity_tp = record.get('ENTITY_TP', '')
                    
                    # Log progress every 500K records
                    if total_parsed % 500000 == 0:
                        print(f"[{cycle}] Parsed {total_parsed:,}, kept {len(batch) + sum(cycle_entity_types.values()):,}")
                    
                    if entity_tp not in ["PAC", "COM", "PTY", "ORG"]:
                        filtered_out += 1
                        continue
                    
                    # Add timestamp and _key for ArangoDB
                    record['updated_at'] = datetime.now().isoformat()
                    
                    # Create unique _key from SUB_ID (FEC's unique transaction ID)
                    if record.get('SUB_ID'):
                        record['_key'] = str(record['SUB_ID'])
                    
                    # Track transaction type and entity types
                    transaction_tp = record.get('TRANSACTION_TP', '')
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
                        db.collection("oth").import_bulk(batch, on_duplicate="replace")
                        batch = []
        
        # Insert remaining
        if batch:
            db.collection("oth").import_bulk(batch, on_duplicate="replace")
        
        total_cycle = sum(cycle_transaction_types.values())
        reduction_pct = (filtered_out / total_parsed * 100) if total_parsed > 0 else 0
        
        print(f"[{cycle}] ✅ {total_cycle:,} records ({reduction_pct:.1f}% filtered)")
        
        result['count'] = total_cycle
        result['transaction_types'] = cycle_transaction_types
        result['entity_types'] = cycle_entity_types
        result['total_parsed'] = total_parsed
        result['filtered_out'] = filtered_out
        
        # Create indexes
        print(f"[{cycle}] 🔧 Creating indexes...")
        collection.add_persistent_index(fields=["CMTE_ID"])
        collection.add_persistent_index(fields=["OTHER_ID"])
        collection.add_persistent_index(fields=["FORM_TP"])
        collection.add_persistent_index(fields=["ENTITY_TP"])
        collection.add_persistent_index(fields=["TRANSACTION_DT"])
        collection.add_persistent_index(fields=["TRANSACTION_AMT"])
        collection.add_persistent_index(fields=["CMTE_ID", "ENTITY_TP"])
        collection.add_persistent_index(fields=["CMTE_ID", "OTHER_ID"])
        
        # Apply JSON schema for documentation
        print(f"[{cycle}] 📋 Applying schema...")
        from src.utils.arango_schema import apply_fec_schemas
        apply_fec_schemas(db, collections=["oth"], level="none")
        
        # Create dump for next time
        create_collection_dump(
            db=db,
            collection_name="oth",
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
    name="oth",
    description="FEC other receipts file (oth.zip) - Committee receipts including PAC-to-PAC transfers. PARALLEL processing across cycles.",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def oth_asset(
    context: AssetExecutionContext,
    config: OtherReceiptsConfig,
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse oth.zip files IN PARALLEL and store in fec_{cycle}.oth collections."""
    
    stats = {'total_receipts': 0, 'by_cycle': {}, 'by_transaction_type': {}, 'by_entity_type': {}}
    
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
                _process_oth_cycle,
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
                    stats['total_receipts'] += count
                    
                    # Merge into global stats
                    for tt, c in result.get('transaction_types', {}).items():
                        stats['by_transaction_type'][tt] = stats['by_transaction_type'].get(tt, 0) + c
                    for et, c in result.get('entity_types', {}).items():
                        stats['by_entity_type'][et] = stats['by_entity_type'].get(et, 0) + c
                    
            except Exception as e:
                context.log.error(f"❌ {cycle} failed: {e}")
    
    context.log.info(f"🎉 COMPLETE: {stats['total_receipts']:,} total records across {len(stats['by_cycle'])} cycles")
    
    return Output(
        value=stats,
        metadata={
            "total_receipts": stats['total_receipts'],
            "transaction_types": MetadataValue.json(dict(sorted(stats['by_transaction_type'].items(), key=lambda x: x[1], reverse=True)[:10])),
            "entity_types": MetadataValue.json(dict(sorted(stats['by_entity_type'].items(), key=lambda x: x[1], reverse=True))),
            "cycles_processed": MetadataValue.json(config.cycles),
            "parallel_workers": max_workers,
            "batch_size": batch_size,
            "memory_tier": tier,
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "oth",
        }
    )
