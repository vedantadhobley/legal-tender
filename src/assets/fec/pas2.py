"""Itemized Transactions Asset - Parse FEC itemized transactions (pas2.zip) using raw FEC field names

NOTE: This file contains ALL itemized transactions (Schedule A receipts and Schedule B disbursements),
not just committee-to-committee transfers! Transaction types include 24A, 24C, 24E, 24F, 24H, 24K, 
24N, 24P, 24R, 24Z. Entity types include CCM, ORG, IND, PAC, COM, PTY.
"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


class ItemizedTransactionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="pas2",
    description="FEC itemized transactions file (pas2.zip) - ALL transaction types with raw FEC field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def pas2_asset(
    context: AssetExecutionContext,
    config: ItemizedTransactionsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse pas2.zip files and store in fec_{cycle}.pas2 collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_transactions': 0, 'by_cycle': {}, 'by_transaction_type': {}, 'by_entity_type': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "pas2", database_name=f"fec_{cycle}")
                zip_path = repo.fec_pas2_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if should_restore_from_dump(cycle, "pas2", zip_path):
                    context.log.info("   🚀 Restoring from dump (30 sec)...")
                    
                    if restore_collection_from_dump(
                        cycle=cycle,
                        collection="pas2",
                        mongo_uri=mongo.connection_string,
                        context=context
                    ):
                        record_count = collection.count_documents({})
                        stats['by_cycle'][cycle] = record_count
                        stats['total_transactions'] += record_count
                        
                        # Get transaction/entity type breakdown
                        for tt in collection.distinct("TRANSACTION_TP"):
                            count = collection.count_documents({"TRANSACTION_TP": tt})
                            stats['by_transaction_type'][tt] = stats['by_transaction_type'].get(tt, 0) + count
                        for et in collection.distinct("ENTITY_TP"):
                            count = collection.count_documents({"ENTITY_TP": et})
                            stats['by_entity_type'][et] = stats['by_entity_type'].get(et, 0) + count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP
                context.log.info(f"   📂 Parsing {zip_path.name} (takes ~15 min)...")
                collection.delete_many({})
                
                batch = []
                cycle_transaction_types = {}
                cycle_entity_types = {}
                schema = FECSchema()
                
                with zipfile.ZipFile(zip_path) as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                    if not txt_files:
                        continue
                    
                    with zf.open(txt_files[0]) as f:
                        for line in f:
                            decoded = line.decode('utf-8', errors='ignore').strip()
                            if not decoded:
                                continue
                            
                            # Parse using official FEC schema
                            record = schema.parse_line('pas2', decoded)
                            if not record:
                                continue
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now()
                            
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
                            if len(batch) >= 10000:
                                collection.insert_many(batch, ordered=False)
                                batch = []
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch, ordered=False)
                
                total_cycle = sum(cycle_transaction_types.values())
                context.log.info(f"   ✅ {cycle}: {total_cycle:,} transactions")
                context.log.info(f"      Transaction types: {dict(sorted(cycle_transaction_types.items(), key=lambda x: x[1], reverse=True)[:5])}")
                context.log.info(f"      Entity types: {dict(sorted(cycle_entity_types.items(), key=lambda x: x[1], reverse=True)[:5])}")
                
                stats['by_cycle'][cycle] = {
                    'total': total_cycle,
                    'transaction_types': cycle_transaction_types,
                    'entity_types': cycle_entity_types
                }
                stats['total_transactions'] += total_cycle
                
                # Merge into global stats
                for tt, count in cycle_transaction_types.items():
                    stats['by_transaction_type'][tt] = stats['by_transaction_type'].get(tt, 0) + count
                for et, count in cycle_entity_types.items():
                    stats['by_entity_type'][et] = stats['by_entity_type'].get(et, 0) + count
                
                # Create indexes BEFORE dump
                collection.create_index([("CMTE_ID", 1)])
                collection.create_index([("CAND_ID", 1)])
                collection.create_index([("OTHER_ID", 1)])
                collection.create_index([("TRANSACTION_TP", 1)])
                collection.create_index([("ENTITY_TP", 1)])
                collection.create_index([("TRANSACTION_DT", -1)])
                collection.create_index([("TRANSACTION_AMT", -1)])
                collection.create_index([("TRANSACTION_TP", 1), ("ENTITY_TP", 1)])
                
                # Create dump for next time (includes indexes)
                create_collection_dump(
                    cycle=cycle,
                    collection="pas2",
                    mongo_uri=mongo.connection_string,
                    source_file=zip_path,
                    record_count=total_cycle,
                    context=context
                )
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_transactions": stats['total_transactions'],
            "transaction_types": MetadataValue.json(dict(sorted(stats['by_transaction_type'].items(), key=lambda x: x[1], reverse=True)[:10])),
            "entity_types": MetadataValue.json(dict(sorted(stats['by_entity_type'].items(), key=lambda x: x[1], reverse=True))),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "pas2",
        }
    )
