"""Other Receipts Asset - Parse FEC other receipts (oth.zip) using raw FEC field names

This file contains Schedule A receipts that don't fit other categories.

CRITICAL FILTERING STRATEGY (November 2025):
- oth.zip contains 18.7M records (2024 cycle)
- 91% are individual donations (IND entity type) - mostly late filings (form 15J)
- We ONLY need committee-to-committee transfers for upstream tracing
- Filter at parse time: ENTITY_TP IN ["PAC", "COM", "PTY", "ORG"] only
- Result: 18.7M → 291K records (98.4% reduction)
- Processing time: 20+ min → ~2 min

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

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


class OtherReceiptsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="oth",
    description="FEC other receipts file (oth.zip) - Committee receipts including PAC-to-PAC transfers",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def oth_asset(
    context: AssetExecutionContext,
    config: OtherReceiptsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse oth.zip files and store in fec_{cycle}.oth collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_receipts': 0, 'by_cycle': {}, 'by_transaction_type': {}, 'by_entity_type': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "oth", database_name=f"fec_{cycle}")
                zip_path = repo.fec_oth_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if should_restore_from_dump(cycle, "oth", zip_path):
                    context.log.info("   🚀 Restoring from dump (30 sec)...")
                    
                    if restore_collection_from_dump(
                        cycle=cycle,
                        collection="oth",
                        mongo_uri=mongo.connection_string,
                        context=context
                    ):
                        record_count = collection.count_documents({})
                        stats['by_cycle'][cycle] = record_count
                        stats['total_receipts'] += record_count
                        
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
                context.log.info(f"   📂 Parsing {zip_path.name} (takes ~2 min with filtering)...")
                collection.delete_many({})
                
                batch = []
                cycle_transaction_types = {}
                cycle_entity_types = {}
                total_parsed = 0
                filtered_out = 0
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
                            record = schema.parse_line('oth', decoded)
                            if not record:
                                continue
                            
                            total_parsed += 1
                            
                            # CRITICAL FILTER: Only load committee-to-committee transfers
                            # Skip individual donations (IND) and candidate committee transfers (CCM)
                            entity_tp = record.get('ENTITY_TP', '')
                            
                            # Log progress every 100K records
                            if total_parsed % 100000 == 0:
                                context.log.info(
                                    f"      Parsed {total_parsed:,} records, "
                                    f"kept {len(batch) + sum(cycle_entity_types.values()):,}, "
                                    f"filtered {filtered_out:,}"
                                )
                            
                            if entity_tp not in ["PAC", "COM", "PTY", "ORG"]:
                                # Skip IND (17.1M records - individual donations)
                                # Skip CCM (1.2M records - candidate committee refunds)
                                # Skip CAN, others
                                filtered_out += 1
                                continue
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now()
                            
                            # Track transaction type and entity types (only for kept records)
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
                            if len(batch) >= 10000:
                                collection.insert_many(batch, ordered=False)
                                batch = []
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch, ordered=False)
                
                total_cycle = sum(cycle_transaction_types.values())
                reduction_pct = (filtered_out / total_parsed * 100) if total_parsed > 0 else 0
                
                context.log.info(f"   ✅ {cycle}: {total_cycle:,} committee transfers (filtered {filtered_out:,} records, {reduction_pct:.1f}% reduction)")
                context.log.info(f"      Total parsed: {total_parsed:,} records")
                context.log.info(f"      Kept: {total_cycle:,} committee transfers")
                context.log.info(f"      Entity types: {dict(sorted(cycle_entity_types.items(), key=lambda x: x[1], reverse=True))}")
                if cycle_transaction_types:
                    context.log.info(f"      Top transaction types: {dict(sorted(cycle_transaction_types.items(), key=lambda x: x[1], reverse=True)[:5])}")
                
                stats['by_cycle'][cycle] = {
                    'total': total_cycle,
                    'transaction_types': cycle_transaction_types,
                    'entity_types': cycle_entity_types
                }
                stats['total_receipts'] += total_cycle
                
                # Merge into global stats
                for tt, count in cycle_transaction_types.items():
                    stats['by_transaction_type'][tt] = stats['by_transaction_type'].get(tt, 0) + count
                for et, count in cycle_entity_types.items():
                    stats['by_entity_type'][et] = stats['by_entity_type'].get(et, 0) + count
                
                # Create indexes BEFORE dump
                collection.create_index([("CMTE_ID", 1)])  # CRITICAL: Recipient committee
                collection.create_index([("OTHER_ID", 1)])  # Donor committee (if transfer)
                collection.create_index([("FORM_TP", 1)])
                collection.create_index([("ENTITY_TP", 1)])
                collection.create_index([("TRANSACTION_DT", -1)])
                collection.create_index([("TRANSACTION_AMT", -1)])
                collection.create_index([("CMTE_ID", 1), ("ENTITY_TP", 1)])
                collection.create_index([("CMTE_ID", 1), ("OTHER_ID", 1)])  # For committee-to-committee lookups
                
                # Create dump for next time (includes indexes)
                create_collection_dump(
                    cycle=cycle,
                    collection="oth",
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
            "total_receipts": stats['total_receipts'],
            "transaction_types": MetadataValue.json(dict(sorted(stats['by_transaction_type'].items(), key=lambda x: x[1], reverse=True)[:10])),
            "entity_types": MetadataValue.json(dict(sorted(stats['by_entity_type'].items(), key=lambda x: x[1], reverse=True))),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "oth",
        }
    )
