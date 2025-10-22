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


class ItemizedTransactionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="itpas2",
    description="FEC itemized transactions file (pas2.zip) - ALL transaction types with raw FEC field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def itpas2_asset(
    context: AssetExecutionContext,
    config: ItemizedTransactionsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse pas2.zip files and store in fec_{cycle}.itpas2 collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_transactions': 0, 'by_cycle': {}, 'by_transaction_type': {}, 'by_entity_type': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "itpas2", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_pas2_path(cycle)
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                batch = []
                cycle_transaction_types = {}
                cycle_entity_types = {}
                
                with zipfile.ZipFile(zip_path) as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                    if not txt_files:
                        continue
                    
                    with zf.open(txt_files[0]) as f:
                        for line in f:
                            decoded = line.decode('utf-8', errors='ignore').strip()
                            if not decoded:
                                continue
                            
                            fields = decoded.split('|')
                            if len(fields) < 22:
                                continue
                            
                            # Track transaction and entity types
                            transaction_tp = fields[5]
                            entity_tp = fields[6]
                            cycle_transaction_types[transaction_tp] = cycle_transaction_types.get(transaction_tp, 0) + 1
                            cycle_entity_types[entity_tp] = cycle_entity_types.get(entity_tp, 0) + 1
                            
                            # Use EXACT field names from fec.md - ALL 22 fields
                            batch.append({

                                'CMTE_ID': fields[0],
                                'AMNDT_IND': fields[1],
                                'RPT_TP': fields[2],
                                'TRANSACTION_PGI': fields[3],
                                'IMAGE_NUM': fields[4],
                                'TRANSACTION_TP': transaction_tp,
                                'ENTITY_TP': entity_tp,
                                'NAME': fields[7],
                                'CITY': fields[8],
                                'STATE': fields[9],
                                'ZIP_CODE': fields[10],
                                'EMPLOYER': fields[11],
                                'OCCUPATION': fields[12],
                                'TRANSACTION_DT': fields[13],
                                'TRANSACTION_AMT': float(fields[14]) if fields[14] else None,
                                'OTHER_ID': fields[15],
                                'CAND_ID': fields[16],
                                'TRAN_ID': fields[17],
                                'FILE_NUM': fields[18],
                                'MEMO_CD': fields[19],
                                'MEMO_TEXT': fields[20],
                                'SUB_ID': fields[21],
                                'updated_at': datetime.now(),
                            })
                            
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
                
                # Create indexes on key fields
                collection.create_index([("CMTE_ID", 1)])
                collection.create_index([("CAND_ID", 1)])
                collection.create_index([("OTHER_ID", 1)])
                collection.create_index([("TRANSACTION_TP", 1)])
                collection.create_index([("ENTITY_TP", 1)])
                collection.create_index([("TRANSACTION_DT", -1)])
                collection.create_index([("TRANSACTION_AMT", -1)])
                collection.create_index([("TRANSACTION_TP", 1), ("ENTITY_TP", 1)])
                
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
            "mongodb_collection": "itpas2",
        }
    )
