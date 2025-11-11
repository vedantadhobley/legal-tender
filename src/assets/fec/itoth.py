"""Other Receipts Asset - Parse FEC other receipts (oth.zip) using raw FEC field names

This file contains Schedule A receipts that don't fit other categories:
- Committee-to-committee transfers (PAC → PAC, PAC → Party, etc.)
- Corporate/union contributions to PACs
- Refunds, interest, and other miscellaneous receipts

CRITICAL for upstream tracing: Shows WHO gave money TO committees (CMTE_ID = recipient)
"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class OtherReceiptsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="itoth",
    description="FEC other receipts file (oth.zip) - Committee receipts including PAC-to-PAC transfers",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def itoth_asset(
    context: AssetExecutionContext,
    config: OtherReceiptsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse oth.zip files and store in fec_{cycle}.itoth collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_receipts': 0, 'by_cycle': {}, 'by_form_type': {}, 'by_entity_type': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "itoth", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_oth_path(cycle)
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                batch = []
                cycle_form_types = {}
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
                            if len(fields) < 21:
                                continue
                            
                            # Track form and entity types
                            form_tp = fields[5]  # Form type (e.g., 10J, 11AI, etc.)
                            entity_tp = fields[6]
                            cycle_form_types[form_tp] = cycle_form_types.get(form_tp, 0) + 1
                            cycle_entity_types[entity_tp] = cycle_entity_types.get(entity_tp, 0) + 1
                            
                            # Parse oth fields - 21 total fields
                            # Based on FEC oth structure (similar to itpas2 but missing CAND_ID)
                            batch.append({
                                'CMTE_ID': fields[0],          # Filing committee (RECIPIENT of money)
                                'AMNDT_IND': fields[1],         # Amendment indicator
                                'RPT_TP': fields[2],            # Report type
                                'TRANSACTION_PGI': fields[3],   # Primary/General indicator
                                'IMAGE_NUM': fields[4],         # Microfilm location
                                'FORM_TP': form_tp,             # Form type (10J, 11AI, etc.)
                                'ENTITY_TP': entity_tp,         # Entity type (ORG, PAC, COM, PTY)
                                'NAME': fields[7],              # Contributor name
                                'CITY': fields[8],              # Contributor city
                                'STATE': fields[9],             # Contributor state
                                'ZIP_CODE': fields[10],         # Contributor zip
                                'EMPLOYER': fields[11],         # Employer
                                'OCCUPATION': fields[12],       # Occupation
                                'TRANSACTION_DT': fields[13],   # Transaction date
                                'TRANSACTION_AMT': float(fields[14]) if fields[14] else None,
                                'OTHER_ID': fields[15],         # Other committee ID (donor if transfer)
                                'TRAN_ID': fields[16],          # Transaction ID
                                'FILE_NUM': fields[17],         # File number
                                'MEMO_CD': fields[18],          # Memo code
                                'MEMO_TEXT': fields[19],        # Memo text
                                'SUB_ID': fields[20],           # Submission ID
                                'updated_at': datetime.now(),
                            })
                            
                            # Batch insert for performance
                            if len(batch) >= 10000:
                                collection.insert_many(batch, ordered=False)
                                batch = []
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch, ordered=False)
                
                total_cycle = sum(cycle_form_types.values())
                context.log.info(f"   ✅ {cycle}: {total_cycle:,} receipts")
                context.log.info(f"      Form types: {dict(sorted(cycle_form_types.items(), key=lambda x: x[1], reverse=True)[:5])}")
                context.log.info(f"      Entity types: {dict(sorted(cycle_entity_types.items(), key=lambda x: x[1], reverse=True)[:5])}")
                
                stats['by_cycle'][cycle] = {
                    'total': total_cycle,
                    'form_types': cycle_form_types,
                    'entity_types': cycle_entity_types
                }
                stats['total_receipts'] += total_cycle
                
                # Merge into global stats
                for ft, count in cycle_form_types.items():
                    stats['by_form_type'][ft] = stats['by_form_type'].get(ft, 0) + count
                for et, count in cycle_entity_types.items():
                    stats['by_entity_type'][et] = stats['by_entity_type'].get(et, 0) + count
                
                # Create indexes on key fields
                collection.create_index([("CMTE_ID", 1)])  # CRITICAL: Recipient committee
                collection.create_index([("OTHER_ID", 1)])  # Donor committee (if transfer)
                collection.create_index([("FORM_TP", 1)])
                collection.create_index([("ENTITY_TP", 1)])
                collection.create_index([("TRANSACTION_DT", -1)])
                collection.create_index([("TRANSACTION_AMT", -1)])
                collection.create_index([("CMTE_ID", 1), ("ENTITY_TP", 1)])
                collection.create_index([("CMTE_ID", 1), ("OTHER_ID", 1)])  # For committee-to-committee lookups
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_receipts": stats['total_receipts'],
            "form_types": MetadataValue.json(dict(sorted(stats['by_form_type'].items(), key=lambda x: x[1], reverse=True)[:10])),
            "entity_types": MetadataValue.json(dict(sorted(stats['by_entity_type'].items(), key=lambda x: x[1], reverse=True))),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "itoth",
        }
    )
