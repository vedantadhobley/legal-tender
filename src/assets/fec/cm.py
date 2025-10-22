"""Committees Asset - Parse FEC committee master files (cm.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class CommitteesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="cm",
    description="FEC committee master file (cm.zip) - raw FEC data with original field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def cm_asset(
    context: AssetExecutionContext,
    config: CommitteesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse cm.zip files and store in fec_{cycle}.cm collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_committees': 0, 'leadership_pacs': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "cm", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_cm_path(cycle)
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                batch = []
                leadership_count = 0
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
                            if len(fields) < 15:
                                continue
                            
                            cmte_tp = fields[9]  # Committee type
                            
                            if cmte_tp == 'O':
                                leadership_count += 1
                            
                            # Use EXACT field names from fec.md
                            batch.append({

                                'CMTE_ID': fields[0],
                                'CMTE_NM': fields[1],
                                'TRES_NM': fields[2],
                                'CMTE_ST1': fields[3],
                                'CMTE_ST2': fields[4],
                                'CMTE_CITY': fields[5],
                                'CMTE_ST': fields[6],
                                'CMTE_ZIP': fields[7],
                                'CMTE_DSGN': fields[8],
                                'CMTE_TP': cmte_tp,
                                'CMTE_PTY_AFFILIATION': fields[10],
                                'CMTE_FILING_FREQ': fields[11],
                                'ORG_TP': fields[12],
                                'CONNECTED_ORG_NM': fields[13],
                                'CAND_ID': fields[14],
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} committees ({leadership_count} Leadership PACs)")
                    stats['by_cycle'][cycle] = {'total': len(batch), 'leadership_pacs': leadership_count}
                    stats['total_committees'] += len(batch)
                    stats['leadership_pacs'] += leadership_count
                
                # Create indexes on key fields
                collection.create_index([("CMTE_TP", 1)])
                collection.create_index([("CONNECTED_ORG_NM", 1)])
                collection.create_index([("CMTE_NM", 1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_committees": stats['total_committees'],
            "leadership_pacs": stats['leadership_pacs'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "cm",
        }
    )
