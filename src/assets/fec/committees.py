"""Committees Asset - Parse FEC committee master files (cm.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class CommitteesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="committees",
    description="FEC committee master file - identifies Leadership PACs (type='O') and all committees",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def committees_asset(
    context: AssetExecutionContext,
    config: CommitteesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse cm.zip files and store in fec_{cycle}.committees collections."""
    
    repo = get_repository()
    stats = {'total_committees': 0, 'leadership_pacs': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "committees", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_committees_path(cycle)
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
                            
                            cmte_id = fields[0]
                            cmte_type = fields[9]  # FIXED: was fields[5] (city!)
                            
                            if cmte_type == 'O':
                                leadership_count += 1
                            
                            batch.append({
                                '_id': cmte_id,
                                'committee_id': cmte_id,  # Keep original FEC field
                                'name': fields[1],
                                'treasurer_name': fields[2],
                                'street_1': fields[3],
                                'street_2': fields[4],
                                'city': fields[5],
                                'state': fields[6],
                                'zip': fields[7],
                                'designation': fields[8],  # P, A, B, D, J, U
                                'type': cmte_type,  # O=Leadership PAC, Q=PAC, etc.
                                'party': fields[10],
                                'filing_frequency': fields[11],
                                'interest_group': fields[12],
                                'connected_org_name': fields[13],
                                'candidate_id': fields[14],
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} committees ({leadership_count} Leadership PACs)")
                    stats['by_cycle'][cycle] = {'total': len(batch), 'leadership_pacs': leadership_count}
                    stats['total_committees'] += len(batch)
                    stats['leadership_pacs'] += leadership_count
                
                collection.create_index([("type", 1)])
                collection.create_index([("connected_org_name", 1)])
                collection.create_index([("name", 1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_committees": stats['total_committees'],
            "leadership_pacs": stats['leadership_pacs'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "committees",
        }
    )
