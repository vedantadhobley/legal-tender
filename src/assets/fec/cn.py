"""Candidates Asset - Parse FEC candidate master files (cn.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class CandidatesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


@asset(
    name="cn",
    description="FEC candidate master file (cn.zip) - raw FEC data with original field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def cn_asset(
    context: AssetExecutionContext,
    config: CandidatesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse cn.zip files and store in fec_{cycle}.cn collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_candidates': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "cn", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_candidates_path(cycle)
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                batch = []
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
                            if len(fields) < 15:  # cn.txt has 15 fields per FEC.md
                                continue
                            
                            # Use EXACT field names from fec.md (15 fields - basic candidate registration)
                            batch.append({
                                'CAND_ID': fields[0],
                                'CAND_NAME': fields[1],
                                'CAND_PTY_AFFILIATION': fields[2],
                                'CAND_ELECTION_YR': int(fields[3]) if fields[3] else None,
                                'CAND_OFFICE_ST': fields[4],
                                'CAND_OFFICE': fields[5],
                                'CAND_OFFICE_DISTRICT': fields[6],
                                'CAND_ICI': fields[7],
                                'CAND_STATUS': fields[8],
                                'CAND_PCC': fields[9],
                                'CAND_ST1': fields[10],
                                'CAND_ST2': fields[11],
                                'CAND_CITY': fields[12],
                                'CAND_ST': fields[13],
                                'CAND_ZIP': fields[14],
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} candidates")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_candidates'] += len(batch)
                
                # Create indexes on key fields
                collection.create_index([("CAND_ID", 1)])
                collection.create_index([("CAND_NAME", 1)])
                collection.create_index([("CAND_OFFICE_ST", 1), ("CAND_OFFICE_DISTRICT", 1)])
                collection.create_index([("CAND_PTY_AFFILIATION", 1)])
                collection.create_index([("CAND_OFFICE", 1)])
                collection.create_index([("CAND_ELECTION_YR", 1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_candidates": stats['total_candidates'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "cn",
        }
    )
