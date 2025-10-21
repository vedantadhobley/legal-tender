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
    name="candidates",
    description="FEC candidate master file - maps candidate IDs to names, offices, states",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def candidates_asset(
    context: AssetExecutionContext,
    config: CandidatesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse cn.zip files and store in fec_{cycle}.candidates collections."""
    
    repo = get_repository()
    stats = {'total_candidates': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "candidates", database_name=f"fec_{cycle}")
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
                            if len(fields) < 15:
                                continue
                            
                            cand_id = fields[0]
                            batch.append({
                                '_id': cand_id,
                                'candidate_id': cand_id,  # Keep original FEC field
                                'name': fields[1],
                                'party': fields[2],  # REP, DEM, IND, etc.
                                'election_year': fields[3],
                                'state': fields[4],  # AL, TX, etc.
                                'office': fields[5],  # H, S, P
                                'district': fields[6],  # 01, 02, 00 for statewide
                                'incumbent_challenger_status': fields[7],  # I, C, O
                                'candidate_status': fields[8],  # C, N, F
                                'principal_campaign_committee': fields[9],
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} candidates")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_candidates'] += len(batch)
                
                collection.create_index([("name", 1)])
                collection.create_index([("state", 1), ("district", 1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_candidates": stats['total_candidates'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "candidates",
        }
    )
