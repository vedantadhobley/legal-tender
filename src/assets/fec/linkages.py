"""Linkages Asset - Parse FEC candidate-committee linkages (ccl.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class LinkagesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="linkages",
    description="FEC candidate-committee linkages - maps candidates to their authorized committees",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def linkages_asset(
    context: AssetExecutionContext,
    config: LinkagesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse ccl.zip files and store in fec_{cycle}.linkages collections."""
    
    repo = get_repository()
    stats = {'total_linkages': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "linkages", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_linkages_path(cycle)
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
                            if len(fields) < 7:
                                continue
                            
                            cand_id = fields[0]
                            cmte_id = fields[3]
                            linkage_id = fields[6]
                            
                            batch.append({
                                '_id': linkage_id,  # FEC's unique linkage ID
                                'candidate_id': cand_id,
                                'candidate_election_year': fields[1],
                                'fec_election_year': fields[2],
                                'committee_id': cmte_id,
                                'committee_type': fields[4],
                                'committee_designation': fields[5],
                                'linkage_id': linkage_id,
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} linkages")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_linkages'] += len(batch)
                
                collection.create_index([("candidate_id", 1)])
                collection.create_index([("committee_id", 1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_linkages": stats['total_linkages'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "linkages",
        }
    )
