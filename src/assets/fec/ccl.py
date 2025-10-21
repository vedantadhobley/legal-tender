"""Linkages Asset - Parse FEC candidate-committee linkages (ccl.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class LinkagesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="ccl",
    description="FEC candidate-committee linkages (ccl.zip) - raw FEC data with original field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def ccl_asset(
    context: AssetExecutionContext,
    config: LinkagesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse ccl.zip files and store in fec_{cycle}.ccl collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_linkages': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "ccl", database_name=f"fec_{cycle}")
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
                            
                            # Use EXACT field names from fec.md
                            batch.append({

                                'CAND_ID': fields[0],
                                'CAND_ELECTION_YR': fields[1],
                                'FEC_ELECTION_YR': fields[2],
                                'CMTE_ID': fields[3],
                                'CMTE_TP': fields[4],
                                'CMTE_DSGN': fields[5],
                                'LINKAGE_ID': fields[6],
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} linkages")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_linkages'] += len(batch)
                
                # Create indexes on key fields
                collection.create_index([("CAND_ID", 1)])
                collection.create_index([("CMTE_ID", 1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_linkages": stats['total_linkages'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "ccl",
        }
    )
