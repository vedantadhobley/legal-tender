"""Candidates Asset - Parse FEC candidate master files (cn.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


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
                zip_path = repo.fec_cn_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if should_restore_from_dump(cycle, "cn", zip_path):
                    context.log.info("   🚀 Restoring from dump (30 sec)...")
                    
                    if restore_collection_from_dump(
                        cycle=cycle,
                        collection="cn",
                        mongo_uri=mongo.connection_string,
                        context=context
                    ):
                        record_count = collection.count_documents({})
                        stats['by_cycle'][cycle] = record_count
                        stats['total_candidates'] += record_count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP
                context.log.info(f"   📂 Parsing {zip_path.name}...")
                collection.delete_many({})
                
                batch = []
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
                            record = schema.parse_line('cn', decoded)
                            if not record:
                                continue
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now()
                            
                            # Type conversion for election year
                            if record.get('CAND_ELECTION_YR'):
                                try:
                                    record['CAND_ELECTION_YR'] = int(record['CAND_ELECTION_YR'])
                                except (ValueError, TypeError):
                                    record['CAND_ELECTION_YR'] = None
                            
                            batch.append(record)
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} candidates")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_candidates'] += len(batch)
                    
                    # Create indexes BEFORE dump
                    collection.create_index([("CAND_ID", 1)])
                    collection.create_index([("CAND_NAME", 1)])
                    collection.create_index([("CAND_OFFICE_ST", 1), ("CAND_OFFICE_DISTRICT", 1)])
                    collection.create_index([("CAND_PTY_AFFILIATION", 1)])
                    collection.create_index([("CAND_OFFICE", 1)])
                    collection.create_index([("CAND_ELECTION_YR", 1)])
                    
                    # Create dump for next time (includes indexes)
                    create_collection_dump(
                        cycle=cycle,
                        collection="cn",
                        mongo_uri=mongo.connection_string,
                        source_file=zip_path,
                        record_count=len(batch),
                        context=context
                    )
                
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
