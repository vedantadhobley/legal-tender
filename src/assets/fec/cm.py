"""Committees Asset - Parse FEC committee master files (cm.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


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
                zip_path = repo.fec_cm_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if should_restore_from_dump(cycle, "cm", zip_path):
                    context.log.info("   🚀 Restoring from dump (30 sec)...")
                    
                    if restore_collection_from_dump(
                        cycle=cycle,
                        collection="cm",
                        mongo_uri=mongo.connection_string,
                        context=context
                    ):
                        record_count = collection.count_documents({})
                        leadership_count = collection.count_documents({"CMTE_TP": "O"})
                        stats['by_cycle'][cycle] = {'total': record_count, 'leadership_pacs': leadership_count}
                        stats['total_committees'] += record_count
                        stats['leadership_pacs'] += leadership_count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP
                context.log.info(f"   📂 Parsing {zip_path.name}...")
                collection.delete_many({})
                
                batch = []
                leadership_count = 0
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
                            record = schema.parse_line('cm', decoded)
                            if not record:
                                continue
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now()
                            
                            # Track leadership PACs
                            if record.get('CMTE_TP') == 'O':
                                leadership_count += 1
                            
                            batch.append(record)
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} committees ({leadership_count} Leadership PACs)")
                    stats['by_cycle'][cycle] = {'total': len(batch), 'leadership_pacs': leadership_count}
                    stats['total_committees'] += len(batch)
                    stats['leadership_pacs'] += leadership_count
                    
                    # Create indexes BEFORE dump
                    collection.create_index([("CMTE_TP", 1)])
                    collection.create_index([("CONNECTED_ORG_NM", 1)])
                    collection.create_index([("CMTE_NM", 1)])
                    
                    # Create dump for next time (includes indexes)
                    create_collection_dump(
                        cycle=cycle,
                        collection="cm",
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
            "total_committees": stats['total_committees'],
            "leadership_pacs": stats['leadership_pacs'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "cm",
        }
    )
