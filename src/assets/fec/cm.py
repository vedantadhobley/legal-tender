"""Committees Asset - Parse FEC committee master files (cm.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.arango import ArangoDBResource
from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


class CommitteesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


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
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse cm.zip files and store in fec_{cycle}.cm collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_committees': 0, 'leadership_pacs': 0, 'by_cycle': {}}
    
    with arango.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                db = arango.get_database(client, f"fec_{cycle}")
                collection = arango.get_collection(db, "cm")
                zip_path = repo.fec_cm_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if not config.force_refresh and should_restore_from_dump("cm", zip_path, "fec", cycle):
                    context.log.info("   🚀 Restoring from dump...")
                    
                    result = restore_collection_from_dump(
                        db=db,
                        collection_name="cm",
                        dump_type="fec",
                        cycle=cycle,
                        context=context
                    )
                    if result:
                        record_count = result['record_count']
                        # Count leadership PACs after restore
                        cursor = db.aql.execute('FOR doc IN cm FILTER doc.CMTE_TP == "O" RETURN 1')
                        leadership_count = sum(1 for _ in cursor)
                        stats['by_cycle'][cycle] = {'total': record_count, 'leadership_pacs': leadership_count}
                        stats['total_committees'] += record_count
                        stats['leadership_pacs'] += leadership_count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP
                context.log.info(f"   📂 Parsing {zip_path.name}...")
                
                # Clear existing data
                collection.truncate()
                
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
                            
                            # Use CMTE_ID as the document _key for ArangoDB
                            if record.get('CMTE_ID'):
                                record['_key'] = record['CMTE_ID']
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now().isoformat()
                            
                            # Track leadership PACs
                            if record.get('CMTE_TP') == 'O':
                                leadership_count += 1
                            
                            batch.append(record)
                
                if batch:
                    # Bulk import with upsert behavior
                    result = arango.bulk_import(collection, batch, on_duplicate="replace")
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} committees ({leadership_count} Leadership PACs)")
                    stats['by_cycle'][cycle] = {'total': len(batch), 'leadership_pacs': leadership_count}
                    stats['total_committees'] += len(batch)
                    stats['leadership_pacs'] += leadership_count
                    
                    # Create indexes
                    collection.add_persistent_index(fields=["CMTE_ID"], unique=True)
                    collection.add_persistent_index(fields=["CMTE_TP"])
                    collection.add_persistent_index(fields=["CONNECTED_ORG_NM"])
                    collection.add_persistent_index(fields=["CMTE_NM"])
                    
                    # Apply JSON schema for documentation
                    from src.utils.arango_schema import apply_fec_schemas
                    apply_fec_schemas(db, collections=["cm"], level="none")
                    
                    # Create dump for next time
                    create_collection_dump(
                        db=db,
                        collection_name="cm",
                        source_file=zip_path,
                        dump_type="fec",
                        cycle=cycle,
                        context=context
                    )
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
                import traceback
                context.log.error(traceback.format_exc())
    
    return Output(
        value=stats,
        metadata={
            "total_committees": stats['total_committees'],
            "leadership_pacs": stats['leadership_pacs'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "cm",
        }
    )
