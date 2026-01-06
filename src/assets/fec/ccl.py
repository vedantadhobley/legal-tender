"""Linkages Asset - Parse FEC candidate-committee linkages (ccl.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.arango import ArangoDBResource
from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


class LinkagesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


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
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse ccl.zip files and store in fec_{cycle}.ccl collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_linkages': 0, 'by_cycle': {}}
    
    with arango.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                db = arango.get_database(client, f"fec_{cycle}")
                collection = arango.get_collection(db, "ccl")
                zip_path = repo.fec_ccl_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if not config.force_refresh and should_restore_from_dump("ccl", zip_path, "fec", cycle):
                    context.log.info("   🚀 Restoring from dump...")
                    
                    result = restore_collection_from_dump(
                        db=db,
                        collection_name="ccl",
                        dump_type="fec",
                        cycle=cycle,
                        context=context
                    )
                    if result:
                        record_count = result['record_count']
                        stats['by_cycle'][cycle] = record_count
                        stats['total_linkages'] += record_count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP
                context.log.info(f"   📂 Parsing {zip_path.name}...")
                
                # Clear existing data
                collection.truncate()
                
                batch = []
                schema = FECSchema()
                line_num = 0
                
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
                            record = schema.parse_line('ccl', decoded)
                            if not record:
                                continue
                            
                            # CCL doesn't have a unique ID, so create a composite key
                            # Format: CAND_ID_CMTE_ID_LINKAGE_ID (if available) or line number
                            cand_id = record.get('CAND_ID', '')
                            cmte_id = record.get('CMTE_ID', '')
                            linkage_id = record.get('LINKAGE_ID', str(line_num))
                            record['_key'] = f"{cand_id}_{cmte_id}_{linkage_id}"
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now().isoformat()
                            
                            batch.append(record)
                            line_num += 1
                
                if batch:
                    # Bulk import with upsert behavior
                    result = arango.bulk_import(collection, batch, on_duplicate="replace")
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} linkages")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_linkages'] += len(batch)
                    
                    # Create indexes
                    collection.add_persistent_index(fields=["CAND_ID"])
                    collection.add_persistent_index(fields=["CMTE_ID"])
                    
                    # Apply JSON schema for documentation
                    from src.utils.arango_schema import apply_fec_schemas
                    apply_fec_schemas(db, collections=["ccl"], level="none")
                    
                    # Create dump for next time
                    create_collection_dump(
                        db=db,
                        collection_name="ccl",
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
            "total_linkages": stats['total_linkages'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "ccl",
        }
    )
