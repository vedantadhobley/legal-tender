"""Candidates Asset - Parse FEC candidate master files (cn.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.arango import ArangoDBResource
from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
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
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse cn.zip files and store in fec_{cycle}.cn collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_candidates': 0, 'by_cycle': {}}
    
    with arango.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                db = arango.get_database(client, f"fec_{cycle}")
                collection = arango.get_collection(db, "cn")
                zip_path = repo.fec_cn_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if not config.force_refresh and should_restore_from_dump("cn", zip_path, "fec", cycle):
                    context.log.info("   🚀 Restoring from dump...")
                    
                    result = restore_collection_from_dump(
                        db=db,
                        collection_name="cn",
                        dump_type="fec",
                        cycle=cycle,
                        context=context
                    )
                    if result:
                        record_count = result['record_count']
                        stats['by_cycle'][cycle] = record_count
                        stats['total_candidates'] += record_count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP
                context.log.info(f"   📂 Parsing {zip_path.name}...")
                
                # Clear existing data
                collection.truncate()
                
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
                            
                            # Use CAND_ID as the document _key for ArangoDB
                            if record.get('CAND_ID'):
                                record['_key'] = record['CAND_ID']
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now().isoformat()
                            
                            # Type conversion for election year
                            if record.get('CAND_ELECTION_YR'):
                                try:
                                    record['CAND_ELECTION_YR'] = int(record['CAND_ELECTION_YR'])
                                except (ValueError, TypeError):
                                    record['CAND_ELECTION_YR'] = None
                            
                            batch.append(record)
                
                if batch:
                    # Bulk import with upsert behavior
                    result = arango.bulk_import(collection, batch, on_duplicate="replace")
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} candidates (created: {result['created']}, updated: {result['updated']})")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_candidates'] += len(batch)
                    
                    # Create indexes
                    collection.add_persistent_index(fields=["CAND_ID"], unique=True)
                    collection.add_persistent_index(fields=["CAND_NAME"])
                    collection.add_persistent_index(fields=["CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT"])
                    collection.add_persistent_index(fields=["CAND_PTY_AFFILIATION"])
                    collection.add_persistent_index(fields=["CAND_OFFICE"])
                    collection.add_persistent_index(fields=["CAND_ELECTION_YR"])
                    
                    # Apply JSON schema for documentation
                    from src.utils.arango_schema import apply_fec_schemas
                    apply_fec_schemas(db, collections=["cn"], level="none")
                    
                    # Create dump for next time
                    create_collection_dump(
                        db=db,
                        collection_name="cn",
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
            "total_candidates": stats['total_candidates'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "cn",
        }
    )
