"""Individual Contributions Asset - Parse FEC individual contributions (indiv.zip) using raw FEC field names

This file contains Schedule A itemized individual contributions >$200.

RAW DATA STRATEGY (November 2025):
- indiv.zip contains ~40M records per cycle (2-4GB compressed)
- We load ALL records into MongoDB raw (no filtering at parse time)
- Processing time: ~10-15 minutes to parse + dump all records
- Dump/restore: 30 seconds to reload complete dataset

What we load:
- ALL individual contributions from indiv.zip
- Complete raw dataset for flexible enrichment/aggregation

Filtering happens in ENRICHMENT layer:
- Apply $10K threshold per donor per cycle
- Aggregate donations by (NAME, EMPLOYER, CMTE_ID)
- Filter in enrichment allows changing thresholds without re-parsing

CRITICAL for Super PAC transparency:
- 44.9% of Super PACs have ZERO visible upstream funding without this
- Example: RED SENATE spent $914K, only $1K visible (need individual donors)
- Enables tracing: "MUSK, ELON → Super PAC → Candidate" influence chains
"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.mongo_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


class IndividualContributionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="indiv",
    description="FEC individual contributions file (indiv.zip) - ALL individual contributions, unfiltered raw data",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def indiv_asset(
    context: AssetExecutionContext,
    config: IndividualContributionsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse indiv.zip files and store ALL individual contributions in fec_{cycle}.indiv collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {
        'total_contributions': 0,
        'by_cycle': {}
    }
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"💰 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "indiv", database_name=f"fec_{cycle}")
                zip_path = repo.fec_indiv_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # Check if we can restore from dump
                if should_restore_from_dump(cycle, "indiv", zip_path):
                    context.log.info("   🚀 Restoring from dump (30 sec)...")
                    
                    if restore_collection_from_dump(
                        cycle=cycle,
                        collection="indiv",
                        mongo_uri=mongo.connection_string,
                        context=context
                    ):
                        record_count = collection.count_documents({})
                        
                        stats['by_cycle'][cycle] = record_count
                        stats['total_contributions'] += record_count
                        
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")
                
                # Parse from ZIP - NO FILTERING, load ALL records
                context.log.info(f"   📂 Parsing {zip_path.name} (takes ~10-15 min for ALL records)...")
                collection.delete_many({})
                
                batch = []
                total_records = 0
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
                            
                            # Parse using official FEC schema - NO FILTERING
                            record = schema.parse_line('indiv', decoded)
                            if not record:
                                continue
                            
                            total_records += 1
                            
                            # Log progress every 1M records
                            if total_records % 1000000 == 0:
                                context.log.info(f"      Parsed {total_records:,} records...")
                            
                            # Add timestamp
                            record['updated_at'] = datetime.now()
                            
                            # Type conversion for transaction amount
                            if record.get('TRANSACTION_AMT'):
                                try:
                                    record['TRANSACTION_AMT'] = float(record['TRANSACTION_AMT'])
                                except (ValueError, TypeError):
                                    record['TRANSACTION_AMT'] = None
                            
                            batch.append(record)
                            
                            # Batch insert for performance
                            if len(batch) >= 10000:
                                collection.insert_many(batch, ordered=False)
                                batch = []
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch, ordered=False)
                
                context.log.info(f"   ✅ {cycle}: {total_records:,} records loaded (complete raw dataset)")
                
                stats['by_cycle'][cycle] = total_records
                stats['total_contributions'] += total_records
                
                # Create indexes BEFORE dump so they're included in the backup
                context.log.info(f"🔧 Creating indexes for {total_records:,} records...")
                collection.create_index([("CMTE_ID", 1)])
                collection.create_index([("NAME", 1)])
                collection.create_index([("EMPLOYER", 1)])
                collection.create_index([("ENTITY_TP", 1)])
                collection.create_index([("TRANSACTION_AMT", -1)])
                collection.create_index([("TRANSACTION_DT", -1)])
                collection.create_index([("NAME", 1), ("EMPLOYER", 1)])
                context.log.info("✅ Indexes created")
                
                # Create dump for next time (includes indexes)
                create_collection_dump(
                    cycle=cycle,
                    collection="indiv",
                    mongo_uri=mongo.connection_string,
                    source_file=zip_path,
                    record_count=total_records,
                    context=context
                )
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_contributions": stats['total_contributions'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "indiv",
        }
    )
