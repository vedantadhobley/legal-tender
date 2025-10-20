"""Individual Contributions Asset

Parse and store individual contributions (indiv.zip) - direct donations from people to candidate committees.
This is the foundation for corporate influence analysis (aggregate by employer).
"""

from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    Config,
    AssetIn,
)

from src.data import get_repository
from src.api.fec_bulk_data import parse_indiv_file, download_fec_file
from src.resources.mongo import MongoDBResource
from src.utils.memory import get_recommended_batch_size


class IndividualContributionsConfig(Config):
    """Configuration for individual contributions processing."""
    
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    """Election cycles to process (8 years of data).
    
    Override in Dagster UI to test specific cycles:
    - ["2024"] - Single cycle for testing
    - ["2024", "2026"] - Two cycles
    - ["2020", "2022", "2024", "2026"] - All four cycles (default, full 8 years)
    """
    
    max_contributors_per_member: int = 100
    """Maximum number of top contributors to store per member"""
    
    batch_size: int | None = None
    """Number of records to process in each batch.
    
    If None (default), will automatically calculate based on available memory.
    
    Manual batch sizes by available RAM:
    - 1-2 GB free:  1,000-2,000 (very conservative, cloud instances)
    - 4 GB free:    10,000 (moderate, small cloud instances)
    - 8 GB free:    25,000 (comfortable, medium cloud instances)
    - 16 GB free:   50,000 (fast, local development or large instances)
    - 32 GB+ free:  100,000 (very fast, workstations/servers)
    """


@asset(
    name="individual_contributions",
    description="Individual contributions from people to candidate committees (direct donations with $3,300 limit per election)",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
    metadata={
        "source": "FEC bulk data (indiv files)",
        "file_size": "2-4 GB per cycle",
        "contains": "Name, employer, occupation, amount, date, committee",
    },
)
def individual_contributions_asset(
    context: AssetExecutionContext,
    config: IndividualContributionsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse and store individual contributions.
    
    These are direct donations from individuals to candidate committees.
    Key fields:
    - NAME: Donor name
    - EMPLOYER: Donor's employer (for corporate influence analysis)
    - OCCUPATION: Donor's job
    - TRANSACTION_AMT: Amount donated ($3,300 limit per election)
    - CMTE_ID: Committee that received donation
    
    Args:
        config: Configuration
        mongo: MongoDB resource
        data_sync: Data sync results (ensures files are downloaded)
        
    Returns:
        Summary statistics
    """
    context.log.info("=" * 80)
    context.log.info("👤 PROCESSING INDIVIDUAL CONTRIBUTIONS")
    context.log.info("=" * 80)
    
    # Determine batch size (auto-detect if not specified)
    if config.batch_size is None:
        batch_size, explanation = get_recommended_batch_size()
        context.log.info(f"🧠 Auto-detected batch size: {explanation}")
    else:
        batch_size = config.batch_size
        context.log.info(f"📝 Using configured batch size: {batch_size:,}")
    
    repo = get_repository()
    
    stats = {
        'total_contributions': 0,
        'by_cycle': {},
        'unique_donors': 0,
        'unique_employers': 0,
        'total_amount': 0.0,
    }
    
    # ==========================================================================
    # STREAMING APPROACH: Process each cycle incrementally
    # Parse → Process → Write to MongoDB in batches
    # Never load full file into memory!
    # ==========================================================================
    
    context.log.info("\n📥 STREAMING INDIVIDUAL CONTRIBUTIONS (Batch Processing)")
    context.log.info(f"   Batch size: {batch_size:,} records per insert")
    context.log.info("-" * 80)
    
    # Initialize MongoDB
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "individual_contributions")
        
        # Clear existing data
        collection.delete_many({})
        context.log.info("🗑️  Cleared existing contributions\n")
        
        # Process each cycle independently
        # Note: We track unique counts via MongoDB queries post-processing to save memory
        # during the streaming phase (sets can consume 100MB+ for millions of unique values)
        total_amount = 0.0
        total_contributions = 0
        
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                zip_path = repo.fec_individual_contributions_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    context.log.warning("   Run data_sync with sync_individual_contributions=True first\n")
                    continue
                
                # Stream process this cycle
                context.log.info(f"   📖 Streaming {zip_path.name}...")
                
                import zipfile
                batch = []
                cycle_count = 0
                cycle_amount = 0.0
                lines_read = 0
                
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                    
                    if not txt_files:
                        context.log.warning(f"   ❌ No .txt files in {zip_path.name}\n")
                        continue
                    
                    txt_file = txt_files[0]
                    
                    with zf.open(txt_file) as f:
                        for line in f:
                            lines_read += 1
                            
                            try:
                                # Decode and parse
                                decoded = line.decode('utf-8', errors='ignore').strip()
                                if not decoded:
                                    continue
                                
                                fields = decoded.split('|')
                                if len(fields) < 21:
                                    continue
                                
                                # Parse amount
                                try:
                                    amount = float(fields[14]) if fields[14] else 0.0
                                except:
                                    amount = 0.0
                                
                                # Clean fields
                                name = (fields[7] or '').strip().upper()
                                employer = (fields[11] or 'NOT PROVIDED').strip().upper()
                                occupation = (fields[12] or 'NOT PROVIDED').strip().upper()
                                
                                # Don't track unique donors/employers in memory during streaming
                                # We'll calculate these via MongoDB aggregation after data is loaded
                                # This saves 100MB+ of memory for sets with millions of unique values
                                
                                cycle_amount += amount
                                total_amount += amount
                                
                                # Build record
                                record = {
                                    '_id': fields[20],  # SUB_ID
                                    'cycle': cycle,
                                    'donor_name': name,
                                    'employer': employer,
                                    'occupation': occupation,
                                    'amount': round(amount, 2),
                                    'date': fields[13],
                                    'committee_id': fields[0],
                                    'city': (fields[8] or '').strip().upper(),
                                    'state': fields[9],
                                    'zip_code': fields[10],
                                    'transaction_type': fields[5],
                                    'memo_text': fields[19],
                                    'updated_at': datetime.now(),
                                }
                                
                                batch.append(record)
                                
                                # Write batch when full
                                if len(batch) >= batch_size:
                                    try:
                                        collection.insert_many(batch, ordered=False)
                                        cycle_count += len(batch)
                                        total_contributions += len(batch)
                                        context.log.info(f"   💾 Batch inserted: {cycle_count:,} records so far...")
                                    except Exception as e:
                                        context.log.warning(f"   ⚠️  Batch insert error (skipping duplicates): {str(e)[:100]}")
                                        # Try inserting one by one to skip duplicates
                                        for rec in batch:
                                            try:
                                                collection.insert_one(rec)
                                                cycle_count += 1
                                                total_contributions += 1
                                            except:
                                                pass  # Skip duplicate
                                    batch = []  # Clear memory!
                                
                            except Exception as e:
                                continue  # Skip bad lines
                            
                            # Progress every 100K lines
                            if lines_read % 100000 == 0:
                                context.log.info(f"   � Read {lines_read:,} lines, {cycle_count:,} valid records...")
                    
                    # Insert final batch
                    if batch:
                        try:
                            collection.insert_many(batch, ordered=False)
                            cycle_count += len(batch)
                            total_contributions += len(batch)
                        except Exception as e:
                            context.log.warning(f"   ⚠️  Final batch insert error (skipping duplicates): {str(e)[:100]}")
                            for rec in batch:
                                try:
                                    collection.insert_one(rec)
                                    cycle_count += 1
                                    total_contributions += 1
                                except:
                                    pass
                        batch = []  # Clear memory!
                
                context.log.info(f"   ✅ {cycle}: {cycle_count:,} contributions, ${cycle_amount:,.2f}\n")
                
                stats['by_cycle'][cycle] = {
                    'count': cycle_count,
                    'amount': round(cycle_amount, 2),
                }
            
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}\n")
                continue
        
        # Create indexes once after all data loaded
        context.log.info("📇 Creating indexes...")
        collection.create_index([("employer", 1)])
        collection.create_index([("committee_id", 1)])
        collection.create_index([("cycle", 1)])
        collection.create_index([("donor_name", 1)])
        collection.create_index([("amount", -1)])
        context.log.info("✅ Indexes created")
        
        # Calculate unique counts via MongoDB aggregation (memory-efficient)
        context.log.info("\n📊 Calculating unique counts...")
        unique_donors = collection.distinct("donor_name")
        unique_employers = collection.distinct("employer")
        context.log.info(f"   ✅ Unique donors: {len(unique_donors):,}")
        context.log.info(f"   ✅ Unique employers: {len(unique_employers):,}")
    
    # ==========================================================================
    # Summary
    # ==========================================================================
    
    context.log.info("\n" + "=" * 80)
    context.log.info("🎉 INDIVIDUAL CONTRIBUTIONS COMPLETE!")
    context.log.info("=" * 80)
    context.log.info(f"   Total: {total_contributions:,} contributions")
    context.log.info(f"   Unique donors: {len(unique_donors):,}")
    context.log.info(f"   Unique employers: {len(unique_employers):,}")
    context.log.info(f"   Total amount: ${total_amount:,.2f}")
    
    stats['total_contributions'] = total_contributions
    stats['unique_donors'] = len(unique_donors)
    stats['unique_employers'] = len(unique_employers)
    stats['total_amount'] = round(total_amount, 2)
    
    return Output(
        value=stats,
        metadata={
            "total_contributions": total_contributions,
            "unique_donors": len(unique_donors),
            "unique_employers": len(unique_employers),
            "total_amount_usd": MetadataValue.float(total_amount),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_collection": "individual_contributions",
        }
    )
