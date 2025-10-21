"""Committee Transfers Asset

Parse and store committee-to-committee transfers (pas2.zip) - Money flowing between PACs.
This is CRITICAL for Leadership PAC tracking and detecting corporate money laundering.

Key Use Cases:
1. Corporate PAC → Leadership PAC → Politician (hidden influence)
2. Corporate PAC → Candidate Committee (direct influence)
3. Leadership PAC → Multiple Politicians (distribution analysis)
"""

from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path
import zipfile

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    Config,
    AssetIn,
)

from src.data import get_repository
from src.api.fec_bulk_data import download_fec_file
from src.resources.mongo import MongoDBResource
from src.utils.memory import get_recommended_batch_size


class CommitteeTransfersConfig(Config):
    """Configuration for committee transfers asset."""
    
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    """Election cycles to process (8 years of data).
    
    Override in Dagster UI to test specific cycles:
    - ["2024"] - Single cycle for testing
    - ["2024", "2026"] - Two cycles
    - ["2020", "2022", "2024", "2026"] - All four cycles (default, full 8 years)
    """
    
    force_refresh: bool = False
    batch_size: int | None = None  # Auto-detect if None


@asset(
    name="committee_transfers",
    description="Committee-to-committee transfers (Corporate PAC → Leadership PAC → Politicians)",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
    metadata={
        "source": "FEC bulk data (pas2 files)",
        "file_size": "~300 MB per cycle",
        "contains": "From committee, to committee, amount, date, transaction type",
        "critical_for": "Leadership PAC money laundering detection",
    },
)
def committee_transfers_asset(
    context: AssetExecutionContext,
    config: CommitteeTransfersConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse and store committee transfers.
    
    These are transfers between committees (PACs, Leadership PACs, candidate committees).
    CRITICAL for tracking Leadership PAC money laundering.
    
    Key fields:
    - OTHER_ID (field 15): Committee SENDING the transfer (FROM)
    - CMTE_ID (field 0): Committee RECEIVING the transfer (TO)
    - TRANSACTION_AMT (field 14): Amount transferred
    - TRANSACTION_DT (field 13): Date (MMDDYYYY)
    - NAME (field 7): Name of sending committee
    
    Example Flow:
    1. Lockheed Martin PAC → Pelosi Leadership PAC ($50K)
    2. Pelosi Leadership PAC → 10 Politicians ($10K each)
    3. We calculate: Each politician got 10% Lockheed influence
    
    Args:
        config: Configuration
        mongo: MongoDB resource
        data_sync: Data sync results (ensures files are downloaded)
        
    Returns:
        Summary statistics
    """
    context.log.info("=" * 80)
    context.log.info("🔄 PROCESSING COMMITTEE TRANSFERS (Leadership PAC Tracking)")
    context.log.info("=" * 80)
    context.log.info("💡 This data enables upstream influence calculation")
    
    # Determine batch size (auto-detect if not specified)
    if config.batch_size is None:
        batch_size, explanation = get_recommended_batch_size()
        context.log.info(f"🧠 Auto-detected batch size: {explanation}")
    else:
        batch_size = config.batch_size
        context.log.info(f"📝 Using configured batch size: {batch_size:,}")
    
    repo = get_repository()
    
    stats = {
        'total_transfers': 0,
        'by_cycle': {},
        'total_amount': 0.0,
    }
    
    # ==========================================================================
    # STREAMING APPROACH: Process each cycle incrementally
    # Parse → Process → Write to MongoDB in batches
    # Never load full file into memory!
    # ==========================================================================
    
    context.log.info("\n📥 STREAMING COMMITTEE TRANSFERS (Batch Processing)")
    context.log.info(f"   Batch size: {batch_size:,} records per insert")
    context.log.info("-" * 80)
    
    # Initialize MongoDB - use fec database for raw data
    with mongo.get_client() as client:
        
        total_transfers = 0
        total_amount = 0.0
        
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                zip_path = repo.fec_committee_transfers_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    context.log.warning("   Run data_sync first to download pas2 files\n")
                    continue
                
                # Get collection for this cycle - each cycle in its own database
                collection = mongo.get_collection(
                    client, 
                    "committee_transfers",
                    database_name=f"fec_{cycle}"
                )
                
                # Clear existing data for this cycle
                collection.delete_many({})
                context.log.info(f"   🗑️  Cleared existing {cycle} transfers")
                
                # Stream process this cycle
                context.log.info(f"   📖 Streaming {zip_path.name}...")
                
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
                                if len(fields) < 22:  # pas2 has 22 fields
                                    continue
                                
                                # Parse amount
                                try:
                                    amount = float(fields[14]) if fields[14] else 0.0
                                except:
                                    amount = 0.0
                                
                                cycle_amount += amount
                                total_amount += amount
                                
                                # Build record
                                record = {
                                    '_id': fields[21],  # SUB_ID (unique identifier)
                                    'sub_id': fields[21],  # Keep original FEC field
                                    'cycle': cycle,
                                    'from_committee_id': fields[15],  # OTHER_ID (sender)
                                    'from_committee_name': (fields[7] or '').strip().upper(),  # NAME
                                    'to_committee_id': fields[0],  # CMTE_ID (receiver)
                                    'amount': round(amount, 2),
                                    'date': fields[13],  # TRANSACTION_DT
                                    'transaction_type': fields[5],  # TRANSACTION_TP
                                    'entity_type': fields[6],  # ENTITY_TP
                                    'candidate_id': fields[16],  # CAND_ID (if applicable)
                                    'city': (fields[8] or '').strip(),
                                    'state': fields[9],
                                    'zip_code': fields[10],
                                    'memo_text': fields[20],
                                    'updated_at': datetime.now(),
                                }
                                
                                batch.append(record)
                                
                                # Write batch when full
                                if len(batch) >= batch_size:
                                    try:
                                        collection.insert_many(batch, ordered=False)
                                        cycle_count += len(batch)
                                        total_transfers += len(batch)
                                        context.log.info(f"   💾 Batch inserted: {cycle_count:,} records so far...")
                                    except Exception as e:
                                        context.log.warning(f"   ⚠️  Batch insert error (skipping duplicates): {str(e)[:100]}")
                                        # Try inserting one by one
                                        for rec in batch:
                                            try:
                                                collection.insert_one(rec)
                                                cycle_count += 1
                                                total_transfers += 1
                                            except:
                                                pass
                                    batch = []  # Clear memory!
                                
                            except Exception as e:
                                continue  # Skip bad lines
                            
                            # Progress every 100K lines
                            if lines_read % 100000 == 0:
                                context.log.info(f"   📄 Read {lines_read:,} lines, {cycle_count:,} valid records...")
                    
                    # Insert final batch
                    if batch:
                        try:
                            collection.insert_many(batch, ordered=False)
                            cycle_count += len(batch)
                            total_transfers += len(batch)
                        except Exception as e:
                            context.log.warning(f"   ⚠️  Final batch insert error (skipping duplicates): {str(e)[:100]}")
                            for rec in batch:
                                try:
                                    collection.insert_one(rec)
                                    cycle_count += 1
                                    total_transfers += 1
                                except:
                                    pass
                        batch = []  # Clear memory!
                
                # Create indexes for this cycle
                context.log.info(f"   📇 Creating indexes for {cycle}...")
                collection.create_index([("from_committee_id", 1)])
                collection.create_index([("to_committee_id", 1)])
                collection.create_index([("amount", -1)])
                collection.create_index([("date", -1)])
                
                context.log.info(f"   ✅ {cycle}: {cycle_count:,} transfers, ${cycle_amount:,.2f}\n")
                
                stats['by_cycle'][cycle] = {
                    'count': cycle_count,
                    'amount': round(cycle_amount, 2),
                }
            
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}\n")
                continue
    
    # ==========================================================================
    # Summary
    # ==========================================================================
    
    context.log.info("\n" + "=" * 80)
    context.log.info("🎉 COMMITTEE TRANSFERS COMPLETE!")
    context.log.info("=" * 80)
    context.log.info(f"   Total: {total_transfers:,} transfers")
    context.log.info(f"   Total amount: ${total_amount:,.2f}")
    context.log.info("\n💡 Next Step: Build donors aggregation asset to calculate upstream influence")
    
    stats['total_transfers'] = total_transfers
    stats['total_amount'] = round(total_amount, 2)
    
    return Output(
        value=stats,
        metadata={
            "total_transfers": total_transfers,
            "total_amount_usd": MetadataValue.float(total_amount),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "committee_transfers",
        }
    )
