"""Independent Expenditures Asset

Parse and store independent expenditures (oppexp.zip) - Super PAC spending FOR and AGAINST candidates.
Uses NEGATIVE values for opposition spending to simplify influence calculations later.
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
from src.api.fec_bulk_data import parse_oppexp_file, download_fec_file
from src.resources.mongo import MongoDBResource
from src.utils.memory import get_recommended_batch_size


class IndependentExpendituresConfig(Config):
    """Configuration for independent expenditures asset."""
    
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
    name="independent_expenditures",
    description="Super PAC spending FOR and AGAINST candidates (opposition spending stored as NEGATIVE values)",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
    metadata={
        "source": "FEC bulk data (oppexp files)",
        "file_size": "2-3 GB per cycle",
        "contains": "Committee, candidate, amount, support/oppose indicator, purpose",
        "note": "Opposition spending stored as NEGATIVE values",
    },
)
def independent_expenditures_asset(
    context: AssetExecutionContext,
    config: IndependentExpendituresConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse and store independent expenditures.
    
    These are Super PAC/dark money expenditures that can't coordinate with campaigns (allegedly).
    Key innovation: OPPOSITION spending is stored as NEGATIVE amounts for easy influence calculations.
    
    Key fields:
    - CAND_ID: Candidate this spending benefits/harms
    - TRANSACTION_AMT: Amount (NEGATIVE if opposition!)
    - SUPPORT_OPPOSE_INDICATOR: 'S' = Support (positive), 'O' = Oppose (negative)
    - CMTE_ID: Super PAC that spent the money
    - PURPOSE: What the money was spent on
    
    Args:
        config: Configuration
        mongo: MongoDB resource
        data_sync: Data sync results (ensures files are downloaded)
        
    Returns:
        Summary statistics
    """
    context.log.info("=" * 80)
    context.log.info("💥 PROCESSING INDEPENDENT EXPENDITURES (Super PAC Spending)")
    context.log.info("=" * 80)
    context.log.info("💡 Innovation: Opposition spending stored as NEGATIVE values")
    
    # Determine batch size (auto-detect if not specified)
    if config.batch_size is None:
        batch_size, explanation = get_recommended_batch_size()
        context.log.info(f"🧠 Auto-detected batch size: {explanation}")
    else:
        batch_size = config.batch_size
        context.log.info(f"📝 Using configured batch size: {batch_size:,}")
    
    repo = get_repository()
    
    stats = {
        'total_expenditures': 0,
        'support_expenditures': 0,
        'oppose_expenditures': 0,
        'by_cycle': {},
        'total_support_amount': 0.0,
        'total_oppose_amount': 0.0,
        'net_amount': 0.0,
    }
    
    # ==========================================================================
    # STREAMING APPROACH: Process each cycle incrementally
    # Parse → Process → Write to MongoDB in batches
    # Never load full file into memory!
    # ==========================================================================
    
    context.log.info("\n📥 STREAMING INDEPENDENT EXPENDITURES (Batch Processing)")
    context.log.info(f"   Batch size: {batch_size:,} records per insert")
    context.log.info("-" * 80)
    
    # Initialize MongoDB
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "independent_expenditures")
        
        # Clear existing data
        collection.delete_many({})
        context.log.info("🗑️  Cleared existing expenditures\n")
        
        # Process each cycle independently
        support_count = 0
        oppose_count = 0
        total_support = 0.0
        total_oppose = 0.0
        total_expenditures = 0
        top_support_samples = []  # Keep a few for display
        top_oppose_samples = []
        
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                zip_path = repo.fec_independent_expenditures_path(cycle)
                
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    context.log.warning("   Run data_sync with sync_independent_expenditures=True first\n")
                    continue
                
                # Stream process this cycle
                context.log.info(f"   📖 Streaming {zip_path.name}...")
                
                import zipfile
                batch = []
                cycle_count = 0
                cycle_support = 0
                cycle_oppose = 0
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
                                if len(fields) < 41:  # oppexp has 41 fields
                                    continue
                                
                                # Parse amount and support/oppose
                                try:
                                    raw_amount = float(fields[14]) if fields[14] else 0.0
                                except:
                                    raw_amount = 0.0
                                
                                indicator = (fields[15] or '').strip().upper()
                                
                                # Apply negative for opposition!
                                if indicator == 'O':
                                    amount = -abs(raw_amount)
                                    oppose_count += 1
                                    cycle_oppose += 1
                                    total_oppose += abs(raw_amount)
                                else:
                                    amount = abs(raw_amount)
                                    support_count += 1
                                    cycle_support += 1
                                    total_support += abs(raw_amount)
                                
                                # Build record
                                record = {
                                    '_id': fields[40],  # SUB_ID
                                    'cycle': cycle,
                                    'committee_id': fields[0],
                                    'committee_name': (fields[6] or '').strip(),
                                    'candidate_id': fields[1],
                                    'candidate_name': (fields[7] or '').strip(),
                                    'candidate_office': fields[5],
                                    'candidate_state': fields[17],
                                    'candidate_district': fields[18],
                                    'amount': round(amount, 2),
                                    'raw_amount': round(raw_amount, 2),
                                    'support_oppose': indicator,
                                    'is_support': indicator == 'S',
                                    'is_oppose': indicator == 'O',
                                    'purpose': (fields[12] or '').strip(),
                                    'category': fields[13],
                                    'date': fields[11],
                                    'updated_at': datetime.now(),
                                }
                                
                                batch.append(record)
                                
                                # Keep samples for display
                                if indicator == 'S' and len(top_support_samples) < 5:
                                    top_support_samples.append(record)
                                elif indicator == 'O' and len(top_oppose_samples) < 5:
                                    top_oppose_samples.append(record)
                                
                                # Write batch when full
                                if len(batch) >= batch_size:
                                    try:
                                        collection.insert_many(batch, ordered=False)
                                        cycle_count += len(batch)
                                        total_expenditures += len(batch)
                                        context.log.info(f"   💾 Batch inserted: {cycle_count:,} records so far...")
                                    except Exception as e:
                                        context.log.warning(f"   ⚠️  Batch insert error (skipping duplicates): {str(e)[:100]}")
                                        for rec in batch:
                                            try:
                                                collection.insert_one(rec)
                                                cycle_count += 1
                                                total_expenditures += 1
                                            except:
                                                pass
                                    batch = []  # Clear memory!
                                
                            except Exception as e:
                                continue  # Skip bad lines
                            
                            # Progress every 50K lines (smaller files than indiv)
                            if lines_read % 50000 == 0:
                                context.log.info(f"   � Read {lines_read:,} lines, {cycle_count:,} valid records...")
                    
                    # Insert final batch
                    if batch:
                        try:
                            collection.insert_many(batch, ordered=False)
                            cycle_count += len(batch)
                            total_expenditures += len(batch)
                        except Exception as e:
                            context.log.warning(f"   ⚠️  Final batch insert error (skipping duplicates): {str(e)[:100]}")
                            for rec in batch:
                                try:
                                    collection.insert_one(rec)
                                    cycle_count += 1
                                    total_expenditures += 1
                                except:
                                    pass
                        batch = []  # Clear memory!
                
                context.log.info(f"   ✅ {cycle}: {cycle_count:,} expenditures ({cycle_support:,} support, {cycle_oppose:,} oppose)\n")
                
                stats['by_cycle'][cycle] = {
                    'count': cycle_count,
                    'support': cycle_support,
                    'oppose': cycle_oppose,
                }
            
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}\n")
                continue
        
        net_amount = total_support - total_oppose
        
        # Create indexes once after all data loaded
        context.log.info("📇 Creating indexes...")
        collection.create_index([("candidate_id", 1)])
        collection.create_index([("committee_id", 1)])
        collection.create_index([("cycle", 1)])
        collection.create_index([("amount", -1)])
        collection.create_index([("is_support", 1)])
        collection.create_index([("is_oppose", 1)])
        context.log.info("✅ Indexes created")
    
    # ==========================================================================
    # Summary
    # ==========================================================================
    
    context.log.info("\n" + "=" * 80)
    context.log.info("� INDEPENDENT EXPENDITURES COMPLETE!")
    context.log.info("=" * 80)
    context.log.info(f"   Total: {total_expenditures:,} expenditures")
    context.log.info(f"   Support: {support_count:,} (${total_support:,.2f})")
    context.log.info(f"   Oppose: {oppose_count:,} (${total_oppose:,.2f})")
    context.log.info(f"   Net influence: ${net_amount:,.2f}")
    
    # Show samples
    if top_support_samples:
        context.log.info("\n💚 Sample Support Expenditures:")
        for exp in top_support_samples[:3]:
            context.log.info(f"   {exp['committee_name'][:40]} → {exp['candidate_name'][:30]} ${exp['amount']:,.0f}")
    
    if top_oppose_samples:
        context.log.info("\n💔 Sample Opposition Expenditures:")
        for exp in top_oppose_samples[:3]:
            context.log.info(f"   {exp['committee_name'][:40]} → {exp['candidate_name'][:30]} ${exp['amount']:,.0f} (NEGATIVE)")
    
    stats['total_expenditures'] = total_expenditures
    stats['support_expenditures'] = support_count
    stats['oppose_expenditures'] = oppose_count
    stats['total_support_amount'] = round(total_support, 2)
    stats['total_oppose_amount'] = round(total_oppose, 2)
    stats['net_amount'] = round(net_amount, 2)
    
    return Output(
        value=stats,
        metadata={
            "total_expenditures": total_expenditures,
            "support_expenditures": support_count,
            "oppose_expenditures": oppose_count,
            "total_support_usd": MetadataValue.float(total_support),
            "total_oppose_usd": MetadataValue.float(total_oppose),
            "net_influence_usd": MetadataValue.float(net_amount),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_collection": "independent_expenditures",
        }
    )
