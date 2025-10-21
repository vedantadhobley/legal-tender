"""Independent Expenditures Asset

Parse and store independent expenditures (Schedule E CSV files) - Super PAC spending FOR and AGAINST candidates.
Uses NEGATIVE values for opposition spending to simplify influence calculations later.

CRITICAL: This parses independent_expenditure_{YYYY}.csv files, NOT oppexp.zip!
- oppexp.zip = Operating Expenditures (Schedule B) - WRONG FILE!
- independent_expenditure_{YYYY}.csv = Independent Expenditures (Schedule E) - CORRECT FILE!
"""

from typing import Dict, Any, List
from datetime import datetime
import csv

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    Config,
    AssetIn,
)

from src.data import get_repository
from src.resources.mongo import MongoDBResource
from src.utils.memory import get_recommended_batch_size


class IndependentExpendituresConfig(Config):
    """Configuration for independent expenditures asset."""
    
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False
    batch_size: int | None = None  # Auto-detect if None


@asset(
    name="independent_expenditures",
    description="Super PAC spending FOR and AGAINST candidates (Schedule E - opposition spending stored as NEGATIVE values)",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
    metadata={
        "source": "FEC bulk data (independent_expenditure_{YYYY}.csv files)",
        "file_size": "~20 MB per cycle",
        "contains": "Committee, candidate, amount, support/oppose indicator, purpose",
        "note": "Opposition spending stored as NEGATIVE values",
        "format": "CSV (NOT oppexp.zip!)",
    },
)
def independent_expenditures_asset(
    context: AssetExecutionContext,
    config: IndependentExpendituresConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse and store independent expenditures from CSV files."""
    context.log.info("=" * 80)
    context.log.info("💥 PROCESSING INDEPENDENT EXPENDITURES (Super PAC Spending)")
    context.log.info("=" * 80)
    context.log.info("📝 Source: independent_expenditure_{YYYY}.csv (Schedule E)")
    context.log.info("💡 Innovation: Opposition spending stored as NEGATIVE values")
    
    # Determine batch size
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
    
    context.log.info("\n📥 STREAMING INDEPENDENT EXPENDITURES (CSV Batch Processing)")
    context.log.info(f"   Batch size: {batch_size:,} records per insert")
    context.log.info("-" * 80)
    
    # Initialize MongoDB
    with mongo.get_client() as client:
        support_count = 0
        oppose_count = 0
        total_support = 0.0
        total_oppose = 0.0
        total_expenditures = 0
        top_support_samples = []
        top_oppose_samples = []
        
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                # Each cycle gets its own database: fec_2024, fec_2022, etc.
                collection = mongo.get_collection(
                    client, 
                    "independent_expenditures",
                    database_name=f"fec_{cycle}"
                )
                
                collection.delete_many({})
                context.log.info(f"   🗑️  Cleared existing {cycle} expenditures")
                
                csv_path = repo.fec_independent_expenditures_path(cycle)
                
                if not csv_path.exists():
                    context.log.warning(f"⚠️  File not found: {csv_path}")
                    context.log.warning("   Run data_sync with sync_independent_expenditures=True first\n")
                    continue
                
                context.log.info(f"   📖 Streaming {csv_path.name}...")
                
                batch = []
                cycle_count = 0
                cycle_support = 0
                cycle_oppose = 0
                lines_read = 0
                
                with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    
                    for row in reader:
                        lines_read += 1
                        
                        try:
                            indicator = (row.get('sup_opp', '') or '').strip().upper()
                            
                            try:
                                amount = float(row.get('exp_amo', 0) or 0)
                            except (ValueError, TypeError):
                                amount = 0.0
                            
                            if amount == 0:
                                continue
                            
                            # Track support vs oppose counts
                            if indicator == 'O':
                                oppose_count += 1
                                cycle_oppose += 1
                                total_oppose += amount
                            else:  # 'S' or anything else defaults to support
                                support_count += 1
                                cycle_support += 1
                                total_support += amount
                            
                            record_id = f"{row.get('tran_id', '')}|{row.get('image_num', '')}"
                            if not record_id or record_id == '|':
                                record_id = f"{row.get('file_num', '')}_{lines_read}"
                            
                            record = {
                                '_id': record_id,  # tran_id + image_num for uniqueness across amendments
                                'cycle': cycle,
                                'committee_id': row.get('spe_id', '').strip(),
                                'committee_name': (row.get('spe_nam', '') or '').strip(),
                                'candidate_id': row.get('cand_id', '').strip(),
                                'candidate_name': (row.get('cand_name', '') or '').strip(),
                                'amount': round(amount, 2),
                                'support_oppose': indicator,
                                'is_support': indicator == 'S',
                                'is_oppose': indicator == 'O',
                                'purpose': (row.get('pur', '') or '').strip(),
                                'payee': (row.get('pay', '') or '').strip(),
                                'expenditure_date': row.get('exp_date', '').strip(),
                                'file_num': row.get('file_num', '').strip(),
                                'transaction_id': row.get('tran_id', '').strip(),  # Keep original field
                                'image_num': row.get('image_num', '').strip(),      # Keep original field
                                'updated_at': datetime.now(),
                            }
                            
                            batch.append(record)
                            
                            if indicator == 'S' and len(top_support_samples) < 5:
                                top_support_samples.append(record)
                            elif indicator == 'O' and len(top_oppose_samples) < 5:
                                top_oppose_samples.append(record)
                            
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
                                        except Exception:
                                            pass
                                batch = []
                        
                        except Exception:
                            continue
                        
                        if lines_read % 10000 == 0:
                            context.log.info(f"   📄 Read {lines_read:,} lines, {cycle_count:,} valid records...")
                
                # Insert final batch
                if batch:
                    try:
                        collection.insert_many(batch, ordered=False)
                        cycle_count += len(batch)
                        total_expenditures += len(batch)
                    except Exception as e:
                        context.log.warning(f"   ⚠️  Final batch insert error: {str(e)[:100]}")
                        for rec in batch:
                            try:
                                collection.insert_one(rec)
                                cycle_count += 1
                                total_expenditures += 1
                            except Exception:
                                pass
                    batch = []
                
                context.log.info(f"   ✅ {cycle}: {cycle_count:,} expenditures ({cycle_support:,} support, {cycle_oppose:,} oppose)")
                
                # Create indexes
                context.log.info(f"   📇 Creating indexes for {cycle}...")
                collection.create_index([("candidate_id", 1)])
                collection.create_index([("committee_id", 1)])
                collection.create_index([("cycle", 1)])
                collection.create_index([("amount", -1)])
                collection.create_index([("is_support", 1)])
                collection.create_index([("is_oppose", 1)])
                context.log.info(f"   ✅ Indexes created for {cycle}\n")
                
                stats['by_cycle'][cycle] = {
                    'count': cycle_count,
                    'support': cycle_support,
                    'oppose': cycle_oppose,
                }
            
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}\n")
                continue
        
        net_amount = total_support - total_oppose
    
    context.log.info("\n" + "=" * 80)
    context.log.info("💥 INDEPENDENT EXPENDITURES COMPLETE!")
    context.log.info("=" * 80)
    context.log.info(f"   Total: {total_expenditures:,} expenditures")
    context.log.info(f"   Support: {support_count:,} (${total_support:,.2f})")
    context.log.info(f"   Oppose: {oppose_count:,} (${total_oppose:,.2f})")
    context.log.info(f"   Net influence: ${net_amount:,.2f}")
    
    if top_support_samples:
        context.log.info("\n💚 Sample Support Expenditures:")
        for exp in top_support_samples[:3]:
            context.log.info(f"   {exp['committee_name'][:40]} → {exp['candidate_name'][:30]} ${exp['amount']:,.0f}")
    
    if top_oppose_samples:
        context.log.info("\n�� Sample Opposition Expenditures:")
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
            "mongodb_databases": MetadataValue.json([f"fec_{cycle}" for cycle in config.cycles]),
            "mongodb_collection": "independent_expenditures",
            "file_format": "CSV (independent_expenditure_{YYYY}.csv)",
        }
    )
