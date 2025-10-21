"""Independent Expenditure Asset - Parse independent_expenditure.csv using raw FEC field names

CRITICAL: This parses independent_expenditure_{YYYY}.csv files, NOT oppexp.zip!
"""

from typing import Dict, Any, List
from datetime import datetime
import csv

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class IndependentExpenditureConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="independent_expenditure",
    description="FEC independent expenditures (independent_expenditure.csv) - raw FEC data with original field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def independent_expenditure_asset(
    context: AssetExecutionContext,
    config: IndependentExpenditureConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse and store independent expenditures from CSV files."""
    context.log.info("=" * 80)
    context.log.info("💥 PROCESSING INDEPENDENT EXPENDITURES (Super PAC Spending)")
    context.log.info("=" * 80)
    context.log.info("📝 Source: independent_expenditure_{YYYY}.csv (Schedule E)")
    context.log.info("💡 Innovation: Opposition spending stored as NEGATIVE values")
    context.log.info("📊 File size: ~20MB, 50-200K records per cycle (no batching needed)")
    
    repo = get_repository()
    stats = {'total_expenditures': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "independent_expenditure", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                csv_path = repo.fec_independent_expenditures_path(cycle)
                if not csv_path.exists():
                    context.log.warning(f"⚠️  File not found: {csv_path}")
                    continue
                
                batch = []
                with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                    # CSV has headers (unlike ZIP files)
                    reader = csv.DictReader(f)
                    
                    for row in reader:
                        try:
                            # CSV has lowercase headers, store as uppercase per FEC.md
                            batch.append({
                                'CAND_ID': (row.get('cand_id') or '').strip(),
                                'CAND_NAME': (row.get('cand_name') or '').strip(),
                                'SPE_ID': (row.get('spe_id') or '').strip(),
                                'SPE_NAM': (row.get('spe_nam') or '').strip(),
                                'ELE_TYPE': (row.get('ele_type') or '').strip(),
                                'CAN_OFFICE_STATE': (row.get('can_office_state') or '').strip(),
                                'CAN_OFFICE_DIS': (row.get('can_office_dis') or '').strip(),
                                'CAN_OFFICE': (row.get('can_office') or '').strip(),
                                'CAND_PTY_AFF': (row.get('cand_pty_aff') or '').strip(),
                                'EXP_AMO': float(row.get('exp_amo') or 0) if row.get('exp_amo') else None,
                                'EXP_DATE': (row.get('exp_date') or '').strip(),
                                'AGG_AMO': float(row.get('agg_amo') or 0) if row.get('agg_amo') else None,
                                'SUP_OPP': (row.get('sup_opp') or '').strip(),
                                'PUR': (row.get('pur') or '').strip(),
                                'PAY': (row.get('pay') or '').strip(),
                                'FILE_NUM': (row.get('file_num') or '').strip(),
                                'AMNDT_IND': (row.get('amndt_ind') or '').strip(),
                                'TRAN_ID': (row.get('tran_id') or '').strip(),
                                'IMAGE_NUM': (row.get('image_num') or '').strip(),
                                'RECEIPT_DAT': (row.get('receipt_dat') or '').strip(),
                                'FEC_ELECTION_YR': (row.get('fec_election_yr') or '').strip(),
                                'PREV_FILE_NUM': (row.get('prev_file_num') or '').strip(),
                                'DISSEM_DT': (row.get('dissem_dt') or '').strip(),
                                'updated_at': datetime.now(),
                            })
                            
                            # Batch insert for performance
                            if len(batch) >= 5000:
                                collection.insert_many(batch, ordered=False)
                                batch = []
                        
                        except Exception as e:
                            context.log.warning(f"   ⚠️  Error parsing row: {str(e)[:100]}")
                            continue
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch, ordered=False)
                
                total_count = collection.count_documents({})
                context.log.info(f"   ✅ {cycle}: {total_count:,} independent expenditures")
                stats['by_cycle'][cycle] = total_count
                stats['total_expenditures'] += total_count
                
                # Create indexes on key fields
                collection.create_index([("CAND_ID", 1)])
                collection.create_index([("SPE_ID", 1)])
                collection.create_index([("SUP_OPP", 1)])
                collection.create_index([("EXP_AMO", -1)])
                collection.create_index([("EXP_DATE", -1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_expenditures": stats['total_expenditures'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "independent_expenditure",
        }
    )
