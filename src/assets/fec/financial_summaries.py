"""Financial Summaries Asset - Parse webl.zip and webk.zip summary files"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class FinancialSummariesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="financial_summaries",
    description="FEC financial summaries - committee and PAC financial totals including individual contributions",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def financial_summaries_asset(
    context: AssetExecutionContext,
    config: FinancialSummariesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse webl.zip (committee summaries) and webk.zip (PAC summaries)."""
    
    repo = get_repository()
    stats = {'committee_summaries': 0, 'pac_summaries': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            # Parse webl.zip → committee_summaries
            try:
                collection = mongo.get_collection(client, "committee_summaries", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_committee_summary_path(cycle)
                if zip_path.exists():
                    batch = []
                    with zipfile.ZipFile(zip_path) as zf:
                        txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                        if txt_files:
                            with zf.open(txt_files[0]) as f:
                                for line in f:
                                    decoded = line.decode('utf-8', errors='ignore').strip()
                                    if not decoded:
                                        continue
                                    
                                    fields = decoded.split('|')
                                    if len(fields) < 30:
                                        continue
                                    
                                    cmte_id = fields[0]
                                    batch.append({
                                        '_id': f"{cmte_id}|{fields[27]}",
                                        'committee_id': cmte_id,
                                        'committee_name': fields[1],
                                        'coverage_from_date': fields[26],
                                        'coverage_through_date': fields[27],
                                        'total_receipts': float(fields[5]) if fields[5] else 0.0,
                                        'individual_contributions': float(fields[17]) if fields[17] else 0.0,
                                        'pac_contributions': float(fields[11]) if fields[11] else 0.0,
                                        'total_disbursements': float(fields[7]) if fields[7] else 0.0,
                                        'cash_on_hand_beginning': float(fields[8]) if fields[8] else 0.0,
                                        'cash_on_hand_end': float(fields[9]) if fields[9] else 0.0,
                                        'debts_owed_by': float(fields[28]) if fields[28] else 0.0,
                                        'updated_at': datetime.now(),
                                    })
                    
                    if batch:
                        collection.insert_many(batch, ordered=False)
                        context.log.info(f"   ✅ Committee summaries: {len(batch):,}")
                        stats['committee_summaries'] += len(batch)
                        collection.create_index([("committee_id", 1)])
            except Exception as e:
                context.log.error(f"   ❌ Error with committee summaries: {e}")
            
            # Parse webk.zip → pac_summaries
            try:
                collection = mongo.get_collection(client, "pac_summaries", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_pac_summary_path(cycle)
                if zip_path.exists():
                    batch = []
                    with zipfile.ZipFile(zip_path) as zf:
                        txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                        if txt_files:
                            with zf.open(txt_files[0]) as f:
                                for line in f:
                                    decoded = line.decode('utf-8', errors='ignore').strip()
                                    if not decoded:
                                        continue
                                    
                                    fields = decoded.split('|')
                                    if len(fields) < 25:
                                        continue
                                    
                                    cmte_id = fields[0]
                                    # Use committee_id + coverage_through_date as _id
                                    # Multiple reports per cycle (Q1, Q2, Q3, year-end) need distinction
                                    # coverage_through_date is fields[23]
                                    batch.append({
                                        '_id': f"{cmte_id}|{fields[23]}",
                                        'committee_id': cmte_id,
                                        'committee_name': fields[1],
                                        'coverage_from_date': fields[22],
                                        'coverage_through_date': fields[23],
                                        'total_receipts': float(fields[5]) if fields[5] else 0.0,
                                        'contributions_to_committees': float(fields[15]) if fields[15] else 0.0,
                                        'independent_expenditures': float(fields[16]) if fields[16] else 0.0,
                                        'cash_on_hand_end': float(fields[9]) if fields[9] else 0.0,
                                        'updated_at': datetime.now(),
                                    })
                    
                    if batch:
                        collection.insert_many(batch, ordered=False)
                        context.log.info(f"   ✅ PAC summaries: {len(batch):,}")
                        stats['pac_summaries'] += len(batch)
                        collection.create_index([("committee_id", 1)])
            except Exception as e:
                context.log.error(f"   ❌ Error with PAC summaries: {e}")
    
    return Output(
        value=stats,
        metadata={
            "committee_summaries": stats['committee_summaries'],
            "pac_summaries": stats['pac_summaries'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collections": "committee_summaries, pac_summaries",
        }
    )
