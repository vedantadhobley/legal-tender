"""PAC Summaries Asset - Parse FEC PAC financial summaries (webk.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class PacSummariesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="pac_summaries",
    description="FEC PAC financial summaries (webk.zip) - quarterly/annual reports for PACs",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def pac_summaries_asset(
    context: AssetExecutionContext,
    config: PacSummariesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse webk.zip and store RAW data in fec_{cycle}.pac_summaries.
    
    NO AGGREGATION - just raw FEC data.
    """
    
    repo = get_repository()
    stats = {'total_summaries': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "pac_summaries", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_pac_summary_path(cycle)
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                batch = []
                with zipfile.ZipFile(zip_path) as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                    if not txt_files:
                        continue
                    
                    with zf.open(txt_files[0]) as f:
                        for line in f:
                            decoded = line.decode('utf-8', errors='ignore').strip()
                            if not decoded:
                                continue
                            
                            fields = decoded.split('|')
                            if len(fields) < 27:
                                continue
                            
                            cmte_id = fields[0]
                            # Multiple reports per cycle - use committee_id + coverage_through_date
                            # NOTE: webk.zip has 27 fields (different from webl.zip's 30 fields)
                            batch.append({
                                '_id': f"{cmte_id}|{fields[26]}",
                                'committee_id': cmte_id,
                                'committee_name': fields[1],
                                'committee_type': fields[2],  # Q=PAC, N=Non-party, etc.
                                'coverage_through_date': fields[26],  # webk only has through_date, not from_date
                                'total_receipts': float(fields[5]) if fields[5] else 0.0,
                                'total_disbursements': float(fields[7]) if fields[7] else 0.0,
                                'contributions_to_committees': float(fields[22]) if fields[22] else 0.0,
                                'independent_expenditures': float(fields[23]) if fields[23] else 0.0,
                                'cash_on_hand_beginning': float(fields[18]) if fields[18] else 0.0,
                                'cash_on_hand_end': float(fields[19]) if fields[19] else 0.0,
                                'debts_owed_by': float(fields[20]) if fields[20] else 0.0,
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} PAC summaries")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_summaries'] += len(batch)
                
                collection.create_index([("committee_id", 1)])
                collection.create_index([("coverage_through_date", -1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_summaries": stats['total_summaries'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "pac_summaries",
        }
    )
