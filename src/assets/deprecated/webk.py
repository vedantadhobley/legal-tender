"""PAC Summary Asset - Parse FEC PAC summary files (webk.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class PacSummariesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="webk",
    description="FEC PAC summary file (webk.zip) - raw FEC data with original field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def webk_asset(
    context: AssetExecutionContext,
    config: PacSummariesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse webk.zip files and store in fec_{cycle}.webk collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_summaries': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "webk", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_webk_path(cycle)
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
                            
                            # Use EXACT field names from fec.md
                            batch.append({

                                'CMTE_ID': fields[0],
                                'CMTE_NM': fields[1],
                                'CMTE_TP': fields[2],
                                'CMTE_DSGN': fields[3],
                                'CMTE_FILING_FREQ': fields[4],
                                'TTL_RECEIPTS': float(fields[5]) if fields[5] else None,
                                'TRANS_FROM_AFF': float(fields[6]) if fields[6] else None,
                                'INDV_CONTRIB': float(fields[7]) if fields[7] else None,
                                'OTHER_POL_CMTE_CONTRIB': float(fields[8]) if fields[8] else None,
                                'CAND_CONTRIB': float(fields[9]) if fields[9] else None,
                                'CAND_LOANS': float(fields[10]) if fields[10] else None,
                                'TTL_LOANS_RECEIVED': float(fields[11]) if fields[11] else None,
                                'TTL_DISB': float(fields[12]) if fields[12] else None,
                                'TRANF_TO_AFF': float(fields[13]) if fields[13] else None,
                                'INDV_REFUNDS': float(fields[14]) if fields[14] else None,
                                'OTHER_POL_CMTE_REFUNDS': float(fields[15]) if fields[15] else None,
                                'CAND_LOAN_REPAY': float(fields[16]) if fields[16] else None,
                                'LOAN_REPAY': float(fields[17]) if fields[17] else None,
                                'COH_BOP': float(fields[18]) if fields[18] else None,
                                'COH_COP': float(fields[19]) if fields[19] else None,
                                'DEBTS_OWED_BY': float(fields[20]) if fields[20] else None,
                                'NONFED_TRANS_RECEIVED': float(fields[21]) if fields[21] else None,
                                'CONTRIB_TO_OTHER_CMTE': float(fields[22]) if fields[22] else None,
                                'IND_EXP': float(fields[23]) if fields[23] else None,
                                'PTY_COORD_EXP': float(fields[24]) if fields[24] else None,
                                'NONFED_SHARE_EXP': float(fields[25]) if fields[25] else None,
                                'CVG_END_DT': fields[26],
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} PAC summaries")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_summaries'] += len(batch)
                
                # Create indexes on key fields
                collection.create_index([("CMTE_NM", 1)])
                collection.create_index([("CMTE_TP", 1)])
                collection.create_index([("TTL_RECEIPTS", -1)])
                collection.create_index([("CONTRIB_TO_OTHER_CMTE", -1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_summaries": stats['total_summaries'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "webk",
        }
    )
