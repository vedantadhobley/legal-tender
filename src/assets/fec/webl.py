"""Committee Summary Asset - Parse FEC committee summary files (webl.zip) using raw FEC field names"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class CommitteeSummariesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="webl",
    description="FEC committee summary file (webl.zip) - raw FEC data with original field names",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def webl_asset(
    context: AssetExecutionContext,
    config: CommitteeSummariesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse webl.zip files and store in fec_{cycle}.webl collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {'total_summaries': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "webl", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_committee_summary_path(cycle)
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
                            if len(fields) < 30:
                                continue
                            
                            # Use EXACT field names from fec.md (same as weball for committee summaries)
                            batch.append({

                                'CAND_ID': fields[0],
                                'CAND_NAME': fields[1],
                                'CAND_ICI': fields[2],
                                'PTY_CD': fields[3],
                                'CAND_PTY_AFFILIATION': fields[4],
                                'TTL_RECEIPTS': float(fields[5]) if fields[5] else None,
                                'TRANS_FROM_AUTH': float(fields[6]) if fields[6] else None,
                                'TTL_DISB': float(fields[7]) if fields[7] else None,
                                'TRANS_TO_AUTH': float(fields[8]) if fields[8] else None,
                                'COH_BOP': float(fields[9]) if fields[9] else None,
                                'COH_COP': float(fields[10]) if fields[10] else None,
                                'CAND_CONTRIB': float(fields[11]) if fields[11] else None,
                                'CAND_LOANS': float(fields[12]) if fields[12] else None,
                                'OTHER_LOANS': float(fields[13]) if fields[13] else None,
                                'CAND_LOAN_REPAY': float(fields[14]) if fields[14] else None,
                                'OTHER_LOAN_REPAY': float(fields[15]) if fields[15] else None,
                                'DEBTS_OWED_BY': float(fields[16]) if fields[16] else None,
                                'TTL_INDIV_CONTRIB': float(fields[17]) if fields[17] else None,
                                'CAND_OFFICE_ST': fields[18],
                                'CAND_OFFICE_DISTRICT': fields[19],
                                'SPEC_ELECTION': fields[20],
                                'PRIM_ELECTION': fields[21],
                                'RUN_ELECTION': fields[22],
                                'GEN_ELECTION': fields[23],
                                'GEN_ELECTION_PRECENT': float(fields[24]) if fields[24] else None,
                                'OTHER_POL_CMTE_CONTRIB': float(fields[25]) if fields[25] else None,
                                'POL_PTY_CONTRIB': float(fields[26]) if fields[26] else None,
                                'CVG_END_DT': fields[27],
                                'INDIV_REFUNDS': float(fields[28]) if fields[28] else None,
                                'CMTE_REFUNDS': float(fields[29]) if fields[29] else None,
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} committee summaries")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_summaries'] += len(batch)
                
                # Create indexes on key fields
                collection.create_index([("CAND_NAME", 1)])
                collection.create_index([("TTL_RECEIPTS", -1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_summaries": stats['total_summaries'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "webl",
        }
    )
