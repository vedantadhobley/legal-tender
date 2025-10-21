"""Candidate Summaries Asset - Parse FEC candidate financial summaries (weball.zip)"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class CandidateSummariesConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="candidate_summaries",
    description="FEC candidate financial summaries - per-candidate fundraising totals, spending, cash on hand",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def candidate_summaries_asset(
    context: AssetExecutionContext,
    config: CandidateSummariesConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse weball.zip files and store in fec_{cycle}.candidate_summaries collections.
    
    This file contains financial summaries for each candidate, including:
    - Total receipts (fundraising)
    - Total disbursements (spending)
    - Cash on hand
    - Individual contributions
    - PAC contributions
    - Debts
    
    Note: A candidate may have multiple entries if they run in multiple elections
    (e.g., House primary AND House general, or House AND Senate in same cycle).
    The aggregation of these into a single member view happens in assets/processed/.
    """
    
    repo = get_repository()
    stats = {'total_summaries': 0, 'by_cycle': {}}
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "candidate_summaries", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_candidate_summary_path(cycle)
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
                            
                            cand_id = fields[0]
                            
                            # FEC weball.zip field mapping (30 fields):
                            # [0] = candidate_id
                            # [1] = candidate_name
                            # [2] = incumbent_challenger_open (I/C/O)
                            # [3] = party_code (1=DEM, 2=REP, 3=OTHER)
                            # [4] = party_affiliation (DEM, REP, etc.)
                            # [5] = total_receipts
                            # [6] = transfers_from_authorized
                            # [7] = total_disbursements
                            # [8] = transfers_to_authorized
                            # [9] = beginning_cash
                            # [10] = ending_cash
                            # [11] = contributions_from_candidate
                            # [12] = loans_from_candidate
                            # [13] = other_loans
                            # [14] = candidate_loan_repayments
                            # [15] = other_loan_repayments
                            # [16] = debts_owed_by
                            # [17] = total_individual_contributions
                            # [18] = state
                            # [19] = district
                            # [20-29] = coverage dates, net contributions, etc.
                            
                            batch.append({
                                '_id': cand_id,  # Use candidate_id as unique identifier
                                'candidate_id': cand_id,
                                'candidate_name': fields[1],
                                'incumbent_challenger_open': fields[2],  # I, C, O
                                'party': fields[4],  # DEM, REP, IND, etc.
                                'state': fields[18],
                                'district': fields[19],
                                'total_receipts': float(fields[5]) if fields[5] else 0.0,
                                'total_disbursements': float(fields[7]) if fields[7] else 0.0,
                                'cash_beginning': float(fields[9]) if fields[9] else 0.0,
                                'cash_on_hand': float(fields[10]) if fields[10] else 0.0,
                                'individual_contributions': float(fields[17]) if fields[17] else 0.0,
                                'candidate_contributions': float(fields[11]) if fields[11] else 0.0,
                                'candidate_loans': float(fields[12]) if fields[12] else 0.0,
                                'other_loans': float(fields[13]) if fields[13] else 0.0,
                                'debts_owed_by': float(fields[16]) if fields[16] else 0.0,
                                'transfers_from_authorized': float(fields[6]) if fields[6] else 0.0,
                                'transfers_to_authorized': float(fields[8]) if fields[8] else 0.0,
                                'updated_at': datetime.now(),
                            })
                
                if batch:
                    collection.insert_many(batch, ordered=False)
                    context.log.info(f"   ✅ {cycle}: {len(batch):,} candidate summaries")
                    stats['by_cycle'][cycle] = len(batch)
                    stats['total_summaries'] += len(batch)
                
                # Create indexes
                collection.create_index([("candidate_id", 1)])
                collection.create_index([("state", 1), ("district", 1)])
                collection.create_index([("total_receipts", -1)])
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_summaries": stats['total_summaries'],
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "candidate_summaries",
        }
    )
