"""
Cross-cycle aggregation of FEC committee financial summaries (webk).
Aggregates PAC/committee summary data by committee ID across all cycles.
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext, Output
from typing import Dict, Any
from datetime import datetime
from src.resources.mongo import MongoDBResource


class CommitteeSummariesConfig(Config):
    """Configuration for committee summaries aggregation."""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="committee_summaries",
    group_name="aggregation",
    ins={
        "enriched_webk": AssetIn(key="enriched_webk"),
    },
    compute_kind="aggregation",
    description="Cross-cycle aggregation of FEC committee summaries. Aggregates PAC/committee financial data by committee ID."
)
def committee_summaries_asset(
    context: AssetExecutionContext,
    config: CommitteeSummariesConfig,
    mongo: MongoDBResource,
    enriched_webk: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Aggregate FEC committee summaries across all cycles.
    
    Creates committee-level aggregations with:
    - Per-cycle webk summary data
    - Lifetime totals (contributions, expenditures, cash)
    - Independent expenditure validation (webk.IND_EXP vs our 24A/24E calc)
    
    Returns:
        Output with aggregated committee summaries
    """
    
    stats = {
        'total_committees': 0,
        'total_cycle_records': 0,
        'committees_with_indie': 0,
        'total_indie_exp': 0.0,
    }
    
    with mongo.get_client() as client:
        summaries_collection = mongo.get_collection(client, "committee_summaries", database_name="aggregation")
        
        # Clear existing data
        summaries_collection.delete_many({})
        context.log.info("Cleared existing committee_summaries")
        
        # Track all unique committee IDs across cycles
        all_committees: Dict[str, Dict[str, Any]] = {}
        
        for cycle in config.cycles:
            context.log.info(f"Processing cycle {cycle}...")
            
            # Get collection for this cycle
            webk_collection = mongo.get_collection(client, "webk", database_name=f"enriched_{cycle}")
            
            # Check if collection exists
            db = client[f"enriched_{cycle}"]
            if "webk" not in db.list_collection_names():
                context.log.warning(f"Collection enriched_{cycle}.webk not found, skipping")
                continue
            
            webk_records = list(webk_collection.find({}))
            context.log.info(f"Found {len(webk_records)} committee records in {cycle}")
            
            for record in webk_records:
                cmte_id = record.get("CMTE_ID")
                if not cmte_id:
                    continue
                
                # Initialize committee if not seen before
                if cmte_id not in all_committees:
                    all_committees[cmte_id] = {
                        "_id": cmte_id,
                        "committee_id": cmte_id,
                        "committee_name": record.get("CMTE_NM", ""),
                        "committee_type": record.get("CMTE_TP", ""),
                        "cycles": []
                    }
                
                # Add cycle-specific data
                cycle_data = {
                    "cycle": cycle,
                    "committee_name": record.get("CMTE_NM", ""),
                    "committee_type": record.get("CMTE_TP", ""),
                    "total_receipts": record.get("TTL_RECEIPTS", 0),
                    "total_disbursements": record.get("TTL_DISB", 0),
                    "individual_contributions": record.get("INDV_CONTRIB", 0),
                    "other_committee_contributions": record.get("OTHER_CMTE_CONTRIB", 0),
                    "candidate_contributions": record.get("CAND_CONTRIB", 0),
                    "candidate_loans": record.get("CAND_LOANS", 0),
                    "total_loans": record.get("TTL_LOANS", 0),
                    "independent_expenditures": record.get("IND_EXP", 0),  # Validation field
                    "coordinated_expenditures": record.get("COORD_EXP_BY_PTY_CMTE", 0),
                    "contributions_to_other_committees": record.get("CONTRIB_TO_OTHER_CMTE", 0),
                    "transfers_from_other_authorized_committees": record.get("TRANF_FROM_OTHER_AUTH_CMTE", 0),
                    "transfers_to_other_authorized_committees": record.get("TRANF_TO_OTHER_AUTH_CMTE", 0),
                    "cash_on_hand_beginning": record.get("COH_BOP", 0),
                    "cash_on_hand_end": record.get("COH_COP", 0),
                    "debts_owed_by": record.get("DEBTS_OWED_BY", 0),
                    "debts_owed_to": record.get("DEBTS_OWED_TO", 0),
                    "operating_expenditures": record.get("OPER_EXP", 0),
                    "refunds_individual_contributions": record.get("INDV_REFUNDS", 0),
                    "refunds_other_committee_contributions": record.get("OTHER_CMTE_REFUNDS", 0)
                }
                
                all_committees[cmte_id]["cycles"].append(cycle_data)
                stats['total_cycle_records'] += 1
        
        # Calculate lifetime totals and insert
        batch = []
        for cmte_id, committee_data in all_committees.items():
            totals = {
                "total_receipts_all_time": 0,
                "total_disbursements_all_time": 0,
                "total_individual_contributions": 0,
                "total_independent_expenditures": 0,
                "total_contributions_to_other_committees": 0,
                "total_coordinated_expenditures": 0,
                "current_cash_on_hand": 0,
                "current_debts_owed_by": 0
            }
            
            # Sum across all cycles
            latest_cycle_num = 0
            for cycle_data in committee_data["cycles"]:
                cycle_num = int(cycle_data["cycle"])
                
                totals["total_receipts_all_time"] += cycle_data["total_receipts"] or 0
                totals["total_disbursements_all_time"] += cycle_data["total_disbursements"] or 0
                totals["total_individual_contributions"] += cycle_data["individual_contributions"] or 0
                totals["total_independent_expenditures"] += cycle_data["independent_expenditures"] or 0
                totals["total_contributions_to_other_committees"] += cycle_data["contributions_to_other_committees"] or 0
                totals["total_coordinated_expenditures"] += cycle_data["coordinated_expenditures"] or 0
                
                # Track most recent cycle for current values
                if cycle_num > latest_cycle_num:
                    latest_cycle_num = cycle_num
                    totals["current_cash_on_hand"] = cycle_data["cash_on_hand_end"] or 0
                    totals["current_debts_owed_by"] = cycle_data["debts_owed_by"] or 0
            
            committee_data["totals"] = totals
            committee_data["cycle_count"] = len(committee_data["cycles"])
            committee_data["updated_at"] = datetime.now()
            
            if totals["total_independent_expenditures"] > 0:
                stats['committees_with_indie'] += 1
                stats['total_indie_exp'] += totals["total_independent_expenditures"]
            
            batch.append(committee_data)
        
        if batch:
            summaries_collection.insert_many(batch, ordered=False)
            stats['total_committees'] = len(batch)
            context.log.info(f"✅ Inserted {len(batch)} committees with {stats['total_cycle_records']} cycle records")
            context.log.info(f"Total independent expenditures: ${stats['total_indie_exp']:,.2f}")
            context.log.info(f"Committees with independent expenditures: {stats['committees_with_indie']}")
        
        # Create indexes
        summaries_collection.create_index([("committee_id", 1)])
        summaries_collection.create_index([("committee_name", 1)])
    
    return Output(
        value=stats,
        metadata={
            "total_committees": stats['total_committees'],
            "total_cycle_records": stats['total_cycle_records'],
            "total_independent_expenditures": float(stats['total_indie_exp']),
            "committees_with_independent_expenditures": stats['committees_with_indie'],
        }
    )
