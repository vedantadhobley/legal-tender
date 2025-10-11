"""Finance-related data assets for tracking money influence in Congress."""
from typing import List, Dict, Any

from dagster import asset, AssetExecutionContext, AssetIn, Output
from src.api.election_api import (
    search_candidate_by_name,
    get_candidate_committees,
    get_candidate_financial_summary,
    get_committee_contributions_paginated,
    get_independent_expenditures_for_candidate,
    aggregate_contributions_by_donor,
    aggregate_independent_expenditures_by_committee
)
from src.resources.mongo import MongoDBResource


@asset(
    name="member_fec_mapping",
    description="Maps Congress bioguideId to FEC candidate_id for all 538 members",
    group_name="finance",
    compute_kind="api",
    ins={
        "congress_members": AssetIn(key="congress_members"),
    },
    metadata={
        "source": "OpenFEC API",
    }
)
def member_fec_mapping_asset(
    context: AssetExecutionContext,
    congress_members: List[Dict[str, Any]]
) -> Output[Dict[str, Any]]:
    """Map all Congress members to their FEC candidate IDs."""
    context.log.info("=" * 80)
    context.log.info("🗺️  MAPPING CONGRESS MEMBERS TO FEC CANDIDATE IDS")
    context.log.info("=" * 80)
    
    mapping = {}
    mapped_count = 0
    failed_count = 0
    
    for idx, member in enumerate(congress_members, 1):
        bioguide_id = member.get("bioguideId")
        first_name = member.get("firstName", "")
        last_name = member.get("lastName", "")
        state = member.get("state", "")
        office = "H" if "House" in str(member.get("terms", [])) else "S"
        
        if not bioguide_id or not last_name:
            failed_count += 1
            continue
        
        context.log.info(f"[{idx}/{len(congress_members)}] {last_name}, {first_name} ({state}-{office})")
        
        candidates = search_candidate_by_name(last_name, state=state, office=office)
        
        if not candidates:
            context.log.warning("  ❌ Not found")
            failed_count += 1
            mapping[bioguide_id] = {
                "bioguide_id": bioguide_id,
                "member_name": f"{last_name}, {first_name}",
                "state": state,
                "office": office,
                "fec_candidate_id": None,
                "fec_committees": [],
                "mapping_status": "not_found"
            }
            continue
        
        candidate = candidates[0]
        fec_candidate_id = candidate.get("candidate_id")
        if not fec_candidate_id:
            context.log.warning("  ❌ No candidate_id in result")
            failed_count += 1
            mapping[bioguide_id] = {
                "bioguide_id": bioguide_id,
                "member_name": f"{last_name}, {first_name}",
                "state": state,
                "office": office,
                "fec_candidate_id": None,
                "fec_committees": [],
                "mapping_status": "no_candidate_id"
            }
            continue
        
        committees = get_candidate_committees(fec_candidate_id, cycle=2024) or []
        
        context.log.info(f"  ✅ {fec_candidate_id} ({len(committees)} committees)")
        mapped_count += 1
        
        mapping[bioguide_id] = {
            "bioguide_id": bioguide_id,
            "member_name": f"{last_name}, {first_name}",
            "state": state,
            "office": office,
            "fec_candidate_id": fec_candidate_id,
            "fec_committees": [
                {
                    "committee_id": c.get("committee_id"),
                    "committee_name": c.get("name"),
                    "committee_type": c.get("committee_type_full")
                }
                for c in committees
            ],
            "mapping_status": "mapped"
        }
    
    context.log.info("")
    context.log.info(f"✅ Mapped: {mapped_count}, ❌ Failed: {failed_count}")
    
    return Output(
        value=mapping,
        metadata={
            "mapped_count": mapped_count,
            "failed_count": failed_count
        }
    )


@asset(
    name="member_finance",
    description="Complete financial profile for all Congress members",
    group_name="finance",
    compute_kind="api",
    ins={
        "member_fec_mapping": AssetIn(key="member_fec_mapping"),
    },
    metadata={
        "source": "OpenFEC API",
        "cycle": "2024",
    }
)
def member_finance_asset(
    context: AssetExecutionContext,
    mongo: MongoDBResource,
    member_fec_mapping: Dict[str, Any]
) -> Output[int]:
    """Fetch complete financial data for all members and store in MongoDB."""
    context.log.info("=" * 80)
    context.log.info("💰 FETCHING COMPLETE FINANCIAL DATA")
    context.log.info("=" * 80)
    
    members_to_process = [m for m in member_fec_mapping.values() if m.get("fec_candidate_id")]
    processed_count = 0
    
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "member_finance")
        
        for idx, member in enumerate(members_to_process, 1):
            bioguide_id = member["bioguide_id"]
            member_name = member["member_name"]
            fec_candidate_id = member["fec_candidate_id"]
            committees = member.get("fec_committees", [])
            
            context.log.info(f"[{idx}/{len(members_to_process)}] {member_name}")
            
            # Get financial summary
            financial_summary = get_candidate_financial_summary(fec_candidate_id, cycle=2024)
            if financial_summary:
                context.log.info(f"  💵 ${financial_summary.get('receipts', 0):,.0f}")
            
            # Get direct contributions (limit 10 pages)
            all_contributions = []
            for committee in committees[:1]:
                contributions = list(get_committee_contributions_paginated(
                    committee["committee_id"],
                    two_year_period=2024,
                    max_pages=10
                ))
                all_contributions.extend(contributions)
            
            donor_aggregates = aggregate_contributions_by_donor(all_contributions) if all_contributions else {}
            top_donors = sorted(donor_aggregates.values(), key=lambda x: x["total_amount"], reverse=True)[:100]
            
            context.log.info(f"  👥 {len(all_contributions)} contributions")
            
            # Get independent expenditures
            independent_exp = list(get_independent_expenditures_for_candidate(
                fec_candidate_id,
                two_year_period=2024,
                max_pages=10
            ))
            
            committee_aggregates = aggregate_independent_expenditures_by_committee(independent_exp) if independent_exp else {}
            spenders_for = [c for c in committee_aggregates.values() if c["net_support"] > 0]
            spenders_against = [c for c in committee_aggregates.values() if c["net_support"] < 0]
            
            total_for = sum(c["support_amount"] for c in spenders_for)
            total_against = sum(c["oppose_amount"] for c in spenders_against)
            
            if independent_exp:
                context.log.info(f"  🎯 FOR: ${total_for:,.0f}, AGAINST: ${total_against:,.0f}")
            
            # Build document
            finance_doc = {
                "_id": bioguide_id,
                "bioguide_id": bioguide_id,
                "member_name": member_name,
                "state": member.get("state"),
                "office": member.get("office"),
                "fec_candidate_id": fec_candidate_id,
                "fec_committees": committees,
                "financial_summary": {
                    "total_receipts": financial_summary.get("receipts", 0) if financial_summary else 0,
                    "total_disbursements": financial_summary.get("disbursements", 0) if financial_summary else 0,
                    "cash_on_hand": financial_summary.get("cash_on_hand_end_period", 0) if financial_summary else 0,
                    "debts_owed": financial_summary.get("debts_owed_by_committee", 0) if financial_summary else 0,
                } if financial_summary else {},
                "direct_contributions": {
                    "total_amount": sum(c["contribution_receipt_amount"] for c in all_contributions),
                    "contribution_count": len(all_contributions),
                    "unique_donor_count": len(donor_aggregates),
                    "top_donors": top_donors,
                },
                "independent_expenditures": {
                    "for": {
                        "total_amount": total_for,
                        "expenditure_count": sum(c["support_count"] for c in spenders_for),
                        "top_spenders": sorted(spenders_for, key=lambda x: x["support_amount"], reverse=True)[:20]
                    },
                    "against": {
                        "total_amount": total_against,
                        "expenditure_count": sum(c["oppose_count"] for c in spenders_against),
                        "top_spenders": sorted(spenders_against, key=lambda x: x["oppose_amount"], reverse=True)[:20]
                    },
                    "net_support": total_for - total_against
                },
                "election_cycle": 2024
            }
            
            collection.replace_one({"_id": bioguide_id}, finance_doc, upsert=True)
            processed_count += 1
    
    context.log.info(f"✅ Processed {processed_count} members")
    
    return Output(
        value=processed_count,
        metadata={"members_processed": processed_count}
    )
