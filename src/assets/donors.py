"""Election/donor-related data assets."""
from typing import List, Dict, Any

from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue, Output
from src.api.election_api import get_candidate_committees, get_committee_contributions
from src.resources.mongo import MongoDBResource


@asset(
    name="member_donor_data",
    description="Donor contributions for all Congress members from OpenFEC API",
    group_name="donors",
    compute_kind="api",
    ins={
        "congress_members": AssetIn(key="congress_members"),  # Depends on congress_members asset
    },
    metadata={
        "source": "OpenFEC API",
        "cycle": "2024",
    }
)
def member_donor_data_asset(
    context: AssetExecutionContext,
    mongo: MongoDBResource,
    congress_members: List[Dict[str, Any]]
) -> Output[List[Dict[str, Any]]]:
    """Fetch donor data for all Congress members and persist to MongoDB.
    
    This asset:
    1. Takes congress_members as input (Dagster tracks this dependency!)
    2. For each member, fetches their FEC candidate committees
    3. For each committee, fetches contribution data
    4. Stores all donations in MongoDB with member references
    5. Returns metadata about the operation
    
    Args:
        congress_members: List of member dicts (from upstream asset)
        
    Returns:
        List of donation records with metadata
    """
    context.log.info("=" * 60)
    context.log.info("💰 FETCHING DONOR DATA FOR CONGRESS MEMBERS")
    context.log.info("=" * 60)
    context.log.info(f"📊 Processing {len(congress_members)} members...")
    context.log.info("")
    
    all_donations = []
    members_with_data = 0
    members_without_data = 0
    total_donations_fetched = 0
    
    with mongo.get_client() as client:
        donations_collection = mongo.get_collection(client, "donations")
        
        for idx, member in enumerate(congress_members, 1):
            member_id = member.get("bioguideId")
            member_name = f"{member.get('firstName', '')} {member.get('lastName', '')}".strip()
            fec_candidate_id = member.get("fecCandidateId")  # If available in Congress API
            
            if not member_id:
                continue
            
            context.log.info(f"[{idx}/{len(congress_members)}] Processing {member_name} ({member_id})...")
            
            # Skip if no FEC candidate ID (you might need to match by name or skip)
            if not fec_candidate_id:
                context.log.warning(f"   ⚠️  No FEC candidate ID for {member_name}")
                members_without_data += 1
                continue
            
            # Get committees for this candidate
            committees = get_candidate_committees(fec_candidate_id, cycle=2024)
            
            if not committees:
                context.log.warning(f"   ⚠️  No committees found for {member_name}")
                members_without_data += 1
                continue
            
            context.log.info(f"   ✅ Found {len(committees)} committees")
            
            # Fetch contributions for each committee
            for committee_id in committees:
                contributions = get_committee_contributions(committee_id, cycle=2024)
                
                if not contributions or "results" not in contributions:
                    continue
                
                results = contributions["results"]
                context.log.info(f"      💵 {len(results)} contributions from {committee_id}")
                
                # Store each contribution with member reference
                for contribution in results:
                    donation_doc = {
                        **contribution,
                        "member_bioguide_id": member_id,
                        "member_name": member_name,
                        "committee_id": committee_id,
                        "cycle": 2024,
                    }
                    
                    # Use a unique ID if available, otherwise create one
                    donation_id = contribution.get("sub_id") or f"{committee_id}_{contribution.get('contributor_id', idx)}"
                    donation_doc["_id"] = donation_id
                    
                    donations_collection.replace_one(
                        {"_id": donation_id},
                        donation_doc,
                        upsert=True
                    )
                    
                    all_donations.append(donation_doc)
                    total_donations_fetched += 1
            
            members_with_data += 1
    
    context.log.info("")
    context.log.info("📊 DONOR DATA SYNC SUMMARY:")
    context.log.info("-" * 60)
    context.log.info(f"   ✅ Members with data: {members_with_data}")
    context.log.info(f"   ⚠️  Members without data: {members_without_data}")
    context.log.info(f"   💵 Total donations fetched: {total_donations_fetched}")
    context.log.info("-" * 60)
    context.log.info("🎉 DONOR DATA SYNC COMPLETE!")
    context.log.info("=" * 60)
    
    # Return with metadata
    return Output(
        value=all_donations,
        metadata={
            "members_processed": len(congress_members),
            "members_with_data": members_with_data,
            "members_without_data": members_without_data,
            "total_donations": total_donations_fetched,
            "avg_donations_per_member": total_donations_fetched / members_with_data if members_with_data > 0 else 0,
            "sample_donation": MetadataValue.json(all_donations[0] if all_donations else {}),
            "cycle": "2024",
        }
    )
