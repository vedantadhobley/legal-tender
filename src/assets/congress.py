"""Congress-related data assets."""
from typing import List, Dict, Any

from dagster import asset, AssetExecutionContext, MetadataValue, Output
from src.api.congress_api import get_members
from src.resources.mongo import MongoDBResource


@asset(
    name="congress_members",
    description="Current members of the U.S. Congress (House and Senate) from Congress.gov API",
    group_name="congress",
    compute_kind="api",
    metadata={
        "source": "Congress.gov API",
        "congress": "118",
        "chambers": "House + Senate",
    }
)
def congress_members_asset(
    context: AssetExecutionContext,
    mongo: MongoDBResource
) -> Output[List[Dict[str, Any]]]:
    """Fetch and persist current Congress members to MongoDB.
    
    This asset:
    1. Fetches all current members from Congress.gov API
    2. Upserts them to MongoDB (keyed by bioguideId)
    3. Removes obsolete members no longer in the API response
    4. Returns metadata about the operation
    
    Returns:
        List of member dictionaries with metadata attached
    """
    context.log.info("=" * 60)
    context.log.info("🏛️  FETCHING CONGRESS MEMBERS")
    context.log.info("=" * 60)
    context.log.info("📡 Connecting to Congress.gov API...")
    context.log.info("   Source: https://api.congress.gov/v3/member")
    context.log.info("   Congress: 118 (Current)")
    context.log.info("   Chambers: House + Senate")
    context.log.info("")
    
    # Fetch members from API
    members = get_members() or []
    
    context.log.info("✅ API Response Received")
    context.log.info(f"📊 Total Members Fetched: {len(members)}")
    context.log.info("=" * 60)
    
    # Persist to MongoDB
    context.log.info("💾 SYNCING TO DATABASE")
    context.log.info("=" * 60)
    
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "members")
        
        upserted = 0
        skipped = 0
        current_ids = set()
        
        context.log.info(f"📦 Processing {len(members)} member records...")
        
        for member in members:
            member_doc = dict(member)
            member_id = member_doc.get("bioguideId")
            
            if not member_id:
                context.log.warning(
                    f"⚠️  Skipping member with missing bioguideId: {list(member_doc.keys())[:5]}..."
                )
                skipped += 1
                continue
                
            member_doc["_id"] = member_id
            collection.replace_one({"_id": member_doc["_id"]}, member_doc, upsert=True)
            upserted += 1
            current_ids.add(member_id)
        
        context.log.info(f"✅ Upserted: {upserted} records")
        if skipped > 0:
            context.log.warning(f"⚠️  Skipped: {skipped} records (missing bioguideId)")
        
        # Clean up obsolete members
        context.log.info("")
        context.log.info("🧹 Cleaning up obsolete records...")
        result = collection.delete_many({"_id": {"$nin": list(current_ids)}})
        deleted = result.deleted_count
        
        if deleted > 0:
            context.log.info(f"🗑️  Deleted: {deleted} obsolete records")
        else:
            context.log.info("✨ No obsolete records found")
    
    # Summary
    context.log.info("")
    context.log.info("📊 SYNC SUMMARY:")
    context.log.info("-" * 60)
    context.log.info(f"   ✅ Records Upserted: {upserted}")
    context.log.info(f"   ⚠️  Records Skipped:  {skipped}")
    context.log.info(f"   🗑️  Records Deleted:  {deleted}")
    context.log.info(f"   💾 Total in Database: {upserted}")
    context.log.info("-" * 60)
    context.log.info("🎉 DATABASE SYNC COMPLETE!")
    context.log.info("=" * 60)
    
    # Return with rich metadata that Dagster will track
    return Output(
        value=members,
        metadata={
            "total_fetched": len(members),
            "upserted": upserted,
            "skipped": skipped,
            "deleted": deleted,
            "total_in_db": upserted,
            "preview": MetadataValue.json(members[:3] if members else []),  # Show first 3 members
            "api_source": "https://api.congress.gov/v3/member",
            "congress_session": "118",
        }
    )
