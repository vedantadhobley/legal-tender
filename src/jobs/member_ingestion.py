"""Member ingestion job for syncing Congress members to MongoDB."""
from dagster import job, op, OpExecutionContext, Out, In
from pymongo import MongoClient
import os
from src.api.congress_api import get_members

MONGO_URI = os.getenv("MONGO_URI", "mongodb://ltuser:ltpass@mongo:27017/admin")
MONGO_DB = os.getenv("MONGO_DB", "legal_tender")


@op(
    tags={"kind": "fetch"},
    description="Fetches current members from Congress API",
    out=Out(list, description="List of member dictionaries from Congress API")
)
def fetch_congress_members(context: OpExecutionContext) -> list:
    """Fetch all current members from Congress API.
    
    Returns:
        list: List of member dictionaries
    """
    context.log.info("=" * 60)
    context.log.info("🏛️  FETCHING CONGRESS MEMBERS")
    context.log.info("=" * 60)
    context.log.info("📡 Connecting to Congress.gov API...")
    context.log.info("   Source: https://api.congress.gov/v3/member")
    context.log.info("   Congress: 118 (Current)")
    context.log.info("   Chambers: House + Senate")
    context.log.info("")
    
    members = get_members() or []
    
    context.log.info("✅ API Response Received")
    context.log.info(f"📊 Total Members Fetched: {len(members)}")
    context.log.info("=" * 60)
    
    return members


@op(
    tags={"kind": "storage"},
    description="Upserts members to MongoDB and removes obsolete records",
    ins={"members": In(list, description="List of members to upsert")}
)
def upsert_members_to_db(context: OpExecutionContext, members: list) -> dict:
    """Upsert members to MongoDB and remove obsolete members.
    
    Args:
        members: List of member dictionaries from Congress API
        
    Returns:
        dict: Statistics about the upsert operation
    """
    context.log.info("=" * 60)
    context.log.info("💾 SYNCING TO DATABASE")
    context.log.info("=" * 60)
    context.log.info(f"📦 Processing {len(members)} member records...")
    context.log.info(f"🗄️  Database: {MONGO_DB}")
    context.log.info("📂 Collection: members")
    context.log.info("")
    
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db["members"]
    
    upserted = 0
    skipped = 0
    current_ids = set()
    
    context.log.info("🔄 Upserting member records...")
    
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
    
    # Remove any members not in the current API response
    context.log.info("")
    context.log.info("🧹 Cleaning up obsolete records...")
    result = collection.delete_many({"_id": {"$nin": list(current_ids)}})
    deleted = result.deleted_count
    
    if deleted > 0:
        context.log.info(f"🗑️  Deleted: {deleted} obsolete records")
    else:
        context.log.info("✨ No obsolete records found")
    
    client.close()
    
    stats = {
        "upserted": upserted,
        "skipped": skipped,
        "deleted": deleted,
        "total_in_db": upserted
    }
    
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
    
    return stats


@job(
    description="Ingests all current members from the Congress API and upserts them into MongoDB",
    tags={"team": "data", "priority": "high", "schedule": "daily"}
)
def member_ingestion_job():
    """Job to ingest current Congress members into MongoDB."""
    members = fetch_congress_members()
    upsert_members_to_db(members)
