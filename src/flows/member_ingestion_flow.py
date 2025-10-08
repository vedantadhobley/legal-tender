from prefect import flow, task
from pymongo import MongoClient
import os
from datetime import datetime, timezone
from src.api.congress_api import get_members

MONGO_URI = os.getenv("MONGO_URI", "mongodb://ltuser:ltpass@mongo:27017/admin")
MONGO_DB = os.getenv("MONGO_DB", "legal_tender")

@task
def upsert_members(members):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db["members"]
    upserted = 0
    skipped = 0
    for member in members:
        member_doc = dict(member)
        member_id = member_doc.get("bioguideId")
        if not member_id:
            print(f"Skipping member with missing bioguideId: keys={list(member_doc.keys())}")
            skipped += 1
            continue
        member_doc["_id"] = member_id
        collection.replace_one({"_id": member_doc["_id"]}, member_doc, upsert=True)
        upserted += 1
    print(f"Upserted {upserted} members, skipped {skipped} with missing bioguideId.")
    client.close()

@flow(name="Member Ingestion Flow")
def member_ingestion_flow():
    now = datetime.now(timezone.utc)
    flow_run_name = now.strftime("member-ingestion %Y-%m-%d %H:%M:%S UTC")
    members = get_members() or []
    upsert_members.with_options(name=f"Upsert Members ({flow_run_name})")(members)