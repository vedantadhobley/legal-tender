"""Employed By Edge Collection - Links donors to their employers.

Creates edges from donors to employers for tracking corporate influence
through employee donations.

Source: aggregation.donors
Target: aggregation.employed_by (edge collection)
"""

from typing import Dict, Any
from datetime import datetime
from hashlib import sha256
import re

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


def normalize_employer(employer: str) -> str:
    """Normalize employer name."""
    if not employer:
        return ""
    employer = employer.upper().strip()
    employer = re.sub(r'[,.]', '', employer)
    employer = re.sub(r'\s+', ' ', employer)
    return employer


def make_employer_key(employer: str) -> str:
    """Create employer _key."""
    normalized = normalize_employer(employer)
    return sha256(normalized.encode()).hexdigest()[:16]


@asset(
    name="employed_by",
    description="Edges from donors to employers - tracks corporate influence through employee donations.",
    group_name="graph",
    compute_kind="graph_edge",
    deps=["donors", "employers"],
)
def employed_by_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create employed_by edge collection linking donors to employers.
    
    Edge structure:
    {
        _from: "donors/{donor_key}",
        _to: "employers/{employer_key}"
    }
    """
    
    with arango.get_client() as client:
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Create/truncate employed_by edge collection
        if agg_db.has_collection("employed_by"):
            agg_db.collection("employed_by").truncate()
            context.log.info("Truncated existing employed_by collection")
        else:
            agg_db.create_collection("employed_by", edge=True)
            context.log.info("Created employed_by edge collection")
        
        edges_coll = agg_db.collection("employed_by")
        
        # Get valid employers
        valid_employers = set()
        cursor = agg_db.aql.execute("FOR e IN employers RETURN e._key", ttl=3600)
        for key in cursor:
            valid_employers.add(key)
        
        context.log.info(f"📋 Found {len(valid_employers):,} valid employers")
        
        # Skip non-employment values
        skip_employers = {
            "NOT EMPLOYED", "SELF-EMPLOYED", "SELF EMPLOYED", 
            "RETIRED", "NONE", "N/A", ""
        }
        
        context.log.info("🔗 Building employed_by edges...")
        
        # Query donors and create edges to employers
        cursor = agg_db.aql.execute(
            """
            FOR donor IN donors
                FILTER donor.canonical_employer != null
                FILTER donor.canonical_employer NOT IN @skip
                RETURN {
                    donor_key: donor._key,
                    employer: donor.canonical_employer
                }
            """,
            bind_vars={"skip": list(skip_employers)},
            ttl=3600,
            batch_size=5000,
            stream=True
        )
        
        batch = []
        inserted = 0
        skipped = 0
        
        for record in cursor:
            employer_key = make_employer_key(record['employer'])
            
            # Skip if employer not in our employers collection
            if employer_key not in valid_employers:
                skipped += 1
                continue
            
            edge = {
                '_key': f"{record['donor_key']}_{employer_key}",
                '_from': f"donors/{record['donor_key']}",
                '_to': f"employers/{employer_key}",
                'updated_at': datetime.now().isoformat()
            }
            
            batch.append(edge)
            
            if len(batch) >= 10000:
                edges_coll.import_bulk(batch, on_duplicate="replace")
                inserted += len(batch)
                batch = []
                if inserted % 50000 == 0:
                    context.log.info(f"  Inserted {inserted:,} edges...")
        
        if batch:
            edges_coll.import_bulk(batch, on_duplicate="replace")
            inserted += len(batch)
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        edges_coll.add_persistent_index(fields=["_from"])
        edges_coll.add_persistent_index(fields=["_to"])
        
        context.log.info(f"🎉 Complete: {inserted:,} employed_by edges (skipped {skipped:,})")
        
        return Output(
            value={"edges_created": inserted, "skipped": skipped},
            metadata={
                "edges_count": inserted,
                "skipped": skipped,
                "arangodb_database": "aggregation",
                "arangodb_collection": "employed_by",
            }
        )
