"""Employers Vertex Collection - Unique employers extracted from donors.

Creates employer vertices for tracking corporate influence through employee donations.
Only employers with donors above the threshold are included.

Source: aggregation.donors
Target: aggregation.employers (vertex collection)
"""

from typing import Dict, Any
from datetime import datetime
from hashlib import sha256
import re

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


def normalize_employer(employer: str) -> str:
    """Normalize employer name for deduplication."""
    if not employer:
        return ""
    employer = employer.upper().strip()
    employer = re.sub(r'[,.]', '', employer)
    employer = re.sub(r'\s+', ' ', employer)
    return employer


def make_employer_key(employer: str) -> str:
    """Create unique _key for employer."""
    normalized = normalize_employer(employer)
    return sha256(normalized.encode()).hexdigest()[:16]


@asset(
    name="employers",
    description="Employer vertices - companies with employees who donated $10K+ in any cycle.",
    group_name="graph",
    compute_kind="graph_vertex",
    deps=["donors"],  # Depends on donors being created first
)
def employers_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create employers vertex collection from donor employer data.
    
    Aggregates all donors by employer and creates employer vertices with:
    - Total contributions from all employees
    - Count of employees (donors)
    - Per-cycle breakdown
    """
    
    with arango.get_client() as client:
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("donors"):
            raise RuntimeError("donors collection does not exist - run donors asset first")
        
        # Create/truncate employers collection
        if agg_db.has_collection("employers"):
            agg_db.collection("employers").truncate()
            context.log.info("Truncated existing employers collection")
        else:
            agg_db.create_collection("employers")
            context.log.info("Created employers collection")
        
        employers_coll = agg_db.collection("employers")
        
        context.log.info("📊 Aggregating employers from donors...")
        
        # Aggregate donors by employer using AQL
        aql = """
        FOR donor IN donors
            FILTER donor.canonical_employer != null 
            FILTER donor.canonical_employer != ""
            FILTER donor.canonical_employer NOT IN ["NOT EMPLOYED", "SELF-EMPLOYED", "SELF EMPLOYED", "RETIRED", "NONE", "N/A"]
            
            COLLECT employer = donor.canonical_employer
            AGGREGATE 
                total_from_employees = SUM(donor.total_amount),
                employee_count = COUNT(1),
                total_transactions = SUM(donor.transaction_count)
            
            SORT total_from_employees DESC
            
            RETURN {
                employer: employer,
                total_from_employees: total_from_employees,
                employee_donor_count: employee_count,
                total_transactions: total_transactions
            }
        """
        
        cursor = agg_db.aql.execute(aql, ttl=3600, batch_size=10000)
        
        batch = []
        inserted = 0
        total_employers = 0
        
        for record in cursor:
            total_employers += 1
            
            employer_key = make_employer_key(record['employer'])
            
            employer_doc = {
                '_key': employer_key,
                'name': record['employer'],
                'total_from_employees': record['total_from_employees'],
                'employee_donor_count': record['employee_donor_count'],
                'total_transactions': record['total_transactions'],
                'updated_at': datetime.now().isoformat()
            }
            
            batch.append(employer_doc)
            
            if len(batch) >= 10000:
                employers_coll.import_bulk(batch, on_duplicate="replace")
                inserted += len(batch)
                batch = []
                context.log.info(f"  Inserted {inserted:,} employers...")
        
        # Insert remaining
        if batch:
            employers_coll.import_bulk(batch, on_duplicate="replace")
            inserted += len(batch)
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        employers_coll.add_persistent_index(fields=["name"])
        employers_coll.add_persistent_index(fields=["total_from_employees"])
        employers_coll.add_persistent_index(fields=["employee_donor_count"])
        
        context.log.info(f"🎉 Complete: {total_employers:,} employers in aggregation.employers")
        
        return Output(
            value={"employers_count": total_employers},
            metadata={
                "employers_count": total_employers,
                "arangodb_database": "aggregation",
                "arangodb_collection": "employers",
            }
        )
