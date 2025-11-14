"""
LT weball Asset - FEC Candidate Financial Summaries (All Activity)

Filters weball.zip data to tracked members only and enriches with bioguide IDs.
This contains OFFICIAL FEC summary data for ALL candidates with activity in the period,
regardless of when they're up for election.

Input: fec_{cycle}.weball (raw FEC all-candidate summaries)
Output: enriched_{cycle}.weball (filtered to tracked members)

Key Features:
- Filters to tracked members via member_fec_mapping
- Already contains all candidate info (no enrichment needed)
- Official FEC financial summary (not transaction-level)
- Includes candidates raising money for FUTURE cycles

Difference from webl:
- webl = only candidates running THIS cycle
- weball = ALL candidates with ANY activity this cycle (broader)

Use Cases:
- "Which 2026 candidates are already raising money in 2024?"
- "Show me all fundraising activity for tracked members"
- "Compare webl vs weball to spot early campaign activity"
"""

from typing import Dict, Any, List
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    AssetIn,
    Output,
    Config,
    MetadataValue,
)

from src.resources.mongo import MongoDBResource


class EnrichedWeballConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="enriched_weball",
    description="FEC all-candidate financial summaries filtered to tracked members (includes future cycle activity)",
    group_name="enrichment",
    compute_kind="enrichment",
    ins={
        "weball": AssetIn("weball"),
        "member_fec_mapping": AssetIn("member_fec_mapping"),
    },
)
def enriched_weball_asset(
    context: AssetExecutionContext,
    config: EnrichedWeballConfig,
    mongo: MongoDBResource,
    weball: Dict[str, Any],
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Filter weball all-candidate summaries to tracked members.
    
    This provides FEC's OFFICIAL candidate financial summary for ALL candidates
    with activity in the period, including those raising for future cycles.
    """
    
    stats = {
        'total_candidates': 0,
        'by_cycle': {}
    }
    
    with mongo.get_client() as client:
        # Get member FEC mapping
        mapping_collection = mongo.get_collection(client, "member_fec_mapping", database_name="aggregation")
        
        # Build lookup: candidate_id -> bioguide_id
        cand_to_bioguide = {}
        for doc in mapping_collection.find():
            bioguide_id = doc.get('_id')  # bioguide_id is the _id
            candidate_ids = doc.get('fec', {}).get('candidate_ids', [])
            for cand_id in candidate_ids:
                cand_to_bioguide[cand_id] = bioguide_id
        
        context.log.info(f"Found {len(cand_to_bioguide)} tracked candidate IDs")
        
        # Process each cycle
        for cycle in config.cycles:
            context.log.info(f"Processing cycle {cycle}")
            
            weball_collection = mongo.get_collection(client, "weball", database_name=f"fec_{cycle}")
            enriched_weball_collection = mongo.get_collection(client, "weball", database_name=f"enriched_{cycle}")
            
            # Clear existing data
            enriched_weball_collection.delete_many({})
            
            # Filter to tracked candidates
            batch = []
            for doc in weball_collection.find():
                cand_id = doc.get('CAND_ID')
                
                # Only include tracked candidates
                if cand_id not in cand_to_bioguide:
                    continue
                
                # Add enrichment fields
                enriched_doc = {
                    **doc,
                    'bioguide_id': cand_to_bioguide[cand_id],
                    'is_tracked_member': True,
                    'cycle': cycle,
                    'source_file': 'weball',
                    'computed_at': datetime.now()
                }
                
                batch.append(enriched_doc)
            
            # Insert batch
            if batch:
                enriched_weball_collection.insert_many(batch, ordered=False)
                context.log.info(f"  ✅ {cycle}: {len(batch)} tracked candidates")
                stats['by_cycle'][cycle] = len(batch)
                stats['total_candidates'] += len(batch)
            else:
                context.log.warning(f"  ⚠️  {cycle}: No tracked candidates found")
                stats['by_cycle'][cycle] = 0
            
            # Create indexes
            enriched_weball_collection.create_index([("CAND_ID", 1)])
            enriched_weball_collection.create_index([("bioguide_id", 1)])
            enriched_weball_collection.create_index([("CAND_NAME", 1)])
            enriched_weball_collection.create_index([("TTL_RECEIPTS", -1)])
    
    return Output(
        value=stats,
        metadata={
            "total_candidates": stats['total_candidates'],
            "cycles_processed": MetadataValue.json(list(stats['by_cycle'].keys())),
            "candidates_per_cycle": MetadataValue.json(stats['by_cycle']),
        }
    )
