"""
LT webk Asset - FEC PAC/Committee Financial Summaries

Filters webk.zip data to committees linked to tracked members and enriches with context.
This contains OFFICIAL FEC summary data for PACs and party committees.

Input: fec_{cycle}.webk (raw FEC PAC/committee summaries)
Output: lt_{cycle}.webk (filtered to relevant committees)

Key Features:
- Filters to committees linked to tracked members (via ccl linkages)
- Already contains all committee info (no enrichment needed)
- Official FEC financial summary (not transaction-level)
- Shows PAC funding sources (individual vs PAC contributions)
- Includes independent expenditure totals (IND_EXP field)

Use Cases:
- "Show me FEC's official summary for AIPAC"
- "Where does this PAC get its money? (INDV_CONTRIB field)"
- "Cross-check: Does webk.IND_EXP match our 24A/24E calculation?"
- "How much did this PAC give to other committees?"
"""

from typing import Dict, Any, List, Set
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


class LtWebkConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="lt_webk",
    description="FEC PAC/committee financial summaries filtered to committees linked to tracked members",
    group_name="lt",
    compute_kind="computation",
    ins={
        "webk": AssetIn("webk"),
        "ccl": AssetIn("ccl"),
        "member_fec_mapping": AssetIn("member_fec_mapping"),
    },
)
def lt_webk_asset(
    context: AssetExecutionContext,
    config: LtWebkConfig,
    mongo: MongoDBResource,
    webk: Dict[str, Any],
    ccl: Dict[str, Any],
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Filter webk PAC/committee summaries to committees linked to tracked members.
    
    This provides FEC's OFFICIAL committee/PAC financial summary, including:
    - Where PACs get their money (individual vs PAC contributions)
    - How much they spend (independent expenditures, contributions to other committees)
    - Financial health (cash on hand, debts)
    """
    
    stats = {
        'total_committees': 0,
        'by_cycle': {}
    }
    
    with mongo.get_client() as client:
        # Get member FEC mapping to get tracked candidate IDs
        mapping_collection = mongo.get_collection(client, "member_fec_mapping", database_name="legal_tender")
        
        tracked_candidate_ids: Set[str] = set()
        cand_to_bioguide = {}
        for doc in mapping_collection.find():
            bioguide_id = doc.get('_id')  # bioguide_id is the _id
            candidate_ids = doc.get('fec', {}).get('candidate_ids', [])
            for cand_id in candidate_ids:
                tracked_candidate_ids.add(cand_id)
                cand_to_bioguide[cand_id] = bioguide_id
        
        context.log.info(f"Found {len(tracked_candidate_ids)} tracked candidate IDs")
        
        # Process each cycle
        for cycle in config.cycles:
            context.log.info(f"Processing cycle {cycle}")
            
            ccl_collection = mongo.get_collection(client, "ccl", database_name=f"fec_{cycle}")
            webk_collection = mongo.get_collection(client, "webk", database_name=f"fec_{cycle}")
            lt_webk_collection = mongo.get_collection(client, "webk", database_name=f"lt_{cycle}")
            
            # Clear existing data
            lt_webk_collection.delete_many({})
            
            # Get committees linked to tracked candidates
            linked_committee_ids: Set[str] = set()
            for doc in ccl_collection.find({"CAND_ID": {"$in": list(tracked_candidate_ids)}}):
                cmte_id = doc.get('CMTE_ID')
                if cmte_id:
                    linked_committee_ids.add(cmte_id)
            
            context.log.info(f"  Found {len(linked_committee_ids)} committees linked to tracked candidates")
            
            # Filter webk to linked committees
            batch = []
            for doc in webk_collection.find({"CMTE_ID": {"$in": list(linked_committee_ids)}}):
                # Add enrichment fields
                enriched_doc = {
                    **doc,
                    'is_linked_to_tracked_member': True,
                    'cycle': cycle,
                    'source_file': 'webk',
                    'computed_at': datetime.now()
                }
                
                batch.append(enriched_doc)
            
            # Insert batch
            if batch:
                lt_webk_collection.insert_many(batch, ordered=False)
                context.log.info(f"  ✅ {cycle}: {len(batch)} committees")
                stats['by_cycle'][cycle] = len(batch)
                stats['total_committees'] += len(batch)
            else:
                context.log.warning(f"  ⚠️  {cycle}: No linked committees found")
                stats['by_cycle'][cycle] = 0
            
            # Create indexes
            lt_webk_collection.create_index([("CMTE_ID", 1)])
            lt_webk_collection.create_index([("CMTE_NM", 1)])
            lt_webk_collection.create_index([("TTL_RECEIPTS", -1)])
            lt_webk_collection.create_index([("IND_EXP", -1)])  # Independent expenditures
    
    return Output(
        value=stats,
        metadata={
            "total_committees": stats['total_committees'],
            "cycles_processed": MetadataValue.json(list(stats['by_cycle'].keys())),
            "committees_per_cycle": MetadataValue.json(stats['by_cycle']),
        }
    )
