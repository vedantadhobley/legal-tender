"""LT Independent Expenditure Asset - Enriched independent expenditures.

This asset processes independent expenditure data (Super PAC spending FOR or AGAINST 
candidates) and enriches it with candidate details for easy analysis.

Input: fec_{cycle}.independent_expenditure (raw data with IDs)
Output: lt_{cycle}.independent_expenditure (enriched with names, offices, parties)

Key Features:
- Tracks support vs oppose spending separately
- Handles negative amounts (corrections/refunds)
- Filters to tracked members only (via member_fec_mapping)
- Denormalizes candidate info for easy querying

Use Cases:
- "Show me all Super PAC spending on Ted Cruz"
- "Which candidates received the most AIPAC support?"
- "How much did opposition groups spend against this candidate?"
"""

from typing import Dict, Any, List
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    AssetIn,
    Output,
    Config,
)

from src.resources.mongo import MongoDBResource


class LtIndependentExpenditureConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


@asset(
    name="lt_independent_expenditure",
    description="Enriched independent expenditure data for tracked members across all cycles",
    group_name="lt",
    compute_kind="computation",
    ins={
        "independent_expenditure": AssetIn("independent_expenditure"),
        "cn": AssetIn("cn"),
        "member_fec_mapping": AssetIn("member_fec_mapping"),
    },
)
def lt_independent_expenditure_asset(
    context: AssetExecutionContext,
    config: LtIndependentExpenditureConfig,
    mongo: MongoDBResource,
    independent_expenditure: Dict[str, Any],
    cn: Dict[str, Any],
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Process independent expenditures and enrich with candidate details.
    
    Data Flow:
    1. Load member_fec_mapping → get tracked member FEC IDs
    2. For each cycle:
       - Load cn → get candidate details
       - Load independent_expenditure → get Super PAC spending
       - Filter to tracked members only
       - Enrich with candidate names, offices, parties
       - Store in lt_{cycle}.independent_expenditure
    """
    context.log.info("=" * 80)
    context.log.info("💰 PROCESSING INDEPENDENT EXPENDITURES")
    context.log.info("=" * 80)
    
    overall_stats = {
        'cycles_processed': [],
        'total_expenditures': 0,
        'tracked_expenditures': 0,
        'by_cycle': {},
    }
    
    with mongo.get_client() as client:
        # ================================================================
        # Load Member FEC Mapping (once for all cycles)
        # ================================================================
        context.log.info("📥 Loading member FEC mapping...")
        context.log.info("-" * 80)
        
        member_mapping_collection = mongo.get_collection(
            client, "member_fec_mapping", database_name="legal_tender"
        )
        
        # Build set of tracked FEC candidate IDs
        tracked_candidate_ids = set()
        fec_to_bioguide = {}
        
        for member_doc in member_mapping_collection.find():
            bioguide_id = member_doc['_id']
            candidate_ids = member_doc.get('fec', {}).get('candidate_ids', [])
            
            for cand_id in candidate_ids:
                tracked_candidate_ids.add(cand_id)
                fec_to_bioguide[cand_id] = bioguide_id
        
        context.log.info(f"✅ Loaded {len(fec_to_bioguide)} tracked candidate IDs")
        context.log.info("")
        
        # ================================================================
        # Process Each Cycle
        # ================================================================
        for cycle in config.cycles:
            context.log.info("=" * 80)
            context.log.info(f"📊 {cycle} CYCLE")
            context.log.info("=" * 80)
            
            cycle_stats = {
                'total_expenditures': 0,
                'tracked_expenditures': 0,
                'support_spending': 0,
                'oppose_spending': 0,
                'corrections': 0,
                'unique_candidates': set(),
                'unique_committees': set(),
            }
            
            try:
                # Load Candidate Master
                context.log.info(f"📥 Loading candidate master ({cycle})...")
                cn_collection = mongo.get_collection(client, "cn", database_name=f"fec_{cycle}")
                
                candidate_details = {}
                for cn_doc in cn_collection.find():
                    cand_id = cn_doc.get('CAND_ID')
                    if cand_id:
                        candidate_details[cand_id] = {
                            'candidate_id': cand_id,
                            'candidate_name': cn_doc.get('CAND_NAME', ''),
                            'party': cn_doc.get('CAND_PTY_AFFILIATION', ''),
                            'office': cn_doc.get('CAND_OFFICE', ''),
                            'state': cn_doc.get('CAND_OFFICE_ST', ''),
                            'district': cn_doc.get('CAND_OFFICE_DISTRICT', ''),
                        }
                
                context.log.info(f"   ✅ {len(candidate_details)} candidate records")
                
                # Process Independent Expenditures
                context.log.info(f"🔨 Processing independent expenditures ({cycle})...")
                
                ie_collection = mongo.get_collection(
                    client, "independent_expenditure", database_name=f"fec_{cycle}"
                )
                
                lt_ie_collection = mongo.get_collection(
                    client, "independent_expenditure", database_name=f"lt_{cycle}"
                )
                
                # Clear existing data
                lt_ie_collection.delete_many({})
                
                batch = []
                batch_size = 5000
                processed = 0
                
                for ie_doc in ie_collection.find():
                    cycle_stats['total_expenditures'] += 1
                    
                    # Get candidate ID
                    cand_id = ie_doc.get('CAND_ID')
                    
                    # Filter: Only tracked members
                    if not cand_id or cand_id not in tracked_candidate_ids:
                        continue
                    
                    # Parse amount
                    amount_str = ie_doc.get('EXP_AMO', '0')
                    try:
                        amount = float(str(amount_str).replace(',', '')) if amount_str else 0
                    except (ValueError, AttributeError, TypeError):
                        amount = 0
                    
                    # Get support/oppose indicator
                    sup_opp = ie_doc.get('SUP_OPP', '')
                    is_support = sup_opp == 'S'
                    is_oppose = sup_opp == 'O'
                    is_correction = amount < 0
                    
                    # Get candidate details
                    cand_info = candidate_details.get(cand_id, {})
                    bioguide_id = fec_to_bioguide.get(cand_id)
                    
                    # Build enriched record
                    enriched_record = {
                        # Candidate (target) fields
                        'candidate_id': cand_id,
                        'candidate_name': cand_info.get('candidate_name', ''),
                        'candidate_party': cand_info.get('party', ''),
                        'candidate_office': cand_info.get('office', ''),
                        'candidate_state': cand_info.get('state', ''),
                        'candidate_district': cand_info.get('district', ''),
                        'bioguide_id': bioguide_id,
                        'is_tracked_member': True,
                        
                        # Spender (from) fields - USE CORRECT FEC FIELD NAMES
                        'committee_id': ie_doc.get('SPE_ID', ''),
                        'committee_name': ie_doc.get('SPE_NAM', ''),
                        'spender_name': ie_doc.get('SPE_NAM', ''),  # Same as committee name for independent expenditures
                        
                        # Expenditure details - USE CORRECT FEC FIELD NAMES
                        'amount': amount,
                        'support_oppose': sup_opp,
                        'is_support': is_support,
                        'is_oppose': is_oppose,
                        'is_correction': is_correction,
                        'expenditure_date': ie_doc.get('EXP_DATE', ''),
                        'expenditure_description': ie_doc.get('PUR', ''),
                        'payee_name': ie_doc.get('PAY', ''),
                        
                        # Original fields for reference
                        'transaction_id': ie_doc.get('TRAN_ID', ''),
                        'image_number': ie_doc.get('IMAGE_NUM', ''),
                        'file_number': ie_doc.get('FILE_NUM', ''),
                        
                        # Metadata
                        'cycle': cycle,
                        'source_file': 'independent_expenditure',
                        'computed_at': datetime.now(),
                    }
                    
                    batch.append(enriched_record)
                    
                    # Update stats
                    cycle_stats['tracked_expenditures'] += 1
                    if is_support:
                        cycle_stats['support_spending'] += amount
                    elif is_oppose:
                        cycle_stats['oppose_spending'] += amount
                    if is_correction:
                        cycle_stats['corrections'] += 1
                    cycle_stats['unique_candidates'].add(cand_id)
                    cycle_stats['unique_committees'].add(ie_doc.get('SPE_ID', ''))
                    
                    # Insert batch
                    if len(batch) >= batch_size:
                        lt_ie_collection.insert_many(batch)
                        processed += len(batch)
                        context.log.info(f"   💾 Inserted {processed:,} records...")
                        batch = []
                
                # Insert remaining
                if batch:
                    lt_ie_collection.insert_many(batch)
                    processed += len(batch)
                
                # Create Indexes
                lt_ie_collection.create_index([("bioguide_id", 1)])
                lt_ie_collection.create_index([("candidate_id", 1)])
                lt_ie_collection.create_index([("committee_id", 1)])
                lt_ie_collection.create_index([("committee_name", 1)])
                lt_ie_collection.create_index([("support_oppose", 1)])
                lt_ie_collection.create_index([("amount", -1)])
                lt_ie_collection.create_index([("expenditure_date", -1)])
                
                context.log.info(f"✅ {cycle}: {cycle_stats['tracked_expenditures']:,} tracked expenditures")
                context.log.info(f"   Support: ${cycle_stats['support_spending']:,.2f} | Oppose: ${cycle_stats['oppose_spending']:,.2f}")
                context.log.info("")
                
                # Update overall stats
                overall_stats['cycles_processed'].append(cycle)
                overall_stats['total_expenditures'] += cycle_stats['total_expenditures']
                overall_stats['tracked_expenditures'] += cycle_stats['tracked_expenditures']
                overall_stats['by_cycle'][cycle] = {
                    'total': cycle_stats['total_expenditures'],
                    'tracked': cycle_stats['tracked_expenditures'],
                    'support': cycle_stats['support_spending'],
                    'oppose': cycle_stats['oppose_spending'],
                    'candidates': len(cycle_stats['unique_candidates']),
                    'committees': len(cycle_stats['unique_committees']),
                }
                
            except Exception as e:
                context.log.error(f"❌ Error processing {cycle}: {e}")
    
    # Summary
    context.log.info("=" * 80)
    context.log.info("📊 PROCESSING SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Cycles Processed: {', '.join(overall_stats['cycles_processed'])}")
    context.log.info(f"Total Expenditures: {overall_stats['total_expenditures']:,}")
    context.log.info(f"Tracked Expenditures: {overall_stats['tracked_expenditures']:,}")
    for cycle, stats in overall_stats['by_cycle'].items():
        context.log.info(f"  {cycle}: {stats['tracked']:,} tracked (Support: ${stats['support']:,.2f}, Oppose: ${stats['oppose']:,.2f})")
    context.log.info("=" * 80)
    
    return Output(
        value=overall_stats,
        metadata={
            "cycles": overall_stats['cycles_processed'],
            "total": overall_stats['total_expenditures'],
            "tracked": overall_stats['tracked_expenditures'],
        }
    )
