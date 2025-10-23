"""LT Operating Expenditures Asset - Enriched committee spending.

This asset processes operating expenditure data (WHERE committees spend money - 
vendors, ads, services) and enriches it by linking committees to candidates.

Input: fec_{cycle}.oppexp (raw operating expenditure data)
Output: enriched_{cycle}.oppexp (enriched with committee→candidate linkages)

Key Features:
- Links committee spending to their candidates via ccl
- Enriches with committee details (name, type, party)
- Enriches with candidate details (name, office, party)
- Filters to tracked member committees only
- Tracks spending categories and purposes

Use Cases:
- "Where did Ted Cruz's campaign spend money?"
- "Which vendors received the most from tracked member committees?"
- "How much did committees spend on advertising?"
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


class EnrichedOppexpConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


@asset(
    name="enriched_oppexp",
    description="Enriched operating expenditures for tracked member committees across all cycles",
    group_name="enrichment",
    compute_kind="enrichment",
    ins={
        "oppexp": AssetIn("oppexp"),
        "cn": AssetIn("cn"),
        "cm": AssetIn("cm"),
        "ccl": AssetIn("ccl"),
        "member_fec_mapping": AssetIn("member_fec_mapping"),
    },
)
def enriched_oppexp_asset(
    context: AssetExecutionContext,
    config: EnrichedOppexpConfig,
    mongo: MongoDBResource,
    oppexp: Dict[str, Any],
    cn: Dict[str, Any],
    cm: Dict[str, Any],
    ccl: Dict[str, Any],
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Process operating expenditures and enrich with committee→candidate context.
    
    Data Flow:
    1. Load member_fec_mapping → get tracked member FEC IDs
    2. For each cycle:
       - Load ccl → map committee IDs to candidate IDs
       - Load cn → get candidate details
       - Load cm → get committee details
       - Load oppexp → get spending records
       - Filter to tracked member committees
       - Enrich with full context
       - Store in enriched_{cycle}.oppexp
    """
    context.log.info("=" * 80)
    context.log.info("💰 PROCESSING OPERATING EXPENDITURES")
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
            client, "member_fec_mapping", database_name="aggregation"
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
                'total_amount': 0,
                'unique_committees': set(),
                'unique_candidates': set(),
                'unique_recipients': set(),
            }
            
            try:
                # Load Committee→Candidate Linkages
                context.log.info(f"📥 Loading committee→candidate linkages ({cycle})...")
                ccl_collection = mongo.get_collection(client, "ccl", database_name=f"fec_{cycle}")
                
                committee_to_candidate = {}
                for ccl_doc in ccl_collection.find():
                    cmte_id = ccl_doc.get('CMTE_ID')
                    cand_id = ccl_doc.get('CAND_ID')
                    if cmte_id and cand_id:
                        committee_to_candidate[cmte_id] = cand_id
                
                context.log.info(f"   ✅ {len(committee_to_candidate)} committee→candidate links")
                
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
                
                # Load Committee Master
                context.log.info(f"📥 Loading committee master ({cycle})...")
                cm_collection = mongo.get_collection(client, "cm", database_name=f"fec_{cycle}")
                
                committee_details = {}
                for cm_doc in cm_collection.find():
                    cmte_id = cm_doc.get('CMTE_ID')
                    if cmte_id:
                        committee_details[cmte_id] = {
                            'committee_id': cmte_id,
                            'committee_name': cm_doc.get('CMTE_NM', ''),
                            'committee_type': cm_doc.get('CMTE_TP', ''),
                            'committee_designation': cm_doc.get('CMTE_DSGN', ''),
                            'party': cm_doc.get('CMTE_PTY_AFFILIATION', ''),
                            'connected_org': cm_doc.get('CONNECTED_ORG_NM', ''),
                        }
                
                context.log.info(f"   ✅ {len(committee_details)} committee records")
                
                # Process Operating Expenditures
                context.log.info(f"🔨 Processing operating expenditures ({cycle})...")
                
                oppexp_collection = mongo.get_collection(
                    client, "oppexp", database_name=f"fec_{cycle}"
                )
                
                enriched_oppexp_collection = mongo.get_collection(
                    client, "oppexp", database_name=f"enriched_{cycle}"
                )
                
                # Clear existing data
                enriched_oppexp_collection.delete_many({})
                
                batch = []
                batch_size = 5000
                processed = 0
                
                for oppexp_doc in oppexp_collection.find():
                    cycle_stats['total_expenditures'] += 1
                    
                    # Get committee ID
                    cmte_id = oppexp_doc.get('CMTE_ID')
                    
                    # Resolve to candidate via ccl
                    cand_id = committee_to_candidate.get(cmte_id)
                    
                    # Filter: Only tracked member committees
                    if not cand_id or cand_id not in tracked_candidate_ids:
                        continue
                    
                    # Parse amount - USE CORRECT FIELD NAME FROM FEC.md
                    amount_str = oppexp_doc.get('TRANSACTION_AMT', '0')
                    try:
                        amount = float(str(amount_str).replace(',', '')) if amount_str else 0
                    except (ValueError, AttributeError, TypeError):
                        amount = 0
                    
                    # Get details
                    cmte_info = committee_details.get(cmte_id, {})
                    cand_info = candidate_details.get(cand_id, {})
                    bioguide_id = fec_to_bioguide.get(cand_id)
                    
                    # Build enriched record - USE CORRECT FIELD NAMES FROM FEC.md
                    enriched_record = {
                        # Committee (spender) fields
                        'committee_id': cmte_id,
                        'committee_name': cmte_info.get('committee_name', ''),
                        'committee_type': cmte_info.get('committee_type', ''),
                        'committee_designation': cmte_info.get('committee_designation', ''),
                        'committee_party': cmte_info.get('party', ''),
                        'connected_org': cmte_info.get('connected_org', ''),
                        
                        # Candidate (linked via ccl) fields
                        'candidate_id': cand_id,
                        'candidate_name': cand_info.get('candidate_name', ''),
                        'candidate_party': cand_info.get('party', ''),
                        'candidate_office': cand_info.get('office', ''),
                        'candidate_state': cand_info.get('state', ''),
                        'candidate_district': cand_info.get('district', ''),
                        'bioguide_id': bioguide_id,
                        'is_tracked_member': True,
                        
                        # Expenditure details - USE CORRECT FIELD NAMES FROM FEC.md
                        'amount': amount,
                        'recipient_name': oppexp_doc.get('NAME', ''),
                        'recipient_city': oppexp_doc.get('CITY', ''),
                        'recipient_state': oppexp_doc.get('STATE', ''),
                        'recipient_zip': oppexp_doc.get('ZIP_CODE', ''),
                        'expenditure_date': oppexp_doc.get('TRANSACTION_DT', ''),
                        'category': oppexp_doc.get('CATEGORY', ''),
                        'purpose': oppexp_doc.get('PURPOSE', ''),
                        
                        # Original fields for reference - USE CORRECT FIELD NAMES FROM FEC.md
                        'transaction_id': oppexp_doc.get('TRAN_ID', ''),
                        'image_number': oppexp_doc.get('IMAGE_NUM', ''),
                        'file_number': oppexp_doc.get('FILE_NUM', ''),
                        'amendment_indicator': oppexp_doc.get('AMNDT_IND', ''),
                        'report_type': oppexp_doc.get('RPT_TP', ''),
                        'transaction_pgi': oppexp_doc.get('TRANSACTION_PGI', ''),
                        'entity_type': oppexp_doc.get('ENTITY_TP', ''),
                        
                        # Metadata
                        'cycle': cycle,
                        'source_file': 'oppexp',
                        'computed_at': datetime.now(),
                    }
                    
                    batch.append(enriched_record)
                    
                    # Update stats
                    cycle_stats['tracked_expenditures'] += 1
                    cycle_stats['total_amount'] += amount
                    cycle_stats['unique_committees'].add(cmte_id)
                    cycle_stats['unique_candidates'].add(cand_id)
                    cycle_stats['unique_recipients'].add(oppexp_doc.get('NAME', ''))
                    
                    # Insert batch
                    if len(batch) >= batch_size:
                        enriched_oppexp_collection.insert_many(batch)
                        processed += len(batch)
                        context.log.info(f"   💾 Inserted {processed:,} records...")
                        batch = []
                
                # Insert remaining
                if batch:
                    enriched_oppexp_collection.insert_many(batch)
                    processed += len(batch)
                
                # Create Indexes
                enriched_oppexp_collection.create_index([("bioguide_id", 1)])
                enriched_oppexp_collection.create_index([("candidate_id", 1)])
                enriched_oppexp_collection.create_index([("committee_id", 1)])
                enriched_oppexp_collection.create_index([("committee_name", 1)])
                enriched_oppexp_collection.create_index([("recipient_name", 1)])
                enriched_oppexp_collection.create_index([("category", 1)])
                enriched_oppexp_collection.create_index([("purpose", 1)])
                enriched_oppexp_collection.create_index([("amount", -1)])
                enriched_oppexp_collection.create_index([("expenditure_date", -1)])
                
                context.log.info(f"✅ {cycle}: {cycle_stats['tracked_expenditures']:,} tracked expenditures (${cycle_stats['total_amount']:,.2f})")
                context.log.info("")
                
                # Update overall stats
                overall_stats['cycles_processed'].append(cycle)
                overall_stats['total_expenditures'] += cycle_stats['total_expenditures']
                overall_stats['tracked_expenditures'] += cycle_stats['tracked_expenditures']
                overall_stats['by_cycle'][cycle] = {
                    'total': cycle_stats['total_expenditures'],
                    'tracked': cycle_stats['tracked_expenditures'],
                    'amount': cycle_stats['total_amount'],
                    'committees': len(cycle_stats['unique_committees']),
                    'candidates': len(cycle_stats['unique_candidates']),
                    'recipients': len(cycle_stats['unique_recipients']),
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
        context.log.info(f"  {cycle}: {stats['tracked']:,} tracked (${stats['amount']:,.2f})")
    context.log.info("=" * 80)
    
    return Output(
        value=overall_stats,
        metadata={
            "cycles": overall_stats['cycles_processed'],
            "total": overall_stats['total_expenditures'],
            "tracked": overall_stats['tracked_expenditures'],
        }
    )
