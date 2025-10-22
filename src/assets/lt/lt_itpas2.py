"""LT itpas2 Asset - Enriched itemized contributions and transfers.

This asset processes pas2.zip (itemized transactions) and enriches it with 
full donor and recipient details. This is THE critical file for tracking
PAC → Candidate money flows.

Input: fec_{cycle}.itpas2 (raw transaction data)
Output: lt_{cycle}.itpas2 (enriched with donor/recipient names, offices, parties)

Key Features:
- Tracks contributions from PACs to candidates
- Enriches with committee names (via cm.zip)
- Links to candidates (via ccl.zip)
- Filters to tracked members only
- Handles corrections (negative amounts)

Use Cases:
- "Show me all AIPAC contributions to Ted Cruz"
- "Which PACs gave the most to this candidate?"
- "How much did corporate PACs contribute to tracked members?"
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


class LtItpas2Config(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


@asset(
    name="lt_itpas2",
    description="Enriched itemized contributions for tracked members across all cycles",
    group_name="lt",
    compute_kind="computation",
    ins={
        "itpas2": AssetIn("itpas2"),
        "cn": AssetIn("cn"),
        "cm": AssetIn("cm"),
        "ccl": AssetIn("ccl"),
        "member_fec_mapping": AssetIn("member_fec_mapping"),
    },
)
def lt_itpas2_asset(
    context: AssetExecutionContext,
    config: LtItpas2Config,
    mongo: MongoDBResource,
    itpas2: Dict[str, Any],
    cn: Dict[str, Any],
    cm: Dict[str, Any],
    ccl: Dict[str, Any],
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Process itemized contributions and enrich with full context.
    
    Data Flow:
    1. Load member_fec_mapping → get tracked member FEC IDs
    2. For each cycle:
       - Load ccl → map committee IDs to candidate IDs
       - Load cn → get candidate details
       - Load cm → get committee (donor) details
       - Load itpas2 → get transaction records
       - Join and enrich → create denormalized records
       - Store in lt_{cycle}.itpas2
    """
    context.log.info("=" * 80)
    context.log.info("💰 PROCESSING ITEMIZED CONTRIBUTIONS (itpas2)")
    context.log.info("=" * 80)
    
    overall_stats = {
        'cycles_processed': [],
        'total_transactions': 0,
        'tracked_transactions': 0,
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
                'total_transactions': 0,
                'tracked_member_transactions': 0,
                'corrections': 0,
                'total_amount': 0,
                'unique_donors': set(),
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
                
                # Process itpas2 Transactions
                context.log.info(f"🔨 Processing itemized transactions ({cycle})...")
                
                itpas2_collection = mongo.get_collection(
                    client, "itpas2", database_name=f"fec_{cycle}"
                )
                
                lt_itpas2_collection = mongo.get_collection(
                    client, "itpas2", database_name=f"lt_{cycle}"
                )
                
                # Clear existing data
                lt_itpas2_collection.delete_many({})
                
                batch = []
                batch_size = 5000
                processed = 0
                
                for tx_doc in itpas2_collection.find():
                    cycle_stats['total_transactions'] += 1
                    
                    # Get recipient candidate ID
                    recipient_cand_id = tx_doc.get('CAND_ID')
                    recipient_cmte_id = tx_doc.get('OTHER_ID')
                    
                    # Try to resolve candidate via direct CAND_ID or via committee linkage
                    resolved_cand_id = None
                    if recipient_cand_id and recipient_cand_id in tracked_candidate_ids:
                        resolved_cand_id = recipient_cand_id
                    elif recipient_cmte_id and recipient_cmte_id in committee_to_candidate:
                        potential_cand_id = committee_to_candidate[recipient_cmte_id]
                        if potential_cand_id in tracked_candidate_ids:
                            resolved_cand_id = potential_cand_id
                    
                    # Filter: Only tracked members
                    if not resolved_cand_id:
                        continue
                    
                    # Get filer committee ID (CMTE_ID from FEC)
                    filer_cmte_id = tx_doc.get('CMTE_ID')
                    
                    # Parse amount
                    amount_str = tx_doc.get('TRANSACTION_AMT', '0')
                    try:
                        amount = float(str(amount_str).replace(',', ''))
                    except (ValueError, AttributeError):
                        amount = 0
                    
                    is_correction = amount < 0
                    
                    # Get details
                    filer_info = committee_details.get(filer_cmte_id, {})
                    candidate_info = candidate_details.get(resolved_cand_id, {})
                    bioguide_id = fec_to_bioguide.get(resolved_cand_id)
                    
                    # Build enriched record
                    enriched_record = {
                        # Filer committee fields (CMTE_ID from FEC - the committee filing the report)
                        'filer_committee_id': filer_cmte_id,
                        'filer_committee_name': filer_info.get('committee_name', ''),
                        'filer_committee_type': filer_info.get('committee_type', ''),
                        'filer_connected_org': filer_info.get('connected_org', ''),
                        'filer_party': filer_info.get('party', ''),
                        
                        # Candidate fields (CAND_ID from FEC - the candidate associated with transaction)
                        'candidate_id': resolved_cand_id,
                        'candidate_name': candidate_info.get('candidate_name', ''),
                        'candidate_party': candidate_info.get('party', ''),
                        'candidate_office': candidate_info.get('office', ''),
                        'candidate_state': candidate_info.get('state', ''),
                        'candidate_district': candidate_info.get('district', ''),
                        'candidate_bioguide_id': bioguide_id,
                        'is_tracked_member': True,
                        
                        # Transaction details
                        'amount': amount,
                        'transaction_date': tx_doc.get('TRANSACTION_DT', ''),
                        'transaction_type': tx_doc.get('TRANSACTION_TP', ''),
                        'entity_type': tx_doc.get('ENTITY_TP', ''),
                        'transaction_pgi': tx_doc.get('TRANSACTION_PGI', ''),
                        'is_correction': is_correction,
                        
                        # Original fields for reference
                        'image_number': tx_doc.get('IMAGE_NUM', ''),
                        'transaction_id': tx_doc.get('TRAN_ID', ''),
                        'file_number': tx_doc.get('FILE_NUM', ''),
                        'memo_code': tx_doc.get('MEMO_CD', ''),
                        'memo_text': tx_doc.get('MEMO_TEXT', ''),
                        
                        # Metadata
                        'cycle': cycle,
                        'source_file': 'itpas2',
                        'computed_at': datetime.now(),
                    }
                    
                    batch.append(enriched_record)
                    
                    # Update stats
                    cycle_stats['tracked_member_transactions'] += 1
                    cycle_stats['total_amount'] += amount
                    if is_correction:
                        cycle_stats['corrections'] += 1
                    cycle_stats['unique_donors'].add(filer_cmte_id)
                    cycle_stats['unique_recipients'].add(resolved_cand_id)
                    
                    # Insert batch
                    if len(batch) >= batch_size:
                        lt_itpas2_collection.insert_many(batch)
                        processed += len(batch)
                        context.log.info(f"   💾 Inserted {processed:,} records...")
                        batch = []
                
                # Insert remaining
                if batch:
                    lt_itpas2_collection.insert_many(batch)
                    processed += len(batch)
                
                # Create Indexes
                lt_itpas2_collection.create_index([("candidate_bioguide_id", 1)])
                lt_itpas2_collection.create_index([("candidate_id", 1)])
                lt_itpas2_collection.create_index([("filer_committee_id", 1)])
                lt_itpas2_collection.create_index([("filer_committee_name", 1)])
                lt_itpas2_collection.create_index([("filer_connected_org", 1)])
                lt_itpas2_collection.create_index([("amount", -1)])
                lt_itpas2_collection.create_index([("transaction_date", -1)])
                
                context.log.info(f"✅ {cycle}: {cycle_stats['tracked_member_transactions']:,} tracked transactions (${cycle_stats['total_amount']:,.2f})")
                context.log.info("")
                
                # Update overall stats
                overall_stats['cycles_processed'].append(cycle)
                overall_stats['total_transactions'] += cycle_stats['total_transactions']
                overall_stats['tracked_transactions'] += cycle_stats['tracked_member_transactions']
                overall_stats['by_cycle'][cycle] = {
                    'total': cycle_stats['total_transactions'],
                    'tracked': cycle_stats['tracked_member_transactions'],
                    'amount': cycle_stats['total_amount'],
                    'donors': len(cycle_stats['unique_donors']),
                    'recipients': len(cycle_stats['unique_recipients']),
                }
                
            except Exception as e:
                context.log.error(f"❌ Error processing {cycle}: {e}")
    
    # Summary
    context.log.info("=" * 80)
    context.log.info("📊 PROCESSING SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Cycles Processed: {', '.join(overall_stats['cycles_processed'])}")
    context.log.info(f"Total Transactions: {overall_stats['total_transactions']:,}")
    context.log.info(f"Tracked Transactions: {overall_stats['tracked_transactions']:,}")
    for cycle, stats in overall_stats['by_cycle'].items():
        context.log.info(f"  {cycle}: {stats['tracked']:,} tracked (${stats['amount']:,.2f})")
    context.log.info("=" * 80)
    
    return Output(
        value=overall_stats,
        metadata={
            "cycles": overall_stats['cycles_processed'],
            "total": overall_stats['total_transactions'],
            "tracked": overall_stats['tracked_transactions'],
        }
    )
