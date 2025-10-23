"""
Candidate Summaries Asset - FEC Official Candidate Financial Summaries (Cross-Cycle)

Aggregates FEC's official candidate financial summaries across all cycles.
This provides a COMPLETE view of each tracked candidate's official FEC data.

Input: lt_{cycle}.webl + lt_{cycle}.weball (per-cycle summaries)
Output: legal_tender.candidate_summaries (cross-cycle aggregation)

Structure per candidate:
{
    _id: "C001118",  # bioguide_id
    bioguide_id: "C001118",
    candidate_name: "CRUZ, TED",
    
    cycles: [
        {
            cycle: "2024",
            source: "webl",  # cycle-specific (running this cycle)
            candidate_id: "S2TX00312",
            total_receipts: 50000000,
            total_disbursements: 45000000,
            individual_contributions: 25000000,
            pac_contributions: 2500000,
            party_contributions: 500000,
            candidate_self_funding: 0,
            candidate_loans: 0,
            cash_on_hand_beginning: 5000000,
            cash_on_hand_ending: 10000000,
            debts_owed: 0,
            coverage_end_date: "12/31/2024"
        },
        {
            cycle: "2024",
            source: "weball",  # all-candidate (may include future fundraising)
            # ... same fields
        }
    ],
    
    totals: {
        cycles_active: 2,
        total_receipts_all_time: 150000000,
        total_individual_contributions: 80000000,
        total_pac_contributions: 8000000
    }
}

Use Cases:
- "Show me FEC's official financial summary for Ted Cruz across all cycles"
- "Compare cycle-specific (webl) vs all-activity (weball) data"
- "Validate: Does FEC summary match our transaction-level calculations?"
"""

from typing import Dict, Any
from datetime import datetime

from dagster import asset, AssetIn, Config, AssetExecutionContext, Output

from src.resources.mongo import MongoDBResource


class CandidateSummariesConfig(Config):
    """Configuration for candidate_summaries asset"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="candidate_summaries",
    group_name="legal_tender",
    ins={
        "lt_webl": AssetIn(key="lt_webl"),
        "lt_weball": AssetIn(key="lt_weball"),
    },
    compute_kind="aggregation",
    description="FEC official candidate financial summaries aggregated across all cycles (from webl + weball)"
)
def candidate_summaries(
    context: AssetExecutionContext,
    config: CandidateSummariesConfig,
    mongo: MongoDBResource,
    lt_webl: Dict[str, Any],
    lt_weball: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Aggregate FEC official candidate summaries across all cycles.
    """
    
    stats = {
        'total_candidates': 0,
        'total_cycle_records': 0,
        'webl_records': 0,
        'weball_records': 0,
    }
    
    with mongo.get_client() as client:
        summaries_collection = mongo.get_collection(client, "candidate_summaries", database_name="legal_tender")
        
        # Clear existing data
        summaries_collection.delete_many({})
        context.log.info("Cleared existing candidate_summaries")
        
        # Aggregate by bioguide_id
        all_candidates = {}
        
        # Process webl data (cycle-specific)
        for cycle in config.cycles:
            context.log.info(f"Processing webl for cycle {cycle}")
            webl_collection = mongo.get_collection(client, "webl", database_name=f"lt_{cycle}")
            
            for doc in webl_collection.find():
                bioguide_id = doc.get('bioguide_id')
                if not bioguide_id:
                    continue
                
                if bioguide_id not in all_candidates:
                    all_candidates[bioguide_id] = {
                        'bioguide_id': bioguide_id,
                        'candidate_name': doc.get('CAND_NAME', ''),
                        'party': doc.get('CAND_PTY_AFFILIATION', ''),
                        'office': doc.get('CAND_OFFICE_ST', ''),
                        'cycles': []
                    }
                
                all_candidates[bioguide_id]['cycles'].append({
                    'cycle': cycle,
                    'source': 'webl',
                    'candidate_id': doc.get('CAND_ID'),
                    'total_receipts': doc.get('TTL_RECEIPTS'),
                    'total_disbursements': doc.get('TTL_DISB'),
                    'individual_contributions': doc.get('TTL_INDIV_CONTRIB'),
                    'pac_contributions': doc.get('OTHER_POL_CMTE_CONTRIB'),
                    'party_contributions': doc.get('POL_PTY_CONTRIB'),
                    'candidate_self_funding': doc.get('CAND_CONTRIB'),
                    'candidate_loans': doc.get('CAND_LOANS'),
                    'cash_on_hand_beginning': doc.get('COH_BOP'),
                    'cash_on_hand_ending': doc.get('COH_COP'),
                    'debts_owed': doc.get('DEBTS_OWED_BY'),
                    'coverage_end_date': doc.get('CVG_END_DT'),
                })
                stats['webl_records'] += 1
        
        # Process weball data (all-candidate)
        for cycle in config.cycles:
            context.log.info(f"Processing weball for cycle {cycle}")
            weball_collection = mongo.get_collection(client, "weball", database_name=f"lt_{cycle}")
            
            for doc in weball_collection.find():
                bioguide_id = doc.get('bioguide_id')
                if not bioguide_id:
                    continue
                
                if bioguide_id not in all_candidates:
                    all_candidates[bioguide_id] = {
                        'bioguide_id': bioguide_id,
                        'candidate_name': doc.get('CAND_NAME', ''),
                        'party': doc.get('CAND_PTY_AFFILIATION', ''),
                        'office': doc.get('CAND_OFFICE_ST', ''),
                        'cycles': []
                    }
                
                all_candidates[bioguide_id]['cycles'].append({
                    'cycle': cycle,
                    'source': 'weball',
                    'candidate_id': doc.get('CAND_ID'),
                    'total_receipts': doc.get('TTL_RECEIPTS'),
                    'total_disbursements': doc.get('TTL_DISB'),
                    'individual_contributions': doc.get('TTL_INDIV_CONTRIB'),
                    'pac_contributions': doc.get('OTHER_POL_CMTE_CONTRIB'),
                    'party_contributions': doc.get('POL_PTY_CONTRIB'),
                    'candidate_self_funding': doc.get('CAND_CONTRIB'),
                    'candidate_loans': doc.get('CAND_LOANS'),
                    'cash_on_hand_beginning': doc.get('COH_BOP'),
                    'cash_on_hand_ending': doc.get('COH_COP'),
                    'debts_owed': doc.get('DEBTS_OWED_BY'),
                    'coverage_end_date': doc.get('CVG_END_DT'),
                })
                stats['weball_records'] += 1
        
        # Calculate totals and insert
        batch = []
        for bioguide_id, cand_data in all_candidates.items():
            # Calculate cross-cycle totals (from webl only to avoid double-counting)
            webl_cycles = [c for c in cand_data['cycles'] if c['source'] == 'webl']
            
            cand_data['totals'] = {
                'cycles_active': len(webl_cycles),
                'total_receipts_all_time': sum(c.get('total_receipts') or 0 for c in webl_cycles),
                'total_disbursements_all_time': sum(c.get('total_disbursements') or 0 for c in webl_cycles),
                'total_individual_contributions': sum(c.get('individual_contributions') or 0 for c in webl_cycles),
                'total_pac_contributions': sum(c.get('pac_contributions') or 0 for c in webl_cycles),
                'total_party_contributions': sum(c.get('party_contributions') or 0 for c in webl_cycles),
            }
            
            cand_data['_id'] = bioguide_id
            cand_data['updated_at'] = datetime.now()
            batch.append(cand_data)
        
        if batch:
            summaries_collection.insert_many(batch, ordered=False)
            stats['total_candidates'] = len(batch)
            stats['total_cycle_records'] = sum(len(c['cycles']) for c in batch)
            context.log.info(f"✅ Inserted {len(batch)} candidates with {stats['total_cycle_records']} cycle records")
        
        # Create indexes
        summaries_collection.create_index([("bioguide_id", 1)])
        summaries_collection.create_index([("candidate_name", 1)])
    
    return Output(
        value=stats,
        metadata={
            "total_candidates": stats['total_candidates'],
            "total_cycle_records": stats['total_cycle_records'],
            "webl_records": stats['webl_records'],
            "weball_records": stats['weball_records'],
        }
    )
