"""
Enriched Candidate Financials Asset (Per-Cycle)
Aggregates ALL money flows TO each tracked candidate within a single cycle.
Keeps sources separate with detailed breakdowns of support vs opposition.
Stored in enriched_{cycle}.candidate_financials
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext
from typing import Dict, Any
from datetime import datetime

from src.resources.mongo import MongoDBResource


class EnrichedCandidateFinancialsConfig(Config):
    """Configuration for enriched_candidate_financials asset"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="enriched_candidate_financials",
    group_name="enrichment",
    ins={
        "enriched_itpas2": AssetIn(key="enriched_itpas2"),
        "enriched_oppexp": AssetIn(key="enriched_oppexp"),
    },
    compute_kind="aggregation",
    description="Per-cycle aggregation of all money flows to each tracked candidate with source separation"
)
def enriched_candidate_financials_asset(
    context: AssetExecutionContext,
    config: EnrichedCandidateFinancialsConfig,
    mongo: MongoDBResource,
    enriched_itpas2: Dict[str, Any],
    enriched_oppexp: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Aggregate ALL money flows TO each tracked candidate within a single cycle.
    Stored in enriched_{cycle}.candidate_financials
    
    Structure per candidate (within each cycle):
    {
        candidate_id: "H8TX21091",
        bioguide_id: "C001118",
        candidate_name: "CRUZ, TED",
        party: "REP",
        office: "S",
        state: "TX",
        district: "00",
        
        // Independent Expenditures (Super PAC spending FOR/AGAINST)
        independent_expenditures: {
            support: {
                total_amount: 1500000,
                transaction_count: 45,
                committees: [
                    {
                        committee_id: "C00...",
                        committee_name: "Pro-Cruz Super PAC",
                        total_amount: 500000,
                        transaction_count: 15,
                        transactions: [...]  // top 10 largest
                    }
                ]
            },
            oppose: {
                total_amount: 300000,
                transaction_count: 12,
                committees: [...]
            }
        },
        
        // PAC Contributions (from itpas2 - PAC to Candidate committee)
        pac_contributions: {
            total_amount: 250000,
            transaction_count: 89,
            committees: [
                {
                    committee_id: "C00...",
                    committee_name: "AIPAC",
                    committee_type: "Q",
                    total_amount: 50000,
                    transaction_count: 5,
                    transactions: [...]  // top 10 largest
                }
            ]
        },
        
        // Operating Expenditures (indirect - committees that mention this candidate)
        operating_expenditures: {
            total_amount: 75000,
            transaction_count: 34,
            committees: [...]
        },
        
        // Summary totals
        summary: {
            total_support: 1750000,  // indie support + pac contributions
            total_oppose: 300000,
            net_support: 1450000,
            total_transactions: 180
        },
        
        cycle: "2024",
        computed_at: ISODate(...)
    }
    """
    
    stats = {
        'cycles_processed': [],
        'candidates_processed': 0,
        'total_independent_expenditures': 0,
        'total_pac_contributions': 0,
        'total_operating_expenditures': 0,
    }
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"Processing cycle {cycle}")
            
            itpas2_collection = mongo.get_collection(client, "itpas2", database_name=f"enriched_{cycle}")
            financials_collection = mongo.get_collection(client, "candidate_financials", database_name=f"enriched_{cycle}")
            
            # Clear existing data for this cycle
            financials_collection.delete_many({})
            context.log.info(f"Cleared existing candidate_financials for cycle {cycle}")
            
            # Get all unique tracked candidates from itpas2
            tracked_candidates = {}
            
            # Gather from itpas2 (covers both indie expenditures and PAC contributions)
            for doc in itpas2_collection.find({"is_tracked_member": True}):
                cand_id = doc.get('candidate_id')
                if cand_id and cand_id not in tracked_candidates:
                    tracked_candidates[cand_id] = {
                        'candidate_id': cand_id,
                        'bioguide_id': doc.get('candidate_bioguide_id'),
                        'candidate_name': doc.get('candidate_name'),
                        'party': doc.get('candidate_party'),
                        'office': doc.get('candidate_office'),
                        'state': doc.get('candidate_state'),
                        'district': doc.get('candidate_district'),
                    }
            
            context.log.info(f"Found {len(tracked_candidates)} unique tracked candidates in cycle {cycle}")
            
            # Process each candidate
            batch = []
            for cand_id, cand_info in tracked_candidates.items():
                context.log.info(f"Processing candidate {cand_id} - {cand_info['candidate_name']}")
                
                # === INDEPENDENT EXPENDITURES ===
                # Using itpas2 data instead of independent_expenditure collection
                # because independent_expenditure.csv contains billions in fake/troll filings
                # Transaction types (per official FEC documentation):
                # 24E = Independent expenditure advocating election (58,432 txns, $1.9B) → support
                # 24A = Independent expenditure opposing election (19,229 txns, $2.5B) → oppose
                # 24F = Communication cost for candidate (708 txns, $7M) → support (minor)
                # 24N = Communication cost against candidate (91 txns, $57K) → oppose (negligible)
                indie_support_committees = {}
                indie_oppose_committees = {}
                
                for doc in itpas2_collection.find({
                    "candidate_id": cand_id,
                    "transaction_type": {"$in": ["24A", "24E", "24F", "24N"]}
                }):
                    cmte_id = doc.get('filer_committee_id')
                    amount = doc.get('amount', 0)
                    transaction_type = doc.get('transaction_type', '')
                    
                    # Map transaction types to support/oppose
                    # 24E/24F = support, 24A/24N = oppose
                    is_support = transaction_type in ['24E', '24F']
                    
                    # Choose support or oppose bucket
                    committees_dict = indie_support_committees if is_support else indie_oppose_committees
                    
                    if cmte_id not in committees_dict:
                        committees_dict[cmte_id] = {
                            'committee_id': cmte_id,
                            'committee_name': doc.get('filer_committee_name', ''),
                            'total_amount': 0,
                            'transaction_count': 0,
                            'transactions': []
                        }
                    
                    committees_dict[cmte_id]['total_amount'] += amount
                    committees_dict[cmte_id]['transaction_count'] += 1
                    committees_dict[cmte_id]['transactions'].append({
                        'amount': amount,
                        'date': doc.get('transaction_date', ''),
                        'description': doc.get('memo_text', ''),
                        'payee': '',  # itpas2 doesn't have payee info for indie expenditures
                        'transaction_id': doc.get('transaction_id', ''),
                        'transaction_type': transaction_type,  # Include type for reference
                    })
                
                # Sort and limit transactions to top 10 per committee
                for cmte_dict in [indie_support_committees, indie_oppose_committees]:
                    for cmte in cmte_dict.values():
                        cmte['transactions'] = sorted(
                            cmte['transactions'], 
                            key=lambda x: x['amount'], 
                            reverse=True
                        )[:10]
                
                indie_support_list = sorted(
                    indie_support_committees.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )
                indie_oppose_list = sorted(
                    indie_oppose_committees.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )
                
                indie_support_total = sum(c['total_amount'] for c in indie_support_list)
                indie_support_count = sum(c['transaction_count'] for c in indie_support_list)
                indie_oppose_total = sum(c['total_amount'] for c in indie_oppose_list)
                indie_oppose_count = sum(c['transaction_count'] for c in indie_oppose_list)
                
                # === PAC CONTRIBUTIONS ===
                # Net PAC contributions from committees to candidates
                # Transaction types (per official FEC documentation):
                # 24C = Coordinated party expenditure (218 transactions)
                # 24F = Communication cost for candidate (512 transactions)
                # 24K = Contribution to nonaffiliated committee (454K transactions - main one!)
                # 24Z = In-kind contribution to registered filer (1,650 transactions)
                # Note: 24A/24E are independent expenditures (tracked separately)
                pac_committees = {}

                for doc in itpas2_collection.find({"candidate_id": cand_id}):
                    cmte_id = doc.get('filer_committee_id')
                    amount = doc.get('amount', 0)
                    transaction_type = doc.get('transaction_type', '')
                    
                    # Only include direct contribution types
                    if transaction_type not in ['24C', '24F', '24K', '24Z']:
                        continue
                    
                    if cmte_id not in pac_committees:
                        pac_committees[cmte_id] = {
                            'committee_id': cmte_id,
                            'committee_name': doc.get('filer_committee_name', ''),
                            'committee_type': doc.get('filer_committee_type', ''),
                            'connected_org': doc.get('filer_connected_org', ''),
                            'total_amount': 0,
                            'transaction_count': 0,
                            'transactions': []
                        }
                    
                    # Add the amount as-is (signs are already correct)
                    pac_committees[cmte_id]['total_amount'] += amount
                    pac_committees[cmte_id]['transaction_count'] += 1
                    pac_committees[cmte_id]['transactions'].append({
                        'amount': amount,
                        'date': doc.get('transaction_date', ''),
                        'transaction_type': transaction_type,
                        'transaction_id': doc.get('transaction_id', ''),
                    })
                
                # Sort and limit (sort by absolute value for display purposes)
                for cmte in pac_committees.values():
                    cmte['transactions'] = sorted(
                        cmte['transactions'],
                        key=lambda x: abs(x['amount']),
                        reverse=True
                    )[:10]
                
                pac_list = sorted(
                    pac_committees.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )
                
                pac_total = sum(c['total_amount'] for c in pac_list)
                pac_count = sum(c['transaction_count'] for c in pac_list)
                
                # === OPERATING EXPENDITURES ===
                # Note: oppexp doesn't directly link to candidates, so this is a placeholder
                # We'd need to match by candidate name in memo_text or other fields
                oppexp_total = 0
                oppexp_count = 0
                
                # Build final record
                financial_record = {
                    **cand_info,
                    
                    'independent_expenditures': {
                        'support': {
                            'total_amount': indie_support_total,
                            'transaction_count': indie_support_count,
                            'committees': indie_support_list
                        },
                        'oppose': {
                            'total_amount': indie_oppose_total,
                            'transaction_count': indie_oppose_count,
                            'committees': indie_oppose_list
                        }
                    },
                    
                    'pac_contributions': {
                        'total_amount': pac_total,
                        'transaction_count': pac_count,
                        'committees': pac_list
                    },
                    
                    'operating_expenditures': {
                        'total_amount': oppexp_total,
                        'transaction_count': oppexp_count,
                        'committees': []
                    },
                    
                    'summary': {
                        'total_support': indie_support_total + pac_total,
                        'total_oppose': indie_oppose_total,
                        'net_support': (indie_support_total + pac_total) - indie_oppose_total,
                        'total_transactions': indie_support_count + indie_oppose_count + pac_count + oppexp_count
                    },
                    
                    'cycle': cycle,
                    'computed_at': datetime.now()
                }
                
                batch.append(financial_record)
                stats['total_independent_expenditures'] += indie_support_count + indie_oppose_count
                stats['total_pac_contributions'] += pac_count
                
                if len(batch) >= 100:
                    financials_collection.insert_many(batch)
                    batch = []
            
            # Insert remaining
            if batch:
                financials_collection.insert_many(batch)
            
            stats['candidates_processed'] += len(tracked_candidates)
            stats['cycles_processed'].append(cycle)
            
            context.log.info(f"Cycle {cycle} complete: {len(tracked_candidates)} candidates processed")
    
    context.log.info(f"Enriched candidate financials aggregation complete: {stats}")
    return stats
