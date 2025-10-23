"""
Donor Financials Asset (Cross-Cycle)
Aggregates donor financials across ALL cycles.
Depends on enriched_{cycle}.donor_financials for each cycle.
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext
from typing import Dict, Any
from datetime import datetime

from src.resources.mongo import MongoDBResource


class DonorFinancialsConfig(Config):
    """Configuration for cross-cycle donor financials"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="donor_financials",
    group_name="aggregation",
    ins={
        "enriched_donor_financials": AssetIn(key="enriched_donor_financials"),
    },
    compute_kind="aggregation",
    description="Cross-cycle aggregation of donor financials with top candidates and spending across all cycles"
)
def donor_financials_asset(
    context: AssetExecutionContext,
    config: DonorFinancialsConfig,
    mongo: MongoDBResource,
    enriched_donor_financials: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Roll up ALL cycles of donor financial data.
    Source: enriched_{cycle}.donor_financials (per-cycle aggregations)
    Target: aggregation.donor_financials (cross-cycle rollup)
    """
    
    with mongo.get_client() as client:
        financials_collection = mongo.get_collection(client, "donor_financials", database_name="aggregation")
        
        # Clear existing cross-cycle data
        financials_collection.delete_many({})
        context.log.info("Cleared existing cross-cycle donor_financials")
        
        # Collect all committees across all cycles
        all_committees = {}
        
        for cycle in config.cycles:
            context.log.info(f"Reading cycle {cycle} donor financials")
            lt_financials = mongo.get_collection(client, "donor_financials", database_name=f"enriched_{cycle}")
            
            for doc in lt_financials.find():
                cmte_id = doc.get('committee_id')
                if not cmte_id:
                    continue
                
                if cmte_id not in all_committees:
                    all_committees[cmte_id] = {
                        'committee_id': cmte_id,
                        'committee_name': doc.get('committee_name'),
                        'committee_type': doc.get('committee_type'),
                        'connected_org': doc.get('connected_org'),
                        'party': doc.get('party'),
                        'by_cycle': {},
                    }
                
                # Store cycle-specific data (without individual transactions to save space)
                cycle_data = {
                    'independent_expenditures': {
                        'support': {
                            'total_amount': doc['independent_expenditures']['support']['total_amount'],
                            'transaction_count': doc['independent_expenditures']['support']['transaction_count'],
                            'candidates': [
                                {
                                    'candidate_id': c['candidate_id'],
                                    'candidate_name': c['candidate_name'],
                                    'bioguide_id': c['bioguide_id'],
                                    'party': c['party'],
                                    'state': c['state'],
                                    'office': c['office'],
                                    'total_amount': c['total_amount'],
                                    'transaction_count': c['transaction_count'],
                                }
                                for c in doc['independent_expenditures']['support']['candidates']
                            ]
                        },
                        'oppose': {
                            'total_amount': doc['independent_expenditures']['oppose']['total_amount'],
                            'transaction_count': doc['independent_expenditures']['oppose']['transaction_count'],
                            'candidates': [
                                {
                                    'candidate_id': c['candidate_id'],
                                    'candidate_name': c['candidate_name'],
                                    'bioguide_id': c['bioguide_id'],
                                    'party': c['party'],
                                    'state': c['state'],
                                    'office': c['office'],
                                    'total_amount': c['total_amount'],
                                    'transaction_count': c['transaction_count'],
                                }
                                for c in doc['independent_expenditures']['oppose']['candidates']
                            ]
                        }
                    },
                    'pac_contributions': {
                        'total_amount': doc['pac_contributions']['total_amount'],
                        'transaction_count': doc['pac_contributions']['transaction_count'],
                        'candidates': [
                            {
                                'candidate_id': c['candidate_id'],
                                'candidate_name': c['candidate_name'],
                                'bioguide_id': c['bioguide_id'],
                                'party': c['party'],
                                'state': c['state'],
                                'office': c['office'],
                                'total_amount': c['total_amount'],
                                'transaction_count': c['transaction_count'],
                            }
                            for c in doc['pac_contributions']['candidates']
                        ]
                    },
                    'operating_expenditures': {
                        'total_amount': doc['operating_expenditures']['total_amount'],
                        'transaction_count': doc['operating_expenditures']['transaction_count'],
                        'top_categories': doc['operating_expenditures']['top_categories'],
                        'top_payees': [
                            {
                                'payee_name': p['payee_name'],
                                'total_amount': p['total_amount'],
                                'transaction_count': p['transaction_count'],
                            }
                            for p in doc['operating_expenditures']['top_payees']
                        ]
                    },
                    'summary': doc['summary']
                }
                
                all_committees[cmte_id]['by_cycle'][cycle] = cycle_data
        
        context.log.info(f"Found {len(all_committees)} unique committees across all cycles")
        
        # Compute totals across cycles
        batch = []
        for cmte_id, cmte_data in all_committees.items():
            # Aggregate independent expenditures
            indie_support_candidates = {}
            indie_oppose_candidates = {}
            
            for cycle, cycle_data in cmte_data['by_cycle'].items():
                # Support
                for cand in cycle_data['independent_expenditures']['support']['candidates']:
                    cand_id = cand['candidate_id']
                    if cand_id not in indie_support_candidates:
                        indie_support_candidates[cand_id] = {
                            'candidate_id': cand_id,
                            'candidate_name': cand['candidate_name'],
                            'bioguide_id': cand['bioguide_id'],
                            'party': cand['party'],
                            'state': cand['state'],
                            'office': cand['office'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    indie_support_candidates[cand_id]['total_amount'] += cand['total_amount']
                    indie_support_candidates[cand_id]['transaction_count'] += cand['transaction_count']
                    if cycle not in indie_support_candidates[cand_id]['cycles']:
                        indie_support_candidates[cand_id]['cycles'].append(cycle)
                
                # Oppose
                for cand in cycle_data['independent_expenditures']['oppose']['candidates']:
                    cand_id = cand['candidate_id']
                    if cand_id not in indie_oppose_candidates:
                        indie_oppose_candidates[cand_id] = {
                            'candidate_id': cand_id,
                            'candidate_name': cand['candidate_name'],
                            'bioguide_id': cand['bioguide_id'],
                            'party': cand['party'],
                            'state': cand['state'],
                            'office': cand['office'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    indie_oppose_candidates[cand_id]['total_amount'] += cand['total_amount']
                    indie_oppose_candidates[cand_id]['transaction_count'] += cand['transaction_count']
                    if cycle not in indie_oppose_candidates[cand_id]['cycles']:
                        indie_oppose_candidates[cand_id]['cycles'].append(cycle)
            
            # Aggregate PAC contributions
            pac_candidates = {}
            for cycle, cycle_data in cmte_data['by_cycle'].items():
                for cand in cycle_data['pac_contributions']['candidates']:
                    cand_id = cand['candidate_id']
                    if cand_id not in pac_candidates:
                        pac_candidates[cand_id] = {
                            'candidate_id': cand_id,
                            'candidate_name': cand['candidate_name'],
                            'bioguide_id': cand['bioguide_id'],
                            'party': cand['party'],
                            'state': cand['state'],
                            'office': cand['office'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    pac_candidates[cand_id]['total_amount'] += cand['total_amount']
                    pac_candidates[cand_id]['transaction_count'] += cand['transaction_count']
                    if cycle not in pac_candidates[cand_id]['cycles']:
                        pac_candidates[cand_id]['cycles'].append(cycle)
            
            # Aggregate operating expenditures
            oppexp_categories = {}
            oppexp_payees = {}
            
            for cycle, cycle_data in cmte_data['by_cycle'].items():
                for cat in cycle_data['operating_expenditures']['top_categories']:
                    cat_name = cat['category']
                    if cat_name not in oppexp_categories:
                        oppexp_categories[cat_name] = {
                            'category': cat_name,
                            'total_amount': 0,
                            'transaction_count': 0
                        }
                    oppexp_categories[cat_name]['total_amount'] += cat['total_amount']
                    oppexp_categories[cat_name]['transaction_count'] += cat['transaction_count']
                
                for payee in cycle_data['operating_expenditures']['top_payees']:
                    payee_name = payee['payee_name']
                    if payee_name not in oppexp_payees:
                        oppexp_payees[payee_name] = {
                            'payee_name': payee_name,
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    oppexp_payees[payee_name]['total_amount'] += payee['total_amount']
                    oppexp_payees[payee_name]['transaction_count'] += payee['transaction_count']
                    if cycle not in oppexp_payees[payee_name]['cycles']:
                        oppexp_payees[payee_name]['cycles'].append(cycle)
            
            # Sort and take top 20
            indie_support_top = sorted(
                indie_support_candidates.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )[:20]
            
            indie_oppose_top = sorted(
                indie_oppose_candidates.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )[:20]
            
            pac_top = sorted(
                pac_candidates.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )[:20]
            
            oppexp_categories_top = sorted(
                oppexp_categories.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )[:20]
            
            oppexp_payees_top = sorted(
                oppexp_payees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )[:20]
            
            # Calculate totals
            indie_support_total = sum(c['total_amount'] for c in indie_support_candidates.values())
            indie_support_count = sum(c['transaction_count'] for c in indie_support_candidates.values())
            indie_oppose_total = sum(c['total_amount'] for c in indie_oppose_candidates.values())
            indie_oppose_count = sum(c['transaction_count'] for c in indie_oppose_candidates.values())
            pac_total = sum(c['total_amount'] for c in pac_candidates.values())
            pac_count = sum(c['transaction_count'] for c in pac_candidates.values())
            oppexp_total = sum(c['total_amount'] for c in oppexp_categories.values())
            oppexp_count = sum(c['transaction_count'] for c in oppexp_categories.values())
            
            cross_cycle_record = {
                '_id': cmte_data['committee_id'],
                'committee_id': cmte_data['committee_id'],
                'committee_name': cmte_data['committee_name'],
                'committee_type': cmte_data['committee_type'],
                'connected_org': cmte_data['connected_org'],
                'party': cmte_data['party'],
                
                'by_cycle': cmte_data['by_cycle'],
                
                'totals': {
                    'independent_expenditures': {
                        'support': {
                            'total_amount': indie_support_total,
                            'transaction_count': indie_support_count,
                            'top_candidates': indie_support_top
                        },
                        'oppose': {
                            'total_amount': indie_oppose_total,
                            'transaction_count': indie_oppose_count,
                            'top_candidates': indie_oppose_top
                        }
                    },
                    'pac_contributions': {
                        'total_amount': pac_total,
                        'transaction_count': pac_count,
                        'top_candidates': pac_top
                    },
                    'operating_expenditures': {
                        'total_amount': oppexp_total,
                        'transaction_count': oppexp_count,
                        'top_categories': oppexp_categories_top,
                        'top_payees': oppexp_payees_top
                    },
                    'summary': {
                        'total_spent': indie_support_total + indie_oppose_total + pac_total + oppexp_total,
                        'total_to_candidates': indie_support_total + indie_oppose_total + pac_total,
                        'total_support': indie_support_total + pac_total,
                        'total_oppose': indie_oppose_total,
                        'total_operating': oppexp_total,
                        'total_transactions': indie_support_count + indie_oppose_count + pac_count + oppexp_count,
                        'cycles_tracked': sorted(cmte_data['by_cycle'].keys())
                    }
                },
                
                'computed_at': datetime.now()
            }
            
            batch.append(cross_cycle_record)
            
            if len(batch) >= 100:
                financials_collection.insert_many(batch)
                batch = []
        
        if batch:
            financials_collection.insert_many(batch)
        
        stats = {
            'committees_aggregated': len(all_committees),
            'cycles_processed': config.cycles,
        }
        
        context.log.info(f"Cross-cycle donor financials aggregation complete: {stats}")
        return stats
