"""
Candidate Financials Asset (Cross-Cycle)
Aggregates candidate financials across ALL cycles.
Depends on enriched_{cycle}.candidate_financials for each cycle.
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext
from typing import Dict, Any
from datetime import datetime

from src.resources.mongo import MongoDBResource


class CandidateFinancialsConfig(Config):
    """Configuration for cross-cycle candidate financials"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="candidate_financials",
    group_name="aggregation",
    ins={
        "enriched_candidate_financials": AssetIn(key="enriched_candidate_financials"),
    },
    compute_kind="aggregation",
    description="Cross-cycle aggregation of candidate financials with top committees across all cycles"
)
def candidate_financials_asset(
    context: AssetExecutionContext,
    config: CandidateFinancialsConfig,
    mongo: MongoDBResource,
    enriched_candidate_financials: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Roll up ALL cycles of candidate financial data.
    Source: enriched_{cycle}.candidate_financials (per-cycle aggregations)
    Target: aggregation.candidate_financials (cross-cycle rollup)
    """
    
    with mongo.get_client() as client:
        financials_collection = mongo.get_collection(client, "candidate_financials", database_name="aggregation")
        
        # Clear existing cross-cycle data
        financials_collection.delete_many({})
        context.log.info("Cleared existing cross-cycle candidate_financials")
        
        # Collect all candidates across all cycles
        all_candidates = {}
        
        for cycle in config.cycles:
            context.log.info(f"Reading cycle {cycle} candidate financials")
            lt_financials = mongo.get_collection(client, "candidate_financials", database_name=f"enriched_{cycle}")
            
            for doc in lt_financials.find():
                cand_id = doc.get('candidate_id')
                if not cand_id:
                    continue
                
                if cand_id not in all_candidates:
                    all_candidates[cand_id] = {
                        'candidate_id': cand_id,
                        'bioguide_id': doc.get('bioguide_id'),
                        'candidate_name': doc.get('candidate_name'),
                        'party': doc.get('party'),
                        'office': doc.get('office'),
                        'state': doc.get('state'),
                        'district': doc.get('district'),
                        'by_cycle': {},
                    }
                
                # Store cycle-specific data (without transactions to save space)
                cycle_data = {
                    'independent_expenditures': {
                        'support': {
                            'total_amount': doc['independent_expenditures']['support']['total_amount'],
                            'transaction_count': doc['independent_expenditures']['support']['transaction_count'],
                            'committees': [
                                {
                                    'committee_id': c['committee_id'],
                                    'committee_name': c['committee_name'],
                                    'total_amount': c['total_amount'],
                                    'transaction_count': c['transaction_count'],
                                }
                                for c in doc['independent_expenditures']['support']['committees']
                            ]
                        },
                        'oppose': {
                            'total_amount': doc['independent_expenditures']['oppose']['total_amount'],
                            'transaction_count': doc['independent_expenditures']['oppose']['transaction_count'],
                            'committees': [
                                {
                                    'committee_id': c['committee_id'],
                                    'committee_name': c['committee_name'],
                                    'total_amount': c['total_amount'],
                                    'transaction_count': c['transaction_count'],
                                }
                                for c in doc['independent_expenditures']['oppose']['committees']
                            ]
                        }
                    },
                    'pac_contributions': {
                        'total_amount': doc['pac_contributions']['total_amount'],
                        'transaction_count': doc['pac_contributions']['transaction_count'],
                        'committees': [
                            {
                                'committee_id': c['committee_id'],
                                'committee_name': c['committee_name'],
                                'committee_type': c['committee_type'],
                                'connected_org': c['connected_org'],
                                'total_amount': c['total_amount'],
                                'transaction_count': c['transaction_count'],
                            }
                            for c in doc['pac_contributions']['committees']
                        ]
                    },
                    'summary': doc['summary']
                }
                
                all_candidates[cand_id]['by_cycle'][cycle] = cycle_data
        
        context.log.info(f"Found {len(all_candidates)} unique candidates across all cycles")
        
        # Compute totals across cycles
        batch = []
        for cand_id, cand_data in all_candidates.items():
            # Aggregate independent expenditures support
            indie_support_committees = {}
            indie_oppose_committees = {}
            
            for cycle, cycle_data in cand_data['by_cycle'].items():
                # Support
                for cmte in cycle_data['independent_expenditures']['support']['committees']:
                    cmte_id = cmte['committee_id']
                    if cmte_id not in indie_support_committees:
                        indie_support_committees[cmte_id] = {
                            'committee_id': cmte_id,
                            'committee_name': cmte['committee_name'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    indie_support_committees[cmte_id]['total_amount'] += cmte['total_amount']
                    indie_support_committees[cmte_id]['transaction_count'] += cmte['transaction_count']
                    if cycle not in indie_support_committees[cmte_id]['cycles']:
                        indie_support_committees[cmte_id]['cycles'].append(cycle)
                
                # Oppose
                for cmte in cycle_data['independent_expenditures']['oppose']['committees']:
                    cmte_id = cmte['committee_id']
                    if cmte_id not in indie_oppose_committees:
                        indie_oppose_committees[cmte_id] = {
                            'committee_id': cmte_id,
                            'committee_name': cmte['committee_name'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    indie_oppose_committees[cmte_id]['total_amount'] += cmte['total_amount']
                    indie_oppose_committees[cmte_id]['transaction_count'] += cmte['transaction_count']
                    if cycle not in indie_oppose_committees[cmte_id]['cycles']:
                        indie_oppose_committees[cmte_id]['cycles'].append(cycle)
            
            # Aggregate PAC contributions
            pac_committees = {}
            for cycle, cycle_data in cand_data['by_cycle'].items():
                for cmte in cycle_data['pac_contributions']['committees']:
                    cmte_id = cmte['committee_id']
                    if cmte_id not in pac_committees:
                        pac_committees[cmte_id] = {
                            'committee_id': cmte_id,
                            'committee_name': cmte['committee_name'],
                            'committee_type': cmte['committee_type'],
                            'connected_org': cmte['connected_org'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'cycles': []
                        }
                    pac_committees[cmte_id]['total_amount'] += cmte['total_amount']
                    pac_committees[cmte_id]['transaction_count'] += cmte['transaction_count']
                    if cycle not in pac_committees[cmte_id]['cycles']:
                        pac_committees[cmte_id]['cycles'].append(cycle)
            
            # Sort all committees (keep ALL for UI linking)
            indie_support_top = sorted(
                indie_support_committees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            indie_oppose_top = sorted(
                indie_oppose_committees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            pac_top = sorted(
                pac_committees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            # Calculate totals
            indie_support_total = sum(c['total_amount'] for c in indie_support_committees.values())
            indie_support_count = sum(c['transaction_count'] for c in indie_support_committees.values())
            indie_oppose_total = sum(c['total_amount'] for c in indie_oppose_committees.values())
            indie_oppose_count = sum(c['transaction_count'] for c in indie_oppose_committees.values())
            pac_total = sum(c['total_amount'] for c in pac_committees.values())
            pac_count = sum(c['transaction_count'] for c in pac_committees.values())
            
            # Calculate top-level summary metrics for easy sorting/filtering
            total_independent_support = indie_support_total
            total_independent_oppose = indie_oppose_total
            total_pac_contributions = pac_total
            net_independent_expenditures = indie_support_total - indie_oppose_total
            net_support = pac_total + net_independent_expenditures
            net_benefit = (indie_support_total + pac_total) - indie_oppose_total
            
            cross_cycle_record = {
                '_id': cand_data['candidate_id'],
                'candidate_id': cand_data['candidate_id'],
                'bioguide_id': cand_data['bioguide_id'],
                'candidate_name': cand_data['candidate_name'],
                'party': cand_data['party'],
                'office': cand_data['office'],
                'state': cand_data['state'],
                'district': cand_data['district'],
                
                # 🎯 TOP-LEVEL QUICK STATS (for easy viewing/sorting)
                'total_independent_support': total_independent_support,      # Super PAC money FOR
                'total_independent_oppose': total_independent_oppose,        # Super PAC money AGAINST
                'total_pac_contributions': total_pac_contributions,          # Direct PAC donations
                'net_independent_expenditures': net_independent_expenditures, # Indie support - oppose
                'net_support': net_support,                                  # PAC + net indie
                'net_benefit': net_benefit,                                  # Everything helping - opposition
                'total_transactions': indie_support_count + indie_oppose_count + pac_count,
                'cycles_active': sorted(cand_data['by_cycle'].keys()),
                
                # Detailed breakdowns
                'by_cycle': cand_data['by_cycle'],
                
                'totals': {
                    'independent_expenditures': {
                        'support': {
                            'total_amount': indie_support_total,
                            'transaction_count': indie_support_count,
                            'committees': indie_support_top  # All committees, sorted by amount
                        },
                        'oppose': {
                            'total_amount': indie_oppose_total,
                            'transaction_count': indie_oppose_count,
                            'committees': indie_oppose_top  # All committees, sorted by amount
                        }
                    },
                    'pac_contributions': {
                        'total_amount': pac_total,
                        'transaction_count': pac_count,
                        'committees': pac_top  # All committees, sorted by amount
                    },
                    'summary': {
                        'total_independent_support': total_independent_support,
                        'total_independent_oppose': total_independent_oppose,
                        'total_pac_contributions': total_pac_contributions,
                        'net_independent_expenditures': net_independent_expenditures,
                        'net_support': net_support,
                        'net_benefit': net_benefit,
                        'total_transactions': indie_support_count + indie_oppose_count + pac_count,
                        'cycles_tracked': sorted(cand_data['by_cycle'].keys())
                    }
                },
                
                'computed_at': datetime.now()
            }
            
            batch.append(cross_cycle_record)
        
        # Sort all candidates by net_benefit (descending) before inserting
        context.log.info(f"Sorting {len(batch)} candidates by net_benefit (highest to lowest)...")
        batch.sort(key=lambda x: x['net_benefit'], reverse=True)
        
        # Insert in sorted order
        if batch:
            financials_collection.insert_many(batch, ordered=True)  # ordered=True preserves sort order
            
            # Log top 10 for visibility
            context.log.info("📊 Top 10 candidates by net benefit:")
            for i, cand in enumerate(batch[:10], 1):
                context.log.info(
                    f"  {i}. {cand['candidate_name']} ({cand['party']}): "
                    f"${cand['net_benefit']:,.0f} net benefit "
                    f"(${cand['total_pac_contributions']:,.0f} PAC + "
                    f"${cand['net_independent_expenditures']:,.0f} net indie)"
                )
        
        # Create indexes for fast sorting/filtering on top-level fields
        context.log.info("Creating indexes for quick stats...")
        financials_collection.create_index([("net_benefit", -1)])                     # Sort by net benefit (primary)
        financials_collection.create_index([("net_support", -1)])                     # Sort by net support
        financials_collection.create_index([("total_independent_support", -1)])       # Sort by indie support
        financials_collection.create_index([("total_independent_oppose", -1)])        # Sort by indie opposition
        financials_collection.create_index([("total_pac_contributions", -1)])         # Sort by PAC money
        financials_collection.create_index([("net_independent_expenditures", -1)])    # Sort by net indie
        financials_collection.create_index([("party", 1)])                    # Filter by party
        financials_collection.create_index([("office", 1)])                   # Filter by office
        financials_collection.create_index([("candidate_name", 1)])           # Search by name
        
        stats = {
            'candidates_aggregated': len(all_candidates),
            'cycles_processed': config.cycles,
        }
        
        context.log.info(f"Cross-cycle candidate financials aggregation complete: {stats}")
        return stats
