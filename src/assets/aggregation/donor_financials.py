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
        "enriched_committee_funding": AssetIn(key="enriched_committee_funding"),
    },
    compute_kind="aggregation",
    description="Cross-cycle aggregation of donor financials with upstream funding sources and spending across all cycles"
)
def donor_financials_asset(
    context: AssetExecutionContext,
    config: DonorFinancialsConfig,
    mongo: MongoDBResource,
    enriched_donor_financials: Dict[str, Any],
    enriched_committee_funding: Dict[str, Any],
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
                        'categories': doc['operating_expenditures']['categories'],
                        'payees': [
                            {
                                'payee_name': p['payee_name'],
                                'total_amount': p['total_amount'],
                                'transaction_count': p['transaction_count'],
                            }
                            for p in doc['operating_expenditures']['payees']
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
                for cat in cycle_data['operating_expenditures']['categories']:
                    cat_name = cat['category']
                    if cat_name not in oppexp_categories:
                        oppexp_categories[cat_name] = {
                            'category': cat_name,
                            'total_amount': 0,
                            'transaction_count': 0
                        }
                    oppexp_categories[cat_name]['total_amount'] += cat['total_amount']
                    oppexp_categories[cat_name]['transaction_count'] += cat['transaction_count']
                
                for payee in cycle_data['operating_expenditures']['payees']:
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
            
            # Sort all (keep ALL for UI linking)
            indie_support_top = sorted(
                indie_support_candidates.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            indie_oppose_top = sorted(
                indie_oppose_candidates.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            pac_top = sorted(
                pac_candidates.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            oppexp_categories_top = sorted(
                oppexp_categories.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            oppexp_payees_top = sorted(
                oppexp_payees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            # Calculate totals
            indie_support_total = sum(c['total_amount'] for c in indie_support_candidates.values())
            indie_support_count = sum(c['transaction_count'] for c in indie_support_candidates.values())
            indie_oppose_total = sum(c['total_amount'] for c in indie_oppose_candidates.values())
            indie_oppose_count = sum(c['transaction_count'] for c in indie_oppose_candidates.values())
            pac_total = sum(c['total_amount'] for c in pac_candidates.values())
            pac_count = sum(c['transaction_count'] for c in pac_candidates.values())
            oppexp_total = sum(c['total_amount'] for c in oppexp_categories.values())
            oppexp_count = sum(c['transaction_count'] for c in oppexp_categories.values())
            
            # Calculate top-level summary metrics for easy sorting/filtering
            total_independent_expenditures = indie_support_total + indie_oppose_total
            total_to_candidates = total_independent_expenditures + pac_total
            total_spent = total_to_candidates + oppexp_total
            net_independent_expenditures = indie_support_total - indie_oppose_total
            
            # 🎯 FETCH UPSTREAM FUNDING SOURCES (who funds THIS committee)
            upstream_funding = {
                'from_committees': [],
                'from_individuals': [],
                'from_organizations': [],
                'totals': {
                    'from_committees': 0,
                    'from_individuals': 0,
                    'from_organizations': 0,
                    'total_disclosed': 0
                },
                'transparency_score': None,
                'red_flags': []
            }
            
            # Pull from each cycle's committee_funding_sources
            for cycle in config.cycles:
                funding_collection = mongo.get_collection(
                    client, 
                    "committee_funding_sources", 
                    database_name=f"enriched_{cycle}"
                )
                
                funding_doc = funding_collection.find_one({'_id': cmte_data['committee_id']})
                if funding_doc:
                    # Aggregate from_committees across cycles
                    for cmte_source in funding_doc.get('funding_sources', {}).get('from_committees', []):
                        # Check if we already have this committee
                        existing = next(
                            (c for c in upstream_funding['from_committees'] if c['committee_id'] == cmte_source['committee_id']),
                            None
                        )
                        if existing:
                            existing['total_amount'] += cmte_source['total_amount']
                            existing['transaction_count'] += cmte_source['transaction_count']
                            if cycle not in existing['cycles']:
                                existing['cycles'].append(cycle)
                        else:
                            upstream_funding['from_committees'].append({
                                'committee_id': cmte_source['committee_id'],
                                'committee_name': cmte_source['committee_name'],
                                'committee_type': cmte_source.get('committee_type'),
                                'total_amount': cmte_source['total_amount'],
                                'transaction_count': cmte_source['transaction_count'],
                                'cycles': [cycle]
                            })
                    
                    # Aggregate from_individuals across cycles
                    for ind_source in funding_doc.get('funding_sources', {}).get('from_individuals', []):
                        donor_key = f"{ind_source['contributor_name']}|{ind_source.get('employer', 'Unknown')}"
                        existing = next(
                            (i for i in upstream_funding['from_individuals'] 
                             if f"{i['contributor_name']}|{i.get('employer', 'Unknown')}" == donor_key),
                            None
                        )
                        if existing:
                            existing['total_amount'] += ind_source['total_amount']
                            existing['contribution_count'] += ind_source['contribution_count']
                            if cycle not in existing['cycles']:
                                existing['cycles'].append(cycle)
                        else:
                            upstream_funding['from_individuals'].append({
                                'contributor_name': ind_source['contributor_name'],
                                'employer': ind_source.get('employer'),
                                'occupation': ind_source.get('occupation'),
                                'total_amount': ind_source['total_amount'],
                                'contribution_count': ind_source['contribution_count'],
                                'cycles': [cycle]
                            })
                    
                    # Aggregate from_organizations across cycles
                    for org_source in funding_doc.get('funding_sources', {}).get('from_organizations', []):
                        existing = next(
                            (o for o in upstream_funding['from_organizations'] 
                             if o['organization_name'] == org_source['organization_name']),
                            None
                        )
                        if existing:
                            existing['total_amount'] += org_source['total_amount']
                            existing['contribution_count'] += org_source['contribution_count']
                            if cycle not in existing['cycles']:
                                existing['cycles'].append(cycle)
                        else:
                            upstream_funding['from_organizations'].append({
                                'organization_name': org_source['organization_name'],
                                'total_amount': org_source['total_amount'],
                                'contribution_count': org_source['contribution_count'],
                                'cycles': [cycle]
                            })
                    
                    # Aggregate transparency data
                    if funding_doc.get('transparency'):
                        trans = funding_doc['transparency']
                        upstream_funding['totals']['from_committees'] += trans.get('from_committees', 0)
                        upstream_funding['totals']['from_individuals'] += trans.get('from_individuals', 0)
                        upstream_funding['totals']['from_organizations'] += trans.get('from_organizations', 0)
                        upstream_funding['totals']['total_disclosed'] += trans.get('total_disclosed', 0)
                        
                        # Use most recent transparency score
                        if trans.get('transparency_score') is not None:
                            upstream_funding['transparency_score'] = trans['transparency_score']
                        
                        # Collect red flags
                        for flag in trans.get('red_flags', []):
                            if flag not in upstream_funding['red_flags']:
                                upstream_funding['red_flags'].append(flag)
            
            # Sort upstream sources by amount
            upstream_funding['from_committees'].sort(key=lambda x: x['total_amount'], reverse=True)
            upstream_funding['from_individuals'].sort(key=lambda x: x['total_amount'], reverse=True)
            upstream_funding['from_organizations'].sort(key=lambda x: x['total_amount'], reverse=True)
            
            cross_cycle_record = {
                '_id': cmte_data['committee_id'],
                'committee_id': cmte_data['committee_id'],
                'committee_name': cmte_data['committee_name'],
                'committee_type': cmte_data['committee_type'],
                'connected_org': cmte_data['connected_org'],
                'party': cmte_data['party'],
                
                # 🎯 TOP-LEVEL QUICK STATS (for easy viewing/sorting)
                'total_spent': total_spent,                              # Everything this committee spent
                'total_to_candidates': total_to_candidates,              # Money directly to/about candidates (indie + PAC)
                'total_independent_expenditures': total_independent_expenditures,  # Total indie spending (support + oppose)
                'total_independent_support': indie_support_total,        # Independent expenditures FOR
                'total_independent_oppose': indie_oppose_total,          # Independent expenditures AGAINST
                'net_independent_expenditures': net_independent_expenditures,  # Net indie (support - oppose)
                'total_pac_contributions': pac_total,                    # Direct PAC contributions
                'total_operating_expenditures': oppexp_total,            # Operating expenses (ads, vendors, etc)
                'total_transactions': indie_support_count + indie_oppose_count + pac_count + oppexp_count,
                'cycles_active': sorted(cmte_data['by_cycle'].keys()),
                
                # 🎯 UPSTREAM FUNDING (WHO funds this committee - the ROOT MONEY!)
                'upstream_funding': upstream_funding,
                
                # Detailed breakdowns
                'by_cycle': cmte_data['by_cycle'],
                
                'totals': {
                    'independent_expenditures': {
                        'support': {
                            'total_amount': indie_support_total,
                            'transaction_count': indie_support_count,
                            'candidates': indie_support_top  # All candidates, sorted by amount
                        },
                        'oppose': {
                            'total_amount': indie_oppose_total,
                            'transaction_count': indie_oppose_count,
                            'candidates': indie_oppose_top  # All candidates, sorted by amount
                        }
                    },
                    'pac_contributions': {
                        'total_amount': pac_total,
                        'transaction_count': pac_count,
                        'candidates': pac_top  # All candidates, sorted by amount
                    },
                    'operating_expenditures': {
                        'total_amount': oppexp_total,
                        'transaction_count': oppexp_count,
                        'categories': oppexp_categories_top,  # All categories, sorted by amount
                        'payees': oppexp_payees_top  # All payees, sorted by amount
                    },
                    'summary': {
                        # Raw totals
                        'total_independent_expenditures': total_independent_expenditures,
                        'total_independent_support': indie_support_total,
                        'total_independent_oppose': indie_oppose_total,
                        'total_pac_contributions': pac_total,
                        'total_operating': oppexp_total,
                        
                        # Calculated fields
                        'net_independent_expenditures': net_independent_expenditures,
                        'total_to_candidates': total_to_candidates,
                        'total_spent': total_spent,
                        
                        # Metadata
                        'total_transactions': indie_support_count + indie_oppose_count + pac_count + oppexp_count,
                    }
                },
                
                'computed_at': datetime.now()
            }
            
            batch.append(cross_cycle_record)
        
        # Sort all committees by total_to_candidates (descending) before inserting
        context.log.info(f"Sorting {len(batch)} committees by total_to_candidates (highest to lowest)...")
        batch.sort(key=lambda x: x['total_to_candidates'], reverse=True)
        
        # Insert in sorted order
        if batch:
            financials_collection.insert_many(batch, ordered=True)  # ordered=True preserves sort order
            
            # Log top 10 for visibility
            context.log.info("📊 Top 10 committees by total to candidates:")
            for i, cmte in enumerate(batch[:10], 1):
                pac = cmte['total_pac_contributions']
                indie_net = cmte['net_independent_expenditures']
                context.log.info(
                    f"  {i}. {cmte['committee_name']}: "
                    f"${cmte['total_to_candidates']:,.0f} to candidates "
                    f"(${pac:,.0f} PAC + ${indie_net:,.0f} net indie)"
                )
        
        # Create indexes for fast sorting/filtering on top-level fields
        context.log.info("Creating indexes for quick stats...")
        financials_collection.create_index([("total_spent", -1)])                     # Sort by total spending
        financials_collection.create_index([("total_to_candidates", -1)])            # Sort by candidate contributions
        financials_collection.create_index([("total_pac_contributions", -1)])        # Sort by PAC contributions
        financials_collection.create_index([("total_independent_support", -1)])      # Sort by indie support
        financials_collection.create_index([("total_independent_oppose", -1)])       # Sort by indie opposition
        financials_collection.create_index([("net_independent_expenditures", -1)])   # Sort by net indie
        financials_collection.create_index([("total_operating_expenditures", -1)])   # Sort by operating expenses
        financials_collection.create_index([("committee_type", 1)])                  # Filter by type
        financials_collection.create_index([("party", 1)])                           # Filter by party
        financials_collection.create_index([("committee_name", 1)])                  # Search by name
        financials_collection.create_index([("connected_org", 1)])                   # Search by connected org
        
        stats = {
            'committees_aggregated': len(all_committees),
            'cycles_processed': config.cycles,
        }
        
        context.log.info(f"Cross-cycle donor financials aggregation complete: {stats}")
        return stats
