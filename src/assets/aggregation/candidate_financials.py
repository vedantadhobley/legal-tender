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
        "enriched_committee_funding": AssetIn(key="enriched_committee_funding"),
    },
    compute_kind="aggregation",
    description="Cross-cycle aggregation of candidate financials with upstream funding sources for committees"
)
def candidate_financials_asset(
    context: AssetExecutionContext,
    config: CandidateFinancialsConfig,
    mongo: MongoDBResource,
    enriched_candidate_financials: Dict[str, Any],
    enriched_committee_funding: Dict[str, Any],
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
        # Group by bioguide_id (not candidate_id) since members can have multiple candidate IDs
        all_candidates = {}
        
        for cycle in config.cycles:
            context.log.info(f"Reading cycle {cycle} candidate financials")
            lt_financials = mongo.get_collection(client, "candidate_financials", database_name=f"enriched_{cycle}")
            
            for doc in lt_financials.find():
                bioguide_id = doc.get('bioguide_id')
                if not bioguide_id:
                    context.log.warning("Skipping document - missing bioguide_id")
                    continue
                
                if bioguide_id not in all_candidates:
                    all_candidates[bioguide_id] = {
                        'bioguide_id': bioguide_id,
                        'candidate_ids': set(),  # Accumulate ALL candidate IDs across cycles
                        'candidate_name': doc.get('candidate_name'),
                        'party': doc.get('party'),
                        'office': doc.get('office'),
                        'state': doc.get('state'),
                        'district': doc.get('district'),
                        'by_cycle': {},
                    }
                
                # Add candidate IDs from this cycle
                doc_candidate_ids = doc.get('candidate_ids', [])
                all_candidates[bioguide_id]['candidate_ids'].update(doc_candidate_ids)
                
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
                
                all_candidates[bioguide_id]['by_cycle'][cycle] = cycle_data
        
        context.log.info(f"Found {len(all_candidates)} unique members (by bioguide_id) across all cycles")
        
        # Compute totals across cycles
        batch = []
        for bioguide_id, member_data in all_candidates.items():
            # Convert candidate_ids set to sorted list
            candidate_ids_list = sorted(list(member_data['candidate_ids']))
            
            # Aggregate independent expenditures support
            indie_support_committees = {}
            indie_oppose_committees = {}
            
            for cycle, cycle_data in member_data['by_cycle'].items():
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
            
            # Aggregate Direct Contributions (NEW: Separated by type)
            corporate_pac_committees = {}
            political_pac_committees = {}
            
            for cycle, cycle_data in member_data['by_cycle'].items():
                # NEW: Use separated direct_contributions structure
                if 'direct_contributions' in cycle_data:
                    # Corporate PACs (Q/N types)
                    for cmte in cycle_data['direct_contributions']['from_corporate_pacs']['committees']:
                        cmte_id = cmte['committee_id']
                        if cmte_id not in corporate_pac_committees:
                            corporate_pac_committees[cmte_id] = {
                                'committee_id': cmte_id,
                                'committee_name': cmte['committee_name'],
                                'committee_type': cmte['committee_type'],
                                'connected_org': cmte['connected_org'],
                                'total_amount': 0,
                                'transaction_count': 0,
                                'cycles': []
                            }
                        corporate_pac_committees[cmte_id]['total_amount'] += cmte['total_amount']
                        corporate_pac_committees[cmte_id]['transaction_count'] += cmte['transaction_count']
                        if cycle not in corporate_pac_committees[cmte_id]['cycles']:
                            corporate_pac_committees[cmte_id]['cycles'].append(cycle)
                    
                    # Political PACs (O/W/I types)
                    for cmte in cycle_data['direct_contributions']['from_political_pacs']['committees']:
                        cmte_id = cmte['committee_id']
                        if cmte_id not in political_pac_committees:
                            political_pac_committees[cmte_id] = {
                                'committee_id': cmte_id,
                                'committee_name': cmte['committee_name'],
                                'committee_type': cmte['committee_type'],
                                'connected_org': cmte['connected_org'],
                                'total_amount': 0,
                                'transaction_count': 0,
                                'cycles': []
                            }
                        political_pac_committees[cmte_id]['total_amount'] += cmte['total_amount']
                        political_pac_committees[cmte_id]['transaction_count'] += cmte['transaction_count']
                        if cycle not in political_pac_committees[cmte_id]['cycles']:
                            political_pac_committees[cmte_id]['cycles'].append(cycle)
                
                # FALLBACK: Support old pac_contributions structure for backward compatibility
                elif 'pac_contributions' in cycle_data:
                    for cmte in cycle_data['pac_contributions']['committees']:
                        cmte_id = cmte['committee_id']
                        cmte_type = cmte.get('committee_type', 'Unknown')
                        
                        # Separate by type
                        is_corporate = cmte_type in ['Q', 'N']
                        target_dict = corporate_pac_committees if is_corporate else political_pac_committees
                        
                        if cmte_id not in target_dict:
                            target_dict[cmte_id] = {
                                'committee_id': cmte_id,
                                'committee_name': cmte['committee_name'],
                                'committee_type': cmte_type,
                                'connected_org': cmte['connected_org'],
                                'total_amount': 0,
                                'transaction_count': 0,
                                'cycles': []
                            }
                        target_dict[cmte_id]['total_amount'] += cmte['total_amount']
                        target_dict[cmte_id]['transaction_count'] += cmte['transaction_count']
                        if cycle not in target_dict[cmte_id]['cycles']:
                            target_dict[cmte_id]['cycles'].append(cycle)
            
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
            
            corporate_pac_top = sorted(
                corporate_pac_committees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            political_pac_top = sorted(
                political_pac_committees.values(),
                key=lambda x: x['total_amount'],
                reverse=True
            )
            
            # Calculate totals
            indie_support_total = sum(c['total_amount'] for c in indie_support_committees.values())
            indie_support_count = sum(c['transaction_count'] for c in indie_support_committees.values())
            indie_oppose_total = sum(c['total_amount'] for c in indie_oppose_committees.values())
            indie_oppose_count = sum(c['transaction_count'] for c in indie_oppose_committees.values())
            
            corporate_pac_total = sum(c['total_amount'] for c in corporate_pac_committees.values())
            corporate_pac_count = sum(c['transaction_count'] for c in corporate_pac_committees.values())
            political_pac_total = sum(c['total_amount'] for c in political_pac_committees.values())
            political_pac_count = sum(c['transaction_count'] for c in political_pac_committees.values())
            
            # Combined totals (for backward compatibility in summary)
            pac_total = corporate_pac_total + political_pac_total
            pac_count = corporate_pac_count + political_pac_count
            
            # Calculate top-level summary metrics for easy sorting/filtering
            total_independent_support = indie_support_total
            total_independent_oppose = indie_oppose_total
            total_pac_contributions = pac_total
            net_independent_expenditures = indie_support_total - indie_oppose_total
            net_support = indie_support_total + pac_total  # Total positive support (indie support + PAC)
            net_benefit = (indie_support_total + pac_total) - indie_oppose_total  # Everything helping - opposition
            
            # ========================================================================
            # UPSTREAM TRACING FOR POLITICAL COMMITTEES
            # ========================================================================
            # Trace money back through political committees (O/W/I types) to ultimate sources
            # 
            # For PAC Contributions:
            #   - Q/N types (Corporate PACs) = Don't trace, these ARE the organizations
            #   - O/W/I types (Political/Super PACs) = Trace upstream to see who funded them
            # 
            # For Independent Expenditures (ads):
            #   - Almost always O/W/I types = Trace all of them
            # 
            # Goal: Show which orgs/individuals are ultimately influencing candidates
            #       through political committee intermediaries
            # ========================================================================
            
            context.log.info(f"  Tracing upstream funding for political committees influencing {bioguide_id}...")
            
            # NEW: Collect all committees that need upstream tracing
            # 1. All indie expenditure committees (almost all are O/W/I types)
            # 2. Political PAC contribution committees (already separated - political_pac_committees)
            committees_to_trace = list(indie_support_committees.keys()) + list(indie_oppose_committees.keys())
            
            # Add political PAC contribution committees (O/W/I types already separated)
            committees_to_trace.extend(list(political_pac_committees.keys()))
            
            upstream_by_committee = {}
            for cycle in config.cycles:
                funding_collection = mongo.get_collection(
                    client, 
                    "committee_funding_sources", 
                    database_name=f"enriched_{cycle}"
                )
                
                # Batch query: get funding sources for political committees
                for funding_doc in funding_collection.find({'_id': {'$in': committees_to_trace}}):
                    cmte_id = funding_doc['_id']
                    if cmte_id not in upstream_by_committee:
                        upstream_by_committee[cmte_id] = {
                            'from_committees': 0,
                            'from_individuals': 0,
                            'from_organizations': 0
                        }
                    
                    # Aggregate totals from this cycle
                    upstream_by_committee[cmte_id]['from_committees'] += funding_doc.get('transparency', {}).get('from_committees', 0)
                    upstream_by_committee[cmte_id]['from_individuals'] += funding_doc.get('transparency', {}).get('from_individuals', 0)
                    upstream_by_committee[cmte_id]['from_organizations'] += funding_doc.get('transparency', {}).get('from_organizations', 0)
            
            # Initialize upstream funding sources for independent expenditures
            # (PAC contributions now tracked per-committee in direct_funding structure)
            upstream_funding = {
                'independent_expenditures': {
                    'supporting': {
                        'total_amount': indie_support_total,
                        'funded_by': {
                            'organizations': 0,           # Org → Super PAC → Ad FOR candidate
                            'individuals': 0,             # Individual → Super PAC → Ad FOR candidate
                            'political_committees': 0,    # PAC → Super PAC → Ad FOR candidate
                            'unknown': 0                  # Dark money
                        }
                    },
                    'opposing': {
                        'total_amount': indie_oppose_total,
                        'funded_by': {
                            'organizations': 0,           # Org → Super PAC → Ad AGAINST candidate
                            'individuals': 0,             # Individual → Super PAC → Ad AGAINST candidate
                            'political_committees': 0,    # PAC → Super PAC → Ad AGAINST candidate
                            'unknown': 0                  # Dark money
                        }
                    },
                    'net': {
                        'organizations': 0,               # Support - Oppose from orgs
                        'individuals': 0,                 # Support - Oppose from individuals
                        'political_committees': 0,        # Support - Oppose from PACs
                        'unknown': 0                      # Net dark money
                    }
                }
            }
            
            # ========================================================================
            # PROCESS INDEPENDENT EXPENDITURES (Super PAC ads)
            # ========================================================================
            # NOTE: Direct PAC contributions now stored per-committee in direct_funding
            # structure with individual upstream_funding for each political PAC
            
            # Process independent expenditures FOR candidate
            for cmte_id, cmte_data in indie_support_committees.items():
                amount = cmte_data['total_amount']
                upstream = upstream_by_committee.get(cmte_id, {})
                
                if upstream:
                    total_upstream = (
                        upstream.get('from_organizations', 0) +
                        upstream.get('from_individuals', 0) +
                        upstream.get('from_committees', 0)
                    )
                    
                    if total_upstream > 0:
                        # Proportionally attribute ad spending to ultimate sources
                        # Example: Super PAC spent $1M on ads FOR candidate
                        # Super PAC is 60% org-funded, 30% individual-funded, 10% PAC-funded
                        # → $600K from orgs, $300K from individuals, $100K from PACs
                        org_proportion = upstream.get('from_organizations', 0) / total_upstream
                        ind_proportion = upstream.get('from_individuals', 0) / total_upstream
                        pac_proportion = upstream.get('from_committees', 0) / total_upstream
                        
                        upstream_funding['independent_expenditures']['supporting']['funded_by']['organizations'] += amount * org_proportion
                        upstream_funding['independent_expenditures']['supporting']['funded_by']['individuals'] += amount * ind_proportion
                        upstream_funding['independent_expenditures']['supporting']['funded_by']['political_committees'] += amount * pac_proportion
                    else:
                        # No upstream funding = dark money
                        upstream_funding['independent_expenditures']['supporting']['funded_by']['unknown'] += amount
                else:
                    # No upstream data available
                    upstream_funding['independent_expenditures']['supporting']['funded_by']['unknown'] += amount
            
            # Process independent expenditures AGAINST candidate
            for cmte_id, cmte_data in indie_oppose_committees.items():
                amount = cmte_data['total_amount']
                upstream = upstream_by_committee.get(cmte_id, {})
                
                if upstream:
                    total_upstream = (
                        upstream.get('from_organizations', 0) +
                        upstream.get('from_individuals', 0) +
                        upstream.get('from_committees', 0)
                    )
                    
                    if total_upstream > 0:
                        # Proportionally attribute opposition ad spending
                        org_proportion = upstream.get('from_organizations', 0) / total_upstream
                        ind_proportion = upstream.get('from_individuals', 0) / total_upstream
                        pac_proportion = upstream.get('from_committees', 0) / total_upstream
                        
                        upstream_funding['independent_expenditures']['opposing']['funded_by']['organizations'] += amount * org_proportion
                        upstream_funding['independent_expenditures']['opposing']['funded_by']['individuals'] += amount * ind_proportion
                        upstream_funding['independent_expenditures']['opposing']['funded_by']['political_committees'] += amount * pac_proportion
                    else:
                        upstream_funding['independent_expenditures']['opposing']['funded_by']['unknown'] += amount
                else:
                    upstream_funding['independent_expenditures']['opposing']['funded_by']['unknown'] += amount
            
            # Calculate net effect for independent expenditures (support - oppose)
            upstream_funding['independent_expenditures']['net']['organizations'] = (
                upstream_funding['independent_expenditures']['supporting']['funded_by']['organizations'] - 
                upstream_funding['independent_expenditures']['opposing']['funded_by']['organizations']
            )
            upstream_funding['independent_expenditures']['net']['individuals'] = (
                upstream_funding['independent_expenditures']['supporting']['funded_by']['individuals'] - 
                upstream_funding['independent_expenditures']['opposing']['funded_by']['individuals']
            )
            upstream_funding['independent_expenditures']['net']['political_committees'] = (
                upstream_funding['independent_expenditures']['supporting']['funded_by']['political_committees'] - 
                upstream_funding['independent_expenditures']['opposing']['funded_by']['political_committees']
            )
            upstream_funding['independent_expenditures']['net']['unknown'] = (
                upstream_funding['independent_expenditures']['supporting']['funded_by']['unknown'] - 
                upstream_funding['independent_expenditures']['opposing']['funded_by']['unknown']
            )
            
            # Round all values
            for section in ['supporting', 'opposing']:
                for key in upstream_funding['independent_expenditures'][section]['funded_by']:
                    upstream_funding['independent_expenditures'][section]['funded_by'][key] = round(
                        upstream_funding['independent_expenditures'][section]['funded_by'][key], 2
                    )
            for key in upstream_funding['independent_expenditures']['net']:
                upstream_funding['independent_expenditures']['net'][key] = round(
                    upstream_funding['independent_expenditures']['net'][key], 2
                )
            
            cross_cycle_record = {
                '_id': bioguide_id,
                'bioguide_id': bioguide_id,
                'candidate_ids': candidate_ids_list,
                'candidate_name': member_data['candidate_name'],
                'party': member_data['party'],
                'office': member_data['office'],
                'state': member_data['state'],
                'district': member_data['district'],
                
                # 🎯 TOP-LEVEL QUICK STATS (for easy viewing/sorting)
                'total_independent_support': total_independent_support,      # Super PAC money FOR
                'total_independent_oppose': total_independent_oppose,        # Super PAC money AGAINST
                'total_independent_expenditures': indie_support_total + indie_oppose_total,  # Total indie (support + oppose)
                'total_pac_contributions': total_pac_contributions,          # Direct PAC donations
                'net_independent_expenditures': net_independent_expenditures, # Indie support - oppose
                'net_support': net_support,                                  # Total positive money (indie support + PAC)
                'net_benefit': net_benefit,                                  # Everything helping - opposition
                'total_transactions': indie_support_count + indie_oppose_count + pac_count,
                'cycles_active': sorted(member_data['by_cycle'].keys()),
                
                # 💰 UPSTREAM FUNDING for Independent Expenditures (Super PAC ads)
                # (Direct PAC contributions now in totals.direct_funding with per-committee upstream)
                'upstream_funding': {
                    'independent_expenditures': upstream_funding['independent_expenditures']
                },
                
                # Detailed breakdowns
                'by_cycle': member_data['by_cycle'],
                
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
                    
                    # NEW: Direct funding structure with separated lists
                    'direct_funding': {
                        'from_corporate_pacs': {
                            'total_amount': sum(c['total_amount'] for c in corporate_pac_committees.values()),
                            'transaction_count': sum(c['transaction_count'] for c in corporate_pac_committees.values()),
                            'committees': corporate_pac_top  # Q/N types - these ARE the organizations
                        },
                        'from_political_pacs': {
                            'total_amount': sum(c['total_amount'] for c in political_pac_committees.values()),
                            'transaction_count': sum(c['transaction_count'] for c in political_pac_committees.values()),
                            'committees': [
                                {
                                    **cmte,
                                    'upstream_funding': upstream_by_committee.get(cmte['committee_id'], {
                                        'from_organizations': 0,
                                        'from_individuals': 0,
                                        'from_committees': 0
                                    })
                                }
                                for cmte in political_pac_top
                            ]  # O/W/I types - with per-committee upstream tracing
                        },
                        'from_individuals': {
                            'total_amount': 0,  # Future implementation
                            'transaction_count': 0,
                            'donors': []
                        }
                    },
                    
                    # DEPRECATED: Keep for backward compatibility
                    'pac_contributions': {
                        'total_amount': pac_total,
                        'transaction_count': pac_count,
                        'committees': sorted(
                            corporate_pac_top + political_pac_top,
                            key=lambda x: x['total_amount'],
                            reverse=True
                        )[:20]  # Combined top 20 from both types
                    },
                    'summary': {
                        'total_independent_support': total_independent_support,
                        'total_independent_oppose': total_independent_oppose,
                        'total_independent_expenditures': indie_support_total + indie_oppose_total,
                        'total_pac_contributions': total_pac_contributions,
                        'net_independent_expenditures': net_independent_expenditures,
                        'net_support': net_support,
                        'net_benefit': net_benefit,
                        'total_transactions': indie_support_count + indie_oppose_count + pac_count,
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
