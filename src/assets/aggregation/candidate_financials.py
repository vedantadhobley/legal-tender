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
                    'direct_contributions': {
                        'from_corporate_pacs': {
                            'total_amount': doc['direct_contributions']['from_corporate_pacs']['total_amount'],
                            'transaction_count': doc['direct_contributions']['from_corporate_pacs']['transaction_count'],
                            'committees': [
                                {
                                    'committee_id': c['committee_id'],
                                    'committee_name': c['committee_name'],
                                    'committee_type': c['committee_type'],
                                    'connected_org': c['connected_org'],
                                    'total_amount': c['total_amount'],
                                    'transaction_count': c['transaction_count'],
                                }
                                for c in doc['direct_contributions']['from_corporate_pacs']['committees']
                            ]
                        },
                        'from_political_pacs': {
                            'total_amount': doc['direct_contributions']['from_political_pacs']['total_amount'],
                            'transaction_count': doc['direct_contributions']['from_political_pacs']['transaction_count'],
                            'committees': [
                                {
                                    'committee_id': c['committee_id'],
                                    'committee_name': c['committee_name'],
                                    'committee_type': c['committee_type'],
                                    'connected_org': c['connected_org'],
                                    'total_amount': c['total_amount'],
                                    'transaction_count': c['transaction_count'],
                                }
                                for c in doc['direct_contributions']['from_political_pacs']['committees']
                            ]
                        }
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
            
            # Combined PAC totals for summary stats
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
            
            # Initialize ALL committees with empty upstream arrays
            # This ensures committees with no upstream data still have the structure
            upstream_by_committee = {
                cmte_id: {
                    'from_committees': [],
                    'from_individuals': [],
                    'from_organizations': []
                }
                for cmte_id in committees_to_trace
            }
            
            for cycle in config.cycles:
                funding_collection = mongo.get_collection(
                    client, 
                    "committee_funding_sources", 
                    database_name=f"enriched_{cycle}"
                )
                
                # Batch query: get funding sources for political committees
                # Note: Committees not found in this collection will keep their empty arrays
                for funding_doc in funding_collection.find({'_id': {'$in': committees_to_trace}}):
                    cmte_id = funding_doc['_id']
                    
                    # Extract detailed funding sources (WHO gave WHAT)
                    funding_sources = funding_doc.get('funding_sources', {})
                    
                    # Organizations (includes reclassified corporate PACs)
                    for org in funding_sources.get('from_organizations', []):
                        upstream_by_committee[cmte_id]['from_organizations'].append({
                            'organization_name': org.get('organization_name', 'Unknown'),
                            'amount': org.get('total_amount', 0),
                            'pac_committee_id': org.get('pac_committee_id'),
                            'organization_type': org.get('organization_type', 'Unknown')
                        })
                    
                    # Individuals (future: when indiv.zip is parsed)
                    for ind in funding_sources.get('from_individuals', []):
                        upstream_by_committee[cmte_id]['from_individuals'].append({
                            'name': ind.get('name', 'Unknown'),
                            'amount': ind.get('amount', 0)
                        })
                    
                    # Committees (political PACs funding other political PACs)
                    for cmte in funding_sources.get('from_committees', []):
                        upstream_by_committee[cmte_id]['from_committees'].append({
                            'committee_name': cmte.get('committee_name', 'Unknown'),
                            'committee_id': cmte.get('committee_id'),
                            'committee_type': cmte.get('committee_type', 'Unknown'),
                            'amount': cmte.get('total_amount', 0)
                        })
            
            # ========================================================================
            # SORT UPSTREAM FUNDING ARRAYS
            # ========================================================================
            # After aggregating across cycles, sort each committee's upstream arrays
            # by amount (descending) so the largest donors appear first
            for cmte_id in upstream_by_committee:
                upstream_by_committee[cmte_id]['from_committees'].sort(
                    key=lambda x: x['amount'], reverse=True
                )
                upstream_by_committee[cmte_id]['from_organizations'].sort(
                    key=lambda x: x['amount'], reverse=True
                )
                upstream_by_committee[cmte_id]['from_individuals'].sort(
                    key=lambda x: x['amount'], reverse=True
                )
            
            # ========================================================================
            # NO AGGREGATE UPSTREAM CALCULATIONS
            # ========================================================================
            # We provide upstream funding detail at the PER-COMMITTEE level only.
            # Each committee in the arrays below has its own upstream_funding showing
            # WHO gave them WHAT amount.
            

            
            cross_cycle_record = {
                '_id': bioguide_id,
                'bioguide_id': bioguide_id,
                'candidate_ids': candidate_ids_list,
                'candidate_name': member_data['candidate_name'],
                'party': member_data['party'],
                'office': member_data['office'],
                'state': member_data['state'],
                'district': member_data['district'],
                'cycles_active': sorted(member_data['by_cycle'].keys()),
                
                # Detailed breakdowns
                'by_cycle': member_data['by_cycle'],
                
                'totals': {
                    'independent_expenditures': {
                        'support': {
                            'total_amount': indie_support_total,
                            'transaction_count': indie_support_count,
                            'committees': [
                                {
                                    **cmte,
                                    'upstream_funding': upstream_by_committee[cmte['committee_id']]
                                }
                                for cmte in indie_support_top
                            ]
                        },
                        'oppose': {
                            'total_amount': indie_oppose_total,
                            'transaction_count': indie_oppose_count,
                            'committees': [
                                {
                                    **cmte,
                                    'upstream_funding': upstream_by_committee[cmte['committee_id']]
                                }
                                for cmte in indie_oppose_top
                            ]
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
                                    'upstream_funding': upstream_by_committee[cmte['committee_id']]
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
        batch.sort(key=lambda x: x['totals']['summary']['net_benefit'], reverse=True)
        
        # Insert in sorted order
        if batch:
            financials_collection.insert_many(batch, ordered=True)  # ordered=True preserves sort order
            
            # Log top 10 for visibility
            context.log.info("📊 Top 10 candidates by net benefit:")
            for i, cand in enumerate(batch[:10], 1):
                summary = cand['totals']['summary']
                context.log.info(
                    f"  {i}. {cand['candidate_name']} ({cand['party']}): "
                    f"${summary['net_benefit']:,.0f} net benefit "
                    f"(${summary['total_pac_contributions']:,.0f} PAC + "
                    f"${summary['net_independent_expenditures']:,.0f} net indie)"
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
