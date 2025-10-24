"""
Enriched Donor Financials Asset (Per-Cycle)
Aggregates ALL money flows FROM each committee/organization within a single cycle.
Keeps spending categories and recipients separate with detailed breakdowns.
Stored in enriched_{cycle}.donor_financials
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext
from typing import Dict, Any
from datetime import datetime

from src.resources.mongo import MongoDBResource


class EnrichedDonorFinancialsConfig(Config):
    """Configuration for enriched_donor_financials asset"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="enriched_donor_financials",
    group_name="enrichment",
    ins={
        "enriched_itpas2": AssetIn(key="enriched_itpas2"),
        "enriched_oppexp": AssetIn(key="enriched_oppexp"),
    },
    compute_kind="aggregation",
    description="Per-cycle aggregation of all money flows from each committee with recipient and category separation"
)
def enriched_donor_financials_asset(
    context: AssetExecutionContext,
    config: EnrichedDonorFinancialsConfig,
    mongo: MongoDBResource,
    enriched_itpas2: Dict[str, Any],
    enriched_oppexp: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Aggregate ALL money flows FROM each committee within a single cycle.
    Stored in enriched_{cycle}.donor_financials
    
    Structure per committee (within each cycle):
    {
        committee_id: "C00...",
        committee_name: "AIPAC",
        committee_type: "Q",
        connected_org: "American Israel Public Affairs Committee",
        party: "Non-partisan",
        
        // Independent Expenditures (Super PAC spending FOR/AGAINST candidates)
        independent_expenditures: {
            support: {
                total_amount: 5000000,
                transaction_count: 234,
                candidates: [  // ALL candidates sorted by amount (descending)
                    {
                        candidate_id: "H8TX21091",
                        candidate_name: "CRUZ, TED",
                        bioguide_id: "C001118",
                        party: "REP",
                        state: "TX",
                        office: "S",
                        total_amount: 500000,
                        transaction_count: 15,
                        transactions: [...]  // largest 10 transactions only
                    }
                ]
            },
            oppose: {
                total_amount: 800000,
                transaction_count: 45,
                candidates: [...]  // ALL candidates sorted by amount
            }
        },
        
        // PAC Contributions (direct donations to candidate committees)
        pac_contributions: {
            total_amount: 1200000,
            transaction_count: 456,
            candidates: [  // ALL candidates sorted by amount (descending)
                {
                    candidate_id: "H8TX21091",
                    candidate_name: "CRUZ, TED",
                    bioguide_id: "C001118",
                    party: "REP",
                    state: "TX",
                    office: "S",
                    total_amount: 50000,
                    transaction_count: 5,
                    transactions: [...]  // largest 10 transactions only
                }
            ]
        },
        
        // Operating Expenditures (spending on vendors/services)
        operating_expenditures: {
            total_amount: 350000,
            transaction_count: 123,
            categories: [  // ALL categories sorted by amount (descending)
                {
                    category: "Advertising",
                    total_amount: 150000,
                    transaction_count: 45
                }
            ],
            payees: [  // ALL payees sorted by amount (descending)
                {
                    payee_name: "Media Vendor Inc",
                    total_amount: 75000,
                    transaction_count: 12,
                    transactions: [...]  // largest 10 transactions only
                }
            ]
        },
        
        // Summary totals
        summary: {
            total_spent: 7350000,  // all expenditures combined
            total_to_candidates: 6200000,  // indie + pac
            total_support: 5000000,
            total_oppose: 800000,
            total_operating: 350000,
            total_transactions: 813,
            tracked_candidates_count: 45  // number of tracked candidates supported/opposed
        },
        
        cycle: "2024",
        computed_at: ISODate(...)
    }
    """
    
    stats = {
        'cycles_processed': [],
        'committees_processed': 0,
        'total_independent_expenditures': 0,
        'total_pac_contributions': 0,
        'total_operating_expenditures': 0,
    }
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"Processing cycle {cycle}")
            
            itpas2_collection = mongo.get_collection(client, "itpas2", database_name=f"enriched_{cycle}")
            oppexp_collection = mongo.get_collection(client, "oppexp", database_name=f"enriched_{cycle}")
            financials_collection = mongo.get_collection(client, "donor_financials", database_name=f"enriched_{cycle}")
            
            # Clear existing data for this cycle
            financials_collection.delete_many({})
            context.log.info(f"Cleared existing donor_financials for cycle {cycle}")
            
            # Get all unique committees from itpas2 and oppexp
            all_committees = {}
            
            # Gather from itpas2 (covers both indie expenditures and PAC contributions)
            for doc in itpas2_collection.find():
                cmte_id = doc.get('filer_committee_id')
                if cmte_id and cmte_id not in all_committees:
                    all_committees[cmte_id] = {
                        'committee_id': cmte_id,
                        'committee_name': doc.get('filer_committee_name', ''),
                        'committee_type': doc.get('filer_committee_type', ''),
                        'connected_org': doc.get('filer_connected_org', ''),
                        'party': doc.get('filer_party', ''),
                    }
                elif cmte_id:
                    # Update with more complete info if available
                    if not all_committees[cmte_id]['committee_type']:
                        all_committees[cmte_id]['committee_type'] = doc.get('filer_committee_type', '')
                    if not all_committees[cmte_id]['connected_org']:
                        all_committees[cmte_id]['connected_org'] = doc.get('filer_connected_org', '')
                    if not all_committees[cmte_id]['party']:
                        all_committees[cmte_id]['party'] = doc.get('filer_party', '')
            
            # Gather from oppexp
            for doc in oppexp_collection.find():
                cmte_id = doc.get('committee_id')
                if cmte_id and cmte_id not in all_committees:
                    all_committees[cmte_id] = {
                        'committee_id': cmte_id,
                        'committee_name': doc.get('committee_name', ''),
                        'committee_type': '',
                        'connected_org': '',
                        'party': '',
                    }
            
            context.log.info(f"Found {len(all_committees)} unique committees in cycle {cycle}")
            
            # Process each committee
            batch = []
            for cmte_id, cmte_info in all_committees.items():
            
                # === INDEPENDENT EXPENDITURES ===
                # Using itpas2 data instead of independent_expenditure collection
                # because independent_expenditure.csv contains billions in fake/troll filings
                # Transaction types (per official FEC documentation):
                # 24E = Independent expenditure advocating election (58,432 txns, $1.9B) → support
                # 24A = Independent expenditure opposing election (19,229 txns, $2.5B) → oppose
                # 24F = Communication cost for candidate (708 txns, $7M) → support (minor)
                # 24N = Communication cost against candidate (91 txns, $57K) → oppose (negligible)
                indie_support_candidates = {}
                indie_oppose_candidates = {}
                
                for doc in itpas2_collection.find({
                    "filer_committee_id": cmte_id,
                    "transaction_type": {"$in": ["24A", "24E", "24F", "24N"]}
                }):
                    cand_id = doc.get('candidate_id')
                    amount = doc.get('amount', 0)
                    transaction_type = doc.get('transaction_type', '')
                    
                    # Map transaction types to support/oppose
                    # 24E/24F = support, 24A/24N = oppose
                    is_support = transaction_type in ['24E', '24F']
                    
                    # Choose support or oppose bucket
                    candidates_dict = indie_support_candidates if is_support else indie_oppose_candidates
                    
                    if cand_id not in candidates_dict:
                        candidates_dict[cand_id] = {
                            'candidate_id': cand_id,
                            'candidate_name': doc.get('candidate_name', ''),
                            'bioguide_id': doc.get('candidate_bioguide_id', ''),
                            'party': doc.get('candidate_party', ''),
                            'state': doc.get('candidate_state', ''),
                            'office': doc.get('candidate_office', ''),
                            'total_amount': 0,
                            'transaction_count': 0,
                            'transactions': []
                        }
                    
                    candidates_dict[cand_id]['total_amount'] += amount
                    candidates_dict[cand_id]['transaction_count'] += 1
                    candidates_dict[cand_id]['transactions'].append({
                        'amount': amount,
                        'date': doc.get('transaction_date', ''),
                        'description': doc.get('memo_text', ''),
                        'payee': '',  # itpas2 doesn't have payee info for indie expenditures
                        'transaction_id': doc.get('transaction_id', ''),
                        'transaction_type': transaction_type,  # Include type for reference
                    })
                
                # Sort and limit transactions to top 10 per candidate
                for cand_dict in [indie_support_candidates, indie_oppose_candidates]:
                    for cand in cand_dict.values():
                        cand['transactions'] = sorted(
                            cand['transactions'],
                            key=lambda x: x['amount'],
                            reverse=True
                        )[:10]
                
                indie_support_list = sorted(
                    indie_support_candidates.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )
                indie_oppose_list = sorted(
                    indie_oppose_candidates.values(),
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
                pac_candidates = {}

                for doc in itpas2_collection.find({"filer_committee_id": cmte_id}):
                    cand_id = doc.get('candidate_id')
                    amount = doc.get('amount', 0)
                    transaction_type = doc.get('transaction_type', '')
                    
                    # Only include direct contribution types
                    if transaction_type not in ['24C', '24F', '24K', '24Z']:
                        continue
                    
                    if cand_id not in pac_candidates:
                        pac_candidates[cand_id] = {
                            'candidate_id': cand_id,
                            'candidate_name': doc.get('candidate_name', ''),
                            'bioguide_id': doc.get('candidate_bioguide_id', ''),
                            'party': doc.get('candidate_party', ''),
                            'state': doc.get('candidate_state', ''),
                            'office': doc.get('candidate_office', ''),
                            'total_amount': 0,
                            'transaction_count': 0,
                            'transactions': []
                        }
                    
                    # Add the amount as-is (signs are already correct)
                    pac_candidates[cand_id]['total_amount'] += amount
                    pac_candidates[cand_id]['transaction_count'] += 1
                    pac_candidates[cand_id]['transactions'].append({
                        'amount': amount,
                        'date': doc.get('transaction_date', ''),
                        'transaction_type': transaction_type,
                        'transaction_id': doc.get('transaction_id', ''),
                    })
                
                # Sort and limit (sort by absolute value for display purposes)
                for cand in pac_candidates.values():
                    cand['transactions'] = sorted(
                        cand['transactions'],
                        key=lambda x: abs(x['amount']),
                        reverse=True
                    )[:10]
                
                pac_list = sorted(
                    pac_candidates.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )
                
                pac_total = sum(c['total_amount'] for c in pac_list)
                pac_count = sum(c['transaction_count'] for c in pac_list)
                
                # === OPERATING EXPENDITURES ===
                oppexp_categories = {}
                oppexp_payees = {}
                
                for doc in oppexp_collection.find({"committee_id": cmte_id}):
                    amount = doc.get('amount', 0)
                    category = doc.get('category_desc', '') or doc.get('category', '') or 'Uncategorized'
                    payee = doc.get('payee_name', '') or 'Unknown'
                    
                    # Track by category
                    if category not in oppexp_categories:
                        oppexp_categories[category] = {
                            'category': category,
                            'total_amount': 0,
                            'transaction_count': 0
                        }
                    oppexp_categories[category]['total_amount'] += amount
                    oppexp_categories[category]['transaction_count'] += 1
                    
                    # Track by payee
                    if payee not in oppexp_payees:
                        oppexp_payees[payee] = {
                            'payee_name': payee,
                            'total_amount': 0,
                            'transaction_count': 0,
                            'transactions': []
                        }
                    oppexp_payees[payee]['total_amount'] += amount
                    oppexp_payees[payee]['transaction_count'] += 1
                    oppexp_payees[payee]['transactions'].append({
                        'amount': amount,
                        'date': doc.get('transaction_date', ''),
                        'purpose': doc.get('purpose', ''),
                        'transaction_id': doc.get('transaction_id', ''),
                    })
                
                # Sort and limit
                for payee in oppexp_payees.values():
                    payee['transactions'] = sorted(
                        payee['transactions'],
                        key=lambda x: x['amount'],
                        reverse=True
                    )[:10]
                
                oppexp_categories_list = sorted(
                    oppexp_categories.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )  # All categories sorted
                
                oppexp_payees_list = sorted(
                    oppexp_payees.values(),
                    key=lambda x: x['total_amount'],
                    reverse=True
                )  # All payees sorted
                
                oppexp_total = sum(c['total_amount'] for c in oppexp_categories.values())
                oppexp_count = sum(c['transaction_count'] for c in oppexp_categories.values())
                
                # Build final record
                tracked_candidates = set(
                    list(indie_support_candidates.keys()) + 
                    list(indie_oppose_candidates.keys()) + 
                    list(pac_candidates.keys())
                )
                
                financial_record = {
                    **cmte_info,
                    
                    'independent_expenditures': {
                        'support': {
                            'total_amount': indie_support_total,
                            'transaction_count': indie_support_count,
                            'candidates': indie_support_list
                        },
                        'oppose': {
                            'total_amount': indie_oppose_total,
                            'transaction_count': indie_oppose_count,
                            'candidates': indie_oppose_list
                        }
                    },
                    
                    'pac_contributions': {
                        'total_amount': pac_total,
                        'transaction_count': pac_count,
                        'candidates': pac_list
                    },
                    
                    'operating_expenditures': {
                        'total_amount': oppexp_total,
                        'transaction_count': oppexp_count,
                        'categories': oppexp_categories_list,  # All categories, sorted by amount
                        'payees': oppexp_payees_list  # All payees, sorted by amount
                    },
                    
                    'summary': {
                        # Raw totals by category
                        'total_independent_support': indie_support_total,
                        'total_independent_oppose': indie_oppose_total,
                        'total_pac_contributions': pac_total,
                        'total_operating': oppexp_total,
                        
                        # Calculated fields
                        'net_independent_expenditures': indie_support_total - indie_oppose_total,
                        'total_to_candidates': indie_support_total + indie_oppose_total + pac_total,
                        'total_spent': indie_support_total + indie_oppose_total + pac_total + oppexp_total,
                        
                        # Counts
                        'total_transactions': indie_support_count + indie_oppose_count + pac_count + oppexp_count,
                        'tracked_candidates_count': len(tracked_candidates)
                    },
                    
                    'cycle': cycle,
                    'computed_at': datetime.now()
                }
                
                batch.append(financial_record)
                stats['total_independent_expenditures'] += indie_support_count + indie_oppose_count
                stats['total_pac_contributions'] += pac_count
                stats['total_operating_expenditures'] += oppexp_count
            
            # Sort by total_to_candidates before inserting (highest to lowest)
            context.log.info(f"Sorting {len(batch)} committees by total_to_candidates...")
            batch.sort(key=lambda x: x['summary']['total_to_candidates'], reverse=True)
            
            # Insert in sorted order
            if batch:
                financials_collection.insert_many(batch, ordered=True)
                
                # Log top 5 for this cycle
                context.log.info(f"📊 Top 5 committees in {cycle} by total to candidates:")
                for i, cmte in enumerate(batch[:5], 1):
                    indie_net = cmte['summary']['net_independent_expenditures']
                    pac = cmte['summary']['total_pac_contributions']
                    context.log.info(
                        f"  {i}. {cmte['committee_name']}: "
                        f"${cmte['summary']['total_to_candidates']:,.0f} "
                        f"(${pac:,.0f} PAC + ${indie_net:,.0f} net indie)"
                    )
            
            stats['committees_processed'] += len(all_committees)
            stats['cycles_processed'].append(cycle)
            
            context.log.info(f"Cycle {cycle} complete: {len(all_committees)} committees processed")
    
    context.log.info(f"Enriched donor financials aggregation complete: {stats}")
    return stats
