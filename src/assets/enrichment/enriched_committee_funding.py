"""
Enriched Committee Funding Sources Asset (Per-Cycle)
Traces money UPSTREAM - who funds committees that donate to tracked candidates.

CANDIDATE-CENTRIC APPROACH:
1. Get tracked candidates' committee IDs from member_fec_mapping
2. Find all committees that donated TO tracked candidate committees (pas2.OTHER_ID)
3. Separate donors by type:
   - Q/N types (Corporate PACs) → Reclassify as organizations (no upstream tracing)
   - O/W/I types (Political PACs) → Trace upstream funding sources
4. Only create records for committees relevant to tracked candidates

CRITICAL: pas2 contains transactions from FILER's perspective (money OUT), not money IN!
To trace upstream, we query WHERE recipient (OTHER_ID) = target committee.

Currently extracts:
  - Political Committee transfers (Super PACs, ideological PACs, party committees)
  - Corporate/Union PAC contributions (reclassified as from_organizations)
    • Q/N type committees = Corporate/Union PACs (no upstream tracing needed)
    • W/O/I type committees = Political PACs (trace upstream sources)
  - Individual mega-donations ≥$10K (from indiv/indiv.zip) ✅ NOW AVAILABLE!
    • Critical for Super PAC transparency (44.9% had ZERO visible funding)
    • Enables billionaire → Super PAC → Candidate influence tracking
  
LIMITATION: Q-type is overly broad
  - Q includes: Corporate PACs, Leadership PACs, AND Ideological PACs
  - Examples: Boeing PAC (corporate), Club for Growth (ideological), BRIDGE PAC (leadership)
  - Cannot distinguish without parsing ccl.zip (candidate-committee linkage)
  - For now, all Q/N types treated as "organizations" (end of money trail)
  
NOT YET AVAILABLE (need different data sources):
  - Small individual donations <$10K (excluded for performance)
  - Direct corporate contributions (illegal - corps use PACs instead)

See docs/PAS2_DATA_MODEL.md for detailed explanation of the data model.
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any
from datetime import datetime

from src.resources.mongo import MongoDBResource


class EnrichedCommitteeFundingConfig(Config):
    """Configuration for enriched_committee_funding asset"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]


@asset(
    name="enriched_committee_funding",
    group_name="enrichment",
    ins={
        "enriched_pas2": AssetIn(key="enriched_pas2"),
        "oth": AssetIn(key="oth"),
        "indiv": AssetIn(key="indiv"),
        "cm": AssetIn(key="cm"),
        "member_fec_mapping": AssetIn(key="member_fec_mapping"),
    },
    compute_kind="aggregation",
    description="Per-cycle upstream funding sources for committees that donate to tracked candidates"
)
def enriched_committee_funding_asset(
    context: AssetExecutionContext,
    config: EnrichedCommitteeFundingConfig,
    mongo: MongoDBResource,
    enriched_pas2: Dict[str, Any],
    oth: Dict[str, Any],
    indiv: Dict[str, Any],
    cm: Dict[str, Any],
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Trace money UPSTREAM for committees that donate to tracked candidates.
    """
    
    stats = {
        'cycles_processed': [],
        'committees_with_funding': 0,
        'tracked_candidate_committees': 0,
        'donor_committees_found': 0,
        'political_pacs_traced': 0,
        'corporate_pacs_skipped': 0,
        'total_upstream_amount': 0,
    }
    
    with mongo.get_client() as client:
        # ============================================================================
        # STEP 1: Get tracked candidates' committee IDs
        # ============================================================================
        context.log.info("=" * 80)
        context.log.info("STEP 1: Loading tracked candidates' committees")
        context.log.info("=" * 80)
        
        mapping_collection = mongo.get_collection(client, "member_fec_mapping", database_name="aggregation")
        
        tracked_committee_ids = set()
        
        for member in mapping_collection.find():
            committee_ids = member.get('fec', {}).get('committee_ids', [])
            if committee_ids:
                tracked_committee_ids.update(committee_ids)
        
        tracked_committee_ids = list(tracked_committee_ids)
        stats['tracked_candidate_committees'] = len(tracked_committee_ids)
        
        context.log.info(f"✅ Found {len(tracked_committee_ids):,} committee IDs for tracked candidates")
        context.log.info("")
        
        if not tracked_committee_ids:
            context.log.warning("⚠️  No tracked committees found - skipping upstream tracing")
            return Output(value=stats, metadata={"warning": "No tracked committees found"})
        
        # ============================================================================
        # STEP 2: Process each cycle
        # ============================================================================
        for cycle in config.cycles:
            context.log.info("=" * 80)
            context.log.info(f"CYCLE {cycle}: Finding and tracing donor committees")
            context.log.info("=" * 80)
            
            cm_collection = mongo.get_collection(client, "cm", database_name=f"fec_{cycle}")
            funding_collection = mongo.get_collection(client, "committee_funding_sources", database_name=f"enriched_{cycle}")
            
            # Clear existing data
            funding_collection.delete_many({})
            
            # Load committee info (from both cm and cn collections)
            committee_info = {}
            for cmte in cm_collection.find():
                committee_info[cmte['CMTE_ID']] = {
                    'name': cmte.get('CMTE_NM', 'Unknown'),
                    'type': cmte.get('CMTE_TP', 'Unknown')
                }
            
            # Also load candidate committee info (for S/H candidate IDs)
            cn_collection = mongo.get_collection(client, "cn", database_name=f"fec_{cycle}")
            for cand in cn_collection.find():
                cand_id = cand.get('CAND_ID')
                if cand_id:
                    committee_info[cand_id] = {
                        'name': cand.get('CAND_NAME', 'Unknown'),
                        'type': cand.get('CAND_OFFICE', 'Unknown')  # S=Senate, H=House, P=President
                    }
            
            context.log.info(f"Loaded {len(committee_info):,} committee/candidate records")
            context.log.info("")
            
            # ========================================================================
            # STEP 3: Find all committees influencing tracked candidates
            # ========================================================================
            # This includes:
            # 1. Direct donors (24K transactions TO candidate committees)
            # 2. Independent expenditure committees (24A/24E FOR/AGAINST candidates)
            context.log.info("Finding committees influencing tracked candidates...")
            
            # First, get all tracked candidate IDs (needed for both queries)
            tracked_candidate_ids = set()
            for mapping in mapping_collection.find():
                fec_data = mapping.get('fec', {})
                for cand_id in fec_data.get('candidate_ids', []):
                    tracked_candidate_ids.add(cand_id)
            
            context.log.info(f"Found {len(tracked_candidate_ids):,} tracked candidate IDs")
            
            donor_committees = {}
            
            # Query enriched_pas2 (already filtered to tracked candidates)
            # No need to query raw pas2 since enriched is filtered to our tracked members
            enriched_pas2_collection = mongo.get_collection(client, "pas2", database_name=f"enriched_{cycle}")
            
            # Query 1: Direct donations (24K transactions)
            query_result = enriched_pas2_collection.find({
                "entity_type": {"$in": ["COM", "PAC", "PTY", "CCM"]},
                "transaction_type": "24K",
                "amount": {"$ne": None},
            })
            
            txn_count = 0
            for txn in query_result:
                donor_id = txn.get('filer_committee_id')  # enriched schema field
                candidate_id = txn.get('candidate_id')
                amount = abs(txn.get('amount', 0))
                
                if not donor_id or not candidate_id or amount <= 0:
                    continue
                
                if donor_id not in donor_committees:
                    donor_info = committee_info.get(donor_id, {'name': 'Unknown', 'type': 'Unknown'})
                    donor_committees[donor_id] = {
                        'committee_id': donor_id,
                        'committee_name': donor_info['name'],
                        'committee_type': donor_info['type'],
                        'donated_to_tracked_committees': set(),
                        'total_to_tracked': 0,
                        'transaction_count': 0
                    }
                
                donor_committees[donor_id]['donated_to_tracked_committees'].add(candidate_id)
                donor_committees[donor_id]['total_to_tracked'] += amount
                donor_committees[donor_id]['transaction_count'] += 1
                txn_count += 1
            
            context.log.info(f"✅ Found {len(donor_committees):,} direct donor committees ({txn_count:,} transactions)")
            
            # Query 2: Independent expenditure committees (Super PACs making ads)
            context.log.info("Searching indie expenditures...")
            
            indie_query = enriched_pas2_collection.find({
                "transaction_type": {"$in": ["24A", "24E"]},  # Independent expenditures
                "amount": {"$ne": None},
                "candidate_id": {"$in": list(tracked_candidate_ids)}
            })
            
            indie_count = 0
            for txn in indie_query:
                filer_id = txn.get('filer_committee_id')  # Committee making the ad spend
                candidate_id = txn.get('candidate_id')
                amount = abs(txn.get('amount', 0))  # Use absolute value
                
                if not filer_id or not candidate_id or amount <= 0:
                    continue
                
                if filer_id not in donor_committees:
                    filer_info = committee_info.get(filer_id, {'name': 'Unknown', 'type': 'Unknown'})
                    donor_committees[filer_id] = {
                        'committee_id': filer_id,
                        'committee_name': filer_info['name'],
                        'committee_type': filer_info['type'],
                        'donated_to_tracked_committees': set(),
                        'total_to_tracked': 0,
                        'transaction_count': 0
                    }
                
                donor_committees[filer_id]['donated_to_tracked_committees'].add(candidate_id)
                donor_committees[filer_id]['total_to_tracked'] += amount
                donor_committees[filer_id]['transaction_count'] += 1
                indie_count += 1
            
            context.log.info(f"✅ Found {indie_count:,} indie expenditure transactions from {len([c for c in donor_committees.values() if c['transaction_count'] > 0]):,} Super PACs")
            
            stats['donor_committees_found'] += len(donor_committees)
            
            # ========================================================================
            # STEP 4: Separate by type
            # ========================================================================
            corporate_pac_types = {'Q', 'N'}
            
            corporate_pacs = []
            political_pacs = []
            
            for donor_id, donor_data in donor_committees.items():
                cmte_type = donor_data['committee_type']
                if cmte_type in corporate_pac_types:
                    corporate_pacs.append(donor_id)
                else:
                    political_pacs.append(donor_id)
            
            context.log.info(f"  Corporate PACs (Q/N): {len(corporate_pacs):,}")
            context.log.info(f"  Political PACs: {len(political_pacs):,}")
            stats['corporate_pacs_skipped'] += len(corporate_pacs)
            stats['political_pacs_traced'] += len(political_pacs)
            
            # ========================================================================
            # STEP 5: Trace upstream for political PACs only
            # ========================================================================
            context.log.info("Tracing upstream for political PACs...")
            
            funding_by_political_pac = {}
            
            if political_pacs:
                # Query RAW FEC oth (other receipts) to find WHO gave money TO political PACs
                # oth.CMTE_ID = donor committee (who gives the money)
                # oth.OTHER_ID = recipient committee (who receives the money)
                # oth.NAME = donor name (if individual/organization)
                raw_oth_collection = mongo.get_collection(client, "oth", database_name=f"fec_{cycle}")
                
                upstream_query = raw_oth_collection.find({
                    "ENTITY_TP": {"$in": ["COM", "PAC", "PTY", "CCM", "ORG"]},
                    "TRANSACTION_AMT": {"$ne": None},
                    "OTHER_ID": {"$in": political_pacs}  # CRITICAL: OTHER_ID in oth = recipient!
                })
                
                upstream_count = 0
                for txn in upstream_query:
                    political_pac_id = txn.get('OTHER_ID')  # Recipient (the political PAC)
                    upstream_donor_id = txn.get('CMTE_ID')  # Donor committee (if transfer)
                    # donor_name = txn.get('NAME')  # For future individual tracking (Phase 2)
                    amount = txn.get('TRANSACTION_AMT', 0)
                    
                    if not upstream_donor_id or not political_pac_id or amount <= 0:
                        continue
                    
                    if political_pac_id not in funding_by_political_pac:
                        funding_by_political_pac[political_pac_id] = {
                            'from_committees': {},
                            'from_individuals': {},
                            'from_organizations': {}
                        }
                    
                    if upstream_donor_id not in funding_by_political_pac[political_pac_id]['from_committees']:
                        upstream_info = committee_info.get(upstream_donor_id, {'name': 'Unknown', 'type': 'Unknown'})
                        funding_by_political_pac[political_pac_id]['from_committees'][upstream_donor_id] = {
                            'committee_id': upstream_donor_id,
                            'committee_name': upstream_info['name'],
                            'committee_type': upstream_info['type'],
                            'total_amount': 0,
                            'transaction_count': 0,
                            'transactions': []
                        }
                    
                    funding_by_political_pac[political_pac_id]['from_committees'][upstream_donor_id]['total_amount'] += amount
                    funding_by_political_pac[political_pac_id]['from_committees'][upstream_donor_id]['transaction_count'] += 1
                    
                    # Keep top 10 transactions
                    transactions = funding_by_political_pac[political_pac_id]['from_committees'][upstream_donor_id]['transactions']
                    txn_data = {
                        'amount': amount,
                        'date': txn.get('TRANSACTION_DT'),
                        'transaction_id': txn.get('TRAN_ID')
                    }
                    
                    if len(transactions) < 10:
                        transactions.append(txn_data)
                    else:
                        transactions.sort(key=lambda x: x['amount'], reverse=True)
                        if amount > transactions[-1]['amount']:
                            transactions[-1] = txn_data
                    
                    upstream_count += 1
                
                context.log.info(f"✅ Found {upstream_count:,} upstream transactions for {len(funding_by_political_pac):,} political PACs")
            
            # ========================================================================
            # STEP 5.5: Query individual mega-donations (indiv) for political PACs
            # ========================================================================
            context.log.info("Querying individual mega-donations...")
            
            if political_pacs:
                raw_indiv_collection = mongo.get_collection(client, "indiv", database_name=f"fec_{cycle}")
                
                individual_query = raw_indiv_collection.find({
                    "ENTITY_TP": "IND",  # Individual donors only
                    "TRANSACTION_AMT": {"$gte": 10000},  # Should be pre-filtered, but double-check
                    "CMTE_ID": {"$in": political_pacs}  # Recipient is political PAC
                })
                
                individual_count = 0
                for txn in individual_query:
                    political_pac_id = txn.get('CMTE_ID')  # Recipient PAC
                    donor_name = txn.get('NAME')
                    employer = txn.get('EMPLOYER') or 'Not Provided'
                    amount = txn.get('TRANSACTION_AMT', 0)
                    
                    if not donor_name or not political_pac_id or amount < 10000:
                        continue
                    
                    if political_pac_id not in funding_by_political_pac:
                        funding_by_political_pac[political_pac_id] = {
                            'from_committees': {},
                            'from_individuals': {},
                            'from_organizations': {}
                        }
                    
                    # Aggregate by (name, employer) to combine multiple donations from same person
                    donor_key = f"{donor_name}|{employer}"
                    
                    if donor_key not in funding_by_political_pac[political_pac_id]['from_individuals']:
                        funding_by_political_pac[political_pac_id]['from_individuals'][donor_key] = {
                            'donor_name': donor_name,
                            'employer': employer,
                            'total_amount': 0,
                            'contribution_count': 0,
                            'contributions': []
                        }
                    
                    funding_by_political_pac[political_pac_id]['from_individuals'][donor_key]['total_amount'] += amount
                    funding_by_political_pac[political_pac_id]['from_individuals'][donor_key]['contribution_count'] += 1
                    
                    # Keep top 10 contributions
                    contributions = funding_by_political_pac[political_pac_id]['from_individuals'][donor_key]['contributions']
                    txn_data = {
                        'amount': amount,
                        'date': txn.get('TRANSACTION_DT'),
                        'transaction_id': txn.get('TRAN_ID')
                    }
                    
                    if len(contributions) < 10:
                        contributions.append(txn_data)
                    else:
                        contributions.sort(key=lambda x: x['amount'], reverse=True)
                        if amount > contributions[-1]['amount']:
                            contributions[-1] = txn_data
                    
                    individual_count += 1
                
                # Count unique mega-donors
                total_mega_donors = sum(len(pac_data['from_individuals']) for pac_data in funding_by_political_pac.values())
                context.log.info(f"✅ Found {individual_count:,} mega-donation transactions from {total_mega_donors:,} unique donors")
            
            # ========================================================================
            # STEP 6: Build committee funding records
            # ========================================================================
            context.log.info("Building funding records...")
            
            batch = []
            
            # Process each donor committee
            for donor_id, donor_data in donor_committees.items():
                # Get upstream funding if it's a political PAC
                upstream = funding_by_political_pac.get(donor_id, {
                    'from_committees': {},
                    'from_individuals': {},
                    'from_organizations': {}
                })
                
                # Build from_committees list
                from_committees = []
                total_from_committees = 0
                
                # Reclassify corporate PACs as organizations
                from_organizations = []
                total_from_organizations = 0
                
                for upstream_donor_id, upstream_data in upstream['from_committees'].items():
                    upstream_type = upstream_data['committee_type']
                    
                    if upstream_type in corporate_pac_types:
                        # Reclassify as organization
                        from_organizations.append({
                            'organization_name': upstream_data['committee_name'],
                            'pac_committee_id': upstream_donor_id,
                            'pac_committee_type': upstream_type,
                            'organization_type': 'Corporate PAC' if upstream_type == 'Q' else 'Union/Trade PAC',
                            'total_amount': upstream_data['total_amount'],
                            'contribution_count': upstream_data['transaction_count'],
                            'contributions': sorted(upstream_data['transactions'], key=lambda x: x['amount'], reverse=True)[:10]
                        })
                        total_from_organizations += upstream_data['total_amount']
                    else:
                        # Keep as political committee
                        from_committees.append({
                            'committee_id': upstream_donor_id,
                            'committee_name': upstream_data['committee_name'],
                            'committee_type': upstream_type,
                            'total_amount': upstream_data['total_amount'],
                            'transaction_count': upstream_data['transaction_count'],
                            'transactions': sorted(upstream_data['transactions'], key=lambda x: x['amount'], reverse=True)[:10]
                        })
                        total_from_committees += upstream_data['total_amount']
                
                from_committees.sort(key=lambda x: x['total_amount'], reverse=True)
                from_organizations.sort(key=lambda x: x['total_amount'], reverse=True)
                
                # Build from_individuals list
                from_individuals = []
                total_from_individuals = 0
                
                for donor_key, donor_data in upstream['from_individuals'].items():
                    from_individuals.append({
                        'donor_name': donor_data['donor_name'],
                        'employer': donor_data['employer'],
                        'total_amount': donor_data['total_amount'],
                        'contribution_count': donor_data['contribution_count'],
                        'contributions': sorted(donor_data['contributions'], key=lambda x: x['amount'], reverse=True)[:10]
                    })
                    total_from_individuals += donor_data['total_amount']
                
                from_individuals.sort(key=lambda x: x['total_amount'], reverse=True)
                
                # Calculate totals
                total_disclosed = total_from_committees + total_from_organizations + total_from_individuals
                transparency_score = 1.0 if total_disclosed > 0 else 0.0
                
                # Red flags
                red_flags = []
                super_pac_sources = [c for c in from_committees if c['committee_type'] in ['O', 'U']]
                if len(super_pac_sources) >= 3:
                    red_flags.append({
                        'type': 'shell_game',
                        'description': f"Receives from {len(super_pac_sources)} other Super PACs",
                        'severity': 'medium',
                        'amount': sum(c['total_amount'] for c in super_pac_sources)
                    })
                
                record = {
                    '_id': donor_id,
                    'committee_id': donor_id,
                    'committee_name': donor_data['committee_name'],
                    'committee_type': donor_data['committee_type'],
                    'cycle': cycle,
                    'donated_to_tracked_committees': sorted(list(donor_data['donated_to_tracked_committees'])),
                    'total_to_tracked_candidates': donor_data['total_to_tracked'],
                    
                    'funding_sources': {
                        'from_committees': from_committees,
                        'from_individuals': from_individuals,
                        'from_organizations': from_organizations
                    },
                    
                    'transparency': {
                        'total_disclosed': total_disclosed,
                        'from_committees': total_from_committees,
                        'from_individuals': total_from_individuals,
                        'from_organizations': total_from_organizations,
                        'transparency_score': transparency_score,
                        'red_flags': red_flags
                    },
                    
                    'computed_at': datetime.now()
                }
                
                batch.append(record)
                stats['total_upstream_amount'] += total_disclosed
            
            # Insert batch
            if batch:
                funding_collection.insert_many(batch, ordered=False)
                context.log.info(f"✅ Created {len(batch):,} funding records for cycle {cycle}")
                stats['committees_with_funding'] += len(batch)
                stats['cycles_processed'].append(cycle)
                
                # Create indexes
                funding_collection.create_index([("committee_id", 1)])
                funding_collection.create_index([("committee_type", 1)])
                funding_collection.create_index([("donated_to_tracked_committees", 1)])
                funding_collection.create_index([("transparency.total_disclosed", -1)])
            else:
                context.log.warning(f"⚠️  No funding records for cycle {cycle}")
            
            context.log.info("")
    
    context.log.info("=" * 80)
    context.log.info("SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Tracked candidate committees: {stats['tracked_candidate_committees']:,}")
    context.log.info(f"Donor committees found: {stats['donor_committees_found']:,}")
    context.log.info(f"Corporate PACs (skipped upstream): {stats['corporate_pacs_skipped']:,}")
    context.log.info(f"Political PACs (traced upstream): {stats['political_pacs_traced']:,}")
    context.log.info(f"Total funding records created: {stats['committees_with_funding']:,}")
    context.log.info("=" * 80)
    
    return Output(
        value=stats,
        metadata={
            "tracked_candidate_committees": stats['tracked_candidate_committees'],
            "donor_committees_found": stats['donor_committees_found'],
            "political_pacs_traced": stats['political_pacs_traced'],
            "committees_with_funding": stats['committees_with_funding'],
            "total_upstream_amount": MetadataValue.float(stats['total_upstream_amount']),
            "cycles_processed": MetadataValue.json(stats['cycles_processed']),
        }
    )
