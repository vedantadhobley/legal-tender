"""
Enriched Committee Funding Sources Asset (Per-Cycle)
Traces money UPSTREAM - who funds each committee (Super PACs, Party Committees, etc.)

CRITICAL: pas2 contains transactions from FILER's perspective (money OUT), not money IN!
To trace upstream, we query WHERE recipient (OTHER_ID) = target committee.

Currently extracts:
  - Political Committee transfers (Super PACs, ideological PACs, party committees)
  - Corporate/Union PAC contributions (reclassified as from_organizations)
    • Q/N type committees = Corporate/Union PACs (ExxonMobil PAC, Teamsters PAC, etc.)
    • W/O/I type committees = Political PACs (Super PACs, ideological causes)
  
NOT YET AVAILABLE (need different data sources):
  - Direct individual donations (need indiv.zip parsing - see issue #XX)
  - Direct corporate contributions (illegal - corps use PACs instead)

See docs/PAS2_DATA_MODEL.md for detailed explanation of the data model.
This is the "dark money" tracer - follow the money back to its SOURCE.
"""

from dagster import asset, AssetIn, Config, AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any
from datetime import datetime
from collections import defaultdict

from src.resources.mongo import MongoDBResource


class EnrichedCommitteeFundingConfig(Config):
    """Configuration for enriched_committee_funding asset"""
    cycles: list[str] = ["2020", "2022", "2024", "2026"]
    large_donor_threshold: int = 10000  # Flag individual donations above this amount


@asset(
    name="enriched_committee_funding",
    group_name="enrichment",
    ins={
        "itpas2": AssetIn(key="itpas2"),
        "cm": AssetIn(key="cm"),  # Committee master for types
    },
    compute_kind="aggregation",
    description="Per-cycle upstream funding sources for committees (who funds them)"
)
def enriched_committee_funding_asset(
    context: AssetExecutionContext,
    config: EnrichedCommitteeFundingConfig,
    mongo: MongoDBResource,
    itpas2: Dict[str, Any],
    cm: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """
    Trace money UPSTREAM by filtering itpas2 by entity type.
    Stored in enriched_{cycle}.committee_funding_sources
    
    Structure per committee:
    {
        _id: "C00571703",  // Senate Leadership Fund
        committee_name: "SENATE LEADERSHIP FUND",
        committee_type: "O",  // Super PAC
        cycle: "2024",
        
        funding_sources: {
            from_committees: [
                {
                    committee_id: "C00123456",
                    committee_name: "Koch Industries PAC",
                    total_amount: 2000000,
                    transaction_count: 4,
                    transactions: [...]  // Top 10
                }
            ],
            from_individuals: [
                {
                    contributor_name: "MUSK, ELON",
                    employer: "TESLA INC",
                    total_amount: 75000000,
                    transaction_count: 3,
                    contributions: [...]
                }
            ],
            from_organizations: [
                {
                    organization_name: "AMAZON.COM",
                    total_amount: 500000,
                    transaction_count: 2
                }
            ]
        },
        
        transparency: {
            total_disclosed: 85000000,
            from_committees: 70000000,
            from_individuals: 14500000,
            from_organizations: 500000,
            transparency_score: 0.944,
            red_flags: [...]
        },
        
        computed_at: ISODate(...)
    }
    """
    
    stats = {
        'cycles_processed': [],
        'committees_with_funding': 0,
        'total_upstream_amount': 0,
        'by_source_type': {
            'from_committees': 0,
            'from_individuals': 0,
            'from_organizations': 0
        }
    }
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"🔍 Processing cycle {cycle} - Tracing upstream funding sources")
            
            itpas2_collection = mongo.get_collection(client, "itpas2", database_name=f"fec_{cycle}")
            cm_collection = mongo.get_collection(client, "cm", database_name=f"fec_{cycle}")
            funding_collection = mongo.get_collection(client, "committee_funding_sources", database_name=f"enriched_{cycle}")
            
            # Clear existing data
            funding_collection.delete_many({})
            context.log.info(f"Cleared existing committee_funding_sources for cycle {cycle}")
            
            # Load committee names and types for enrichment
            committee_info = {}
            for cmte in cm_collection.find():
                committee_info[cmte['CMTE_ID']] = {
                    'name': cmte.get('CMTE_NM', 'Unknown'),
                    'type': cmte.get('CMTE_TP', 'Unknown')
                }
            
            context.log.info(f"Loaded {len(committee_info):,} committee info records")
            
            # Aggregate funding sources BY RECIPIENT COMMITTEE
            # Key: committee receiving money
            # Value: {from_committees: {}, from_individuals: {}, from_organizations: {}}
            funding_by_committee: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(lambda: {
                'from_committees': {},
                'from_individuals': {},
                'from_organizations': {}
            })
            
            # === COMMITTEE → COMMITTEE TRANSFERS ===
            # IMPORTANT: pas2 contains transactions FROM the filer's perspective (money OUT)
            # CMTE_ID = committee filing report (DONOR - giving money)
            # OTHER_ID = recipient committee (RECIPIENT - receiving money)
            # We want UPSTREAM funding, so we need to find: who gave TO each committee
            # Therefore: donor_id = CMTE_ID, recipient_id = OTHER_ID
            context.log.info("📊 Extracting Committee → Committee transfers...")
            committee_transfers = itpas2_collection.find({
                "ENTITY_TP": {"$in": ["COM", "PAC", "PTY", "CCM"]},
                "TRANSACTION_TP": "24K",  # Only direct contributions, not expenditures!
                "TRANSACTION_AMT": {"$ne": None},
                "OTHER_ID": {"$ne": None}  # Must have recipient committee ID
            })
            
            cmte_count = 0
            for txn in committee_transfers:
                donor_id = txn.get('CMTE_ID')      # Committee GIVING money
                recipient_id = txn.get('OTHER_ID')  # Committee RECEIVING money
                amount = txn.get('TRANSACTION_AMT', 0)
                
                if not recipient_id or not donor_id or amount <= 0:
                    continue
                
                # Initialize if first time seeing this donor
                if donor_id not in funding_by_committee[recipient_id]['from_committees']:
                    funding_by_committee[recipient_id]['from_committees'][donor_id] = {
                        'transactions': [],
                        'total': 0,
                        'count': 0
                    }
                
                funding_by_committee[recipient_id]['from_committees'][donor_id]['transactions'].append({
                    'amount': amount,
                    'date': txn.get('TRANSACTION_DT'),
                    'transaction_id': txn.get('TRAN_ID')
                })
                funding_by_committee[recipient_id]['from_committees'][donor_id]['total'] += amount
                funding_by_committee[recipient_id]['from_committees'][donor_id]['count'] += 1
                cmte_count += 1
            
            context.log.info(f"   ✅ Found {cmte_count:,} committee→committee transfers")
            
            # === INDIVIDUAL → COMMITTEE DONATIONS (Large donors only) ===
            # ENTITY_TP="IND" in pas2 means: PAC attributing contributions to individuals
            # These are PAC→Candidate transfers attributed to individual members
            # CMTE_ID = PAC making contribution (still the filer/donor)
            # OTHER_ID = Candidate receiving contribution (recipient)
            # NAME = Individual the PAC attributes the money to
            # This is NOT direct individual→committee donations!
            # For UPSTREAM tracing, we need individual donations TO committees
            # So we skip pas2 ENTITY_TP="IND" entirely - those are PAC pass-throughs
            context.log.info("📊 Skipping ENTITY_TP='IND' in pas2 - these are PAC attributed contributions, not direct individual donations")
            # TODO: Use itcont collection for true individual→committee donations
            ind_count = 0
            
            context.log.info(f"   ⚠️  Skipped individual donations (not available in pas2) - {ind_count:,} records")
            
            # === ORGANIZATION → COMMITTEE CONTRIBUTIONS ===
            # ENTITY_TP="ORG" in pas2 with TRANSACTION_TP="24K" means:
            # CMTE_ID = Committee making contribution (DONOR/filer)
            # NAME = Organization name OR recipient name (if OTHER_ID exists)
            # OTHER_ID = Recipient committee ID (when contributing TO another committee)
            # 
            # When OTHER_ID exists: This is NOT an org contribution, it's a committee→committee
            # transfer where the committee is making an earmarked/attributed contribution
            # (e.g., "IAO PROPERTY HOLDINGS LLC" via OTHER_ID to candidate)
            #
            # When OTHER_ID is null/empty: This might be a true org contribution, BUT
            # pas2 is still from filer's perspective, so this would be org→committee
            # reported BY the committee making a PAYMENT TO the org (backwards!)
            #
            # For UPSTREAM tracing of org contributions TO committees, we need to query
            # WHERE the committee is the RECIPIENT (OTHER_ID = target_committee)
            context.log.info("📊 Extracting Organization → Committee contributions...")
            org_contributions = itpas2_collection.find({
                "ENTITY_TP": "ORG",
                "TRANSACTION_TP": "24K",
                "TRANSACTION_AMT": {"$ne": None},
                "OTHER_ID": {"$nin": [None, ""]}  # Only transactions with recipient committee
            })
            
            earmark_count = 0  # Track earmarked contributions
            
            for txn in org_contributions:
                donor_id = txn.get('CMTE_ID')  # Committee making the contribution
                recipient_id = txn.get('OTHER_ID')  # Committee receiving the contribution
                org_name = txn.get('NAME', 'Unknown')
                amount = txn.get('TRANSACTION_AMT', 0)
                
                if not recipient_id or not donor_id or amount <= 0:
                    continue
                
                # These are committee→committee transfers with org attribution
                # Add to recipient's from_committees
                if donor_id not in funding_by_committee[recipient_id]['from_committees']:
                    donor_info = committee_info.get(donor_id, {'name': 'Unknown', 'type': 'Unknown'})
                    funding_by_committee[recipient_id]['from_committees'][donor_id] = {
                        'transactions': [],
                        'total': 0,
                        'count': 0
                    }
                
                funding_by_committee[recipient_id]['from_committees'][donor_id]['transactions'].append({
                    'amount': amount,
                    'date': txn.get('TRANSACTION_DT'),
                    'transaction_id': txn.get('TRAN_ID'),
                    'memo': f"Attributed to: {org_name}"
                })
                funding_by_committee[recipient_id]['from_committees'][donor_id]['total'] += amount
                funding_by_committee[recipient_id]['from_committees'][donor_id]['count'] += 1
                earmark_count += 1
            
            context.log.info(f"   ✅ Found {earmark_count:,} org-attributed committee contributions")
            context.log.info("   ⚠️  Note: True org→committee contributions not available in pas2 (would need different data source)")
            
            # === BUILD COMMITTEE FUNDING RECORDS ===
            context.log.info("🏗️  Building committee funding records...")
            
            # RECLASSIFY: Corporate/Union PACs (Q/N types) should go in from_organizations
            # Q = Qualified non-party PAC (connected to corp/union/trade assoc)
            # N = Non-qualified PAC (smaller connected PACs)
            # These represent organizational interests, not political causes
            # 
            # Political PACs (W/O/I/etc.) stay in from_committees:
            # W = Non-connected PAC (ideological causes like Conservative Leadership PAC)
            # O = Super PAC (independent expenditure only)
            # I = Independent expenditure PAC
            corporate_pac_types = {'Q', 'N'}  # Qualified/Non-qualified connected PACs
            
            batch = []
            reclassified_count = 0
            political_pac_count = 0
            
            for recipient_id, sources in funding_by_committee.items():
                # Get committee info
                cmte_info = committee_info.get(recipient_id, {'name': 'Unknown', 'type': 'Unknown'})
                
                # Build from_committees and from_organizations lists
                # Reclassify based on donor committee type
                from_committees = []
                total_from_committees = 0
                
                for donor_id, data in sources['from_committees'].items():
                    donor_info = committee_info.get(donor_id, {'name': 'Unknown', 'type': 'Unknown'})
                    donor_type = donor_info.get('type', 'Unknown')
                    
                    # Sort transactions by amount (descending), keep top 10
                    top_transactions = sorted(
                        data['transactions'],
                        key=lambda x: x['amount'],
                        reverse=True
                    )[:10]
                    
                    # Reclassify corporate/union PACs as organizations
                    if donor_type in corporate_pac_types:
                        # Add to from_organizations instead
                        org_key = donor_info['name']
                        if org_key not in sources['from_organizations']:
                            sources['from_organizations'][org_key] = {
                                'contributions': [],
                                'total': 0,
                                'count': 0,
                                'committee_id': donor_id,
                                'committee_type': donor_type
                            }
                        
                        # Transfer transactions to organizations
                        sources['from_organizations'][org_key]['contributions'].extend(
                            [{'amount': t['amount'], 'date': t.get('date'), 'transaction_id': t.get('transaction_id')} 
                             for t in data['transactions']]
                        )
                        sources['from_organizations'][org_key]['total'] += data['total']
                        sources['from_organizations'][org_key]['count'] += data['count']
                        reclassified_count += 1
                    else:
                        # Keep as political committee
                        from_committees.append({
                            'committee_id': donor_id,
                            'committee_name': donor_info['name'],
                            'committee_type': donor_type,
                            'total_amount': data['total'],
                            'transaction_count': data['count'],
                            'transactions': top_transactions
                        })
                        total_from_committees += data['total']
                        political_pac_count += 1
                
                # Sort committees by total amount (descending)
                from_committees.sort(key=lambda x: x['total_amount'], reverse=True)
                
                # Build from_individuals list
                from_individuals = []
                total_from_individuals = 0
                for donor_key, data in sources['from_individuals'].items():
                    name, employer = donor_key.split('|')
                    
                    # Sort contributions by amount (descending), keep top 10
                    top_contributions = sorted(
                        data['contributions'],
                        key=lambda x: x['amount'],
                        reverse=True
                    )[:10]
                    
                    from_individuals.append({
                        'contributor_name': name,
                        'employer': employer,
                        'occupation': top_contributions[0].get('occupation') if top_contributions else None,
                        'total_amount': data['total'],
                        'contribution_count': data['count'],
                        'contributions': top_contributions
                    })
                    total_from_individuals += data['total']
                
                # Sort individuals by total amount (descending)
                from_individuals.sort(key=lambda x: x['total_amount'], reverse=True)
                
                # Build from_organizations list
                # Now includes reclassified corporate/union PACs (Q/N types)
                from_organizations = []
                total_from_organizations = 0
                for org_name, data in sources['from_organizations'].items():
                    # Sort contributions by amount (descending), keep top 10
                    top_contributions = sorted(
                        data['contributions'],
                        key=lambda x: x['amount'],
                        reverse=True
                    )[:10]
                    
                    org_record = {
                        'organization_name': org_name,
                        'total_amount': data['total'],
                        'contribution_count': data['count'],
                        'contributions': top_contributions
                    }
                    
                    # Include committee_id/type if this is a reclassified PAC
                    if 'committee_id' in data:
                        org_record['pac_committee_id'] = data['committee_id']
                        org_record['pac_committee_type'] = data['committee_type']
                        org_record['organization_type'] = 'Corporate PAC' if data['committee_type'] == 'Q' else 'Union/Trade PAC'
                    
                    from_organizations.append(org_record)
                    total_from_organizations += data['total']
                
                # Sort organizations by total amount (descending)
                from_organizations.sort(key=lambda x: x['total_amount'], reverse=True)
                
                # Calculate transparency metrics
                total_disclosed = total_from_committees + total_from_individuals + total_from_organizations
                
                # TODO: Compare against committee's reported total receipts from webk/webl
                # For now, transparency_score = 1.0 if we have any disclosed sources
                transparency_score = 1.0 if total_disclosed > 0 else 0.0
                
                # Red flags detection
                red_flags = []
                
                # Flag if receiving from Type C committees (dark money)
                dark_money_sources = [c for c in from_committees if c['committee_type'] == 'C']
                if dark_money_sources:
                    red_flags.append({
                        'type': 'dark_money_source',
                        'description': f"Receives from {len(dark_money_sources)} Type C (dark money) committees",
                        'severity': 'high',
                        'amount': sum(c['total_amount'] for c in dark_money_sources)
                    })
                
                # Flag if receiving from multiple other Super PACs (shell game)
                super_pac_sources = [c for c in from_committees if c['committee_type'] in ['O', 'U']]
                if len(super_pac_sources) >= 3:
                    red_flags.append({
                        'type': 'shell_game',
                        'description': f"Receives from {len(super_pac_sources)} other Super PACs (potential layering)",
                        'severity': 'medium',
                        'amount': sum(c['total_amount'] for c in super_pac_sources)
                    })
                
                record = {
                    '_id': recipient_id,
                    'committee_id': recipient_id,
                    'committee_name': cmte_info['name'],
                    'committee_type': cmte_info['type'],
                    'cycle': cycle,
                    
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
                context.log.info(f"✅ {cycle}: Created {len(batch):,} committee funding records")
                context.log.info(f"   📊 Reclassified {reclassified_count:,} corporate/union PACs as organizations")
                context.log.info(f"   📊 Kept {political_pac_count:,} political PACs in committees")
                stats['committees_with_funding'] += len(batch)
                stats['cycles_processed'].append(cycle)
                
                # Update source type stats
                for record in batch:
                    stats['by_source_type']['from_committees'] += record['transparency']['from_committees']
                    stats['by_source_type']['from_individuals'] += record['transparency']['from_individuals']
                    stats['by_source_type']['from_organizations'] += record['transparency']['from_organizations']
            else:
                context.log.warning(f"⚠️  {cycle}: No funding records created")
            
            # Create indexes
            funding_collection.create_index([("committee_id", 1)])
            funding_collection.create_index([("committee_type", 1)])
            funding_collection.create_index([("transparency.total_disclosed", -1)])
            funding_collection.create_index([("transparency.transparency_score", 1)])
            funding_collection.create_index([("transparency.red_flags.type", 1)])
    
    return Output(
        value=stats,
        metadata={
            "committees_with_funding": stats['committees_with_funding'],
            "total_upstream_amount": MetadataValue.float(stats['total_upstream_amount']),
            "cycles_processed": MetadataValue.json(stats['cycles_processed']),
            "by_source_type": MetadataValue.json(stats['by_source_type']),
        }
    )
