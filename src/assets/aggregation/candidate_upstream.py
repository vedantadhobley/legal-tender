"""Candidate Upstream - Trace ALL corporate influence to candidates.

This is the "FIVE PIES" asset - for any candidate, shows exactly WHO is funding them,
traced through any number of committee hops to the TERMINAL SOURCE.

TERMINAL SOURCES (where money originates):
1. CORPORATIONS - Corporate PACs (terminal_type = 'corporation')
2. TRADE ASSOCIATIONS - Industry groups (terminal_type = 'trade_association')  
3. LABOR UNIONS - Union PACs (terminal_type = 'labor_union')
4. IDEOLOGICAL - Issue-based PACs (terminal_type = 'ideological')
5. INDIVIDUALS - People (with corporate connection tracking)

For individuals, we further break down:
- Corporate-connected: Employees of known corporations (via corporate_families)
- Whale donors: $10K+ donors linked to corporations via employer or wikidata
- Independent: Everyone else

The key insight: Money flows through passthrough committees (JFCs, conduits).
We trace BACKWARDS from candidate to find the ORIGINAL source.

Output: candidates collection updated with 'funding_sources' field.
"""

from typing import Dict, Any, List, Set
from datetime import datetime
from collections import defaultdict

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


# Terminal types - these are the REAL funding sources (don't trace further)
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}

# Passthrough types - trace THROUGH these to find real sources
PASSTHROUGH_TYPES = {"passthrough", "unknown", "super_pac_unclassified"}

# Conduit patterns to filter from individual donors
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]


class CandidateUpstreamConfig(Config):
    """Configuration for candidate upstream computation."""
    top_n_sources: int = 25
    top_n_individuals: int = 50
    max_trace_depth: int = 8
    min_amount: float = 1000  # Minimum to include in top lists


@asset(
    name="candidate_upstream",
    description="Trace ALL corporate influence to candidates - the 'Five Pies' breakdown.",
    group_name="aggregation",
    compute_kind="aggregation",
    deps=["committee_receipts", "affiliated_with", "transferred_to", "spent_on", 
          "contributed_to", "wikidata_corporate_resolution"],
)
def candidate_upstream_asset(
    context: AssetExecutionContext,
    config: CandidateUpstreamConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Compute complete upstream funding attribution for all candidates.
    
    Traces money backwards through the graph to find TERMINAL sources:
    - Corporate PACs
    - Trade Association PACs
    - Labor Union PACs
    - Ideological PACs
    - Individuals (with corporate connection tracking)
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("🥧 Computing candidate funding sources (Five Pies)...")
        
        # ================================================================
        # PHASE 1: Load lookup data into memory
        # ================================================================
        context.log.info("📥 Phase 1: Loading lookup data...")
        
        # Committee info with terminal_type
        cmte_info = {}
        for c in db.aql.execute("""
            FOR c IN committees
            RETURN {
                _key: c._key,
                name: c.CMTE_NM,
                terminal_type: c.terminal_type,
                total_receipts: c.total_receipts || 0
            }
        """):
            cmte_info[c['_key']] = c
        context.log.info(f"   Loaded {len(cmte_info):,} committees")
        
        # Corporate families for employer → company mapping
        employer_to_company = {}
        if db.has_collection('employer_canonical_mapping'):
            for m in db.aql.execute("""
                FOR m IN employer_canonical_mapping
                RETURN {employer: m.employer_name, company: m.canonical_name}
            """):
                employer_to_company[m['employer']] = m['company']
        context.log.info(f"   Loaded {len(employer_to_company):,} employer mappings")
        
        # Corporate families with totals
        corporate_families = {}
        if db.has_collection('corporate_families'):
            for f in db.aql.execute("""
                FOR f IN corporate_families
                RETURN {
                    name: f.canonical_name,
                    employee_total: f.total_from_employees,
                    whale_total: f.total_from_whales,
                    wikidata_id: f.wikidata_id
                }
            """):
                corporate_families[f['name']] = f
        context.log.info(f"   Loaded {len(corporate_families):,} corporate families")
        
        # Whale corporate links (billionaires → companies)
        whale_to_company = {}
        if db.has_collection('whale_corporate_links'):
            for link in db.aql.execute("""
                FOR l IN whale_corporate_links
                RETURN {donor: l.donor_name, company: l.canonical_name}
            """):
                whale_to_company[link['donor']] = link['company']
        context.log.info(f"   Loaded {len(whale_to_company):,} whale-corporate links")
        
        # Donor info for whale lookups
        donor_info = {}
        for d in db.aql.execute("""
            FOR d IN donors
            FILTER d.total_amount >= 10000
            RETURN {
                _key: d._key,
                name: d.canonical_name,
                employer: d.canonical_employer,
                total: d.total_amount
            }
        """):
            donor_info[d['_key']] = d
        context.log.info(f"   Loaded {len(donor_info):,} whale donors ($10K+)")
        
        # Transfer edges: to_cmte -> [(from_cmte, amount), ...]
        transfer_edges = defaultdict(list)
        for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            from_cmte = e['_from'].split('/')[1]
            transfer_edges[to_cmte].append((from_cmte, e.get('total_amount', 0) or 0))
        context.log.info(f"   Loaded {sum(len(v) for v in transfer_edges.values()):,} transfer edges")
        
        # Contribution edges: cmte -> [(donor_key, amount), ...]
        contrib_edges = defaultdict(list)
        for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            donor_key = e['_from'].split('/')[1]
            contrib_edges[to_cmte].append((donor_key, e.get('total_amount', 0) or 0))
        context.log.info(f"   Loaded {sum(len(v) for v in contrib_edges.values()):,} contribution edges")
        
        # IE spending: candidate -> {support: [(cmte, amount)], oppose: [(cmte, amount)]}
        ie_by_candidate = defaultdict(lambda: {'support': [], 'oppose': []})
        for e in db.aql.execute("""
            FOR e IN spent_on
            RETURN {
                cand_id: SPLIT(e._to, '/')[1],
                cmte_id: SPLIT(e._from, '/')[1],
                amount: e.total_amount,
                support_oppose: e.support_oppose
            }
        """):
            cand_id = e['cand_id']
            cmte_id = e['cmte_id']
            amount = e['amount'] or 0
            if e['support_oppose'] == 'S':
                ie_by_candidate[cand_id]['support'].append((cmte_id, amount))
            else:
                ie_by_candidate[cand_id]['oppose'].append((cmte_id, amount))
        context.log.info(f"   Loaded IEs for {len(ie_by_candidate):,} candidates")
        
        # ================================================================
        # PHASE 2: Helper function - trace upstream from a committee
        # ================================================================
        
        def is_conduit(name: str) -> bool:
            """Check if donor name is a conduit (WinRed, ActBlue, etc.)"""
            if not name:
                return False
            name_upper = name.upper()
            return any(p in name_upper for p in CONDUIT_PATTERNS)
        
        def trace_committee_sources(
            start_cmte_ids: List[str],
            multiplier: float = 1.0
        ) -> Dict[str, Any]:
            """
            Trace backwards from committees to find terminal sources.
            
            Returns:
            {
                'corporations': {name: amount, ...},
                'trade_associations': {name: amount, ...},
                'labor_unions': {name: amount, ...},
                'ideological': {name: amount, ...},
                'cooperatives': {name: amount, ...},
                'individuals': {name: {'amount': X, 'employer': Y, 'company': Z}, ...},
            }
            """
            results = {
                'corporations': defaultdict(float),
                'trade_associations': defaultdict(float),
                'labor_unions': defaultdict(float),
                'ideological': defaultdict(float),
                'cooperatives': defaultdict(float),
                'individuals': {},  # name -> {amount, employer, company}
            }
            
            # BFS queue: (cmte_id, multiplier, depth)
            queue = [(cmte_id, multiplier, 0) for cmte_id in start_cmte_ids]
            visited_edges = set()  # (from, to) to avoid double-counting
            
            while queue:
                cmte_id, mult, depth = queue.pop(0)
                
                if depth > config.max_trace_depth:
                    continue
                if mult < 0.0001:  # < 0.01% attribution
                    continue
                
                cmte = cmte_info.get(cmte_id, {})
                total_receipts = cmte.get('total_receipts', 0) or 0
                
                # Get individual contributions to this committee
                for donor_key, amount in contrib_edges.get(cmte_id, []):
                    edge_key = ('contrib', donor_key, cmte_id)
                    if edge_key in visited_edges:
                        continue
                    visited_edges.add(edge_key)
                    
                    donor = donor_info.get(donor_key, {})
                    name = donor.get('name', donor_key)
                    
                    # Skip conduits
                    if is_conduit(name):
                        continue
                    
                    attr_amount = amount * mult
                    employer = donor.get('employer', '')
                    
                    # Resolve to company
                    company = None
                    if name in whale_to_company:
                        company = whale_to_company[name]
                    elif employer and employer in employer_to_company:
                        company = employer_to_company[employer]
                    
                    if name not in results['individuals']:
                        results['individuals'][name] = {
                            'amount': 0,
                            'employer': employer,
                            'company': company
                        }
                    results['individuals'][name]['amount'] += attr_amount
                
                # Get committee transfers to this committee
                for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
                    edge_key = ('transfer', from_cmte_id, cmte_id)
                    if edge_key in visited_edges:
                        continue
                    visited_edges.add(edge_key)
                    
                    from_cmte = cmte_info.get(from_cmte_id, {})
                    term_type = from_cmte.get('terminal_type', 'unknown')
                    attr_amount = amount * mult
                    from_name = from_cmte.get('name', from_cmte_id)
                    
                    if term_type == 'corporation':
                        results['corporations'][from_name] += attr_amount
                    elif term_type == 'trade_association':
                        results['trade_associations'][from_name] += attr_amount
                    elif term_type == 'labor_union':
                        results['labor_unions'][from_name] += attr_amount
                    elif term_type == 'ideological':
                        results['ideological'][from_name] += attr_amount
                    elif term_type == 'cooperative':
                        results['cooperatives'][from_name] += attr_amount
                    elif term_type in PASSTHROUGH_TYPES or term_type == 'campaign':
                        # Trace further upstream
                        from_receipts = from_cmte.get('total_receipts', 0) or 0
                        if from_receipts > 0:
                            new_mult = mult * (amount / from_receipts)
                            if new_mult >= 0.0001:
                                queue.append((from_cmte_id, new_mult, depth + 1))
            
            return results
        
        def trace_ie_corporate_sources(ie_data: List[tuple]) -> Dict[str, Any]:
            """
            Trace IE spending back to corporate sources.
            
            For each Super PAC that spent on the candidate, trace who funded that PAC
            and attribute proportionally.
            
            Args:
                ie_data: List of (cmte_id, ie_amount) tuples
                
            Returns:
            {
                'by_corporation': {corp_name: amount, ...},
                'by_individual': {name: {'amount': X, 'employer': Y}, ...},
                'by_pac': {pac_name: amount, ...}  # PACs that funded the Super PACs
            }
            """
            results = {
                'by_corporation': defaultdict(float),
                'by_individual': {},
                'by_pac': defaultdict(float),
            }
            
            for cmte_id, ie_amount in ie_data:
                cmte = cmte_info.get(cmte_id, {})
                total_receipts = cmte.get('total_receipts', 0) or 0
                
                if total_receipts <= 0 or ie_amount <= 0:
                    continue
                
                # What fraction of this PAC's spending went to this candidate
                multiplier = min(ie_amount / total_receipts, 1.0)
                
                # Trace donors to this PAC
                for donor_key, amount in contrib_edges.get(cmte_id, []):
                    donor = donor_info.get(donor_key, {})
                    if not donor:
                        continue
                    
                    name = donor.get('name', donor_key)
                    if is_conduit(name):
                        continue
                    
                    attr_amount = amount * multiplier
                    if attr_amount < 100:  # Skip tiny amounts
                        continue
                    
                    employer = donor.get('employer', '')
                    
                    # Check if whale with corporate link
                    company = None
                    if name in whale_to_company:
                        company = whale_to_company[name]
                    elif employer and employer in employer_to_company:
                        company = employer_to_company[employer]
                    
                    if company:
                        results['by_corporation'][company] += attr_amount
                    else:
                        if name not in results['by_individual']:
                            results['by_individual'][name] = {'amount': 0, 'employer': employer}
                        results['by_individual'][name]['amount'] += attr_amount
                
                # Trace committee transfers to this PAC
                for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
                    from_cmte = cmte_info.get(from_cmte_id, {})
                    from_name = from_cmte.get('name', from_cmte_id)
                    term_type = from_cmte.get('terminal_type', 'unknown')
                    
                    attr_amount = amount * multiplier
                    if attr_amount < 100:
                        continue
                    
                    if term_type == 'corporation':
                        # Corporate PAC funded the Super PAC
                        results['by_corporation'][from_name] += attr_amount
                    else:
                        # Other PAC funded it
                        results['by_pac'][from_name] += attr_amount
            
            return results

        # ================================================================
        # PHASE 3: Process each candidate
        # ================================================================
        context.log.info("👤 Phase 3: Processing candidates...")
        
        candidates = list(db.aql.execute("""
            FOR c IN candidates
                LET affiliated_cmtes = (
                    FOR v, e IN INBOUND c affiliated_with
                    RETURN v._key
                )
                FILTER LENGTH(affiliated_cmtes) > 0
                RETURN {
                    _key: c._key,
                    name: c.CAND_NAME,
                    party: c.CAND_PTY_AFFILIATION,
                    office: c.CAND_OFFICE,
                    state: c.CAND_OFFICE_ST,
                    cmte_ids: affiliated_cmtes
                }
        """))
        context.log.info(f"   Found {len(candidates):,} candidates with committees")
        
        stats = {
            'candidates_processed': 0,
            'candidates_with_funding': 0,
        }
        
        batch_updates = []
        
        for cand in candidates:
            cand_key = cand['_key']
            cmte_ids = cand['cmte_ids']
            
            # Trace direct funding
            sources = trace_committee_sources(cmte_ids, multiplier=1.0)
            
            # Compute totals
            corp_total = sum(sources['corporations'].values())
            trade_total = sum(sources['trade_associations'].values())
            labor_total = sum(sources['labor_unions'].values())
            ideological_total = sum(sources['ideological'].values())
            coop_total = sum(sources['cooperatives'].values())
            
            # Process individuals
            indiv_total = sum(d['amount'] for d in sources['individuals'].values())
            
            # Split individuals by corporate connection
            corp_connected = {}
            independent = {}
            for name, data in sources['individuals'].items():
                if data['company']:
                    if data['company'] not in corp_connected:
                        corp_connected[data['company']] = {'amount': 0, 'donors': []}
                    corp_connected[data['company']]['amount'] += data['amount']
                    if data['amount'] >= config.min_amount:
                        corp_connected[data['company']]['donors'].append({
                            'name': name,
                            'amount': data['amount']
                        })
                else:
                    independent[name] = data['amount']
            
            corp_connected_total = sum(c['amount'] for c in corp_connected.values())
            independent_total = indiv_total - corp_connected_total
            
            # Get IE data
            ie_support_data = ie_by_candidate.get(cand_key, {}).get('support', [])
            ie_oppose_data = ie_by_candidate.get(cand_key, {}).get('oppose', [])
            ie_support_total = sum(amt for _, amt in ie_support_data)
            ie_oppose_total = sum(amt for _, amt in ie_oppose_data)
            
            # Trace IE funding to corporate sources
            ie_support_sources = trace_ie_corporate_sources(ie_support_data) if ie_support_data else {'by_corporation': {}, 'by_individual': {}, 'by_pac': {}}
            ie_oppose_sources = trace_ie_corporate_sources(ie_oppose_data) if ie_oppose_data else {'by_corporation': {}, 'by_individual': {}, 'by_pac': {}}
            
            # ================================================================
            # BUILD BY-ORGANIZATION VIEW (The True Five Pies)
            # Aggregate all funding methods per organization
            # ================================================================
            by_org = defaultdict(lambda: {
                'direct_pac': 0,        # Corporate PAC donations
                'direct_employees': 0,  # Employee donations
                'ie_support': 0,        # IE spending FOR the candidate
                'ie_oppose': 0,         # IE spending AGAINST the candidate
                # 'lobbying': 0,        # Future: lobbying spend
                'total': 0,
            })
            
            # 1. Corporate PAC direct donations
            for corp_name, amount in sources['corporations'].items():
                by_org[corp_name]['direct_pac'] += amount
                by_org[corp_name]['total'] += amount
            
            # 2. Trade association PAC donations (treat as their own org)
            for assoc_name, amount in sources['trade_associations'].items():
                by_org[assoc_name]['direct_pac'] += amount
                by_org[assoc_name]['total'] += amount
            
            # 3. Labor union PAC donations
            for union_name, amount in sources['labor_unions'].items():
                by_org[union_name]['direct_pac'] += amount
                by_org[union_name]['total'] += amount
            
            # 4. Employee donations (corporate-connected individuals)
            for company, data in corp_connected.items():
                by_org[company]['direct_employees'] += data['amount']
                by_org[company]['total'] += data['amount']
            
            # 5. IE Support corporate attribution
            for corp_name, amount in ie_support_sources['by_corporation'].items():
                by_org[corp_name]['ie_support'] += amount
                by_org[corp_name]['total'] += amount
            
            # 6. IE Oppose corporate attribution
            for corp_name, amount in ie_oppose_sources['by_corporation'].items():
                by_org[corp_name]['ie_oppose'] += amount
                # Note: IE oppose is NOT added to total (it's against the candidate)
            
            # Direct funding total (excludes IEs)
            direct_total = corp_total + trade_total + labor_total + ideological_total + coop_total + indiv_total
            
            # Total pro-candidate money
            total_funding = direct_total + ie_support_total
            
            if total_funding <= 0:
                stats['candidates_processed'] += 1
                continue
            
            stats['candidates_with_funding'] += 1
            
            # Build funding sources object
            def top_sources(d: Dict[str, float], n: int) -> List[Dict]:
                sorted_items = sorted(d.items(), key=lambda x: -x[1])[:n]
                return [{'name': k, 'amount': v} for k, v in sorted_items if v >= config.min_amount]
            
            def top_companies(d: Dict[str, Dict], n: int) -> List[Dict]:
                sorted_items = sorted(d.items(), key=lambda x: -x[1]['amount'])[:n]
                return [{
                    'company': k,
                    'amount': v['amount'],
                    'top_donors': sorted(v['donors'], key=lambda x: -x['amount'])[:5]
                } for k, v in sorted_items if v['amount'] >= config.min_amount]
            
            funding_sources = {
                'total_funding': total_funding,
                'direct_funding': direct_total,
                'ie_support': ie_support_total,
                'ie_oppose': ie_oppose_total,
                
                # The Five Pies
                'corporations': {
                    'total': corp_total,
                    'pct': (corp_total / total_funding * 100) if total_funding > 0 else 0,
                    'top': top_sources(sources['corporations'], config.top_n_sources),
                },
                'trade_associations': {
                    'total': trade_total,
                    'pct': (trade_total / total_funding * 100) if total_funding > 0 else 0,
                    'top': top_sources(sources['trade_associations'], config.top_n_sources),
                },
                'labor_unions': {
                    'total': labor_total,
                    'pct': (labor_total / total_funding * 100) if total_funding > 0 else 0,
                    'top': top_sources(sources['labor_unions'], config.top_n_sources),
                },
                'ideological': {
                    'total': ideological_total,
                    'pct': (ideological_total / total_funding * 100) if total_funding > 0 else 0,
                    'top': top_sources(sources['ideological'], config.top_n_sources),
                },
                
                # Individuals with corporate breakdown
                'individuals': {
                    'total': indiv_total,
                    'pct': (indiv_total / total_funding * 100) if total_funding > 0 else 0,
                    'corporate_connected': {
                        'total': corp_connected_total,
                        'pct': (corp_connected_total / total_funding * 100) if total_funding > 0 else 0,
                        'by_company': top_companies(corp_connected, config.top_n_sources),
                    },
                    'independent': {
                        'total': independent_total,
                        'pct': (independent_total / total_funding * 100) if total_funding > 0 else 0,
                        'top': top_sources(independent, config.top_n_individuals),
                    },
                },
                
                # IE breakdown with corporate attribution
                'ie': {
                    'support': {
                        'total': ie_support_total,
                        'top_pacs': [
                            {'name': cmte_info.get(c, {}).get('name', c), 'amount': amt}
                            for c, amt in sorted(ie_support_data, key=lambda x: -x[1])[:10]
                            if amt >= config.min_amount
                        ],
                        # WHO funded those PACs (traced to corporations)
                        'by_corporation': top_sources(ie_support_sources['by_corporation'], config.top_n_sources),
                        'by_pac': top_sources(ie_support_sources['by_pac'], 10),
                    },
                    'oppose': {
                        'total': ie_oppose_total,
                        'top_pacs': [
                            {'name': cmte_info.get(c, {}).get('name', c), 'amount': amt}
                            for c, amt in sorted(ie_oppose_data, key=lambda x: -x[1])[:10]
                            if amt >= config.min_amount
                        ],
                        # WHO funded those PACs (traced to corporations)
                        'by_corporation': top_sources(ie_oppose_sources['by_corporation'], config.top_n_sources),
                        'by_pac': top_sources(ie_oppose_sources['by_pac'], 10),
                    },
                },
                
                # ========================================================
                # BY ORGANIZATION - The True Five Pies
                # Each organization with breakdown by funding method
                # ========================================================
                'by_organization': sorted(
                    [
                        {
                            'name': org_name,
                            'direct_pac': data['direct_pac'],
                            'direct_employees': data['direct_employees'],
                            'ie_support': data['ie_support'],
                            'ie_oppose': data['ie_oppose'],
                            'total_pro': data['total'],  # Total supporting the candidate
                            'total_against': data['ie_oppose'],
                        }
                        for org_name, data in by_org.items()
                        if data['total'] >= config.min_amount or data['ie_oppose'] >= config.min_amount
                    ],
                    key=lambda x: -x['total_pro'],
                )[:50],  # Top 50 organizations by total pro-candidate funding
                
                'computed_at': datetime.now().isoformat(),
            }
            
            batch_updates.append({
                '_key': cand_key,
                'funding_sources': funding_sources,
            })
            
            stats['candidates_processed'] += 1
            
            if len(batch_updates) >= 500:
                db.aql.execute("""
                    FOR doc IN @batch
                        UPDATE doc._key WITH { funding_sources: doc.funding_sources } IN candidates
                """, bind_vars={"batch": batch_updates})
                context.log.info(f"   Updated {stats['candidates_processed']:,} candidates...")
                batch_updates = []
        
        # Final batch
        if batch_updates:
            db.aql.execute("""
                FOR doc IN @batch
                    UPDATE doc._key WITH { funding_sources: doc.funding_sources } IN candidates
            """, bind_vars={"batch": batch_updates})
        
        # ================================================================
        # PHASE 4: Validation
        # ================================================================
        context.log.info("✅ Phase 4: Validation...")
        
        # Check Ted Cruz
        cruz = list(db.aql.execute("""
            FOR c IN candidates
                FILTER c.CAND_OFFICE == 'S' AND CONTAINS(c.CAND_NAME, 'CRUZ') AND CONTAINS(c.CAND_NAME, 'TED')
                LIMIT 1
                RETURN {
                    name: c.CAND_NAME,
                    fs: c.funding_sources
                }
        """))
        
        if cruz and cruz[0].get('fs'):
            fs = cruz[0]['fs']
            context.log.info(f"\n🎯 Validation - {cruz[0]['name']}:")
            context.log.info(f"   Total funding: ${fs['total_funding']:,.0f}")
            context.log.info(f"\n   FIVE PIES:")
            context.log.info(f"   - Corporations: ${fs['corporations']['total']:,.0f} ({fs['corporations']['pct']:.1f}%)")
            context.log.info(f"   - Trade Assocs: ${fs['trade_associations']['total']:,.0f} ({fs['trade_associations']['pct']:.1f}%)")
            context.log.info(f"   - Labor Unions: ${fs['labor_unions']['total']:,.0f} ({fs['labor_unions']['pct']:.1f}%)")
            context.log.info(f"   - Ideological:  ${fs['ideological']['total']:,.0f} ({fs['ideological']['pct']:.1f}%)")
            context.log.info(f"   - Individuals:  ${fs['individuals']['total']:,.0f} ({fs['individuals']['pct']:.1f}%)")
            context.log.info(f"     - Corp-connected: ${fs['individuals']['corporate_connected']['total']:,.0f}")
            context.log.info(f"     - Independent:    ${fs['individuals']['independent']['total']:,.0f}")
            context.log.info(f"   - IE Support:   ${fs['ie']['support']['total']:,.0f}")
            context.log.info(f"   - IE Oppose:    ${fs['ie']['oppose']['total']:,.0f}")
            
            if fs['corporations']['top']:
                context.log.info(f"\n   Top Corporate PACs:")
                for c in fs['corporations']['top'][:5]:
                    context.log.info(f"     - {c['name']}: ${c['amount']:,.0f}")
        
        context.log.info(f"\n📊 Summary:")
        context.log.info(f"   Candidates processed: {stats['candidates_processed']:,}")
        context.log.info(f"   Candidates with funding: {stats['candidates_with_funding']:,}")
        
        return Output(
            value=stats,
            metadata={
                "candidates_processed": MetadataValue.int(stats['candidates_processed']),
                "candidates_with_funding": MetadataValue.int(stats['candidates_with_funding']),
            }
        )
