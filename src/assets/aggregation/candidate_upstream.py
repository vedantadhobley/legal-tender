"""Candidate Funding - Trace ALL money flowing to/against candidates.

This is the FUNDING CHANNELS asset. For any candidate, shows exactly how money
reaches them through distinct channels, traced through committee hops to terminal sources.

FUNDING CHANNELS (how money reaches candidates):
1. ORGANIZATIONAL DIRECT - Corporate PACs, trade assocs, labor unions, ideological PACs
   giving directly to candidate committees. All org types collapsed into one channel.
2. IE SUPPORT - Independent expenditures FOR the candidate by Super PACs, traced
   upstream to see who funded those Super PACs.
3. IE OPPOSE - Independent expenditures AGAINST the candidate (traced similarly).
4. INDIVIDUALS - People giving directly to candidate committees ($200+ itemized).
5. UNACCOUNTED - Gap between total receipts and what we can trace. Dark money,
   unitemized small donors (<$200), and other untraceable flows live here.

For organizational money, we trace BACKWARDS through passthrough committees (JFCs,
conduits, party committees) to the TERMINAL SOURCE -- the org PAC whose terminal_type
tells us what kind of organization it is (corporation, trade, labor, ideological, cooperative).

For individuals, we further track corporate connections:
- Corporate-connected: Employees of known corporations (via canonical_employers/wikidata)
- Whale donors: $10K+ donors linked to corporations via employer
- Independent: Everyone else

UNACCOUNTED captures:
- Unitemized individual contributions (<$200, not in indiv file)
- Transfers we can't trace (missing edges, data gaps)
- The gap between committee total_receipts and sum of traced inflows

OUTPUT STRUCTURE:
candidates.funding_channels = {
    by_cycle: { "2024": { channels... }, ... },
    aggregate: { channels... },
    total_funding, direct_funding, ie_support, ie_oppose,
    by_organization: [{ name, direct_pac, direct_employees, ie_support, ie_oppose, total_pro }],
    computed_at, cycles_available,
}

Each channel block:
- organizational_direct: { total, pct, by_type: { corporation: {total, top}, trade: {...}, ... } }
- ie_support: { total, pct, top_pacs, by_corporation, by_pac }
- ie_oppose: { total, pct, top_pacs, by_corporation, by_pac }
- individuals: { total, pct, corporate_connected: {...}, independent: {...} }
- unaccounted: { total, pct, explanation }

Source: aggregation graph (committees, donors, edges)
Target: candidates collection updated with 'funding_channels' field.
"""

from typing import Dict, Any, List, Set, Optional
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

# Election cycles to process
CYCLES = ["2020", "2022", "2024"]

# Map terminal_type to channel bucket name
TERMINAL_TYPE_BUCKET = {
    "corporation": "corporation",
    "trade_association": "trade_association",
    "labor_union": "labor_union",
    "ideological": "ideological",
    "cooperative": "cooperative",
}


class CandidateFundingConfig(Config):
    """Configuration for candidate funding computation."""
    top_n_sources: int = 25
    top_n_individuals: int = 50
    max_trace_depth: int = 8
    min_amount: float = 1000  # Minimum to include in top lists


@asset(
    name="candidate_funding",
    description="Trace ALL money to candidates by funding channel -- organizational direct, IE support/oppose, individuals, unaccounted.",
    group_name="aggregation",
    compute_kind="aggregation",
    deps=["committee_classification", "committee_receipts", "affiliated_with", "transferred_to", "spent_on", 
          "contributed_to", "wikidata_corporate_resolution"],
)
def candidate_funding_asset(
    context: AssetExecutionContext,
    config: CandidateFundingConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Compute complete upstream funding attribution for all candidates.
    
    Traces money backwards through the graph to find TERMINAL sources,
    then organizes results by funding channel.
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("Computing candidate funding channels...")
        
        # ================================================================
        # PHASE 1: Load lookup data into memory
        # ================================================================
        context.log.info("Phase 1: Loading lookup data...")
        
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
        
        # Corporate families for employer -> company mapping
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
        
        # Whale corporate links (billionaires -> companies)
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
        
        # Transfer edges BY CYCLE: cycle -> to_cmte -> [(from_cmte, amount), ...]
        transfer_edges_by_cycle = {cycle: defaultdict(list) for cycle in CYCLES}
        transfer_edges_all = defaultdict(list)
        for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            from_cmte = e['_from'].split('/')[1]
            amount = e.get('total_amount', 0) or 0
            cycle = e.get('cycle', '2024')
            if cycle in CYCLES:
                transfer_edges_by_cycle[cycle][to_cmte].append((from_cmte, amount))
            transfer_edges_all[to_cmte].append((from_cmte, amount))
        context.log.info(f"   Loaded transfer edges by cycle: " + 
                        ", ".join(f"{c}={sum(len(v) for v in transfer_edges_by_cycle[c].values()):,}" for c in CYCLES))
        
        # Contribution edges BY CYCLE: cycle -> cmte -> [(donor_key, amount), ...]
        contrib_edges_by_cycle = {cycle: defaultdict(list) for cycle in CYCLES}
        contrib_edges_all = defaultdict(list)
        for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            donor_key = e['_from'].split('/')[1]
            amount = e.get('total_amount', 0) or 0
            cycle = e.get('cycle', '2024')
            if cycle in CYCLES:
                contrib_edges_by_cycle[cycle][to_cmte].append((donor_key, amount))
            contrib_edges_all[to_cmte].append((donor_key, amount))
        context.log.info(f"   Loaded contribution edges by cycle: " +
                        ", ".join(f"{c}={sum(len(v) for v in contrib_edges_by_cycle[c].values()):,}" for c in CYCLES))
        
        # IE spending BY CYCLE: cycle -> candidate -> {support: [(cmte, amount)], oppose: [(cmte, amount)]}
        ie_by_cycle = {cycle: defaultdict(lambda: {'support': [], 'oppose': []}) for cycle in CYCLES}
        ie_all = defaultdict(lambda: {'support': [], 'oppose': []})
        for e in db.aql.execute("""
            FOR e IN spent_on
            RETURN {
                cand_id: SPLIT(e._to, '/')[1],
                cmte_id: SPLIT(e._from, '/')[1],
                amount: e.total_amount,
                support_oppose: e.support_oppose,
                cycle: e.cycle
            }
        """):
            cand_id = e['cand_id']
            cmte_id = e['cmte_id']
            amount = e['amount'] or 0
            cycle = e.get('cycle', '2024')
            
            if e['support_oppose'] == 'S':
                if cycle in CYCLES:
                    ie_by_cycle[cycle][cand_id]['support'].append((cmte_id, amount))
                ie_all[cand_id]['support'].append((cmte_id, amount))
            else:
                if cycle in CYCLES:
                    ie_by_cycle[cycle][cand_id]['oppose'].append((cmte_id, amount))
                ie_all[cand_id]['oppose'].append((cmte_id, amount))
        context.log.info(f"   Loaded IE data by cycle: " +
                        ", ".join(f"{c}={len(ie_by_cycle[c]):,} cands" for c in CYCLES))
        
        # ================================================================
        # PHASE 2: Helper functions
        # ================================================================
        
        def is_conduit(name: str) -> bool:
            """Check if donor name is a conduit (WinRed, ActBlue, etc.)"""
            if not name:
                return False
            name_upper = name.upper()
            return any(p in name_upper for p in CONDUIT_PATTERNS)
        
        def trace_committee_sources(
            start_cmte_ids: List[str],
            contrib_edges: Dict,
            transfer_edges: Dict,
            multiplier: float = 1.0
        ) -> Dict[str, Any]:
            """
            BFS trace backwards from committees to find terminal sources.
            
            Returns:
            {
                'organizational': {
                    'corporation': {name: amount, ...},
                    'trade_association': {name: amount, ...},
                    'labor_union': {name: amount, ...},
                    'ideological': {name: amount, ...},
                    'cooperative': {name: amount, ...},
                },
                'individuals': {name: {'amount': X, 'employer': Y, 'company': Z}, ...},
                'traced_total': float,  # Total we could attribute
            }
            """
            org_results = {
                'corporation': defaultdict(float),
                'trade_association': defaultdict(float),
                'labor_union': defaultdict(float),
                'ideological': defaultdict(float),
                'cooperative': defaultdict(float),
            }
            individual_results = {}  # name -> {amount, employer, company}
            traced_total = 0.0
            
            # BFS queue: (cmte_id, multiplier, depth)
            queue = [(cmte_id, multiplier, 0) for cmte_id in start_cmte_ids]
            visited_edges = set()
            
            while queue:
                cmte_id, mult, depth = queue.pop(0)
                
                if depth > config.max_trace_depth:
                    continue
                if mult < 0.0001:
                    continue
                
                cmte = cmte_info.get(cmte_id, {})
                
                # Get individual contributions to this committee
                for donor_key, amount in contrib_edges.get(cmte_id, []):
                    edge_key = ('contrib', donor_key, cmte_id)
                    if edge_key in visited_edges:
                        continue
                    visited_edges.add(edge_key)
                    
                    donor = donor_info.get(donor_key, {})
                    name = donor.get('name', donor_key)
                    
                    if is_conduit(name):
                        continue
                    
                    attr_amount = amount * mult
                    traced_total += attr_amount
                    employer = donor.get('employer', '')
                    
                    # Resolve to company
                    company = None
                    if name in whale_to_company:
                        company = whale_to_company[name]
                    elif employer and employer in employer_to_company:
                        company = employer_to_company[employer]
                    
                    if name not in individual_results:
                        individual_results[name] = {
                            'amount': 0,
                            'employer': employer,
                            'company': company
                        }
                    individual_results[name]['amount'] += attr_amount
                
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
                    
                    bucket = TERMINAL_TYPE_BUCKET.get(term_type)
                    if bucket:
                        org_results[bucket][from_name] += attr_amount
                        traced_total += attr_amount
                    elif term_type in PASSTHROUGH_TYPES or term_type == 'campaign':
                        # Trace further upstream
                        from_receipts = from_cmte.get('total_receipts', 0) or 0
                        if from_receipts > 0:
                            new_mult = mult * (amount / from_receipts)
                            if new_mult >= 0.0001:
                                queue.append((from_cmte_id, new_mult, depth + 1))
            
            return {
                'organizational': org_results,
                'individuals': individual_results,
                'traced_total': traced_total,
            }
        
        def trace_ie_sources(
            ie_data: List[tuple],
            contrib_edges: Dict,
            transfer_edges: Dict
        ) -> Dict[str, Any]:
            """
            Trace IE spending back to sources -- who funded the Super PACs?
            
            Returns:
            {
                'by_corporation': {corp_name: amount, ...},
                'by_individual': {name: {'amount': X, 'employer': Y}, ...},
                'by_pac': {pac_name: amount, ...},
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
                    if attr_amount < 100:
                        continue
                    
                    employer = donor.get('employer', '')
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
                    
                    if term_type in TERMINAL_TYPES:
                        results['by_corporation'][from_name] += attr_amount
                    else:
                        results['by_pac'][from_name] += attr_amount
            
            return results

        # ================================================================
        # PHASE 3: Funding channels computation
        # ================================================================
        
        def compute_funding_channels(
            cmte_ids: List[str],
            cand_key: str,
            contrib_edges: Dict,
            transfer_edges: Dict,
            ie_data: Dict,
        ) -> Optional[Dict[str, Any]]:
            """
            Compute funding channels for a candidate given cycle-specific edges.
            
            Returns the funding_channels dict or None if no funding.
            """
            # Trace direct funding through candidate's committees
            sources = trace_committee_sources(cmte_ids, contrib_edges, transfer_edges, multiplier=1.0)
            
            # --- CHANNEL 1: Organizational Direct ---
            org = sources['organizational']
            corp_total = sum(org['corporation'].values())
            trade_total = sum(org['trade_association'].values())
            labor_total = sum(org['labor_union'].values())
            ideological_total = sum(org['ideological'].values())
            coop_total = sum(org['cooperative'].values())
            org_direct_total = corp_total + trade_total + labor_total + ideological_total + coop_total
            
            # --- CHANNEL 4: Individuals ---
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
            
            # --- CHANNELS 2 & 3: IE Support / Oppose ---
            ie_support_data = ie_data.get('support', [])
            ie_oppose_data = ie_data.get('oppose', [])
            ie_support_total = sum(amt for _, amt in ie_support_data)
            ie_oppose_total = sum(amt for _, amt in ie_oppose_data)
            
            # Trace IE funding to find who bankrolls the Super PACs
            ie_support_sources = trace_ie_sources(ie_support_data, contrib_edges, transfer_edges) if ie_support_data else {'by_corporation': {}, 'by_individual': {}, 'by_pac': {}}
            ie_oppose_sources = trace_ie_sources(ie_oppose_data, contrib_edges, transfer_edges) if ie_oppose_data else {'by_corporation': {}, 'by_individual': {}, 'by_pac': {}}
            
            # --- Direct funding total (what candidate committees received) ---
            direct_total = org_direct_total + indiv_total
            
            # --- CHANNEL 5: Unaccounted ---
            # Sum total_receipts for all candidate committees
            cmte_total_receipts = sum(
                (cmte_info.get(cid, {}).get('total_receipts', 0) or 0) for cid in cmte_ids
            )
            traced_direct = sources['traced_total']
            unaccounted = max(0, cmte_total_receipts - traced_direct)
            
            # Total pro-candidate money (direct + IE support)
            total_funding = direct_total + ie_support_total
            
            if total_funding <= 0 and ie_oppose_total <= 0:
                return None
            
            # Helper functions
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
            
            def safe_pct(num, denom):
                return (num / denom * 100) if denom > 0 else 0
            
            # --- Build by-organization view ---
            by_org = defaultdict(lambda: {
                'direct_pac': 0,
                'direct_employees': 0,
                'ie_support': 0,
                'ie_oppose': 0,
                'total': 0,
            })
            
            # All org types into one view
            for bucket_name in ['corporation', 'trade_association', 'labor_union', 'ideological', 'cooperative']:
                for org_name, amount in org[bucket_name].items():
                    by_org[org_name]['direct_pac'] += amount
                    by_org[org_name]['total'] += amount
            
            for company, data in corp_connected.items():
                by_org[company]['direct_employees'] += data['amount']
                by_org[company]['total'] += data['amount']
            for corp_name, amount in ie_support_sources['by_corporation'].items():
                by_org[corp_name]['ie_support'] += amount
                by_org[corp_name]['total'] += amount
            for corp_name, amount in ie_oppose_sources['by_corporation'].items():
                by_org[corp_name]['ie_oppose'] += amount
            
            return {
                # Top-level totals
                'total_funding': total_funding,
                'direct_funding': direct_total,
                'ie_support': ie_support_total,
                'ie_oppose': ie_oppose_total,
                
                # CHANNEL 1: Organizational Direct (all org types collapsed)
                'organizational_direct': {
                    'total': org_direct_total,
                    'pct': safe_pct(org_direct_total, total_funding),
                    'by_type': {
                        'corporation': {
                            'total': corp_total,
                            'pct': safe_pct(corp_total, total_funding),
                            'top': top_sources(org['corporation'], config.top_n_sources),
                        },
                        'trade_association': {
                            'total': trade_total,
                            'pct': safe_pct(trade_total, total_funding),
                            'top': top_sources(org['trade_association'], config.top_n_sources),
                        },
                        'labor_union': {
                            'total': labor_total,
                            'pct': safe_pct(labor_total, total_funding),
                            'top': top_sources(org['labor_union'], config.top_n_sources),
                        },
                        'ideological': {
                            'total': ideological_total,
                            'pct': safe_pct(ideological_total, total_funding),
                            'top': top_sources(org['ideological'], config.top_n_sources),
                        },
                        'cooperative': {
                            'total': coop_total,
                            'pct': safe_pct(coop_total, total_funding),
                            'top': top_sources(org['cooperative'], config.top_n_sources),
                        },
                    },
                },
                
                # CHANNEL 2: IE Support
                'ie': {
                    'support': {
                        'total': ie_support_total,
                        'pct': safe_pct(ie_support_total, total_funding),
                        'top_pacs': [
                            {'name': cmte_info.get(c, {}).get('name', c), 'amount': amt}
                            for c, amt in sorted(ie_support_data, key=lambda x: -x[1])[:10]
                            if amt >= config.min_amount
                        ],
                        'by_corporation': top_sources(ie_support_sources['by_corporation'], config.top_n_sources),
                        'by_pac': top_sources(ie_support_sources['by_pac'], 10),
                    },
                    # CHANNEL 3: IE Oppose
                    'oppose': {
                        'total': ie_oppose_total,
                        'pct': safe_pct(ie_oppose_total, total_funding) if total_funding > 0 else 0,
                        'top_pacs': [
                            {'name': cmte_info.get(c, {}).get('name', c), 'amount': amt}
                            for c, amt in sorted(ie_oppose_data, key=lambda x: -x[1])[:10]
                            if amt >= config.min_amount
                        ],
                        'by_corporation': top_sources(ie_oppose_sources['by_corporation'], config.top_n_sources),
                        'by_pac': top_sources(ie_oppose_sources['by_pac'], 10),
                    },
                },
                
                # CHANNEL 4: Individuals
                'individuals': {
                    'total': indiv_total,
                    'pct': safe_pct(indiv_total, total_funding),
                    'corporate_connected': {
                        'total': corp_connected_total,
                        'pct': safe_pct(corp_connected_total, total_funding),
                        'by_company': top_companies(corp_connected, config.top_n_sources),
                    },
                    'independent': {
                        'total': independent_total,
                        'pct': safe_pct(independent_total, total_funding),
                        'top': top_sources(independent, config.top_n_individuals),
                    },
                },
                
                # CHANNEL 5: Unaccounted
                'unaccounted': {
                    'total': unaccounted,
                    'pct': safe_pct(unaccounted, cmte_total_receipts) if cmte_total_receipts > 0 else 0,
                    'cmte_total_receipts': cmte_total_receipts,
                    'traced_total': traced_direct,
                    'explanation': (
                        "Gap between committee total receipts and traced inflows. "
                        "Includes: unitemized individual contributions (<$200), "
                        "candidate self-funding not yet in donor graph, "
                        "and other untraceable transfers."
                    ),
                },
                
                # Cross-channel org view
                'by_organization': sorted(
                    [
                        {
                            'name': org_name,
                            'direct_pac': data['direct_pac'],
                            'direct_employees': data['direct_employees'],
                            'ie_support': data['ie_support'],
                            'ie_oppose': data['ie_oppose'],
                            'total_pro': data['total'],
                            'total_against': data['ie_oppose'],
                        }
                        for org_name, data in by_org.items()
                        if data['total'] >= config.min_amount or data['ie_oppose'] >= config.min_amount
                    ],
                    key=lambda x: -x['total_pro'],
                )[:50],
            }

        # ================================================================
        # PHASE 4: Process each candidate
        # ================================================================
        context.log.info("Phase 4: Processing candidates...")
        
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
            
            # Compute per cycle
            funding_by_cycle = {}
            
            for cycle in CYCLES:
                cycle_ie = ie_by_cycle[cycle].get(cand_key, {'support': [], 'oppose': []})
                cycle_channels = compute_funding_channels(
                    cmte_ids=cmte_ids,
                    cand_key=cand_key,
                    contrib_edges=contrib_edges_by_cycle[cycle],
                    transfer_edges=transfer_edges_by_cycle[cycle],
                    ie_data=cycle_ie,
                )
                if cycle_channels:
                    funding_by_cycle[cycle] = cycle_channels
            
            # Compute aggregate (all cycles combined)
            aggregate_ie = ie_all.get(cand_key, {'support': [], 'oppose': []})
            funding_aggregate = compute_funding_channels(
                cmte_ids=cmte_ids,
                cand_key=cand_key,
                contrib_edges=contrib_edges_all,
                transfer_edges=transfer_edges_all,
                ie_data=aggregate_ie,
            )
            
            if not funding_aggregate and not funding_by_cycle:
                stats['candidates_processed'] += 1
                continue
            
            stats['candidates_with_funding'] += 1
            
            funding_channels = {
                'by_cycle': funding_by_cycle,
                'aggregate': funding_aggregate,
                
                # Convenience top-level fields from aggregate
                'total_funding': funding_aggregate['total_funding'] if funding_aggregate else 0,
                'direct_funding': funding_aggregate['direct_funding'] if funding_aggregate else 0,
                'ie_support': funding_aggregate['ie_support'] if funding_aggregate else 0,
                'ie_oppose': funding_aggregate['ie_oppose'] if funding_aggregate else 0,
                
                # Top-level by_organization (from aggregate)
                'by_organization': funding_aggregate['by_organization'] if funding_aggregate else [],
                
                'computed_at': datetime.now().isoformat(),
                'cycles_available': list(funding_by_cycle.keys()),
            }
            
            batch_updates.append({
                '_key': cand_key,
                'funding_channels': funding_channels,
            })
            
            stats['candidates_processed'] += 1
            
            if len(batch_updates) >= 500:
                db.aql.execute("""
                    FOR doc IN @batch
                        UPDATE doc._key WITH { funding_channels: doc.funding_channels } IN candidates
                """, bind_vars={"batch": batch_updates})
                context.log.info(f"   Updated {stats['candidates_processed']:,} candidates...")
                batch_updates = []
        
        # Final batch
        if batch_updates:
            db.aql.execute("""
                FOR doc IN @batch
                    UPDATE doc._key WITH { funding_channels: doc.funding_channels } IN candidates
            """, bind_vars={"batch": batch_updates})
        
        # ================================================================
        # PHASE 5: Validation
        # ================================================================
        context.log.info("Phase 5: Validation...")
        
        # Check Ted Cruz
        cruz = list(db.aql.execute("""
            FOR c IN candidates
                FILTER c.CAND_OFFICE == 'S' AND CONTAINS(c.CAND_NAME, 'CRUZ') AND CONTAINS(c.CAND_NAME, 'TED')
                LIMIT 1
                RETURN {
                    name: c.CAND_NAME,
                    fc: c.funding_channels
                }
        """))
        
        if cruz and cruz[0].get('fc'):
            fc = cruz[0]['fc']
            context.log.info(f"\nValidation - {cruz[0]['name']}:")
            context.log.info(f"   Cycles available: {fc.get('cycles_available', [])}")
            context.log.info(f"   Total funding (aggregate): ${fc['total_funding']:,.0f}")
            
            if fc.get('aggregate'):
                agg = fc['aggregate']
                context.log.info(f"\n   FUNDING CHANNELS:")
                context.log.info(f"   Ch1 - Organizational Direct: ${agg['organizational_direct']['total']:,.0f} ({agg['organizational_direct']['pct']:.1f}%)")
                context.log.info(f"         Corp:       ${agg['organizational_direct']['by_type']['corporation']['total']:,.0f}")
                context.log.info(f"         Trade:      ${agg['organizational_direct']['by_type']['trade_association']['total']:,.0f}")
                context.log.info(f"         Labor:      ${agg['organizational_direct']['by_type']['labor_union']['total']:,.0f}")
                context.log.info(f"         Ideological: ${agg['organizational_direct']['by_type']['ideological']['total']:,.0f}")
                context.log.info(f"         Cooperative: ${agg['organizational_direct']['by_type']['cooperative']['total']:,.0f}")
                context.log.info(f"   Ch2 - IE Support:           ${agg['ie']['support']['total']:,.0f} ({agg['ie']['support']['pct']:.1f}%)")
                context.log.info(f"   Ch3 - IE Oppose:            ${agg['ie']['oppose']['total']:,.0f}")
                context.log.info(f"   Ch4 - Individuals:          ${agg['individuals']['total']:,.0f} ({agg['individuals']['pct']:.1f}%)")
                context.log.info(f"         Corp-connected:  ${agg['individuals']['corporate_connected']['total']:,.0f}")
                context.log.info(f"         Independent:     ${agg['individuals']['independent']['total']:,.0f}")
                context.log.info(f"   Ch5 - Unaccounted:          ${agg['unaccounted']['total']:,.0f} ({agg['unaccounted']['pct']:.1f}% of receipts)")
                context.log.info(f"         (receipts: ${agg['unaccounted']['cmte_total_receipts']:,.0f}, traced: ${agg['unaccounted']['traced_total']:,.0f})")
            
            if fc.get('by_cycle'):
                context.log.info(f"\n   BY CYCLE:")
                for cycle, data in sorted(fc['by_cycle'].items()):
                    context.log.info(f"   - {cycle}: ${data['total_funding']:,.0f} total, org=${data['organizational_direct']['total']:,.0f}, indiv=${data['individuals']['total']:,.0f}")
        
        context.log.info(f"\nSummary:")
        context.log.info(f"   Candidates processed: {stats['candidates_processed']:,}")
        context.log.info(f"   Candidates with funding: {stats['candidates_with_funding']:,}")
        
        return Output(
            value=stats,
            metadata={
                "candidates_processed": MetadataValue.int(stats['candidates_processed']),
                "candidates_with_funding": MetadataValue.int(stats['candidates_with_funding']),
            }
        )
