"""Candidate Funding - Trace ALL money flowing to/against candidates.

This is the FUNDING CHANNELS asset. For any candidate, shows exactly how money
reaches them through distinct channels, traced through committee hops to terminal sources.

FUNDING CHANNELS (how money reaches candidates):
1. ORGANIZATIONAL DIRECT - Corporate PACs, trade assocs, labor unions, ideological PACs
   giving directly to candidate committees. All org types collapsed into one channel.
2. IE SUPPORT - Independent expenditures FOR the candidate by Super PACs, traced
   upstream to see who funded those Super PACs.
3. IE OPPOSE - Independent expenditures AGAINST the candidate (traced similarly).
4. INDIVIDUALS - All individual contributions to candidate committees, split into:
   a. Whale donors (maxed out at FEC per-election limit to any committee) - fully traced
      through graph with employer/corporate detail
      - Corporate-connected: employees of known corps (via canonical_employers/wikidata)
      - Independent: everyone else
   b. Grassroots donors (below per-election limit) - known total from raw FEC data, no per-donor detail
5. UNACCOUNTED - TRUE residual gap: committee trace loss through passthroughs, data gaps,
   unitemized contributions (<$200), and actual dark money. Should be small (< 15%).

For organizational money, we trace BACKWARDS through passthrough committees (JFCs,
conduits, party committees) to the TERMINAL SOURCE -- the org PAC whose terminal_type
tells us what kind of organization it is (corporation, trade, labor, ideological, cooperative).

KEY INSIGHT: The donors graph contains individuals who maxed out at the FEC per-election
contribution limit ($2,800-$3,500 depending on cycle) to at least one committee. These are
people who deliberately hit the legal ceiling for a specific candidate — not casual donors.
committee_receipts computes actual totals from ALL raw FEC transactions. The difference
(sub-threshold individuals) is a KNOWN quantity folded into the individuals channel as
'grassroots', NOT dumped into unaccounted.

UNACCOUNTED captures only true unknowns:
- Committee trace loss (proportional loss through passthrough hops)
- Unitemized individual contributions (<$200 aggregate, not in FEC indiv file)
- Data gaps and edge cases

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
- individuals: { total, pct, whale: {corporate_connected, independent}, grassroots: {total, pct} }
  whale = maxed out at FEC per-election limit; grassroots = below that limit
- unaccounted: { total, pct, explanation }  (TRUE residual only)

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
CYCLES = ["2020", "2022", "2024", "2026"]

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


def is_conduit(name: str) -> bool:
    """Whether a donor name represents a conduit (WinRed, ActBlue, etc.) —
    aggregator pseudo-donors that pass through earmarked individual money.
    Filtered out so we don't double-count individuals via the conduit."""
    if not name:
        return False
    name_upper = name.upper()
    return any(p in name_upper for p in CONDUIT_PATTERNS)


def _resolve_company(name: str, employer: str,
                     whale_to_company: Dict[str, str],
                     employer_to_company: Dict[str, str]) -> Optional[str]:
    """Resolve a donor to a corporate-family canonical name. Whale links
    (founder/CEO/employee-by-name from Wikidata) take precedence over
    employer canonical-mapping, since whale linkage is more specific."""
    if name in whale_to_company:
        return whale_to_company[name]
    if employer and employer in employer_to_company:
        return employer_to_company[employer]
    return None


def trace_committee_sources(
    start_cmte_ids: List[str],
    contrib_edges: Dict,
    transfer_edges: Dict,
    cycle_cmte_info: Dict,
    donor_info: Dict[str, Dict],
    whale_to_company: Dict[str, str],
    employer_to_company: Dict[str, str],
    max_trace_depth: int = 8,
    multiplier: float = 1.0,
) -> Dict[str, Any]:
    """Two-phase proportional trace backwards from committees to terminal
    sources. See module docstring for the funding-channels model.

    Phase 1: Propagate multipliers level-by-level through passthrough
    committees. Each committee accumulates the TOTAL proportion of its
    money that flows to this candidate. Handles multiple transfer edges
    correctly (e.g. JFC→candidate across 3 cycles = 3 edges, combined
    into one mult).

    Phase 2: Process each committee ONCE with its accumulated mult.
    Attribute whale individuals, terminal org PACs, and upstream
    grassroots.

    Cycle break + multiplier caps prevent the catastrophic blowup
    documented in decisions.md 2026-05-08.
    """
    org_results = {
        'corporation': defaultdict(float),
        'trade_association': defaultdict(float),
        'labor_union': defaultdict(float),
        'ideological': defaultdict(float),
        'cooperative': defaultdict(float),
    }
    individual_results: Dict[str, Dict[str, Any]] = {}
    traced_total = 0.0
    grassroots_upstream = 0.0
    start_set = set(start_cmte_ids)

    # Phase 1: propagate multipliers level-by-level through the
    # passthrough graph, attributing terminal orgs as we go.
    all_mults: Dict[str, float] = defaultdict(float)
    for cmte_id in start_cmte_ids:
        all_mults[cmte_id] += multiplier
    current_level: Dict[str, float] = defaultdict(float)
    for cmte_id in start_cmte_ids:
        current_level[cmte_id] += multiplier
    propagated_from = set(start_cmte_ids)

    for _depth in range(max_trace_depth):
        next_level: Dict[str, float] = defaultdict(float)
        for cmte_id, mult in current_level.items():
            if mult < 0.0001:
                continue
            for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
                from_cmte = cycle_cmte_info.get(from_cmte_id, {})
                term_type = from_cmte.get('terminal_type', 'unknown')
                attr_amount = amount * mult
                from_name = from_cmte.get('name', from_cmte_id)

                bucket = TERMINAL_TYPE_BUCKET.get(term_type)
                if bucket:
                    org_results[bucket][from_name] += attr_amount
                    traced_total += attr_amount
                elif term_type in PASSTHROUGH_TYPES or term_type == 'campaign':
                    if from_cmte_id in propagated_from:
                        continue
                    from_receipts = from_cmte.get('total_receipts', 0) or 0
                    if from_receipts <= 0:
                        continue
                    edge_fraction = min(1.0, amount / from_receipts)
                    new_mult = mult * edge_fraction
                    if new_mult >= 0.0001:
                        next_level[from_cmte_id] += new_mult
                        all_mults[from_cmte_id] = min(
                            1.0, all_mults[from_cmte_id] + new_mult
                        )
        if not next_level:
            break
        propagated_from.update(current_level.keys())
        current_level = next_level

    # Phase 2: at each visited committee, attribute whale individuals
    # and (for non-starting cmtes) the small-donor pool proportionally.
    for cmte_id, mult in all_mults.items():
        if mult < 0.0001:
            continue
        for donor_key, amount in contrib_edges.get(cmte_id, []):
            donor = donor_info.get(donor_key, {})
            name = donor.get('name', donor_key)
            if is_conduit(name):
                continue
            attr_amount = amount * mult
            traced_total += attr_amount
            employer = donor.get('employer', '')
            company = _resolve_company(name, employer, whale_to_company, employer_to_company)
            if name not in individual_results:
                individual_results[name] = {'amount': 0, 'employer': employer, 'company': company}
            individual_results[name]['amount'] += attr_amount

        if cmte_id not in start_set:
            from_grassroots = cycle_cmte_info.get(cmte_id, {}).get('small_donor_total', 0) or 0
            if from_grassroots > 0:
                grassroots_upstream += from_grassroots * mult

    return {
        'organizational': org_results,
        'individuals': individual_results,
        'traced_total': traced_total,
        'grassroots_upstream': grassroots_upstream,
    }


def trace_ie_sources(
    ie_data: List[tuple],
    contrib_edges: Dict,
    transfer_edges: Dict,
    cycle_cmte_info: Dict,
    donor_info: Dict[str, Dict],
    whale_to_company: Dict[str, str],
    employer_to_company: Dict[str, str],
    min_attr_amount: float = 100.0,
) -> Dict[str, Any]:
    """Trace IE spending back to its donors — who funded the Super PACs?

    For each (spending_cmte, ie_amount) tuple, computes
    `multiplier = min(ie_amount / cmte.total_receipts, 1.0)` and
    attributes each donor to the PAC's spending in proportion to their
    share. Whale donors mapped to a corporate family (via Wikidata or
    employer canonical_name) accumulate at the company level; the rest
    accumulate at the individual or PAC level.

    `by_corporation_via_donors[company][donor_name] = amount` carries
    the donor names that produced the corporate attribution so
    `by_organization` can show "Pan Am Railways via MELLON, TIMOTHY"
    transparently rather than implying corporate spending.
    """
    results: Dict[str, Any] = {
        'by_corporation': defaultdict(float),
        'by_corporation_via_donors': defaultdict(lambda: defaultdict(float)),
        'by_individual': {},
        'by_pac': defaultdict(float),
    }

    for cmte_id, ie_amount in ie_data:
        cmte = cycle_cmte_info.get(cmte_id, {})
        total_receipts = cmte.get('total_receipts', 0) or 0
        if total_receipts <= 0 or ie_amount <= 0:
            continue
        multiplier = min(ie_amount / total_receipts, 1.0)

        for donor_key, amount in contrib_edges.get(cmte_id, []):
            donor = donor_info.get(donor_key, {})
            if not donor:
                continue
            name = donor.get('name', donor_key)
            if is_conduit(name):
                continue
            attr_amount = amount * multiplier
            if attr_amount < min_attr_amount:
                continue
            employer = donor.get('employer', '')
            company = _resolve_company(name, employer, whale_to_company, employer_to_company)
            if company:
                results['by_corporation'][company] += attr_amount
                results['by_corporation_via_donors'][company][name] += attr_amount
            else:
                if name not in results['by_individual']:
                    results['by_individual'][name] = {'amount': 0, 'employer': employer}
                results['by_individual'][name]['amount'] += attr_amount

        for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
            from_cmte = cycle_cmte_info.get(from_cmte_id, {})
            from_name = from_cmte.get('name', from_cmte_id)
            term_type = from_cmte.get('terminal_type', 'unknown')
            attr_amount = amount * multiplier
            if attr_amount < min_attr_amount:
                continue
            if term_type in TERMINAL_TYPES:
                results['by_corporation'][from_name] += attr_amount
            else:
                results['by_pac'][from_name] += attr_amount

    return results


@asset(
    name="candidate_funding",
    description="Trace ALL money to candidates by funding channel -- organizational direct, IE support/oppose, individuals, unaccounted.",
    group_name="aggregation",
    compute_kind="aggregation",
    # NOTE: The asset code is graceful re: missing wikidata data (uses
    # `db.has_collection('corporate_families')` defensively). We intentionally
    # do NOT declare wikidata_corporate_resolution as a dep here — Dagster's
    # enforcement would block running candidate_funding when wikidata is
    # broken/incomplete (which it currently is — see docs/todo.md for the
    # negative-cache + batched-VALUES refactor). Removed 2026-05-08.
    deps=["committee_classification", "committee_receipts", "affiliated_with",
          "transferred_to", "spent_on", "contributed_to"],
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
        
        # Committee info with terminal_type and per-cycle receipts
        cmte_info = {}
        for c in db.aql.execute("""
            FOR c IN committees
            RETURN {
                _key: c._key,
                name: c.CMTE_NM,
                terminal_type: c.terminal_type,
                total_receipts: c.total_receipts || 0,
                total_from_individuals: c.total_from_individuals || 0,
                total_from_committees: c.total_from_committees || 0,
                small_donor_total: c.small_donor_total || 0,
                self_funding_total: c.self_funding_total || 0,
                receipts_by_cycle: c.receipts_by_cycle || {}
            }
        """):
            cmte_info[c['_key']] = c
        context.log.info(f"   Loaded {len(cmte_info):,} committees")

        # Build per-cycle cmte_info dicts (per-cycle receipts for correct multipliers)
        cmte_info_by_cycle = {}
        for cycle in CYCLES:
            cycle_info = {}
            for cmte_id, info in cmte_info.items():
                cycle_data = info.get('receipts_by_cycle', {}).get(cycle, {})
                cycle_info[cmte_id] = {
                    '_key': cmte_id,
                    'name': info['name'],
                    'terminal_type': info['terminal_type'],
                    'total_receipts': cycle_data.get('total_receipts', 0) or 0,
                    'total_from_individuals': cycle_data.get('total_from_individuals', 0) or 0,
                    'total_from_committees': cycle_data.get('total_from_committees', 0) or 0,
                    'small_donor_total': cycle_data.get('small_donor_total', 0) or 0,
                    'self_funding_total': cycle_data.get('self_funding_total', 0) or 0,
                }
            cmte_info_by_cycle[cycle] = cycle_info
        context.log.info(f"   Built per-cycle cmte_info dicts")
        
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
        for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            from_cmte = e['_from'].split('/')[1]
            amount = e.get('total_amount', 0) or 0
            cycle = e.get('cycle', '2024')
            if cycle in CYCLES:
                transfer_edges_by_cycle[cycle][to_cmte].append((from_cmte, amount))
        context.log.info(f"   Loaded transfer edges by cycle: " +
                        ", ".join(f"{c}={sum(len(v) for v in transfer_edges_by_cycle[c].values()):,}" for c in CYCLES))
        
        # Contribution edges BY CYCLE: cycle -> cmte -> [(donor_key, amount), ...]
        contrib_edges_by_cycle = {cycle: defaultdict(list) for cycle in CYCLES}
        for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            donor_key = e['_from'].split('/')[1]
            amount = e.get('total_amount', 0) or 0
            cycle = e.get('cycle', '2024')
            if cycle in CYCLES:
                contrib_edges_by_cycle[cycle][to_cmte].append((donor_key, amount))
        context.log.info(f"   Loaded contribution edges by cycle: " +
                        ", ".join(f"{c}={sum(len(v) for v in contrib_edges_by_cycle[c].values()):,}" for c in CYCLES))
        
        # IE spending BY CYCLE: cycle -> candidate -> {support: [(cmte, amount)], oppose: [(cmte, amount)]}
        ie_by_cycle = {cycle: defaultdict(lambda: {'support': [], 'oppose': []}) for cycle in CYCLES}
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
            else:
                if cycle in CYCLES:
                    ie_by_cycle[cycle][cand_id]['oppose'].append((cmte_id, amount))
        context.log.info(f"   Loaded IE data by cycle: " +
                        ", ".join(f"{c}={len(ie_by_cycle[c]):,} cands" for c in CYCLES))
        
        # ================================================================
        # PHASE 2-3: Per-candidate computation (uses module-level helpers
        # trace_committee_sources / trace_ie_sources lifted above)
        # ================================================================
        
        def compute_funding_channels(
            cmte_ids: List[str],
            cand_key: str,
            contrib_edges: Dict,
            transfer_edges: Dict,
            ie_data: Dict,
            cycle_cmte_info: Dict,
        ) -> Optional[Dict[str, Any]]:
            """
            Compute funding channels for a candidate given cycle-specific edges.
            
            Returns the funding_channels dict or None if no funding.
            """
            # Trace direct funding through candidate's committees
            sources = trace_committee_sources(
                cmte_ids, contrib_edges, transfer_edges, cycle_cmte_info,
                donor_info=donor_info,
                whale_to_company=whale_to_company,
                employer_to_company=employer_to_company,
                max_trace_depth=config.max_trace_depth,
                multiplier=1.0,
            )
            
            # --- CHANNEL 1: Organizational Direct ---
            org = sources['organizational']
            corp_total = sum(org['corporation'].values())
            trade_total = sum(org['trade_association'].values())
            labor_total = sum(org['labor_union'].values())
            ideological_total = sum(org['ideological'].values())
            coop_total = sum(org['cooperative'].values())
            org_direct_total = corp_total + trade_total + labor_total + ideological_total + coop_total
            
            # --- CHANNEL 4: Individuals ---
            # Whale donors: $10K+ aggregate, traced through graph with employer detail
            whale_indiv_total = sum(d['amount'] for d in sources['individuals'].values())
            
            # Split whale individuals by corporate connection
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
            independent_total = whale_indiv_total - corp_connected_total
            
            # Grassroots donors: sub-$10K aggregate, known total from raw FEC but no per-donor detail
            # committee_receipts computes this as: total_from_individuals - whale_donor_total
            cmte_total_receipts = sum(
                (cycle_cmte_info.get(cid, {}).get('total_receipts', 0) or 0) for cid in cmte_ids
            )
            cmte_total_from_individuals = sum(
                (cycle_cmte_info.get(cid, {}).get('total_from_individuals', 0) or 0) for cid in cmte_ids
            )
            cmte_total_from_committees = sum(
                (cycle_cmte_info.get(cid, {}).get('total_from_committees', 0) or 0) for cid in cmte_ids
            )
            grassroots_direct = sum(
                (cycle_cmte_info.get(cid, {}).get('small_donor_total', 0) or 0) for cid in cmte_ids
            )
            # Grassroots at upstream passthroughs (BFS-traced proportionally)
            grassroots_upstream = sources.get('grassroots_upstream', 0)
            grassroots_total = grassroots_direct + grassroots_upstream

            # Self-funding (CAND_CONTRIB + CAND_LOANS from weball, sourced via
            # committee_receipts). Reported as its own sub-bucket under individuals
            # so journalists can read "Trone self-funded $62.9M" cleanly without it
            # masquerading as small donors or whale donations.
            self_funded_total = sum(
                (cycle_cmte_info.get(cid, {}).get('self_funding_total', 0) or 0) for cid in cmte_ids
            )

            # All individuals = whale (graph-traced) + grassroots (direct + upstream) + self-funding
            all_indiv_total = whale_indiv_total + grassroots_total + self_funded_total
            
            # --- CHANNELS 2 & 3: IE Support / Oppose ---
            ie_support_data = ie_data.get('support', [])
            ie_oppose_data = ie_data.get('oppose', [])
            ie_support_total = sum(amt for _, amt in ie_support_data)
            ie_oppose_total = sum(amt for _, amt in ie_oppose_data)
            
            # Trace IE funding to find who bankrolls the Super PACs
            _ie_kwargs = dict(
                donor_info=donor_info,
                whale_to_company=whale_to_company,
                employer_to_company=employer_to_company,
            )
            _empty_ie = {'by_corporation': {}, 'by_corporation_via_donors': {}, 'by_individual': {}, 'by_pac': {}}
            ie_support_sources = trace_ie_sources(ie_support_data, contrib_edges, transfer_edges, cycle_cmte_info, **_ie_kwargs) if ie_support_data else _empty_ie
            ie_oppose_sources = trace_ie_sources(ie_oppose_data, contrib_edges, transfer_edges, cycle_cmte_info, **_ie_kwargs) if ie_oppose_data else _empty_ie
            
            # --- Direct funding total (what candidate committees received) ---
            direct_total = org_direct_total + all_indiv_total
            
            # --- CHANNEL 5: Unaccounted (TRUE residual only) ---
            # BFS traced: whale individuals + terminal org transfers (proportional through passthroughs)
            traced_direct = sources['traced_total']
            # Total accounted = BFS-traced + grassroots + self-funding (all from raw FEC)
            total_accounted = traced_direct + grassroots_total + self_funded_total
            unaccounted = max(0, cmte_total_receipts - total_accounted)
            
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
            # Each org may have:
            #   direct_pac     — real corporate-PAC contributions to this candidate
            #   direct_employees — whale donations from individuals whose employer
            #                      maps to this org via canonical_employers
            #   ie_support / ie_oppose — IE money traced back to donors who map
            #                            to this org via whale_corporate_links or
            #                            employer_to_company
            #
            # The direct_employees + IE columns can attribute to a corporate
            # NAME via founder personal donations (e.g. Mellon → Pan Am
            # Railways). _via_donors makes that transparency explicit so
            # readers don't mistake founder personal donations for corporate
            # PAC money.
            by_org = defaultdict(lambda: {
                'direct_pac': 0,
                'direct_employees': 0,
                'ie_support': 0,
                'ie_oppose': 0,
                'total': 0,
                # donor_name → {ie_support, ie_oppose, employees}
                '_via_donors': defaultdict(lambda: {'ie_support': 0, 'ie_oppose': 0, 'employees': 0}),
            })

            # All org types into one view
            for bucket_name in ['corporation', 'trade_association', 'labor_union', 'ideological', 'cooperative']:
                for org_name, amount in org[bucket_name].items():
                    by_org[org_name]['direct_pac'] += amount
                    by_org[org_name]['total'] += amount

            for company, data in corp_connected.items():
                by_org[company]['direct_employees'] += data['amount']
                by_org[company]['total'] += data['amount']
                for d in data.get('donors', []):
                    by_org[company]['_via_donors'][d['name']]['employees'] += d['amount']
            for corp_name, amount in ie_support_sources['by_corporation'].items():
                by_org[corp_name]['ie_support'] += amount
                by_org[corp_name]['total'] += amount
                for donor_name, donor_amt in ie_support_sources.get('by_corporation_via_donors', {}).get(corp_name, {}).items():
                    by_org[corp_name]['_via_donors'][donor_name]['ie_support'] += donor_amt
            for corp_name, amount in ie_oppose_sources['by_corporation'].items():
                by_org[corp_name]['ie_oppose'] += amount
                for donor_name, donor_amt in ie_oppose_sources.get('by_corporation_via_donors', {}).get(corp_name, {}).items():
                    by_org[corp_name]['_via_donors'][donor_name]['ie_oppose'] += donor_amt
            
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
                            {'name': cycle_cmte_info.get(c, {}).get('name', c), 'amount': amt}
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
                            {'name': cycle_cmte_info.get(c, {}).get('name', c), 'amount': amt}
                            for c, amt in sorted(ie_oppose_data, key=lambda x: -x[1])[:10]
                            if amt >= config.min_amount
                        ],
                        'by_corporation': top_sources(ie_oppose_sources['by_corporation'], config.top_n_sources),
                        'by_pac': top_sources(ie_oppose_sources['by_pac'], 10),
                    },
                },
                
                # CHANNEL 4: Individuals (whale + grassroots)
                'individuals': {
                    'total': all_indiv_total,
                    'pct': safe_pct(all_indiv_total, total_funding),
                    # Whale donors: $10K+ aggregate, graph-traced with employer detail
                    'whale': {
                        'total': whale_indiv_total,
                        'pct': safe_pct(whale_indiv_total, total_funding),
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
                    # Grassroots: sub-$10K aggregate, known total from raw FEC, no per-donor detail
                    'grassroots': {
                        'total': grassroots_total,
                        'pct': safe_pct(grassroots_total, total_funding),
                        'direct': grassroots_direct,
                        'upstream': grassroots_upstream,
                        'explanation': (
                            "Individual donors below $10K aggregate threshold. "
                            "'direct' = grassroots giving to candidate's own committees. "
                            "'upstream' = grassroots at feeder committees (JFCs, conduits, "
                            "party committees) attributed proportionally through transfer chain."
                        ),
                    },
                    # Self-funding: candidate's own contributions + loans to their committee
                    'self_funded': {
                        'total': self_funded_total,
                        'pct': safe_pct(self_funded_total, total_funding),
                    },
                },
                
                # CHANNEL 5: Unaccounted (TRUE residual only)
                'unaccounted': {
                    'total': unaccounted,
                    'pct': safe_pct(unaccounted, cmte_total_receipts) if cmte_total_receipts > 0 else 0,
                    'cmte_total_receipts': cmte_total_receipts,
                    'total_accounted': total_accounted,
                    'breakdown': {
                        'from_individuals_raw': cmte_total_from_individuals,
                        'from_committees_raw': cmte_total_from_committees,
                    },
                    'explanation': (
                        "True residual: committee trace loss through passthrough hops, "
                        "unitemized contributions (<$200 aggregate not in FEC indiv file), "
                        "and data gaps. Sub-$10K individual donors are accounted for "
                        "in the individuals.grassroots channel."
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
                            # Top contributing donor names that produced any
                            # employees/IE attribution rolled up to this org.
                            # Empty when the org's totals come purely from
                            # corporate-PAC contributions (direct_pac).
                            'via_donors': sorted(
                                [
                                    {
                                        'name': dn,
                                        'ie_support': vd['ie_support'],
                                        'ie_oppose': vd['ie_oppose'],
                                        'employees': vd['employees'],
                                    }
                                    for dn, vd in data['_via_donors'].items()
                                    if (vd['ie_support'] + vd['ie_oppose'] + vd['employees']) >= config.min_amount
                                ],
                                key=lambda x: -(x['ie_support'] + x['ie_oppose'] + x['employees']),
                            )[:5],
                        }
                        for org_name, data in by_org.items()
                        if data['total'] >= config.min_amount or data['ie_oppose'] >= config.min_amount
                    ],
                    key=lambda x: -x['total_pro'],
                )[:50],
            }

        # ================================================================
        # merge_funding_channels: Aggregate = sum of per-cycle results
        # ================================================================
        def merge_funding_channels(cycle_results: Dict[str, Dict]) -> Optional[Dict[str, Any]]:
            """Merge per-cycle funding channel results into aggregate.

            Sums numeric values, merges top-source lists by name, recomputes percentages.
            """
            results = [v for v in cycle_results.values() if v is not None]
            if not results:
                return None

            def safe_pct(num, denom):
                return (num / denom * 100) if denom > 0 else 0

            def merge_named_list(lists, n=None):
                """Merge lists of {name, amount} dicts, summing by name."""
                merged = defaultdict(float)
                for lst in lists:
                    for item in lst:
                        merged[item['name']] += item['amount']
                result = sorted(
                    [{'name': k, 'amount': v} for k, v in merged.items() if v >= config.min_amount],
                    key=lambda x: -x['amount']
                )
                return result[:n] if n else result

            # Top-level sums
            total_funding = sum(r['total_funding'] for r in results)
            direct_funding = sum(r['direct_funding'] for r in results)
            ie_support_total = sum(r['ie_support'] for r in results)
            ie_oppose_total = sum(r['ie_oppose'] for r in results)

            # Organizational direct by type
            org_by_type = {}
            for t in ['corporation', 'trade_association', 'labor_union', 'ideological', 'cooperative']:
                total = sum(r['organizational_direct']['by_type'][t]['total'] for r in results)
                top = merge_named_list(
                    [r['organizational_direct']['by_type'][t]['top'] for r in results],
                    n=config.top_n_sources
                )
                org_by_type[t] = {'total': total, 'pct': safe_pct(total, total_funding), 'top': top}
            org_direct_total = sum(v['total'] for v in org_by_type.values())

            # IE support/oppose
            ie_support = {
                'total': ie_support_total,
                'pct': safe_pct(ie_support_total, total_funding),
                'top_pacs': merge_named_list(
                    [r['ie']['support'].get('top_pacs', []) for r in results], n=10
                ),
                'by_corporation': merge_named_list(
                    [r['ie']['support'].get('by_corporation', []) for r in results],
                    n=config.top_n_sources
                ),
                'by_pac': merge_named_list(
                    [r['ie']['support'].get('by_pac', []) for r in results], n=10
                ),
            }
            ie_oppose = {
                'total': ie_oppose_total,
                'pct': safe_pct(ie_oppose_total, total_funding) if total_funding > 0 else 0,
                'top_pacs': merge_named_list(
                    [r['ie']['oppose'].get('top_pacs', []) for r in results], n=10
                ),
                'by_corporation': merge_named_list(
                    [r['ie']['oppose'].get('by_corporation', []) for r in results],
                    n=config.top_n_sources
                ),
                'by_pac': merge_named_list(
                    [r['ie']['oppose'].get('by_pac', []) for r in results], n=10
                ),
            }

            # Individuals
            whale_total = sum(r['individuals']['whale']['total'] for r in results)
            corp_connected_total = sum(r['individuals']['whale']['corporate_connected']['total'] for r in results)
            independent_total = sum(r['individuals']['whale']['independent']['total'] for r in results)
            grassroots_direct = sum(r['individuals']['grassroots']['direct'] for r in results)
            grassroots_upstream = sum(r['individuals']['grassroots']['upstream'] for r in results)
            grassroots_total = grassroots_direct + grassroots_upstream
            self_funded_total = sum(
                r['individuals'].get('self_funded', {}).get('total', 0) for r in results
            )
            all_indiv_total = whale_total + grassroots_total + self_funded_total

            # Merge corporate connected by_company
            corp_by_company = defaultdict(lambda: {'amount': 0, 'donors': defaultdict(float)})
            for r in results:
                for item in r['individuals']['whale']['corporate_connected'].get('by_company', []):
                    corp_by_company[item['company']]['amount'] += item['amount']
                    for donor in item.get('top_donors', []):
                        corp_by_company[item['company']]['donors'][donor['name']] += donor['amount']

            merged_by_company = sorted(
                [{
                    'company': k,
                    'amount': v['amount'],
                    'top_donors': sorted(
                        [{'name': dk, 'amount': dv} for dk, dv in v['donors'].items()],
                        key=lambda x: -x['amount']
                    )[:5]
                } for k, v in corp_by_company.items() if v['amount'] >= config.min_amount],
                key=lambda x: -x['amount']
            )[:config.top_n_sources]

            # Independent top
            indep_top = merge_named_list(
                [r['individuals']['whale']['independent'].get('top', []) for r in results],
                n=config.top_n_individuals
            )

            # Unaccounted
            unaccounted_total = sum(r['unaccounted']['total'] for r in results)
            cmte_total_receipts = sum(r['unaccounted']['cmte_total_receipts'] for r in results)
            total_accounted = sum(r['unaccounted']['total_accounted'] for r in results)
            from_individuals_raw = sum(r['unaccounted']['breakdown']['from_individuals_raw'] for r in results)
            from_committees_raw = sum(r['unaccounted']['breakdown']['from_committees_raw'] for r in results)

            # By organization
            by_org_merged = defaultdict(lambda: {
                'direct_pac': 0, 'direct_employees': 0,
                'ie_support': 0, 'ie_oppose': 0,
                'total': 0, 'total_against': 0,
                # donor_name → {ie_support, ie_oppose, employees}
                'via_donors': defaultdict(lambda: {'ie_support': 0, 'ie_oppose': 0, 'employees': 0}),
            })
            for r in results:
                for org in r.get('by_organization', []):
                    by_org_merged[org['name']]['direct_pac'] += org.get('direct_pac', 0)
                    by_org_merged[org['name']]['direct_employees'] += org.get('direct_employees', 0)
                    by_org_merged[org['name']]['ie_support'] += org.get('ie_support', 0)
                    by_org_merged[org['name']]['ie_oppose'] += org.get('ie_oppose', 0)
                    by_org_merged[org['name']]['total'] += org.get('total_pro', 0)
                    by_org_merged[org['name']]['total_against'] += org.get('total_against', 0)
                    for vd in org.get('via_donors', []):
                        by_org_merged[org['name']]['via_donors'][vd['name']]['ie_support'] += vd.get('ie_support', 0)
                        by_org_merged[org['name']]['via_donors'][vd['name']]['ie_oppose'] += vd.get('ie_oppose', 0)
                        by_org_merged[org['name']]['via_donors'][vd['name']]['employees'] += vd.get('employees', 0)

            return {
                'total_funding': total_funding,
                'direct_funding': direct_funding,
                'ie_support': ie_support_total,
                'ie_oppose': ie_oppose_total,
                'organizational_direct': {
                    'total': org_direct_total,
                    'pct': safe_pct(org_direct_total, total_funding),
                    'by_type': org_by_type,
                },
                'ie': {
                    'support': ie_support,
                    'oppose': ie_oppose,
                },
                'individuals': {
                    'total': all_indiv_total,
                    'pct': safe_pct(all_indiv_total, total_funding),
                    'whale': {
                        'total': whale_total,
                        'pct': safe_pct(whale_total, total_funding),
                        'corporate_connected': {
                            'total': corp_connected_total,
                            'pct': safe_pct(corp_connected_total, total_funding),
                            'by_company': merged_by_company,
                        },
                        'independent': {
                            'total': independent_total,
                            'pct': safe_pct(independent_total, total_funding),
                            'top': indep_top,
                        },
                    },
                    'grassroots': {
                        'total': grassroots_total,
                        'pct': safe_pct(grassroots_total, total_funding),
                        'direct': grassroots_direct,
                        'upstream': grassroots_upstream,
                        'explanation': (
                            "Individual donors below $10K aggregate threshold. "
                            "'direct' = grassroots giving to candidate's own committees. "
                            "'upstream' = grassroots at feeder committees (JFCs, conduits, "
                            "party committees) attributed proportionally through transfer chain."
                        ),
                    },
                    'self_funded': {
                        'total': self_funded_total,
                        'pct': safe_pct(self_funded_total, total_funding),
                    },
                },
                'unaccounted': {
                    'total': unaccounted_total,
                    'pct': safe_pct(unaccounted_total, cmte_total_receipts) if cmte_total_receipts > 0 else 0,
                    'cmte_total_receipts': cmte_total_receipts,
                    'total_accounted': total_accounted,
                    'breakdown': {
                        'from_individuals_raw': from_individuals_raw,
                        'from_committees_raw': from_committees_raw,
                    },
                    'explanation': (
                        "True residual: committee trace loss through passthrough hops, "
                        "unitemized contributions (<$200 aggregate not in FEC indiv file), "
                        "and data gaps. Sub-$10K individual donors are accounted for "
                        "in the individuals.grassroots channel."
                    ),
                },
                'by_organization': sorted(
                    [
                        {
                            'name': org_name,
                            'direct_pac': data['direct_pac'],
                            'direct_employees': data['direct_employees'],
                            'ie_support': data['ie_support'],
                            'ie_oppose': data['ie_oppose'],
                            'total_pro': data['total'],
                            'total_against': data['total_against'],
                            'via_donors': sorted(
                                [
                                    {
                                        'name': dn,
                                        'ie_support': vd['ie_support'],
                                        'ie_oppose': vd['ie_oppose'],
                                        'employees': vd['employees'],
                                    }
                                    for dn, vd in data['via_donors'].items()
                                    if (vd['ie_support'] + vd['ie_oppose'] + vd['employees']) >= config.min_amount
                                ],
                                key=lambda x: -(x['ie_support'] + x['ie_oppose'] + x['employees']),
                            )[:5],
                        }
                        for org_name, data in by_org_merged.items()
                        if data['total'] >= config.min_amount or data['total_against'] >= config.min_amount
                    ],
                    key=lambda x: -x['total_pro'],
                )[:50],
            }

        # ================================================================
        # PHASE 4: Process each candidate
        # ================================================================
        context.log.info("Phase 4: Processing candidates...")

        # Get campaign-only affiliated committees, grouped by cycle
        # Only CMTE_TP in (H, S, P) = actual campaign committees
        # Party committees, JFCs, leadership PACs only appear via upstream tracing
        candidates = list(db.aql.execute("""
            FOR c IN candidates
                LET campaign_cmtes_by_cycle = (
                    FOR v, e IN INBOUND c affiliated_with
                    FILTER e.cmte_type IN ['H', 'S', 'P']
                    COLLECT cycle = e.cycle INTO cmtes = v._key
                    RETURN { cycle: cycle, cmte_ids: UNIQUE(cmtes) }
                )
                FILTER LENGTH(campaign_cmtes_by_cycle) > 0
                RETURN {
                    _key: c._key,
                    name: c.CAND_NAME,
                    party: c.CAND_PTY_AFFILIATION,
                    office: c.CAND_OFFICE,
                    state: c.CAND_OFFICE_ST,
                    cmtes_by_cycle: campaign_cmtes_by_cycle
                }
        """))
        context.log.info(f"   Found {len(candidates):,} candidates with campaign committees")

        stats = {
            'candidates_processed': 0,
            'candidates_with_funding': 0,
        }

        batch_updates = []

        for cand in candidates:
            cand_key = cand['_key']
            cmtes_by_cycle = {
                item['cycle']: item['cmte_ids']
                for item in cand['cmtes_by_cycle']
            }

            # Compute per cycle (with cycle-specific committees and receipts)
            funding_by_cycle = {}

            for cycle in CYCLES:
                cycle_cmte_ids = cmtes_by_cycle.get(cycle, [])
                if not cycle_cmte_ids:
                    continue

                cycle_ie = ie_by_cycle[cycle].get(cand_key, {'support': [], 'oppose': []})
                cycle_channels = compute_funding_channels(
                    cmte_ids=cycle_cmte_ids,
                    cand_key=cand_key,
                    contrib_edges=contrib_edges_by_cycle[cycle],
                    transfer_edges=transfer_edges_by_cycle[cycle],
                    ie_data=cycle_ie,
                    cycle_cmte_info=cmte_info_by_cycle[cycle],
                )
                if cycle_channels:
                    funding_by_cycle[cycle] = cycle_channels

            if not funding_by_cycle:
                stats['candidates_processed'] += 1
                continue

            # Aggregate = merge per-cycle results (not independent computation)
            funding_aggregate = merge_funding_channels(funding_by_cycle)

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
                whale = agg['individuals'].get('whale', {})
                context.log.info(f"         Whale ($10K+):   ${whale.get('total', 0):,.0f}")
                context.log.info(f"           Corp-connected:  ${whale.get('corporate_connected', {}).get('total', 0):,.0f}")
                context.log.info(f"           Independent:     ${whale.get('independent', {}).get('total', 0):,.0f}")
                context.log.info(f"         Grassroots (<$10K): ${agg['individuals'].get('grassroots', {}).get('total', 0):,.0f}")
                grass = agg['individuals'].get('grassroots', {})
                context.log.info(f"           Direct:         ${grass.get('direct', 0):,.0f}")
                context.log.info(f"           Upstream:       ${grass.get('upstream', 0):,.0f}")
                context.log.info(f"   Ch5 - Unaccounted:          ${agg['unaccounted']['total']:,.0f} ({agg['unaccounted']['pct']:.1f}% of receipts)")
                context.log.info(f"         (receipts: ${agg['unaccounted']['cmte_total_receipts']:,.0f}, accounted: ${agg['unaccounted']['total_accounted']:,.0f})")
            
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
