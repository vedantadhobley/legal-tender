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

import os
import multiprocessing as _mp
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Any, List, Set, Optional
from datetime import datetime
from collections import defaultdict

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.config import ACTIVE_CYCLES
from src.resources.arango import ArangoDBResource


# Terminal types - these are the REAL funding sources (don't trace further)
TERMINAL_TYPES = {"corporation", "trade_association", "labor_union", "ideological", "cooperative"}

# Passthrough types - trace THROUGH these to find real sources
PASSTHROUGH_TYPES = {"passthrough", "unknown", "super_pac_unclassified"}

# Conduit patterns to filter from individual donors
CONDUIT_PATTERNS = ["WINRED", "ACTBLUE", "EARMARK", "CONDUIT", "UNITEMIZED"]

# Election cycles to process — single source: src/config.ACTIVE_CYCLES.
# Kept as module-local alias since this file uses CYCLES as a positional
# constant in many list comprehensions (no behavior change vs literal).
CYCLES = list(ACTIVE_CYCLES)

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


def _safe_pct(num: float, denom: float) -> float:
    """Percentage helper that returns 0 instead of dividing by zero."""
    return (num / denom * 100) if denom > 0 else 0


def _merge_data_quality(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate per-cycle donor-detail-coverage into a candidate-aggregate.

    Returns the same shape as the per-cycle `individuals.data_quality`
    block (detail_coverage, individuals_itemized, individuals_summary_only,
    primary_source). detail_coverage is the dollar-weighted average across
    cycles. primary_source is `mixed` when cycles disagree, else the
    common value.
    """
    itemized = sum(
        (r.get('individuals', {}).get('data_quality', {}).get('individuals_itemized', 0) or 0)
        for r in results
    )
    summary_only = sum(
        (r.get('individuals', {}).get('data_quality', {}).get('individuals_summary_only', 0) or 0)
        for r in results
    )
    total = itemized + summary_only
    coverage = (itemized / total) if total > 0 else 1.0
    sources = {
        r.get('individuals', {}).get('data_quality', {}).get('primary_source')
        for r in results
    } - {None}
    primary = (
        'mixed' if len(sources) > 1
        else next(iter(sources)) if sources
        else 'unknown'
    )
    return {
        'detail_coverage': coverage,
        'individuals_itemized': itemized,
        'individuals_summary_only': summary_only,
        'primary_source': primary,
    }


def _top_sources(d: Dict[str, float], n: int, min_amount: float) -> List[Dict[str, Any]]:
    """Top-N {name, amount} entries from a {name -> amount} dict, threshold-filtered."""
    sorted_items = sorted(d.items(), key=lambda x: -x[1])[:n]
    return [{'name': k, 'amount': v} for k, v in sorted_items if v >= min_amount]


def _top_individuals(d: Dict[str, Dict[str, Any]], n: int, min_amount: float) -> List[Dict[str, Any]]:
    """Top-N {name, amount, employer} entries from a {name -> {amount, employer}}
    dict, threshold-filtered. Used to surface IE donor attribution that
    didn't resolve to a corporate family."""
    sorted_items = sorted(d.items(), key=lambda x: -x[1].get('amount', 0))[:n]
    return [
        {'name': k, 'amount': v.get('amount', 0), 'employer': v.get('employer', '')}
        for k, v in sorted_items
        if v.get('amount', 0) >= min_amount
    ]


def _top_companies(d: Dict[str, Dict[str, Any]], n: int, min_amount: float) -> List[Dict[str, Any]]:
    """Top-N companies from a {company -> {amount, donors}} dict, with top-5
    donors per company, threshold-filtered."""
    sorted_items = sorted(d.items(), key=lambda x: -x[1]['amount'])[:n]
    return [
        {
            'company': k,
            'amount': v['amount'],
            'top_donors': sorted(v['donors'], key=lambda x: -x['amount'])[:5],
        }
        for k, v in sorted_items
        if v['amount'] >= min_amount
    ]


def trace_ie_sources(
    ie_data: List[tuple],
    contrib_edges: Dict,
    transfer_edges: Dict,
    cycle_cmte_info: Dict,
    donor_info: Dict[str, Dict],
    whale_to_company: Dict[str, str],
    employer_to_company: Dict[str, str],
    min_attr_amount: float = 100.0,
    max_trace_depth: int = 8,
) -> Dict[str, Any]:
    """Trace IE spending back to its donors — who funded the Super PACs?

    For each (spending_cmte, ie_amount) tuple, walk *recursively* upstream
    through passthrough committees (incl. super_pac_unclassified) until
    hitting a terminal organizational PAC (corp / trade / labor / ideo /
    coop) or running out of depth. At each visited committee, attribute
    its individual donors proportionally (donor_amount × accumulated_mult).

    Why recursive: many big Super PACs are funded primarily by *other*
    Super PACs. Senate Leadership Fund ($1.14B) is funded by One Nation,
    which is funded by individuals + other SPACs, etc. Single-level
    attribution (the pre-2026-05-12 behavior) stopped at "One Nation"
    and dumped the real donors into by_pac as one opaque line item.

    Mirrors `trace_committee_sources` Phase-1 propagation + Phase-2
    attribution structure. Same correctness guards (cycle break,
    per-edge fraction cap at 1.0, accumulated multiplier cap at 1.0)
    documented in decisions.md 2026-05-08.

    `by_corporation_via_donors[company][donor_name] = amount` carries
    the donor names that produced each corporate attribution so
    `by_organization` can show "Pan Am Railways via MELLON, TIMOTHY"
    transparently rather than implying corporate spending.

    `by_pac` collects passthrough PACs whose donor edges we couldn't
    proportionally trace further (zero or missing total_receipts) —
    a "trace ended here" signal that preserves the upstream PAC name
    rather than silently dropping it.
    """
    results: Dict[str, Any] = {
        'by_corporation': defaultdict(float),
        'by_corporation_via_donors': defaultdict(lambda: defaultdict(float)),
        'by_individual': {},
        'by_pac': defaultdict(float),
    }

    for spending_cmte_id, ie_amount in ie_data:
        spending_cmte = cycle_cmte_info.get(spending_cmte_id, {})
        total_receipts = spending_cmte.get('total_receipts', 0) or 0
        if total_receipts <= 0 or ie_amount <= 0:
            continue
        initial_mult = min(ie_amount / total_receipts, 1.0)

        # Phase 1: propagate multipliers level-by-level through the
        # transfer-graph upstream of the spending committee. At each
        # terminal-type hit, attribute directly to by_corporation.
        all_mults: Dict[str, float] = defaultdict(float)
        all_mults[spending_cmte_id] = initial_mult
        current_level: Dict[str, float] = {spending_cmte_id: initial_mult}
        propagated_from: Set[str] = {spending_cmte_id}

        for _depth in range(max_trace_depth):
            next_level: Dict[str, float] = defaultdict(float)
            for cmte_id, mult in current_level.items():
                if mult < 0.0001:
                    continue
                for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
                    from_cmte = cycle_cmte_info.get(from_cmte_id, {})
                    from_name = from_cmte.get('name', from_cmte_id)
                    term_type = from_cmte.get('terminal_type', 'unknown')
                    attr_amount = amount * mult
                    if attr_amount < min_attr_amount:
                        continue

                    if term_type in TERMINAL_TYPES:
                        # Terminal org PAC → attribute and stop walking
                        results['by_corporation'][from_name] += attr_amount
                    elif term_type in PASSTHROUGH_TYPES or term_type == 'campaign':
                        if from_cmte_id in propagated_from:
                            continue
                        from_receipts = from_cmte.get('total_receipts', 0) or 0
                        if from_receipts <= 0:
                            # Can't proportionally trace further. Record
                            # as upstream PAC so the name surfaces in
                            # by_pac instead of disappearing silently.
                            results['by_pac'][from_name] += attr_amount
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

        # Phase 2: at each visited committee (including the original
        # spending committee), attribute its direct individual donors
        # proportionally to the accumulated multiplier.
        for cmte_id, mult in all_mults.items():
            if mult < 0.0001:
                continue
            for donor_key, amount in contrib_edges.get(cmte_id, []):
                donor = donor_info.get(donor_key, {})
                if not donor:
                    continue
                name = donor.get('name', donor_key)
                if is_conduit(name):
                    continue
                attr_amount = amount * mult
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

    return results


def _merge_individuals_list(lists: List[List[Dict[str, Any]]], n: Optional[int], min_amount: float) -> List[Dict[str, Any]]:
    """Merge per-cycle lists of {name, amount, employer} dicts by name,
    summing amounts and keeping the most recent non-empty employer."""
    merged: Dict[str, Dict[str, Any]] = {}
    for lst in lists:
        for item in lst:
            name = item['name']
            if name not in merged:
                merged[name] = {'name': name, 'amount': 0, 'employer': item.get('employer', '')}
            merged[name]['amount'] += item.get('amount', 0)
            if item.get('employer') and not merged[name].get('employer'):
                merged[name]['employer'] = item['employer']
    result = sorted(
        [v for v in merged.values() if v['amount'] >= min_amount],
        key=lambda x: -x['amount'],
    )
    return result[:n] if n else result


def _merge_named_list(lists: List[List[Dict[str, Any]]], n: Optional[int], min_amount: float) -> List[Dict[str, Any]]:
    """Merge per-cycle lists of {name, amount} dicts by name, threshold-
    filtered, sorted descending. Used by merge_funding_channels."""
    merged: Dict[str, float] = defaultdict(float)
    for lst in lists:
        for item in lst:
            merged[item['name']] += item['amount']
    result = sorted(
        [{'name': k, 'amount': v} for k, v in merged.items() if v >= min_amount],
        key=lambda x: -x['amount'],
    )
    return result[:n] if n else result


# ----------------------------------------------------------------------------
# Per-candidate computation + ProcessPool worker
# ----------------------------------------------------------------------------
#
# `_WORKER_LOOKUPS` and `_WORKER_CONFIG` are set in the parent process
# *before* the ProcessPool is created. On Linux (Python's default
# ProcessPoolExecutor uses fork), the worker processes inherit the
# parent's memory copy-on-write — so workers can read these globals
# without any of the lookup state ever being pickled. Only the small
# per-candidate `cand` dict gets pickled across the boundary per task.
# Without this trick, passing ~hundreds of MB of edges-by-cycle dicts
# to each of 24 workers via initargs would dominate runtime.

_WORKER_LOOKUPS: Optional[Dict[str, Any]] = None
_WORKER_CONFIG: Optional['CandidateFundingConfig'] = None


def _compute_for_candidate(
    cand: Dict[str, Any],
    lookups: Dict[str, Any],
    config: 'CandidateFundingConfig',
) -> Optional[Dict[str, Any]]:
    """Pure function: compute the upsert doc for one candidate.

    Returns `{'_key': cand_key, 'funding_channels': {...}}` if the
    candidate has any funding in any cycle, else None. Used by both
    the in-process serial path and the ProcessPool worker.
    """
    cand_key = cand['_key']
    cmtes_by_cycle = {item['cycle']: item['cmte_ids'] for item in cand['cmtes_by_cycle']}

    funding_by_cycle: Dict[str, Dict[str, Any]] = {}
    for cycle in CYCLES:
        cycle_cmte_ids = cmtes_by_cycle.get(cycle, [])
        if not cycle_cmte_ids:
            continue
        cycle_ie = lookups['ie_by_cycle'][cycle].get(cand_key, {'support': [], 'oppose': []})
        cycle_channels = compute_funding_channels(
            cmte_ids=cycle_cmte_ids,
            cand_key=cand_key,
            contrib_edges=lookups['contrib_edges_by_cycle'][cycle],
            transfer_edges=lookups['transfer_edges_by_cycle'][cycle],
            ie_data=cycle_ie,
            cycle_cmte_info=lookups['cmte_info_by_cycle'][cycle],
            donor_info=lookups['donor_info'],
            whale_to_company=lookups['whale_to_company'],
            employer_to_company=lookups['employer_to_company'],
            config=config,
        )
        if cycle_channels:
            funding_by_cycle[cycle] = cycle_channels

    if not funding_by_cycle:
        return None

    funding_aggregate = merge_funding_channels(funding_by_cycle, config)
    funding_channels = {
        'by_cycle': funding_by_cycle,
        'aggregate': funding_aggregate,
        'total_funding': funding_aggregate['total_funding'] if funding_aggregate else 0,
        'direct_funding': funding_aggregate['direct_funding'] if funding_aggregate else 0,
        'ie_support': funding_aggregate['ie_support'] if funding_aggregate else 0,
        'ie_oppose': funding_aggregate['ie_oppose'] if funding_aggregate else 0,
        'by_organization': funding_aggregate['by_organization'] if funding_aggregate else [],
        'computed_at': datetime.now().isoformat(),
        'cycles_available': list(funding_by_cycle.keys()),
    }
    return {'_key': cand_key, 'funding_channels': funding_channels}


def _process_one_candidate(cand: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """ProcessPool worker entry point. Reads parent-set module globals
    (avoids pickling shared state per task) and delegates to
    _compute_for_candidate."""
    return _compute_for_candidate(cand, _WORKER_LOOKUPS, _WORKER_CONFIG)


def load_lookup_data(db) -> Dict[str, Any]:
    """Load all the in-memory lookup state candidate_funding needs to
    process candidates: committee info per cycle, donor info, employer
    and whale → corporate-family mappings, transfer/contrib/IE edges
    per cycle.

    Returns a dict with these keys (each is what its name suggests):
        cmte_info_by_cycle
        employer_to_company
        whale_to_company
        donor_info
        transfer_edges_by_cycle
        contrib_edges_by_cycle
        ie_by_cycle

    No light side effects beyond the obvious DB reads. Caller logs
    counts after each step using the returned dict's sizes.
    """
    # Committee info with terminal_type and per-cycle receipts
    cmte_info: Dict[str, Dict[str, Any]] = {}
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

    # Per-cycle slices of committee state — used as the per-candidate
    # cycle_cmte_info argument to compute_funding_channels.
    cmte_info_by_cycle: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for cycle in CYCLES:
        cycle_info: Dict[str, Dict[str, Any]] = {}
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
                # Data-quality provenance: did committee_receipts itemize
                # this committee's individual money from indiv.zip records,
                # or did it fall back to FEC's aggregate summary (webl/
                # weball)? Carried through to the per-candidate output so
                # consumers can tell whether the whale/grassroots breakdown
                # is real or a summary-fed placeholder.
                'total_from_individuals_source':
                    cycle_data.get('total_from_individuals_source'),
                'total_from_individuals_indiv_zip':
                    cycle_data.get('total_from_individuals_indiv_zip', 0) or 0,
            }
        cmte_info_by_cycle[cycle] = cycle_info

    # Employer → canonical corporate family. Built by the
    # canonical_employers + wikidata_corporate_resolution assets.
    employer_to_company: Dict[str, str] = {}
    if db.has_collection('employer_canonical_mapping'):
        for m in db.aql.execute(
            "FOR m IN employer_canonical_mapping RETURN { employer: m.employer_name, company: m.canonical_name }"
        ):
            employer_to_company[m['employer']] = m['company']

    # Whale donor name → corporate family (founder/owner/CEO links).
    whale_to_company: Dict[str, str] = {}
    if db.has_collection('whale_corporate_links'):
        for link in db.aql.execute(
            "FOR l IN whale_corporate_links RETURN { donor: l.donor_name, company: l.canonical_name }"
        ):
            whale_to_company[link['donor']] = link['company']

    # Whale donor info (≥$10K aggregate, deduped per (name, employer)).
    donor_info: Dict[str, Dict[str, Any]] = {}
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

    # Transfer + contribution + IE edges, partitioned by cycle.
    transfer_edges_by_cycle: Dict[str, Dict[str, List]] = {c: defaultdict(list) for c in CYCLES}
    for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
        to_cmte = e['_to'].split('/')[1]
        from_cmte = e['_from'].split('/')[1]
        amount = e.get('total_amount', 0) or 0
        cycle = e.get('cycle', '2024')
        if cycle in CYCLES:
            transfer_edges_by_cycle[cycle][to_cmte].append((from_cmte, amount))

    contrib_edges_by_cycle: Dict[str, Dict[str, List]] = {c: defaultdict(list) for c in CYCLES}
    for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
        to_cmte = e['_to'].split('/')[1]
        donor_key = e['_from'].split('/')[1]
        amount = e.get('total_amount', 0) or 0
        cycle = e.get('cycle', '2024')
        if cycle in CYCLES:
            contrib_edges_by_cycle[cycle][to_cmte].append((donor_key, amount))

    ie_by_cycle: Dict[str, Dict[str, Dict[str, List]]] = {
        c: defaultdict(lambda: {'support': [], 'oppose': []}) for c in CYCLES
    }
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
        if cycle not in CYCLES:
            continue
        if e['support_oppose'] == 'S':
            ie_by_cycle[cycle][cand_id]['support'].append((cmte_id, amount))
        else:
            ie_by_cycle[cycle][cand_id]['oppose'].append((cmte_id, amount))

    return {
        'cmte_info_by_cycle': cmte_info_by_cycle,
        'employer_to_company': employer_to_company,
        'whale_to_company': whale_to_company,
        'donor_info': donor_info,
        'transfer_edges_by_cycle': transfer_edges_by_cycle,
        'contrib_edges_by_cycle': contrib_edges_by_cycle,
        'ie_by_cycle': ie_by_cycle,
    }


def merge_funding_channels(cycle_results: Dict[str, Dict], config: 'CandidateFundingConfig') -> Optional[Dict[str, Any]]:
    """Merge per-cycle funding_channels dicts into a single aggregate.

    Sums numeric values, merges {name, amount} top-source lists by
    name, recomputes percentages against the new aggregate total. Top
    lists are re-sorted and re-truncated at the merged level so the
    aggregate's "top corporations" reflects the cross-cycle total
    rather than concatenating per-cycle tops.
    """
    results = [v for v in cycle_results.values() if v is not None]
    if not results:
        return None

    total_funding = sum(r['total_funding'] for r in results)
    direct_funding = sum(r['direct_funding'] for r in results)
    ie_support_total = sum(r['ie_support'] for r in results)
    ie_oppose_total = sum(r['ie_oppose'] for r in results)

    # Org direct by type (corporation/trade_assoc/labor_union/...)
    org_by_type: Dict[str, Dict[str, Any]] = {}
    for t in ('corporation', 'trade_association', 'labor_union', 'ideological', 'cooperative'):
        total = sum(r['organizational_direct']['by_type'][t]['total'] for r in results)
        top = _merge_named_list(
            [r['organizational_direct']['by_type'][t]['top'] for r in results],
            n=config.top_n_sources,
            min_amount=config.min_amount,
        )
        org_by_type[t] = {'total': total, 'pct': _safe_pct(total, total_funding), 'top': top}
    org_direct_total = sum(v['total'] for v in org_by_type.values())

    ie_support = {
        'total': ie_support_total,
        'pct': _safe_pct(ie_support_total, total_funding),
        'top_pacs': _merge_named_list([r['ie']['support'].get('top_pacs', []) for r in results], 10, config.min_amount),
        'by_corporation': _merge_named_list(
            [r['ie']['support'].get('by_corporation', []) for r in results],
            config.top_n_sources, config.min_amount,
        ),
        'by_individual': _merge_individuals_list(
            [r['ie']['support'].get('by_individual', []) for r in results],
            config.top_n_individuals, config.min_amount,
        ),
        'by_pac': _merge_named_list([r['ie']['support'].get('by_pac', []) for r in results], 10, config.min_amount),
    }
    ie_oppose = {
        'total': ie_oppose_total,
        'pct': _safe_pct(ie_oppose_total, total_funding) if total_funding > 0 else 0,
        'top_pacs': _merge_named_list([r['ie']['oppose'].get('top_pacs', []) for r in results], 10, config.min_amount),
        'by_corporation': _merge_named_list(
            [r['ie']['oppose'].get('by_corporation', []) for r in results],
            config.top_n_sources, config.min_amount,
        ),
        'by_individual': _merge_individuals_list(
            [r['ie']['oppose'].get('by_individual', []) for r in results],
            config.top_n_individuals, config.min_amount,
        ),
        'by_pac': _merge_named_list([r['ie']['oppose'].get('by_pac', []) for r in results], 10, config.min_amount),
    }

    whale_total = sum(r['individuals']['whale']['total'] for r in results)
    corp_connected_total = sum(r['individuals']['whale']['corporate_connected']['total'] for r in results)
    independent_total = sum(r['individuals']['whale']['independent']['total'] for r in results)
    grassroots_direct = sum(r['individuals']['grassroots']['direct'] for r in results)
    grassroots_upstream = sum(r['individuals']['grassroots']['upstream'] for r in results)
    grassroots_total = grassroots_direct + grassroots_upstream
    self_funded_total = sum(r['individuals'].get('self_funded', {}).get('total', 0) for r in results)
    all_indiv_total = whale_total + grassroots_total + self_funded_total

    # Merge corporate_connected.by_company across cycles, with each
    # company's top_donors merged by donor name.
    corp_by_company: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'amount': 0, 'donors': defaultdict(float)})
    for r in results:
        for item in r['individuals']['whale']['corporate_connected'].get('by_company', []):
            corp_by_company[item['company']]['amount'] += item['amount']
            for donor in item.get('top_donors', []):
                corp_by_company[item['company']]['donors'][donor['name']] += donor['amount']
    merged_by_company = sorted(
        [
            {
                'company': k,
                'amount': v['amount'],
                'top_donors': sorted(
                    [{'name': dk, 'amount': dv} for dk, dv in v['donors'].items()],
                    key=lambda x: -x['amount'],
                )[:5],
            }
            for k, v in corp_by_company.items()
            if v['amount'] >= config.min_amount
        ],
        key=lambda x: -x['amount'],
    )[:config.top_n_sources]

    indep_top = _merge_named_list(
        [r['individuals']['whale']['independent'].get('top', []) for r in results],
        config.top_n_individuals, config.min_amount,
    )

    unaccounted_total = sum(r['unaccounted']['total'] for r in results)
    cmte_total_receipts = sum(r['unaccounted']['cmte_total_receipts'] for r in results)
    total_accounted = sum(r['unaccounted']['total_accounted'] for r in results)
    from_individuals_raw = sum(r['unaccounted']['breakdown']['from_individuals_raw'] for r in results)
    from_committees_raw = sum(r['unaccounted']['breakdown']['from_committees_raw'] for r in results)

    # Merge by_organization with via_donors propagated per-org.
    by_org_merged: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'direct_pac': 0, 'direct_employees': 0,
        'ie_support': 0, 'ie_oppose': 0,
        'total': 0, 'total_against': 0,
        'via_donors': defaultdict(lambda: {'ie_support': 0, 'ie_oppose': 0, 'employees': 0}),
    })
    for r in results:
        for org in r.get('by_organization', []):
            entry = by_org_merged[org['name']]
            entry['direct_pac'] += org.get('direct_pac', 0)
            entry['direct_employees'] += org.get('direct_employees', 0)
            entry['ie_support'] += org.get('ie_support', 0)
            entry['ie_oppose'] += org.get('ie_oppose', 0)
            entry['total'] += org.get('total_pro', 0)
            entry['total_against'] += org.get('total_against', 0)
            for vd in org.get('via_donors', []):
                entry['via_donors'][vd['name']]['ie_support'] += vd.get('ie_support', 0)
                entry['via_donors'][vd['name']]['ie_oppose'] += vd.get('ie_oppose', 0)
                entry['via_donors'][vd['name']]['employees'] += vd.get('employees', 0)

    return {
        'total_funding': total_funding,
        'direct_funding': direct_funding,
        'ie_support': ie_support_total,
        'ie_oppose': ie_oppose_total,
        'organizational_direct': {
            'total': org_direct_total,
            'pct': _safe_pct(org_direct_total, total_funding),
            'by_type': org_by_type,
        },
        'ie': {'support': ie_support, 'oppose': ie_oppose},
        'individuals': {
            'total': all_indiv_total,
            'pct': _safe_pct(all_indiv_total, total_funding),
            'whale': {
                'total': whale_total,
                'pct': _safe_pct(whale_total, total_funding),
                'corporate_connected': {
                    'total': corp_connected_total,
                    'pct': _safe_pct(corp_connected_total, total_funding),
                    'by_company': merged_by_company,
                },
                'independent': {
                    'total': independent_total,
                    'pct': _safe_pct(independent_total, total_funding),
                    'top': indep_top,
                },
            },
            'grassroots': {
                'total': grassroots_total,
                'pct': _safe_pct(grassroots_total, total_funding),
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
                'pct': _safe_pct(self_funded_total, total_funding),
            },
            'data_quality': _merge_data_quality(results),
        },
        'unaccounted': {
            'total': unaccounted_total,
            'pct': _safe_pct(unaccounted_total, cmte_total_receipts) if cmte_total_receipts > 0 else 0,
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


def compute_funding_channels(
    cmte_ids: List[str],
    cand_key: str,
    contrib_edges: Dict,
    transfer_edges: Dict,
    ie_data: Dict,
    cycle_cmte_info: Dict,
    donor_info: Dict[str, Dict],
    whale_to_company: Dict[str, str],
    employer_to_company: Dict[str, str],
    config: 'CandidateFundingConfig',
) -> Optional[Dict[str, Any]]:
    """Compute the per-cycle funding_channels dict for one candidate.

    Combines a backwards trace through the candidate's affiliated
    committees (organizational direct + whale individuals + upstream
    grassroots) with IE Support/Oppose source attribution, plus the
    grassroots and self-funding totals from committee_receipts.

    Returns None if the candidate has no funding in this cycle.
    """
    # --- Trace backwards through candidate's committees ---
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

    # --- CHANNEL 4: Individuals (whale graph + grassroots from cmte_receipts) ---
    whale_indiv_total = sum(d['amount'] for d in sources['individuals'].values())

    corp_connected: Dict[str, Dict[str, Any]] = {}
    independent: Dict[str, float] = {}
    for name, data in sources['individuals'].items():
        if data['company']:
            if data['company'] not in corp_connected:
                corp_connected[data['company']] = {'amount': 0, 'donors': []}
            corp_connected[data['company']]['amount'] += data['amount']
            if data['amount'] >= config.min_amount:
                corp_connected[data['company']]['donors'].append({
                    'name': name,
                    'amount': data['amount'],
                })
        else:
            independent[name] = data['amount']
    corp_connected_total = sum(c['amount'] for c in corp_connected.values())
    independent_total = whale_indiv_total - corp_connected_total

    # Aggregate cmte-side totals from committee_receipts
    cmte_total_receipts = sum(
        (cycle_cmte_info.get(cid, {}).get('total_receipts', 0) or 0) for cid in cmte_ids
    )
    cmte_total_from_individuals = sum(
        (cycle_cmte_info.get(cid, {}).get('total_from_individuals', 0) or 0) for cid in cmte_ids
    )
    cmte_total_from_committees = sum(
        (cycle_cmte_info.get(cid, {}).get('total_from_committees', 0) or 0) for cid in cmte_ids
    )
    # Donor-detail coverage: how much of the candidate's individual money
    # is backed by itemized records (indiv.zip) vs how much came in via
    # FEC's summary-file fallback. Critical for downstream consumers: a
    # candidate whose individual money is summary-sourced has NO real
    # whale/grassroots breakdown — the entire amount gets routed to
    # grassroots by default, which would otherwise misrepresent who's
    # funding them. We surface coverage so the UI can show "donor-level
    # detail unknown for $X of $Y" instead of pretending.
    cmte_individuals_itemized = sum(
        (cycle_cmte_info.get(cid, {}).get('total_from_individuals_indiv_zip', 0) or 0)
        for cid in cmte_ids
    )
    donor_detail_coverage = (
        cmte_individuals_itemized / cmte_total_from_individuals
        if cmte_total_from_individuals > 0 else 1.0
    )
    cmte_sources = [
        cycle_cmte_info.get(cid, {}).get('total_from_individuals_source')
        for cid in cmte_ids
    ]
    primary_source = (
        'fec_summary'
        if any(s == 'fec_summary' for s in cmte_sources)
        and not any(s == 'indiv_zip' for s in cmte_sources)
        else 'indiv_zip' if any(s == 'indiv_zip' for s in cmte_sources)
        else 'mixed' if any(s for s in cmte_sources)
        else 'unknown'
    )
    grassroots_direct = sum(
        (cycle_cmte_info.get(cid, {}).get('small_donor_total', 0) or 0) for cid in cmte_ids
    )
    grassroots_upstream = sources.get('grassroots_upstream', 0)
    grassroots_total = grassroots_direct + grassroots_upstream

    # Self-funding (CAND_CONTRIB + CAND_LOANS) — separate sub-bucket so
    # journalists see it cleanly without it masquerading as small donors
    # or whale donations.
    self_funded_total = sum(
        (cycle_cmte_info.get(cid, {}).get('self_funding_total', 0) or 0) for cid in cmte_ids
    )
    all_indiv_total = whale_indiv_total + grassroots_total + self_funded_total

    # --- CHANNELS 2 & 3: IE Support / Oppose ---
    ie_support_data = ie_data.get('support', [])
    ie_oppose_data = ie_data.get('oppose', [])
    ie_support_total = sum(amt for _, amt in ie_support_data)
    ie_oppose_total = sum(amt for _, amt in ie_oppose_data)

    _ie_kwargs = dict(
        donor_info=donor_info,
        whale_to_company=whale_to_company,
        employer_to_company=employer_to_company,
        max_trace_depth=config.max_trace_depth,
    )
    _empty_ie = {'by_corporation': {}, 'by_corporation_via_donors': {}, 'by_individual': {}, 'by_pac': {}}
    ie_support_sources = (
        trace_ie_sources(ie_support_data, contrib_edges, transfer_edges, cycle_cmte_info, **_ie_kwargs)
        if ie_support_data else _empty_ie
    )
    ie_oppose_sources = (
        trace_ie_sources(ie_oppose_data, contrib_edges, transfer_edges, cycle_cmte_info, **_ie_kwargs)
        if ie_oppose_data else _empty_ie
    )

    direct_total = org_direct_total + all_indiv_total
    traced_direct = sources['traced_total']
    total_accounted = traced_direct + grassroots_total + self_funded_total
    unaccounted = max(0, cmte_total_receipts - total_accounted)
    total_funding = direct_total + ie_support_total

    if total_funding <= 0 and ie_oppose_total <= 0:
        return None

    # --- by_organization cross-cut (combines direct PAC + employees + IE)
    # via_donors per org tracks which donor names produced the
    # employees/IE attribution rolled up to a corporate identity, so
    # readers can distinguish founder personal donations from corporate
    # PAC money.
    by_org: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'direct_pac': 0,
        'direct_employees': 0,
        'ie_support': 0,
        'ie_oppose': 0,
        'total': 0,
        '_via_donors': defaultdict(lambda: {'ie_support': 0, 'ie_oppose': 0, 'employees': 0}),
    })
    for bucket_name in ('corporation', 'trade_association', 'labor_union', 'ideological', 'cooperative'):
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
        'total_funding': total_funding,
        'direct_funding': direct_total,
        'ie_support': ie_support_total,
        'ie_oppose': ie_oppose_total,

        'organizational_direct': {
            'total': org_direct_total,
            'pct': _safe_pct(org_direct_total, total_funding),
            'by_type': {
                'corporation': {
                    'total': corp_total,
                    'pct': _safe_pct(corp_total, total_funding),
                    'top': _top_sources(org['corporation'], config.top_n_sources, config.min_amount),
                },
                'trade_association': {
                    'total': trade_total,
                    'pct': _safe_pct(trade_total, total_funding),
                    'top': _top_sources(org['trade_association'], config.top_n_sources, config.min_amount),
                },
                'labor_union': {
                    'total': labor_total,
                    'pct': _safe_pct(labor_total, total_funding),
                    'top': _top_sources(org['labor_union'], config.top_n_sources, config.min_amount),
                },
                'ideological': {
                    'total': ideological_total,
                    'pct': _safe_pct(ideological_total, total_funding),
                    'top': _top_sources(org['ideological'], config.top_n_sources, config.min_amount),
                },
                'cooperative': {
                    'total': coop_total,
                    'pct': _safe_pct(coop_total, total_funding),
                    'top': _top_sources(org['cooperative'], config.top_n_sources, config.min_amount),
                },
            },
        },

        'ie': {
            'support': {
                'total': ie_support_total,
                'pct': _safe_pct(ie_support_total, total_funding),
                'top_pacs': [
                    {'name': cycle_cmte_info.get(c, {}).get('name', c), 'amount': amt}
                    for c, amt in sorted(ie_support_data, key=lambda x: -x[1])[:10]
                    if amt >= config.min_amount
                ],
                'by_corporation': _top_sources(ie_support_sources['by_corporation'], config.top_n_sources, config.min_amount),
                'by_individual': _top_individuals(ie_support_sources['by_individual'], config.top_n_individuals, config.min_amount),
                'by_pac': _top_sources(ie_support_sources['by_pac'], 10, config.min_amount),
            },
            'oppose': {
                'total': ie_oppose_total,
                'pct': _safe_pct(ie_oppose_total, total_funding) if total_funding > 0 else 0,
                'top_pacs': [
                    {'name': cycle_cmte_info.get(c, {}).get('name', c), 'amount': amt}
                    for c, amt in sorted(ie_oppose_data, key=lambda x: -x[1])[:10]
                    if amt >= config.min_amount
                ],
                'by_corporation': _top_sources(ie_oppose_sources['by_corporation'], config.top_n_sources, config.min_amount),
                'by_individual': _top_individuals(ie_oppose_sources['by_individual'], config.top_n_individuals, config.min_amount),
                'by_pac': _top_sources(ie_oppose_sources['by_pac'], 10, config.min_amount),
            },
        },

        'individuals': {
            'total': all_indiv_total,
            'pct': _safe_pct(all_indiv_total, total_funding),
            'whale': {
                'total': whale_indiv_total,
                'pct': _safe_pct(whale_indiv_total, total_funding),
                'corporate_connected': {
                    'total': corp_connected_total,
                    'pct': _safe_pct(corp_connected_total, total_funding),
                    'by_company': _top_companies(corp_connected, config.top_n_sources, config.min_amount),
                },
                'independent': {
                    'total': independent_total,
                    'pct': _safe_pct(independent_total, total_funding),
                    'top': _top_sources(independent, config.top_n_individuals, config.min_amount),
                },
            },
            'grassroots': {
                'total': grassroots_total,
                'pct': _safe_pct(grassroots_total, total_funding),
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
                'pct': _safe_pct(self_funded_total, total_funding),
            },
            # Data-quality provenance — see compute_funding_channels for the
            # rationale. When detail_coverage < 1.0, the whale/grassroots
            # split above is partly (or entirely) made up of FEC summary
            # totals routed to grassroots by default. Consumers should
            # render this honestly rather than treating the breakdown as
            # ground truth.
            'data_quality': {
                'detail_coverage': donor_detail_coverage,
                'individuals_itemized': cmte_individuals_itemized,
                'individuals_summary_only': max(0, cmte_total_from_individuals - cmte_individuals_itemized),
                'primary_source': primary_source,
            },
        },

        'unaccounted': {
            'total': unaccounted,
            'pct': _safe_pct(unaccounted, cmte_total_receipts) if cmte_total_receipts > 0 else 0,
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
                "in the individuals.grassroots channel. NOTE: if "
                "individuals.data_quality.detail_coverage < 1.0, the "
                "unaccounted figure understates uncertainty — donor-level "
                "detail is missing for part of the individual money."
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
                    'total_against': data['ie_oppose'],
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
        lookups = load_lookup_data(db)
        cmte_info_by_cycle = lookups["cmte_info_by_cycle"]
        employer_to_company = lookups["employer_to_company"]
        whale_to_company = lookups["whale_to_company"]
        donor_info = lookups["donor_info"]
        transfer_edges_by_cycle = lookups["transfer_edges_by_cycle"]
        contrib_edges_by_cycle = lookups["contrib_edges_by_cycle"]
        ie_by_cycle = lookups["ie_by_cycle"]
        context.log.info(
            f"   Loaded {len(cmte_info_by_cycle[CYCLES[0]]):,} committees, "
            f"{len(donor_info):,} whales, {len(employer_to_company):,} employer mappings, "
            f"{len(whale_to_company):,} whale-corporate links"
        )

        # ================================================================
        # PHASE 2-3: Per-candidate computation (uses module-level helpers
        # trace_committee_sources / trace_ie_sources lifted above)
        # ================================================================

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

        batch_updates: List[Dict[str, Any]] = []

        # ProcessPool the candidate loop. Each candidate's funding_channels
        # computation is independent (~3 min total for 14K candidates serial
        # → ~10-30s parallel on a 24-core box). Set lookup state into module
        # globals BEFORE creating the pool so workers see it via Linux fork
        # copy-on-write — avoids pickling the ~hundreds of MB of edges-by-
        # cycle dicts to each worker via initargs.
        global _WORKER_LOOKUPS, _WORKER_CONFIG
        _WORKER_LOOKUPS = lookups
        _WORKER_CONFIG = config

        max_workers = int(os.environ.get('LT_MAX_WORKERS') or max(2, (os.cpu_count() or 4) - 2))
        max_workers = min(max_workers, len(candidates))
        context.log.info(f"   Computing funding_channels with {max_workers} worker processes...")

        # Force fork start method explicitly — Dagster's executor may set the
        # default to 'spawn' which doesn't inherit parent globals, defeating
        # the copy-on-write trick. fork is Linux-only; this asset is
        # container-scoped to Linux so that's fine.
        fork_ctx = _mp.get_context('fork')

        try:
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=fork_ctx) as pool:
                # chunksize batches tasks to amortize pool overhead. 50 is a
                # reasonable balance for ~14K candidates / 24 workers.
                for result in pool.map(_process_one_candidate, candidates, chunksize=50):
                    stats['candidates_processed'] += 1
                    if result is None:
                        continue
                    stats['candidates_with_funding'] += 1
                    batch_updates.append(result)
                    if len(batch_updates) >= 500:
                        db.aql.execute(
                            "FOR doc IN @batch UPDATE doc._key WITH { funding_channels: doc.funding_channels } IN candidates",
                            bind_vars={"batch": batch_updates},
                        )
                        context.log.info(f"   Updated {stats['candidates_processed']:,} candidates...")
                        batch_updates = []
            if batch_updates:
                db.aql.execute(
                    "FOR doc IN @batch UPDATE doc._key WITH { funding_channels: doc.funding_channels } IN candidates",
                    bind_vars={"batch": batch_updates},
                )
        finally:
            # Release the worker-shared state so the GC can collect it.
            _WORKER_LOOKUPS = None
            _WORKER_CONFIG = None
        
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
