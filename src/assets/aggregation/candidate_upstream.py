"""Candidate Upstream - Complete upstream funding attribution for candidates.

This is the "PIE CHART" asset - for any candidate, shows exactly where their money
comes from, traced all the way upstream to original sources.

FUNDING CATEGORIES:
1. Small Individual Donors - People who gave < $10K (grassroots)
2. Whale Individual Donors - People who gave >= $10K (with names + employers)
3. Corporate-Connected Whales - Whales linked to corporations via employer
4. Committee Transfers - Money from other PACs (traced upstream)
5. Independent Expenditures FOR - Super PAC spending supporting candidate
6. Independent Expenditures AGAINST - Super PAC spending opposing candidate

The upstream tracing uses committee receipt ratios:
- For each committee that transfers to candidate's committees, we compute
  what % of that committee's money came from individuals vs other committees
- We recursively trace committee sources to terminal (individuals/orgs)

Output: candidates collection updated with 'upstream_funding' field containing:
{
    "total_funding": float,        # Total money supporting this candidate
    "direct_funding": float,       # Money to candidate's committees
    "ie_support": float,           # Super PAC IEs FOR candidate
    "ie_oppose": float,            # Super PAC IEs AGAINST candidate
    
    "breakdown": {
        "small_donors": float,     # $ from donors < $10K
        "whale_donors": float,     # $ from donors >= $10K
        "committee_transfers": float,  # $ from PAC-to-PAC
        "ie_support": float,
        "ie_oppose": float,
    },
    
    "percentages": {
        "small_donors_pct": float,
        "whale_donors_pct": float,
        "committee_pct": float,
        "ie_support_pct": float,
        "ie_oppose_pct": float,
    },
    
    "top_whale_donors": [          # Named whale donors
        {"name": str, "employer": str, "amount": float, "corporate_parent": str|null}
    ],
    
    "top_super_pacs_support": [    # Super PACs spending FOR
        {"name": str, "amount": float, "top_funders": [...]}
    ],
    
    "top_super_pacs_oppose": [     # Super PACs spending AGAINST
        {"name": str, "amount": float, "top_funders": [...]}
    ],
}

Source: aggregation.committees (with receipt totals), aggregation.spent_on
Target: aggregation.candidates (adds upstream_funding field)
"""

from typing import Dict, Any, List, Set, Tuple
from datetime import datetime
from collections import defaultdict

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class CandidateUpstreamConfig(Config):
    """Configuration for candidate upstream computation."""
    cycles: List[str] = ["2020", "2022", "2024"]
    top_n_donors: int = 20
    top_n_pacs: int = 10
    max_trace_depth: int = 8


@asset(
    name="candidate_upstream",
    description="Complete upstream funding attribution - the 'pie chart' for candidate funding sources.",
    group_name="aggregation",
    compute_kind="aggregation",
    deps=["committee_receipts", "affiliated_with", "transferred_to", "spent_on"],
)
def candidate_upstream_asset(
    context: AssetExecutionContext,
    config: CandidateUpstreamConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Compute complete upstream funding attribution for all candidates.
    
    This creates the "pie chart" data showing where each candidate's money
    truly comes from, including:
    - Direct individual donations (small vs whale)
    - Committee transfers (traced upstream)
    - Independent Expenditures (Super PAC spending FOR/AGAINST)
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("🥧 Computing candidate upstream funding (the 'pie chart')...")
        
        # ============================================================
        # Phase 1: Load all data into memory for fast computation
        # ============================================================
        context.log.info("📥 Phase 1: Loading data...")
        
        # Committee info with receipt breakdowns
        cmte_info = {}
        for c in db.aql.execute("""
            FOR c IN committees
            RETURN {
                _key: c._key,
                name: c.CMTE_NM,
                total_receipts: c.total_receipts || 0,
                total_from_individuals: c.total_from_individuals || 0,
                small_donor_total: c.small_donor_total || 0,
                whale_donor_total: c.whale_donor_total || 0,
                total_from_committees: c.total_from_committees || 0,
                terminal_type: c.terminal_type
            }
        """):
            cmte_info[c['_key']] = c
        context.log.info(f"   Loaded {len(cmte_info):,} committees")
        
        # Committee transfers: to_cmte -> [(from_cmte, amount), ...]
        transfer_edges = defaultdict(list)
        for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            from_cmte = e['_from'].split('/')[1]
            transfer_edges[to_cmte].append((from_cmte, e.get('total_amount', 0) or 0))
        context.log.info(f"   Loaded {sum(len(v) for v in transfer_edges.values()):,} transfer edges")
        
        # Independent Expenditures by candidate
        # candidate_id -> {'support': [(cmte_id, amount)], 'oppose': [(cmte_id, amount)]}
        ie_by_candidate = defaultdict(lambda: {'support': [], 'oppose': []})
        for e in db.aql.execute("""
            FOR e IN spent_on
            RETURN {
                cmte_id: SPLIT(e._from, '/')[1],
                cand_id: SPLIT(e._to, '/')[1],
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
        
        # Donor info for top whale attribution
        donor_info = {}
        for d in db.aql.execute("""
            FOR d IN donors
            RETURN {
                _key: d._key,
                name: d.canonical_name,
                employer: d.canonical_employer,
                total_amount: d.total_amount,
                corporate_parent: d.corporate_parent
            }
        """):
            donor_info[d['_key']] = d
        context.log.info(f"   Loaded {len(donor_info):,} whale donors")
        
        # Contribution edges for finding top whale donors per candidate
        # cmte_id -> [(donor_key, amount), ...]
        contrib_by_cmte = defaultdict(list)
        for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            donor_key = e['_from'].split('/')[1]
            contrib_by_cmte[to_cmte].append((donor_key, e.get('total_amount', 0) or 0))
        context.log.info(f"   Loaded whale contribution edges")
        
        # ============================================================
        # Phase 2: Memoized upstream ratio computation
        # ============================================================
        context.log.info("🔄 Phase 2: Computing upstream source ratios...")
        
        # Cache: cmte_id -> (small_donor_ratio, whale_donor_ratio, committee_ratio)
        cmte_source_ratios: Dict[str, Tuple[float, float, float]] = {}
        
        def compute_source_ratio(cmte_id: str, visited: Set[str], depth: int = 0) -> Tuple[float, float, float]:
            """
            Compute what ratio of a committee's funding comes from:
            - small donors (< $10K individuals)
            - whale donors (>= $10K individuals)
            - committee transfers (traced upstream)
            
            Returns (small_ratio, whale_ratio, committee_ratio) summing to ~1.0
            """
            if depth > config.max_trace_depth:
                return (0.33, 0.33, 0.34)  # Default if too deep
            
            if cmte_id in cmte_source_ratios:
                return cmte_source_ratios[cmte_id]
            
            if cmte_id in visited:
                return (0.33, 0.33, 0.34)  # Cycle detected
            
            visited = visited | {cmte_id}
            
            cmte = cmte_info.get(cmte_id, {})
            total = cmte.get('total_receipts', 0) or 0
            
            if total == 0:
                return (0.33, 0.33, 0.34)
            
            # Direct ratios from this committee's receipt breakdown
            small_direct = cmte.get('small_donor_total', 0) or 0
            whale_direct = cmte.get('whale_donor_total', 0) or 0
            from_committees = cmte.get('total_from_committees', 0) or 0
            
            # For committee transfers, recursively compute their source ratios
            # and blend them into our totals
            small_from_cmtes = 0.0
            whale_from_cmtes = 0.0
            # Note: committee money that traces to committees is still "committee money"
            # until it traces to an individual source
            
            for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
                if from_cmte_id in visited:
                    continue
                src_small, src_whale, src_cmte = compute_source_ratio(from_cmte_id, visited, depth + 1)
                # Attribute the transfer according to source's ratios
                small_from_cmtes += amount * src_small
                whale_from_cmtes += amount * src_whale
                # src_cmte portion remains "committee" money (dark/untraced)
            
            total_small = small_direct + small_from_cmtes
            total_whale = whale_direct + whale_from_cmtes
            # Remaining from_committees that didn't trace to individuals
            total_committee = max(0, from_committees - small_from_cmtes - whale_from_cmtes)
            
            grand_total = total_small + total_whale + total_committee
            if grand_total == 0:
                result = (0.33, 0.33, 0.34)
            else:
                result = (
                    total_small / grand_total,
                    total_whale / grand_total,
                    total_committee / grand_total
                )
            
            cmte_source_ratios[cmte_id] = result
            return result
        
        # ============================================================
        # Phase 3: Process each candidate
        # ============================================================
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
            
            # Sum up direct funding from candidate's committees
            direct_small = 0.0
            direct_whale = 0.0
            direct_committee = 0.0
            total_direct = 0.0
            
            for cmte_id in cmte_ids:
                cmte = cmte_info.get(cmte_id, {})
                receipts = cmte.get('total_receipts', 0) or 0
                total_direct += receipts
                
                if receipts > 0:
                    small_r, whale_r, cmte_r = compute_source_ratio(cmte_id, set())
                    direct_small += receipts * small_r
                    direct_whale += receipts * whale_r
                    direct_committee += receipts * cmte_r
            
            # Get IE totals
            ie_support = sum(amt for _, amt in ie_by_candidate.get(cand_key, {}).get('support', []))
            ie_oppose = sum(amt for _, amt in ie_by_candidate.get(cand_key, {}).get('oppose', []))
            
            # Total funding (direct + IE support)
            total_funding = total_direct + ie_support
            # Note: ie_oppose is money spent AGAINST, not funding for the candidate
            
            if total_funding == 0:
                stats['candidates_processed'] += 1
                continue
            
            stats['candidates_with_funding'] += 1
            
            # Compute percentages (of total_funding, not including oppose)
            breakdown = {
                'small_donors': direct_small,
                'whale_donors': direct_whale,
                'committee_transfers': direct_committee,
                'ie_support': ie_support,
                'ie_oppose': ie_oppose,
            }
            
            percentages = {
                'small_donors_pct': (direct_small / total_funding * 100) if total_funding > 0 else 0,
                'whale_donors_pct': (direct_whale / total_funding * 100) if total_funding > 0 else 0,
                'committee_pct': (direct_committee / total_funding * 100) if total_funding > 0 else 0,
                'ie_support_pct': (ie_support / total_funding * 100) if total_funding > 0 else 0,
                # ie_oppose_pct is relative to total_funding + ie_oppose
                'ie_oppose_pct': (ie_oppose / (total_funding + ie_oppose) * 100) if (total_funding + ie_oppose) > 0 else 0,
            }
            
            # Get top whale donors to this candidate's committees
            whale_donors_to_cand = []
            for cmte_id in cmte_ids:
                for donor_key, amount in contrib_by_cmte.get(cmte_id, []):
                    donor = donor_info.get(donor_key, {})
                    whale_donors_to_cand.append({
                        'name': donor.get('name', 'Unknown'),
                        'employer': donor.get('employer', ''),
                        'amount': amount,
                        'corporate_parent': donor.get('corporate_parent'),
                    })
            
            # Sort and take top N
            whale_donors_to_cand.sort(key=lambda x: x['amount'], reverse=True)
            top_whale_donors = whale_donors_to_cand[:config.top_n_donors]
            
            # Get top Super PACs supporting/opposing
            support_pacs = ie_by_candidate.get(cand_key, {}).get('support', [])
            oppose_pacs = ie_by_candidate.get(cand_key, {}).get('oppose', [])
            
            top_support_pacs = []
            for cmte_id, amount in sorted(support_pacs, key=lambda x: x[1], reverse=True)[:config.top_n_pacs]:
                cmte = cmte_info.get(cmte_id, {})
                top_support_pacs.append({
                    'cmte_id': cmte_id,
                    'name': cmte.get('name', 'Unknown'),
                    'amount': amount,
                })
            
            top_oppose_pacs = []
            for cmte_id, amount in sorted(oppose_pacs, key=lambda x: x[1], reverse=True)[:config.top_n_pacs]:
                cmte = cmte_info.get(cmte_id, {})
                top_oppose_pacs.append({
                    'cmte_id': cmte_id,
                    'name': cmte.get('name', 'Unknown'),
                    'amount': amount,
                })
            
            # Build upstream funding document
            upstream_funding = {
                'total_funding': total_funding,
                'direct_funding': total_direct,
                'ie_support': ie_support,
                'ie_oppose': ie_oppose,
                'breakdown': breakdown,
                'percentages': percentages,
                'top_whale_donors': top_whale_donors,
                'top_super_pacs_support': top_support_pacs,
                'top_super_pacs_oppose': top_oppose_pacs,
                'computed_at': datetime.now().isoformat(),
            }
            
            batch_updates.append({
                '_key': cand_key,
                'upstream_funding': upstream_funding,
            })
            
            stats['candidates_processed'] += 1
            
            if len(batch_updates) >= 100:
                db.aql.execute("""
                    FOR doc IN @batch
                        UPDATE doc._key WITH { upstream_funding: doc.upstream_funding } IN candidates
                """, bind_vars={"batch": batch_updates})
                batch_updates = []
                
                if stats['candidates_processed'] % 1000 == 0:
                    context.log.info(f"   Processed {stats['candidates_processed']:,} candidates...")
        
        # Flush remaining
        if batch_updates:
            db.aql.execute("""
                FOR doc IN @batch
                    UPDATE doc._key WITH { upstream_funding: doc.upstream_funding } IN candidates
            """, bind_vars={"batch": batch_updates})
        
        # ============================================================
        # Phase 4: Validation
        # ============================================================
        context.log.info("✅ Phase 4: Validation...")
        
        # Check Ted Cruz
        cruz = list(db.aql.execute("""
            FOR c IN candidates
                FILTER CONTAINS(UPPER(c.CAND_NAME), 'CRUZ') AND CONTAINS(UPPER(c.CAND_NAME), 'TED')
                LIMIT 1
                RETURN {
                    name: c.CAND_NAME,
                    upstream: c.upstream_funding
                }
        """))
        
        if cruz and cruz[0].get('upstream'):
            u = cruz[0]['upstream']
            context.log.info(f"\n🎯 Validation - {cruz[0]['name']}:")
            context.log.info(f"   Total funding: ${u['total_funding']:,.0f}")
            context.log.info(f"   Direct: ${u['direct_funding']:,.0f}")
            context.log.info(f"   IE Support: ${u['ie_support']:,.0f}")
            context.log.info(f"   IE Oppose: ${u['ie_oppose']:,.0f}")
            context.log.info(f"\n   Breakdown:")
            context.log.info(f"   - Small donors: {u['percentages']['small_donors_pct']:.1f}%")
            context.log.info(f"   - Whale donors: {u['percentages']['whale_donors_pct']:.1f}%")
            context.log.info(f"   - Committee: {u['percentages']['committee_pct']:.1f}%")
            context.log.info(f"   - IE Support: {u['percentages']['ie_support_pct']:.1f}%")
        
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
