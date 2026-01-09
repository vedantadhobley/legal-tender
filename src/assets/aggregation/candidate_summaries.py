"""Candidate Summaries Aggregation - Pre-computed funding data for UI and RAG.

RECURSIVE UPSTREAM TRACING - traces money to ORIGINAL terminal sources.

Money Flow Example:
  Individual → ActBlue → Bernie 2020 → Friends of Bernie Sanders
  
Without upstream tracing: counts as "committee money" (WRONG!)
With upstream tracing: traces back to original individual (CORRECT!)

Terminal Types (stop tracing, attribute to this source):
- individuals (via contributed_to edges) → "individual" money
- corporation, labor_union, trade_association → "organization" money
- cooperative, ideological → "organization" money

Passthrough Types (keep tracing upstream through these):
- passthrough (JFCs, party committees, conduits like ActBlue/WinRed)
- campaign (candidate's own committees - trace to THEIR sources)
- super_pac_unclassified (trace to their donors)

Algorithm: Memoized recursive computation of source ratios AND funding distance.
For each committee, compute what RATIO of its funding ultimately came from
individuals vs organizations, PLUS the weighted average hop distance.

Computes:
- Funding by cycle with TRUE original sources (individual vs org)
- Top individual donors (direct to candidate committees)
- Top organizational donors (corporations, unions - terminal orgs)
- Top committee intermediaries (passthroughs - for transparency)
- Whale donor counts
- TRUE individual_pct / organization_pct based on recursive tracing
- funding_distance: weighted average hops from terminal source to candidate
  - Formula: Σ(amount × hops) / Σ(amount)
  - Lower = more direct access to donors
  - Higher = more intermediary committees involved

Dependencies: committee_classification, donor_classification, committee_financials
Target: Updates candidates collection with funding_summary field
"""

from typing import Dict, Any, List, Set, Tuple
from datetime import datetime
from collections import defaultdict

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


# Terminal organization types - these are the END of the money trail (organizational money)
TERMINAL_ORG_TYPES = {'corporation', 'labor_union', 'trade_association', 'cooperative', 'ideological'}


class CandidateSummariesConfig(Config):
    """Configuration for candidate summaries aggregation."""
    cycles: List[str] = ["2020", "2022", "2024"]
    top_n_donors: int = 10
    top_n_committees: int = 10
    batch_size: int = 100
    max_trace_depth: int = 8  # Prevent infinite recursion


@asset(
    name="candidate_summaries",
    description="Pre-computed candidate funding summaries with RECURSIVE upstream tracing to original sources.",
    group_name="aggregation",
    compute_kind="aggregation",
    deps=["committee_classification", "donor_classification", "committee_financials", "affiliated_with", "contributed_to", "transferred_to"],
)
def candidate_summaries_asset(
    context: AssetExecutionContext,
    config: CandidateSummariesConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Compute candidate funding summaries with RECURSIVE upstream tracing.
    
    Uses memoized source ratio computation:
    1. For each committee, compute (individual_ratio, org_ratio)
    2. Cache results to avoid recomputation
    3. Apply ratios to actual receipts for true attribution
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("📊 Computing candidate funding summaries with RECURSIVE upstream tracing...")
        
        # ============================================================
        # Phase 1: Pre-load all data into memory for fast tracing
        # ============================================================
        context.log.info("  Phase 1: Loading data into memory...")
        
        # Committee info
        cmte_info = {}
        for c in db.aql.execute("""
            FOR c IN committees 
            RETURN { 
                _key: c._key, 
                name: c.CMTE_NM, 
                terminal_type: c.terminal_type,
                total_receipts: c.total_receipts
            }
        """):
            cmte_info[c['_key']] = c
        context.log.info(f"    Loaded {len(cmte_info):,} committees")
        
        # Individual contributions: cmte_id -> [(donor_key, amount), ...]
        contrib_edges = defaultdict(list)
        for e in db.aql.execute("FOR e IN contributed_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            from_donor = e['_from'].split('/')[1]
            contrib_edges[to_cmte].append((from_donor, e.get('total_amount', 0) or 0))
        context.log.info(f"    Loaded {sum(len(v) for v in contrib_edges.values()):,} contribution edges")
        
        # Committee transfers: to_cmte -> [(from_cmte, amount), ...]
        transfer_edges = defaultdict(list)
        for e in db.aql.execute("FOR e IN transferred_to RETURN e"):
            to_cmte = e['_to'].split('/')[1]
            from_cmte = e['_from'].split('/')[1]
            transfer_edges[to_cmte].append((from_cmte, e.get('total_amount', 0) or 0))
        context.log.info(f"    Loaded {sum(len(v) for v in transfer_edges.values()):,} transfer edges")
        
        # Donor info
        donor_info = {}
        for d in db.aql.execute("""
            FOR d IN donors 
            RETURN { 
                _key: d._key, 
                name: d.canonical_name,
                donor_type: d.donor_type,
                whale_tier: d.whale_tier
            }
        """):
            donor_info[d['_key']] = d
        context.log.info(f"    Loaded {len(donor_info):,} donors")
        
        # ============================================================
        # Phase 2: Compute source ratios AND funding distance with memoization
        # ============================================================
        context.log.info("  Phase 2: Computing upstream source ratios + funding distance (memoized)...")
        
        # Cache: cmte_id -> (individual_ratio, org_ratio, weighted_avg_hops)
        # weighted_avg_hops = average distance from terminal source to this committee
        cmte_source_ratios: Dict[str, Tuple[float, float, float]] = {}
        
        def compute_source_ratio(cmte_id: str, visited: Set[str], depth: int = 0) -> Tuple[float, float, float]:
            """
            Compute what RATIO of a committee's funding comes from individuals vs orgs,
            AND the weighted average hop distance from terminal sources.
            
            Returns (individual_ratio, org_ratio, weighted_avg_hops)
            - individual_ratio + org_ratio sum to ~1.0
            - weighted_avg_hops: Σ(amount × hops) / Σ(amount) for this committee's sources
              - 0 = this committee IS a terminal source
              - 1 = all money comes directly from terminals (individuals or terminal orgs)
              - 2+ = money flows through intermediary committees
            """
            if depth > config.max_trace_depth:
                return (0.5, 0.5, float(depth))  # Default if too deep
            
            if cmte_id in cmte_source_ratios:
                return cmte_source_ratios[cmte_id]
            
            if cmte_id in visited:
                return (0.5, 0.5, float(depth))  # Cycle detected
            
            visited = visited | {cmte_id}
            
            cmte = cmte_info.get(cmte_id, {})
            terminal_type = cmte.get('terminal_type')
            
            # If this is a terminal org, it's 100% organizational, 0 hops from itself
            if terminal_type in TERMINAL_ORG_TYPES:
                cmte_source_ratios[cmte_id] = (0.0, 1.0, 0.0)
                return (0.0, 1.0, 0.0)
            
            total_receipts = cmte.get('total_receipts', 0) or 0
            if total_receipts == 0:
                return (0.5, 0.5, 1.0)
            
            # Sum direct individual contributions (excluding conduits)
            # These are 1 hop from terminal (the individual)
            ind_direct = 0
            for donor_key, amount in contrib_edges.get(cmte_id, []):
                donor = donor_info.get(donor_key, {})
                if donor.get('donor_type') != 'conduit':
                    ind_direct += amount
            
            # For committee transfers, recursively compute their source ratios
            ind_from_cmtes = 0
            org_from_cmtes = 0
            # Track weighted hop sums: Σ(amount × hops_to_terminal)
            weighted_hop_sum = 0.0
            weighted_hop_total = 0.0
            
            # Individual contributions are 1 hop from terminal (the individual donor)
            weighted_hop_sum += ind_direct * 1.0
            weighted_hop_total += ind_direct
            
            for from_cmte_id, amount in transfer_edges.get(cmte_id, []):
                from_cmte = cmte_info.get(from_cmte_id, {})
                from_terminal_type = from_cmte.get('terminal_type')
                
                if from_terminal_type in TERMINAL_ORG_TYPES:
                    org_from_cmtes += amount
                    # Terminal org transfer = 1 hop from terminal
                    weighted_hop_sum += amount * 1.0
                    weighted_hop_total += amount
                else:
                    src_ind_ratio, src_org_ratio, src_avg_hops = compute_source_ratio(from_cmte_id, visited, depth + 1)
                    ind_from_cmtes += amount * src_ind_ratio
                    org_from_cmtes += amount * src_org_ratio
                    # Add 1 hop for this transfer, plus the upstream hops
                    weighted_hop_sum += amount * (src_avg_hops + 1.0)
                    weighted_hop_total += amount
            
            total_ind = ind_direct + ind_from_cmtes
            total_org = org_from_cmtes
            total = total_ind + total_org
            
            if total == 0:
                result = (0.5, 0.5, 1.0)
            else:
                avg_hops = weighted_hop_sum / weighted_hop_total if weighted_hop_total > 0 else 1.0
                result = (total_ind / total, total_org / total, avg_hops)
            
            cmte_source_ratios[cmte_id] = result
            return result
        
        # ============================================================
        # Phase 3: Process candidates
        # ============================================================
        context.log.info("  Phase 3: Processing candidates...")
        
        candidates_query = """
        FOR c IN candidates
            LET affiliated_cmtes = (
                FOR v, e IN INBOUND c affiliated_with
                RETURN { cmte_id: v._key, cycle: e.cycle }
            )
            FILTER LENGTH(affiliated_cmtes) > 0
            RETURN { 
                _key: c._key, 
                name: c.CAND_NAME,
                party: c.CAND_PTY_AFFILIATION,
                office: c.CAND_OFFICE,
                state: c.CAND_OFFICE_ST,
                affiliated_cmtes: affiliated_cmtes
            }
        """
        
        candidates = list(db.aql.execute(candidates_query))
        context.log.info(f"    Found {len(candidates):,} candidates with committee affiliations")
        
        stats = {
            'candidates_processed': 0,
            'candidates_with_funding': 0,
            'total_individual_traced': 0,
            'total_org_traced': 0,
        }
        
        batch_updates = []
        
        for cand in candidates:
            cand_key = cand['_key']
            cmte_ids = [c['cmte_id'] for c in cand['affiliated_cmtes']]
            
            # Compute true source breakdown for this candidate
            # Also compute weighted average funding distance
            total_individual = 0
            total_org = 0
            weighted_hop_sum = 0.0
            weighted_hop_total = 0.0
            
            for cmte_id in cmte_ids:
                cmte = cmte_info.get(cmte_id, {})
                total_receipts = cmte.get('total_receipts', 0) or 0
                if total_receipts == 0:
                    continue
                
                ind_ratio, org_ratio, avg_hops = compute_source_ratio(cmte_id, set())
                total_individual += total_receipts * ind_ratio
                total_org += total_receipts * org_ratio
                
                # Candidate receives from committee = +1 hop from committee's sources
                weighted_hop_sum += total_receipts * (avg_hops + 1.0)
                weighted_hop_total += total_receipts
            
            total_raised = total_individual + total_org
            
            if total_raised == 0:
                stats['candidates_processed'] += 1
                continue
            
            # Get top DIRECT individual donors (for display purposes)
            # These are people who donated directly to candidate committees
            top_individuals_query = """
            FOR cmte_id IN @cmte_ids
                FOR e IN contributed_to
                    FILTER e._to == CONCAT('committees/', cmte_id)
                    FOR d IN donors
                        FILTER d._id == e._from
                        FILTER d.donor_type != "conduit"
                        COLLECT donor_key = d._key, donor_name = d.canonical_name, 
                                donor_type = d.donor_type, whale_tier = d.whale_tier
                        AGGREGATE total = SUM(e.total_amount)
                        SORT total DESC
                        LIMIT @top_n
                        RETURN { 
                            name: donor_name, 
                            amount: total,
                            donor_type: donor_type,
                            whale_tier: whale_tier
                        }
            """
            top_individuals = list(db.aql.execute(
                top_individuals_query,
                bind_vars={"cmte_ids": cmte_ids, "top_n": config.top_n_donors}
            ))
            
            # Get top committee intermediaries (direct transfers)
            top_committees_query = """
            FOR cmte_id IN @cmte_ids
                FOR e IN transferred_to
                    FILTER e._to == CONCAT('committees/', cmte_id)
                    FOR c IN committees
                        FILTER c._id == e._from
                        COLLECT cmte_key = c._key, cmte_name = c.CMTE_NM,
                                terminal_type = c.terminal_type
                        AGGREGATE total = SUM(e.total_amount)
                        SORT total DESC
                        LIMIT @top_n
                        RETURN {
                            name: cmte_name,
                            cmte_id: cmte_key,
                            amount: total,
                            terminal_type: terminal_type
                        }
            """
            top_committees = list(db.aql.execute(
                top_committees_query,
                bind_vars={"cmte_ids": cmte_ids, "top_n": config.top_n_committees}
            ))
            
            # Count whale tiers (direct donors)
            whale_counts_query = """
            FOR cmte_id IN @cmte_ids
                FOR e IN contributed_to
                    FILTER e._to == CONCAT('committees/', cmte_id)
                    FOR d IN donors
                        FILTER d._id == e._from AND d.whale_tier != null
                        COLLECT tier = d.whale_tier WITH COUNT INTO cnt
                        RETURN { tier: tier, count: cnt }
            """
            whale_counts = list(db.aql.execute(
                whale_counts_query,
                bind_vars={"cmte_ids": cmte_ids}
            ))
            whale_summary = {w['tier']: w['count'] for w in whale_counts}
            
            # Calculate TRUE funding source percentages
            funding_sources = {
                'individual_pct': round(total_individual / total_raised * 100, 1) if total_raised > 0 else 0,
                'organization_pct': round(total_org / total_raised * 100, 1) if total_raised > 0 else 0,
                'individual_total': total_individual,
                'organization_total': total_org,
            }
            
            # Calculate funding_distance: weighted average hops from terminal source to candidate
            # Lower = more direct access (good for grassroots transparency)
            # Higher = more intermediary committees (potential for obscured sources)
            funding_distance = round(weighted_hop_sum / weighted_hop_total, 2) if weighted_hop_total > 0 else 1.0
            
            # Build summary document
            summary = {
                'total_raised': total_raised,
                'funding_sources': funding_sources,
                'funding_distance': funding_distance,  # Weighted avg hops from terminal source
                'top_individual_donors': top_individuals[:config.top_n_donors],
                'top_committee_intermediaries': top_committees[:config.top_n_committees],
                'whale_donor_counts': whale_summary,
                'summary_updated_at': datetime.now().isoformat(),
                'trace_method': 'recursive_upstream_memoized',
            }
            
            batch_updates.append({
                '_key': cand_key,
                'funding_summary': summary
            })
            
            stats['candidates_with_funding'] += 1
            stats['total_individual_traced'] += total_individual
            stats['total_org_traced'] += total_org
            stats['candidates_processed'] += 1
            
            # Process in batches
            if len(batch_updates) >= config.batch_size:
                _upsert_summaries(db, batch_updates)
                context.log.info(f"    Processed {stats['candidates_processed']:,} candidates...")
                batch_updates = []
        
        # Final batch
        if batch_updates:
            _upsert_summaries(db, batch_updates)
        
        # Create index
        try:
            db.collection("candidates").add_persistent_index(
                fields=["funding_summary.total_raised"],
                sparse=True
            )
        except Exception:
            pass
        
        context.log.info(f"✅ Computed summaries for {stats['candidates_with_funding']:,} candidates")
        context.log.info(f"   Total INDIVIDUAL (upstream traced): ${stats['total_individual_traced']:,.0f}")
        context.log.info(f"   Total ORGANIZATION (terminal orgs): ${stats['total_org_traced']:,.0f}")
        context.log.info(f"   Committees with cached ratios: {len(cmte_source_ratios):,}")
        
        return Output(
            value=stats,
            metadata={
                "candidates_processed": stats['candidates_processed'],
                "candidates_with_funding": stats['candidates_with_funding'],
                "total_individual_traced": MetadataValue.float(float(stats['total_individual_traced'])),
                "total_org_traced": MetadataValue.float(float(stats['total_org_traced'])),
                "committees_cached": MetadataValue.int(len(cmte_source_ratios)),
            }
        )


def _upsert_summaries(db, updates: List[Dict]):
    """Upsert funding summaries to candidates collection."""
    aql = """
    FOR doc IN @updates
        UPDATE { _key: doc._key } WITH { funding_summary: doc.funding_summary } IN candidates
    """
    db.aql.execute(aql, bind_vars={"updates": updates})
