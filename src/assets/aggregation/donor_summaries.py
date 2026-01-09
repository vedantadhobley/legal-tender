"""Donor Summaries Aggregation - Pre-computed donor profiles for UI and RAG.

Creates comprehensive donor profiles for whale-tier donors ($50K+):
- Total contributed across all cycles
- Breakdown by cycle
- Top recipients (committees and campaigns)
- Political lean (D/R/Mixed based on recipient parties)
- Employment history (from FEC data)

This is especially important for RAG enrichment - provides the structured
context needed before querying external sources for net worth, company info, etc.

Dependencies: donor_classification, contributed_to, transferred_to, affiliated_with
Target: Updates donors collection with summary fields (only for whale-tier donors)
"""

from typing import Dict, Any, List
from datetime import datetime

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class DonorSummariesConfig(Config):
    """Configuration for donor summaries aggregation."""
    cycles: List[str] = ["2020", "2022", "2024"]
    top_n_recipients: int = 10
    min_total: float = 50000.0  # Only summarize notable+ donors
    batch_size: int = 100


@asset(
    name="donor_summaries",
    description="Pre-computed donor profiles for whale-tier donors ($50K+).",
    group_name="aggregation",
    compute_kind="aggregation",
    deps=["donor_classification", "contributed_to", "affiliated_with"],
)
def donor_summaries_asset(
    context: AssetExecutionContext,
    config: DonorSummariesConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Compute donor profiles for whale-tier donors.
    
    For each significant donor:
    1. Breakdown contributions by cycle
    2. Identify top recipient committees
    3. Calculate political lean (% to D vs R)
    4. Track career contribution stats
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("📊 Computing donor profiles for whale-tier donors...")
        
        # Get whale-tier donors (notable = $50K+)
        donors_query = """
        FOR d IN donors
            FILTER d.total_amount >= @min_total
            FILTER d.donor_type != "conduit"  // Skip conduit PAC earmarks
            RETURN { 
                _key: d._key, 
                name: d.canonical_name,
                employer: d.canonical_employer,
                donor_type: d.donor_type,
                whale_tier: d.whale_tier,
                total_amount: d.total_amount,
                cycles: d.cycles
            }
        """
        
        donors = list(db.aql.execute(
            donors_query, 
            bind_vars={"min_total": config.min_total}
        ))
        context.log.info(f"  Found {len(donors):,} donors with ${config.min_total:,.0f}+ total contributions")
        
        stats = {
            'donors_processed': 0,
            'donors_with_recipients': 0,
            'total_contributed': 0,
            'by_tier': {'ultra': 0, 'mega': 0, 'whale': 0, 'notable': 0}
        }
        
        batch_updates = []
        
        for donor in donors:
            donor_key = donor['_key']
            donor_id = f"donors/{donor_key}"
            
            # Get contributions by cycle
            contributions_by_cycle = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._from == @donor_id
                    COLLECT cycle = e.cycle
                    AGGREGATE total = SUM(e.total_amount), count = LENGTH(1)
                    RETURN { cycle, total, count }
            """, bind_vars={"donor_id": donor_id}))
            
            cycle_breakdown = {}
            for cycle in config.cycles:
                c = next((x for x in contributions_by_cycle if x['cycle'] == cycle), None)
                cycle_breakdown[cycle] = {
                    'total': c['total'] if c else 0,
                    'count': c['count'] if c else 0
                }
            
            # Get top recipient committees
            top_committees = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._from == @donor_id
                    FOR c IN committees
                        FILTER c._id == e._to
                        COLLECT cmte_key = c._key, cmte_name = c.CMTE_NM,
                                cmte_type = c.terminal_type
                        AGGREGATE total = SUM(e.total_amount)
                        SORT total DESC
                        LIMIT @limit
                        RETURN { cmte_id: cmte_key, name: cmte_name, amount: total, type: cmte_type }
            """, bind_vars={"donor_id": donor_id, "limit": config.top_n_recipients}))
            
            # Calculate political lean by looking at committee affiliations
            # Get parties of candidates supported through contributions
            party_breakdown = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._from == @donor_id
                    FOR cmte IN committees
                        FILTER cmte._id == e._to
                        FOR cand, aff IN OUTBOUND cmte affiliated_with
                            FILTER cand.CAND_PTY_AFFILIATION != null
                            COLLECT party = cand.CAND_PTY_AFFILIATION
                            AGGREGATE total = SUM(e.total_amount)
                            RETURN { party, total }
            """, bind_vars={"donor_id": donor_id}))
            
            # Calculate lean
            dem_total = sum(p['total'] for p in party_breakdown if p['party'] in ['DEM', 'D'])
            rep_total = sum(p['total'] for p in party_breakdown if p['party'] in ['REP', 'R'])
            other_total = sum(p['total'] for p in party_breakdown if p['party'] not in ['DEM', 'D', 'REP', 'R'])
            party_total = dem_total + rep_total + other_total
            
            if party_total > 0:
                political_lean = {
                    'dem_pct': round(dem_total / party_total * 100, 1),
                    'rep_pct': round(rep_total / party_total * 100, 1),
                    'other_pct': round(other_total / party_total * 100, 1),
                    'lean': 'DEM' if dem_total > rep_total * 1.5 else ('REP' if rep_total > dem_total * 1.5 else 'MIXED')
                }
            else:
                political_lean = {
                    'dem_pct': 0,
                    'rep_pct': 0,
                    'other_pct': 0,
                    'lean': 'UNKNOWN'
                }
            
            # Get direct candidate support (through affiliated committees)
            candidates_supported = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._from == @donor_id
                    FOR cmte IN committees
                        FILTER cmte._id == e._to
                        FOR cand, aff IN OUTBOUND cmte affiliated_with
                            COLLECT cand_key = cand._key, cand_name = cand.CAND_NAME,
                                    party = cand.CAND_PTY_AFFILIATION, office = cand.CAND_OFFICE
                            AGGREGATE total = SUM(e.total_amount)
                            SORT total DESC
                            LIMIT @limit
                            RETURN { cand_id: cand_key, name: cand_name, party, office, amount: total }
            """, bind_vars={"donor_id": donor_id, "limit": config.top_n_recipients}))
            
            summary = {
                'by_cycle': cycle_breakdown,
                'lifetime_total': donor['total_amount'],
                'top_committees': top_committees,
                'top_candidates': candidates_supported,
                'political_lean': political_lean,
                'committee_count': len(top_committees),
                'summary_updated_at': datetime.now().isoformat(),
            }
            
            batch_updates.append({
                '_key': donor_key,
                'funding_summary': summary
            })
            
            stats['donors_processed'] += 1
            if top_committees:
                stats['donors_with_recipients'] += 1
            stats['total_contributed'] += donor['total_amount']
            
            tier = donor.get('whale_tier')
            if tier and tier in stats['by_tier']:
                stats['by_tier'][tier] += 1
            
            if len(batch_updates) >= config.batch_size:
                _upsert_donor_summaries(db, batch_updates)
                context.log.info(f"  Processed {stats['donors_processed']:,} donors...")
                batch_updates = []
        
        # Final batch
        if batch_updates:
            _upsert_donor_summaries(db, batch_updates)
        
        # Create index on political lean
        try:
            db.collection("donors").add_persistent_index(
                fields=["funding_summary.political_lean.lean"],
                sparse=True
            )
        except Exception:
            pass
        
        context.log.info(f"✅ Computed summaries for {stats['donors_processed']:,} donors")
        context.log.info(f"   By tier: ultra={stats['by_tier']['ultra']}, mega={stats['by_tier']['mega']}, whale={stats['by_tier']['whale']}, notable={stats['by_tier']['notable']}")
        context.log.info(f"   Total contributed: ${stats['total_contributed']:,.0f}")
        
        return Output(
            value=stats,
            metadata={
                "donors_processed": stats['donors_processed'],
                "donors_with_recipients": stats['donors_with_recipients'],
                "total_contributed": MetadataValue.float(float(stats['total_contributed'])),
                "by_tier": MetadataValue.json(stats['by_tier']),
            }
        )


def _upsert_donor_summaries(db, updates: List[Dict]):
    """Upsert funding summaries to donors collection."""
    aql = """
    FOR doc IN @updates
        UPDATE { _key: doc._key } WITH { funding_summary: doc.funding_summary } IN donors
    """
    db.aql.execute(aql, bind_vars={"updates": updates})
