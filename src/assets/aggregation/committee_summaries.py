"""Committee Summaries Aggregation - Pre-computed committee data for UI and RAG.

Similar to candidate_summaries, this pre-computes committee-level data:
- Total received and disbursed
- Top individual donors
- Top committee sources (for passthrough committees)
- Downstream recipients (campaigns/PACs they fund)
- Cycle breakdown

This enables fast UI queries for committee profiles.

Dependencies: committee_classification, committee_financials, contributed_to, transferred_to
Target: Updates committees collection with summary fields
"""

from typing import Dict, Any, List
from datetime import datetime

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class CommitteeSummariesConfig(Config):
    """Configuration for committee summaries aggregation."""
    cycles: List[str] = ["2020", "2022", "2024"]
    top_n_donors: int = 10
    top_n_recipients: int = 10
    min_receipts: float = 100000.0  # Only summarize committees with $100K+ receipts
    batch_size: int = 100


@asset(
    name="committee_summaries",
    description="Pre-computed committee summaries for UI and RAG queries.",
    group_name="aggregation",
    compute_kind="aggregation",
    deps=["committee_classification", "committee_financials", "donor_classification", "contributed_to", "transferred_to"],
)
def committee_summaries_asset(
    context: AssetExecutionContext,
    config: CommitteeSummariesConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Compute committee funding summaries from graph traversal.
    
    For each significant committee:
    1. Aggregate incoming contributions by cycle and source type
    2. Identify top individual donors
    3. Identify top committee sources  
    4. Track downstream recipients
    """
    
    with arango.get_client() as client:
        db = client.db("aggregation", username=arango.username, password=arango.password)
        
        context.log.info("📊 Computing committee funding summaries...")
        
        # Get significant committees (already have total_receipts from committee_financials)
        committees_query = """
        FOR c IN committees
            FILTER c.total_receipts >= @min_receipts
            RETURN { 
                _key: c._key, 
                name: c.CMTE_NM,
                terminal_type: c.terminal_type,
                total_receipts: c.total_receipts,
                total_disbursed: c.total_disbursed
            }
        """
        
        committees = list(db.aql.execute(
            committees_query, 
            bind_vars={"min_receipts": config.min_receipts}
        ))
        context.log.info(f"  Found {len(committees):,} committees with ${config.min_receipts:,.0f}+ receipts")
        
        stats = {
            'committees_processed': 0,
            'committees_with_donors': 0,
            'total_receipts_summarized': 0,
        }
        
        batch_updates = []
        
        for cmte in committees:
            cmte_key = cmte['_key']
            cmte_id = f"committees/{cmte_key}"
            
            # Get individual contributions by cycle
            individual_funding = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._to == @cmte_id
                    COLLECT cycle = e.cycle
                    AGGREGATE total = SUM(e.total_amount), count = LENGTH(1)
                    RETURN { cycle, total, count }
            """, bind_vars={"cmte_id": cmte_id}))
            
            # Get committee transfers received by cycle
            committee_funding = list(db.aql.execute("""
                FOR e IN transferred_to
                    FILTER e._to == @cmte_id
                    COLLECT cycle = e.cycle
                    AGGREGATE total = SUM(e.total_amount), count = LENGTH(1)
                    RETURN { cycle, total, count }
            """, bind_vars={"cmte_id": cmte_id}))
            
            # Build funding by cycle
            funding_by_cycle = {}
            for cycle in config.cycles:
                ind = next((f for f in individual_funding if f['cycle'] == cycle), None)
                cmte_f = next((f for f in committee_funding if f['cycle'] == cycle), None)
                
                funding_by_cycle[cycle] = {
                    'individual': ind['total'] if ind else 0,
                    'individual_count': ind['count'] if ind else 0,
                    'committee': cmte_f['total'] if cmte_f else 0,
                    'committee_count': cmte_f['count'] if cmte_f else 0,
                }
            
            # Get top individual donors (exclude conduits like WinRed/ActBlue)
            top_individuals = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._to == @cmte_id
                    FOR d IN donors
                        FILTER d._id == e._from
                        FILTER d.donor_type != "conduit"
                        COLLECT donor_key = d._key, donor_name = d.canonical_name,
                                donor_type = d.donor_type, whale_tier = d.whale_tier
                        AGGREGATE total = SUM(e.total_amount)
                        SORT total DESC
                        LIMIT @limit
                        RETURN { name: donor_name, amount: total, donor_type, whale_tier }
            """, bind_vars={"cmte_id": cmte_id, "limit": config.top_n_donors}))
            
            # Get top committee sources (who transferred to this committee)
            top_sources = list(db.aql.execute("""
                FOR e IN transferred_to
                    FILTER e._to == @cmte_id
                    FOR src IN committees
                        FILTER src._id == e._from
                        COLLECT src_key = src._key, src_name = src.CMTE_NM,
                                src_type = src.terminal_type
                        AGGREGATE total = SUM(e.total_amount)
                        SORT total DESC
                        LIMIT @limit
                        RETURN { cmte_id: src_key, name: src_name, amount: total, terminal_type: src_type }
            """, bind_vars={"cmte_id": cmte_id, "limit": config.top_n_donors}))
            
            # Get top recipients (who this committee transferred to)
            top_recipients = list(db.aql.execute("""
                FOR e IN transferred_to
                    FILTER e._from == @cmte_id
                    FOR dest IN committees
                        FILTER dest._id == e._to
                        COLLECT dest_key = dest._key, dest_name = dest.CMTE_NM,
                                dest_type = dest.terminal_type
                        AGGREGATE total = SUM(e.total_amount)
                        SORT total DESC
                        LIMIT @limit
                        RETURN { cmte_id: dest_key, name: dest_name, amount: total, terminal_type: dest_type }
            """, bind_vars={"cmte_id": cmte_id, "limit": config.top_n_recipients}))
            
            # Count whale donors
            whale_counts = list(db.aql.execute("""
                FOR e IN contributed_to
                    FILTER e._to == @cmte_id
                    FOR d IN donors
                        FILTER d._id == e._from AND d.whale_tier != null
                        COLLECT tier = d.whale_tier WITH COUNT INTO cnt
                        RETURN { tier, count: cnt }
            """, bind_vars={"cmte_id": cmte_id}))
            
            whale_summary = {w['tier']: w['count'] for w in whale_counts}
            
            # Calculate totals
            total_individual = sum(f['individual'] for f in funding_by_cycle.values())
            total_committee = sum(f['committee'] for f in funding_by_cycle.values())
            total_received = total_individual + total_committee
            
            summary = {
                'funding_by_cycle': funding_by_cycle,
                'funding_sources': {
                    'individual_total': total_individual,
                    'committee_total': total_committee,
                    'individual_pct': round(total_individual / total_received * 100, 1) if total_received > 0 else 0,
                    'committee_pct': round(total_committee / total_received * 100, 1) if total_received > 0 else 0,
                },
                'top_individual_donors': top_individuals,
                'top_committee_sources': top_sources,
                'top_recipients': top_recipients,
                'whale_donor_counts': whale_summary,
                'summary_updated_at': datetime.now().isoformat(),
            }
            
            batch_updates.append({
                '_key': cmte_key,
                'funding_summary': summary
            })
            
            stats['committees_processed'] += 1
            if top_individuals or top_sources:
                stats['committees_with_donors'] += 1
            stats['total_receipts_summarized'] += total_received
            
            if len(batch_updates) >= config.batch_size:
                _upsert_committee_summaries(db, batch_updates)
                context.log.info(f"  Processed {stats['committees_processed']:,} committees...")
                batch_updates = []
        
        # Final batch
        if batch_updates:
            _upsert_committee_summaries(db, batch_updates)
        
        # Create index
        try:
            db.collection("committees").add_persistent_index(
                fields=["funding_summary.funding_sources.individual_total"],
                sparse=True
            )
        except Exception:
            pass
        
        context.log.info(f"✅ Computed summaries for {stats['committees_processed']:,} committees")
        context.log.info(f"   Total receipts summarized: ${stats['total_receipts_summarized']:,.0f}")
        
        return Output(
            value=stats,
            metadata={
                "committees_processed": stats['committees_processed'],
                "committees_with_donors": stats['committees_with_donors'],
                "total_receipts": MetadataValue.float(float(stats['total_receipts_summarized'])),
            }
        )


def _upsert_committee_summaries(db, updates: List[Dict]):
    """Upsert funding summaries to committees collection."""
    aql = """
    FOR doc IN @updates
        UPDATE { _key: doc._key } WITH { funding_summary: doc.funding_summary } IN committees
    """
    db.aql.execute(aql, bind_vars={"updates": updates})
