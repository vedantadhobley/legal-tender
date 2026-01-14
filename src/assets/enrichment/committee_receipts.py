"""Committee Receipts - Pre-compute ACTUAL receipt totals from raw FEC data.

CRITICAL FIX: The previous committee_financials only summed contributed_to edges,
which only includes $10K+ whale donors. This misses 90-95% of individual contributions!

STRATEGY:
1. Get total individual contributions per committee from raw FEC (simple aggregation)
2. Use existing contributed_to edges for whale donor totals (already filtered $10K+)
3. Small donor total = total - whale total

This avoids expensive per-donor aggregation while still giving us accurate breakdowns.

Source: fec_{cycle}.indiv, fec_{cycle}.pas2, fec_{cycle}.oth, aggregation.contributed_to
Target: aggregation.committees (updates financial fields)
"""

from typing import Dict, Any, List
from datetime import datetime
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource


class CommitteeReceiptsConfig(Config):
    """Configuration for committee receipts aggregation."""
    cycles: List[str] = ["2020", "2022", "2024"]
    batch_size: int = 1000


@asset(
    name="committee_receipts",
    description="Pre-compute ACTUAL committee receipt totals from raw FEC data (not just whale edges).",
    group_name="enrichment",
    compute_kind="enrichment",
    deps=["indiv", "pas2", "oth", "contributed_to"],
)
def committee_receipts_asset(
    context: AssetExecutionContext,
    config: CommitteeReceiptsConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Calculate TRUE receipt totals from raw FEC data for all committees.
    
    Strategy:
    1. Get total individual contributions per committee (simple COLLECT by CMTE_ID)
    2. Get whale totals from existing contributed_to edges
    3. Small donors = total - whale
    
    This avoids the expensive per-donor aggregation that times out.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("committees"):
            raise RuntimeError("committees collection missing - run contributed_to asset first")
        
        context.log.info("💰 Computing ACTUAL committee receipts from raw FEC data...")
        
        # Get whale totals from existing contributed_to edges
        # These are already filtered to $10K+ donors
        context.log.info("📊 Phase 1: Getting whale totals from contributed_to edges...")
        whale_totals = {}
        whale_counts = {}
        
        cursor = agg_db.aql.execute("""
            FOR e IN contributed_to
                COLLECT cmte_id = SPLIT(e._to, '/')[1]
                AGGREGATE total = SUM(e.total_amount), count = COUNT(1)
                RETURN { cmte_id: cmte_id, total: total, count: count }
        """, ttl=3600, stream=True)
        
        for row in cursor:
            whale_totals[row['cmte_id']] = row['total'] or 0
            whale_counts[row['cmte_id']] = row['count'] or 0
        
        context.log.info(f"   Got whale totals for {len(whale_totals):,} committees")
        
        # Get committee transfers from existing transferred_to edges
        context.log.info("📊 Phase 2: Getting committee transfer totals...")
        transfer_totals = {}
        
        cursor = agg_db.aql.execute("""
            FOR e IN transferred_to
                COLLECT cmte_id = SPLIT(e._to, '/')[1]
                AGGREGATE total = SUM(e.total_amount)
                RETURN { cmte_id: cmte_id, total: total }
        """, ttl=3600, stream=True)
        
        for row in cursor:
            transfer_totals[row['cmte_id']] = row['total'] or 0
        
        context.log.info(f"   Got transfer totals for {len(transfer_totals):,} committees")
        
        # Initialize receipt tracking
        committee_receipts: Dict[str, Dict[str, float]] = {}
        
        stats = {
            'cycles_processed': 0,
            'total_individual_amount': 0,
            'total_committee_amount': 0,
            'committees_updated': 0,
        }
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                context.log.warning(f"   {db_name} not found, skipping")
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            stats['cycles_processed'] += 1
            
            # ================================================================
            # Get total individual contributions per committee
            # Simple aggregation - no per-donor breakdown needed
            # ================================================================
            if cycle_db.has_collection("indiv"):
                context.log.info(f"📥 Processing {cycle} individual contributions...")
                
                aql_indiv = """
                FOR d IN indiv
                    FILTER d.CMTE_ID != null
                    FILTER d.TRANSACTION_AMT != null
                    
                    COLLECT cmte_id = d.CMTE_ID
                    AGGREGATE 
                        total = SUM(TO_NUMBER(d.TRANSACTION_AMT)),
                        count = COUNT(1)
                    
                    RETURN { cmte_id: cmte_id, total: total, count: count }
                """
                
                cursor = cycle_db.aql.execute(
                    aql_indiv,
                    ttl=14400,  # 4 hours
                    batch_size=5000,
                    stream=True
                )
                
                cycle_total = 0
                cycle_cmtes = 0
                
                for row in cursor:
                    cmte_id = row['cmte_id']
                    total = row['total'] or 0
                    count = row['count'] or 0
                    
                    if cmte_id not in committee_receipts:
                        committee_receipts[cmte_id] = {
                            'total_individuals': 0,
                            'donation_count': 0,
                        }
                    
                    committee_receipts[cmte_id]['total_individuals'] += total
                    committee_receipts[cmte_id]['donation_count'] += count
                    cycle_total += total
                    cycle_cmtes += 1
                
                stats['total_individual_amount'] += cycle_total
                context.log.info(f"   {cycle}: ${cycle_total:,.0f} across {cycle_cmtes:,} committees")
                gc.collect()
        
        # ================================================================
        # Compute small donor totals and write to committees
        # ================================================================
        context.log.info("📝 Phase 3: Computing small donor totals and updating committees...")
        
        batch = []
        for cmte_id, receipts in committee_receipts.items():
            total_individuals = receipts['total_individuals']
            whale_total = whale_totals.get(cmte_id, 0)
            whale_count = whale_counts.get(cmte_id, 0)
            transfer_total = transfer_totals.get(cmte_id, 0)
            
            # Small donors = total - whales
            small_total = max(0, total_individuals - whale_total)
            
            total_receipts = total_individuals + transfer_total
            
            update = {
                '_key': cmte_id,
                'total_from_individuals': total_individuals,
                'small_donor_total': small_total,
                'whale_donor_total': whale_total,
                'whale_donor_count': whale_count,
                'donation_count': receipts['donation_count'],
                'total_from_committees': transfer_total,
                'total_receipts': total_receipts,
                'receipts_updated_at': datetime.now().isoformat(),
            }
            batch.append(update)
            
            if len(batch) >= 1000:
                agg_db.aql.execute("""
                    FOR doc IN @batch
                        UPSERT { _key: doc._key }
                        INSERT doc
                        UPDATE doc
                        IN committees
                """, bind_vars={"batch": batch})
                stats['committees_updated'] += len(batch)
                batch = []
        
        if batch:
            agg_db.aql.execute("""
                FOR doc IN @batch
                    UPSERT { _key: doc._key }
                    INSERT doc
                    UPDATE doc
                    IN committees
            """, bind_vars={"batch": batch})
            stats['committees_updated'] += len(batch)
        
        # ================================================================
        # Validation
        # ================================================================
        context.log.info("✅ Phase 4: Validation...")
        
        # Check Cruz
        cruz = list(agg_db.aql.execute("""
            FOR c IN committees
                FILTER c._key == 'C00492785'
                RETURN {
                    name: c.CMTE_NM,
                    total_receipts: c.total_receipts,
                    total_from_individuals: c.total_from_individuals,
                    small_donor_total: c.small_donor_total,
                    whale_donor_total: c.whale_donor_total,
                    whale_donor_count: c.whale_donor_count,
                    donation_count: c.donation_count
                }
        """))
        
        if cruz:
            c = cruz[0]
            context.log.info(f"\n🎯 Validation - {c['name']}:")
            context.log.info(f"   Total receipts: ${c['total_receipts']:,.0f}")
            context.log.info(f"   From individuals: ${c['total_from_individuals']:,.0f}")
            context.log.info(f"   - Small donors: ${c['small_donor_total']:,.0f}")
            context.log.info(f"   - Whale donors: ${c['whale_donor_total']:,.0f} ({c['whale_donor_count']} whales)")
            context.log.info(f"   Donations: {c['donation_count']:,}")
        
        context.log.info(f"\n📈 Summary:")
        context.log.info(f"   Cycles processed: {stats['cycles_processed']}")
        context.log.info(f"   Committees updated: {stats['committees_updated']:,}")
        context.log.info(f"   Total individual contributions: ${stats['total_individual_amount']:,.0f}")
        
        return Output(
            value=stats,
            metadata={
                "cycles_processed": MetadataValue.int(stats['cycles_processed']),
                "committees_updated": MetadataValue.int(stats['committees_updated']),
                "total_individual_amount": MetadataValue.float(stats['total_individual_amount']),
            }
        )
