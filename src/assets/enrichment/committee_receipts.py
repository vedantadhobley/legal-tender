"""Committee Receipts - Pre-compute ACTUAL receipt totals from raw FEC data, PER CYCLE.

CRITICAL: All receipt data is computed per-cycle first, then aggregated. This ensures
correct proportional tracing in the candidate_funding asset (passthrough multipliers
need per-cycle receipts to match per-cycle transfer amounts).

STRATEGY:
1. Get whale donor totals per committee per cycle from contributed_to edges
2. Get committee transfer totals per committee per cycle from transferred_to edges
3. Get total individual contributions per committee per cycle from raw FEC data
4. Small donor total = total - whale total (per cycle)
5. Store receipts_by_cycle + aggregate on each committee doc

"Whale" = gave >= FEC per-election limit ($2,800-$3,500 depending on cycle) to any
single committee. These donors have graph vertices with employer/corporate detail.
"Small donor" = everyone below that threshold. Known total but no per-donor detail.

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
    description="Pre-compute ACTUAL committee receipt totals from raw FEC data, per cycle.",
    group_name="enrichment",
    compute_kind="enrichment",
    deps=["indiv", "pas2", "oth", "contributed_to"],
)
def committee_receipts_asset(
    context: AssetExecutionContext,
    config: CommitteeReceiptsConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Calculate TRUE receipt totals from raw FEC data for all committees, per cycle.

    All data is computed per-cycle first, then aggregated. This ensures the
    candidate_funding asset has correct per-cycle receipts for proportional tracing.
    """

    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)

        if not agg_db.has_collection("committees"):
            raise RuntimeError("committees collection missing - run contributed_to asset first")

        context.log.info("Computing committee receipts per cycle from raw FEC data...")

        # ================================================================
        # Phase 1: Whale totals BY CYCLE from contributed_to edges
        # ================================================================
        context.log.info("Phase 1: Getting whale totals by cycle from contributed_to edges...")
        whale_by_cycle: Dict[str, Dict[str, Dict]] = {c: {} for c in config.cycles}

        cursor = agg_db.aql.execute("""
            FOR e IN contributed_to
                COLLECT cmte_id = SPLIT(e._to, '/')[1], cycle = e.cycle
                AGGREGATE total = SUM(e.total_amount), count = COUNT(1)
                RETURN { cmte_id, cycle, total, count }
        """, ttl=3600, stream=True)

        for row in cursor:
            cycle = row.get('cycle')
            if cycle in whale_by_cycle:
                whale_by_cycle[cycle][row['cmte_id']] = {
                    'total': row['total'] or 0,
                    'count': row['count'] or 0,
                }

        context.log.info(f"   Whale totals: " +
                        ", ".join(f"{c}={len(whale_by_cycle[c]):,}" for c in config.cycles))

        # ================================================================
        # Phase 2: Committee transfer totals BY CYCLE from transferred_to edges
        # ================================================================
        context.log.info("Phase 2: Getting committee transfer totals by cycle...")
        transfer_by_cycle: Dict[str, Dict[str, float]] = {c: {} for c in config.cycles}

        cursor = agg_db.aql.execute("""
            FOR e IN transferred_to
                COLLECT cmte_id = SPLIT(e._to, '/')[1], cycle = e.cycle
                AGGREGATE total = SUM(e.total_amount)
                RETURN { cmte_id, cycle, total }
        """, ttl=3600, stream=True)

        for row in cursor:
            cycle = row.get('cycle')
            if cycle in transfer_by_cycle:
                transfer_by_cycle[cycle][row['cmte_id']] = row['total'] or 0

        context.log.info(f"   Transfer totals: " +
                        ", ".join(f"{c}={len(transfer_by_cycle[c]):,}" for c in config.cycles))

        # ================================================================
        # Phase 3: Individual contributions per committee PER CYCLE
        # ================================================================
        indiv_by_cycle: Dict[str, Dict[str, Dict]] = {c: {} for c in config.cycles}

        stats = {
            'cycles_processed': 0,
            'total_individual_amount': 0,
            'committees_updated': 0,
        }

        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                context.log.warning(f"   {db_name} not found, skipping")
                continue

            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            stats['cycles_processed'] += 1

            if cycle_db.has_collection("indiv"):
                context.log.info(f"Processing {cycle} individual contributions...")

                cursor = cycle_db.aql.execute("""
                    FOR d IN indiv
                        FILTER d.CMTE_ID != null
                        FILTER d.TRANSACTION_AMT != null
                        COLLECT cmte_id = d.CMTE_ID
                        AGGREGATE
                            total = SUM(TO_NUMBER(d.TRANSACTION_AMT)),
                            count = COUNT(1)
                        RETURN { cmte_id, total, count }
                """, ttl=14400, batch_size=5000, stream=True)

                cycle_total = 0
                cycle_cmtes = 0

                for row in cursor:
                    cmte_id = row['cmte_id']
                    total = row['total'] or 0
                    count = row['count'] or 0

                    indiv_by_cycle[cycle][cmte_id] = {
                        'total': total,
                        'count': count,
                    }
                    cycle_total += total
                    cycle_cmtes += 1

                stats['total_individual_amount'] += cycle_total
                context.log.info(f"   {cycle}: ${cycle_total:,.0f} across {cycle_cmtes:,} committees")
                gc.collect()

        # ================================================================
        # Phase 4: Compute per-cycle receipts and write to committees
        # ================================================================
        context.log.info("Phase 4: Computing per-cycle receipts and updating committees...")

        # Collect all committee IDs seen across all data sources
        all_cmte_ids = set()
        for cycle in config.cycles:
            all_cmte_ids.update(indiv_by_cycle[cycle].keys())
            all_cmte_ids.update(whale_by_cycle[cycle].keys())
            all_cmte_ids.update(transfer_by_cycle[cycle].keys())

        context.log.info(f"   {len(all_cmte_ids):,} committees to process")

        batch = []
        for cmte_id in all_cmte_ids:
            receipts_by_cycle = {}

            for cycle in config.cycles:
                indiv = indiv_by_cycle[cycle].get(cmte_id, {})
                whale = whale_by_cycle[cycle].get(cmte_id, {})
                transfer = transfer_by_cycle[cycle].get(cmte_id, 0)

                total_individuals = indiv.get('total', 0)
                whale_total = whale.get('total', 0)
                whale_count = whale.get('count', 0)
                donation_count = indiv.get('count', 0)
                small_total = max(0, total_individuals - whale_total)
                total_receipts = total_individuals + transfer

                if total_individuals > 0 or transfer > 0 or whale_total > 0:
                    receipts_by_cycle[cycle] = {
                        'total_from_individuals': total_individuals,
                        'small_donor_total': small_total,
                        'whale_donor_total': whale_total,
                        'whale_donor_count': whale_count,
                        'donation_count': donation_count,
                        'total_from_committees': transfer,
                        'total_receipts': total_receipts,
                    }

            if not receipts_by_cycle:
                continue

            # Aggregate = sum of per-cycle values
            agg_total_individuals = sum(r.get('total_from_individuals', 0) for r in receipts_by_cycle.values())
            agg_whale_total = sum(r.get('whale_donor_total', 0) for r in receipts_by_cycle.values())
            agg_whale_count = sum(r.get('whale_donor_count', 0) for r in receipts_by_cycle.values())
            agg_donation_count = sum(r.get('donation_count', 0) for r in receipts_by_cycle.values())
            agg_transfer = sum(r.get('total_from_committees', 0) for r in receipts_by_cycle.values())
            agg_small_total = max(0, agg_total_individuals - agg_whale_total)
            agg_total_receipts = agg_total_individuals + agg_transfer

            update = {
                '_key': cmte_id,
                'receipts_by_cycle': receipts_by_cycle,
                # Aggregate fields (backward compat)
                'total_from_individuals': agg_total_individuals,
                'small_donor_total': agg_small_total,
                'whale_donor_total': agg_whale_total,
                'whale_donor_count': agg_whale_count,
                'donation_count': agg_donation_count,
                'total_from_committees': agg_transfer,
                'total_receipts': agg_total_receipts,
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
        context.log.info("Phase 5: Validation...")

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
                    donation_count: c.donation_count,
                    receipts_by_cycle: c.receipts_by_cycle
                }
        """))

        if cruz:
            c = cruz[0]
            context.log.info(f"\nValidation - {c['name']}:")
            context.log.info(f"   Total receipts (aggregate): ${c['total_receipts']:,.0f}")
            context.log.info(f"   From individuals: ${c['total_from_individuals']:,.0f}")
            context.log.info(f"   - Small donors: ${c['small_donor_total']:,.0f}")
            context.log.info(f"   - Whale donors: ${c['whale_donor_total']:,.0f} ({c['whale_donor_count']} whales)")
            context.log.info(f"   Donations: {c['donation_count']:,}")
            if c.get('receipts_by_cycle'):
                for cycle, data in sorted(c['receipts_by_cycle'].items()):
                    context.log.info(f"   {cycle}: receipts=${data['total_receipts']:,.0f}, "
                                    f"indiv=${data['total_from_individuals']:,.0f}, "
                                    f"small=${data['small_donor_total']:,.0f}")

        context.log.info(f"\nSummary:")
        context.log.info(f"   Cycles processed: {stats['cycles_processed']}")
        context.log.info(f"   Committees updated: {stats['committees_updated']:,}")
        context.log.info(f"   Total individual contributions: ${stats['total_individual_amount']:,.0f}")

        return Output(
            value=stats,
            metadata={
                "cycles_processed": MetadataValue.int(stats['cycles_processed']),
                "committees_updated": MetadataValue.int(stats['committees_updated']),
                "total_individual_amount": MetadataValue.float(float(stats['total_individual_amount'])),
            }
        )
