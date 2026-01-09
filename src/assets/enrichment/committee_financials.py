"""Committee Financials - Pre-compute total_receipts and total_disbursed from edges.

FEC's cm.zip contains TOTAL_RECEIPTS and TOTAL_DISBURSEMENTS fields, but these are
lifetime totals across ALL cycles. Our graph only contains edges for specific cycles
(2020, 2022, 2024), causing mismatches.

This enrichment calculates cycle-specific totals from actual edges:
- total_receipts = SUM(contributed_to._to + transferred_to._to) for this committee
- total_disbursed = SUM(transferred_to._from) for this committee

These pre-computed values are used by proportional attribution queries.

CRITICAL: Previously we were calling get_total_receipts() dynamically during traversal,
which was slow and resulted in None values when FEC data was stale. This asset fixes
that by pre-computing once during enrichment.

Source: aggregation.contributed_to, aggregation.transferred_to
Target: aggregation.committees (adds total_receipts, total_disbursed fields)
"""

from typing import Dict, Any

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


@asset(
    name="committee_financials",
    description="Pre-compute total_receipts and total_disbursed from edges for each committee.",
    group_name="enrichment",
    compute_kind="enrichment",
    deps=["contributed_to", "transferred_to"],
)
def committee_financials_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Calculate and store financial totals for all committees.
    
    Uses server-side AQL aggregation for efficiency.
    Runs in two separate phases to avoid write-write conflicts.
    """
    
    with arango.get_client() as client:
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("committees"):
            raise RuntimeError("committees collection missing - run contributed_to asset first")
        
        context.log.info("💰 Calculating committee financials from edges...")
        
        # Phase 1: Calculate and update total_receipts
        context.log.info("  📥 Phase 1: Calculating total_receipts...")
        receipts_aql = """
        FOR c IN committees
            // Contributions received (from donors)
            LET contrib_receipts = SUM(
                FOR e IN contributed_to
                    FILTER e._to == c._id
                    RETURN e.total_amount || 0
            )
            // Transfers received (from other committees)
            LET transfer_receipts = SUM(
                FOR e IN transferred_to
                    FILTER e._to == c._id
                    RETURN e.total_amount || 0
            )
            LET total_receipts = contrib_receipts + transfer_receipts
            
            UPDATE c WITH { total_receipts: total_receipts } IN committees
            
            RETURN { cmte: c.CMTE_ID, receipts: total_receipts }
        """
        receipts_result = list(agg_db.aql.execute(receipts_aql))
        context.log.info(f"     Updated {len(receipts_result):,} committees with total_receipts")
        
        # Phase 2: Calculate and update total_disbursed
        context.log.info("  📤 Phase 2: Calculating total_disbursed...")
        disbursed_aql = """
        FOR c IN committees
            // Transfers sent (to other committees)
            LET total_disbursed = SUM(
                FOR e IN transferred_to
                    FILTER e._from == c._id
                    RETURN e.total_amount || 0
            )
            
            UPDATE c WITH { total_disbursed: total_disbursed } IN committees
            
            RETURN { cmte: c.CMTE_ID, disbursed: total_disbursed }
        """
        disbursed_result = list(agg_db.aql.execute(disbursed_aql))
        context.log.info(f"     Updated {len(disbursed_result):,} committees with total_disbursed")
        
        # Collect stats
        stats_aql = """
        RETURN {
            total_committees: LENGTH(committees),
            with_receipts: LENGTH(FOR c IN committees FILTER c.total_receipts > 0 RETURN 1),
            with_disbursed: LENGTH(FOR c IN committees FILTER c.total_disbursed > 0 RETURN 1),
            with_both: LENGTH(FOR c IN committees FILTER c.total_receipts > 0 AND c.total_disbursed > 0 RETURN 1)
        }
        """
        stats = list(agg_db.aql.execute(stats_aql))[0]
        
        total_committees = stats['total_committees']
        committees_with_receipts = stats['with_receipts']
        committees_with_disbursed = stats['with_disbursed']
        committees_both = stats['with_both']
        
        context.log.info(f"✅ Updated {total_committees:,} committees:")
        context.log.info(f"  With receipts: {committees_with_receipts:,} ({committees_with_receipts/total_committees*100:.1f}%)")
        context.log.info(f"  With disbursements: {committees_with_disbursed:,} ({committees_with_disbursed/total_committees*100:.1f}%)")
        context.log.info(f"  With both: {committees_both:,} ({committees_both/total_committees*100:.1f}%)")
        
        # Get some examples to validate
        validation_query = """
        FOR c IN committees
            FILTER c.total_receipts > 0 OR c.total_disbursed > 0
            SORT c.total_receipts DESC
            LIMIT 5
            RETURN {
                cmte_id: c.CMTE_ID,
                name: c.CMTE_NM,
                total_receipts: c.total_receipts,
                total_disbursed: c.total_disbursed,
                terminal_type: c.terminal_type
            }
        """
        examples = list(agg_db.aql.execute(validation_query))
        
        context.log.info("\n📊 Top 5 committees by receipts:")
        for ex in examples:
            context.log.info(
                f"  {ex['cmte_id']} ({ex.get('terminal_type', 'unknown')}): "
                f"${ex['total_receipts']:,.0f} receipts, ${ex['total_disbursed']:,.0f} disbursed"
            )
        
        # Create indices for fast lookups
        collection = agg_db.collection("committees")
        collection.add_persistent_index(fields=["total_receipts"], unique=False, sparse=False)
        collection.add_persistent_index(fields=["total_disbursed"], unique=False, sparse=False)
        context.log.info("📇 Created indices on total_receipts and total_disbursed")
        
        return Output(
            value={
                "total_committees": total_committees,
                "with_receipts": committees_with_receipts,
                "with_disbursed": committees_with_disbursed,
                "with_both": committees_both,
                "examples": examples,
            },
            metadata={
                "total_committees": MetadataValue.int(total_committees),
                "committees_with_receipts": MetadataValue.int(committees_with_receipts),
                "committees_with_disbursed": MetadataValue.int(committees_with_disbursed),
                "committees_with_both": MetadataValue.int(committees_both),
                "top_examples": MetadataValue.json(examples),
            }
        )
