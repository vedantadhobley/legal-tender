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

from typing import Dict, Any, List, Optional
from datetime import datetime
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource
from src.utils.parallel import parallel_cycles


class CommitteeReceiptsConfig(Config):
    """Configuration for committee receipts aggregation."""
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    batch_size: int = 1000


@asset(
    name="committee_receipts",
    description="Pre-compute ACTUAL committee receipt totals from raw FEC data, per cycle.",
    group_name="enrichment",
    compute_kind="enrichment",
    # NOTE: We read transferred_to in Phase 2 to compute per-cycle
    # total_from_committees. Without declaring it as a dep, Dagster can
    # start this asset before transferred_to is rebuilt — leading to stale
    # divisor data in candidate_funding's trace and 8.8x over-attribution.
    # Bug discovered during validation against FEC weball, 2026-05-08.
    #
    # weball + webk are needed for the AUTHORITATIVE total_from_individuals.
    # Without them, we'd count only itemized donations from indiv.zip and
    # miss unitemized small donors — major undercount (33% median delta) for
    # grassroots-heavy candidates. See docs/funding-channels.md "unitemized
    # grassroots gap" + decisions.md 2026-05-08 for full context.
    deps=["indiv", "pas2", "oth", "contributed_to", "transferred_to", "weball", "webk"],
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
        # Also separately track ENTITY_TP=CAN totals — these are the candidate's
        # own itemized contributions to their own committee (Bloomberg's $1.09B,
        # etc.). They're already in the whale graph because donors.py includes
        # ENTITY_TP IN ['IND','CAN']. We need this to subtract from weball's
        # CAND_CONTRIB so self_funding_total only contains the *unitemized + loans*
        # portion, avoiding double-count with whale_indiv_total downstream.
        can_indiv_by_cycle: Dict[str, Dict[str, float]] = {c: {} for c in config.cycles}

        stats = {
            'cycles_processed': 0,
            'total_individual_amount': 0,
            'committees_updated': 0,
        }

        # Each cycle hits a different database (fec_2020, fec_2022, etc.) — no
        # contention, so run them concurrently. Threads are correct here
        # because the AQL execute() blocks on socket I/O (GIL released).
        def _phase3_for_cycle(cycle: str) -> Dict[str, Any]:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                return {'cycle': cycle, 'skipped': True}

            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            indiv_map: Dict[str, Dict] = {}
            can_map: Dict[str, float] = {}
            cycle_total = 0
            cycle_cmtes = 0
            n_can = 0

            if cycle_db.has_collection("indiv"):
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
                for row in cursor:
                    cmte_id = row['cmte_id']
                    total = row['total'] or 0
                    count = row['count'] or 0
                    indiv_map[cmte_id] = {'total': total, 'count': count}
                    cycle_total += total
                    cycle_cmtes += 1

                # ENTITY_TP=CAN subtotal — used to avoid double-counting between
                # whale_indiv_total and self_funding_total.
                can_cursor = cycle_db.aql.execute("""
                    FOR d IN indiv
                        FILTER d.CMTE_ID != null
                        FILTER d.ENTITY_TP == 'CAN'
                        FILTER d.TRANSACTION_AMT != null
                        COLLECT cmte_id = d.CMTE_ID
                        AGGREGATE total = SUM(TO_NUMBER(d.TRANSACTION_AMT))
                        RETURN { cmte_id, total }
                """, ttl=14400, batch_size=5000, stream=True)
                for row in can_cursor:
                    if row.get('cmte_id') and row.get('total'):
                        can_map[row['cmte_id']] = float(row['total'])
                        n_can += 1

            return {
                'cycle': cycle,
                'indiv_map': indiv_map,
                'can_map': can_map,
                'cycle_total': cycle_total,
                'cycle_cmtes': cycle_cmtes,
                'n_can': n_can,
                'skipped': False,
            }

        context.log.info("Phase 3: Aggregating indiv per cycle (parallel)...")
        phase3_results = parallel_cycles(_phase3_for_cycle, config.cycles, max_workers=4)
        for cycle in config.cycles:
            r = phase3_results.get(cycle)
            if not r or r.get('skipped'):
                context.log.warning(f"   fec_{cycle} not found, skipping")
                continue
            indiv_by_cycle[cycle] = r['indiv_map']
            can_indiv_by_cycle[cycle] = r['can_map']
            stats['cycles_processed'] += 1
            stats['total_individual_amount'] += r['cycle_total']
            context.log.info(f"   {cycle}: ${r['cycle_total']:,.0f} across {r['cycle_cmtes']:,} cmtes "
                             f"({r['n_can']:,} with ENTITY_TP=CAN)")
        gc.collect()

        # ================================================================
        # Phase 3.5: Authoritative individual contribution totals from FEC's
        # own summary files (weball + webk).
        # ================================================================
        # indiv_by_cycle (Phase 3) only has ITEMIZED donations from indiv.zip.
        # FEC reports both itemized + unitemized in the summary files:
        #   - webk.INDV_CONTRIB     — per-PAC, covers most non-candidate cmtes
        #   - weball.TTL_INDIV_CONTRIB — per-CANDIDATE; resolve to principal
        #     campaign committee via cn.CAND_PCC
        # Use whichever covers a given committee; fall back to indiv-summed
        # when neither has data. This closes the "unitemized grassroots gap"
        # documented in docs/funding-channels.md.
        context.log.info("Phase 3.5: Loading authoritative TTL_INDIV_CONTRIB from weball/webk...")
        auth_indiv_by_cycle: Dict[str, Dict[str, float]] = {c: {} for c in config.cycles}
        # Self-funding (CAND_CONTRIB + CAND_LOANS) lives only in weball — never in
        # indiv.zip, where candidates show up only via small itemized splits.
        # Without this, self-funders like Trone (-99% delta) look like they
        # raised almost nothing because $62.9M of loans aren't in any donor edge.
        self_funding_by_cycle: Dict[str, Dict[str, float]] = {c: {} for c in config.cycles}
        # Per-cycle worker — independent across cycles (different databases).
        # Reads indiv_by_cycle / can_indiv_by_cycle that Phase 3 has already
        # populated; those dicts are read-only here so no synchronization
        # needed.
        def _phase35_for_cycle(cycle: str) -> Dict[str, Any]:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                return {'cycle': cycle, 'skipped': True}
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)

            auth: Dict[str, float] = {}
            self_fund: Dict[str, float] = {}
            n_webk = 0
            n_weball = 0
            n_self = 0
            n_rerouted = 0

            # webk first (PACs and most non-candidate committees)
            if cycle_db.has_collection("webk"):
                for r in cycle_db.aql.execute(
                    "FOR c IN webk RETURN { cmte_id: c.CMTE_ID, val: c.INDV_CONTRIB }"
                ):
                    if r.get('val') is not None and r.get('cmte_id'):
                        auth[r['cmte_id']] = float(r['val'])
                        n_webk += 1

            # weball overrides for candidate principal committees.
            # Route via ccl-linked principals: among each candidate's
            # CMTE_DSGN='P' committees for this cycle, pick the one with the
            # most indiv records. Falls back to cn.CAND_PCC only when no
            # principal has activity. (Diagnosed via Sanders 2020: cn.CAND_PCC
            # pointed at dormant BERNIE 2016 instead of operational BERNIE 2020.)
            principals_per_cand: Dict[str, List[str]] = {}
            if cycle_db.has_collection("ccl"):
                for r in cycle_db.aql.execute(
                    "FOR l IN ccl FILTER l.CMTE_DSGN == 'P' "
                    "RETURN { cand_id: l.CAND_ID, cmte_id: l.CMTE_ID }"
                ):
                    if r.get('cand_id') and r.get('cmte_id'):
                        principals_per_cand.setdefault(r['cand_id'], []).append(r['cmte_id'])

            cycle_indiv = indiv_by_cycle.get(cycle, {})
            cycle_can = can_indiv_by_cycle.get(cycle, {})

            if cycle_db.has_collection("weball") and cycle_db.has_collection("cn"):
                for r in cycle_db.aql.execute("""
                    FOR w IN weball
                        LET cn_rec = FIRST(FOR cn IN cn FILTER cn.CAND_ID == w.CAND_ID RETURN cn)
                        RETURN {
                            cand_id: w.CAND_ID,
                            fallback_pcc: cn_rec ? cn_rec.CAND_PCC : null,
                            indiv: w.TTL_INDIV_CONTRIB,
                            cand_contrib: w.CAND_CONTRIB,
                            cand_loans: w.CAND_LOANS
                        }
                """):
                    cand_id = r.get('cand_id')
                    fallback = r.get('fallback_pcc') or ''
                    principals = principals_per_cand.get(cand_id, [])
                    active = [
                        (c, cycle_indiv.get(c, {}).get('count', 0))
                        for c in principals
                        if cycle_indiv.get(c, {}).get('count', 0) > 0
                    ]
                    if active:
                        target_cmte = max(active, key=lambda x: x[1])[0]
                        if fallback and target_cmte != fallback:
                            n_rerouted += 1
                    elif fallback:
                        target_cmte = fallback
                    else:
                        continue

                    if r.get('indiv') is not None:
                        auth[target_cmte] = float(r['indiv'])
                        n_weball += 1
                    cand_contrib = float(r.get('cand_contrib') or 0)
                    cand_loans = float(r.get('cand_loans') or 0)
                    # Subtract indiv.zip ENTITY_TP=CAN overlap to avoid double-
                    # counting with whale_indiv_total. Loans aren't in indiv.zip
                    # so they survive; itemized candidate contributions net out.
                    can_already_counted = cycle_can.get(target_cmte, 0)
                    self_amt = max(0.0, cand_contrib + cand_loans - can_already_counted)
                    if self_amt > 0:
                        self_fund[target_cmte] = self_amt
                        n_self += 1

            return {
                'cycle': cycle,
                'auth': auth,
                'self_fund': self_fund,
                'n_webk': n_webk,
                'n_weball': n_weball,
                'n_self': n_self,
                'n_rerouted': n_rerouted,
                'skipped': False,
            }

        phase35_results = parallel_cycles(_phase35_for_cycle, config.cycles, max_workers=4)
        for cycle in config.cycles:
            r = phase35_results.get(cycle)
            if not r or r.get('skipped'):
                continue
            auth_indiv_by_cycle[cycle] = r['auth']
            self_funding_by_cycle[cycle] = r['self_fund']
            context.log.info(f"   {cycle}: {r['n_webk']:,} from webk + {r['n_weball']:,} from weball "
                             f"(total {len(r['auth']):,} cmtes with auth values, "
                             f"{r['n_self']:,} self-funded, {r['n_rerouted']:,} rerouted)")

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
            all_cmte_ids.update(auth_indiv_by_cycle[cycle].keys())
            all_cmte_ids.update(self_funding_by_cycle[cycle].keys())

        context.log.info(f"   {len(all_cmte_ids):,} committees to process")

        # Track how often we used the authoritative value vs fell back
        stats['cmtes_using_auth_total'] = 0
        stats['cmtes_using_indiv_fallback'] = 0

        # Per-cmte computation is pure CPU/dict-lookup; the bottleneck is the
        # UPSERT roundtrip. Split into N chunks, each chunk processed by a
        # worker thread that builds doc updates and flushes 1000-row batches.
        # ArangoDB serializes UPSERT per-document, and our chunks have disjoint
        # _keys, so concurrent batches can't conflict on the same doc.
        UPSERT_AQL = """
        FOR doc IN @batch
            UPSERT { _key: doc._key }
            INSERT doc
            UPDATE doc
            IN committees
            OPTIONS { mergeObjects: false }
        """

        def _process_one_cmte(cmte_id: str) -> Optional[Dict[str, Any]]:
            """Compute the receipts_by_cycle dict + aggregate for one cmte.
            Returns the upsert doc, or None if cmte has no data this cycle."""
            receipts_by_cycle = {}
            cmte_auth_hits = 0
            cmte_fallback_hits = 0

            for cycle in config.cycles:
                indiv = indiv_by_cycle[cycle].get(cmte_id, {})
                whale = whale_by_cycle[cycle].get(cmte_id, {})
                transfer = transfer_by_cycle[cycle].get(cmte_id, 0)

                indiv_summed = indiv.get('total', 0)
                whale_total = whale.get('total', 0)
                whale_count = whale.get('count', 0)
                donation_count = indiv.get('count', 0)
                self_funding = self_funding_by_cycle[cycle].get(cmte_id, 0)
                can_overlap = can_indiv_by_cycle[cycle].get(cmte_id, 0)

                auth_total = auth_indiv_by_cycle[cycle].get(cmte_id)
                if auth_total is not None:
                    individuals_external = auth_total
                    indiv_source = 'fec_summary'
                    cmte_auth_hits += 1
                else:
                    individuals_external = indiv_summed
                    indiv_source = 'indiv_zip'
                    cmte_fallback_hits += 1

                total_individuals = individuals_external + self_funding + can_overlap
                small_total = max(0, individuals_external - whale_total)
                total_receipts = total_individuals + transfer

                if total_individuals > 0 or transfer > 0 or whale_total > 0:
                    receipts_by_cycle[cycle] = {
                        'total_from_individuals': total_individuals,
                        'total_from_individuals_source': indiv_source,
                        'total_from_individuals_indiv_zip': indiv_summed,
                        'total_from_individuals_external': individuals_external,
                        'self_funding_total': self_funding,
                        'small_donor_total': small_total,
                        'whale_donor_total': whale_total,
                        'whale_donor_count': whale_count,
                        'donation_count': donation_count,
                        'total_from_committees': transfer,
                        'total_receipts': total_receipts,
                    }

            if not receipts_by_cycle:
                return None

            agg_total_individuals = sum(r.get('total_from_individuals', 0) for r in receipts_by_cycle.values())
            agg_individuals_external = sum(r.get('total_from_individuals_external', 0) for r in receipts_by_cycle.values())
            agg_self_funding = sum(r.get('self_funding_total', 0) for r in receipts_by_cycle.values())
            agg_whale_total = sum(r.get('whale_donor_total', 0) for r in receipts_by_cycle.values())
            agg_whale_count = sum(r.get('whale_donor_count', 0) for r in receipts_by_cycle.values())
            agg_donation_count = sum(r.get('donation_count', 0) for r in receipts_by_cycle.values())
            agg_transfer = sum(r.get('total_from_committees', 0) for r in receipts_by_cycle.values())
            agg_small_total = max(0, agg_individuals_external - agg_whale_total)
            agg_total_receipts = agg_total_individuals + agg_transfer

            return {
                '_doc': {
                    '_key': cmte_id,
                    'receipts_by_cycle': receipts_by_cycle,
                    'total_from_individuals': agg_total_individuals,
                    'total_from_individuals_external': agg_individuals_external,
                    'self_funding_total': agg_self_funding,
                    'small_donor_total': agg_small_total,
                    'whale_donor_total': agg_whale_total,
                    'whale_donor_count': agg_whale_count,
                    'donation_count': agg_donation_count,
                    'total_from_committees': agg_transfer,
                    'total_receipts': agg_total_receipts,
                    'receipts_updated_at': datetime.now().isoformat(),
                },
                'auth_hits': cmte_auth_hits,
                'fallback_hits': cmte_fallback_hits,
            }

        def _process_chunk(chunk: List[str]) -> Dict[str, int]:
            """Process a chunk of cmte_ids. Each thread owns its own connection
            (created lazily by python-arango) and its own batch list."""
            local_db = client.db("aggregation", username=arango.username, password=arango.password)
            batch = []
            written = 0
            auth_hits = 0
            fallback_hits = 0
            for cmte_id in chunk:
                result = _process_one_cmte(cmte_id)
                if result is None:
                    continue
                batch.append(result['_doc'])
                auth_hits += result['auth_hits']
                fallback_hits += result['fallback_hits']
                if len(batch) >= 1000:
                    local_db.aql.execute(UPSERT_AQL, bind_vars={"batch": batch})
                    written += len(batch)
                    batch = []
            if batch:
                local_db.aql.execute(UPSERT_AQL, bind_vars={"batch": batch})
                written += len(batch)
            return {'written': written, 'auth_hits': auth_hits, 'fallback_hits': fallback_hits}

        # Split cmte_ids into N chunks. 8 workers is a good balance —
        # enough to saturate Arango's writers without overwhelming the
        # connection pool.
        cmte_list = list(all_cmte_ids)
        N_WORKERS = 8
        chunk_size = (len(cmte_list) + N_WORKERS - 1) // N_WORKERS
        chunks = [cmte_list[i:i + chunk_size] for i in range(0, len(cmte_list), chunk_size)]
        context.log.info(f"   Splitting {len(cmte_list):,} cmtes into {len(chunks)} chunks of ~{chunk_size:,} for parallel UPSERT")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
            futures = [pool.submit(_process_chunk, chunk) for chunk in chunks]
            for fut in as_completed(futures):
                r = fut.result()
                stats['committees_updated'] += r['written']
                stats['cmtes_using_auth_total'] += r['auth_hits']
                stats['cmtes_using_indiv_fallback'] += r['fallback_hits']

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
