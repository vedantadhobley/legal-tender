"""Donors Vertex Collection - Normalized individual donors for graph traversal.

Threshold: Per-election max-out donors. A donor qualifies if they gave >= the
FEC per-election individual contribution limit to ANY SINGLE committee in a cycle.

FEC per-election limits (individual → candidate committee):
  2020: $2,800    2022: $2,900    2024: $3,300    2026: $3,500

WHY THIS THRESHOLD: The per-election limit is the legal maximum an individual
can give to one candidate per election (primary/general). Anyone at or near this
ceiling made a deliberate, maxed-out commitment to that specific candidate —
these are NOT casual donors. This is a legally-defined, principled threshold
rather than an arbitrary dollar amount.

This captures ~3.5x more donors than the old $10K-total approach, and the RIGHT
donors: people who maxed out to a specific candidate but may have given less than
$10K in total across all recipients.

MEMORY OPTIMIZATION: Uses server-side AQL aggregation and streaming inserts.
Never holds more than one batch in Python memory at a time.

Source: fec_{cycle}.indiv collections
Target: aggregation.donors (vertex collection)
"""

from typing import Dict, Any, List
from datetime import datetime
from hashlib import sha256
import re
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource
from src.utils.parallel import parallel_cycles
from src.config import ACTIVE_CYCLES


class DonorsConfig(Config):
    """Configuration for donors vertex asset."""
    cycles: List[str] = list(ACTIVE_CYCLES)
    batch_size: int = 5000  # Smaller batch for memory


# PER_ELECTION_LIMITS moved to src/config.py. Re-exported here as a
# module attribute so existing internal references stay working.
from src.config import PER_ELECTION_LIMITS  # noqa: E402


def normalize_name(name: str) -> str:
    """Normalize donor name for deduplication."""
    if not name:
        return ""
    name = re.sub(r'\s*,\s*', ' ', name.upper().strip())
    name = re.sub(r'\s+', ' ', name)
    return name


def normalize_employer(employer: str) -> str:
    """Normalize employer name for deduplication."""
    if not employer:
        return ""
    employer = employer.upper().strip()
    employer = re.sub(r'[,.]', '', employer)
    employer = re.sub(r'\s+', ' ', employer)
    return employer


def make_donor_key(name: str, employer: str) -> str:
    """Create unique _key for donor from normalized name + employer."""
    normalized = f"{normalize_name(name)}|{normalize_employer(employer)}"
    return sha256(normalized.encode()).hexdigest()[:16]


def upsert_donors_batch(db, batch: List[Dict], cycle: str):
    """Upsert a batch of donors, merging cycle data for existing donors."""
    
    aql = """
    FOR doc IN @batch
        UPSERT { _key: doc._key }
        INSERT doc
        UPDATE {
            total_amount: OLD.total_amount + doc.total_amount,
            transaction_count: OLD.transaction_count + doc.transaction_count,
            cycles: APPEND(OLD.cycles, doc.cycles, true),
            updated_at: doc.updated_at
        }
        IN donors
    """
    
    db.aql.execute(aql, bind_vars={"batch": batch})


@asset(
    name="donors",
    description="Normalized donor vertices - individuals who maxed out (>= per-election limit) to any committee.",
    group_name="graph",
    compute_kind="graph_vertex",
    deps=["indiv"],
)
def donors_asset(
    context: AssetExecutionContext,
    config: DonorsConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create donors vertex collection - MEMORY EFFICIENT.
    
    Two-pass per cycle:
    1. Find qualifying donors: aggregate by (name, employer, cmte_id), check if
       any single per-committee total >= FEC per-election limit for that cycle.
    2. Get full donor totals: re-aggregate qualifying donors by (name, employer)
       across ALL their committees for the vertex data.
    
    Uses server-side AQL for both passes. Streams results directly to donors
    collection using UPSERT. Force GC between cycles.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        
        # Ensure aggregation database exists
        if not sys_db.has_database("aggregation"):
            sys_db.create_database("aggregation")
            context.log.info("Created 'aggregation' database")
        
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        # Create/truncate donors collection
        if agg_db.has_collection("donors"):
            agg_db.collection("donors").truncate()
            context.log.info("Truncated existing donors collection")
        else:
            agg_db.create_collection("donors")
            context.log.info("Created donors collection")
        
        stats = {
            'cycles_processed': 0,
            'total_donors_inserted': 0,
            'by_cycle': {}
        }

        # Build per-cycle committee name → CMTE_ID map for name-match earmark
        # re-attribution. About 1% of earmark records in indiv have a memo
        # like `EARMARKED FOR JON OSSOFF FOR SENATE` with no parenthetical
        # CMTE_ID (or `(PENDING)`), so the AQL's primary regex-based parse
        # can't recover the target. Falling back to a name lookup recovers
        # ~80% of these (the remainder are ambiguous or have memo text that
        # diverges from the canonical CMTE_NM — see `docs/data-quality.md`).
        #
        # Disambiguation: when two committees share the same CMTE_NM
        # (e.g. "ALTMAN FOR CONGRESS" exists for both Susan Altman NJ-12
        # 2026 and an unrelated NJ-7 Altman from prior cycles), prefer the
        # committee whose `cycles` array contains the current cycle being
        # processed. Falls back to "skip on ambiguity" if no cycle match.
        context.log.info("📋 Loading committee name → CMTE_ID maps per cycle for earmark name-resolution...")
        all_committees = list(agg_db.aql.execute(
            "FOR cmte IN committees RETURN {key: cmte._key, name: cmte.CMTE_NM, cycles: cmte.cycles}"
        ))
        # name_maps_by_cycle[cycle] = {upper_committee_name: cmte_id}
        # Prefers committees active in `cycle`; uses unique-only when no cycle preference resolves.
        name_maps_by_cycle: Dict[str, Dict[str, str]] = {}
        for cycle in config.cycles:
            cmte_to_in_cycle: Dict[str, list] = {}
            cmte_to_other: Dict[str, list] = {}
            for c_doc in all_committees:
                nm = (c_doc.get('name') or '').upper().strip()
                if not nm:
                    continue
                cycs = c_doc.get('cycles') or []
                if cycle in cycs:
                    cmte_to_in_cycle.setdefault(nm, []).append(c_doc['key'])
                else:
                    cmte_to_other.setdefault(nm, []).append(c_doc['key'])
            cycle_map: Dict[str, str] = {}
            for nm in set(list(cmte_to_in_cycle.keys()) + list(cmte_to_other.keys())):
                in_cyc = cmte_to_in_cycle.get(nm, [])
                other = cmte_to_other.get(nm, [])
                if len(in_cyc) == 1:
                    cycle_map[nm] = in_cyc[0]
                elif len(in_cyc) == 0 and len(other) == 1:
                    cycle_map[nm] = other[0]
                # else: ambiguous → skip
            name_maps_by_cycle[cycle] = cycle_map
            context.log.info(f"  {cycle}: {len(cycle_map):,} unique committee names resolvable")

        # NOTE on the deleted donor-NAME substring filter:
        # Previously this asset had `FILTER NOT REGEX_TEST(doc.NAME,
        # '(ACTBLUE|WINRED|EARMARK|CONDUIT)', true)` intended to exclude
        # conduit-as-donor records. Empirically that filter matched only
        # 9 rows across 2026 — all of them legitimate individuals named
        # McConduit or Conduitte. The intended targets (ActBlue/WinRed
        # bulk transfers) don't appear here because they have
        # ENTITY_TP=COM, already excluded by the IND/CAN filter below.
        # Deleted 2026-05-21 — the filter was a hardcode that didn't
        # achieve its stated purpose and was excluding real donors as a
        # side effect.

        # Per-cycle worker. Each cycle reads its own fec_{cycle}.indiv (no
        # contention) and writes to aggregation.donors via per-document UPSERT,
        # which Arango serializes per-_key. Concurrent UPSERTs from different
        # cycles on the same donor merge correctly (cycles array appended,
        # totals summed atomically inside the document lock).
        def _process_cycle(cycle: str) -> Dict[str, Any]:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                return {'cycle': cycle, 'skipped': True, 'reason': 'no_db'}

            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            if not cycle_db.has_collection("indiv"):
                return {'cycle': cycle, 'skipped': True, 'reason': 'no_indiv'}

            per_election_limit = PER_ELECTION_LIMITS.get(cycle, 3300)
            context.log.info(f"📊 Processing {cycle} - per-election limit: ${per_election_limit:,}...")
            
            # Triple-COLLECT aggregation with earmark-aware re-attribution:
            # 0. Per-record: derive effective_recipient from MEMO_TEXT when the
            #    record is a conduit-side earmark forward. A record like
            #    {CMTE_ID=ActBlue, MEMO="EARMARKED FOR <X> (CXXXXXXXX)"} gets
            #    effective_recipient = CXXXXXXXX, i.e. the actual target. A
            #    record without that memo pattern keeps effective_recipient =
            #    CMTE_ID. This makes the donor visible to the target committee
            #    when the target hasn't yet filed its own matching 15E receipts
            #    (the data-void case — Sue Altman pre-Q1-receipt-matching).
            # 1. COLLECT by (donor, effective_recipient, source) → split totals
            #    into direct and earmark streams.
            # 2. Per-(donor, effective_recipient): MAX(direct, earmark) — dedupes
            #    against the case where the recipient HAS filed matching 15E
            #    receipts for the same money. If direct >= earmark, the
            #    recipient is fully matched; if direct < earmark (or = 0), the
            #    earmark side has the more complete picture.
            # 3. COLLECT by (donor) → roll up per-donor totals, track max
            #    per-committee amount.
            # 4. FILTER max_single_cmte >= per-election limit (whale threshold).
            #
            # Notes:
            # - The "EARMARKED FOR" pattern distinguishes conduit-side forwards
            #   from recipient-side receipts (which say "EARMARKED CONTRIBUTION:
            #   SEE BELOW"). Only forwards have a parenthetical CMTE_ID target.
            # - transaction_count is approximate (may slightly over-count when
            #   earmark and direct paths exist for the same donor-recipient
            #   pair); not load-bearing.
            # - Filter to IND/CAN entities only — ORG/PAC/COM/CCM/PTY belong
            #   in transferred_to, not donors.
            aql = """
            FOR doc IN indiv
                FILTER doc.ENTITY_TP IN ['IND', 'CAN']
                FILTER doc.NAME != null AND doc.NAME != ""
                FILTER doc.TRANSACTION_AMT != null

                /* Parse earmark target CMTE_ID from MEMO_TEXT.
                   Primary: parenthetical CMTE_ID like (C00937730) — 99% of
                   earmark records.
                   Fallback: name-match against @name_map for the ~1% of
                   earmark records that have memos like
                   "EARMARKED FOR JON OSSOFF FOR SENATE" (no CMTE_ID parens)
                   or "EARMARKED FOR ALTMAN FOR CONGRESS (PENDING)".
                   The name extracted is whatever follows "EARMARKED FOR ",
                   trimmed of trailing parens/whitespace.
                */
                LET memo_upper = UPPER(doc.MEMO_TEXT || "")
                LET has_earmark = CONTAINS(memo_upper, "EARMARKED FOR")
                LET earmark_match = REGEX_MATCHES(memo_upper, "\\\\(C\\\\d{8}\\\\)", false)
                LET parsed_target_by_id = (
                    has_earmark AND LENGTH(earmark_match) > 0
                    ? SUBSTRING(earmark_match[0], 1, 9)
                    : null
                )
                LET earmark_pos = POSITION(memo_upper, "EARMARKED FOR ")
                LET after_earmark = (has_earmark AND parsed_target_by_id == null)
                    ? SUBSTRING(memo_upper, earmark_pos + 14)
                    : null
                LET name_only = after_earmark != null
                    ? TRIM(REGEX_REPLACE(after_earmark, "\\\\s*\\\\(.*$", ""))
                    : null
                LET parsed_target_by_name = name_only != null
                    ? @name_map[name_only]
                    : null
                LET parsed_target = parsed_target_by_id != null
                    ? parsed_target_by_id
                    : parsed_target_by_name
                LET effective_cmte = (parsed_target != null AND parsed_target != doc.CMTE_ID)
                    ? parsed_target
                    : doc.CMTE_ID
                LET is_redirect = effective_cmte != doc.CMTE_ID

                /* First COLLECT: per (donor, effective_recipient) totals split
                   by attribution source */
                COLLECT
                    name = doc.NAME,
                    employer = (doc.EMPLOYER == null OR doc.EMPLOYER == "") ? "NOT EMPLOYED" : doc.EMPLOYER,
                    eff_cmte = effective_cmte
                AGGREGATE
                    direct_total = SUM(is_redirect ? 0 : TO_NUMBER(doc.TRANSACTION_AMT)),
                    earmark_total = SUM(is_redirect ? TO_NUMBER(doc.TRANSACTION_AMT) : 0),
                    pair_count = COUNT(1)

                /* MAX dedup: the recipient may have filed matching 15E receipts
                   for the same money — taking MAX avoids double-counting */
                LET cmte_total = direct_total > earmark_total ? direct_total : earmark_total

                /* Second COLLECT: roll up to per-donor totals, track max committee */
                COLLECT
                    d_name = name,
                    d_employer = employer
                AGGREGATE
                    total_amount = SUM(cmte_total),
                    transaction_count = SUM(pair_count),
                    max_single_cmte = MAX(cmte_total)

                /* Only keep donors who maxed out to at least one committee */
                FILTER max_single_cmte >= @limit

                RETURN {
                    name: d_name,
                    employer: d_employer,
                    total_amount: total_amount,
                    transaction_count: transaction_count
                }
            """
            
            cursor = cycle_db.aql.execute(
                aql,
                bind_vars={
                    "limit": per_election_limit,
                    "name_map": name_maps_by_cycle.get(cycle, {}),
                },
                ttl=14400,  # 4 hours - double COLLECT is heavier than single
                batch_size=config.batch_size,
                stream=True
            )

            cycle_donors = 0
            cycle_total = 0.0
            cycle_inserted = 0
            batch = []

            for record in cursor:
                donor_key = make_donor_key(record['name'], record['employer'])
                doc = {
                    '_key': donor_key,
                    'canonical_name': record['name'],
                    'canonical_employer': record['employer'],
                    'total_amount': record['total_amount'],
                    'transaction_count': record['transaction_count'],
                    'cycles': [cycle],
                    'updated_at': datetime.now().isoformat(),
                }
                batch.append(doc)
                cycle_donors += 1
                cycle_total += record['total_amount']

                if len(batch) >= config.batch_size:
                    upsert_donors_batch(agg_db, batch, cycle)
                    cycle_inserted += len(batch)
                    batch = []
                    if cycle_donors % 25000 == 0:
                        context.log.info(f"  [{cycle}] {cycle_donors:,} donors...")
                        gc.collect()

            if batch:
                upsert_donors_batch(agg_db, batch, cycle)
                cycle_inserted += len(batch)

            context.log.info(f"✅ {cycle}: {cycle_donors:,} donors, ${cycle_total:,.0f}")
            gc.collect()
            return {
                'cycle': cycle,
                'per_election_limit': per_election_limit,
                'donors_above_threshold': cycle_donors,
                'total_contributed': cycle_total,
                'inserted': cycle_inserted,
                'skipped': False,
            }

        results = parallel_cycles(_process_cycle, config.cycles, max_workers=4)
        for cycle in config.cycles:
            r = results.get(cycle)
            if not r:
                continue
            if r.get('skipped'):
                context.log.warning(f"  {cycle}: skipped ({r.get('reason')})")
                continue
            stats['total_donors_inserted'] += r['inserted']
            stats['by_cycle'][cycle] = {
                'per_election_limit': r['per_election_limit'],
                'donors_above_threshold': r['donors_above_threshold'],
                'total_contributed': r['total_contributed'],
            }
            stats['cycles_processed'] += 1
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        donors_coll = agg_db.collection("donors")
        donors_coll.add_persistent_index(fields=["canonical_name"])
        donors_coll.add_persistent_index(fields=["canonical_employer"])
        donors_coll.add_persistent_index(fields=["total_amount"])
        
        final_count = donors_coll.count()
        
        context.log.info(f"🎉 Complete: {final_count:,} unique donors")
        
        return Output(
            value=stats,
            metadata={
                "donors_count": final_count,
                "threshold_type": "per_election_maxout",
                "per_election_limits": MetadataValue.json(
                    {c: PER_ELECTION_LIMITS.get(c) for c in config.cycles}
                ),
                "cycles_processed": stats['cycles_processed'],
                "by_cycle": MetadataValue.json(stats['by_cycle']),
            }
        )
