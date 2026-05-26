"""Contributed To Edge Collection - Individual donations to committees.

Creates edges from donors to committees representing individual contributions.
Only includes donations from donors who qualified for a graph vertex (maxed out
to at least one committee at the FEC per-election limit).

Note: Once a donor qualifies, ALL of their contributions get edges — not just
the maxed-out one. This gives full visibility into their giving pattern.

MEMORY OPTIMIZATION: Streaming processing with immediate writes.

Source: fec_{cycle}.indiv + aggregation.donors
Target: aggregation.contributed_to (edge collection)
"""

from typing import Dict, Any, List
from datetime import datetime
from hashlib import sha256
import re
import gc

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config

from src.resources.arango import ArangoDBResource
from src.config import ACTIVE_CYCLES


class ContributedToConfig(Config):
    """Configuration for contributed_to edge asset."""
    cycles: List[str] = list(ACTIVE_CYCLES)
    batch_size: int = 5000


def normalize_name(name: str) -> str:
    """Normalize donor name."""
    if not name:
        return ""
    name = re.sub(r'\s*,\s*', ' ', name.upper().strip())
    name = re.sub(r'\s+', ' ', name)
    return name


def normalize_employer(employer: str) -> str:
    """Normalize employer name."""
    if not employer:
        return ""
    employer = employer.upper().strip()
    employer = re.sub(r'[,.]', '', employer)
    employer = re.sub(r'\s+', ' ', employer)
    return employer


def make_donor_key(name: str, employer: str) -> str:
    """Create donor _key from normalized name + employer."""
    normalized = f"{normalize_name(name)}|{normalize_employer(employer)}"
    return sha256(normalized.encode()).hexdigest()[:16]


@asset(
    name="contributed_to",
    description="Edges from donors to committees - individual contributions.",
    group_name="graph",
    compute_kind="graph_edge",
    deps=["donors", "indiv", "cm", "cn"],
)
def contributed_to_asset(
    context: AssetExecutionContext,
    config: ContributedToConfig,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create contributed_to edge collection - MEMORY EFFICIENT.
    
    Streams aggregated data and writes directly to ArangoDB using UPSERT.
    """
    
    with arango.get_client() as client:
        sys_db = client.db("_system", username=arango.username, password=arango.password)
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("donors"):
            raise RuntimeError("donors collection missing - run donors asset first")
        
        # Create/truncate edge collection
        if agg_db.has_collection("contributed_to"):
            agg_db.collection("contributed_to").truncate()
            context.log.info("Truncated contributed_to")
        else:
            agg_db.create_collection("contributed_to", edge=True)
            context.log.info("Created contributed_to edge collection")
        
        # Ensure committees exists
        if not agg_db.has_collection("committees"):
            agg_db.create_collection("committees")
        else:
            agg_db.collection("committees").truncate()
        
        # Ensure candidates exists  
        if not agg_db.has_collection("candidates"):
            agg_db.create_collection("candidates")
        else:
            agg_db.collection("candidates").truncate()
        
        # Copy committees and candidates from FEC data
        context.log.info("📋 Copying committees & candidates...")
        
        # Build committees dictionary with cycles array (handles multi-cycle committees)
        committees_dict = {}
        candidates_dict = {}
        
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            
            # Collect committees - merge cycles for same committee
            if cycle_db.has_collection("cm"):
                cursor = cycle_db.aql.execute(
                    "FOR doc IN cm RETURN doc",
                    ttl=3600, batch_size=5000, stream=True
                )
                for doc in cursor:
                    cmte_id = doc['CMTE_ID']
                    if cmte_id in committees_dict:
                        # Add this cycle to existing committee
                        if cycle not in committees_dict[cmte_id]['cycles']:
                            committees_dict[cmte_id]['cycles'].append(cycle)
                        # Update with latest data (later cycles take precedence for name changes etc)
                        for key in doc:
                            if key not in ('CMTE_ID', '_key', 'cycles'):
                                committees_dict[cmte_id][key] = doc[key]
                    else:
                        doc['_key'] = cmte_id
                        doc['cycles'] = [cycle]
                        committees_dict[cmte_id] = doc
                gc.collect()
            
            # Collect candidates - merge cycles for same candidate
            if cycle_db.has_collection("cn"):
                cursor = cycle_db.aql.execute(
                    "FOR doc IN cn RETURN doc",
                    ttl=3600, batch_size=5000, stream=True
                )
                for doc in cursor:
                    cand_id = doc['CAND_ID']
                    if cand_id in candidates_dict:
                        if cycle not in candidates_dict[cand_id]['cycles']:
                            candidates_dict[cand_id]['cycles'].append(cycle)
                        for key in doc:
                            if key not in ('CAND_ID', '_key', 'cycles'):
                                candidates_dict[cand_id][key] = doc[key]
                    else:
                        doc['_key'] = cand_id
                        doc['cycles'] = [cycle]
                        candidates_dict[cand_id] = doc
                gc.collect()
            
            context.log.info(f"  Collected from {cycle}")
        
        # Write committees in batches
        context.log.info(f"📋 Writing {len(committees_dict):,} committees...")
        batch = []
        for doc in committees_dict.values():
            batch.append(doc)
            if len(batch) >= 5000:
                agg_db.collection("committees").import_bulk(batch, on_duplicate="replace")
                batch = []
        if batch:
            agg_db.collection("committees").import_bulk(batch, on_duplicate="replace")
        gc.collect()
        
        # Write candidates in batches
        context.log.info(f"📋 Writing {len(candidates_dict):,} candidates...")
        batch = []
        for doc in candidates_dict.values():
            batch.append(doc)
            if len(batch) >= 5000:
                agg_db.collection("candidates").import_bulk(batch, on_duplicate="replace")
                batch = []
        if batch:
            agg_db.collection("candidates").import_bulk(batch, on_duplicate="replace")
        gc.collect()

        
        # Load donor keys into memory (this should be small - only $10K+ donors)
        context.log.info("📋 Loading donor keys...")
        donor_keys = set()
        cursor = agg_db.aql.execute("FOR d IN donors RETURN d._key", ttl=3600)
        for key in cursor:
            donor_keys.add(key)
        context.log.info(f"  {len(donor_keys):,} donors to match")

        # Per-cycle committee name → CMTE_ID map for name-match earmark
        # re-attribution. Mirrors donors.py logic. See that file for
        # the disambiguation rules.
        context.log.info("📋 Loading committee name → CMTE_ID maps per cycle...")
        all_committees = list(agg_db.aql.execute(
            "FOR cmte IN committees RETURN {key: cmte._key, name: cmte.CMTE_NM, cycles: cmte.cycles}"
        ))
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
            name_maps_by_cycle[cycle] = cycle_map

        stats = {'edges_created': 0, 'by_cycle': {}}
        
        # Process each cycle
        for cycle in config.cycles:
            db_name = f"fec_{cycle}"
            if not sys_db.has_database(db_name):
                continue
            
            cycle_db = client.db(db_name, username=arango.username, password=arango.password)
            if not cycle_db.has_collection("indiv"):
                continue
            
            context.log.info(f"📊 Processing {cycle} contributions...")
            
            # Aggregate by donor-effective_recipient with earmark-aware
            # re-attribution. Same algorithm as donors.py — see comments there
            # for the full rationale. Briefly:
            # - Records with MEMO_TEXT "EARMARKED FOR <X> (CXXXXXXXX)" attribute
            #   the donation to the target X, not to the conduit (ActBlue/etc).
            #   Critical for candidates whose own committees haven't yet filed
            #   matching 15E receipts — without this they show $0 itemized
            #   donations even when ActBlue has bundled $100K+ for them.
            # - MAX(direct, earmark) per (donor, effective_recipient) dedupes
            #   against recipients who HAVE filed matching 15E, so we don't
            #   double-count.
            # - The donor-NAME REGEX filter was deleted 2026-05-21: it was
            #   matching only 9 records across 2026, all legitimate individuals
            #   (McConduit, Conduitte). Conduits-as-donor have ENTITY_TP=COM
            #   and are already excluded by the IND/CAN filter.
            aql = """
            FOR doc IN indiv
                FILTER doc.ENTITY_TP IN ['IND', 'CAN']
                FILTER doc.NAME != null AND doc.NAME != ""
                FILTER doc.CMTE_ID != null
                FILTER doc.TRANSACTION_AMT != null

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

                COLLECT
                    name = doc.NAME,
                    employer = (doc.EMPLOYER == null OR doc.EMPLOYER == "") ? "NOT EMPLOYED" : doc.EMPLOYER,
                    cmte_id = effective_cmte
                AGGREGATE
                    direct_total = SUM(is_redirect ? 0 : TO_NUMBER(doc.TRANSACTION_AMT)),
                    earmark_total = SUM(is_redirect ? TO_NUMBER(doc.TRANSACTION_AMT) : 0),
                    transaction_count = COUNT(1)

                LET total_amount = direct_total > earmark_total ? direct_total : earmark_total

                FILTER total_amount > 0

                RETURN {
                    name: name,
                    employer: employer,
                    cmte_id: cmte_id,
                    total_amount: total_amount,
                    transaction_count: transaction_count
                }
            """
            
            cursor = cycle_db.aql.execute(
                aql,
                bind_vars={"name_map": name_maps_by_cycle.get(cycle, {})},
                ttl=7200, batch_size=config.batch_size, stream=True
            )
            
            cycle_edges = 0
            batch = []
            
            for record in cursor:
                donor_key = make_donor_key(record['name'], record['employer'])
                
                # Skip if donor not in our filtered list
                if donor_key not in donor_keys:
                    continue
                
                edge_key = f"{donor_key}_{record['cmte_id']}_{cycle}"
                
                edge = {
                    '_key': edge_key,
                    '_from': f"donors/{donor_key}",
                    '_to': f"committees/{record['cmte_id']}",
                    'total_amount': record['total_amount'],
                    'transaction_count': record['transaction_count'],
                    'cycle': cycle,
                    'updated_at': datetime.now().isoformat()
                }
                
                batch.append(edge)
                cycle_edges += 1
                
                if len(batch) >= config.batch_size:
                    agg_db.collection("contributed_to").import_bulk(batch, on_duplicate="replace")
                    stats['edges_created'] += len(batch)
                    batch = []
                    
                    if cycle_edges % 50000 == 0:
                        context.log.info(f"  [{cycle}] {cycle_edges:,} edges...")
                        gc.collect()
            
            if batch:
                agg_db.collection("contributed_to").import_bulk(batch, on_duplicate="replace")
                stats['edges_created'] += len(batch)
            
            stats['by_cycle'][cycle] = cycle_edges
            context.log.info(f"✅ {cycle}: {cycle_edges:,} edges")
            gc.collect()
        
        # Create indexes
        context.log.info("🔧 Creating indexes...")
        edges_coll = agg_db.collection("contributed_to")
        edges_coll.add_persistent_index(fields=["_from"])
        edges_coll.add_persistent_index(fields=["_to"])
        edges_coll.add_persistent_index(fields=["total_amount"])
        
        context.log.info(f"🎉 Complete: {stats['edges_created']:,} edges")
        
        return Output(
            value=stats,
            metadata={
                "edges_count": stats['edges_created'],
                "by_cycle": MetadataValue.json(stats['by_cycle']),
            }
        )
