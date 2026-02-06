#!/usr/bin/env python3
"""Compute committee receipt totals from raw FEC data."""
import gc
from arango import ArangoClient

print("Computing committee receipts from raw FEC data...")

client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
sys_db = client.db('_system', username='root', password='ltpass')
agg_db = client.db('aggregation', username='root', password='ltpass')

cycles = [2020, 2022, 2024]

# Get whale totals from contributed_to edges
print("Phase 1: Getting whale totals from contributed_to edges...")
whale_totals = {}
cursor = agg_db.aql.execute("""
    FOR e IN contributed_to
    COLLECT cmte_id = SPLIT(e._to, '/')[1]
    AGGREGATE total = SUM(e.total_amount)
    RETURN { cmte_id, total }
""", ttl=3600)
for row in cursor:
    whale_totals[row['cmte_id']] = row['total'] or 0
print(f"  Got whale totals for {len(whale_totals):,} committees")

# Get total individual contributions from raw FEC data
print("Phase 2: Getting total individual contributions from raw FEC...")
indiv_totals = {}
for cycle in cycles:
    db_name = f"fec_{cycle}"
    if db_name not in sys_db.databases():
        continue
    
    cycle_db = client.db(db_name, username='root', password='ltpass')
    
    # indiv collection
    if cycle_db.has_collection('indiv'):
        cursor = cycle_db.aql.execute("""
            FOR r IN indiv
            FILTER r.TRANSACTION_AMT > 0
            COLLECT cmte_id = r.CMTE_ID
            AGGREGATE total = SUM(r.TRANSACTION_AMT)
            RETURN {cmte_id, total}
        """, ttl=3600)
        for row in cursor:
            cmte_id = row['cmte_id']
            indiv_totals[cmte_id] = indiv_totals.get(cmte_id, 0) + (row['total'] or 0)
    
    print(f"  Processed indiv for {cycle}")
    gc.collect()

print(f"  Total individual contributions for {len(indiv_totals):,} committees")

# Get transfer totals from transferred_to
print("Phase 3: Getting transfer totals...")
transfer_totals = {}
cursor = agg_db.aql.execute("""
    FOR e IN transferred_to
    COLLECT cmte_id = SPLIT(e._to, '/')[1]
    AGGREGATE total = SUM(e.total_amount)
    RETURN {cmte_id, total}
""", ttl=3600)
for row in cursor:
    transfer_totals[row['cmte_id']] = row['total'] or 0
print(f"  Got transfer totals for {len(transfer_totals):,} committees")

# Update committees with financial data
print("Phase 4: Updating committees...")
batch = []
updated = 0

for cmte_id in set(indiv_totals.keys()) | set(transfer_totals.keys()) | set(whale_totals.keys()):
    total_individuals = indiv_totals.get(cmte_id, 0)
    total_transfers = transfer_totals.get(cmte_id, 0)
    whale_amount = whale_totals.get(cmte_id, 0)
    small_donor_amount = max(0, total_individuals - whale_amount)
    total_receipts = total_individuals + total_transfers
    
    batch.append({
        '_key': cmte_id,
        'total_receipts': total_receipts,
        'total_individuals': total_individuals,
        'total_transfers': total_transfers,
        'whale_amount': whale_amount,
        'small_donor_amount': small_donor_amount,
    })
    
    if len(batch) >= 1000:
        agg_db.aql.execute("""
            FOR doc IN @batch
            UPDATE doc._key WITH {
                total_receipts: doc.total_receipts,
                total_individuals: doc.total_individuals,
                total_transfers: doc.total_transfers,
                whale_amount: doc.whale_amount,
                small_donor_amount: doc.small_donor_amount
            } IN committees
            OPTIONS {ignoreErrors: true}
        """, bind_vars={'batch': batch})
        updated += len(batch)
        print(f"  Updated {updated:,} committees...")
        batch = []

if batch:
    agg_db.aql.execute("""
        FOR doc IN @batch
        UPDATE doc._key WITH {
            total_receipts: doc.total_receipts,
            total_individuals: doc.total_individuals,
            total_transfers: doc.total_transfers,
            whale_amount: doc.whale_amount,
            small_donor_amount: doc.small_donor_amount
        } IN committees
        OPTIONS {ignoreErrors: true}
    """, bind_vars={'batch': batch})
    updated += len(batch)

print(f"\nDone! Updated {updated:,} committees with financial data")

# Verify
result = list(agg_db.aql.execute("""
    FOR c IN committees
    FILTER c.total_receipts > 0
    COLLECT WITH COUNT INTO cnt
    RETURN cnt
"""))
print(f"Committees with receipts > 0: {result[0]:,}")

result = list(agg_db.aql.execute("""
    FOR c IN committees
    FILTER c.terminal_type == 'corporation' AND c.total_receipts > 0
    SORT c.total_receipts DESC
    LIMIT 5
    RETURN {name: c.CMTE_NM, receipts: c.total_receipts}
"""))
print("\nTop 5 corporate PACs by receipts:")
for r in result:
    print(f"  {r['name'][:40]}: ${r['receipts']/1e6:.1f}M")
