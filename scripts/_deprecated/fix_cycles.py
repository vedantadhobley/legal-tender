#!/usr/bin/env python3
"""Fix cycles field on committees and candidates collections."""
import gc
from arango import ArangoClient

client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
sys_db = client.db('_system', username='root', password='ltpass')
agg_db = client.db('aggregation', username='root', password='ltpass')

cycles = [2020, 2022, 2024]

# Build committees dictionary with cycles array
committees_dict = {}
candidates_dict = {}

for cycle in cycles:
    db_name = f"fec_{cycle}"
    if db_name not in sys_db.databases():
        continue
    
    cycle_db = client.db(db_name, username='root', password='ltpass')
    
    # Collect committees
    if cycle_db.has_collection("cm"):
        cursor = cycle_db.aql.execute("FOR doc IN cm RETURN doc", ttl=3600)
        for doc in cursor:
            cmte_id = doc['CMTE_ID']
            if cmte_id in committees_dict:
                if cycle not in committees_dict[cmte_id]['cycles']:
                    committees_dict[cmte_id]['cycles'].append(cycle)
                for key in doc:
                    if key not in ('CMTE_ID', '_key', 'cycles'):
                        committees_dict[cmte_id][key] = doc[key]
            else:
                doc['_key'] = cmte_id
                doc['cycles'] = [cycle]
                committees_dict[cmte_id] = doc
        gc.collect()
    
    # Collect candidates
    if cycle_db.has_collection("cn"):
        cursor = cycle_db.aql.execute("FOR doc IN cn RETURN doc", ttl=3600)
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
    
    print(f"Collected from {cycle}")

print(f"\nWriting {len(committees_dict):,} committees...")
agg_db.collection("committees").truncate()
batch = []
for doc in committees_dict.values():
    batch.append(doc)
    if len(batch) >= 5000:
        agg_db.collection("committees").import_bulk(batch, on_duplicate="replace")
        batch = []
if batch:
    agg_db.collection("committees").import_bulk(batch, on_duplicate="replace")
gc.collect()

print(f"Writing {len(candidates_dict):,} candidates...")
agg_db.collection("candidates").truncate()
batch = []
for doc in candidates_dict.values():
    batch.append(doc)
    if len(batch) >= 5000:
        agg_db.collection("candidates").import_bulk(batch, on_duplicate="replace")
        batch = []
if batch:
    agg_db.collection("candidates").import_bulk(batch, on_duplicate="replace")
gc.collect()

print("Done!")
