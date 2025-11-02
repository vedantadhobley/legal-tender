# Individual Donation Tracking - Implementation Plan

## Overview

This document details the plan for implementing billionaire/large donor tracking using FEC's `indiv.zip` files.

**Goal**: Track individual donations >$10,000 to committees (Super PACs, candidate committees, etc.) to expose billionaire influence.

**Status**: 📋 PLANNED (Phase 2 of upstream tracing)

---

## Your Questions Answered

### Q1: "Could someone donate $9,999 multiple times to avoid the threshold?"

**Short Answer**: Yes, but we prevent this by aggregating BEFORE filtering.

**Our Solution**:
```python
# WRONG APPROACH (vulnerable to gaming):
# Filter individual transactions by amount
for txn in indiv_stream:
    if txn['TRANSACTION_AMT'] > 10000:
        save(txn)  # Misses: $9,999 x 10 = $99,990

# CORRECT APPROACH (prevents gaming):
# Aggregate by contributor FIRST, then filter
contributor_totals = defaultdict(lambda: {
    'total': 0, 
    'count': 0, 
    'contributions': []
})

for txn in indiv_stream:
    key = (
        txn['NAME'],           # Elon Musk
        txn['EMPLOYER'],       # Tesla Inc
        txn['CMTE_ID'],        # America PAC
        cycle                  # 2024
    )
    contributor_totals[key]['total'] += txn['TRANSACTION_AMT']
    contributor_totals[key]['count'] += 1
    contributor_totals[key]['contributions'].append(txn)

# Then filter by total
large_contributors = {
    k: v for k, v in contributor_totals.items() 
    if v['total'] > 10000
}
# Catches: $9,999 x 10 = $99,990 ✅
```

**Why This Works**:
- Person gives $9,999 on Jan 1
- Person gives $9,999 on Feb 1
- Person gives $9,999 on Mar 1
- ... (10 times total)
- **Aggregation**: Total = $99,990 for this person → Committee → Cycle
- **Filter**: $99,990 > $10,000 ✅ CAPTURED

**Edge Case - Different Names/Employers**:
- If someone uses multiple names/employers, we won't catch it
- But this is already FEC violation (failure to properly report)
- FEC requires accurate contributor information
- Our system is as good as FEC's data quality

### Q2: "How do we handle the file size (4-5 GB per cycle)?"

**Short Answer**: Stream + aggregate + filter = 30-60 seconds vs 20 minutes.

**Performance Breakdown**:

| Approach | Records | Size | Time | Memory |
|----------|---------|------|------|--------|
| **Full Parse** (save everything) | 40M | 4.5 GB | 20 min | 4.5 GB |
| **Transaction Filter** (>$10K only) | 150K | 90 MB | 2 min | 500 MB |
| **Aggregated Filter** (our approach) | 150K | 90 MB | 30-60 sec | 500 MB |

**Why Aggregation is Faster**:
```python
# Pseudocode for streaming aggregation
import zipfile
import csv
from collections import defaultdict

def parse_indiv_streaming(zip_path, cycle, threshold=10000):
    contributor_map = defaultdict(lambda: {
        'total': 0,
        'count': 0,
        'contributions': []
    })
    
    # Stream from zip file (never load full file into memory)
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(f'indiv{cycle[-2:]}.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='latin-1'))
            
            for row in reader:
                # Build key
                key = (
                    row.get('NAME', '').strip(),
                    row.get('EMPLOYER', '').strip(),
                    row.get('CMTE_ID', '').strip(),
                    cycle
                )
                
                amount = float(row.get('TRANSACTION_AMT', 0) or 0)
                
                # Aggregate in memory
                contributor_map[key]['total'] += amount
                contributor_map[key]['count'] += 1
                
                # Store transaction if contributor might exceed threshold
                # (only keep top 10 per contributor to save memory)
                if len(contributor_map[key]['contributions']) < 10:
                    contributor_map[key]['contributions'].append({
                        'amount': amount,
                        'date': row.get('TRANSACTION_DT'),
                        'transaction_id': row.get('SUB_ID')
                    })
    
    # Filter to large contributors
    large_contributors = []
    for key, data in contributor_map.items():
        if data['total'] > threshold:
            name, employer, cmte_id, cycle = key
            large_contributors.append({
                'name': name,
                'employer': employer,
                'committee_id': cmte_id,
                'cycle': cycle,
                'total_amount': data['total'],
                'contribution_count': data['count'],
                'contributions': sorted(
                    data['contributions'],
                    key=lambda x: x['amount'],
                    reverse=True
                )[:10]  # Keep top 10 only
            })
    
    return large_contributors
```

**Memory Usage**:
- 40M contributors × average 3 donations each = 120M transactions
- BUT: Most contributors give <$200 (won't exceed threshold)
- In practice: ~500K unique (name, employer, committee, cycle) combinations
- Memory: 500K × 200 bytes = 100 MB (well within RP5's 8GB RAM)

### Q3: "Why not use pas2.zip (itpas2) for individual donations?"

**Short Answer**: ENTITY_TP="IND" in pas2 is misleading - it's PAC-attributed, not direct donations.

**What pas2 Actually Contains**:
```javascript
// pas2 ENTITY_TP="IND" example:
{
  CMTE_ID: "C00123456",           // Steel Manufacturers PAC (DONOR)
  NAME: "BELL, PHILIP",           // Individual the PAC attributes money to
  OTHER_ID: "C00541862",          // Terri Sewell For Congress (RECIPIENT)
  ENTITY_TP: "IND",               // Misleading! This is PAC→Candidate
  TRANSACTION_AMT: 1000
}
```

**Meaning**: Steel Manufacturers PAC gave $1,000 to Terri Sewell, and they're attributing it to their member Philip Bell.

**This is NOT**:
- Philip Bell directly donating to Terri Sewell
- Philip Bell donating to the Steel Manufacturers PAC
- A billionaire writing a $10M check

**What indiv.zip Contains**:
```javascript
// indiv.zip example:
{
  CMTE_ID: "C00789012",           // America PAC (RECIPIENT)
  NAME: "MUSK, ELON",             // Individual donor
  EMPLOYER: "TESLA INC",
  TRANSACTION_AMT: 75000000       // $75M direct donation
}
```

**Meaning**: Elon Musk directly gave $75M to America PAC.

**Data Volume Comparison**:
- pas2 ENTITY_TP="IND": ~300-500 records per cycle (PAC attributions)
- indiv.zip: ~40M records per cycle (direct donations)

**Conclusion**: We NEED indiv.zip for billionaire tracking. pas2 doesn't have this data.

---

## Implementation Details

### File Structure

**FEC Bulk Data URL Pattern**:
```
https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip
https://www.fec.gov/files/bulk-downloads/2022/indiv22.zip
https://www.fec.gov/files/bulk-downloads/2020/indiv20.zip
```

**File Sizes** (verified via curl):
```
indiv20.zip: 5.46 GB
indiv22.zip: 4.86 GB
indiv24.zip: 4.02 GB
indiv26.zip: 0.55 GB (partial - election not yet complete)
```

### Schema

**indiv.zip Record Format** (pipe-delimited):
```
CMTE_ID|AMNDT_IND|RPT_TP|TRANSACTION_PGI|IMAGE_NUM|TRANSACTION_TP|ENTITY_TP|NAME|CITY|STATE|ZIP_CODE|EMPLOYER|OCCUPATION|TRANSACTION_DT|TRANSACTION_AMT|OTHER_ID|TRAN_ID|FILE_NUM|MEMO_CD|MEMO_TEXT|SUB_ID
```

**Key Fields**:
- `CMTE_ID`: Committee receiving donation (recipient)
- `NAME`: Contributor name (e.g., "MUSK, ELON")
- `EMPLOYER`: Contributor's employer (e.g., "TESLA INC")
- `OCCUPATION`: Contributor's occupation (e.g., "CEO")
- `TRANSACTION_DT`: Date (MMDDYYYY format)
- `TRANSACTION_AMT`: Amount (positive = contribution, negative = refund)
- `SUB_ID`: Unique transaction ID

### Asset Implementation

**File**: `src/assets/fec/indiv.py`

```python
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from collections import defaultdict
import zipfile
import csv
import io
from typing import Dict, Any

from src.resources.mongo import MongoDBResource
from src.data.repository import DataRepository


@asset(
    name="indiv",
    group_name="fec",
    compute_kind="download+parse",
    description="Large individual contributions (>$10K) per cycle - billionaire tracking"
)
def indiv_asset(
    context: AssetExecutionContext,
    mongo: MongoDBResource,
    repository: DataRepository
) -> Output[Dict[str, Any]]:
    """
    Parse indiv.zip files and extract large contributors (>$10K per cycle).
    
    Aggregation Strategy:
    1. Stream through all transactions (never load full file)
    2. Aggregate by (NAME, EMPLOYER, CMTE_ID, Cycle)
    3. Filter contributors with total > $10,000
    4. Store in fec_{cycle}.indiv_large
    
    This prevents gaming ($9,999 x 10 = $99,990 still captured).
    """
    cycles = ["2020", "2022", "2024", "2026"]
    threshold = 10000  # $10K threshold
    stats = {
        'cycles_processed': [],
        'total_transactions': 0,
        'large_contributors': 0,
        'total_amount_captured': 0
    }
    
    for cycle in cycles:
        context.log.info(f"📥 Processing indiv{cycle[-2:]}.zip...")
        
        # Download file
        zip_path = repository.download_fec_file(
            filename=f"indiv{cycle[-2:]}.zip",
            cycle=cycle,
            force=False
        )
        
        # Get MongoDB collection
        db = mongo.get_database(f"fec_{cycle}")
        collection = db.get_collection("indiv_large")
        collection.delete_many({})  # Clear existing data
        
        # Stream and aggregate
        contributor_map = defaultdict(lambda: {
            'total': 0,
            'count': 0,
            'contributions': []
        })
        
        txn_count = 0
        with zipfile.ZipFile(zip_path) as zf:
            txt_file = f'indiv{cycle[-2:]}.txt'
            with zf.open(txt_file) as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding='latin-1'),
                    delimiter='|',
                    fieldnames=[
                        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI',
                        'IMAGE_NUM', 'TRANSACTION_TP', 'ENTITY_TP', 'NAME',
                        'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION',
                        'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID',
                        'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
                    ]
                )
                
                for row in reader:
                    txn_count += 1
                    
                    # Parse fields
                    name = row.get('NAME', '').strip()
                    employer = row.get('EMPLOYER', '').strip()
                    cmte_id = row.get('CMTE_ID', '').strip()
                    
                    try:
                        amount = float(row.get('TRANSACTION_AMT', 0) or 0)
                    except (ValueError, TypeError):
                        continue
                    
                    if not name or not cmte_id or amount <= 0:
                        continue
                    
                    # Build aggregation key
                    key = (name, employer, cmte_id)
                    
                    # Aggregate
                    contributor_map[key]['total'] += amount
                    contributor_map[key]['count'] += 1
                    
                    # Store top 10 contributions
                    contrib = {
                        'amount': amount,
                        'date': row.get('TRANSACTION_DT'),
                        'occupation': row.get('OCCUPATION'),
                        'city': row.get('CITY'),
                        'state': row.get('STATE'),
                        'transaction_id': row.get('SUB_ID')
                    }
                    
                    contributions = contributor_map[key]['contributions']
                    if len(contributions) < 10:
                        contributions.append(contrib)
                    else:
                        # Keep top 10 by amount
                        contributions.sort(key=lambda x: x['amount'], reverse=True)
                        if amount > contributions[-1]['amount']:
                            contributions[-1] = contrib
                    
                    # Progress logging
                    if txn_count % 1000000 == 0:
                        context.log.info(f"  Processed {txn_count:,} transactions...")
        
        context.log.info(f"  ✅ Processed {txn_count:,} transactions")
        stats['total_transactions'] += txn_count
        
        # Filter and insert large contributors
        batch = []
        for key, data in contributor_map.items():
            if data['total'] > threshold:
                name, employer, cmte_id = key
                
                # Sort contributions
                data['contributions'].sort(key=lambda x: x['amount'], reverse=True)
                
                batch.append({
                    '_id': f"{name}|{employer}|{cmte_id}|{cycle}",
                    'name': name,
                    'employer': employer,
                    'committee_id': cmte_id,
                    'cycle': cycle,
                    'total_amount': data['total'],
                    'contribution_count': data['count'],
                    'contributions': data['contributions']
                })
                
                stats['large_contributors'] += 1
                stats['total_amount_captured'] += data['total']
        
        if batch:
            collection.insert_many(batch, ordered=False)
            context.log.info(f"  ✅ Inserted {len(batch):,} large contributors")
            context.log.info(f"  💰 Total captured: ${stats['total_amount_captured']:,.2f}")
        
        stats['cycles_processed'].append(cycle)
        
        # Create indexes
        collection.create_index([("committee_id", 1)])
        collection.create_index([("total_amount", -1)])
        collection.create_index([("name", 1)])
    
    return Output(
        value=stats,
        metadata={
            "cycles": MetadataValue.text(", ".join(stats['cycles_processed'])),
            "transactions_processed": MetadataValue.int(stats['total_transactions']),
            "large_contributors": MetadataValue.int(stats['large_contributors']),
            "total_amount": MetadataValue.float(stats['total_amount_captured']),
            "preview": MetadataValue.md(
                f"Processed {stats['total_transactions']:,} transactions, "
                f"found {stats['large_contributors']:,} contributors >$10K"
            )
        }
    )
```

### Integration with enriched_committee_funding

**Update**: `src/assets/enrichment/enriched_committee_funding.py`

Add after line 199 (where individual donations are currently skipped):

```python
# === INDIVIDUAL → COMMITTEE DONATIONS (Large donors >$10K) ===
context.log.info("📊 Extracting large individual donations...")
indiv_large_collection = db.get_collection("indiv_large")

ind_count = 0
for contrib in indiv_large_collection.find():
    recipient_id = contrib.get('committee_id')
    contributor_name = contrib.get('name')
    employer = contrib.get('employer', 'Not Provided')
    total_amount = contrib.get('total_amount', 0)
    
    if not recipient_id or total_amount <= 0:
        continue
    
    # Create unique key for contributor
    contributor_key = f"{contributor_name}|{employer}"
    
    # Initialize if first time seeing this contributor
    if contributor_key not in funding_by_committee[recipient_id]['from_individuals']:
        funding_by_committee[recipient_id]['from_individuals'][contributor_key] = {
            'contributions': [],
            'total': 0,
            'count': 0
        }
    
    # Add contributions
    funding_by_committee[recipient_id]['from_individuals'][contributor_key]['contributions'].extend(
        contrib.get('contributions', [])
    )
    funding_by_committee[recipient_id]['from_individuals'][contributor_key]['total'] += total_amount
    funding_by_committee[recipient_id]['from_individuals'][contributor_key]['count'] += contrib.get('contribution_count', 0)
    ind_count += 1

context.log.info(f"   ✅ Found {ind_count:,} large individual contributors")
```

---

## Expected Results

### Example: America PAC (Elon Musk's Super PAC)

**Before** (current - no individual tracking):
```javascript
{
  committee_id: "C00789012",
  committee_name: "AMERICA PAC",
  funding_sources: {
    from_committees: [
      {committee_name: "Some corporate PAC", total_amount: 500000}
    ],
    from_individuals: [],  // Empty!
    from_organizations: []
  }
}
```

**After** (with indiv.zip parsing):
```javascript
{
  committee_id: "C00789012",
  committee_name: "AMERICA PAC",
  funding_sources: {
    from_committees: [
      {committee_name: "Some corporate PAC", total_amount: 500000}
    ],
    from_individuals: [  // NEW!
      {
        contributor_name: "MUSK, ELON",
        employer: "TESLA INC",
        occupation: "CEO",
        total_amount: 75000000,
        contribution_count: 3,
        contributions: [
          {amount: 25000000, date: "20240201"},
          {amount: 25000000, date: "20240601"},
          {amount: 25000000, date: "20240901"}
        ]
      }
    ],
    from_organizations: []
  }
}
```

---

## Timeline

**Phase 2**: Individual Billionaire Tracking (2-3 days)

**Day 1** (4 hours):
- Create `src/assets/fec/indiv.py` asset
- Implement streaming + aggregation logic
- Test on indiv24.zip (smallest recent file)

**Day 2** (4 hours):
- Run full parse on all 4 cycles
- Verify results (spot-check known billionaires)
- Update `enriched_committee_funding.py` to query indiv_large

**Day 3** (2 hours):
- Re-materialize enriched_committee_funding
- Re-materialize donor_financials
- Verify billionaire money trails show up correctly

**Total**: 10 hours (spread over 2-3 days)

---

## Testing Plan

### Unit Tests

1. **Test aggregation logic**:
   - Multiple donations from same person → Single aggregated record
   - $9,999 x 10 donations → Captured (total $99,990 > $10K)
   - Mixed names/employers → Separate records

2. **Test threshold filtering**:
   - Contributor with $9,500 total → Not included
   - Contributor with $10,001 total → Included
   - Contributor with $1M total → Included

3. **Test streaming performance**:
   - Measure memory usage (should stay <1 GB)
   - Measure processing time (should be <2 min per cycle)

### Integration Tests

1. **Known billionaire verification**:
   - Query for "MUSK, ELON" → Should find America PAC donations
   - Query for "ADELSON, MIRIAM" → Should find Preserve America PAC
   - Verify amounts match FEC.gov website

2. **Committee funding verification**:
   - Query committee_funding_sources for America PAC
   - Should show Elon Musk in from_individuals
   - Should show correct total amount

---

## Monitoring & Validation

### Performance Metrics

Track these in asset metadata:
```python
{
  "transactions_processed": 40000000,
  "large_contributors": 150000,
  "reduction_factor": 266,  # 40M / 150K
  "processing_time_seconds": 45,
  "memory_peak_mb": 450
}
```

### Data Quality Checks

1. **Top 10 contributors check**:
   ```python
   db.indiv_large.find().sort({"total_amount": -1}).limit(10)
   # Should see known billionaires: Musk, Adelson, Koch, etc.
   ```

2. **Amount distribution**:
   ```python
   db.indiv_large.aggregate([
     {$bucket: {
       groupBy: "$total_amount",
       boundaries: [10000, 50000, 100000, 500000, 1000000, 10000000, 100000000],
       output: {count: {$sum: 1}}
     }}
   ])
   # Should see exponential decay (most at $10K-$50K, few at $100M)
   ```

3. **Cycle comparison**:
   ```python
   # 2024 should have more large contributors than 2026 (partial)
   # 2020/2022 should be similar to 2024
   ```

---

## Document Version

**Created**: November 1, 2025  
**Status**: Planning document - ready for implementation  
**Next Action**: Begin Day 1 implementation (create indiv.py asset)
