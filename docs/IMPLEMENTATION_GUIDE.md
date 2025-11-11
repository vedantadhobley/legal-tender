# Legal Tender Implementation Guide

**Last Updated**: November 11, 2025

This document consolidates all technical implementation details, data model explanations, and architectural decisions for the Legal Tender project.

---

## Table of Contents

1. [FEC Data Model Understanding](#fec-data-model-understanding)
2. [Upstream Money Tracing Strategy](#upstream-money-tracing-strategy)
3. [MongoDB Structure](#mongodb-structure)
4. [Memory Optimization](#memory-optimization)
5. [Data Quality & Known Issues](#data-quality--known-issues)

---

## FEC Data Model Understanding

### Critical Discovery: itoth vs itcont vs itpas2

After extensive investigation and analysis of actual FEC data, here's what each file **actually** contains:

#### `pas2.zip` → itpas2 (Committee Contributions)
**What it is**: Schedule A - Committee contributions FROM the filer's perspective (money OUT)

- **CMTE_ID** = Committee FILING the report (DONOR giving money)
- **OTHER_ID** = Recipient committee ID (RECIPIENT receiving money)
- **CAND_ID** = Candidate person (if recipient is candidate committee)
- **ENTITY_TP** = Type of recipient
  - `"CCM"` = Candidate Committee (transfers to campaigns)
  - `"PAC"` = PAC-to-PAC transfers
  - `"COM"` = Committee transfers
  - `"PTY"` = Party committee transfers
  - `"IND"` = ⚠️ MISLEADING! Not direct individual donations - these are PAC→candidate transfers attributed to PAC members
  - `"ORG"` = ⚠️ MISLEADING! Not direct org donations - these are committee transfers with org attribution

**What it contains**:
- ✅ Committee → Candidate Committee transfers (24K transaction type)
- ✅ Independent expenditures (24A/24E transaction types)
- ✅ PAC → Candidate money flow
- ❌ Does NOT contain: Transfers to non-candidate committees (party committees, Super PACs)

**Size**: ~300 MB per cycle compressed, 2-5M records
**Our usage**: Track direct PAC influence on candidates

---

#### `oth.zip` → itoth (Other Receipts)
**What it is**: Schedule A - "Other receipts" showing WHO gave money TO committees (recipient perspective)

- **CMTE_ID** = Committee receiving the money (RECIPIENT)
- **OTHER_ID** = Donor committee ID (if committee-to-committee transfer) OR empty string
- **NAME** = Donor name (if individual/organization donation)
- **ENTITY_TP** = Type of donor
  - `"IND"` = Individual donations (17.1M records, 91% of file)
  - `"PAC"` = PAC-to-PAC transfers (156K records, $2.4B)
  - `"COM"` = Committee transfers (24K records, $2.4B)
  - `"PTY"` = Party committee transfers (112K records, $2.4B)
  - `"CCM"` = Candidate committee transfers/refunds (1.2M records)
  - `"ORG"` = Organization donations (85K records, $5.4B)

**What it contains**:
- ✅ PAC-to-PAC transfers (156K records) - **CRITICAL for upstream tracing**
- ✅ Committee-to-committee transfers (24K records)
- ✅ Individual donations to ANY committee type (17.1M records)
  - Including mega-donations to party committees ($860M over $6,600 limit)
  - Including donations to Super PACs
- ✅ Refunds, loans, adjustments
- ⚠️ 99.1% of IND records are form type **15J** (late/amended filings)

**Size**: ~483 MB for 2024 alone (931MB total across 4 cycles), 18.7M records
**Key insight**: This is NOT the primary source for individual donations!

**Critical finding**:
- itoth contains only **17.1M individual donation records**
- FEC documentation states itcont should have **~40M records per cycle**
- This means itoth has only ~43% of individual donations
- The 99.1% form 15J suggests these are **late-filed or amended contributions**, NOT the main dataset

---

#### `indiv.zip` → itcont (Individual Contributions) - PRIMARY SOURCE
**What it is**: Schedule A - Itemized individual contributions to ANY committee type

- **CMTE_ID** = Committee receiving the contribution (recipient)
- **OTHER_ID** = Donor committee ID (null for individuals, populated for committee donors)
- **NAME** = Individual donor name
- **EMPLOYER** = Donor's employer
- **OCCUPATION** = Donor's occupation
- **ENTITY_TP** = Entity type
  - `"IND"` = Individual person
  - Other entity types included per FEC rules

**Inclusion rules** (per FEC documentation):
- **2015-present**: Contributions >$200 per:
  - Election cycle-to-date for candidate committees
  - Calendar year-to-date for PACs and party committees
- **1989-2014**: Contributions ≥$200 per reporting period
- **1975-1988**: Contributions ≥$500 per reporting period

**What it contains**:
- ✅ ALL itemized individual donations >$200 to ANY committee
- ✅ Donations to candidate committees (limited to $6,600 per election)
- ✅ Donations to party committees (higher limits ~$400K per year)
- ✅ Donations to Super PACs (UNLIMITED amounts allowed!)
- ✅ Donations to corporate PACs
- ✅ Complete donor information (employer, occupation, address)

**Size**: 2-4 GB per cycle compressed, ~40M records
**Key insight**: This is the AUTHORITATIVE source for individual donations!

**Critical differences from itoth**:
- itcont: ~40M records (100% coverage of itemized donations)
- itoth: ~17M IND records (43% coverage, mostly late filings)
- itcont: All donation types and amounts >$200
- itoth: Mostly form 15J (late/amended filings)

---

### Summary: Which File for What Purpose?

| Purpose | Use This File | Filter | Why |
|---------|--------------|--------|-----|
| **PAC → Candidate transfers** | `pas2.zip` (itpas2) | `TRANSACTION_TP: "24K"` | Only file with candidate transfers |
| **Independent expenditures** | `pas2.zip` (itpas2) | `TRANSACTION_TP: "24A/24E"` | Super PAC ad spending |
| **PAC → PAC transfers** | `oth.zip` (itoth) | `ENTITY_TP: "PAC/COM/PTY"` | Shows upstream dark money flow |
| **Individual mega-donations** | `indiv.zip` (itcont) | `ENTITY_TP: "IND", amount ≥ $10K` | Primary source, 40M records |
| **Party committee mega-donors** | `indiv.zip` (itcont) | Recipient CMTE_TP: "Y", amount > $6,600 | UNLIMITED donations to parties |
| **Super PAC mega-donors** | `indiv.zip` (itcont) | Recipient CMTE_TP: "O/W/I", any amount | UNLIMITED donations to Super PACs |
| **Corporate PAC receipts** | `oth.zip` (itoth) | `ENTITY_TP: "PAC/COM/ORG"` | Who funds corporate PACs |

---

### Key Insights from Data Analysis

**Performance implications**:
```
itpas2: 621,922 records (2024) - FAST, filtered to candidates only
itoth:  18,758,696 records (2024) - SLOW, needs filtering at parse time
itcont: ~40,000,000 records (2024) - HUGE, stream with threshold filtering
```

**Filtering strategy for itoth** (98.4% reduction):
```python
# Filter at parse time to only committee-to-committee transfers
if fields[6] not in ["PAC", "COM", "PTY"]:  # ENTITY_TP
    continue  # Skip IND, CCM, ORG, CAN
    
# Result: 18.7M → 292K records (156K PAC + 24K COM + 112K PTY)
# Processing time: 20+ min → ~2 min
```

**Filtering strategy for itcont** (99.9% reduction):
```python
# Filter to high-dollar donations only
if float(fields[14]) < 10000:  # TRANSACTION_AMT
    continue  # Skip donations under $10K
    
# Result: ~40M → ~150K records
# Focus on billionaire/mega-donor influence only
```

---

## Upstream Money Tracing Strategy

### The Goal: Follow Corporate Money Through the System

**End-to-end money flow we want to trace**:
```
Corporate PAC (Boeing, AT&T)
  ↓ $500K (itoth - PAC entity type)
Political PAC / Party Committee (NRSC, DCCC)
  ↓ $100K (itpas2 - 24K transaction)
Candidate Committee (Ted Cruz)
  ↓ Receives money (appears in candidate financial summaries)
```

### Data Sources Required

1. **itpas2** (pas2.zip):
   - Shows: PAC → Candidate direct transfers
   - Shows: Independent expenditures FOR/AGAINST candidates
   - Filtered: Only transactions involving our 564 tracked candidates
   - Collection: `enriched_{cycle}.itpas2`

2. **itoth** (oth.zip) - **FILTERED**:
   - Shows: PAC → Political PAC/Party Committee transfers
   - Filter: `ENTITY_TP IN ["PAC", "COM", "PTY"]` only (ignore IND records)
   - Reduces: 18.7M → 292K records (98.4% reduction)
   - Collection: `fec_{cycle}.itoth`

3. **itcont** (indiv.zip) - **FUTURE PHASE**:
   - Shows: Individual mega-donations to any committee
   - Filter: `TRANSACTION_AMT >= $10,000` only
   - Reduces: 40M → ~150K records (99.6% reduction)
   - Collection: `fec_{cycle}.itcont` (not yet implemented)

4. **cm** (cm.zip):
   - Committee master data (names, types, affiliations)
   - Used to identify committee types and categorize transfers
   - Collection: `fec_{cycle}.cm`

### Implementation Architecture

```
Layer 1: Raw FEC Data (fec_{cycle} databases)
├── itpas2: Direct PAC → Candidate transfers
├── itoth: PAC → PAC/Party upstream transfers (FILTERED)
├── itcont: Individual mega-donations (FUTURE)
└── cm: Committee metadata

Layer 2: Enrichment (enriched_{cycle} databases)
├── enriched_itpas2: Filtered to 564 tracked candidates only
└── enriched_committee_funding: Committee funding sources with upstream

Layer 3: Aggregation (aggregation database)
├── candidate_financials: Per-candidate totals
├── donor_financials: Per-donor totals
└── committee_summaries: Per-committee totals
```

### enriched_committee_funding Schema

**Purpose**: Build complete funding picture for each committee that spends on candidates

```javascript
{
  _id: ObjectId(),
  cycle: "2024",
  committee_id: "C00027466",  // NRSC
  committee_name: "NATIONAL REPUBLICAN SENATORIAL COMMITTEE",
  committee_type: "Y",  // Party committee
  
  // Direct spending on candidates (from enriched_itpas2)
  direct_candidate_spending: {
    total: 25000000,  // Total spent on all candidates
    candidate_count: 35,  // Number of candidates supported
    transactions: [
      {
        candidate_id: "S8TX00106",  // Ted Cruz
        candidate_name: "CRUZ, RAFAEL EDWARD 'TED'",
        amount: 1500000,
        transaction_type: "24K",  // Direct transfer
        transaction_count: 15
      }
    ]
  },
  
  // Independent expenditures (from enriched_itpas2)
  independent_expenditures: {
    total_support: 50000000,
    total_oppose: 20000000,
    net_spending: 30000000,
    candidate_count: 45,
    transactions: [
      {
        candidate_id: "S8TX00106",
        candidate_name: "CRUZ, RAFAEL EDWARD 'TED'",
        support: 2000000,
        oppose: 0,
        net: 2000000,
        transaction_count: 25
      }
    ]
  },
  
  // Upstream funding sources (from itoth - PAC/COM/PTY only)
  upstream_funding: {
    total: 100000000,  // Total received from other committees
    from_committees: [
      {
        donor_committee_id: "C00024869",  // Boeing PAC
        donor_committee_name: "BOEING COMPANY POLITICAL ACTION COMMITTEE",
        donor_committee_type: "Q",  // Corporate PAC
        amount: 500000,
        transaction_count: 5
      },
      {
        donor_committee_id: "C00109017",  // AT&T PAC
        donor_committee_name: "AT&T INC. FEDERAL PAC",
        donor_committee_type: "Q",
        amount: 300000,
        transaction_count: 3
      }
    ]
  },
  
  // Metadata
  created_at: ISODate("2024-11-11T12:00:00Z"),
  updated_at: ISODate("2024-11-11T12:00:00Z")
}
```

**Query patterns**:

1. Find all corporate PACs funding a political committee:
```javascript
db.committee_funding_sources.findOne(
  { committee_id: "C00027466" },  // NRSC
  { "upstream_funding.from_committees": 1 }
)
// Shows: Boeing, AT&T, Lockheed, etc. funding NRSC
```

2. Trace money: Corporate PAC → Political PAC → Candidate:
```javascript
// Step 1: Find candidates NRSC supports
var nrsc = db.committee_funding_sources.findOne({committee_id: "C00027466"});
var tedCruz = nrsc.direct_candidate_spending.transactions.find(
  t => t.candidate_id === "S8TX00106"
);
// Ted Cruz got $1.5M from NRSC

// Step 2: Find who funds NRSC
var boeing = nrsc.upstream_funding.from_committees.find(
  c => c.donor_committee_id === "C00024869"
);
// Boeing gave $500K to NRSC

// Step 3: Calculate weighted influence
var nrscTotal = nrsc.upstream_funding.total;  // $100M
var boeingWeight = boeing.amount / nrscTotal;  // 0.005 (0.5%)
var boeingInfluence = tedCruz.amount * boeingWeight;  // $7,500

// Conclusion: Boeing indirectly gave Ted Cruz ~$7,500 through NRSC
```

### Performance Optimization: Filtering itoth at Parse Time

**Problem**: itoth has 18.7M records (91% are IND donations we don't need for upstream tracing)

**Solution**: Filter during parsing to only load committee-to-committee transfers

```python
# In itoth asset parsing loop
for line in txt_file:
    fields = line.strip().split('|')
    
    entity_tp = fields[6]  # ENTITY_TP position
    
    # CRITICAL: Skip individual donations (IND)
    # We only care about committee-to-committee transfers
    if entity_tp not in ["PAC", "COM", "PTY", "CCM", "ORG"]:
        continue  # Skip 17.1M IND records
    
    # Further optimization: Skip candidate committee transfers
    # (these are mostly refunds, not upstream influence)
    if entity_tp == "CCM":
        continue  # Skip 1.2M refund records
    
    # Now only parse PAC/COM/PTY/ORG (291K records)
    record = {
        'CMTE_ID': fields[0],  # Recipient committee
        'OTHER_ID': fields[15],  # Donor committee (if populated)
        'NAME': fields[7],  # Donor name
        'ENTITY_TP': entity_tp,
        'TRANSACTION_AMT': float(fields[14]),
        # ... other fields
    }
    
    batch.append(record)
```

**Result**:
- Before: 18.7M records, 20+ minutes processing
- After: 291K records, ~2 minutes processing
- Reduction: 98.4% fewer records, 90% faster
- Data loss: None (we don't need IND records for upstream tracing)

**Further optimization for upstream query**:
```python
# In enriched_committee_funding asset
# Query only PAC/COM/PTY entity types (committee donors)
upstream_query = raw_itoth_collection.find({
    "ENTITY_TP": {"$in": ["PAC", "COM", "PTY"]},  # Committee transfers only
    "TRANSACTION_AMT": {"$ne": None},
    "CMTE_ID": {"$in": political_pacs}  # Recipient is political PAC
})

# This queries 291K records instead of 18.7M
# Query time: <1 second instead of cancelled query
```

---

## MongoDB Structure

### Database Architecture

**Per-cycle raw data**: `fec_2020`, `fec_2022`, `fec_2024`, `fec_2026`
- One database per election cycle
- Collections named without year suffixes (simpler)
- Contains raw FEC data exactly as parsed

**Enriched data**: `enriched_2020`, `enriched_2022`, `enriched_2024`, `enriched_2026`
- One database per election cycle
- Contains filtered/enriched versions of raw data
- Example: `enriched_2024.itpas2` filtered to 564 tracked candidates

**Aggregated data**: `aggregation`
- Single database for cross-cycle analytics
- Contains aggregated totals and summary data
- Example: `candidate_financials`, `donor_financials`

**Application data**: `legal_tender`
- Single database for app-specific data
- Member mappings, lobbying data, etc.
- Example: `member_fec_mapping`

### Collection Schemas

#### fec_{cycle}.itpas2 (Raw)
```javascript
{
  _id: ObjectId(),
  CMTE_ID: "C00000000",  // Donor committee (filer)
  AMNDT_IND: "N",
  RPT_TP: "M10",
  TRANSACTION_PGI: "G2024",
  IMAGE_NUM: "202411120900001234",
  TRANSACTION_TP: "24K",  // or "24A", "24E"
  ENTITY_TP: "CCM",  // or "IND", "ORG", "PAC", etc.
  NAME: "COMMITTEE NAME",
  CITY: "Washington",
  STATE: "DC",
  ZIP_CODE: "20001",
  EMPLOYER: null,
  OCCUPATION: null,
  TRANSACTION_DT: "01152024",
  TRANSACTION_AMT: 5000.00,
  OTHER_ID: "C00111111",  // Recipient committee
  CAND_ID: "H8TX00000",  // Candidate (if applicable)
  TRAN_ID: "SA11AI.1234",
  FILE_NUM: 1234567,
  MEMO_CD: null,
  MEMO_TEXT: null,
  SUB_ID: "1234567890123456789"
}
```

#### enriched_{cycle}.itpas2 (Filtered to tracked candidates)
```javascript
{
  _id: ObjectId(),
  cycle: "2024",
  filer_committee_id: "C00000000",
  filer_committee_name: "SUPER PAC NAME",
  candidate_id: "H8TX00000",
  candidate_name: "DOE, JOHN",
  transaction_type: "24A",  // Normalized from TRANSACTION_TP
  amount: 5000.00,  // Normalized from TRANSACTION_AMT
  transaction_date: "2024-01-15",  // Parsed from TRANSACTION_DT
  support_oppose: "S",  // For 24A/24E only
  entity_type: "CCM",
  raw_record: { /* original record */ }
}
```

#### fec_{cycle}.itoth (Filtered to committee transfers only)
```javascript
{
  _id: ObjectId(),
  CMTE_ID: "C00027466",  // Recipient committee (NRSC)
  AMNDT_IND: "N",
  RPT_TP: "M10",
  TRANSACTION_PGI: "G2024",
  IMAGE_NUM: "202411120900001234",
  TRANSACTION_TP: null,  // Usually undefined in itoth
  ENTITY_TP: "PAC",  // Only PAC/COM/PTY/ORG (IND filtered out)
  NAME: "BOEING COMPANY PAC",
  CITY: "Arlington",
  STATE: "VA",
  ZIP_CODE: "22202",
  EMPLOYER: null,
  OCCUPATION: null,
  TRANSACTION_DT: "03152024",
  TRANSACTION_AMT: 500000.00,
  OTHER_ID: "C00024869",  // Donor committee (Boeing PAC)
  TRAN_ID: "SA11AI.5678",
  FILE_NUM: 7654321,
  MEMO_CD: null,
  MEMO_TEXT: null,
  SUB_ID: "9876543210987654321",
  FORM_TP: "10J"  // Often 15J for late filings
}
```

#### enriched_{cycle}.committee_funding_sources
```javascript
{
  _id: ObjectId(),
  cycle: "2024",
  committee_id: "C00027466",
  committee_name: "NRSC",
  committee_type: "Y",
  
  direct_candidate_spending: {
    total: 25000000,
    candidate_count: 35,
    transactions: [
      {
        candidate_id: "S8TX00106",
        candidate_name: "CRUZ, RAFAEL EDWARD 'TED'",
        amount: 1500000,
        transaction_type: "24K",
        transaction_count: 15
      }
    ]
  },
  
  independent_expenditures: {
    total_support: 50000000,
    total_oppose: 20000000,
    net_spending: 30000000,
    candidate_count: 45,
    transactions: [ /* ... */ ]
  },
  
  upstream_funding: {
    total: 100000000,
    from_committees: [
      {
        donor_committee_id: "C00024869",
        donor_committee_name: "BOEING COMPANY PAC",
        donor_committee_type: "Q",
        amount: 500000,
        transaction_count: 5
      }
    ]
  }
}
```

---

## Memory Optimization

### Dynamic Batch Sizing

The pipeline automatically calculates optimal batch size based on available system memory:

```python
def calculate_optimal_batch_size():
    """Calculate optimal batch size based on available memory."""
    mem = psutil.virtual_memory()
    available_gb = mem.available / (1024**3)
    
    # Use 50% of available memory for processing
    usable_gb = available_gb * 0.5
    
    # Each 1000 FEC records ≈ 80 MB in memory
    records_per_gb = 1000 / 0.08
    optimal_size = int(usable_gb * records_per_gb)
    
    # Clamp between 1K and 100K
    return max(1000, min(100000, optimal_size))
```

**Performance data** (16GB machine, 10GB free):
- Calculated batch size: 65,000 records
- Memory used: 11GB peak (stable)
- Processing rate: ~2.6M records/minute
- No memory swapping or crashes

### Streaming Processing Pattern

**All FEC parsers follow this pattern**:

```python
@asset
def parse_fec_file(context, mongo, data_sync):
    batch = []
    batch_size = calculate_optimal_batch_size()
    
    with zipfile.ZipFile(file_path) as zf:
        with zf.open(txt_file) as f:
            for line in f:
                # Parse line
                record = parse_line(line)
                
                # Optional: Filter at parse time
                if not should_include(record):
                    continue
                
                batch.append(record)
                
                # Flush when batch full
                if len(batch) >= batch_size:
                    collection.insert_many(batch, ordered=False)
                    batch.clear()  # Free memory immediately
                    
        # Flush remaining
        if batch:
            collection.insert_many(batch, ordered=False)
```

**Key principles**:
1. **Never load entire file into memory**
2. **Stream line-by-line** from ZIP
3. **Filter early** (at parse time, not after)
4. **Batch for performance** (bulk inserts are 900x faster)
5. **Clear immediately** (free memory after each batch)
6. **Handle remainders** (flush final partial batch)

---

## Data Quality & Known Issues

### Field Mapping Corrections (October 2025)

Several parsers had incorrect field mappings that caused data corruption. All have been fixed:

**webk.zip (PAC summaries)**:
- **Issue**: Used webl field indices (30 fields) on webk data (27 fields)
- **Impact**: coverage_through_date showed "0", coverage_from_date showed cash amounts
- **Fix**: Corrected all field mappings for 27-field format
- **Status**: ✅ Fixed

**cn.zip (Candidates)**:
- **Issue**: Wrong field positions for party, election_year, state, district
- **Fix**: Corrected to match FEC documentation
- **Status**: ✅ Fixed

**cm.zip (Committees)**:
- **Issue**: Committee type field (CMTE_TP) mapped incorrectly
- **Fix**: Corrected field position
- **Status**: ✅ Fixed

**ccl.zip (Linkages)**:
- **Issue**: Composite key generation could cause duplicates
- **Fix**: Use CAND_ID + CMTE_ID + CMTE_TP for unique key
- **Status**: ✅ Fixed

### Known Data Quirks

**Transaction types in itoth are mostly undefined**:
- 99%+ of itoth records have `TRANSACTION_TP: null`
- This is normal - itoth uses FORM_TP instead
- Most common: FORM_TP = "15J" (late filings)

**OTHER_ID in itoth is empty string, not null**:
- MongoDB counts empty strings as "not null"
- Use `OTHER_ID: {$ne: ""}` to filter for committee transfers
- Or better: Filter by ENTITY_TP in ["PAC", "COM", "PTY"]

**Independent expenditures are in pas2.zip, NOT oppexp.zip**:
- oppexp.zip = operating expenses (payroll, overhead)
- independent_expenditure_YYYY.csv = actual Super PAC spending
- Different file, different format, different data!

**itoth contains mostly late filings (15J forms)**:
- 99.1% of IND records are form type 15J
- These are late-filed or amended contributions
- NOT the primary dataset for individual donations
- Use itcont (indiv.zip) for complete individual donation data

---

## Future Implementation: itcont (indiv.zip)

### Why We Need itcont

**The problem with itoth for individual donations**:
- Only has 17.1M individual donation records
- 99.1% are form 15J (late/amended filings)
- Missing ~23M records compared to itcont
- Almost NO individual donations to PACs ($91K total)

**What itcont provides**:
- ~40M individual donation records per cycle (COMPLETE dataset)
- ALL donations >$200 to ANY committee type
- Includes mega-donations to party committees (UNLIMITED)
- Includes mega-donations to Super PACs (UNLIMITED)
- Complete donor information (employer, occupation)

### Implementation Strategy

**Phase 1**: Download and parse with high threshold filter
```python
@asset
def itcont_asset(context, mongo, data_sync):
    """Parse individual contributions, filtering to mega-donations only."""
    
    # Filter at parse time: only donations ≥ $10,000
    threshold = 10000
    batch = []
    batch_size = calculate_optimal_batch_size()
    skipped = 0
    kept = 0
    
    with zipfile.ZipFile(indiv_zip_path) as zf:
        with zf.open(f'indiv{cycle[-2:]}.txt') as f:
            for line in f:
                fields = line.strip().split('|')
                
                # Field 14 = TRANSACTION_AMT
                amount = float(fields[14] or 0)
                
                if amount < threshold:
                    skipped += 1
                    continue
                
                kept += 1
                record = parse_itcont_record(fields)
                batch.append(record)
                
                if len(batch) >= batch_size:
                    collection.insert_many(batch, ordered=False)
                    batch.clear()
                    
                    if kept % 10000 == 0:
                        context.log.info(
                            f"Parsed {kept:,} mega-donations "
                            f"(skipped {skipped:,} small donations)"
                        )
    
    context.log.info(
        f"Final: {kept:,} mega-donations, "
        f"{skipped:,} filtered out, "
        f"{(kept/(kept+skipped)*100):.2f}% reduction"
    )
```

**Expected results**:
- Input: ~40M records per cycle
- Output: ~150K records per cycle (mega-donations only)
- Reduction: 99.6%
- Processing time: ~5-10 minutes (with streaming + filtering)

**Phase 2**: Aggregate by donor for gaming prevention
```python
# Group by (name, employer, committee, cycle) before final insert
# This prevents gaming via multiple $9,999 donations
from collections import defaultdict

contributor_totals = defaultdict(lambda: {
    'total': 0,
    'count': 0,
    'contributions': []
})

for record in filtered_records:
    key = (
        record['NAME'],
        record['EMPLOYER'],
        record['CMTE_ID'],
        cycle
    )
    contributor_totals[key]['total'] += record['TRANSACTION_AMT']
    contributor_totals[key]['count'] += 1
    contributor_totals[key]['contributions'].append(record)

# Then insert aggregated records
mega_donors = [
    v for v in contributor_totals.values()
    if v['total'] >= threshold
]
```

**Phase 3**: Link to committee funding sources
```python
# Add individual mega-donor data to enriched_committee_funding
{
  upstream_funding: {
    from_committees: [ /* ... */ ],
    from_individuals: [  // NEW
      {
        donor_name: "MUSK, ELON",
        employer: "TESLA INC",
        total_amount: 75000000,
        contribution_count: 15,
        contributions: [ /* detailed records */ ]
      }
    ]
  }
}
```

---

## Conclusion

This implementation guide consolidates all technical knowledge for the Legal Tender project. Key takeaways:

1. **itpas2**: Direct PAC → Candidate transfers (enriched to tracked candidates)
2. **itoth**: PAC → PAC upstream transfers (filter to PAC/COM/PTY only, 98% reduction)
3. **itcont**: Individual mega-donations (future phase, filter to ≥$10K, 99.6% reduction)
4. **Memory**: Stream + batch + filter early = safe processing on 4-16GB machines
5. **MongoDB**: Per-cycle raw data + enriched data + cross-cycle aggregation

For detailed FEC file descriptions, see `FEC.md`.
For specific implementation questions, see code comments in `src/assets/`.
