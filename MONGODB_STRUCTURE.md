# MongoDB Structure for Legal Tender

## Architecture Overview

**Per-Year Database Design** for raw FEC data + single analytics database:
- **`fec_2020`**, **`fec_2022`**, **`fec_2024`**, **`fec_2026`** - Raw FEC bulk files (one database per election cycle)
- **`legal_tender`** - Processed analytics (cross-cycle aggregations, lobbying, votes, influence scores)

**Why Per-Year Databases?**
1. **Logical Separation**: Each cycle is a complete, self-contained dataset
2. **Simpler Collection Names**: `independent_expenditures` instead of `independent_expenditures_2024`
3. **Better Performance**: All 2024 data in one database, no year filters needed
4. **Easier Joins**: Natural relationships within a cycle (candidates ↔ committees ↔ expenditures)
5. **Clear Architecture**: Each file type = one collection per database

**Key Design Decision**: We track **corporate/PAC influence**, NOT individual donors. Individual donor behavior is captured via aggregated totals in summary files (`webl.zip`), not granular 40M+ record datasets (`indiv.zip` - removed for performance).

---

## Raw FEC Data: `fec_YYYY` Databases

Each election cycle (2020, 2022, 2024, 2026) has its own database with **7 collections** (one per FEC file type).

### Example: `fec_2024` Database

```
fec_2024/
├── candidates              (~10K records, cn24.zip)
├── committees              (~20K records, cm24.zip)
├── linkages                (~20K records, ccl24.zip)
├── candidate_summaries     (~10K records, weball24.zip)
├── committee_summaries     (~10K records, webl24.zip)
├── pac_summaries           (~5K records, webk24.zip)
├── committee_transfers     (~2-5M records, pas224.zip)
└── independent_expenditures (~50-200K records, independent_expenditure_2024.csv)
```

---

## Collection Schemas (Per-Year Raw Data)

**Important Note on Field Formats:**
- **webl.zip** (committee_summaries): 30 fields per record
- **webk.zip** (pac_summaries): 27 fields per record (DIFFERENT FORMAT!)
- **weball.zip** (candidate_summaries): 30 fields per record
- Field indices are NOT interchangeable between formats!

### 1. `candidates` Collection
**Source**: `cnYY.zip` (Candidate Master File)  
**Records**: ~5,000-10,000 per cycle  
**Purpose**: Map candidate IDs to names, offices, states, districts

```js
{
  _id: "H4CA12034",           // Candidate ID (H=House, S=Senate, P=President)
  name: "PELOSI, NANCY",
  party: "DEM",
  office: "H",                 // H=House, S=Senate, P=President
  state: "CA",
  district: "12",
  incumbent_challenger: "I",   // I=Incumbent, C=Challenger, O=Open
  status: "C",                 // C=Candidate, N=Not yet candidate
  principal_campaign_committee: "C00295527",
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Indexes:**
- `{ _id: 1 }` (primary key)
- `{ name: 1 }`
- `{ state: 1, district: 1 }`

---

### 2. `committees` Collection
**Source**: `cmYY.zip` (Committee Master File)  
**Records**: ~10,000-20,000 per cycle  
**Purpose**: Identify committee types (especially Leadership PACs!)

```js
{
  _id: "C00295527",            // Committee ID
  name: "NANCY PELOSI FOR CONGRESS",
  designation: "P",            // P=Principal campaign committee, A=Authorized, U=Unauthorized
  type: "H",                   // H=House, S=Senate, P=Presidential, O=Leadership PAC ⭐
  party: "DEM",
  filing_frequency: "Q",       // Q=Quarterly, M=Monthly
  interest_group: "",          // For PACs
  connected_org_name: "PELOSI, NANCY",  // ⭐ For Leadership PACs - who owns it
  candidate_id: "H4CA12034",   // For campaign committees
  treasurer_name: "SMITH, JOHN",
  street_1: "123 Main St",
  city: "San Francisco",
  state: "CA",
  zip: "94102",
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Key Field for Leadership PAC Identification:**
- `type: "O"` = Leadership PAC ⭐
- Cross-reference `connected_org_name` to identify which politician controls it

**Indexes:**
- `{ _id: 1 }` (primary key)
- `{ type: 1 }` (find all Leadership PACs)
- `{ connected_org_name: 1 }` (find PACs owned by politician)
- `{ name: 1 }`

---

### 3. `linkages` Collection
**Source**: `cclYY.zip` (Candidate-Committee Linkage File)  
**Records**: ~20,000 per cycle  
**Purpose**: Link candidates to their authorized committees

```js
{
  _id: "H4CA12034|C00295527",  // Composite: candidate_id|committee_id
  candidate_id: "H4CA12034",
  committee_id: "C00295527",
  committee_type: "P",         // P=Principal, A=Authorized, J=Joint fundraiser
  committee_designation: "P",
  linkage_id: "12345",
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Indexes:**
- `{ candidate_id: 1 }`
- `{ committee_id: 1 }`

---

### 4. `candidate_summaries` Collection
**Source**: `weballYY.zip` (Candidate Financial Summary)  
**Records**: ~10,000 per cycle  
**Purpose**: Per-candidate financial summaries across all authorized committees

```js
{
  _id: "H4CA12034|2024Q2",     // candidate_id|coverage_period
  candidate_id: "H4CA12034",
  candidate_name: "PELOSI, NANCY",
  incumbent_challenger: "I",   // I=Incumbent, C=Challenger, O=Open
  party: "DEM",
  
  coverage_from_date: "20240401",
  coverage_through_date: "20240630",
  
  // Income
  total_receipts: 5000000.00,
  individual_contributions: 3500000.00,
  pac_contributions: 1000000.00,
  candidate_contributions: 50000.00,
  
  // Spending
  total_disbursements: 4000000.00,
  operating_expenditures: 500000.00,
  
  // Balances
  cash_on_hand_beginning: 2000000.00,
  cash_on_hand_end: 3000000.00,
  debts_owed_by: 0.00,
  debts_owed_to: 0.00,
  
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Indexes:**
- `{ candidate_id: 1 }`
- `{ total_receipts: -1 }` (top fundraisers)

---

### 5. `committee_summaries` Collection
**Source**: `weblYY.zip` (Committee Financial Summary - 30 fields)  
**Records**: ~10,000 per cycle  
**Purpose**: Committee financial totals INCLUDING aggregated individual donor amounts

```js
{
  _id: "C00295527|20240630",   // committee_id|coverage_through_date
  committee_id: "C00295527",
  committee_name: "NANCY PELOSI FOR CONGRESS",
  coverage_from_date: "20240401",      // Field [26]
  coverage_through_date: "20240630",   // Field [27]
  
  // Income
  total_receipts: 5000000.00,
  individual_contributions: 3500000.00,  // ⭐ Aggregated individual donor total
  pac_contributions: 1000000.00,
  candidate_contributions: 50000.00,
  transfers_from_authorized: 100000.00,
  
  // Spending
  total_disbursements: 4000000.00,
  operating_expenditures: 500000.00,
  transfers_to_authorized: 50000.00,
  
  // Balances
  cash_on_hand_beginning: 2000000.00,   // Field [8]
  cash_on_hand_end: 3000000.00,         // Field [9]
  debts_owed_by: 0.00,                  // Field [28]
  debts_owed_to: 0.00,
  
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Why This Instead of `indiv.zip`:**
- Individual donor totals aggregated here (no need for 40M+ individual records)
- We track **corporate** influence, not individual donors
- Saves 16GB of storage and processing time

**Indexes:**
- `{ committee_id: 1 }`
- `{ individual_contributions: -1 }` (top individual-funded committees)

---

### 6. `pac_summaries` Collection
**Source**: `webkYY.zip` (PAC Financial Summary - 27 fields, DIFFERENT FORMAT!)  
**Records**: ~5,000 per cycle  
**Purpose**: PAC-specific financial summaries

**⚠️ CRITICAL: webk.zip has 27 fields, NOT 30 like webl.zip!**
- Does NOT have `coverage_from_date` (only `coverage_through_date` at field [26])
- Financial field indices are DIFFERENT from webl format
- Must use correct field mappings or data will be corrupted

```js
{
  _id: "C00123456|20240630",            // committee_id|coverage_through_date
  committee_id: "C00123456",            // Field [0]
  committee_name: "LOCKHEED MARTIN PAC",
  committee_type: "Q",                  // Field [2] - Q=Qualified PAC
  
  coverage_through_date: "20240630",    // Field [26] - NO from_date!
  
  // Income
  total_receipts: 1000000.00,           // Field [5]
  
  // Spending
  total_disbursements: 800000.00,       // Field [7]
  contributions_to_committees: 750000.00,  // ⭐ Field [22] - Money given to candidates/Leadership PACs
  independent_expenditures: 100000.00,     // Field [23] - Super PAC-style spending
  operating_expenditures: 50000.00,
  
  // Balances
  cash_on_hand_beginning: 500000.00,    // Field [18]
  cash_on_hand_end: 600000.00,          // Field [19]
  debts_owed_by: 0.00,                  // Field [20]
  
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Field Mapping Notes:**
- Fixed in October 2025: Was incorrectly using webl field indices (30 fields) on webk data (27 fields)
- Before fix: coverage_through_date showed as "0", coverage_from_date showed cash amounts
- After fix: All fields correctly mapped to actual webk structure

**Indexes:**
- `{ committee_id: 1 }`
- `{ contributions_to_committees: -1 }` (biggest spenders)

---

### 7. `committee_transfers` Collection ⭐ CRITICAL FOR LEADERSHIP PAC TRACKING
**Source**: `pas2YY.zip` (Committee-to-Committee Transfers)  
**Records**: ~2-5 million per cycle  
**Purpose**: Track money flowing from Corporate PACs → Leadership PACs → Politicians

```js
{
  _id: "C00123456|C00789012|20240315",  // from|to|date
  from_committee_id: "C00123456",
  from_committee_name: "LOCKHEED MARTIN PAC",
  to_committee_id: "C00789012",
  to_committee_name: "PELOSI LEADERSHIP PAC",
  amount: 50000.00,
  date: "20240315",
  transaction_id: "SA11AI.12345",
  transaction_type: "24K",
  memo_text: "Political contribution",
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Indexes:**
- `{ from_committee_id: 1, amount: -1 }` (who's giving)
- `{ to_committee_id: 1, amount: -1 }` (who's receiving)
- `{ date: 1 }`
- `{ amount: -1 }` (largest transfers)

**Usage:**
1. Find Leadership PACs (from `committees` collection where `type='O'`)
2. Find transfers TO Leadership PACs (from corporate PACs)
3. Calculate "upstream influence" for politicians (money laundered through their Leadership PACs)



### 6. `pac_summaries` Collection
**Source**: `webkYY.zip` (PAC Financial Summary)  
**Records**: ~5,000 per cycle  
**Purpose**: PAC-specific financial summaries

```js
{
  _id: "C00123456|2024Q2",
  committee_id: "C00123456",
  committee_name: "LOCKHEED MARTIN PAC",
  coverage_from_date: "20240401",
  coverage_through_date: "20240630",
  
  total_receipts: 1000000.00,
  contributions_to_committees: 750000.00,  // ⭐ Money given to candidates/Leadership PACs
  independent_expenditures: 100000.00,
  operating_expenditures: 50000.00,
  
  cash_on_hand_end: 500000.00,
  
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Indexes:**
- `{ committee_id: 1 }`
- `{ contributions_to_committees: -1 }` (biggest spenders)

---

### 8. `independent_expenditures` Collection ⭐ SUPER PAC SPENDING
**Source**: `independent_expenditure_YYYY.csv` (Schedule E Filings)  
**Records**: ~50,000-200,000 per cycle  
**Purpose**: Track Super PAC spending FOR and AGAINST specific candidates  
**Innovation**: OPPOSITION spending stored as **NEGATIVE** values

```js
{
  _id: "SUB_ID_12345",         // Transaction ID
  committee_id: "C00789012",
  committee_name: "FUTURE FORWARD USA",
  candidate_id: "P80001571",
  candidate_name: "BIDEN, JOSEPH R JR",
  
  amount: -5000000.00,         // ⭐ NEGATIVE = opposition, POSITIVE = support
  raw_amount: 5000000.00,      // Original amount (always positive)
  
  support_oppose: "O",         // S=Support, O=Oppose
  is_support: false,
  is_oppose: true,
  
  purpose: "TV advertising opposing candidate",
  payee: "Media Buying Inc",
  expenditure_date: "20241015",
  file_num: "1234567",
  
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

**Indexes:**
- `{ candidate_id: 1, amount: -1 }` (net spending per candidate)
- `{ committee_id: 1 }`
- `{ is_support: 1 }`
- `{ is_oppose: 1 }`
- `{ amount: -1 }` (biggest expenditures)

**Usage:**
```js
// Net Super PAC influence for a candidate
db.independent_expenditures.aggregate([
  { $match: { candidate_id: "P80001571" } },
  { $group: { 
      _id: "$candidate_id",
      net_support: { $sum: "$amount" },  // Negative oppose cancels positive support
      support_total: { $sum: { $cond: ["$is_support", "$raw_amount", 0] } },
      oppose_total: { $sum: { $cond: ["$is_oppose", "$raw_amount", 0] } }
  }}
])
```

---

## Processed Data: `legal_tender` Database

Single database for cross-cycle analytics and processed data.

### Collection: `donors` (Future - Aggregated Influence)
```js
{
  _id: "C00123456",            // Committee ID (corporate PAC)
  name: "LOCKHEED MARTIN PAC",
  type: "PAC",
  
  // Direct contributions (from committee_transfers, linkages, etc.)
  direct_contributions: {
    "H4CA12034": 50000.00,     // Direct to Pelosi campaign
    "S6NY00111": 25000.00,     // Direct to Schumer campaign
    // ... 
  },
  
  // Upstream influence (via Leadership PACs)
  upstream_contributions: {
    "H4CA12034": 100000.00,    // Via Pelosi Leadership PAC
    "S6NY00111": 75000.00,     // Via Schumer Leadership PAC
    // ...
  },
  
  // Combined weighted influence
  total_influence: {
    "H4CA12034": 150000.00,    // 50K direct + 100K upstream
    "S6NY00111": 100000.00,    // 25K direct + 75K upstream
    // ...
  },
  
  cycles: ["2020", "2022", "2024", "2026"],
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

### Collection: `members` (Current - Politicians)
```js
{
  _id: "pelosi-nancy-ca12",
  bioguide_id: "P000197",
  fec_candidate_ids: ["H4CA12034", "H6CA12034", ...],  // Per cycle
  fec_leadership_pac_ids: ["C00789012"],                // ⭐ Leadership PAC
  
  name: "Nancy Pelosi",
  state: "CA",
  district: "12",
  party: "DEM",
  
  // Populated from fec databases
  total_direct_contributions: 5000000.00,
  total_upstream_contributions: 2000000.00,  // Via Leadership PAC
  
  updated_at: ISODate("2024-10-20T00:00:00Z")
}
```

### Collection: `lobbying_filings` (Future)
Senate LDA API data - lobbying disclosures linked to bills/issues.

### Collection: `bills` (Future)
Congress.gov API data - bill text, sponsors, votes.

### Collection: `votes` (Future)
Voting records linked to bills and donor influence.

### Collection: `influence_analysis` (Future)
Final correlation: Money → Votes.

---

## Data Flow Summary

```
1. Download FEC Bulk Files (data_sync asset)
   ├── cn24.zip, cm24.zip, ccl24.zip (metadata)
   ├── weball24.zip (candidate summaries)
   ├── webl24.zip, webk24.zip (committee/PAC summaries)
   ├── pas224.zip (transfers)
   └── independent_expenditure_2024.csv (Super PAC)
   
2. Parse into fec_2024 Database (8 separate parser assets, 1 per file)
   ├── candidates.py → candidates (10K)
   ├── committees.py → committees (20K) → Find Leadership PACs (type='O')
   ├── linkages.py → linkages (20K)
   ├── candidate_summaries.py → candidate_summaries (10K)
   ├── committee_summaries.py → committee_summaries (10K) → Individual donor aggregates
   ├── pac_summaries.py → pac_summaries (5K) ⚠️ Fixed field mappings Oct 2025
   ├── committee_transfers.py → committee_transfers (2-5M) → Track Corporate → Leadership PAC flows
   └── independent_expenditures.py → independent_expenditures (50-200K) → Super PAC FOR/AGAINST

3. Aggregate into legal_tender Database (member_fec_mapping asset)
   ├── members (politicians with FEC IDs + Leadership PAC IDs)
   └── donors (corporate PACs with direct + upstream influence) - FUTURE
   
4. Cross-Reference (Future)
   ├── Lobbying data → Bills
   ├── Votes → Bills
   └── Donors → Votes (CORRELATION!)
```

---

## Storage Estimates

**Per Cycle (2024 example):**
```
fec_2024:
├── candidates:              ~1 MB
├── committees:              ~2 MB
├── linkages:                ~2 MB
├── candidate_summaries:     ~10 MB
├── committee_summaries:     ~10 MB
├── pac_summaries:           ~5 MB
├── committee_transfers:     ~300 MB  ⭐ Largest
└── independent_expenditures: ~20 MB
Total per cycle:             ~350 MB
```

**All 4 Cycles (2020-2026):**
```
fec_2020: ~350 MB
fec_2022: ~350 MB
fec_2024: ~350 MB
fec_2026: ~350 MB
Total raw FEC: ~1.4 GB
```

**Compared to Old Approach (with indiv.zip):**
- Old: 25 GB (4GB indiv × 4 cycles + other files)
- New: 1.4 GB (removed indiv.zip, corrected independent expenditures)
- **Savings: 95% reduction!**

---

## Query Examples

### Find all Leadership PACs in 2024
```js
use fec_2024
db.committees.find({ type: "O" })
```

### Find money flowing TO Pelosi's Leadership PAC
```js
use fec_2024
db.committee_transfers.find({ 
  to_committee_id: "C00789012"  // Pelosi Leadership PAC
}).sort({ amount: -1 })
```

### Calculate net Super PAC support for Biden
```js
use fec_2024
db.independent_expenditures.aggregate([
  { $match: { candidate_id: "P80001571" } },
  { $group: { 
      _id: "$candidate_id",
      net_support: { $sum: "$amount" },
      total_support: { $sum: { $cond: ["$is_support", "$raw_amount", 0] } },
      total_oppose: { $sum: { $cond: ["$is_oppose", "$raw_amount", 0] } },
      count: { $sum: 1 }
  }}
])
```

### Cross-cycle analysis: All candidates who received from Lockheed Martin PAC
```js
// Requires aggregating across multiple databases
const cycles = ["2020", "2022", "2024", "2026"];
cycles.forEach(cycle => {
  db.getSiblingDB(`fec_${cycle}`).committee_transfers.find({
    from_committee_id: "C00123456"  // Lockheed Martin PAC
  })
})
```

---

## Current Pipeline Status (October 2025)

### Completed ✅
1. ✅ Per-year database structure implemented (`fec_YYYY` per cycle)
2. ✅ Clean collection names (no year suffixes)
3. ✅ All 8 FEC parsers created (1:1 file-to-parser mapping)
   - candidates.py, committees.py, linkages.py
   - candidate_summaries.py, committee_summaries.py, pac_summaries.py
   - committee_transfers.py, independent_expenditures.py
4. ✅ Fixed field mappings across all parsers
   - candidates: party, election_year, state, district
   - committees: type field correction
   - linkages: unique key (linkage_id)
   - pac_summaries: webk format (27 fields) - major fix Oct 2025
   - independent_expenditures: _id composite key
5. ✅ Data sync with smart caching and proper logging
6. ✅ Cleaned job/asset structure (1 job, 10 assets)
7. ✅ Raw data only (no transformations in parsers)

### In Progress 🚧
1. 🚧 Docker rebuild and testing with corrected pac_summaries.py
2. 🚧 Build `donors` aggregation (direct + upstream influence)

### Future Work 📋
1. 📋 Integrate lobbying data (Senate LDA API)
2. 📋 Build correlation analysis (Money → Votes)
3. 📋 Congress.gov API integration (bills, votes)
