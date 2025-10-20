# MongoDB Structure for Legal Tender

## Overview

Two-database architecture designed for corporate influence tracking:
1. **`fec_bulk`** - Raw FEC bulk data files (one collection per file per cycle)
2. **`legal_tender`** - Processed analytics (donors with upstream influence, lobbying, votes)

**Key Design Decision**: We track **corporate/PAC influence**, NOT individual donors. All aggregations focus on employers, PACs, and Leadership PAC money flows.

---

## Database: `fec_bulk` (Raw FEC Bulk Data)

### Individual Contributions (per cycle)
```
fec_bulk.individual_contributions_2020
fec_bulk.individual_contributions_2022
fec_bulk.individual_contributions_2024
fec_bulk.individual_contributions_2026
```
**Purpose**: Raw contribution data for corporate influence aggregation  
**Schema:**
```js
{
  _id: "SUB_ID",              // FEC unique transaction ID
  cycle: "2024",
  donor_name: "JOHN DOE",
  employer: "LOCKHEED MARTIN",  // ← Key field for aggregation
  occupation: "ENGINEER",
  amount: 3300.00,
  date: "12312024",
  committee_id: "C00123456",
  city: "ARLINGTON",
  state: "VA",
  zip_code: "22201"
}
```

### Independent Expenditures (per cycle)
```
fec_bulk.independent_expenditures_2020
fec_bulk.independent_expenditures_2022
fec_bulk.independent_expenditures_2024
fec_bulk.independent_expenditures_2026
```
**Purpose**: Super PAC spending FOR/AGAINST candidates  
**Innovation**: Opposition stored as **NEGATIVE** values

**Schema:**
```js
{
  _id: "SUB_ID",
  cycle: "2024",
  committee_id: "C00789012",
  committee_name: "FUTURE FORWARD USA",
  candidate_id: "P80001571",
  candidate_name: "BIDEN, JOSEPH R JR",
  amount: -5000000.00,        // NEGATIVE = opposition
  raw_amount: 5000000.00,
  support_oppose: "O",        // S=Support, O=Oppose
  is_support: false,
  is_oppose: true,
  purpose: "TV ads against candidate",
  date: "10152024"
}
```

### Committee Transfers (per cycle) - Leadership PAC Tracking
```
fec_bulk.committee_transfers_2020
fec_bulk.committee_transfers_2022
fec_bulk.committee_transfers_2024
fec_bulk.committee_transfers_2026
```
**Purpose**: Track money flowing between committees (Leadership PAC money laundering)  
**Source**: FEC `pas2YY.zip` files

**Schema:**
```js
{
  _id: "from_committee|to_committee|date",
  cycle: "2024",
  from_committee_id: "C00123456",
  from_committee_name: "LOCKHEED MARTIN PAC",
  to_committee_id: "C00789012",
  to_committee_name: "PELOSI LEADERSHIP PAC",
  amount: 50000.00,
  date: "20240315",
  transaction_type: "24K"
}
```

### Candidates (per cycle)
```
fec_bulk.candidates_2020..2026
```
**Schema:**
```js
{
  _id: "CAND_ID",
  cycle: "2024",
  name: "SMITH, JOHN A",
  party: "DEM",
  office: "H",         // H=House, S=Senate, P=President
  state: "CA",
  district: "12"
}
```

### Committees (per cycle)
```
fec_bulk.committees_2020..2026
```
**Purpose**: Identify Leadership PACs and corporate PACs  
**Schema:**
```js
{
  _id: "CMTE_ID",
  cycle: "2024",
  name: "PELOSI LEADERSHIP PAC",
  type: "O",           // O=Leadership PAC, Q=Super PAC, etc.
  party: "DEM",
  connected_org: "PELOSI, NANCY"  // For Leadership PACs
}
```

---

## Database: `legal_tender` (Processed Application Data)

### Legislators (from GitHub, NOT FEC)
```
legal_tender.legislators_current
```
**Source**: `@unitedstates/congress-legislators` GitHub repo  
**Purpose**: Map bioguide IDs to FEC IDs

**Schema:**
```js
{
  _id: "bioguide_id",
  name: {
    first: "Nancy",
    last: "Pelosi",
    official_full: "Nancy Pelosi"
  },
  ids: {
    bioguide: "P000197",
    fec: ["H8CA05035"],
    govtrack: 400314
  },
  terms: [
    {
      type: "rep",
      start: "2021-01-03",
      state: "CA",
      district: 11,
      party: "Democrat"
    }
  ]
}
```

### Donors (aggregated corporate/PAC influence)
```
legal_tender.donors
```
**Purpose**: Aggregated donor influence with Leadership PAC upstream tracking  
**Key Innovation**: Tracks both direct donations AND weighted upstream influence via Leadership PACs

**Schema:**
```js
{
  _id: "normalized_donor_name",
  display_name: "LOCKHEED MARTIN PAC",
  donor_type: "corporate_pac",  // corporate_pac, super_pac, leadership_pac
  
  // Direct contributions (from individual_contributions)
  total_direct: 5000000.00,
  direct_by_cycle: {
    "2020": 1200000,
    "2022": 1300000,
    "2024": 1500000,
    "2026": 1000000
  },
  
  // Upstream influence (via Leadership PACs from committee_transfers)
  total_upstream: 2000000.00,
  upstream_influence: {
    leadership_pacs: [
      {
        pac_id: "C00789012",
        pac_name: "Pelosi Leadership PAC",
        gave_to_pac: 50000.00,           // Lockheed → Pelosi PAC
        pac_gave_total: 500000.00,        // Pelosi PAC → all politicians
        weight: 0.10,                     // 50K / 500K = 10%
        weighted_influence: 50000.00,     // 10% of what Pelosi PAC distributed
        recipients: [
          { 
            candidate_id: "H1CA12345", 
            direct_from_pac: 10000,      // Pelosi PAC → politician
            weighted_from_lockheed: 1000 // 10% × 10K = $1K Lockheed influence
          }
        ]
      }
    ]
  },
  
  // Total influence
  total_influence: 7000000.00,   // direct + upstream
  
  // Top recipients (sorted by total influence)
  top_recipients: [
    {
      candidate_id: "H1TX23456",
      name: "Smith, John",
      direct: 15000,
      upstream: 2500,
      total: 17500
    }
  ],
  
  industry: "Defense",  // To be added later (AI/manual tagging)
  updated_at: ISODate("2025-10-20")
}
```

### Member Financial Summaries (aggregated campaign finance)
```
legal_tender.member_financial_summary
```
**Purpose**: Total receipts, disbursements, cash on hand per member  
**Source**: FEC `webl.zip` files (committee financial summaries)  
**Key Innovation**: Provides **total individual contributions** without processing 40M+ granular records  
**Use Case**: Compare Corporate PAC money vs Individual Donor money (treat individuals as "virtual PAC")

**Schema:**
```js
{
  _id: "bioguide_id",
  name: "Bernie Sanders",
  candidate_ids: ["S6VT00033"],
  
  // Per-cycle breakdown
  by_cycle: {
    "2024": {
      total_raised: 5000000.00,
      total_spent: 4500000.00,
      cash_on_hand: 500000.00,
      individual_contributions: 4800000.00,  // ← KEY: Total from individuals (aggregated)
      debts: 0.00,
      num_entries: 2  // Multiple committees/filings deduplicated
    },
    "2022": { ... },
    "2020": { ... }
  },
  
  // Career totals (sum across all cycles)
  career_totals: {
    total_raised: 48000000.00,
    total_spent: 45000000.00,
    individual_contributions: 46000000.00,  // ← 96% from individuals!
    cycles_included: ["2020", "2022", "2024", "2026"]
  },
  
  // Quick stats
  latest_cycle: "2024",
  cycles_with_data: 4,
  updated_at: ISODate("2025-10-20")
}
```

**Comparative Analysis:**
```js
// Example: Compare funding sources for a politician
{
  bioguide_id: "P000197",  // Nancy Pelosi
  career_totals: {
    total_raised: 150000000,
    individual_contributions: 90000000  // 60% from individuals
  },
  // From donors collection (separate query):
  corporate_pac_total: 45000000,        // 30% from corporate PACs
  super_pac_total: 15000000             // 10% from Super PACs
}

// Compare to Bernie Sanders:
{
  bioguide_id: "S000033",
  career_totals: {
    total_raised: 48000000,
    individual_contributions: 46000000  // 96% from individuals!
  },
  corporate_pac_total: 500000,          // 1% from corporate PACs
  super_pac_total: 1500000              // 3% from Super PACs
}
```

**Why This Matters:**
- **No performance cost**: 7MB summary files vs 2-4GB transaction files
- **Instant comparison**: Corporate-funded vs grassroots-funded politicians
- **Already implemented**: `src/assets/financial_summary.py` loads this data today
- **Treat individuals as collective entity**: Compare "Corporate PAC influence" vs "Individual Donor influence"

---

### Members (politicians with FEC linkage)
```
legal_tender.members
```
**Purpose**: Complete politician profiles with donor breakdown

**Schema:**
```js
{
  _id: "bioguide_id",
  name: "Nancy Pelosi",
  state: "CA",
  district: 11,
  party: "Democrat",
  office: "House",
  fec_ids: ["H8CA05035"],
  committees: ["C00123456"],  // Authorized committees
  
  // Top donors by total influence (direct + upstream)
  top_donors: [
    {
      donor_id: "LOCKHEED MARTIN PAC",
      direct: 15000,
      upstream: 5000,
      total: 20000
    }
  ],
  
  updated_at: ISODate("2025-10-20")
}
```

### Lobbying Filings
```
legal_tender.lobbying_filings
```
**Source**: Senate LDA API (1.8M+ filings)  
**Purpose**: Track corporate lobbying spending (NOT campaign donations)

**Schema:**
```js
{
  _id: "filing_uuid",
  client_name: "AMAZON.COM",
  registrant_name: "Akin Gump Strauss Hauer & Feld",
  income: 500000.00,
  year: 2024,
  quarter: "Q2",
  filing_type: "LD-2",
  
  lobbying_activities: [
    {
      general_issue_code: "TAX",
      description: "H.R. 1234 - Tax Reform Act",
      lobbyists: ["John Doe", "Jane Smith"],
      government_entities: ["HOUSE OF REPRESENTATIVES", "SENATE"]
    }
  ],
  
  dt_posted: ISODate("2024-07-30"),
  updated_at: ISODate("2025-10-20")
}
```

**Note**: Lobbying data has NO politician names - lobbying is by issue/bill, not person. We'll correlate via bill sponsors and committee assignments.

---

## Future Collections (Phase 4-6)

### Bills (Congress.gov API)
```
legal_tender.bills
```
**Purpose**: Bill metadata, sponsors, committee assignments

### Votes (Congress.gov API)
```
legal_tender.votes
```
**Purpose**: How each member voted on each bill

### Donor Profiles (AI-Generated)
```
legal_tender.donor_profiles
```
**Purpose**: LLM-generated PROS/CONS for each corporate PAC

### Influence Analysis (Final Output)
```
legal_tender.influence_analysis
```
**Purpose**: Correlate money → votes, calculate influence scores

---

## Asset Dependency Graph

```
# FEC BULK DATA INGESTION (per-file assets → fec_bulk database)
data_sync_indiv_2020 → individual_contributions_2020
data_sync_indiv_2022 → individual_contributions_2022
data_sync_indiv_2024 → individual_contributions_2024
data_sync_indiv_2026 → individual_contributions_2026

data_sync_oppexp_2020 → independent_expenditures_2020
data_sync_oppexp_2022 → independent_expenditures_2022
data_sync_oppexp_2024 → independent_expenditures_2024
data_sync_oppexp_2026 → independent_expenditures_2026

data_sync_pas2_2020 → committee_transfers_2020
data_sync_pas2_2022 → committee_transfers_2022
data_sync_pas2_2024 → committee_transfers_2024
data_sync_pas2_2026 → committee_transfers_2026

data_sync_cn_2020 → candidates_2020
data_sync_cm_2020 → committees_2020
data_sync_ccl_2020 → linkages_2020
... (same for 2022, 2024, 2026)

# FEC SUMMARY DATA (webl files → legal_tender database)
member_financial_summary → member_financial_summary
  ← webl_2020, webl_2022, webl_2024, webl_2026 (committee financial summaries)
  ← legislators_current (for bioguide mapping)

# CONGRESS DATA (GitHub → legal_tender database)
data_sync_legislators → legislators_current

# LOBBYING DATA (Senate LDA API → legal_tender database)
lobbying_sync → lobbying_filings

# AGGREGATION & PROCESSING (legal_tender database)
[individual_contributions_* + committee_transfers_* + committees_*] → donors
[donors + legislators_current + candidates_*] → members
[lobbying_filings + bills] → bill_lobbying_links (future)

# AI ANALYSIS (future)
[donors] → donor_profiles (AI-generated PROS/CONS)
[bills + donor_profiles] → bill_scores (alignment scoring)
[members + votes + bill_scores + donors] → influence_analysis (final output)
```

---

## Implementation Plan

### Phase 1: FEC Bulk Data (Current) ✅ → 🚧
1. ✅ Two-database MongoDB structure (`fec_bulk` + `legal_tender`)
2. ✅ Per-file download assets with weekly refresh
3. ✅ Streaming parsers for `indiv.zip` and `oppexp.zip`
4. ✅ Dynamic batch sizing (50% RAM allocation)
5. 🚧 Add `pas2.zip` parser (committee transfers)
6. 🚧 Refactor to per-file assets (enable parallel processing)

### Phase 2: Leadership PAC Tracking 🚧
1. Parse `pas2.zip` files (committee-to-committee transfers)
2. Identify Leadership PACs via `CMTE_TP = "O"` in `cm.zip`
3. Calculate upstream influence weights
4. Create `donors` aggregation asset:
   - Read from `individual_contributions_*` (direct)
   - Read from `committee_transfers_*` (upstream)
   - Read from `committees_*` (identify Leadership PACs)
   - Calculate weighted influence per donor per politician
   - Write to `legal_tender.donors`

### Phase 3: Lobbying Data 📋
1. Create `lobbying_sync` asset
2. Use Senate LDA API pagination to download all 1.8M+ filings
3. Store in `legal_tender.lobbying_filings`
4. Incremental updates quarterly (required filing schedule)
5. Link to bills via issue descriptions (future)

### Phase 4: Congressional Data 📋
1. Congress.gov API integration
2. Download bill metadata, sponsors, committee assignments
3. Download voting records
4. Store in `legal_tender.bills` and `legal_tender.votes`

### Phase 5: AI Analysis 📋
1. Generate donor profiles (LLM PROS/CONS for each PAC)
2. Score bills against donor profiles (alignment analysis)
3. Predict votes based on donor influence

### Phase 6: Final Output 📋
1. Correlate money → votes in `influence_analysis` collection
2. Calculate influence scores per politician per donor
3. Identify vote misalignments (voted against donor interests)
4. Generate public dashboard

---

## MongoDB Connection Examples

```python
from src.resources.mongo import MongoDBResource

# In assets
def my_asset(mongo: MongoDBResource):
    client = mongo.get_client()
    
    # Raw FEC data
    indiv_2024 = mongo.get_collection(client, "individual_contributions_2024", db="fec_bulk")
    
    # Processed data
    donors = mongo.get_collection(client, "donors", db="legal_tender")
    
    # Do work...
```

**Collection Naming Convention:**
- FEC raw: `{file_type}_{cycle}` (e.g. `individual_contributions_2024`)
- Processed: `{entity_plural}` (e.g. `donors`, `members`, `bills`)

---

## Key Design Decisions

### 1. Corporate Focus, Not Individual Donors
- We aggregate by `employer` field, not individual names
- Track PAC-to-PAC transfers (Leadership PAC money laundering)
- Focus on corporate influence, not individual behavior

### 2. Leadership PAC Upstream Tracking
- Calculate weighted influence via committee transfers
- Track money flowing: Corporate PAC → Leadership PAC → Politician
- Expose hidden corporate influence

### 3. Lobbying as Separate Layer
- Lobbying spending ≠ campaign donations
- No direct politician linkage in lobbying data
- Correlate via bill sponsors and committee assignments

### 4. Two-Database Architecture
- Raw FEC data preserved permanently
- Can rebuild analytics from raw data anytime
- Clear separation of concerns
- Fast queries on aggregated data
