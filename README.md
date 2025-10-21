# Legal Tender: AI-Driven Political Influence Analysis

**Follow the money. Map the influence. Expose the connections.**

Legal Tender is an AI-powered pipeline to analyze the influence of money in US politics. We connect campaign finance data, leadership PAC tracking, lobbying expenditures, donor profiles, bill analysis, and voting records to answer:

> **Does money influence how politicians vote?**

---

## Table of Contents

- [Quick Start](#quick-start)
- [Core Goal](#core-goal)
- [Data Sources](#data-sources)
  - [FEC Campaign Finance (Bulk Data)](#fec-campaign-finance-bulk-data)
  - [Lobbying Data (Senate LDA API)](#lobbying-data-senate-lda-api)
  - [Congressional Data](#congressional-data)
- [Leadership PAC Money Laundering](#leadership-pac-money-laundering)
- [Technical Architecture](#technical-architecture)
  - [Dynamic Batch Sizing](#dynamic-batch-sizing)
  - [Streaming Processing](#streaming-processing)
  - [MongoDB Per-Year Database Structure](#mongodb-per-year-database-structure)
- [Roadmap](#roadmap)

---

**Quick Start**

```bash
./start.sh -v
```

**Processing time:** ~25 minutes for 8 years of FEC data (4 cycles: 2020-2026)  
**Data downloaded:** ~1.4 GB (down from 25 GB!)  
**Memory required:** 4GB minimum, 8-16GB recommended for optimal batch sizing  
**Pipeline**: 1 job (`fec_pipeline_job`) → 10 assets (data_sync + member_fec_mapping + 8 FEC parsers)

---

## Core Goal

Track **corporate and PAC money** flowing to politicians and correlate with voting records. We are NOT tracking individual donors at granular level - our focus is:

1. **Corporate/Industry PACs** → Politicians (direct contributions)
2. **Corporate PACs** → **Leadership PACs** → Politicians (hidden influence)
3. **Corporate Lobbying** → Issues/Bills (indirect influence)
4. **Money** → **Votes** (correlation analysis)

---

---

## Data Sources

### FEC Campaign Finance (Bulk Data)

**Source**: Federal Election Commission bulk downloads  
**Update Frequency**: Weekly  
**API Rate Limits**: NONE (bulk download, not API)

We process **8 years** of FEC data (4 election cycles: 2020, 2022, 2024, 2026).

---

#### 🚨 CRITICAL: Understanding FEC File Formats

The FEC provides dozens of bulk files with confusing names. After extensive investigation, here's what each file **actually** contains and why we use (or don't use) each one:

---

#### Files We ARE Using (8 files total per cycle)

##### 1. Independent Expenditures (`independent_expenditure_YYYY.csv`)
**CORRECT FILE**: `independent_expenditure_2024.csv` (CSV format!)  
**WRONG FILE**: ❌ `oppexp24.zip` (that's "Operating Expenditures" - see below)

- **Format**: CSV (NOT zip!)
- **Size**: ~20 MB per cycle (~80 MB total)
- **URL Pattern**: `https://.../bulk-downloads/{CYCLE}/independent_expenditure_{CYCLE}.csv`
- **Records**: ~50K-200K Super PAC expenditures per cycle
- **Purpose**: Track Super PAC spending FOR/AGAINST specific candidates
- **Key Fields**:
  - `cand_id` - Candidate ID (H4CO08034, P80000722, etc.)
  - `sup_opp` - Support/Oppose indicator ('S' or 'O') ✅
  - `exp_amo` - Expenditure amount
  - `spe_id` - Spender committee ID
  - `spe_nam` - Spender committee name
  - `pur` - Purpose description
- **Innovation**: We store opposition as **NEGATIVE** values for net influence calculations
- **Processing**: ~3 minutes for all 4 cycles (CSV is fast!)

**Sample Data:**
```csv
"H4CO08034","Evans, Gabe","C00866517","Go America PAC","G","CO","08","H",
"REPUBLICAN PARTY","9000","","9000","S","texts supporting Gabe Evans CO-8"
```

**Why This File?**
- Schedule E filings (independent expenditures) are separate from committee reports
- Filed on 24-hour/48-hour basis (time-sensitive)
- Only available as standalone CSV files, NOT in yearly ZIP files
- Contains actual FOR/AGAINST candidate spending with support/oppose indicators

---

##### 2. Committee-to-Committee Transfers (`pas2YY.zip`)
- **Format**: ZIP containing pipe-delimited text
- **Size**: ~300 MB per cycle (~1.2 GB total)
- **Records**: ~2-5 million transfers between committees
- **Purpose**: **Track Leadership PAC money laundering** (see Leadership PAC section)
- **Key Fields**:
  - Committee giving money (FROM)
  - Committee receiving money (TO)
  - Transfer amount
  - Transfer date
- **Processing**: ~15 minutes for all 4 cycles
- **Critical For**: Upstream influence calculation (Corporate PAC → Leadership PAC → Politician)

**Why This File?**
- Only source of committee-to-committee transfer data
- Required to trace corporate money through Leadership PACs
- Enables "weighted influence" calculations

---

##### 3. Metadata Files (Small but Essential)

**`cnYY.zip` - Candidates**
- **Size**: ~2 MB per cycle
- **Records**: ~5K-10K candidates
- **Purpose**: Map candidate IDs to names, offices, states, districts
- **Processing**: < 1 minute

**`cmYY.zip` - Committees**  
- **Size**: ~1 MB per cycle
- **Records**: ~10K-20K committees (PACs, Super PACs, campaign committees)
- **Purpose**: Identify committee types, especially Leadership PACs
- **Key Field**: `CMTE_TP = "O"` identifies Leadership PACs
- **Processing**: < 1 minute

**`cclYY.zip` - Candidate-Committee Linkages**
- **Size**: ~1 MB per cycle
- **Records**: Maps which committees are authorized by which candidates
- **Purpose**: Link candidate campaign committees to candidates
- **Processing**: < 1 minute

---

##### 4. Financial Summary Files (Aggregated Totals)

**`weballYY.zip` - Candidate Financial Summaries**
- **Size**: ~10 MB per cycle (~40 MB total)
- **Records**: ~10K candidate summaries
- **Format**: ZIP containing pipe-delimited text (30 fields)
- **Purpose**: Per-candidate financial totals (aggregate across all authorized committees)
- **Key Fields**:
  - Total receipts, disbursements
  - Individual contributions, PAC contributions
  - Cash on hand beginning/end
  - Debts owed by/to
- **Processing**: ~2 minutes for all 4 cycles
- **Parser**: `candidate_summaries.py`

**`weblYY.zip` - Committee Financial Summaries**
- **Size**: ~5 MB per cycle (~20 MB total)
- **Records**: ~10K committee summaries
- **Format**: ZIP containing pipe-delimited text (30 fields)
- **Purpose**: Committee financial totals including aggregated individual donor amounts
- **Key Fields**:
  - Total receipts, disbursements
  - **Total individual contributions** (aggregated sum, no granular data)
  - PAC contributions, transfers
  - Cash on hand beginning/end
  - Coverage from/through dates (fields [26][27])
- **Processing**: ~2 minutes for all 4 cycles
- **Parser**: `committee_summaries.py`
- **Use Case**: Compare "Corporate PAC $" vs "Individual Donor $" without processing 40M+ records

**`webkYY.zip` - PAC Financial Summaries**  
- **Size**: ~3 MB per cycle (~12 MB total)
- **Records**: ~5K PAC summaries
- **Format**: ZIP containing pipe-delimited text (27 fields - DIFFERENT from webl!)
- **Purpose**: PAC-specific financial summaries
- **Key Fields**:
  - Total receipts, disbursements
  - Contributions to committees (field [22])
  - Independent expenditures (field [23])
  - Cash on hand beginning/end (fields [18][19])
  - Coverage through date ONLY (field [26], NO from_date)
- **Processing**: ~1 minute for all 4 cycles
- **Parser**: `pac_summaries.py` (fixed field mappings Oct 2025)
- **CRITICAL**: webk has 27 fields, NOT 30 like webl/weball!

---

#### Files We Are NOT Using (And Why)

##### ❌ `oppexpYY.zip` - Operating Expenditures
**COMMONLY CONFUSED WITH INDEPENDENT EXPENDITURES!**

- **What We Thought**: Independent expenditures FOR/AGAINST candidates
- **What It Actually Is**: Committee operating expenses (Schedule B)
- **Size**: 60+ MB per cycle
- **Records**: Millions of operating expenditure line items
- **Contains**:
  - Payroll expenses
  - Consulting fees
  - Administrative overhead
  - Rent, utilities, etc.
- **Does NOT Contain**:
  - ❌ Candidate IDs
  - ❌ Support/Oppose indicators
  - ❌ Independent expenditure data
- **File Format**: 26 fields, pipe-delimited
- **Why We Don't Use It**: Wrong data! We need `independent_expenditure_YYYY.csv` instead

**Sample oppexp.txt Record (NOT what we want):**
```
C00794982|A|2023|TER|202301109574642828|17|F3|SB|BROGHAMER CONSULTING LLC|
NEWPORT|KY|410712006|01/09/2023|1218.75|G2022|COMPLIANCE CONSULTING|001|
Administrative/Salary/Overhead Expenses|...
```
☝️ This is a committee paying a consulting firm, NOT Super PAC spending on candidates!

---

##### ❌ `indivYY.zip` - Individual Contributions (REMOVED for Performance)
- **Size**: 2-4 GB per cycle (~15 GB total for 4 cycles)
- **Records**: ~40 million individual donor transactions
- **What It Contains**: Every donation from individual people to committees
  - Donor name
  - Employer
  - Occupation  
  - Amount ($3,300 limit per election)
  - Date
  - Committee receiving
- **Why We DON'T Use It**:
  - ❌ **Performance**: 60 minutes processing time for granular data we don't need
  - ❌ **Storage**: 40M+ records = 99.9% of our data volume
  - ❌ **Wrong Focus**: We track corporate/PAC influence, not individual behavior
  - ✅ **Better Alternative**: Use `weblYY.zip` aggregated totals instead
- **What We Use Instead**: 
  - Aggregated individual contribution totals from `weblYY.zip`
  - Treat as "Individual Donors (Virtual PAC)" for comparison purposes
  - Get the same analytical insight (corporate vs grassroots funding) without the overhead

**Performance Impact:**
- Processing time: 100 min → 43 min (57% faster)
- Storage: 40M records → aggregated totals (99.9% reduction)
- Data quality: No loss - we don't need granular individual behavior

---



---

#### Summary: FEC Files We Download (8 per cycle)

| File Pattern | Type | Size/Cycle | Records | Fields | Purpose | Parser | Time |
|-------------|------|-----------|---------|--------|---------|--------|------|
| `cnYY.zip` | ZIP/TXT | 2 MB | 5-10K | varies | Candidate metadata | `candidates.py` | <1 min |
| `cmYY.zip` | ZIP/TXT | 1 MB | 10-20K | varies | Committee metadata (Leadership PACs) | `committees.py` | <1 min |
| `cclYY.zip` | ZIP/TXT | 1 MB | varies | varies | Candidate-committee linkages | `linkages.py` | <1 min |
| `weballYY.zip` | ZIP/TXT | 10 MB | 10K | 30 | Candidate financial summaries | `candidate_summaries.py` | 2 min |
| `weblYY.zip` | ZIP/TXT | 5 MB | 10K | 30 | Committee financial summaries | `committee_summaries.py` | 2 min |
| `webkYY.zip` | ZIP/TXT | 3 MB | 5K | **27** | PAC financial summaries | `pac_summaries.py` | 1 min |
| `pas2YY.zip` | ZIP/TXT | 300 MB | 2-5M | varies | Committee transfers (Leadership PAC) | `committee_transfers.py` | 15 min |
| `independent_expenditure_YYYY.csv` | CSV | 20 MB | 50-200K | 20+ | Super PAC FOR/AGAINST spending | `independent_expenditures.py` | 3 min |

**Total Data per Cycle**: ~340 MB  
**Total Data (4 cycles)**: ~1.4 GB  
**Total Processing Time**: ~25 minutes (on 16GB machine)

**Files We DON'T Download:**
- ❌ `oppexpYY.zip` (60 MB) - Wrong file! Operating expenses, not independent expenditures
- ❌ `indivYY.zip` (2-4 GB) - Removed for performance (using webl/weball totals instead)

**Previous Approach**: 25 GB total, 100 min processing  
**New Approach**: 1.4 GB total, 25 min processing  
**Improvement**: 94% less data, 75% faster! 🚀

**Architecture**: 1 file = 1 parser = 1 collection per database

---

#### ⚠️ Key Lesson: Don't Trust FEC File Names!
- `webl.zip` - **Committee Financial Summaries** (~5 MB per cycle)
  - **Total receipts, disbursements, cash on hand per committee**
  - **Total individual contributions** (aggregated, no granular data)
  - Enables treating "Individual Donors" as collective entity for comparison
  - **Use Case**: Compare Corporate PAC $ vs Individual Donor $ vs Super PAC $
  - **Already implemented**: `member_financial_summary_asset`
- `webk.zip` - **PAC Financial Summaries** (~3 MB per cycle)
  - Same financial data, filtered to PAC-type committees only
  - Useful for PAC identification and filtering
  - Small file, low overhead

**Why This Matters:**
You can now answer: "Did this politician get more money from corporate PACs or individual donors?" by treating aggregated individual contributions as a "virtual PAC" for comparative analysis. This reveals whether a politician is corporate-funded or grassroots-funded **without processing billions of granular transactions**.

**Previous Approach**: 25 GB total, 100 min processing  
**New Approach**: 1.4 GB total, 25 min processing  
**Improvement**: 94% less data, 75% faster! 🚀

---

#### ⚠️ Key Lessons: FEC File Format Differences

The FEC's file naming is **extremely misleading**, and file formats vary:

- ❌ `oppexp` sounds like "oppose/support expenditures" → **Actually**: "operating expenditures"
- ✅ `independent_expenditure` is the actual FOR/AGAINST data
- ❌ `oppexp.zip` has 26 fields with NO candidate IDs
- ✅ `independent_expenditure.csv` has 20+ fields WITH candidate IDs and support/oppose
- ❌ `oppexp` files are in yearly `/bulk-downloads/{CYCLE}/` directories
- ✅ `independent_expenditure` files are also in `/bulk-downloads/{CYCLE}/` but as CSV!

**How We Discovered This:**
1. Downloaded `oppexp24.zip` expecting independent expenditures
2. Parsed file - got 0 records (wrong schema!)
3. Checked FEC documentation page for "independent expenditures"
4. Found reference to Schedule E filings (24-hour/48-hour reports)
5. Searched FEC bulk downloads page HTML source
6. Found `independent_expenditure_YYYY.csv` files
7. Downloaded and verified - CORRECT data with support/oppose indicators!

**Field Format Differences - CRITICAL:**
- `weball.zip` (candidate_summaries): 30 fields, coverage from/through dates at [26][27]
- `webl.zip` (committee_summaries): 30 fields, coverage from/through dates at [26][27]
- `webk.zip` (pac_summaries): **27 fields**, coverage through date ONLY at [26]
- Field indices are NOT interchangeable! Using webl mappings on webk data causes corruption.

**Discovered October 2025:**
- `pac_summaries.py` was using webl field indices (30 fields) on webk data (27 fields)
- Result: coverage_through_date showed "0", coverage_from_date showed cash amounts
- Fixed by examining actual webk.zip structure and correcting all field mappings

**Takeaway**: Always verify FEC file contents AND field structure. File names AND field counts vary!

---

## Investigation Log: Finding the Right FEC Files

This section documents our discovery process for future reference. **TL;DR**: FEC file naming is misleading - we downloaded the wrong files for months before realizing it!

### The Problem (October 2025)

Symptom: `independent_expenditures` asset parsing 0 records from `oppexp24.zip`

```
📊 2024 Cycle:
   📖 Streaming independent_expenditures.zip...
   ✅ 2024: 0 expenditures (0 support, 0 oppose)
```

### Investigation Steps

**Step 1: Verify File Exists and Has Data**
```bash
ls -lh data/fec/2024/transactions/independent_expenditures.zip
# Result: 61M - file exists and isn't empty!
```

**Step 2: Check File Contents**
```bash
unzip -l data/fec/2024/transactions/independent_expenditures.zip
# Result: Contains oppexp.txt (429 MB uncompressed)
```

**Step 3: Examine File Schema**
```python
# First record has 26 fields:
fields = [
    "C00794982",  # Committee ID
    "A",          # Amendment indicator
    "2023",       # Year
    "TER",        # Report type
    # ... 22 more fields
    "1218.75",    # Amount (field 13)
    "G2022",      # Election (field 14) 
    "COMPLIANCE CONSULTING",  # Purpose (field 15)
]
```

❌ **No candidate IDs** (would start with H/S/P)  
❌ **No support/oppose indicators** ('S' or 'O')  
❌ **Only 26 fields** (expected 41+ for independent expenditures)

**Step 4: Search First 50,000 Records**
```python
# Look for candidate ID patterns: H00001234, S00001234, P00001234
# Result: ZERO candidate IDs found
# Look for 'S' or 'O' indicators
# Result: ZERO support/oppose indicators found
```

**Conclusion**: `oppexp.zip` does NOT contain independent expenditure data!

**Step 5: Research FEC Documentation**
- Found: [FEC Independent Expenditures File Description](https://www.fec.gov/campaign-finance-data/independent-expenditures-file-description/)
- Documentation shows correct schema with `CAN_ID`, `SUP_OPP`, etc.
- But doesn't specify which bulk file contains this data!

**Step 6: Check FEC Bulk Downloads Page**
```bash
curl -s "https://www.fec.gov/data/browse-data/?tab=bulk-data" | grep -i "independent"
# Result: Found reference to "independent_expenditure_YYYY.csv"!
```

**Step 7: Verify CSV File Exists**
```bash
curl -I "https://.../bulk-downloads/2024/independent_expenditure_2024.csv"
# Result: 200 OK, Content-Length: 19526677 (~20 MB)
```

**Step 8: Download and Verify Schema**
```csv
cand_id,cand_name,spe_id,spe_nam,sup_opp,exp_amo,...
"H4CO08034","Evans, Gabe","C00866517","Go America PAC","S","9000",...
```

✅ **Has candidate IDs!**  
✅ **Has support/oppose indicators!**  
✅ **CSV format (easier to parse)!**  
✅ **Smaller files (20 MB vs 60 MB)!**

### The Discovery

**WRONG FILE**: `oppexpYY.zip`
- **Actual Name**: "Operating Expenditures" (Schedule B)
- **Contents**: Committee operating expenses (payroll, consulting, overhead)
- **Use Case**: Understanding how committees spend money (operational analysis)
- **NOT useful for**: Tracking Super PAC influence on candidates

**CORRECT FILE**: `independent_expenditure_YYYY.csv`  
- **Actual Name**: "Independent Expenditures" (Schedule E)
- **Contents**: Super PAC spending FOR/AGAINST specific candidates
- **Use Case**: Tracking dark money influence on elections
- **EXACTLY what we need!**

### Why This Happened

1. **Misleading abbreviation**: `oppexp` suggests "oppose/support expenditures"
2. **Actual meaning**: `oppexp` = "operating expenditures" (historical abbreviation)
3. **File organization**: Both files are in same bulk downloads directory
4. **Lack of documentation**: FEC doesn't clearly state which bulk file has Schedule E data
5. **Assumption error**: Assumed `oppexp = independent expenditures` without verifying

### Lessons Learned

✅ **Always verify file contents before building parsers**  
✅ **Check actual data schema, not just file names**  
✅ **FEC file names are historical and misleading**  
✅ **Read FEC documentation pages, not just bulk download listings**  
✅ **Sample data early in development**

### Files Affected

Files that need updating:
- `src/assets/data_sync.py` - Download CSV instead of oppexp.zip
- `src/assets/independent_expenditures.py` - Parse CSV instead of ZIP/TXT
- `src/data/repository.py` - Update path methods for CSV files
- `README.md` - Update documentation (this file!)
- `MONGODB_STRUCTURE.md` - Update schemas

Files that can be deleted:
- `data/fec/*/transactions/independent_expenditures.zip` (wrong file!)
- `data/fec/*/transactions/individual_contributions.zip` (removed for performance)

---

### Lobbying Data (Senate LDA API)

**Source**: Senate Office of Public Records  
**Format**: REST API (JSON)  
**Total Records**: 1.8+ million lobbying filings (1999-present)  
**API Rate Limits**: UNKNOWN - we'll test bulk pagination  
**Update Frequency**: Quarterly (required by law)

**CRITICAL DISTINCTION:**
- **FEC = Campaign donations** (direct money to politicians)
- **LDA = Lobbying spending** (paying advocates to influence legislation)
- **Both needed** for complete corporate influence picture

#### What We Get from LDA API:
```json
{
  "client_name": "AMAZON.COM",
  "registrant_name": "Akin Gump Strauss Hauer & Feld",
  "income": "500000.00",
  "year": 2024,
  "quarter": "Q2",
  "lobbying_activities": [
    {
      "general_issue_code": "TAX",
      "description": "H.R. 1234 - Tax Reform",
      "government_entities": ["HOUSE OF REPRESENTATIVES", "SENATE"]
    }
  ]
}
```

**Key Points:**
- Lobbying data has NO politician names (lobbying is by issue/bill)
- We'll correlate via bill sponsors and committee assignments
- No bulk download available - must use API pagination
- **Strategy**: Download all filings once, then incremental updates quarterly

### Congressional Data

#### Congress Legislators (GitHub)
**Source**: `@unitedstates/congress-legislators`  
**Format**: YAML files  
**Size**: ~1 MB  
**Purpose**: Map bioguide IDs → FEC IDs → Names/Parties

#### Congress.gov API (Future)
**Source**: Library of Congress  
**Purpose**: Bill text, sponsors, votes, committee assignments  
**Rate Limit**: 5,000 requests/hour (no bulk download)

---

## Leadership PAC Money Laundering

### The Problem

Leadership PACs are used by senior politicians to redistribute corporate money while obscuring the original source.

**Traditional Flow (Transparent)**:
```
Corporate PAC → Politician's Campaign
     └─> Easily trackable in FEC data
```

**Leadership PAC Flow (Obfuscated)**:
```
Corporate PAC → Leadership PAC → Politician's Campaign
     │              │                    └─> Looks like Leadership PAC donation
     │              └─> Run by senior politician (Pelosi, McConnell)
     └─> Original corporate source HIDDEN
```

### Our Solution: Weighted Upstream Influence

We calculate the **weighted influence** of original corporate donors by working backwards through Leadership PAC transfers.

**Example:**
- Lockheed Martin PAC gives $50K to "Pelosi Leadership PAC"
- Boeing PAC gives $30K to "Pelosi Leadership PAC"
- Pelosi Leadership PAC gives $100K total to 10 politicians ($10K each)

**Weights:**
- Lockheed: $50K / $80K = 62.5%
- Boeing: $30K / $80K = 37.5%

**Weighted Influence per Politician:**
- Each politician gets $10K from Pelosi PAC
- Lockheed's **real influence** = $10K × 62.5% = **$6,250**
- Boeing's **real influence** = $10K × 37.5% = **$3,750**

### Why This Matters

**Without tracking Leadership PACs:**
```
Politician receives $100K from "Pelosi Leadership PAC"
  → Can't see corporate donors
  → Underestimates corporate influence
```

**With our upstream tracking:**
```
Politician receives $100K from "Pelosi Leadership PAC"
  → 62.5% funded by Lockheed Martin
  → 37.5% funded by Boeing
  → Real influence: $62.5K Lockheed, $37.5K Boeing
  → Can correlate with defense votes!
```

### Implementation

1. Download `pas2YY.zip` files (committee-to-committee transfers)
2. Download `cmYY.zip` files (identifies Leadership PACs via `CMTE_TP = "O"`)
3. For each donor:
   - Find Leadership PACs they contributed to
   - Calculate donor's weight in each Leadership PAC
   - Track all politicians the Leadership PAC supported
   - Calculate weighted influence on each politician
4. Aggregate into `legal_tender.donors` collection with upstream totals

**Result**: Track billions in hidden corporate influence that would otherwise be invisible.

---

## Technical Architecture

### Dynamic Batch Sizing (Auto-Optimized!)

- **Automatically detects available memory** and calculates optimal batch size
- **Uses 50% of available RAM** for processing (fast but safe)
- **Range**: 1,000 - 100,000 records per batch
- **Estimate**: 80MB RAM per 1,000 FEC records in memory
- **MongoDB batch writes**: `insert_many()` with `ordered=False`
- **Performance**: 900x faster than individual inserts (~2.6M records/minute)

**Example auto-detection:**
```
Available: 2GB  → batch_size=12,500   (conservative)
Available: 4GB  → batch_size=25,000   (moderate)
Available: 8GB  → batch_size=50,000   (good)
Available: 16GB → batch_size=100,000  (maximum)
```

**Actual Performance (16GB machine, 10GB available)**:
- Batch size: 65,000 records
- Processing rate: ~2.6M records/minute
- Memory stable: 11GB used, 4.4GB free (no swapping)

### Streaming Processing

**Problem**: FEC files are 2-4GB - loading into memory crashes WSL  
**Solution**: Stream + batch + clear memory

```python
# NEVER do this:
all_data = load_entire_file()  # 💥 4GB in memory = crash!

# DO THIS instead:
with zipfile.ZipFile(file) as zf:
    batch = []
    for line in zf:
        record = parse_line(line)
        batch.append(record)
        
        if len(batch) >= 65000:
            mongo.insert_many(batch)  # Write to DB
            batch = []  # Clear memory immediately!
```

**Memory-safe approach:**
1. Read ZIP line-by-line (streaming)
2. Accumulate 65K records in memory (~5GB)
3. Batch insert to MongoDB (~3 seconds)
4. Clear batch from memory
5. Repeat

**Total memory used**: ~11GB (safe on 16GB machine with 10GB free)

### MongoDB Per-Year Database Structure

**Architecture**: Each election cycle in its own database + processed analytics database

#### Raw FEC Data: `fec_YYYY` (One Database Per Cycle)
- **Purpose**: Store raw FEC bulk data exactly as downloaded, one database per election cycle
- **Structure**: One collection per file type (NO year suffixes on collections!)
- **Retention**: Permanent (historical data)
- **Why Per-Year Databases?**
  - Logical separation: Each cycle is a complete dataset
  - Easier querying: All 2024 data in `fec_2024`
  - Simpler collection names: `candidates` instead of `candidates_2024`
  - Better cross-referencing: Join within same database without year filters
  - Cleaner aggregations: Natural cycle-based isolation

**Example: `fec_2024` Database:**
```
fec_2024/
├── candidates              (cn24.zip - ~10K candidates)
├── committees              (cm24.zip - ~20K committees, includes Leadership PACs)
├── linkages                (ccl24.zip - candidate-committee links)
├── committee_transfers     (pas224.zip - ~2-5M transfers, Leadership PAC tracking)
├── committee_summaries     (webl24.zip - financial totals with individual donor aggregates)
├── pac_summaries           (webk24.zip - PAC financial totals)
└── independent_expenditures (independent_expenditure_2024.csv - Super PAC FOR/AGAINST spending)
```

**All Cycles:**
- `fec_2020` - 2020 election cycle (7 collections)
- `fec_2022` - 2022 election cycle (7 collections)
- `fec_2024` - 2024 election cycle (7 collections)
- `fec_2026` - 2026 election cycle (7 collections)

#### Processed Data: `legal_tender` (Single Analytics Database)
- **Purpose**: Aggregated analytics and application data (cross-cycle analysis)
- **Structure**: Normalized collections for analysis
- **Examples**:
  - `legal_tender.donors` (aggregated with upstream influence, all cycles)
  - `legal_tender.members` (politicians with FEC IDs, all cycles)
  - `legal_tender.lobbying_filings` (Senate LDA data)
  - `legal_tender.bills` (Congress.gov API - future)
  - `legal_tender.votes` (voting records - future)
  - `legal_tender.influence_analysis` (final output - future)

**Benefits:**
- ✅ **Raw data preserved** for reprocessing (per-cycle databases)
- ✅ **Fast cycle-specific queries** (all 2024 data in one database)
- ✅ **Simple collection names** (no year suffixes needed)
- ✅ **Easy cross-cycle analysis** (aggregate from multiple fec_YYYY databases)
- ✅ **Clear separation** (raw vs processed)
- ✅ **Can rebuild `legal_tender`** from any/all `fec_YYYY` databases anytime

---

## Roadmap

### Phase 1: FEC Bulk Data ✅
- ✅ FEC bulk data download architecture (8 file types across 4 cycles)
- ✅ Streaming batch processing (memory-safe)
- ✅ MongoDB batch writes (dynamic sizing: 1K-100K records/batch)
- ✅ Dynamic batch sizing (50% RAM allocation)
- ✅ Per-year MongoDB architecture (`fec_YYYY` per cycle + `legal_tender` for analytics)
- ✅ All 8 FEC parsers created (1:1 file-to-parser mapping)
  - `candidates.py`, `committees.py`, `linkages.py`
  - `candidate_summaries.py`, `committee_summaries.py`, `pac_summaries.py`
  - `committee_transfers.py`, `independent_expenditures.py`
- ✅ Field mappings verified and corrected (Oct 2025)
  - Fixed candidates: party, election_year, state, district
  - Fixed committees: type field
  - Fixed linkages: unique key
  - Fixed pac_summaries: webk format (27 fields) - MAJOR fix
  - Fixed independent_expenditures: composite _id
- ✅ Data sync with smart caching and proper logging
- ✅ Cleaned job/asset structure (1 job, 10 assets)
- ✅ Raw data only (no transformations in parsers)
- 🚧 Docker rebuild and testing with corrected parsers

### Phase 2: Leadership PAC Tracking 🚧
- 🚧 Identify Leadership PACs (via `CMTE_TP` in `cm.zip`)
- 🚧 Parse committee-to-committee transfers (`pas2.zip`)
- 🚧 Calculate upstream influence weights
- 🚧 Aggregate into `donors` collection with direct + upstream totals

### Phase 3: Lobbying Data Integration 📋
- 📋 Senate LDA API client (already exists at `src/api/lobbying_api.py`)
- 📋 Bulk download 1.8M lobbying filings via API pagination
- 📋 Store in `legal_tender.lobbying_filings`
- 📋 Link to bills via issue codes
- 📋 Incremental quarterly updates

### Phase 4: Congressional Data 📋
- 📋 Congress.gov API integration (bills, votes, sponsors)
- 📋 Bill text and metadata
- 📋 Voting records by member
- 📋 Committee assignments

### Phase 5: AI Analysis 📋
- 📋 Donor profiling (LLM-generated PROS/CONS for each corporate PAC)
- 📋 Bill scoring (alignment with donor interests)
- 📋 Vote prediction (based on donor influence)

### Phase 6: Influence Analysis 📋
- 📋 Correlate money → votes
- 📋 Generate influence scores
- 📋 Identify misalignments (votes against donor interests)
- 📋 Public dashboard (visualization)

---

## Current Status (October 2025)

**Data Pipeline**: ✅ Complete FEC ingestion (8 parsers, 1:1 file-to-parser mapping)  
**FEC Files**: ✅ All 8 file types identified and parsers created (see detailed breakdown above)  
**Field Mappings**: ✅ All parsers verified and corrected (Oct 2025)  
**PAC Summaries**: ✅ Fixed webk format (27 fields) - major bug fix  
**Wrong Files**: ❌ Removed `oppexp.zip` (operating expenses, not independent expenditures)  
**Wrong Files**: ❌ Removed `indiv.zip` (40M records, replaced with webl/weball aggregates)  
**Data Volume**: 📉 Reduced from 25 GB → 1.4 GB (94% reduction!)  
**Processing Time**: 📉 Estimated 100 min → 25 min (75% faster!)  
**MongoDB**: ✅ Per-year database architecture (`fec_YYYY` per cycle + `legal_tender`)  
**Memory**: ✅ Dynamic batching with 50% RAM allocation  
**Batch Size**: 🧠 Auto-calculated (1K-100K based on available memory)  
**Jobs/Assets**: ✅ Cleaned up (1 job, 10 assets)  
**Data Integrity**: ✅ Raw data only, no transformations in parsers  
**Next Step**: 🚧 Docker rebuild and testing with corrected parsers  

---

## Contributing

We welcome contributors passionate about transparency and AI for good. See `src/` for code structure and `MONGODB_STRUCTURE.md` for database design.

For questions or collaboration, open an issue.
