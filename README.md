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
  - [MongoDB Two-Database Structure](#mongodb-two-database-structure)
- [Roadmap](#roadmap)

---

## Quick Start

```bash
./start.sh -v
```

**Processing time:** ~100 minutes for 8 years of FEC data (4 cycles: 2020-2026)

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
**Format**: ZIP files containing pipe-delimited text  
**Update Frequency**: Weekly  
**API Rate Limits**: NONE (bulk download, not API)

We process **8 years** of FEC data (4 election cycles: 2020, 2022, 2024, 2026):

#### Individual Contributions (`indiv.zip`)
- **Size**: 2-4 GB per cycle (~12 GB total)
- **Records**: ~40-50 million contributions
- **Purpose**: Aggregate by employer/PAC to track corporate influence
- **Processing**: ~60 minutes for all 4 cycles

#### Independent Expenditures (`oppexp.zip`)
- **Size**: 2-3 GB per cycle (~10 GB total)
- **Records**: ~5-10 million Super PAC expenditures
- **Purpose**: Track Super PAC spending FOR/AGAINST candidates
- **Innovation**: Opposition stored as **NEGATIVE** values for net influence
- **Processing**: ~25 minutes for all 4 cycles

#### Committee-to-Committee Transfers (`pas2.zip`)
- **Size**: ~300 MB per cycle (~1.2 GB total)
- **Records**: ~2-5 million transfers between PACs
- **Purpose**: **Track Leadership PAC money laundering** (see below)
- **Processing**: ~15 minutes for all 4 cycles

#### Financial Summary Files (Aggregated Totals)
- `weball.zip` - **Candidate Financial Summaries** (~7 MB per cycle)
  - **Total receipts, disbursements, cash on hand per candidate**
  - **Total individual contributions** (aggregated, no granular data)
  - Enables treating "Individual Donors" as collective entity for comparison
  - **Use Case**: Compare Corporate PAC $ vs Individual Donor $ vs Super PAC $
  - **Efficiency**: 7MB vs 2-4GB (no need to process 40M+ individual records)
- `webl.zip` - Committee Financial Summaries (~5 MB per cycle)
- `webk.zip` - PAC Financial Summaries (~3 MB per cycle)

**Why This Matters:**
You can now answer: "Did this politician get more money from corporate PACs or individual donors?" by treating aggregated individual contributions as a "virtual PAC" for comparative analysis. This reveals whether a politician is corporate-funded or grassroots-funded **without processing billions of granular transactions**.

#### Metadata Files (Small)
- `cn.zip` - Candidates (~2 MB per cycle)
- `cm.zip` - Committees (~1 MB per cycle, identifies Leadership PACs)
- `ccl.zip` - Candidate-Committee linkages (~1 MB per cycle)

**Total FEC Data**: ~25 GB across 8 years  
**Total Processing**: ~100 minutes on 16GB machine  
**Summary Files**: Already implemented in `src/assets/financial_summary.py`

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

### MongoDB Two-Database Structure

**Architecture**: Separate raw FEC data from processed analytics

#### Database 1: `fec_bulk` (Raw FEC Files)
- **Purpose**: Store raw FEC bulk data exactly as downloaded
- **Structure**: One collection per file per cycle
- **Retention**: Permanent (historical data)
- **Examples**:
  - `fec_bulk.individual_contributions_2020`
  - `fec_bulk.individual_contributions_2022`
  - `fec_bulk.independent_expenditures_2024`
  - `fec_bulk.committee_transfers_2024` (Leadership PAC tracking)

#### Database 2: `legal_tender` (Processed Data)
- **Purpose**: Aggregated analytics and application data
- **Structure**: Normalized collections for analysis
- **Examples**:
  - `legal_tender.donors` (aggregated with upstream influence)
  - `legal_tender.members` (politicians with FEC IDs)
  - `legal_tender.lobbying_filings` (Senate LDA data)
  - `legal_tender.bills` (Congress.gov API - future)
  - `legal_tender.votes` (voting records - future)
  - `legal_tender.influence_analysis` (final output - future)

**Benefits:**
- Raw data preserved for reprocessing
- Fast queries on aggregated data
- Clear separation of concerns
- Can rebuild `legal_tender` from `fec_bulk` anytime

---

## Roadmap

### Phase 1: FEC Bulk Data ✅ → 🚧
- ✅ FEC bulk data download (4 cycles: 2020-2026)
- ✅ Streaming batch processing (memory-safe)
- ✅ MongoDB batch writes (65K records/batch)
- ✅ Individual contributions parser (`indiv.zip`)
- ✅ Independent expenditures parser (`oppexp.zip`)
- ✅ Candidate/committee/linkage data (`cn`, `cm`, `ccl`)
- ✅ Dynamic batch sizing (50% RAM allocation)
- ✅ Two-database MongoDB architecture
- 🚧 **Committee transfers parser** (`pas2.zip` - Leadership PAC tracking)
- 🚧 **Per-file asset refactor** (parallel processing)

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

**Data Pipeline**: ✅ Operational for 8 years of FEC data  
**Processing Time**: ✅ ~100 minutes (auto-optimized for available memory)  
**MongoDB**: ✅ Two-database architecture (`fec_bulk` + `legal_tender`)  
**Memory**: ✅ Dynamic batching with 50% RAM allocation  
**Batch Size**: 🧠 Auto-calculated (1K-100K based on system, typically 65K on 16GB)  
**Leadership PAC Tracking**: 🚧 Designed, implementation pending  
**Lobbying Data**: 🚧 API client exists, integration pending  

---

## Contributing

We welcome contributors passionate about transparency and AI for good. See `src/` for code structure and `MONGODB_STRUCTURE.md` for database design.

For questions or collaboration, open an issue.
