# Upstream Money Tracing & AI Legislative Analysis

**Goal**: Trace money from its SOURCE (corporations, billionaires) → through intermediaries (Super PACs, Party Committees) → to DESTINATION (politicians), then map to legislation and lobbying.

---

## 🎉 MAJOR DISCOVERY: We Already Have The Data!

**TL;DR**: We thought we needed to download 3 new FEC files (`itoth.zip`, `itcont.zip`, `oth.zip`) totaling ~17 GB. 

**REALITY**: `pas2.zip` (which we already download and parse) contains **ALL** itemized transactions including:
- ✅ Committee → Committee transfers (upstream funding!)
- ✅ Individual → Committee donations (billionaire money!)
- ✅ Organization → Committee contributions (corporate money!)
- ✅ Committee → Candidate transfers (what we use now)

**What This Means**:
- ❌ NO new downloads needed
- ❌ NO new storage (we have the data in `itpas2` collection)
- ❌ NO new parsing code
- ✅ Just need to filter `itpas2` differently in enrichment layer
- ✅ Upstream tracing can be built in 2-3 days, not 1 week
- ✅ Zero cost impact

**How We Discovered This**:
1. Planned to add `itoth.zip` for committee→committee transfers
2. Reviewed existing `itpas2.py` asset
3. Found comment: "Contains ALL itemized transactions... Entity types include CCM, ORG, IND, PAC, COM, PTY"
4. Realized `pas2.zip` has everything we need!

**Next Steps**: 
- Create enrichment asset that filters `itpas2` by entity type
- Build `committee_funding_sources` collection
- Start tracing money upstream from Super PACs to corporate sources

---

## The Core Problem: Dark Money Chains

**What You See Now (Downstream Only)**:
```
Senate Leadership Fund → Ted Cruz ($5M)
```
❌ **Problem**: Senate Leadership Fund is NOT the real source. It's a middleman.

**What We Need (Full Chain)**:
```
Koch Industries ($2M) ─┐
Citadel ($1.5M) ───────┤
Unknown/Dark Money ($1.5M) ─→ Senate Leadership Fund → Ted Cruz ($5M)
```
✅ **Solution**: Trace money UPSTREAM to see who's really funding politicians.

---

## FEC Data Files: Upstream vs Downstream

### Files We Already Have (Downstream = Where Money Goes)

| File | Purpose | What It Shows |
|------|---------|---------------|
| `itpas2{YY}.zip` | PAC → Candidate contributions | Committee gives TO candidate |
| `independent_expenditure_{YYYY}.csv` | Super PAC FOR/AGAINST spending | Super PAC spends ON candidate |
| `oppexp{YY}.zip` | Operating expenses | Committee operational spending |

**These show the END of the money chain, not the beginning.**

---

### Files We Need to Add (Upstream = Where Money Comes FROM)

#### 1. `itoth{YY}.zip` - Any Transaction FROM One Committee TO Another ✅ PRIORITY

**What**: Committee → Committee transfers (the reverse of what we track now)  
**Size**: ~100 MB per cycle (~400 MB for 4 cycles)  
**Processing**: ~10 min per cycle (40 min total)  
**Purpose**: See WHO funds Super PACs and Party Committees

**Example Records**:
```
Koch Industries PAC → Senate Leadership Fund: $2,000,000
Citadel PAC → Congressional Leadership Fund: $1,500,000
Corporate PAC Network → America PAC: $5,000,000
```

**Why Critical**:
- Reveals corporate funding of Super PACs
- Shows party committee funding networks
- Enables transparency scoring (how much is traceable?)
- **This is the file we thought we had but DON'T!**

**Status**: ❌ NOT INGESTED YET (we have `pas2` which is different - see below)

---

#### 2. `itcont{YY}.zip` - Individual Contributions TO Committees ⚠️ LARGE

**What**: Individual → Committee donations (billionaires funding Super PACs)  
**Size**: ~2-4 GB per cycle (~16 GB for 4 cycles)  
**Processing**: ~30 min per cycle (2 hours total, requires streaming)  
**Purpose**: See billionaire funding of Super PACs and dark money groups

**Example Records**:
```
MUSK, ELON (TESLA/SPACEX) → America PAC: $75,000,000
ADELSON, MIRIAM (Las Vegas Sands) → Preserve America PAC: $90,000,000
```

**Why Different from `indiv{YY}.zip` (which we removed)**:
- `indiv{YY}.zip`: Individual → **Candidate Committee** (has $3,300 limit per election)
- `itcont{YY}.zip`: Individual → **Super PAC/Party Committee** (NO LIMIT!)

**Key Insight**: Super PACs can accept unlimited individual donations! That's why billionaires use them.

**Optimization Strategy**:
- Don't ingest ALL records (too big)
- Filter to contributions > $10,000 (major donors only)
- This reduces 16 GB → ~500 MB
- Still captures 90% of the money

**Status**: ❌ NOT INGESTED YET (deferred until after itoth)

---

#### 3. `oth{YY}.zip` - Other Receipts (Loans, Refunds, Interest)

**What**: Miscellaneous committee income  
**Size**: ~50 MB per cycle (~200 MB for 4 cycles)  
**Processing**: ~5 min per cycle (20 min total)  
**Purpose**: Complete the funding picture (loans, refunds, interest income)

**Status**: ❌ NOT INGESTED YET (low priority)

---

## What About `pas2{YY}.zip`?

**MAJOR DISCOVERY**: We ARE already ingesting upstream data! 🎉

`pas2.zip` contains **ALL itemized transactions**, including:
- Committee → Candidate (Schedule A/B - what we filter for now)
- Committee → Committee (Schedule A/B - **UPSTREAM DATA WE NEED!**)
- Individual → Committee (Schedule A - large donors!)
- Organization → Committee (Schedule A - corporate contributions!)

**Current State**:
- ✅ We HAVE the data in `fec_{cycle}.itpas2` collection
- ❌ We're NOT filtering/using it for upstream tracing yet
- ❌ We're only using entity type = candidate in enrichment layer

**The Data We Need is Already There**:
```
pas2.zip contains:
├── Committee → Candidate (entity_tp = CAN) ← We use this ✅
├── Committee → Committee (entity_tp = COM/PAC/PTY) ← We IGNORE this ❌
├── Individual → Committee (entity_tp = IND) ← We IGNORE this ❌
└── Organization → Committee (entity_tp = ORG) ← We IGNORE this ❌
```

**What We Need to Do**:
1. Create new enrichment asset that filters `itpas2` for:
   - `entity_tp IN ('COM', 'PAC', 'PTY')` → Committee → Committee
   - `entity_tp = 'IND'` AND `amount > 10000` → Large individual donors
   - `entity_tp = 'ORG'` → Corporate contributions
2. Build `committee_funding_sources` collection from existing data
3. NO NEW DOWNLOADS NEEDED! 🚀

**The Files We DON'T Need**:
- ❌ `itoth.zip` - redundant, data is in `pas2.zip`!
- ❌ `itcont.zip` - redundant, data is in `pas2.zip`!
- ❌ `oth.zip` - might still need for loans/refunds, TBD

**This Changes Everything**:
- No new downloads (we have the data!)
- No new storage (it's already there!)
- No new processing time (data already parsed!)
- Just need to filter differently in enrichment layer!

**Implementation is now MUCH simpler** - see revised Phase 1 below.

---

## Committee Types & Transparency Scoring

### FEC Committee Types (CMTE_TP field)

| Code | Type | Dark Money? | Disclosure |
|------|------|-------------|------------|
| **C** | Communication Cost (501c orgs) | 🔴 YES | No donor disclosure required |
| **E** | Electioneering Communication | 🟡 PARTIAL | Limited disclosure |
| **I** | Independent Expenditor | 🟡 PARTIAL | Disclose spending, not always sources |
| **O** | Super PAC | 🟢 DISCLOSED | Must disclose all donors |
| **U** | Single-Candidate Super PAC | 🟢 DISCLOSED | Must disclose all donors |
| **V/W** | Hybrid PAC | 🟡 MIXED | Two accounts (disclosed + unlimited) |
| **Q** | Traditional PAC | 🟢 DISCLOSED | Must disclose all donors |
| **N** | PAC (Non-qualified) | 🟢 DISCLOSED | Must disclose all donors |
| **Y** | Party Committee (Qualified) | 🟢 DISCLOSED | Must disclose all donors |
| **X** | Party Committee (Non-qualified) | 🟢 DISCLOSED | Must disclose all donors |

### Transparency Score Calculation

```javascript
transparency_score = disclosed_sources / total_receipts

Example (Senate Leadership Fund):
- Total receipts: $90M
- From disclosed PACs (itoth): $70M
- From billionaires (itcont): $15M
- Unknown gap: $5M (potential dark money)
- Score: 85M / 90M = 0.94 (94% transparent)
```

**Red Flags**:
- Receives money from Type C committees (501c dark money)
- Large unknown funding gap (total spending > disclosed receipts)
- Receives from other Super PACs (layering/shell game)
- Unusual refund/loan patterns

---

## MongoDB Collections for Upstream Tracing

### New Collection: `enriched_{cycle}.committee_funding_sources`

```javascript
{
  _id: "C00571703",  // Senate Leadership Fund committee ID
  committee_name: "SENATE LEADERSHIP FUND",
  committee_type: "O",  // Super PAC
  cycle: "2024",
  
  // WHO funds this committee (UPSTREAM)
  funding_sources: {
    // From other committees (itoth data)
    from_committees: [
      {
        committee_id: "C00123456",
        committee_name: "Koch Industries PAC",
        committee_type: "Q",  // Traditional PAC
        connected_org: "KOCH INDUSTRIES",
        total_amount: 2000000,
        transaction_count: 4,
        first_date: "2024-01-15",
        last_date: "2024-10-20",
        transactions: [...]  // Top 10 largest
      },
      {
        committee_id: "C00789012",
        committee_name: "Citadel PAC",
        committee_type: "Q",
        connected_org: "CITADEL LLC",
        total_amount: 1500000,
        transaction_count: 3,
        transactions: [...]
      }
    ],
    
    // From individuals (itcont data, filtered >$10K)
    from_individuals: [
      {
        contributor_name: "MUSK, ELON",
        employer: "TESLA INC",
        occupation: "CEO",
        total_amount: 75000000,
        transaction_count: 3,
        contributions: [...]  // Top 10 only
      }
    ],
    
    // Other receipts (oth data)
    other_receipts: {
      loans: 0,
      refunds: 50000,
      interest: 12000,
      other: 0,
      total: 62000
    }
  },
  
  // Transparency scoring
  transparency: {
    total_receipts: 90062000,
    from_disclosed_committees: 70000000,
    from_disclosed_individuals: 15000000,
    from_other: 62000,
    total_disclosed: 85062000,
    unknown_gap: 5000000,  // Potential dark money
    transparency_score: 0.944,  // 94.4% traceable
    
    // Red flags
    red_flags: [
      {
        type: "shell_game",
        description: "Receives from 3 other Super PACs (potential layering)",
        severity: "medium"
      }
    ]
  },
  
  // WHERE this committee's money GOES (link to existing downstream data)
  spending: {
    to_candidates_support: 30000000,     // From itpas2 + independent_expenditure (support)
    to_candidates_oppose: 10000000,      // From independent_expenditure (oppose)
    to_other_committees: 5000000,        // From pas2 (transfers out)
    operating_expenses: 40000000,        // From oppexp
    total_spent: 85000000
  },
  
  computed_at: ISODate("2025-10-25T...")
}
```

### Enhanced Collection: `aggregation.donor_financials`

Add upstream fields to existing donor records:

```javascript
{
  _id: "C00571703",  // Senate Leadership Fund
  committee_name: "SENATE LEADERSHIP FUND",
  committee_type: "O",
  
  // EXISTING: Where money goes (downstream)
  total_spent: 85000000,
  total_to_candidates: 45000000,
  // ... all existing fields ...
  
  // NEW: Where money comes from (upstream)
  funding: {
    total_receipts: 90062000,
    from_corporate_pacs: 70000000,      // Sum of from_committees where connected_org exists
    from_billionaires: 15000000,        // Sum of individuals > $100K
    from_small_donors: 0,               // Sum of individuals < $100K
    from_dark_money: 5000000,           // Unknown gap
    transparency_score: 0.944
  },
  
  // NEW: Ultimate corporate sources (trace back to connected_org)
  ultimate_sources: [
    {
      organization: "KOCH INDUSTRIES",
      total_amount: 2000000,
      path: "Koch Industries PAC → Senate Leadership Fund"
    },
    {
      organization: "CITADEL LLC",
      total_amount: 1500000,
      path: "Citadel PAC → Senate Leadership Fund"
    }
  ]
}
```

---

## AI Legislative Analysis System

### Phase 1: Committee Position Generation (Cost: ~$25 one-time + $1/month)

**Approach**: For each committee, generate pro/con legislative positions using Claude

**Prompt Template**:
```
Generate 10 legislative positions this committee would SUPPORT and 
10 positions they would OPPOSE. Use web search if needed for accuracy.

Committee: {committee_name}
Type: {committee_type}
Connected Organization: {connected_org}
Top Recipients: {top_candidates}
Spending Pattern: {spending_summary}

Format as JSON:
{
  "would_support": [
    "Specific policy position 1",
    "Specific policy position 2",
    ...
  ],
  "would_oppose": [
    "Specific policy position 1",
    "Specific policy position 2",
    ...
  ]
}
```

**Example Output (Senate Leadership Fund)**:
```json
{
  "organization": "Senate Leadership Fund",
  "would_support": [
    "Conservative judicial appointments and confirmations",
    "Border security and immigration enforcement legislation",
    "Tax cuts and reduced government spending",
    "Energy independence and fossil fuel development",
    "Second Amendment protections and gun rights",
    "Restrictions on transgender participation in women's sports",
    "Criminal justice reforms focused on tougher sentencing",
    "Free market policies and reduced business regulations",
    "Restrictions on abortion access",
    "School choice and education reform initiatives"
  ],
  "would_oppose": [
    "Expanded government spending programs",
    "Green New Deal or aggressive climate change legislation",
    "Universal healthcare or Medicare for All proposals",
    "Gun control measures and assault weapons bans",
    "Progressive tax increases on corporations and wealthy individuals",
    "Expansion of labor union rights",
    "Gender-affirming care for minors",
    "Cashless bail and criminal justice reform reducing sentences",
    "Increased environmental regulations on energy sector",
    "Expansion of voting access measures opposed by Republicans"
  ]
}
```

**Cost Breakdown**:
- Claude 3.5 Sonnet pricing: $3/M tokens input, $15/M tokens output
- Per committee: ~200 tokens input, ~300 tokens output = $0.005
- 5,000 committees initial run: $25
- Monthly updates (200 new/changed): $1/month

**Storage**: Add to `aggregation.donor_financials`:
```javascript
{
  _id: "C00571703",
  committee_name: "SENATE LEADERSHIP FUND",
  // ... existing fields ...
  
  ai_positions: {
    would_support: [...],
    would_oppose: [...],
    generated_at: ISODate("2025-10-25"),
    model: "claude-3-5-sonnet-20241022"
  }
}
```

---

### Phase 2: Bill Analysis & Matching (Cost: ~$8/month)

**Lobbying Data Integration**:
```javascript
// Collection: legal_tender.lobbying_filings
{
  _id: "12345-2024-Q2",
  client_name: "AMAZON.COM",
  registrant_name: "Akin Gump Strauss Hauer & Feld",
  amount: 500000,
  year: 2024,
  quarter: "Q2",
  
  lobbying_activities: [
    {
      bill_number: "H.R.1234",
      bill_title: "Tax Reform Act",
      general_issue: "TAX",
      specific_issues: "Corporate tax rates, offshore profits repatriation",
      government_entities: ["HOUSE WAYS AND MEANS", "SENATE FINANCE"]
    }
  ]
}
```

**Bill Collection** (from Congress.gov API):
```javascript
// Collection: legal_tender.bills
{
  _id: "hr1234-118",
  bill_number: "H.R.1234",
  congress: 118,
  title: "Tax Reform Act of 2024",
  summary: "This bill reduces corporate tax rates from 21% to 15%...",
  introduced_date: "2024-03-15",
  
  sponsors: [
    {
      bioguide_id: "C001118",
      name: "CRUZ, TED",
      role: "sponsor"
    }
  ],
  
  // Total lobbying spend on this bill
  lobbying_spend: {
    total_amount: 2500000,
    client_count: 15,
    top_clients: [
      {client_name: "AMAZON.COM", amount: 500000},
      {client_name: "APPLE INC", amount: 400000}
    ]
  },
  
  // AI-matched committee positions
  committee_matches: [
    {
      committee_id: "C00123456",
      committee_name: "Chamber of Commerce PAC",
      support_score: 95,  // 0-100
      oppose_score: 5,
      rationale: "Strongly aligns with pro-business tax reduction positions"
    }
  ],
  
  ai_analysis: {
    policy_areas: ["Tax Policy", "Corporate Law", "Economic Development"],
    generated_at: ISODate("2025-10-25")
  }
}
```

**Matching Prompt**:
```
Given this bill and these committee positions, calculate alignment scores:

Bill: {bill_title}
Summary: {bill_summary}
Sponsors: {sponsor_list}

Committee: {committee_name}
Support Positions: {would_support}
Oppose Positions: {would_oppose}

Return JSON:
{
  "support_score": 0-100,
  "oppose_score": 0-100,
  "rationale": "2-3 sentence explanation",
  "key_alignments": ["specific position matches"]
}
```

**Cost for 2,000 priority bills** (with tracked sponsors):
- Input: 1,000 tokens per bill
- Output: 200 tokens per bill
- Total: 2M input + 400K output
- Cost: (2M × $3/M) + (400K × $15/M) = $6 + $6 = $12 per cycle
- Monthly: $12 / 24 months = $0.50/month

---

### Phase 3: Money → Legislation Dashboard

**Query Examples**:

**1. Most Lobbied Bills (sorted by spending)**:
```javascript
db.bills.aggregate([
  {$sort: {"lobbying_spend.total_amount": -1}},
  {$limit: 100},
  {$project: {
    bill_number: 1,
    title: 1,
    lobbying_spend: 1,
    sponsors: 1
  }}
])
```

**2. Committee → Money → Sponsor → Bill Chain**:
```javascript
db.bills.aggregate([
  // Match bills sponsored by tracked members
  {$match: {"sponsors.bioguide_id": {$in: tracked_bioguide_ids}}},
  
  // Join to candidate financials
  {$lookup: {
    from: "candidate_financials",
    localField: "sponsors.bioguide_id",
    foreignField: "bioguide_id",
    as: "sponsor_finances"
  }},
  
  // Join to committee positions
  {$lookup: {
    from: "donor_financials",
    localField: "committee_matches.committee_id",
    foreignField: "_id",
    as: "supporting_committees"
  }},
  
  // Calculate influence score
  {$addFields: {
    influence_score: {
      // Sum of: (committee support score × money from committee to sponsor)
      $sum: {
        $map: {
          input: "$supporting_committees",
          as: "cmte",
          in: {
            $multiply: [
              "$$cmte.support_score",
              {$divide: [
                "$$cmte.amount_to_sponsor",
                100
              ]}
            ]
          }
        }
      }
    }
  }}
])
```

**3. Corporate Source → Bill Influence**:
```javascript
// "Show me all bills where Koch Industries has influence"
db.bills.aggregate([
  {$lookup: {
    from: "donor_financials",
    let: {bill_committees: "$committee_matches.committee_id"},
    pipeline: [
      {$match: {
        $expr: {$in: ["$_id", "$$bill_committees"]},
        "ultimate_sources.organization": "KOCH INDUSTRIES"
      }},
      {$project: {
        committee_name: 1,
        "ultimate_sources.organization": 1,
        "ultimate_sources.total_amount": 1
      }}
    ],
    as: "koch_funded_committees"
  }},
  
  {$match: {"koch_funded_committees": {$ne: []}}},
  
  {$project: {
    bill_number: 1,
    title: 1,
    koch_influence: "$koch_funded_committees",
    lobbying_spend: 1
  }}
])
```

---

## Implementation Roadmap

### ✅ Phase 0: Current State (Complete)
- FEC downstream data (candidates, committees, transfers TO candidates)
- Member FEC mapping (bioguide → candidate IDs)
- Candidate financials (who receives money)
- Donor financials (who gives money downstream)

### 🚀 Phase 1: Upstream Tracing (Next - 2-3 days) **SIMPLIFIED!**
**Priority: Filter existing `itpas2` data for upstream sources**

**Discovery**: We already have ALL the data in `pas2.zip` / `itpas2` collections!

**New Implementation Plan**:
1. ~~Create new FEC asset~~ ← NOT NEEDED!
2. ~~Add to data_sync downloads~~ ← NOT NEEDED!
3. Create enrichment asset: `src/assets/enrichment/enriched_committee_funding.py`
4. Query existing `fec_{cycle}.itpas2` collection with filters:
   ```python
   # Committee → Committee transfers
   itpas2.find({"ENTITY_TP": {"$in": ["COM", "PAC", "PTY"]}})
   
   # Large individual donors (>$10K)
   itpas2.find({"ENTITY_TP": "IND", "TRANSACTION_AMT": {"$gt": 10000}})
   
   # Corporate contributions
   itpas2.find({"ENTITY_TP": "ORG"})
   ```
5. Build `enriched_{cycle}.committee_funding_sources` collection
6. Calculate transparency scores
7. Add upstream fields to `aggregation.donor_financials`

**Deliverable**: See corporate PACs funding Super PACs **using data we already have!**

**Files to create**:
- `src/assets/enrichment/enriched_committee_funding.py`

**Processing impact**: +15 min, +0 MB storage (data already exists!)

**Cost impact**: $0 (no new downloads!)

---

### 🎯 Phase 2: AI Committee Positions (1-2 days)
**Priority: Generate legislative positions for top committees**

1. Add Anthropic SDK to requirements: `anthropic>=0.34.0`
2. Create asset: `src/assets/ai/committee_positions.py`
3. Generate positions for top 1,000 committees (by spending)
4. Store in `donor_financials.ai_positions`
5. Build prompt refinement based on results

**Deliverable**: "Senate Leadership Fund supports X, opposes Y"

**Files to create**:
- `src/assets/ai/committee_positions.py`
- `src/assets/ai/__init__.py`

**Cost impact**: $25 one-time + $1/month

---

### 📊 Phase 3: Lobbying Integration (1 week)
**Priority: Ingest Senate LDA API data**

1. Use existing `src/api/lobbying_api.py`
2. Create asset: `src/assets/lobbying/lobbying_filings.py`
3. Bulk download 1.8M filings (paginated API calls)
4. Store in `legal_tender.lobbying_filings`
5. Create indexes on bill_number, client_name, year

**Deliverable**: See lobbying spend by bill

**Files to create**:
- `src/assets/lobbying/lobbying_filings.py`
- `src/assets/lobbying/__init__.py`

**Processing impact**: ~2 hours initial, quarterly updates

---

### 📖 Phase 4: Bill Ingestion (1 week)
**Priority: Congress.gov API integration**

1. Use existing `src/api/congress_api.py`
2. Create asset: `src/assets/congress/bills.py`
3. Download bills from Congress.gov API (rate limit: 5,000/hour)
4. Store in `legal_tender.bills`
5. Link to lobbying via bill_number
6. Link to sponsors via bioguide_id

**Deliverable**: Bills database with sponsors and lobbying

**Files to create**:
- `src/assets/congress/bills.py`
- `src/assets/congress/__init__.py`

**Processing impact**: ~4 hours (rate limited), daily updates

---

### 🤖 Phase 5: AI Bill Matching (2-3 days)
**Priority: Match bills to committee positions**

1. Create asset: `src/assets/ai/bill_matching.py`
2. For each bill with tracked sponsor:
   - Get all committees that funded sponsor
   - Calculate alignment scores with committee positions
   - Store in `bills.committee_matches`
3. Sort bills by influence score

**Deliverable**: "This bill aligns 95% with Koch-funded committees"

**Files to create**:
- `src/assets/ai/bill_matching.py`

**Cost impact**: $8/month

---

### 🎨 Phase 6: Dashboard Queries (ongoing)
**Priority: Build aggregation pipelines for UI**

Common queries to optimize:
1. Most lobbied bills (by spend)
2. Bills by corporate influence (Koch, Citadel, etc.)
3. Member voting vs committee positions (do they vote how funders want?)
4. Dark money tracking (low transparency score committees)
5. Money river visualization (Source → Super PAC → Politician → Bill)

**Deliverable**: Fast queries for frontend

---

### ⏳ Phase 7: Large Donor Tracking (deferred)
**Priority: Add `itcont{YY}.zip` with filtering**

Only if needed for billionaire tracking:
1. Download `itcont` with streaming
2. Filter to contributions > $10,000
3. Store in `enriched_{cycle}.large_individual_donors`
4. Link to committees

**Processing impact**: +2 hours, +500 MB (filtered)

**Decision point**: Do after Phase 1-6, evaluate if needed

---

## Cost Summary

| Phase | Storage | Processing Time | Monthly Cost | One-Time Cost |
|-------|---------|-----------------|--------------|---------------|
| Current | 1.4 GB | 25 min | $0 | $0 |
| + Upstream (filter itpas2) | +0 MB | +15 min | $0 | $0 |
| + AI Positions | +10 MB | +30 min | $1 | $25 |
| + Lobbying | +200 MB | +2 hours | $0 | $0 |
| + Bills | +100 MB | +4 hours | $0 | $0 |
| + AI Matching | +50 MB | +1 hour | $8 | $0 |
| **TOTAL** | **1.76 GB** | **8.25 hours initial** | **$9/month** | **$25** |

**Steady state**: 
- Monthly processing: ~30 min (updates only)
- Monthly cost: $9 (Claude API)
- Storage: 1.76 GB (no new downloads needed!)
- Monthly value: **PRICELESS** 🚀

---

## Technical Notes

### Why itcont vs indiv?

**IMPORTANT DISCOVERY**: Both `indiv.zip` and `itcont.zip` data are IN `pas2.zip`!

**The Real Story**:

`pas2.zip` contains **ALL itemized transactions**:
- Schedule A (Receipts): Money coming INTO committees
- Schedule B (Disbursements): Money going OUT of committees

**What this means**:

**`indiv{YY}.zip`** (standalone file - REMOVED, now redundant):
- Individual → **Candidate Committee** donations
- Subject to $3,300 per election limit (hard cap)
- 40M+ records per cycle
- **Redundant with `pas2.zip` where `ENTITY_TP = 'IND'` AND recipient is candidate committee**
- We removed this because:
  - Already in `pas2.zip`!
  - Too large (16 GB) for limited value
  - Capped contributions (not major influence)
  - Aggregates available in `webl.zip` summaries

**`itcont{YY}.zip`** (standalone file - ALSO REDUNDANT!):
- Individual → **Super PAC/Party Committee** donations
- **NO CONTRIBUTION LIMITS** (post-Citizens United)
- Same size as `indiv` (~2-4 GB per cycle)
- **Also redundant with `pas2.zip` where `ENTITY_TP = 'IND'` AND recipient is Super PAC/Party**
- Contains $10M+ donations from billionaires
- Can be filtered to >$10K contributions = 90% of money, 10% of records

**Your Insight is Correct**:
- `indiv`: Individual → Candidate (limited to $3,300)
- `itcont`: Individual → Super PAC (UNLIMITED!)
- **But BOTH are in `pas2.zip` already!**

**Why FEC has separate files**:
- Historical reasons (different filing schedules)
- Convenience for researchers who only want one type
- But for comprehensive analysis, `pas2.zip` has it all

**What we're doing**:
```python
# We already have this data in itpas2!
# Just need to filter by ENTITY_TP and CMTE_TP:

# Large donors to Super PACs (the billionaire money)
itpas2.find({
  "ENTITY_TP": "IND",           # From individual
  "TRANSACTION_AMT": {"$gt": 10000},  # Major donation
  # Recipient is Super PAC (can query committee type from cm collection)
})

# Small donors to candidates (grassroots)
itpas2.find({
  "ENTITY_TP": "IND",           # From individual  
  "TRANSACTION_AMT": {"$lte": 3300},  # Within limit
  # Recipient is candidate committee
})
```

**Bottom Line**: We already have ALL the individual contribution data in `pas2.zip` / `itpas2` collection. No need to download separate files!

---

## Questions for Future Context

1. **Transparency Scoring**: Should we penalize committees receiving from Type C (dark money) more heavily?

2. **AI Position Validation**: How do we verify Claude's generated positions are accurate? Manual spot-checking? Cross-reference with news?

3. **Bill Priority Filtering**: Which bills should we analyze first?
   - Bills with tracked member sponsors? ✅
   - Bills with >$100K lobbying spend? ✅
   - Bills that passed committee? ✅
   - All bills? ❌ (too many)

4. **Upstream Depth**: How many levels should we trace?
   - Level 0: Individual/Corporate → Committee (itoth + itcont)
   - Level 1: Committee → Committee (itoth)
   - Level 2: Committee → Committee → Committee (recursive?)
   - Terminal: Individual donors or Type C (stop)

5. **Large Donor Threshold**: What's the cutoff for "major donor"?
   - $10,000? (catches most billionaire donations)
   - $50,000? (ultra-wealthy only)
   - $100,000? (mega-donors only)

---

## Document Version

**Last Updated**: October 25, 2025  
**Status**: Planning phase - upstream tracing ready to implement  
**Next Action**: Create `itoth.py` asset and begin Phase 1
