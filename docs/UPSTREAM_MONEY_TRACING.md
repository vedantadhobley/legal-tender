# Upstream Money Tracing & AI Legislative Analysis

**Goal**: Trace money from its SOURCE (corporations, billionaires) → through intermediaries (Super PACs, Party Committees) → to DESTINATION (politicians), then map to legislation and lobbying.

---

## � CURRENT STATUS: Upstream Tracing Implemented!

**What We Built** (November 2025):
- ✅ **Corporate PAC Reclassification**: Q/N type committees (corporate/union PACs) now classified as `from_organizations` instead of `from_committees`
- ✅ **Upstream Committee Tracing**: Using `pas2.zip` (itpas2 collection) to trace committee→committee money flows
- ✅ **Data Model Correction**: Fixed CMTE_ID/OTHER_ID inversion (CMTE_ID = donor, OTHER_ID = recipient)
- ✅ **Transparency Scoring**: Red flag detection for dark money sources and shell game patterns

**What's in `pas2.zip`**:
- ✅ Committee → Committee transfers (ENTITY_TP: COM/PAC/PTY/CCM)
- ✅ Individual → Committee donations (ENTITY_TP: IND) - BUT these are PAC-attributed, not direct
- ✅ Organization → Committee contributions (ENTITY_TP: ORG) - earmarked transfers
- ✅ Committee → Candidate transfers (what we use for downstream)

**What We Still Need**:
- 📋 **Direct Individual Donations** (`indiv.zip`) - for billionaire money tracking
  - Currently DISABLED in data_sync (marked "deprecated")
  - Need streaming parser with >$10K threshold filter
  - Will reduce 40M records → 150K records (billionaires only)
- 📋 **Direct Corporate Contributions** - NOT available (illegal - corporations use PACs)

**Key Documents**:
- `docs/PAS2_DATA_MODEL.md` - Explains the corrected data model
- `src/assets/enrichment/enriched_committee_funding.py` - Implements upstream tracing

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

### What We Have Now vs What We Need

#### ✅ Currently Implemented: Committee→Committee Transfers (from pas2.zip)

**What**: Committee → Committee transfers using `pas2.zip` (itpas2 collection)  
**Size**: Already downloaded (~300 MB per cycle)  
**Processing**: Already parsed, just filtered differently in enrichment layer  
**Purpose**: See WHO funds Super PACs and Party Committees (corporate PACs)

**Example Records** (from itpas2 with ENTITY_TP: COM/PAC/PTY):
```
ExxonMobil PAC → Steve Scalise: $250,000
Lockheed Martin PAC → Jason Smith: $180,000
AIPAC → Multiple Republicans: $7.8M total
```

**Corporate PAC Reclassification** (November 2025):
- **Q/N types** (Qualified/Non-qualified PACs) → Classified as `from_organizations`
  - ExxonMobil PAC, Hallmark PAC, AMA PAC, Teamsters PAC
  - Represent corporate/union interests
- **W/O/I types** (Non-connected/Super PACs) → Stay in `from_committees`
  - Conservative Leadership PAC, Business-Industry PAC
  - Political causes, not organizational interests

**Status**: ✅ IMPLEMENTED (enriched_committee_funding.py)

---

#### 📋 Next: Individual→Committee Donations (indiv.zip, NOT itcont.zip)

**Discovery** (November 2025): `itcont.zip` files **DON'T EXIST** (HTTP 404). Use `indiv.zip` instead!

**What**: Individual → Committee donations (billionaires funding Super PACs/committees)  
**Size**: ~4-5 GB per cycle (~15 GB for 4 cycles)  
**Processing**: Stream + filter >$10K = 30-60 sec per cycle (vs 20 min unfiltered)  
**Purpose**: Track billionaire money (Elon Musk, Koch brothers, etc.)

**Example Records** (from indiv.zip):
```
MUSK, ELON (TESLA/SPACEX) → America PAC: $75,000,000
ADELSON, MIRIAM (Las Vegas Sands) → Preserve America PAC: $90,000,000
```

**File Verification** (curl tests):
- ✅ `indiv20.zip`: 5.46 GB (exists, HTTP 200)
- ✅ `indiv22.zip`: 4.86 GB (exists, HTTP 200)
- ✅ `indiv24.zip`: 4.02 GB (exists, HTTP 200)
- ✅ `indiv26.zip`: 0.55 GB (exists, HTTP 200)
- ❌ `itcont{20,22,24,26}.zip`: ALL HTTP 404 (don't exist!)

**Optimization Strategy** (to prevent gaming):
1. **Aggregate by contributor FIRST** (NAME + EMPLOYER + CMTE_ID + Cycle)
2. Sum ALL donations per person per committee per cycle
3. **Then filter** contributors with total > $10,000
4. This catches: Person gives $9,999 x 10 = $99,990 ✅

**Performance Impact**:
- Full parsing: 40M records, 4.5 GB, 20 min (would fail on Raspberry Pi 5)
- Filtered: 150K records, 90 MB, 30-60 sec (266x fewer records, 50x smaller)
- Memory: <500 MB with streaming (RP5 compatible!)

**Status**: 📋 PLANNED (need to create streaming parser asset)

---

#### ⏸️ Deferred: Other Receipts (oth.zip)

**What**: Miscellaneous committee income (loans, refunds, interest)  
**Size**: ~50 MB per cycle (~200 MB for 4 cycles)  
**Purpose**: Complete funding picture with non-contribution income

**Status**: ⏸️ LOW PRIORITY (focus on committee and individual money first)

---

## Understanding pas2.zip - CRITICAL Data Model Insights

**CRITICAL DISCOVERY**: `pas2.zip` was INVERTED from what we thought! See `docs/PAS2_DATA_MODEL.md` for full details.

### What pas2.zip Actually Contains:

`pas2.zip` contains transactions from the **FILER's perspective** (money OUT), NOT recipient's perspective:
- **CMTE_ID** = Committee FILING report (DONOR - giving money)
- **OTHER_ID** = Recipient committee ID (RECIPIENT - receiving money)
- **ENTITY_TP** = Type of transaction/recipient

### What We Get from pas2.zip:

✅ **Committee → Committee transfers**:
```python
# Query: Find who gave TO a committee
itpas2.find({
  "OTHER_ID": target_committee_id,  # Committee receiving money
  "ENTITY_TP": {"$in": ["COM", "PAC", "PTY", "CCM"]},
  "TRANSACTION_TP": "24K"  # Direct contributions
})
# CMTE_ID = donor, OTHER_ID = recipient
```

⚠️ **ENTITY_TP="IND" is MISLEADING**:
- NOT direct individual→committee donations!
- These are PAC→Candidate transfers attributed to individual PAC members
- Example: "Steel PAC gave $1,000 to Terri Sewell, attributed to Philip Bell"

⚠️ **ENTITY_TP="ORG" is ALSO MISLEADING**:
- NOT direct organization→committee contributions!
- These are committee→committee transfers with org attribution
- Often earmarked contributions (e.g., "IAO Property Holdings via PAC to candidate")

### What We DON'T Get from pas2.zip:

❌ **Direct Individual → Committee Donations**:
- Need `indiv.zip` for true billionaire→committee money
- pas2 only has PAC-attributed names, not direct donations

❌ **Direct Organization → Committee Contributions**:
- Would need different data source
- Direct corporate contributions are illegal anyway (corporations use PACs)

### Implementation Status (November 2025):

✅ **Fixed pas2 data model** (CMTE_ID/OTHER_ID swap corrected)
✅ **Implemented committee→committee upstream tracing**
✅ **Reclassified corporate PACs** (Q/N types) as organizations
📋 **Need to add indiv.zip parsing** for direct individual donations

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

### ✅ Phase 1: Upstream Committee Tracing (COMPLETE - November 2025)
**Status**: Implemented in `src/assets/enrichment/enriched_committee_funding.py`

**What We Built**:
1. ✅ Fixed pas2 data model understanding (CMTE_ID/OTHER_ID swap)
2. ✅ Committee→committee upstream tracing using itpas2
3. ✅ Corporate PAC reclassification (Q/N types → from_organizations)
4. ✅ Transparency scoring with red flag detection
5. ✅ Created `enriched_{cycle}.committee_funding_sources` collection

**Example Output**:
```javascript
{
  committee_id: "C00541862",  // Jason Smith For Congress
  funding_sources: {
    from_organizations: [  // Reclassified corporate PACs
      {
        organization_name: "EXXONMOBIL POLITICAL ACTION COMMITTEE",
        organization_type: "Corporate PAC",
        pac_committee_type: "Q",
        total_amount: 250000
      }
    ],
    from_committees: [  // Political PACs/Super PACs
      {
        committee_name: "AMERICAN BRIDGE 21ST CENTURY",
        committee_type: "O",  // Super PAC
        total_amount: 150000
      }
    ],
    from_individuals: []  // Empty until Phase 2
  }
}
```

**Results**:
- Steve Scalise: $7.3M from corporate PACs (ExxonMobil, Lockheed, AIPAC)
- Jason Smith: $7.8M from corporate PACs (Valero, Marathon Petroleum)
- Corporate PACs correctly showing as `from_organizations` instead of `from_committees`

**Processing impact**: +15 min per cycle, +0 MB storage (uses existing itpas2 data)

**Cost impact**: $0 (no new downloads!)

---

### 🚀 Phase 2: Individual Billionaire Tracking (NEXT - 2-3 days)
**Priority: Add `indiv.zip` streaming parser with >$10K threshold**

**Implementation Plan**:
1. Create `src/assets/fec/indiv.py` - Streaming download + parse asset
2. Implement aggregation strategy (aggregate by contributor FIRST, then filter):
   ```python
   # Step 1: Aggregate all donations by (NAME + EMPLOYER + CMTE_ID + Cycle)
   contributor_totals = defaultdict(lambda: {'total': 0, 'count': 0, 'contributions': []})
   
   for txn in stream_indiv_zip():
       key = (txn['NAME'], txn['EMPLOYER'], txn['CMTE_ID'], cycle)
       contributor_totals[key]['total'] += txn['TRANSACTION_AMT']
       contributor_totals[key]['count'] += 1
       contributor_totals[key]['contributions'].append(txn)
   
   # Step 2: Filter contributors with total > $10K
   large_contributors = {
       k: v for k, v in contributor_totals.items() 
       if v['total'] > 10000
   }
   ```
3. Insert into `fec_{cycle}.indiv_large` collection (150K records vs 40M)
4. Update `enriched_committee_funding.py` to query indiv_large
5. Populate `from_individuals` in committee_funding_sources

**This Prevents Gaming**:
- Person gives $9,999 x 10 = $99,990 → Will be captured ✅
- Aggregate first, then filter (not filter individual transactions)

**Example Output After Implementation**:
```javascript
{
  committee_id: "C00571703",  // Senate Leadership Fund
  funding_sources: {
    from_individuals: [  // NEW!
      {
        contributor_name: "MUSK, ELON",
        employer: "TESLA INC",
        total_amount: 75000000,
        contribution_count: 3
      }
    ],
    from_organizations: [...],  // Corporate PACs
    from_committees: [...]       // Political PACs
  }
}
```

**Files to create**:
- `src/assets/fec/indiv.py` - Streaming parser with aggregation

**Processing impact**: +30-60 sec per cycle (vs 20 min unfiltered)

**Storage impact**: +90 MB per cycle (vs 4.5 GB unfiltered) = 95% reduction!

**Cost impact**: $0 (just parsing time, no API costs)

---

### 🎯 Phase 3: AI Committee Positions (1-2 days)
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

### 📊 Phase 4: Lobbying Integration (1 week)
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

### 📖 Phase 5: Bill Ingestion (1 week)
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

### 🤖 Phase 6: AI Bill Matching (2-3 days)
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

### 🎨 Phase 7: Dashboard Queries (ongoing)
**Priority: Build aggregation pipelines for UI**

Common queries to optimize:
1. Most lobbied bills (by spend)
2. Bills by corporate influence (Koch, Citadel, etc.)
3. Member voting vs committee positions (do they vote how funders want?)
4. Dark money tracking (low transparency score committees)
5. Money river visualization (Source → Super PAC → Politician → Bill)

**Deliverable**: Fast queries for frontend

---

### ⏸️ Phase 8: Other Receipts Tracking (deferred)
**Priority: Add `oth{YY}.zip` for loans/refunds**

Only if needed for complete financial picture:
1. Download `oth.zip` files (~50 MB per cycle)
2. Parse loans, refunds, interest income
3. Store in `fec_{cycle}.oth` collection
4. Add to committee_funding_sources

**Processing impact**: +5 min per cycle, +200 MB total

**Decision point**: Low priority - focus on committee and individual money first

---

## Cost Summary

| Phase | Storage | Processing Time | Monthly Cost | One-Time Cost | Status |
|-------|---------|-----------------|--------------|---------------|--------|
| Current (pas2, etc.) | 1.4 GB | 25 min | $0 | $0 | ✅ Done |
| + Upstream Committee Tracing | +0 MB | +15 min | $0 | $0 | ✅ Done |
| + Individual Donations (indiv.zip filtered) | +360 MB | +2 min | $0 | $0 | 📋 Next |
| + AI Positions | +10 MB | +30 min | $1 | $25 | 🎯 Later |
| + Lobbying | +200 MB | +2 hours | $0 | $0 | 📊 Later |
| + Bills | +100 MB | +4 hours | $0 | $0 | 📖 Later |
| + AI Matching | +50 MB | +1 hour | $8 | $0 | 🤖 Later |
| **TOTAL** | **2.12 GB** | **8.5 hours initial** | **$9/month** | **$25** | |

**Current State** (November 2025):
- Storage: 1.4 GB (raw FEC + enrichments)
- Processing: 40 min per cycle (data sync + enrichments)
- Monthly cost: $0 (no API usage yet)
- **Upstream committee tracing WORKING** - showing corporate PAC → politician flows

**After Individual Donations** (Phase 2):
- Storage: 1.76 GB (+360 MB for indiv_large across 4 cycles)
- Processing: 42 min per cycle (+2 min for streaming indiv parser)
- Monthly cost: $0 (still no API usage)
- **Will show billionaire → committee flows**

**Steady State** (All phases complete):
- Monthly processing: ~45 min (updates only)
- Monthly cost: $9 (Claude API for AI matching)
- Storage: 2.12 GB
- Monthly value: **PRICELESS** 🚀

---

## Technical Notes

### Why indiv.zip vs itcont.zip vs pas2.zip?

**CRITICAL DISCOVERY** (November 2025): File structure is NOT what we thought!

#### File Existence Reality Check:

✅ **`indiv{YY}.zip`** - EXISTS (verified via curl):
- indiv20.zip: 5.46 GB (HTTP 200)
- indiv22.zip: 4.86 GB (HTTP 200)
- indiv24.zip: 4.02 GB (HTTP 200)
- indiv26.zip: 0.55 GB (HTTP 200)
- **Total: ~15 GB across 4 cycles**

❌ **`itcont{YY}.zip`** - DOES NOT EXIST:
- itcont20.zip: HTTP 404
- itcont22.zip: HTTP 404
- itcont24.zip: HTTP 404
- itcont26.zip: HTTP 404
- **Contrary to documentation - these files don't exist!**

⚠️ **`pas2{YY}.zip`** - EXISTS but MISLEADING:
- Contains ENTITY_TP="IND" but these are PAC-attributed, NOT direct donations
- "IND" means: PAC→Candidate transfer attributed to individual PAC member
- Example: "Steel PAC gave $1,000 to Terri Sewell, attributed to Philip Bell"
- **NOT the same as direct individual donations!**

#### What Each File Actually Contains:

**`indiv.zip`** (standalone file):
- **Direct** Individual → Committee/Candidate donations
- Includes ALL committees (candidates, PACs, Super PACs, party committees)
- NO CONTRIBUTION LIMITS for Super PAC donations (post-Citizens United)
- 40M+ records per cycle (includes $5, $10, $25 donations)
- **This is the billionaire money we need!**

**`pas2.zip`** (itpas2 collection):
- Committee-filed transactions (Schedule A/B)
- ENTITY_TP="IND" = PAC-attributed contributions (NOT direct individual donations)
- ENTITY_TP="COM/PAC/PTY" = Committee→Committee transfers ✅ (this we use)
- ENTITY_TP="ORG" = Earmarked contributions (NOT direct org donations)

#### Our Approach (November 2025):

**For Committee→Committee Money** (corporate PACs):
```python
# Use pas2.zip (itpas2 collection) - ALREADY IMPLEMENTED
itpas2.find({
  "ENTITY_TP": {"$in": ["COM", "PAC", "PTY", "CCM"]},
  "TRANSACTION_TP": "24K",
  "OTHER_ID": target_committee_id  # Who received money
})
# ✅ This works! Shows ExxonMobil PAC → politicians
```

**For Individual→Committee Money** (billionaires):
```python
# Need indiv.zip (NOT pas2!) - PLANNED FOR PHASE 2
# Step 1: Aggregate by contributor
contributor_totals = {}
for txn in stream_indiv_zip():
    key = (txn['NAME'], txn['EMPLOYER'], txn['CMTE_ID'], cycle)
    contributor_totals[key]['total'] += txn['TRANSACTION_AMT']

# Step 2: Filter contributors with total > $10K
large_contributors = {k: v for k, v in contributor_totals.items() if v['total'] > 10000}
# 📋 Reduces 40M records → 150K (billionaires only)
```

**Why We Can't Use pas2 for Individual Donations**:
1. ENTITY_TP="IND" in pas2 = PAC-attributed (indirect), not direct
2. Only 309 records with ENTITY_TP="IND" in pas2_2024 (vs 40M in indiv.zip)
3. Missing the actual billionaire→Super PAC money trail

**Bottom Line**: 
- ✅ Committee money = Use pas2 (already done)
- 📋 Individual money = Need indiv.zip (phase 2)
- ❌ itcont.zip doesn't exist (was a documentation error)

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

**Last Updated**: November 1, 2025  
**Status**: Phase 1 complete (upstream committee tracing), Phase 2 planned (individual donations)  
**Next Action**: Create `src/assets/fec/indiv.py` streaming parser with >$10K threshold aggregation

**Recent Changes**:
- November 2025: Implemented upstream committee tracing using pas2.zip
- November 2025: Fixed pas2 data model (CMTE_ID/OTHER_ID inversion)
- November 2025: Reclassified corporate PACs (Q/N types) as organizations
- November 2025: Discovered itcont.zip doesn't exist (use indiv.zip instead)
- November 2025: Designed aggregation strategy to prevent threshold gaming
