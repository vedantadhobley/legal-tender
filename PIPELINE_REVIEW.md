# Legal Tender Data Pipeline Review

## Current Architecture

### 1. Data Flow

```
FEC Bulk Downloads (data_sync)
    ↓
Raw FEC Files → ArangoDB (fec_YYYY databases)
    ├─ cn.zip → candidates
    ├─ cm.zip → committees  
    ├─ ccl.zip → candidate-committee linkages
    ├─ pas2.zip → itemized transactions
    ├─ oth.zip → committee transfers
    └─ indiv.zip → individual contributions
    ↓
Graph Construction (aggregation database)
    ├─ donors (vertices) - $10K+ threshold aggregation
    ├─ employers (vertices)
    ├─ contributed_to (edges) - donor → committee
    ├─ transferred_to (edges) - committee → committee
    ├─ employed_by (edges) - donor → employer
    ├─ affiliated_with (edges) - committee → candidate
    └─ spent_on (edges) - committee → candidate (IEs)
    ↓
Enrichments
    └─ committee_classification - adds terminal_type field
    ↓
Analysis (CLI tools)
    └─ pies_v3.py - graph traversal with proportional attribution
```

### 2. Collections & Their Purpose

**Vertices:**
- `candidates`: FEC candidate master
- `committees`: FEC committee master  
- `donors`: Aggregated individuals ($10K+ threshold)
- `employers`: Unique employer entities

**Edges:**
- `contributed_to`: donor → committee (individual donations)
- `transferred_to`: committee → committee (PAC transfers)
- `employed_by`: donor → employer
- `affiliated_with`: committee → candidate
- `spent_on`: committee → candidate (independent expenditures)

### 3. Current Enrichments

**Three Enrichment Assets (all tracked in Dagster pipeline):**

1. **`committee_classification.py`** - Adds `terminal_type` field to committees
   - Classifies as: campaign, passthrough, corporation, trade_association, labor_union, ideological, cooperative, super_pac_unclassified, unknown
   - Based on CMTE_TP and ORG_TP fields
   - Controls upstream traversal behavior (stop vs trace further)

2. **`donor_classification.py`** ✅ **NEW** - Adds `donor_type` and `whale_tier` fields to donors
   - donor_type: individual, organization, likely_individual, unclear
   - whale_tier: ultra ($1M+), mega ($500K+), whale ($100K+), notable ($50K+), null
   - Pattern matching on name, employer, occupation
   - Creates indices for fast filtering
   - Prepares for RAG enrichment targeting

3. **`committee_financials.py`** ✅ **NEW** - Pre-computes `total_receipts` and `total_disbursed`
   - Calculates from actual edges (contributed_to + transferred_to)
   - Replaces stale FEC cm.zip lifetime totals
   - Critical for proportional attribution accuracy
   - Creates indices for fast lookups

### 4. Remaining Missing Enrichments (future work)

**Should be added later:**

1. **Employer normalization** - Better canonicalization
   - Need to dedupe similar employer names (e.g., "GOOGLE INC" vs "GOOGLE LLC")
   - Could use fuzzy matching or LLM

2. **Geographic data** - ZIP, STATE from raw indiv records
   - Currently not carried to donors vertex
   - Would enable wealthy area analysis

3. **Donor RAG enrichment** - LLM-powered donor profiling
   - Target mega/ultra whales (whale_tier filter)
   - Enrich with: net worth, company role, political history
   - Will be implemented this weekend

### 5. Math Verification

**Proportional Attribution Logic (pies_v3.py):**

```
If Committee A received $100 total and gave Cruz $40:
  flow_ratio = $40 / $100 = 0.4

If AIPAC gave Committee A $20:
  AIPAC's attribution to Cruz = $20 × 0.4 = $8
```

**✅ This is CORRECT** - We calculate:
1. `multiplier = amount_to_target / total_receipts`
2. `attributed = source_amount × multiplier`

**✅ FIXED: committee_financials.py enrichment**
- Now pre-computes total_receipts and total_disbursed from actual edges
- Replaces stale FEC cm.zip lifetime totals with cycle-specific calculations
- Eliminates dynamic recalculation on every query

**Bernie Sanders Verification:**
- Bernie gets $120K from party committees
- Those party committees get millions from corporate PACs
- Our proportional attribution gives Bernie a tiny fraction (correct!)
- Vermont Dem Party $0 issue explained: FEC cm.zip has lifetime totals, our edges are cycle-specific (2020,2022,2024)
- CONCLUSION: Math is correct AND we now calculate from edges instead of relying on FEC totals

### 6. Data Quality Issues

1. ✅ **FIXED: Missing total_disbursed** - Now pre-computed by committee_financials enrichment
2. ✅ **FIXED: Incomplete transfers** - VT Dem Party $0 explained: FEC lifetime totals vs our cycle-specific edges
3. ⚠️ **No donor geography** - Can't analyze by wealthy zip codes (future work)
4. ✅ **FIXED: Manual enrichments** - donor_type now in donor_classification.py pipeline asset
5. ⚠️ **No employer deduplication** - "GOOGLE" vs "GOOGLE INC" vs "GOOGLE LLC" (future work)

### 7. Recommendations

**✅ Priority 1 COMPLETE: Move manual enrichments to Dagster**
- Created `enrichment/donor_classification.py` asset
- Added donor_type AND whale_tier classification to pipeline
- Runs after donors asset

**✅ Priority 2 COMPLETE: Add calculated fields**
- Created `enrichment/committee_financials.py` asset
- Calculates total_receipts, total_disbursed from edges
- Critical for proportional attribution performance

**Priority 3: Geographic enrichment** (future work)
- Carry ZIP/STATE from raw indiv to donors
- Add `enrichment/donor_geography.py` asset
- Enables wealthy area analysis

**Priority 4: Employer normalization** (future work)
- `enrichment/employer_normalization.py` asset
- Dedupe similar employer names
- Improves corporate influence tracking

**✅ Priority 5 COMPLETE: Mega-donor flags**
- Added whale_tier field: ultra ($1M+), mega ($500K+), whale ($100K+), notable ($50K+)
- Enables RAG enrichment targeting
- Helps identify billionaires hiding as "RETIRED"

**Priority 6: RAG enrichment pipeline** (this weekend)
- New asset: `enrichment/donor_rag_enrichment.py`
- For mega/ultra whales, query Wikipedia/Forbes
- Extract: former_employers, net_worth, company_affiliations
- Store in new fields on donor documents

## Summary

**What we have:**
- ✅ Complete FEC data ingestion
- ✅ Graph structure with proper edges
- ✅ Committee classification (terminal_type)
- ✅ Donor classification (donor_type + whale_tier)
- ✅ Committee financials (pre-computed totals)
- ✅ Proportional attribution math (correct!)
- ✅ $10K threshold filtering

**What's missing (future work):**
- ⏳ Geographic data on donors (ZIP/STATE)
- ⏳ Employer normalization
- ⏳ RAG enrichment for mega/ultra whales (this weekend)

**Data quality:**
- ✅ Party committee $0 transfers explained (FEC lifetime vs our cycle-specific)
- ✅ Pre-computed totals eliminate stale FEC data issues
- ⚠️ No historical employer tracking for "RETIRED" donors (will be addressed by RAG)
- ⚠️ Employer names need normalization (future work)

**Performance:**
- ✅ Pre-computed total_receipts/total_disbursed (no more dynamic calculation)
- ✅ Indices on donor_type, whale_tier, terminal_type for fast filtering
- ✅ pies_v3.py now uses pre-computed fields
