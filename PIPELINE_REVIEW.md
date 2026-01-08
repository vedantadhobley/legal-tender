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

**Only One Enrichment Asset:**
- `committee_classification.py` - Adds `terminal_type` field to committees
  - Classifies as: campaign, passthrough, corporation, trade_association, labor_union, ideological, cooperative, super_pac_unclassified, unknown
  - Based on CMTE_TP and ORG_TP fields

**Manual Enrichments (not in Dagster assets):**
- `donor_type` - Added via manual AQL UPDATE query (not tracked in pipeline!)
  - Classifies donors as: organization, individual, likely_individual, unclear
  - Uses pattern matching on donor names

### 4. Missing Enrichments

**Should be Dagster assets:**

1. **`donor_type` classification** - Currently manual!
   - Should be in `enrichment/donor_classification.py`
   - Needs to run after `donors` asset

2. **`total_receipts` / `total_disbursed`** - Currently None on many committees
   - Should aggregate from transferred_to edges
   - Critical for proportional attribution math

3. **Employer normalization** - Currently basic
   - Need better canonicalization (e.g., "GOOGLE INC" vs "GOOGLE LLC")
   - Should dedupe similar employer names

4. **Mega-donor flags** - For "whale" detection
   - Mark donors >$50K, >$100K, >$1M
   - Helps identify enrichment candidates for RAG

5. **Geographic data** - Completely missing
   - ZIP, STATE from raw indiv records not carried to donors
   - Would help with wealthy area analysis

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

**Issue Found:**
- Many committees have `total_disbursed = None` because we don't calculate it
- We calculate it dynamically in queries, but it should be pre-computed
- This forces us to recalculate on every query (inefficient)

**Bernie Sanders Verification:**
- Bernie gets $120K from party committees
- Those party committees get millions from corporate PACs
- Our proportional attribution would give Bernie a tiny fraction
- BUT: Vermont Dem Party shows $0 in our data (data quality issue?)
- CONCLUSION: Math is correct, but we may have incomplete transfer data

### 6. Data Quality Issues

1. **Missing total_disbursed** - Forces dynamic calculation
2. **Incomplete transfers** - VT Dem Party shows $0 disbursed
3. **No donor geography** - Can't analyze by wealthy zip codes
4. **Manual enrichments** - donor_type not in pipeline
5. **No employer deduplication** - "GOOGLE" vs "GOOGLE INC" vs "GOOGLE LLC"

### 7. Recommendations

**Priority 1: Move manual enrichments to Dagster**
- Create `enrichment/donor_classification.py` asset
- Add donor_type classification to pipeline
- Ensure it runs after donors asset

**Priority 2: Add calculated fields**
- `enrichment/committee_financials.py` asset
- Calculate total_receipts, total_disbursed from edges
- Critical for proportional attribution performance

**Priority 3: Geographic enrichment**
- Carry ZIP/STATE from raw indiv to donors
- Add `enrichment/donor_geography.py` asset
- Enables wealthy area analysis

**Priority 4: Employer normalization**
- `enrichment/employer_normalization.py` asset
- Dedupe similar employer names
- Improves corporate influence tracking

**Priority 5: Mega-donor flags**
- Add tier fields: whale_tier (bronze/silver/gold/platinum)
- Mark candidates for RAG enrichment
- Helps identify billionaires hiding as "RETIRED"

**Priority 6: RAG enrichment pipeline**
- New asset: `enrichment/donor_rag_enrichment.py`
- For mega-donors ($50K+), query Wikipedia/Forbes
- Extract: former_employers, net_worth, company_affiliations
- Store in new fields on donor documents

## Summary

**What we have:**
- ✅ Complete FEC data ingestion
- ✅ Graph structure with proper edges
- ✅ Committee classification
- ✅ Proportional attribution math (correct!)
- ✅ $10K threshold filtering

**What's missing:**
- ❌ donor_type in pipeline (manual only)
- ❌ Pre-computed financial totals
- ❌ Geographic data on donors
- ❌ Employer normalization
- ❌ Mega-donor detection
- ❌ RAG enrichment for billionaires

**Data quality:**
- ⚠️ Some party committee transfers showing $0 (incomplete?)
- ⚠️ No historical employer tracking for "RETIRED" donors
- ⚠️ Employer names need normalization

**Performance:**
- ⚠️ Dynamic total_disbursed calculation is inefficient
- ⚠️ Should pre-compute and cache in committee documents
