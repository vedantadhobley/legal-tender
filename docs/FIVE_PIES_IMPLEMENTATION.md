# Five Pies Implementation

**Last Updated**: February 5, 2026  
**Status**: Core implementation complete, needs validation run

## Mission

For **each candidate**, per **election cycle**, show **who is funding them** broken down by:

| Pie | Description | Data Source |
|-----|-------------|-------------|
| **1. Direct PAC** | Corporate/Trade/Union PAC contributions | `transferred_to` traced to terminal |
| **2. Employee Donations** | Individual donations rolled up by employer | `contributed_to` → employer mapping |
| **3. IE Support** | Super PAC spending FOR candidate, traced to funders | `spent_on` → contributor tracing |
| **4. IE Oppose** | Super PAC spending AGAINST candidate, traced to funders | `spent_on` → contributor tracing |
| **5. Lobbying** | *(NOT YET IMPLEMENTED)* | Senate LDA API |

**Key Insight**: We trace through passthrough committees (JFCs, party committees) to find the **terminal source** - the corporation, union, or individual that originally gave the money.

---

## Current State (Feb 5, 2026)

### ✅ What's Built and Working

| Component | Collection | Count | Status |
|-----------|------------|-------|--------|
| Contribution edges | `contributed_to` | 3.3M | ✅ Has cycle field |
| Transfer edges | `transferred_to` | 654K | ✅ Has cycle field |
| IE edges | `spent_on` | 20K | ✅ Has cycle + support_oppose |
| Committee classification | `committees.terminal_type` | 30K | ✅ corp/union/trade/ideological/passthrough |
| Employer normalization | `canonical_employers` | 73K | ✅ Typo grouping |
| Employer→Company mapping | `employer_canonical_mapping` | 500 | ✅ Top employers mapped |
| Whale→Company links | `whale_corporate_links` | 40 | ✅ Billionaire attribution |
| Corporate families | `corporate_families` | 533 | ✅ Aggregated influence |

### ⚠️ Needs Validation

| Component | Issue |
|-----------|-------|
| `candidate_upstream` asset | Code complete but **never actually run** |
| `candidate_funding` collection | **Does not exist** - asset creates it on first run |
| Per-cycle computation | Code ready, needs materialization |

### ❌ Not Implemented

| Component | What's Needed |
|-----------|---------------|
| Lobbying data | Asset to pull from Senate LDA API |
| Lobbying→Member links | Map filings to congresspeople |

---

## Architecture

### The `candidate_upstream` Asset

Located at: `src/assets/aggregation/candidate_upstream.py`

**What it does:**
1. Loads all edges into memory, grouped by cycle (2020, 2022, 2024)
2. For each candidate with affiliated committees:
   - Traces money backwards through `transferred_to` edges
   - Stops at terminal types (corporation, labor_union, etc.)
   - Continues through passthrough types
   - Attributes individual donors via employer mapping
3. Traces IE spending back to funders (who funded the Super PACs)
4. Computes Five Pies per cycle + aggregate
5. Stores in `candidates.funding_sources`

**Output Structure:**
```json
{
  "funding_sources": {
    "by_cycle": {
      "2020": { /* full five pies data */ },
      "2022": { /* full five pies data */ },
      "2024": { /* full five pies data */ }
    },
    "aggregate": { /* combined across all cycles */ },
    "by_organization": [
      {
        "name": "ASANA",
        "direct_pac": 0,
        "direct_employees": 121465,
        "ie_support": 20685972,
        "ie_oppose": 0,
        "total_pro": 20807437
      }
    ],
    "cycles_available": ["2020", "2022", "2024"],
    "computed_at": "2026-02-05T..."
  }
}
```

---

## Dead Code Cleanup (Feb 6, 2026)

### 🗑️ Deleted

| Path | Reason |
|------|--------|
| `src/cli/pies.py` | Superseded by `candidate_upstream` asset. |
| `src/cli/pies_v2.py` | Intermediate version, superseded. |
| `src/utils/upstream.py` | Logic moved into `candidate_upstream.py`. |
| `scripts/_deprecated/*` | Old fix scripts, no longer needed. |

### ✅ Kept

| Path | Reason |
|------|--------|
| `src/cli/pies_v3.py` | Debug tool for ad-hoc queries. |

### ⚠️ Review Before Deleting

| Path | Issue |
|------|-------|
| `src/assets/aggregation/candidate_summaries.py` | Does different thing than `candidate_upstream` but may overlap. Check if still needed. |
| `src/assets/aggregation/committee_summaries.py` | Pre-computed committee data. May be useful for UI. |
| `src/assets/aggregation/donor_summaries.py` | Pre-computed donor profiles. May be useful for UI. |

### ✅ Keep (Active Code)

| Path | Purpose |
|------|---------|
| `src/assets/aggregation/candidate_upstream.py` | **THE** Five Pies implementation |
| `src/rag/employer_normalization.py` | Used by enrichment assets |
| `src/rag/wikidata_client.py` | Used by enrichment assets |
| `src/api/lobbying_api.py` | Will be used for Phase 3 (lobbying integration) |

---

## Relationship: candidate_upstream vs candidate_summaries

**They are DIFFERENT:**

| Asset | Purpose | Output |
|-------|---------|--------|
| `candidate_upstream` | Trace money to corporate origins | `funding_sources` with Five Pies |
| `candidate_summaries` | Pre-compute UI stats | `funding_summary` with totals |

`candidate_summaries` predates the Five Pies work and does simpler aggregation (total by cycle, top donors). It does NOT trace through passthroughs to terminal sources.

**Recommendation**: Keep both for now. `candidate_summaries` may be faster for simple queries. `candidate_upstream` is the authoritative source for corporate attribution.

---

## Next Steps

1. ~~**Run the asset**~~ ✅ Done - 11,796 candidates, 4,879 with funding
2. ~~**Validate output**~~ ✅ Done - Harris, Trump, Cruz, McConnell all verified
3. ~~**Clean up dead code**~~ ✅ Done - Deleted pies.py, pies_v2.py, upstream.py, deprecated scripts
4. **Document API** - How frontend should query `candidates.funding_sources`
5. **Lobbying integration** - Create asset for Senate LDA API

---

## Testing Queries

### Check corporate_families totals
```aql
FOR c IN corporate_families
SORT c.total DESC
LIMIT 10
RETURN {name: c.canonical_name, total: c.total}
```

### Check whale linkages
```aql
FOR w IN whale_corporate_links
RETURN {donor: w.donor_name, company: w.canonical_name, total: w.total}
```

### Check IE with committee info
```aql
FOR s IN spent_on
LIMIT 5
LET cmte = DOCUMENT(s._from)
RETURN {
  candidate: PARSE_IDENTIFIER(s._to).key,
  support_oppose: s.support_oppose,
  amount: s.total_amount,
  pac_name: cmte.CMTE_NM
}
```
