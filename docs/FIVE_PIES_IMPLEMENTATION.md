# Five Pies Implementation Tracker

**Started**: February 4, 2026  
**Goal**: Restructure funding analysis from "by source type" to "by organization, by funding method"

## The True Five Pies Vision

For **each organization** (e.g., Goldman Sachs), show what they paid to **each politician** via:

| Pie | Description | Data Source |
|-----|-------------|-------------|
| **1. Direct Donations** | PAC contributions + employee donations | `contributed_to`, `transferred_to` |
| **2. IE Support** | Independent expenditures FOR the candidate | `spent_on` (support_oppose='S') |
| **3. IE Against** | Independent expenditures AGAINST the candidate | `spent_on` (support_oppose='O') |
| **4. Lobbying** | Money spent lobbying that congressperson | Senate LDA API (not yet integrated) |
| **5. Individuals** | Non-corporate small donors | `contributed_to` (non-whale) |

**Key Insight**: Whale donors (big individual donors) get rolled up into their corporate parent via Wikidata linkage.

---

## Current State Assessment (Feb 4, 2026)

### ✅ Working Components

| Component | Collection | Count | Notes |
|-----------|------------|-------|-------|
| Direct donations | `contributed_to` | 3,340,147 | Whale + small donors to committees |
| PAC transfers | `transferred_to` | 654,530 | Committee-to-committee money flow |
| IE spending | `spent_on` | 20,373 | Super PAC spending FOR/AGAINST candidates |
| Committee classification | `committees.terminal_type` | 30,841 | corporation, trade_association, labor_union, ideological, passthrough |
| Upstream tracing | `candidate_upstream` asset | - | Traces money to terminal sources |
| Whale→Company links | `whale_corporate_links` | 40 | Wikidata-resolved whale donors |
| Employer normalization | `canonical_employers` | 73,696 | Grouped employer names |
| Employer→Company mapping | `employer_canonical_mapping` | 500 | Top employers mapped to parent corps |

### ⚠️ Partially Working

| Component | Issue | Impact |
|-----------|-------|--------|
| `corporate_families` | 533 records but `total=0` | Can't aggregate by corporation |
| Super PAC→Corp trace | IE shows PAC spending but doesn't trace WHO funded the PAC | IE not attributed to corps |
| Whale resolution | Only 40 whales linked | Many big donors not rolling up to corps |

### ❌ Not Built Yet

| Component | What's Needed | Priority |
|-----------|---------------|----------|
| Lobbying data ingestion | Asset to pull from `lobbying_api.py` | HIGH |
| Lobbying→Congressperson link | Map filings to specific members | HIGH |
| Five Pies restructure | Output by org→method instead of by source type | MEDIUM |

---

## Implementation Plan

### Phase 1: Fix Corporate Family Rollups
**Status**: ✅ DONE (was already working)

**Finding**: `corporate_families` collection DOES have totals - field is `total_influence`, not `total`.

**Verified Data**:
- 533 corporate families with influence > 0
- Top: Pan Am Systems ($606M), Adelson Enterprises ($378M), ULINE ($291M), Citadel ($229M)
- Whale linkages working: Steyer → Farallon Capital ($58M)

**No code changes needed** - just needed correct field name.

---

### Phase 2: Trace Super PAC Funding for IE Attribution  
**Status**: ✅ DONE

**Problem**: IE data shows WHICH PACs spent on candidates, but doesn't trace WHO funded those PACs.

**Example**:
- FF PAC spent $312M supporting Harris
- Top funders: Future Forward USA Action ($266M), Dustin Moskovitz/Asana ($96M), Bloomberg ($19M)
- **This data IS in the DB** (contributed_to edges to the PAC)
- **But NOT attributed** in candidate's funding_sources

**Solution Applied** (Feb 4, 2026):
- Added `trace_ie_corporate_sources()` function to `candidate_upstream.py` (lines ~300-380)
- For each Super PAC that spent IE on a candidate:
  1. Get the PAC's total receipts
  2. Calculate `multiplier = ie_amount / total_receipts`
  3. Trace contributors via `contrib_edges` (individual donors)
  4. Trace PAC transfers via `transfer_edges` (committee money)
  5. Attribute to corporations via `whale_to_company` or `employer_to_company`
- Updated `funding_sources.ie` structure with:
  - `ie.support.by_corporation` - Corporations that funded pro-candidate Super PACs
  - `ie.support.by_pac` - Other PACs that funded pro-candidate Super PACs
  - `ie.oppose.by_corporation` - Corporations that funded anti-candidate Super PACs
  - `ie.oppose.by_pac` - Other PACs that funded anti-candidate Super PACs

**Tasks**:
- [x] Verify data exists in DB ✅
- [x] Port IE corporate attribution from pies_v3.py to candidate_upstream asset ✅
- [x] Add `ie_corporate_attribution` field to funding_sources output ✅
- [x] Test asset by running materialization ✅
- [x] Verify output for Harris candidate ✅

**Validation Results** (Feb 5, 2026):
```
Kamala Harris - IE Support Corporate Attribution:
  ASANA: $20,685,972         (Dustin Moskovitz funded FF PAC)
  Bloomberg L.P.: $8,850,466
  GREYLOCK: $7,838,059       (Reid Hoffman)
  RIPPLE: $4,819,800
  NETFLIX: $2,919,972        (Reed Hastings)

Donald Trump - IE Oppose Corporate Attribution:
  Simon Youth Foundation: $8,970,545
  SEQUOIA CAPITAL: $8,390,987
  GREYLOCK: $5,273,996
  ASANA: $4,044,702
  Bloomberg L.P.: $3,445,754
```

---

### Phase 3: Integrate Lobbying Data
**Status**: 🔴 Not Started

**Problem**: `lobbying_api.py` exists but no Dagster asset to ingest data, no collection in DB.

**Solution**:
1. Create `lobbying_filings` asset to pull from Senate LDA API
2. Create `lobbied_by` edge collection: organization → congressperson
3. Map lobbying registrants to our `corporate_families`

**Tasks**:
- [ ] Create `src/assets/fec/lobbying.py` asset
- [ ] Design `lobbying_filings` collection schema
- [ ] Create `lobbied_by` edge collection
- [ ] Map lobbying clients to `corporate_families`
- [ ] Link lobbying to specific congresspeople (by committee membership?)

---

### Phase 4: Restructure Five Pies Output
**Status**: 🔴 Not Started

**Problem**: Current output groups by source type (corporations, trade_associations, etc.). Need to group by organization with breakdown by method.

**Current Structure**:
```json
{
  "corporations": {"total": 500000, "top": [...]},
  "trade_associations": {"total": 200000, "top": [...]},
  "ie_support": 1000000,
  "ie_oppose": 50000
}
```

**Target Structure**:
```json
{
  "by_organization": {
    "Goldman Sachs": {
      "direct_donations": 150000,
      "ie_support": 500000,
      "ie_oppose": 0,
      "lobbying": 2300000,
      "total_influence": 2950000
    },
    "Koch Industries": {...}
  },
  "individuals_non_corporate": {
    "total": 5000000,
    "top": [...]
  }
}
```

**Tasks**:
- [ ] Modify `candidate_upstream` asset to output by-organization structure
- [ ] Include breakdown by funding method per organization
- [ ] Keep individuals (non-corporate) as separate category
- [ ] Update `pies_v3.py` CLI to display new structure

---

## Progress Log

### February 4, 2026
- Created this tracking document
- Assessed current state of all components
- Identified 4-phase implementation plan
- Starting with Phase 1: Fix Corporate Family Rollups

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
