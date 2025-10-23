# FEC Summary Files Integration

## Overview
This document describes the integration of FEC's official financial summary files (webl, weball, webk) into the Legal Tender pipeline. These files provide FEC's official financial summaries and serve as a validation layer against our transaction-level calculations.

## Background

### The Problem
Previously, these FEC summary files were downloaded but never used:
- `webl.zip`: Candidate financial summaries (cycle-specific)
- `weball.zip`: All-candidate financial summaries (includes future cycles)
- `webk.zip`: PAC/committee financial summaries

### The Solution
We now integrate these files as separate summary datasets that:
1. Filter to tracked members/committees
2. Aggregate across cycles
3. Provide validation against transaction-level calculations
4. Fill data gaps (individual contributions, cash balances, self-funding)

## File Descriptions

### webl.zip - Candidate Summaries (Cycle-Specific)
**Purpose**: Financial summaries for candidates running THIS cycle

**Schema**: 30 fields including:
- `TTL_RECEIPTS`, `TTL_DISB`: Overall financial health
- `TTL_INDIV_CONTRIB`: Individual donations (we don't track these)
- `OTHER_POL_CMTE_CONTRIB`: PAC contributions (compare with our calc)
- `POL_PTY_CONTRIB`: Party support
- `CAND_CONTRIB`, `CAND_LOANS`: Self-funding
- `COH_BOP`, `COH_COP`: Cash balances
- `DEBTS_OWED_BY`: Campaign debt

**Use Case**: Primary dataset for candidate financial summaries

### weball.zip - All-Candidate Summaries
**Purpose**: Financial summaries for ALL candidates with activity this cycle (including future cycles)

**Schema**: Same 30 fields as webl

**Key Difference**: 
- webl = candidates running in THIS cycle
- weball = ANY candidate with activity in THIS cycle (includes 2026 candidates in 2024 data)

**Use Case**: Detect early fundraising for future cycles

### webk.zip - Committee/PAC Summaries
**Purpose**: Financial summaries for PACs and committees

**Schema**: 27 fields including:
- `INDV_CONTRIB`: Who funds the PACs
- `IND_EXP`: Independent expenditure total (validation field!)
- `CONTRIB_TO_OTHER_CMTE`: PAC activity
- `COH_BOP`, `COH_COP`: PAC financial health

**Key Difference**: Different schema (27 vs 30 fields), different entity type (committees vs candidates)

**Use Case**: PAC funding sources and validation of independent expenditure calculations

## Data Flow

```
FEC.gov bulk files
  ├─ webl.zip   → fec_{cycle}.webl   → lt_{cycle}.webl   ┐
  ├─ weball.zip → fec_{cycle}.weball → lt_{cycle}.weball ├→ legal_tender.candidate_summaries
  └─ webk.zip   → fec_{cycle}.webk   → lt_{cycle}.webk   ┴→ legal_tender.committee_summaries
```

### Phase 1: FEC Parsers (fec_{cycle} databases)
Raw FEC summary files are parsed and stored per cycle:
- `fec_2020.webl`, `fec_2020.weball`, `fec_2020.webk`
- `fec_2022.webl`, `fec_2022.weball`, `fec_2022.webk`
- `fec_2024.webl`, `fec_2024.weball`, `fec_2024.webk`

### Phase 2: LT Enrichment (lt_{cycle} databases)
Filter to tracked members/committees:

**lt_webl / lt_weball**:
- Uses `member_fec_mapping` to filter to tracked members
- Enriches with `bioguide_id` for cross-cycle aggregation
- No additional enrichment needed (webl/weball already have candidate names)

**lt_webk**:
- Uses `ccl` (candidate-committee linkages) to find committees linked to tracked members
- Filters `webk` to those committee IDs
- Enriches with `bioguide_id` of linked candidates
- No additional enrichment needed (webk already has committee names)

### Phase 3: Cross-Cycle Aggregation (legal_tender database)

**candidate_summaries**:
```python
{
  "_id": "C001118",  # bioguide_id
  "cycles": [
    {
      "cycle": "2024",
      "source": "webl",
      "total_receipts": 5000000,
      "total_disbursements": 4500000,
      ...
    },
    {
      "cycle": "2024", 
      "source": "weball",
      "total_receipts": 5100000,  # May differ from webl
      ...
    }
  ],
  "totals": {
    "total_receipts_all_time": 15000000,  # Sum across cycles (webl only)
    "total_disbursements_all_time": 13500000,
    "current_cash_on_hand": 500000,  # Latest cycle
    "current_debts_owed_by": 100000
  }
}
```

**committee_summaries**:
```python
{
  "_id": "C00123456",  # committee_id
  "committee_name": "Democrats for Progress PAC",
  "committee_type": "O",
  "cycles": [
    {
      "cycle": "2024",
      "total_receipts": 10000000,
      "independent_expenditures": 2500000,  # Validation field!
      ...
    }
  ],
  "totals": {
    "total_receipts_all_time": 30000000,
    "total_independent_expenditures": 7500000,  # Compare with our 24A/24E calc
    "total_contributions_to_other_committees": 5000000,
    ...
  }
}
```

## New Assets Created

### LT Enrichment Layer (src/assets/lt/)
1. **lt_webl.py**: Filter webl to tracked members per cycle
2. **lt_weball.py**: Filter weball to tracked members per cycle
3. **lt_webk.py**: Filter webk to committees linked to tracked members

### Cross-Cycle Aggregation Layer (src/assets/legal_tender/)
4. **candidate_summaries.py**: Aggregate webl + weball across all cycles by bioguide_id
5. **committee_summaries.py**: Aggregate webk across all cycles by committee_id

## Pipeline Integration

The new assets are integrated into `fec_pipeline_job` in these phases:

**Phase 4**: LT cycle computations
- `lt_webl`, `lt_weball`, `lt_webk` (filter to tracked members/committees)

**Phase 6**: Cross-cycle aggregations
- `candidate_summaries` (aggregate webl + weball)
- `committee_summaries` (aggregate webk)

## Use Cases

### 1. Validation Layer
Compare our transaction-level calculations against FEC's official summaries:
- **PAC Contributions**: Our calculation vs `OTHER_POL_CMTE_CONTRIB` in webl
- **Independent Expenditures**: Our 24A/24E sum vs `IND_EXP` in webk
- **Total Receipts/Disbursements**: Sanity check against official totals

### 2. Data Gap Filling
FEC summaries provide data we don't track:
- **Individual Contributions**: `TTL_INDIV_CONTRIB` in webl (we only track PACs)
- **Cash Balances**: `COH_BOP`, `COH_COP` in webl/webk
- **Self-Funding**: `CAND_CONTRIB`, `CAND_LOANS` in webl
- **Campaign Debt**: `DEBTS_OWED_BY` in webl

### 3. Early Detection
`weball` includes future cycle activity:
- 2026 candidates raising money in 2024
- Early fundraising signals for upcoming races

### 4. PAC Analysis
`webk` shows committee funding sources:
- Who funds the PACs (`INDV_CONTRIB`)
- PAC-to-PAC transfers (`CONTRIB_TO_OTHER_CMTE`)
- PAC financial health (`COH_COP`, `DEBTS_OWED_BY`)

## Key Design Decisions

### Why Both webl AND weball?
- **webl**: Cycle-specific, candidates running THIS cycle (primary dataset)
- **weball**: Includes future cycle activity (early fundraising detection)
- They have different filtering logic but same schema
- Storing both allows comparison and early signal detection

### Why Separate lt_webl/lt_weball/lt_webk?
- Follows existing architecture pattern (fec → lt → legal_tender)
- Each cycle gets its own filtered view
- Enables per-cycle analysis and cross-cycle aggregation
- Clean separation of concerns

### Why No Enrichment in lt_webl/lt_weball?
- webl/weball already contain all candidate information (name, party, district)
- Only need bioguide_id from member_fec_mapping for aggregation
- No need to join with cn (candidate master)

### Why No Enrichment in lt_webk?
- webk already contains committee names and types
- Only need bioguide_id of linked candidates from ccl → member_fec_mapping
- No need to join with cm (committee master)

### Why Include Both webl and weball in candidate_summaries.cycles?
```python
"cycles": [
  {"cycle": "2024", "source": "webl", ...},
  {"cycle": "2024", "source": "weball", ...}
]
```
- Allows comparison between cycle-specific and all-activity views
- Detects discrepancies (future cycle activity showing up early)
- Totals use webl only to avoid double-counting

## Validation Examples

### Example 1: PAC Contribution Validation
```python
# Our calculation (from itpas2 transactions)
candidate_financials["C001118"]["by_category"]["pac_contributions"]["total"]

# FEC official summary (from webl)
candidate_summaries["C001118"]["cycles"][0]["other_committee_contributions"]

# Should match (or be close, accounting for timing differences)
```

### Example 2: Independent Expenditure Validation
```python
# Our calculation (from itpas2 24A/24E transactions)
donor_financials["C00123456"]["independent_expenditures"]["total"]

# FEC official summary (from webk)
committee_summaries["C00123456"]["totals"]["total_independent_expenditures"]

# Should match (validates our 24A/24E filtering is correct)
```

## Files Modified

### Created
- `src/assets/lt/lt_webl.py`
- `src/assets/lt/lt_weball.py`
- `src/assets/lt/lt_webk.py`
- `src/assets/legal_tender/candidate_summaries.py`
- `src/assets/legal_tender/committee_summaries.py`

### Updated
- `src/assets/__init__.py`: Export new assets
- `src/assets/lt/__init__.py`: Export new lt assets
- `src/assets/legal_tender/__init__.py`: Export new summaries
- `src/__init__.py`: Register new assets in defs
- `src/jobs/asset_jobs.py`: Add new assets to pipeline

## Next Steps

1. **Test the Pipeline**
   ```bash
   dagster asset materialize --select candidate_summaries
   dagster asset materialize --select committee_summaries
   ```

2. **Validation Queries**
   ```python
   # Compare our PAC calculations with FEC summaries
   # Compare our independent expenditure calculations with webk
   # Check for discrepancies and investigate
   ```

3. **Dashboard Integration**
   - Add FEC summary totals to member detail views
   - Show validation status (our calc vs FEC official)
   - Highlight discrepancies for investigation

4. **Data Quality Monitoring**
   - Alert on large discrepancies between our calc and FEC official
   - Track coverage (% of transactions that match summaries)
   - Monitor for FEC summary file updates

## References

- FEC Bulk Data Documentation: https://www.fec.gov/campaign-finance-data/bulk-data/
- webl/weball schema: 30 fields (candidate financial summaries)
- webk schema: 27 fields (committee/PAC financial summaries)
- Related: `docs/DATA_QUALITY_NOTES.md` (independent_expenditure cleanup)
