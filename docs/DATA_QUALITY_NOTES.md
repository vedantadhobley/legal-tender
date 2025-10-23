# FEC Data Quality Notes

## Independent Expenditure Data Issue

### Problem Discovery (2025-01-22)

The FEC's `independent_expenditure.csv` file contains **unverified data with billions of dollars in fake/troll filings**.

#### Evidence:

1. **Total Amount Discrepancy:**
   - `independent_expenditure.csv`: 73,403 transactions, **$49.7 BILLION**
   - `pas2.zip` (itpas2): 77,661 transactions (24A+24E), **$4.5 BILLION**
   - **10x difference** in dollar amounts despite similar transaction counts

2. **Fake Committee Examples:**
   - "The Court of Divine Justice" (C00875427): $9.9 billion in single filing
   - "The Committee of 300" (C00871210): $9.7 billion
   - "Republican Emo Girl" (C00872432): $6.3 billion
   - "Gus Associates" (C00860767): $255 million (Walter White reference)
   - These committees **DO NOT EXIST** in pas2.zip/itpas2

3. **Data Duplication Error:**
   - Food & Water Action: 9 identical $114,056,874 transactions
   - Total for this one committee: $1.026 billion (obviously wrong)
   - All other transactions for this committee: $21-$2,961 (normal amounts)

### Solution

**Use `pas2.zip` (itpas2 collection) instead of `independent_expenditure.csv`**

The `pas2.zip` file contains **FEC-verified, cleaned transaction data** that excludes fake filings.

#### Transaction Type Mapping (from official FEC documentation):

| Code | Description | Purpose | 2024 Cycle Stats |
|------|-------------|---------|------------------|
| **24A** | Independent expenditure opposing | External spending AGAINST candidates | 19,229 txns, $2.5B |
| **24E** | Independent expenditure advocating | External spending FOR candidates | 58,432 txns, $1.9B |
| **24F** | Communication cost for candidate | Internal member comms (unions/corps) | 708 txns, $7M |
| **24N** | Communication cost against candidate | Internal member comms (unions/corps) | 91 txns, $57K |

**Independent expenditures = 24A + 24E** (+ optionally 24F/24N for completeness)

**NOT** from the `independent_expenditure` collection.

### Implementation Changes (2025-01-22)

#### Files Deleted:
1. ✅ `check_indie_overlap.py` - Temporary analysis script
2. ✅ `src/assets/fec/independent_expenditure.py` - Raw CSV parser (no longer needed)
3. ✅ `src/assets/lt/lt_independent_expenditure.py` - Enrichment layer (no longer needed)

#### Files Updated:
1. ✅ `src/assets/lt/lt_candidate_financials.py` - Now uses itpas2 for indie expenditures
2. ✅ `src/assets/lt/lt_donor_financials.py` - Now uses itpas2 for indie expenditures
3. ✅ `src/__init__.py` - Removed independent_expenditure imports/registrations
4. ✅ `src/assets/__init__.py` - Removed independent_expenditure exports
5. ✅ `src/assets/fec/__init__.py` - Removed independent_expenditure from FEC assets
6. ✅ `src/assets/lt/__init__.py` - Removed lt_independent_expenditure from LT assets
7. ✅ `src/jobs/asset_jobs.py` - Removed independent_expenditure from pipeline job
8. ✅ `src/assets/data_sync.py` - Removed sync_independent_expenditures config
9. ✅ `src/data/repository.py` - Removed fec_independent_expenditure_path method

#### Changes Made:
- Removed dependency on `lt_independent_expenditure` asset
- Changed independent expenditure queries from `independent_expenditure` collection to `itpas2` collection
- Added filter: `transaction_type in ['24A', '24E', '24F', '24N']`
- Mapped transaction types: 24E/24F → support, 24A/24N → oppose
- Updated field mappings to match itpas2 schema (filer_committee_id, transaction_date, memo_text, etc.)
- Removed all references to independent_expenditure from codebase

#### Why This Matters:
- **Data Integrity:** Ensures only FEC-verified transactions are included
- **Accuracy:** Reduces independent expenditure totals from fake $49.7B to real $4.5B
- **Reliability:** Prevents troll filings from contaminating financial aggregations
- **Code Cleanliness:** Eliminates dead code and confusing data sources

#### Next Steps:
- Drop MongoDB collections: `fec_*.independent_expenditure` and `lt_*.independent_expenditure`
- Keep CSV files in `data/fec/*/independent_expenditure.csv` for reference (evidence of data quality issue)

### Other FEC Transaction Types (for reference)

These are handled separately in PAC contributions:

| Code | Description | 2024 Cycle Stats |
|------|-------------|------------------|
| **24K** | Contribution to nonaffiliated committee | 621,922 txns, $513M ⭐ MAIN |
| **24C** | Coordinated party expenditure | 1,094 txns, $90M |
| **24Z** | In-kind contribution | 2,509 txns, $1.7M |

### Lessons Learned

1. **Never trust CSV files from FEC bulk downloads** - they may contain unverified data
2. **Always use ZIP files** (e.g., `pas2.zip`) which contain cleaned, verified data
3. **Verify data against official documentation** - ChatGPT and other AI sources can be completely wrong
4. **Check for outliers** - $9.9 billion from "The Court of Divine Justice" is an obvious red flag

### References

- FEC Transaction Type Codes: https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/
- FEC Bulk Data: https://www.fec.gov/data/browse-data/?tab=bulk-data
- pas2.zip file: Schedule A and E filings (verified transactions)
