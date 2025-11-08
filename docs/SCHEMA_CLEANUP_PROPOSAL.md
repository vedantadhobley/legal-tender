# Schema Cleanup Proposal

## Problems

1. **Redundant top-level fields**: All duplicate `totals.summary` (performance shows NO benefit)
2. **Confusing upstream_funding organization**: Indie vs PAC upstream in different places

## Current Structure (CONFUSING)

```javascript
{
  // REDUNDANT (duplicate totals.summary)
  total_independent_support: 9253311,
  total_pac_contributions: 4268133,
  net_benefit: 10628285,
  // ... 5 more
  
  // INDIE UPSTREAM (aggregated)
  upstream_funding: {
    independent_expenditures: {
      supporting: {funded_by: {organizations, individuals, political_committees}},
      opposing: {funded_by: {...}}
    }
  },
  
  // DIRECT FUNDING (per-committee upstream buried inside)
  totals: {
    direct_funding: {
      from_political_pacs: {
        committees: [
          {committee_id, amount, upstream_funding: {...}}  // ← HIDDEN HERE
        ]
      }
    }
  }
}
```

## Proposed Structure (CLEAN)

**Option A: Inline Everything (Remove top-level upstream_funding)**

```javascript
{
  totals: {
    // INDIE EXPENDITURES
    independent_expenditures: {
      supporting: {
        committees: [
          {
            committee_id, 
            amount, 
            committee_type,  // NEW: Can filter corporate vs political
            upstream_funding: {organizations, individuals, committees}
          }
        ],
        total: 9253311,
        // AGGREGATE upstream at this level
        upstream_funding: {organizations, individuals, committees}
      },
      opposing: {...same structure...}
    },
    
    // DIRECT CONTRIBUTIONS
    direct_funding: {
      from_political_pacs: {
        committees: [{...upstream_funding...}],  // Already here
        total: 3456789,
        // AGGREGATE upstream at this level
        upstream_funding: {organizations, individuals, committees}
      },
      from_corporate_pacs: {
        committees: [{...no upstream...}],
        total: 811344  // Corporate = ultimate source
      }
    },
    
    // SUMMARY (keep, remove redundant top-level)
    summary: {
      total_independent_support: 9253311,
      total_pac_contributions: 4268133,
      net_benefit: 10628285,
      // ...
    }
  }
}
```

**Benefits:**
- ✅ All upstream funding in consistent locations (within each contribution type)
- ✅ Both per-committee AND aggregate upstream available
- ✅ No redundant top-level fields
- ✅ Symmetric structure across contribution types
- ✅ Easy to find: "Show me indie expenditure upstream? Look in indie section"

**Option B: Move Everything to upstream_funding (Keep top-level)**

```javascript
{
  upstream_funding: {
    independent_expenditures: {
      supporting: {...funded_by...},
      opposing: {...funded_by...}
    },
    direct_contributions: {
      from_political_pacs: {...funded_by...},
      from_corporate_pacs: null  // No upstream (ultimate source)
    }
  },
  
  totals: {
    independent_expenditures: {committees, totals},
    direct_funding: {committees, totals},
    summary: {...}
  }
}
```

**Benefits:**
- ✅ All upstream in ONE place
- ❌ Separates upstream from the thing it describes (less intuitive)
- ❌ Can't easily show per-committee upstream

## Recommendation

**Option A** - Inline everything. Reasons:

1. **Consistency**: All contribution types have the same structure (committees array + aggregate)
2. **Locality**: Upstream funding lives WITH the thing it describes
3. **Flexibility**: Can show both per-committee and aggregate upstream
4. **Simplicity**: Remove redundant top-level fields (performance shows no benefit)
5. **Discoverability**: "Where's indie upstream?" → "In the indie section"

## Implementation Changes

### Files to modify:
1. `aggregation/candidate_financials.py` (~581 lines)
   - Remove lines ~465-472 (redundant top-level fields)
   - Move upstream_funding.independent_expenditures INTO totals.independent_expenditures
   - Add aggregate upstream_funding to totals.direct_funding.from_political_pacs

### Backward compatibility:
- ⚠️ BREAKING CHANGE: Remove top-level fields
- ⚠️ BREAKING CHANGE: Move upstream_funding location
- Consider: Keep old fields for 1 cycle with deprecation warning?

## Performance Impact

**NONE** - Performance tests show:
- Top-level fields: 158ms
- Nested fields: 151ms (4% FASTER!)
- Collection size: 538 documents (tiny)
- Indexes: Don't matter at this scale

## Next Steps

1. Discuss/approve this proposal
2. Update aggregation/candidate_financials.py
3. Rematerialize assets
4. Update any queries/UI that rely on old structure
5. Document new structure in MONGODB_STRUCTURE.md
