# Legal Tender Pipeline Status

**Last Updated**: January 9, 2026

This document tracks the current implementation status of the Legal Tender data pipeline.

---

## Executive Summary

Legal Tender tracks political money flow from original sources (individuals, corporations, unions) 
through intermediary committees to final recipients (candidates). The system uses **ArangoDB** 
as a graph database to enable efficient upstream tracing.

**Key Achievement**: Recursive upstream tracing now correctly attributes money to its ORIGINAL 
source, not just the immediate transfer source. This is critical for understanding true funding patterns.

---

## Database Architecture

### ArangoDB Structure (Port 4201)

```
aggregation (Main database)
├── Vertex Collections:
│   ├── candidates      → 4,926 candidates with funding_summary
│   ├── committees      → 30,840 committees with terminal_type classification
│   ├── donors          → 287,000+ individual donors with whale_tier
│   └── employers       → Employer entities
│
├── Edge Collections:
│   ├── contributed_to  → Individual → Committee donations (3.3M edges)
│   ├── transferred_to  → Committee → Committee transfers (654K edges)
│   ├── affiliated_with → Committee → Candidate affiliation
│   └── employed_by     → Donor → Employer relationships
│
└── Graph:
    └── political_money_graph → Named graph combining all vertices/edges

fec_YYYY (Per-cycle raw data: fec_2020, fec_2022, fec_2024, fec_2026)
├── cn   → Candidate master (~7K per cycle)
├── cm   → Committee master (~18K per cycle)
├── ccl  → Candidate-committee linkages
├── pas2 → Committee transfers to candidates
├── oth  → Committee-to-committee transfers (PAC/COM/PTY/ORG only)
└── indiv → Individual contributions (~69M per cycle)
```

---

## Asset Groups & Status

### 1. Sync (`group_name="sync"`)

| Asset | Status | Description |
|-------|--------|-------------|
| `data_sync` | ✅ Working | Downloads all FEC bulk files for configured cycles |

### 2. FEC Raw Data (`group_name="fec"`)

| Asset | Status | Description |
|-------|--------|-------------|
| `cn` | ✅ Working | Candidate master data |
| `cm` | ✅ Working | Committee master data |
| `ccl` | ✅ Working | Candidate-committee linkages |
| `pas2` | ✅ Working | Committee→Candidate transfers |
| `oth` | ✅ Working | Committee→Committee transfers (filtered at parse time) |
| `indiv` | ✅ Working | Individual contributions (69M records per cycle) |

### 3. Graph (`group_name="graph"`)

| Asset | Status | Description |
|-------|--------|-------------|
| `donors` | ✅ Working | Aggregates individuals across cycles, canonical names |
| `employers` | ✅ Working | Employer entities from donation data |
| `contributed_to` | ✅ Working | Individual→Committee contribution edges |
| `transferred_to` | ✅ Working | Committee→Committee transfer edges |
| `affiliated_with` | ✅ Working | Committee→Candidate affiliation edges |
| `employed_by` | ✅ Working | Donor→Employer relationship edges |
| `political_money_graph` | ✅ Working | Named graph combining all collections |

### 4. Enrichment (`group_name="enrichment"`)

| Asset | Status | Description |
|-------|--------|-------------|
| `committee_classification` | ✅ Working | Adds `terminal_type` to committees |
| `donor_classification` | ✅ Working | Adds `whale_tier` to donors |
| `committee_financials` | ✅ Working | Computes `total_receipts`, `total_disbursed` |

### 5. Aggregation (`group_name="aggregation"`)

| Asset | Status | Description |
|-------|--------|-------------|
| `candidate_summaries` | ✅ Working | **RECURSIVE UPSTREAM TRACING** - true source attribution |
| `committee_summaries` | ⚠️ Basic | Direct 1-hop summaries only (needs recursive upgrade) |
| `donor_summaries` | ⚠️ Basic | Direct 1-hop summaries only (needs recursive upgrade) |

---

## Recursive Upstream Tracing

### The Problem (Fixed in January 2026)

Money often flows through multiple intermediaries before reaching candidates:

```
Individual → ActBlue → Bernie 2020 → Friends of Bernie Sanders
```

The OLD (broken) logic only counted DIRECT sources:
- Direct individual donations → "individual" ✓
- Committee transfers → "committee" ✗ (WRONG!)

This caused Bernie Sanders to show **18% individual funding** when the TRUE number is **~60%**.

### The Solution: Recursive Source Ratio Computation

The `candidate_summaries` asset now uses memoized recursive tracing:

```python
def compute_source_ratio(cmte_id, visited, depth=0):
    """Compute (individual_ratio, org_ratio) for a committee."""
    
    # Terminal org? 100% organizational
    if terminal_type in TERMINAL_ORG_TYPES:
        return (0.0, 1.0)
    
    # Sum direct individual contributions
    ind_direct = sum(donations from individuals)
    
    # For committee transfers, RECURSIVELY trace upstream
    for from_cmte in incoming_transfers:
        if from_cmte is terminal org:
            org_from_cmtes += amount
        else:
            # RECURSIVE: trace through this committee
            ind_ratio, org_ratio = compute_source_ratio(from_cmte)
            ind_from_cmtes += amount * ind_ratio
            org_from_cmtes += amount * org_ratio
    
    return (ind_total / total, org_total / total)
```

### Terminal Types (Stop Tracing)

These organization types are the END of the money trail - attribute to "organization":
- `corporation` - Corporate PACs (Boeing PAC, AT&T PAC)
- `labor_union` - Union PACs (UFCW, IBEW, Air Line Pilots)
- `trade_association` - Industry groups (RPAC, NAR PAC)
- `cooperative` - Co-op PACs
- `ideological` - Issue-based PACs with organizational structure

### Passthrough Types (Keep Tracing)

These committees are intermediaries - trace THROUGH them to original sources:
- `passthrough` - JFCs, conduits (ActBlue, WinRed), party committees
- `campaign` - Candidate committees (trace to their donors)
- `super_pac_unclassified` - Trace to their donors

### Validation Results

| Candidate | OLD (Broken) | NEW (Correct) | Notes |
|-----------|-------------|---------------|-------|
| Ted Cruz | 59% individual | 75.9% individual | Large direct donors |
| Bernie Sanders | 18% individual | 58.6% individual | ActBlue→Bernie 2020 traced |
| **Global Average** | ~50% | **68.2% individual** | More accurate |

---

## Data Quality

### Current Stats (January 2026)

- **Candidates with summaries**: 4,926
- **Committees classified**: 30,840
- **Individual donors tracked**: 287,000+
- **Contribution edges**: 3.3 million
- **Transfer edges**: 654,000
- **Total money traced**: $28.2 billion

### Whale Tier Classification

| Tier | Threshold | Description |
|------|-----------|-------------|
| `ultra` | $1M+ | Elite mega-donors |
| `mega` | $250K+ | Major bundlers and donors |
| `whale` | $100K+ | Significant donors |
| `notable` | $50K+ | Notable contributors |

---

## Known Limitations

### 1. Committee Summaries Need Upgrade

`committee_summaries.py` still uses direct 1-hop lookups. The `individual_pct` / `committee_pct` 
fields do NOT trace upstream through intermediaries like `candidate_summaries` does.

**Impact**: Committee profiles show immediate sources, not original sources.

**TODO**: Apply same recursive tracing algorithm to committee_summaries.

### 2. Donor Summaries Need Upgrade

`donor_summaries.py` tracks direct donations only, not downstream flow.

**Impact**: Donor profiles don't show ultimate candidate impact.

**TODO**: Add downstream tracing to show which candidates ultimately received the money.

### 3. Cycle Tracking Simplified

The current recursive algorithm traces across ALL cycles together. It doesn't maintain 
per-cycle source ratios.

**Impact**: Individual_pct is accurate for career totals but not cycle-specific.

**TODO**: Extend algorithm to maintain cycle-specific source tracking if needed.

---

## Metrics Planned

### Funding Distance (Implemented)

Tracks the average number of "hops" money takes from terminal source to candidate, weighted by amount:

```
funding_distance = Σ(amount_i × hops_i) / Σ(amount_i)
```

**Hop Counting**:
- Terminal source (individual/org) = 0 hops (this IS the source)
- Direct donation to committee = 1 hop from terminal
- Transfer from committee A to committee B = +1 hop
- Candidate receives from committee = +1 hop

**Examples**:
- Individual → Campaign Committee → Candidate = 2 hops
- Individual → ActBlue → Campaign → Candidate = 3 hops
- Corporation PAC → Party Committee → Victory Fund → Campaign → Candidate = 4 hops

**Interpretation**:
- **2.0 hops** = All money comes directly from donors to candidate's committee (most transparent)
- **3-4 hops** = Money flows through one intermediary (typical for ActBlue/WinRed)
- **5+ hops** = Money flows through multiple intermediaries (party committees, JFCs)

**Results**:
- Global average: 3.21 hops
- Most direct: 2.0 hops (100% individual, no intermediaries)
- Most intermediated: 11+ hops (complex party committee networks)

**Note**: Higher funding_distance often indicates money flowing through party infrastructure
(DSCC, NRSC, Victory Funds) which is normal for establishment candidates.

---

## Jobs

| Job | Description | Use Case |
|-----|-------------|----------|
| `fec_pipeline_job` | Complete pipeline: sync → parse → graph → enrich → aggregate | Full refresh |
| `graph_rebuild_job` | Graph only (assumes raw data exists) | Rebuild after schema change |
| `raw_data_job` | Download and parse only (no graph) | Data acquisition only |

---

## Next Steps

1. **Implement `funding_distance` metric** in candidate_summaries
2. **Upgrade committee_summaries** with recursive tracing
3. **Upgrade donor_summaries** with downstream tracing
4. **Add API endpoints** for frontend queries
5. **Build RAG integration** for natural language queries

---

## References

- [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Detailed technical documentation
- [FEC.md](FEC.md) - FEC file format specifications
- ArangoDB UI: http://localhost:4201 (user: root, pass: ltpass)
- Dagster UI: http://localhost:3000
