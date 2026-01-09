# ArangoDB Graph Architecture for Political Money Flow

## Why Graph Database?

### The MongoDB Problem
With MongoDB, tracing money flow required **multiple sequential queries**:

```
Query 1: Find donations to Committee A
Query 2: Find where Committee A transferred money
Query 3: Find where those committees transferred money
Query 4: ...and so on
```

Each "hop" was a separate database query. To trace money 5 levels deep = 5 queries minimum. This was:
- **Slow** - N queries for N hops
- **Complex** - Had to manage intermediate results in application code
- **Limited** - Practical limit of ~3-4 hops before performance degraded

### The Graph Solution
ArangoDB's graph engine traverses relationships **in a single query**:

```aql
FOR v, e, p IN 1..UNLIMITED OUTBOUND "donors/MUSK_ELON" 
  GRAPH "political_money_flow"
  RETURN p
```

This traces ALL paths from a donor through unlimited hops in **one query**. The database handles the traversal internally, which is what it's optimized for.

---

## Graph Structure

### Vertex Collections (Nodes = Entities)

| Collection | Source | Description | Key Field |
|------------|--------|-------------|-----------|
| `candidates` | cn.txt | Federal candidates | CAND_ID |
| `committees` | cm.txt | PACs, Super PACs, Campaign committees | CMTE_ID |
| `legislators` | congress API | Current/past members of Congress | bioguide_id |
| `donors` | indiv.txt (aggregated) | Individual donors (normalized) | NAME_EMPLOYER hash |
| `employers` | indiv.txt (extracted) | Unique employers | normalized name |

### Edge Collections (Relationships = Money Flow)

| Collection | From → To | Source | Description |
|------------|-----------|--------|-------------|
| `contributed_to` | donors → committees | indiv.txt | Individual donations |
| `transferred_to` | committees → committees | pas2.txt, oth.txt | PAC-to-PAC transfers |
| `affiliated_with` | committees ↔ candidates | ccl.txt | Committee-candidate linkages |
| `represents` | legislators → candidates | member_fec_mapping | Legislator's candidate records |
| `employed_by` | donors → employers | indiv.txt | Employment relationships |

### Named Graph Definition

```javascript
// Created in ArangoDB
db._createGraph("political_money_flow", [
  // Individual donations flow into committees
  { collection: "contributed_to", from: ["donors"], to: ["committees"] },
  
  // Committee-to-committee transfers (the dark money path)
  { collection: "transferred_to", from: ["committees"], to: ["committees"] },
  
  // Committees support candidates
  { collection: "affiliated_with", from: ["committees"], to: ["candidates"] },
  
  // Legislators have candidate records
  { collection: "represents", from: ["legislators"], to: ["candidates"] },
  
  // Donors work for employers (corporate influence)
  { collection: "employed_by", from: ["donors"], to: ["employers"] }
]);
```

---

## Data Flow Pipeline

### Phase 1: Raw Data (FEC Collections)
Load raw FEC files into cycle-specific databases:

```
fec_2024/
  ├── cn        # Raw candidate records
  ├── cm        # Raw committee records  
  ├── ccl       # Raw committee-candidate links
  ├── indiv     # Raw individual contributions (60M+ records)
  ├── pas2      # Raw PAC contributions
  └── oth       # Raw other receipts
```

### Phase 2: Graph Vertices (Normalized Entities)
Create deduplicated, normalized vertex collections in `aggregation` database:

```
aggregation/
  ├── candidates   # Merged from all cycles, deduplicated by CAND_ID
  ├── committees   # Merged from all cycles, deduplicated by CMTE_ID
  ├── legislators  # From Congress API + FEC mapping
  ├── donors       # Aggregated from indiv (NAME+EMPLOYER normalized)
  └── employers    # Extracted unique employers
```

**Donor Normalization Example:**
```
Raw indiv records:
  - "MUSK, ELON", "TESLA INC", $5000 → Committee A
  - "MUSK, ELON", "TESLA", $2500 → Committee B  
  - "MUSK ELON", "TESLA INC.", $10000 → Committee C

Normalized to single vertex:
  donors/MUSK_ELON_TESLA: {
    names: ["MUSK, ELON", "MUSK ELON"],
    employers: ["TESLA INC", "TESLA", "TESLA INC."],
    canonical_name: "MUSK, ELON",
    canonical_employer: "TESLA INC"
  }
```

### Phase 3: Graph Edges (Relationships)
Create edge collections linking vertices:

```
aggregation/
  ├── contributed_to    # donor → committee edges with amounts
  ├── transferred_to    # committee → committee edges  
  ├── affiliated_with   # committee ↔ candidate edges
  ├── represents        # legislator → candidate edges
  └── employed_by       # donor → employer edges
```

**Edge Document Structure:**
```json
// contributed_to edge
{
  "_from": "donors/MUSK_ELON_TESLA",
  "_to": "committees/C00873398",
  "total_amount": 17500,
  "transaction_count": 3,
  "cycles": ["2024"],
  "dates": ["2024-03-15", "2024-06-20", "2024-09-01"]
}
```

---

## Query Examples

### 1. Trace ALL money from a donor to candidates
```aql
FOR v, e, p IN 1..10 OUTBOUND "donors/MUSK_ELON_TESLA"
  GRAPH "political_money_flow"
  FILTER IS_SAME_COLLECTION("candidates", v)
  RETURN {
    path: p.vertices[*].name,
    amounts: p.edges[*].total_amount,
    final_candidate: v.CAND_NAME,
    hops: LENGTH(p.edges)
  }
```

### 2. Find dark money paths (committee chains > 2 hops)
```aql
FOR v, e, p IN 3..10 OUTBOUND "committees/C00873398"
  GRAPH "political_money_flow"
  FILTER IS_SAME_COLLECTION("candidates", v)
  RETURN {
    origin_committee: FIRST(p.vertices).CMTE_NM,
    intermediaries: p.vertices[1..-1][*].CMTE_NM,
    final_candidate: v.CAND_NAME,
    hop_count: LENGTH(p.edges)
  }
```

### 3. Corporate influence - all donations from employer
```aql
FOR donor IN 1..1 INBOUND "employers/TESLA_INC" employed_by
  FOR v, e, p IN 1..5 OUTBOUND donor
    GRAPH "political_money_flow"
    FILTER IS_SAME_COLLECTION("candidates", v)
    COLLECT candidate = v.CAND_NAME INTO donations
    RETURN {
      candidate: candidate,
      total_from_tesla_employees: SUM(donations[*].e.total_amount),
      donor_count: LENGTH(donations)
    }
```

### 4. Legislator's full funding network
```aql
LET legislator = DOCUMENT("legislators/B001230")  // Sen. Baldwin
FOR cand IN 1..1 OUTBOUND legislator represents
  FOR v, e, p IN 1..5 INBOUND cand
    GRAPH "political_money_flow"
    RETURN DISTINCT {
      source_type: PARSE_IDENTIFIER(FIRST(p.vertices)).collection,
      source_name: FIRST(p.vertices).name || FIRST(p.vertices).CMTE_NM,
      path_length: LENGTH(p.edges),
      amount: e.total_amount
    }
```

---

## Why This Structure?

### Separation of Concerns
1. **Raw FEC data** stays in cycle-specific DBs (`fec_2024/indiv`)
   - Preserves original data
   - Enables re-processing if normalization logic changes
   - Dumps allow fast reload without re-parsing

2. **Normalized graph** lives in `aggregation` DB
   - Deduplicated entities
   - Pre-computed edges with aggregated amounts
   - Optimized for traversal queries

### Performance Characteristics
- **Vertex lookups**: O(1) by `_key`
- **Edge traversal**: O(edges) - handled by ArangoDB's graph engine
- **Path queries**: Single query regardless of depth
- **Aggregations**: Pre-computed in edges, not calculated at query time

### vs. MongoDB Approach

| Operation | MongoDB | ArangoDB Graph |
|-----------|---------|----------------|
| Find donor's direct donations | 1 query | 1 query |
| Trace 3 hops | 3+ queries | 1 query |
| Trace unlimited hops | Impractical | 1 query |
| Find all paths A→B | Complex app logic | Built-in |
| Shortest path | Manual BFS | `SHORTEST_PATH` |

---

## Implementation Order

1. ✅ **Raw FEC data** - cn, cm, ccl, pas2, oth, indiv loaded
2. ⏳ **Vertex normalization** - Create deduplicated candidates, committees, donors
3. ⏳ **Edge creation** - Build contributed_to, transferred_to, affiliated_with
4. ⏳ **Graph definition** - Create named graph in ArangoDB
5. ⏳ **Query layer** - API endpoints for traversal queries

---

## Database Structure Summary

```
ArangoDB Instance
├── _system                    # ArangoDB system DB
├── fec_2020/                  # Raw FEC data - 2020 cycle
│   ├── cn, cm, ccl
│   ├── indiv (69M records)
│   ├── pas2, oth
│   └── [dumps in ~/.legal-tender/arango/fec/2020/]
├── fec_2022/                  # Raw FEC data - 2022 cycle
├── fec_2024/                  # Raw FEC data - 2024 cycle
├── fec_2026/                  # Raw FEC data - 2026 cycle
└── aggregation/               # Normalized graph data
    ├── candidates (vertex)
    ├── committees (vertex)
    ├── legislators (vertex)
    ├── donors (vertex)
    ├── employers (vertex)
    ├── contributed_to (edge)
    ├── transferred_to (edge)
    ├── affiliated_with (edge)
    ├── represents (edge)
    ├── employed_by (edge)
    └── political_money_flow (named graph)
```
