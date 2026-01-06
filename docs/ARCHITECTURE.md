# Legal Tender Architecture Guide

> **For developers new to graph databases**: This guide explains everything from the ground up, including how graph databases differ from SQL/MongoDB and why we chose this approach.

---

## Table of Contents

1. [What is Legal Tender?](#what-is-legal-tender)
2. [Why a Graph Database?](#why-a-graph-database)
3. [Graph Database 101](#graph-database-101)
4. [System Architecture](#system-architecture)
5. [Data Pipeline](#data-pipeline)
6. [The Graph Schema](#the-graph-schema)
7. [Query Patterns](#query-patterns)
8. [UI Integration Guide](#ui-integration-guide)
9. [Development Workflow](#development-workflow)

---

## What is Legal Tender?

Legal Tender tracks **how money flows through American politics**. The goal is simple:

> "Show me where Elon Musk's money went, through how many PACs, and to which candidates."

This requires tracing money through multiple intermediaries:

```
DONOR → PAC #1 → PAC #2 → PAC #3 → CANDIDATE
```

The FEC (Federal Election Commission) publishes this data, but it's spread across dozens of files with millions of records. Legal Tender:

1. **Downloads** FEC bulk data files
2. **Parses** them into a database
3. **Builds a graph** of money relationships
4. **Enables traversal queries** to trace money flows

---

## Why a Graph Database?

### The Problem with Traditional Databases

Imagine tracing donations in **SQL**:

```sql
-- Find committees donor gave to
SELECT * FROM donations WHERE donor_id = 'MUSK_ELON';

-- For EACH committee, find where they transferred money
SELECT * FROM transfers WHERE from_committee = 'C001';
SELECT * FROM transfers WHERE from_committee = 'C002';
-- ... repeat for each committee

-- For EACH of those, find more transfers
-- ... this gets exponentially worse
```

With **MongoDB**, it's similar - you need multiple queries and manual joining in application code.

### The Graph Solution

In a **graph database**, relationships are first-class citizens:

```aql
-- ONE query traces ALL paths from donor to candidates
FOR v, e, p IN 1..10 OUTBOUND "donors/MUSK_ELON" 
  GRAPH "political_money_flow"
  FILTER IS_SAME_COLLECTION("candidates", v)
  RETURN p
```

This single query:
- Starts at the donor
- Follows ALL outgoing edges (donations, transfers)
- Goes up to 10 hops deep
- Returns only paths that end at candidates

**The database handles the traversal internally** - that's what it's optimized for.

### Performance Comparison

| Operation | SQL/MongoDB | Graph DB |
|-----------|-------------|----------|
| Direct donations | 1 query | 1 query |
| 2-hop trace | 2+ queries | 1 query |
| 5-hop trace | 5+ queries + joins | 1 query |
| All paths (unlimited) | Impractical | 1 query |
| Shortest path | Complex algorithm | Built-in |

---

## Graph Database 101

If you've only used SQL or MongoDB, here's the mental model shift:

### SQL Thinking vs Graph Thinking

**SQL**: Data is in TABLES with ROWS. Relationships are via FOREIGN KEYS that you JOIN.

```
┌─────────────────────────────────────────────────────────────┐
│ donors table          │ donations table     │ committees    │
├───────────────────────┼─────────────────────┼───────────────┤
│ id: 1                 │ donor_id: 1   ────► │ id: 100       │
│ name: "Musk"          │ committee_id: 100   │ name: "PAC A" │
└───────────────────────┴─────────────────────┴───────────────┘
```

**Graph**: Data is VERTICES (nodes) and EDGES (connections). Relationships are stored directly.

```
    ┌────────────┐     contributed_to      ┌────────────┐
    │   DONOR    │ ───────────────────────►│ COMMITTEE  │
    │  "Musk"    │     $50,000             │  "PAC A"   │
    └────────────┘                         └────────────┘
         │                                       │
         │ employed_by                           │ transferred_to
         ▼                                       ▼
    ┌────────────┐                         ┌────────────┐
    │  EMPLOYER  │                         │ COMMITTEE  │
    │  "Tesla"   │                         │  "PAC B"   │
    └────────────┘                         └────────────┘
```

### Key Concepts

| Term | Definition | Example |
|------|------------|---------|
| **Vertex** | An entity (node) | A donor, a committee, a candidate |
| **Edge** | A relationship between vertices | "donated to", "transferred to" |
| **Collection** | A group of similar vertices or edges | All donors, all donations |
| **Graph** | A named group of vertex + edge collections | "political_money_flow" |
| **Traversal** | Following edges from vertex to vertex | Tracing money path |

### ArangoDB Specifics

We use **ArangoDB**, which has some unique features:

1. **Document + Graph**: Every vertex/edge is a JSON document
2. **AQL**: SQL-like query language with graph extensions
3. **Named Graphs**: Define which edges connect which vertices
4. **_key, _id, _from, _to**: Special fields for identification

```javascript
// A vertex document
{
  "_key": "a1b2c3d4",           // Unique within collection
  "_id": "donors/a1b2c3d4",     // Globally unique (collection/key)
  "name": "MUSK, ELON",
  "total_amount": 55000
}

// An edge document
{
  "_key": "edge123",
  "_from": "donors/a1b2c3d4",   // Source vertex
  "_to": "committees/C00873398", // Target vertex
  "amount": 55000
}
```

---

## System Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           FEC Website                                │
│                   (fec.gov/data/bulk-data)                          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Download (data_sync asset)
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Raw FEC Files                                │
│   ~/.legal-tender/data/fec/{cycle}/                                 │
│   cn.zip, cm.zip, ccl.zip, indiv.zip, pas2.zip, oth.zip            │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Parse (cn, cm, ccl, indiv, pas2, oth assets)
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ArangoDB - Raw Collections                        │
│   fec_2020/, fec_2022/, fec_2024/                                   │
│   Each contains: cn, cm, ccl, indiv, pas2, oth                      │
│   (~191M individual contribution records)                           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Build Graph (donors, committees, edges)
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ArangoDB - Graph Layer                            │
│   aggregation/                                                       │
│   ├── donors (287K vertices)      ├── contributed_to (3.3M edges)   │
│   ├── employers (82K vertices)    ├── transferred_to (654K edges)   │
│   ├── committees (30K vertices)   ├── affiliated_with (22K edges)   │
│   ├── candidates (15K vertices)   └── employed_by (147K edges)      │
│   └── political_money_flow (named graph)                            │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Query (AQL traversals)
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Your Application                             │
│   "Click on a politician → See who's funding them"                  │
└─────────────────────────────────────────────────────────────────────┘
```

### Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Database | ArangoDB 3.11 | Graph + document storage |
| Orchestration | Dagster | Pipeline scheduling and monitoring |
| Runtime | Python 3.11 | Asset implementation |
| Container | Docker Compose | Local development |

### Databases in ArangoDB

```
ArangoDB Instance (port 4201)
│
├── _system              # ArangoDB internal
│
├── fec_2020/            # Raw 2020 cycle data
│   ├── cn               # Candidates (24K records)
│   ├── cm               # Committees (28K records)
│   ├── ccl              # Committee-Candidate Links
│   ├── indiv            # Individual contributions (69M records)
│   ├── pas2             # PAC transactions
│   └── oth              # Other receipts
│
├── fec_2022/            # Raw 2022 cycle data
│   └── (same collections)
│
├── fec_2024/            # Raw 2024 cycle data  
│   └── (same collections)
│
└── aggregation/         # GRAPH LAYER
    ├── donors           # Vertex: 287K donors ($10K+ threshold)
    ├── employers        # Vertex: 82K unique employers
    ├── committees       # Vertex: 30K PACs/Super PACs
    ├── candidates       # Vertex: 15K federal candidates
    ├── contributed_to   # Edge: donor → committee
    ├── transferred_to   # Edge: committee → committee
    ├── affiliated_with  # Edge: committee → candidate
    ├── employed_by      # Edge: donor → employer
    └── political_money_flow  # Named graph definition
```

---

## Data Pipeline

### Dagster Asset Graph

```
DATA SYNC (downloads FEC files)
     │
     ├──────────┬──────────┬──────────┬──────────┬──────────┐
     ▼          ▼          ▼          ▼          ▼          ▼
    cn         cm        ccl       indiv      pas2       oth
 (candidates) (committees) (links) (donations) (PAC $) (other)
     │          │          │          │          │          │
     │          │          │          └──────────┴──────────┘
     │          │          │                     │
     │          │          │          ┌──────────┘
     │          │          │          ▼
     │          │          │       donors ──────► employed_by ──► employers
     │          │          │          │
     │          │          │          ▼
     │          └──────────┼───► contributed_to
     │                     │          │
     │                     │          ▼
     │                     ├───► affiliated_with
     │                     │          │
     └─────────────────────┼──────────┘
                           │          │
                           │          ▼
                           └───► transferred_to
                                      │
                                      ▼
                           political_money_graph
```

### Asset Descriptions

| Asset | Input | Output | Records |
|-------|-------|--------|---------|
| `data_sync` | FEC website | ZIP files on disk | ~20 files |
| `cn` | cn.zip | fec_{cycle}.cn | ~24K/cycle |
| `cm` | cm.zip | fec_{cycle}.cm | ~28K/cycle |
| `ccl` | ccl.zip | fec_{cycle}.ccl | ~18K/cycle |
| `indiv` | indiv.zip | fec_{cycle}.indiv | ~60M/cycle |
| `pas2` | pas2.zip | fec_{cycle}.pas2 | ~900K/cycle |
| `oth` | oth.zip | fec_{cycle}.oth | ~400K/cycle |
| `donors` | indiv collections | aggregation.donors | 287K |
| `employers` | donors | aggregation.employers | 82K |
| `contributed_to` | donors, indiv, cm, cn | aggregation.contributed_to | 3.3M |
| `transferred_to` | pas2, oth, cm | aggregation.transferred_to | 654K |
| `affiliated_with` | ccl, contributed_to | aggregation.affiliated_with | 22K |
| `employed_by` | donors, employers | aggregation.employed_by | 147K |
| `political_money_graph` | All vertices + edges | Named graph | 1 |

### Processing Notes

**$10K Threshold**: Only donors who gave $10K+ in a cycle are included. This:
- Focuses on major donors (influence)
- Reduces vertices from 60M → 287K
- Makes traversals fast

**Cycle Aggregation**: Donations are aggregated per (donor, committee, cycle) to reduce edge count while preserving totals.

---

## The Graph Schema

### Vertex Collections

#### donors (287,744 vertices)

```python
{
    "_key": "a1b2c3d4e5f6g7h8",      # SHA256(name|employer)[:16]
    "canonical_name": "MUSK, ELON",
    "canonical_employer": "TESLA INC",
    "total_amount": 55000.0,         # Sum across all cycles
    "transaction_count": 5,
    "cycles": ["2024"],
    "updated_at": "2026-01-05T00:00:00"
}
```

#### employers (82,046 vertices)

```python
{
    "_key": "b2c3d4e5f6g7h8i9",      # SHA256(employer)[:16]
    "name": "GOOGLE LLC",
    "donor_count": 1523,
    "total_amount": 45000000.0,
    "updated_at": "2026-01-05T00:00:00"
}
```

#### committees (30,840 vertices)

```python
{
    "_key": "C00873398",             # FEC Committee ID
    "CMTE_ID": "C00873398",
    "CMTE_NM": "AMERICA PAC",
    "CMTE_TP": "O",                  # O = Super PAC
    "CMTE_PTY_AFFILIATION": "REP",
    "total_receipts": 150000000.0,
    "total_disbursements": 148000000.0,
    "cycles": ["2024"]
}
```

#### candidates (15,635 vertices)

```python
{
    "_key": "P00009423",             # FEC Candidate ID
    "CAND_ID": "P00009423",
    "CAND_NAME": "TRUMP, DONALD J.",
    "CAND_PTY_AFFILIATION": "REP",
    "CAND_OFFICE": "P",              # P = President
    "CAND_OFFICE_ST": "US",
    "cycles": ["2024"]
}
```

### Edge Collections

#### contributed_to (3,340,147 edges)

```python
{
    "_key": "c1d2e3f4g5h6i7j8",
    "_from": "donors/a1b2c3d4e5f6g7h8",
    "_to": "committees/C00873398",
    "total_amount": 55000.0,
    "transaction_count": 5,
    "cycles": ["2024"],
    "first_date": "2024-01-15",
    "last_date": "2024-10-01"
}
```

#### transferred_to (654,530 edges)

```python
{
    "_key": "d2e3f4g5h6i7j8k9",
    "_from": "committees/C00873398",
    "_to": "committees/C00543211",
    "total_amount": 500000.0,
    "transaction_count": 3,
    "transaction_types": ["24G"],    # Transfer types
    "cycles": ["2024"]
}
```

#### affiliated_with (22,808 edges)

```python
{
    "_key": "e3f4g5h6i7j8k9l0",
    "_from": "committees/C00618389",
    "_to": "candidates/P00009423",
    "linkage_type": "P",             # P = Principal campaign
    "cycles": ["2024"]
}
```

#### employed_by (147,810 edges)

```python
{
    "_key": "f4g5h6i7j8k9l0m1",
    "_from": "donors/a1b2c3d4e5f6g7h8",
    "_to": "employers/b2c3d4e5f6g7h8i9",
    "donation_amount": 55000.0
}
```

### Named Graph Definition

```javascript
// Graph: political_money_flow
{
  "edgeDefinitions": [
    {
      "collection": "contributed_to",
      "from": ["donors"],
      "to": ["committees"]
    },
    {
      "collection": "transferred_to", 
      "from": ["committees"],
      "to": ["committees"]
    },
    {
      "collection": "affiliated_with",
      "from": ["committees"],
      "to": ["candidates"]
    },
    {
      "collection": "employed_by",
      "from": ["donors"],
      "to": ["employers"]
    }
  ]
}
```

---

## Query Patterns

### For Your UI: "Click on a politician, see who's funding them"

This is the most common query. Here's how to implement it:

#### Step 1: Find the candidate's ID

```aql
FOR c IN candidates
  FILTER CONTAINS(UPPER(c.CAND_NAME), "TRUMP")
  RETURN { id: c._id, name: c.CAND_NAME, party: c.CAND_PTY_AFFILIATION }
```

#### Step 2: Trace money INBOUND to candidate

```aql
// All funding sources (1-5 hops back)
FOR v, e, p IN 1..5 INBOUND "candidates/P00009423"
  GRAPH "political_money_flow"
  RETURN {
    source: FIRST(p.vertices),
    source_type: PARSE_IDENTIFIER(FIRST(p.vertices)._id).collection,
    path_length: LENGTH(p.edges),
    final_amount: LAST(p.edges).total_amount
  }
```

#### Step 3: Aggregate by source type

```aql
FOR v, e, p IN 1..5 INBOUND "candidates/P00009423"
  GRAPH "political_money_flow"
  LET source = FIRST(p.vertices)
  LET source_type = PARSE_IDENTIFIER(source._id).collection
  COLLECT type = source_type INTO sources
  RETURN {
    type: type,
    count: LENGTH(sources),
    total: SUM(sources[*].p.edges[-1].total_amount)
  }
```

### Common Query Patterns

#### 1. Top Donors to a Candidate

```aql
FOR v, e, p IN 1..5 INBOUND "candidates/P00009423"
  GRAPH "political_money_flow"
  FILTER PARSE_IDENTIFIER(FIRST(p.vertices)._id).collection == "donors"
  LET donor = FIRST(p.vertices)
  COLLECT donor_name = donor.canonical_name INTO donations
  LET total = SUM(donations[*].p.edges[-1].total_amount)
  SORT total DESC
  LIMIT 20
  RETURN { donor: donor_name, total: total }
```

#### 2. All Paths from Donor to Candidates

```aql
FOR v, e, p IN 1..10 OUTBOUND "donors/a1b2c3d4e5f6g7h8"
  GRAPH "political_money_flow"
  FILTER IS_SAME_COLLECTION("candidates", v)
  RETURN {
    candidate: v.CAND_NAME,
    hops: LENGTH(p.edges),
    path: p.vertices[*].CMTE_NM || p.vertices[*].canonical_name,
    amounts: p.edges[*].total_amount
  }
```

#### 3. Corporate Influence (Employer → Candidates)

```aql
// All donations from Google employees
FOR donor IN 1..1 INBOUND "employers/GOOGLE_LLC" employed_by
  FOR v, e, p IN 1..5 OUTBOUND donor
    GRAPH "political_money_flow"
    FILTER IS_SAME_COLLECTION("candidates", v)
    COLLECT candidate = v.CAND_NAME INTO donations
    LET total = SUM(donations[*].p.edges[0].total_amount)
    SORT total DESC
    LIMIT 20
    RETURN { candidate: candidate, total: total }
```

#### 4. Dark Money Paths (Long Committee Chains)

```aql
// Find paths with 3+ committee hops
FOR v, e, p IN 3..10 ANY "committees/C00873398"
  GRAPH "political_money_flow"
  FILTER IS_SAME_COLLECTION("candidates", v)
  RETURN {
    candidate: v.CAND_NAME,
    hops: LENGTH(p.edges),
    intermediaries: p.vertices[1..-1][*].CMTE_NM
  }
```

#### 5. Committee Funding Breakdown

```aql
// Where does a committee get its money?
FOR v, e IN 1..1 INBOUND "committees/C00873398"
  GRAPH "political_money_flow"
  LET source_type = PARSE_IDENTIFIER(v._id).collection
  COLLECT type = source_type 
  AGGREGATE total = SUM(e.total_amount), count = COUNT(1)
  RETURN { type: type, total: total, count: count }
```

---

## UI Integration Guide

### API Endpoint Design

For your frontend, you'll want these endpoints:

```
GET /api/candidates/:id/funding
  → All funding sources for a candidate (paginated)
  
GET /api/candidates/:id/funding/summary
  → Aggregated totals by source type
  
GET /api/donors/:id/paths
  → All paths from donor to candidates
  
GET /api/employers/:id/influence
  → Candidates funded by employer's employees
  
GET /api/committees/:id/flows
  → Money in and out of a committee
```

### Response Format Example

```json
// GET /api/candidates/P00009423/funding/summary
{
  "candidate": {
    "id": "P00009423",
    "name": "TRUMP, DONALD J.",
    "party": "REP",
    "office": "President"
  },
  "funding": {
    "direct_from_donors": {
      "total": 45000000,
      "donor_count": 12500
    },
    "via_committees": {
      "total": 350000000,
      "committee_count": 450
    },
    "via_super_pacs": {
      "total": 200000000,
      "pac_count": 25
    }
  },
  "top_donors": [
    { "name": "BLOOMBERG, MICHAEL", "total": 1200000000 },
    { "name": "MUSK, ELON", "total": 55000 }
  ],
  "top_committees": [
    { "name": "AMERICA PAC", "total": 150000000, "type": "Super PAC" }
  ]
}
```

### Graph Visualization

For showing money flow visually:

1. **Use the path data**: Each query returns `p.vertices` and `p.edges`
2. **Build a force-directed graph**: D3.js, vis.js, or Cytoscape.js
3. **Edge weights**: Use `total_amount` for line thickness
4. **Node colors**: By collection type (donors=green, committees=blue, candidates=red)

---

## Development Workflow

### Running Locally

```bash
# Start services
docker compose -f docker-compose.dev.yml up -d

# Access UIs
# Dagster: http://localhost:4200
# ArangoDB: http://localhost:4201 (root/ltpass)
```

### Running the Pipeline

```bash
# In Dagster UI, materialize assets in order:
# 1. data_sync (downloads files)
# 2. cn, cm, ccl, indiv, pas2, oth (parse to raw DBs)
# 3. donors, employers, contributed_to, transferred_to, affiliated_with, employed_by
# 4. political_money_graph
```

### Testing Queries

```bash
# Connect to ArangoDB
docker exec -it legal-tender-dev-arango arangosh \
  --server.username root --server.password ltpass

# Switch to aggregation database
db._useDatabase("aggregation");

# Run a test query
db._query(`
  FOR v, e, p IN 1..3 OUTBOUND "donors/a1b2c3d4e5f6g7h8"
    GRAPH "political_money_flow"
    RETURN p
`).toArray();
```

### Adding New Data Sources

1. Create a new asset in `src/assets/`
2. Define the FEC file parsing
3. Add vertex/edge creation logic
4. Update the graph definition if needed
5. Add to `src/assets/__init__.py`

---

## Glossary

| Term | Definition |
|------|------------|
| **FEC** | Federal Election Commission - regulates campaign finance |
| **PAC** | Political Action Committee - raises money for campaigns |
| **Super PAC** | Independent Expenditure Only Committee - unlimited donations |
| **Cycle** | 2-year election period (e.g., 2024 = 2023-2024) |
| **CMTE_ID** | FEC Committee ID (e.g., C00873398) |
| **CAND_ID** | FEC Candidate ID (e.g., P00009423) |
| **indiv.txt** | FEC file with individual contributions > $200 |
| **pas2.txt** | FEC file with PAC contributions |
| **ccl.txt** | FEC file with committee-candidate linkages |

---

## Quick Reference

### Database Access

- **Host**: localhost
- **Port**: 4201
- **Username**: root
- **Password**: ltpass
- **Graph DB**: aggregation
- **Graph Name**: political_money_flow

### Collection Counts

| Collection | Type | Count |
|------------|------|-------|
| donors | Vertex | 287,744 |
| employers | Vertex | 82,046 |
| committees | Vertex | 30,840 |
| candidates | Vertex | 15,635 |
| contributed_to | Edge | 3,340,147 |
| transferred_to | Edge | 654,530 |
| affiliated_with | Edge | 22,808 |
| employed_by | Edge | 147,810 |

### Key Files

| File | Purpose |
|------|---------|
| `src/assets/graph/*.py` | Graph vertex/edge builders |
| `src/assets/fec/*.py` | Raw FEC file parsers |
| `src/models/*.py` | Pydantic models for graph entities |
| `src/resources/arango.py` | ArangoDB connection |
| `docs/ARCHITECTURE.md` | This file |
