# Legal Tender: Political Money Flow Analysis

**Follow the money. Map the influence. Expose the connections.**

Legal Tender builds a graph database of political money flows, enabling queries like:

> "Show me where Elon Musk's donations went, through how many PACs, and to which candidates."

---

## Quick Start

```bash
# Start services
docker compose -f docker-compose.dev.yml up -d

# Access UIs
# Dagster Pipeline: http://localhost:4200
# ArangoDB (graph DB): http://localhost:4201 (root/ltpass)
```

**First Run**: Materialize assets in Dagster UI (takes ~30-45 minutes for full pipeline)

**Subsequent Runs**: Graph rebuilds in ~5 minutes from cached raw data

---

## What's In The Database?

| Collection | Type | Count | Description |
|------------|------|-------|-------------|
| **donors** | Vertex | 287,744 | Individuals who gave $10K+ |
| **employers** | Vertex | 82,046 | Companies with donating employees |
| **committees** | Vertex | 30,840 | PACs, Super PACs, campaigns |
| **candidates** | Vertex | 15,635 | Federal election candidates |
| **contributed_to** | Edge | 3,340,147 | Donor → Committee donations |
| **transferred_to** | Edge | 654,530 | Committee → Committee transfers |
| **affiliated_with** | Edge | 22,808 | Committee → Candidate links |
| **employed_by** | Edge | 147,810 | Donor → Employer relationships |

**Raw Data**: ~191 million individual contribution records across 2020-2024 cycles.

---

## Example Queries

### Trace money from a donor to candidates

```aql
FOR v, e, p IN 1..5 OUTBOUND "donors/MUSK_ELON_TESLA"
  GRAPH "political_money_flow"
  FILTER IS_SAME_COLLECTION("candidates", v)
  RETURN {
    candidate: v.CAND_NAME,
    hops: LENGTH(p.edges),
    path: p.vertices[*].CMTE_NM
  }
```

### Find who funds a politician

```aql
FOR v, e, p IN 1..5 INBOUND "candidates/P00009423"  -- Trump
  GRAPH "political_money_flow"
  LET source = FIRST(p.vertices)
  COLLECT type = PARSE_IDENTIFIER(source._id).collection
  AGGREGATE total = SUM(e.total_amount)
  RETURN { type, total }
```

### Corporate influence analysis

```aql
FOR donor IN 1..1 INBOUND "employers/GOOGLE_LLC" employed_by
  FOR v, e, p IN 1..5 OUTBOUND donor GRAPH "political_money_flow"
    FILTER IS_SAME_COLLECTION("candidates", v)
    COLLECT candidate = v.CAND_NAME INTO donations
    RETURN { candidate, total: SUM(donations[*].e.total_amount) }
```

---

## Architecture

```
FEC Website (fec.gov)
       │
       ▼ Download
┌─────────────────────────────────────────┐
│     Raw FEC Files (ZIP)                 │
│     cn, cm, ccl, indiv, pas2, oth       │
└─────────────────────────────────────────┘
       │
       ▼ Parse (Dagster assets)
┌─────────────────────────────────────────┐
│     ArangoDB: fec_2020, fec_2022, ...   │
│     ~191M records (raw data)            │
└─────────────────────────────────────────┘
       │
       ▼ Build Graph
┌─────────────────────────────────────────┐
│     ArangoDB: aggregation               │
│     Vertices + Edges + Named Graph      │
│     "political_money_flow"              │
└─────────────────────────────────────────┘
       │
       ▼ Query (AQL traversals)
┌─────────────────────────────────────────┐
│     Your Application                     │
│     "Click politician → See funders"    │
└─────────────────────────────────────────┘
```

---

## Documentation

📖 **[Full Architecture Guide](docs/ARCHITECTURE.md)** - Comprehensive documentation including:
- Graph database concepts (for SQL/MongoDB users)
- Complete schema with examples
- Query patterns for UI integration
- Development workflow

📖 **[FEC Data Reference](docs/FEC.md)** - FEC file formats and field definitions

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Graph Database | ArangoDB 3.11 |
| Pipeline Orchestration | Dagster |
| Runtime | Python 3.11 |
| Containers | Docker Compose |

---

## Project Structure

```
src/
├── assets/
│   ├── sync/           # data_sync - downloads FEC files
│   ├── fec/            # cn, cm, ccl, indiv, pas2, oth parsers
│   ├── graph/          # Vertex & edge builders
│   └── mapping/        # Congress member → FEC ID mapping
├── models/             # Pydantic models for graph entities
├── resources/          # ArangoDB connection
└── utils/              # Helpers (memory, schema, storage)
```

---

## Why Graph Database?

**SQL/MongoDB**: Tracing 5 hops of money requires 5+ queries with manual joins.

**Graph DB**: One query traverses all paths regardless of depth:

```aql
FOR v, e, p IN 1..UNLIMITED OUTBOUND "donors/X" GRAPH "political_money_flow"
  RETURN p
```

The database handles traversal internally - that's what it's optimized for.

---

## Data Sources

- **FEC Bulk Data**: Campaign finance records (fec.gov)
- **Congress API**: Legislator information
- **Election cycles**: 2020, 2022, 2024

---

## License

MIT
