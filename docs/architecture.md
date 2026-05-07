# Architecture

> **Status**: stub. Phase 3 fills this in alongside any architecture-touching change.

The high-level system view: components, boundaries, data flow at a layer above the asset DAG. Should answer:

- What are the major subsystems?
- Which databases own what (fec_YYYY vs aggregation)?
- Where does external data enter? Where do consumers query?
- How does Khoj/joi fit in (Phase 2 onwards)?
- What runs where (this host vs joi vs laptop)?

Until Phase 3, the working substitutes are:
- @pipeline.md for the ETL layer detail
- The Mermaid diagram in `README.md` (until rewritten)
