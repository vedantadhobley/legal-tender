"""Asset materialization jobs for the legal-tender pipeline.

Asset Dependency Graph:
=======================

  data_sync
      │
      ├── cn (candidates)
      ├── cm (committees)  
      ├── ccl (candidate-committee linkages)
      ├── pas2 (PAC transactions)
      ├── oth (other receipts)
      └── indiv (individual contributions)
              │
         ┌────┴────┐
      donors    contributed_to ←── cm
         │           │
    employers   transferred_to ←── pas2, oth
         │           │
    employed_by  affiliated_with ←── ccl
         └─────┬─────┘
               │
      political_money_graph

Parallelization:
- FEC raw files (cn, cm, ccl, pas2, oth, indiv) run in PARALLEL after data_sync
- Graph layer builds sequentially based on dependencies
- employers + contributed_to can run in parallel after donors
- transferred_to + affiliated_with can run in parallel
- employed_by depends only on donors + employers
"""

from dagster import define_asset_job, AssetSelection

# ============================================================================
# MAIN PIPELINE JOB - Full data refresh
# ============================================================================

fec_pipeline_job = define_asset_job(
    name="fec_pipeline_job",
    description="Complete FEC pipeline: Download → Parse raw data → Build graph vertices/edges",
    selection=AssetSelection.all(),  # Run everything in dependency order
    tags={
        "team": "data-engineering",
        "pipeline": "fec-complete",
        "priority": "high",
        "schedule": "weekly-sunday",
    },
)

# ============================================================================
# GRAPH-ONLY JOB - Rebuild graph from existing raw data
# ============================================================================

graph_rebuild_job = define_asset_job(
    name="graph_rebuild_job",
    description="Rebuild graph layer only (assumes raw FEC data already loaded)",
    selection=AssetSelection.keys(
        "donors",
        "employers",
        "contributed_to", 
        "transferred_to",
        "affiliated_with",
        "employed_by",
        "political_money_graph",
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "graph-only",
        "priority": "medium",
    },
)

# ============================================================================
# RAW DATA JOB - Just download and parse FEC files
# ============================================================================

raw_data_job = define_asset_job(
    name="raw_data_job", 
    description="Download and parse raw FEC files only (no graph)",
    selection=AssetSelection.keys(
        "data_sync",
        "cn",
        "cm",
        "ccl",
        "pas2",
        "oth",
        "indiv",
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "raw-only",
        "priority": "medium",
    },
)

# ============================================================================
# ENRICHMENT JOB - Committee classification and financials
# ============================================================================

enrichment_job = define_asset_job(
    name="enrichment_job",
    description="""
    Enrich graph data with:
    - Committee classification (terminal types)
    - Donor classification (whale tiers)
    - Committee receipt totals from raw FEC (fixes small donor gap)
    - Embedding-based employer clustering
    - Wikidata corporate resolution
    
    Run after graph_rebuild_job when graph data is fresh.
    """,
    selection=AssetSelection.keys(
        "committee_classification",
        "donor_classification",
        "committee_receipts",
        "committee_financials",
        "employer_clusters",
        "canonical_employers",
        "wikidata_corporate_resolution",
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "enrichment",
        "priority": "medium",
    },
)

# ============================================================================
# AGGREGATION JOB - Pre-computed summaries for UI/RAG
# ============================================================================

aggregation_job = define_asset_job(
    name="aggregation_job",
    description="""
    Compute pre-aggregated summaries:
    - Candidate upstream funding ("pie chart" - where money comes from)
    - Candidate summaries (top donors, funding breakdown)
    - Committee summaries
    - Donor summaries
    
    Run after enrichment_job for accurate data.
    """,
    selection=AssetSelection.keys(
        "candidate_upstream",
        "candidate_summaries",
        "committee_summaries",
        "donor_summaries",
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "aggregation",
        "priority": "medium",
    },
)

# ============================================================================
# UPSTREAM JOB - Just rebuild upstream funding (fast refresh)
# ============================================================================

upstream_job = define_asset_job(
    name="upstream_job",
    description="Rebuild candidate upstream funding only (requires committee_receipts)",
    selection=AssetSelection.keys(
        "committee_receipts",
        "candidate_upstream",
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "upstream-only",
        "priority": "high",
    },
)

# ============================================================================
# EMPLOYER UNIFICATION JOB - Just employer clustering
# ============================================================================

employer_unification_job = define_asset_job(
    name="employer_unification_job",
    description="Unify employer names using embeddings and Wikidata (no hardcoding)",
    selection=AssetSelection.keys(
        "employer_clusters",
        "canonical_employers",
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "employer-unification",
        "priority": "medium",
    },
)
