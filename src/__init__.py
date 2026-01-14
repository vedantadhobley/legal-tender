"""Legal Tender - Congressional influence tracking and analysis.

This package contains the Dagster definitions for orchestrating data pipelines
that track political donations, congressional voting, and lobbying activity.

Architecture:
- Assets: 
  * sync/ → Data synchronization (downloads all FEC files)
  * fec/ → Raw FEC bulk data parsers (→ fec_YYYY databases) - ALL CONVERTED TO ARANGODB
  * mapping/ → ID mapping assets (→ aggregation database) - CONVERTED TO ARANGODB
  * graph/ → Graph vertices and edges (→ aggregation database) - NEW GRAPH LAYER
  * enrichment/ → Derived fields and classifications
- Jobs: One main pipeline job (fec_pipeline_job)
- Resources: ArangoDB for graph data storage
- Schedules: Weekly automated refresh

Data Flow:
  FEC.gov bulk files → fec_2024 (raw) → aggregation.donors (normalized) → aggregation.contributed_to (edges)
                    → fec_2026 (raw) → aggregation.employers (normalized) → political_money_flow (graph)
"""

from dagster import Definitions
from src.assets import (
    # Data sync
    data_sync_asset,
    
    # FEC parsers (raw data → fec_YYYY databases) - ALL Converted to ArangoDB
    cn_asset,
    cm_asset,
    ccl_asset,
    pas2_asset,
    oth_asset,
    indiv_asset,
    
    # Mapping assets (ID mapping → aggregation database) - Converted to ArangoDB
    member_fec_mapping_asset,
    
    # Graph assets (vertices + edges → aggregation database)
    donors_asset,
    employers_asset,
    contributed_to_asset,
    transferred_to_asset,
    affiliated_with_asset,
    employed_by_asset,
    political_money_graph_asset,
    
    # Enrichment assets (classifications + derived fields)
    committee_classification_asset,
    donor_classification_asset,
    committee_financials_asset,
    canonical_employers_asset,
    employer_clusters_asset,
    employer_cluster_integration_asset,
    corporate_hierarchy_asset,
    wikidata_corporate_resolution,
    
    # Aggregation assets (pre-computed summaries for UI/RAG)
    candidate_summaries_asset,
    committee_summaries_asset,
    donor_summaries_asset,
)
from src.jobs import fec_pipeline_job, graph_rebuild_job, raw_data_job, enrichment_job, employer_unification_job
from src.schedules import (
    weekly_pipeline_schedule,
)
from src.resources import arango_resource, EmbeddingResource

# ============================================================================
# DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        # Data sync (downloads all files)
        data_sync_asset,
        
        # FEC raw data parsers (→ fec_YYYY databases) - ALL Converted to ArangoDB
        cn_asset,
        cm_asset,
        ccl_asset,
        pas2_asset,
        oth_asset,
        indiv_asset,
        
        # Mapping assets (ID mapping → aggregation database) - Converted to ArangoDB
        member_fec_mapping_asset,
        
        # Graph assets (vertices + edges → aggregation database)
        donors_asset,
        employers_asset,
        contributed_to_asset,
        transferred_to_asset,
        affiliated_with_asset,
        employed_by_asset,
        political_money_graph_asset,
        
        # Enrichment assets
        committee_classification_asset,
        donor_classification_asset,
        committee_financials_asset,
        canonical_employers_asset,
        employer_clusters_asset,
        employer_cluster_integration_asset,
        corporate_hierarchy_asset,
        wikidata_corporate_resolution,
        
        # Aggregation assets (pre-computed summaries)
        candidate_summaries_asset,
        committee_summaries_asset,
        donor_summaries_asset,
    ],
    resources={
        "arango": arango_resource,
        "embedding": EmbeddingResource(),
    },
    jobs=[
        fec_pipeline_job,    # Complete pipeline: download → parse → graph
        graph_rebuild_job,   # Rebuild graph only (assumes raw data exists)
        raw_data_job,        # Download and parse only (no graph)
        enrichment_job,      # Employer unification + Wikidata resolution
        employer_unification_job,  # Just employer clustering
    ],
    schedules=[
        weekly_pipeline_schedule,   # Full pipeline (download + mapping + aggregation) every Sunday 2 AM
    ],
)
