"""Legal Tender - FEC political money flow analysis via Dagster + ArangoDB.

Pipeline Layers:
  1. Sync     → Download FEC bulk files
  2. FEC      → Parse into fec_YYYY databases  
  3. Graph    → Build vertices & edges in aggregation DB
  4. Enrich   → Classify committees, resolve employers via Wikidata
  5. Aggregate → Compute "Five Pies" funding summaries

Jobs:
  - fec_pipeline_job    → Full refresh (sync → fec → graph)
  - enrichment_job      → Classification + Wikidata resolution
  - aggregation_job     → Five Pies funding summaries
  - upstream_job        → Quick Five Pies refresh only

See docs/PIPELINE.md for full architecture documentation.
"""

from dagster import Definitions
from src.assets import (
    # Layer 1: Sync (downloads FEC bulk files)
    data_sync_asset,
    
    # Layer 2: FEC parsers (raw → fec_YYYY databases)
    cn_asset,
    cm_asset,
    ccl_asset,
    pas2_asset,
    oth_asset,
    indiv_asset,
    
    # Layer 2.5: Mapping (congress API → aggregation)
    member_fec_mapping_asset,
    
    # Layer 3: Graph (vertices + edges → aggregation)
    donors_asset,
    employers_asset,
    contributed_to_asset,
    transferred_to_asset,
    affiliated_with_asset,
    employed_by_asset,
    spent_on_asset,
    political_money_graph_asset,
    
    # Layer 4: Enrichment (classification + Wikidata)
    committee_classification_asset,
    donor_classification_asset,
    committee_financials_asset,
    committee_receipts_asset,
    canonical_employers_asset,
    employer_clusters_asset,
    employer_cluster_integration_asset,
    corporate_hierarchy_asset,
    wikidata_corporate_resolution,
    
    # Layer 5: Aggregation (Five Pies summaries)
    candidate_summaries_asset,
    committee_summaries_asset,
    donor_summaries_asset,
    candidate_upstream_asset,
)
from src.jobs import (
    fec_pipeline_job,        # Full refresh: sync → fec → graph
    graph_rebuild_job,       # Rebuild graph only (no download)
    raw_data_job,            # Download + parse only
    enrichment_job,          # Classification + Wikidata
    aggregation_job,         # Five Pies summaries
    upstream_job,            # Quick Five Pies refresh
    employer_unification_job,  # Just employer clustering
)
from src.schedules import (
    weekly_pipeline_schedule,
)
from src.resources import arango_resource, EmbeddingResource

# Dagster Definitions
defs = Definitions(
    assets=[
        # Layer 1: Sync
        data_sync_asset,
        
        # Layer 2: FEC parsers
        cn_asset,
        cm_asset,
        ccl_asset,
        pas2_asset,
        oth_asset,
        indiv_asset,
        
        # Layer 2.5: Mapping
        member_fec_mapping_asset,
        
        # Layer 3: Graph
        donors_asset,
        employers_asset,
        contributed_to_asset,
        transferred_to_asset,
        affiliated_with_asset,
        employed_by_asset,
        spent_on_asset,
        political_money_graph_asset,
        
        # Layer 4: Enrichment
        committee_classification_asset,
        donor_classification_asset,
        committee_financials_asset,
        committee_receipts_asset,
        canonical_employers_asset,
        employer_clusters_asset,
        employer_cluster_integration_asset,
        corporate_hierarchy_asset,
        wikidata_corporate_resolution,
        
        # Layer 5: Aggregation
        candidate_summaries_asset,
        committee_summaries_asset,
        donor_summaries_asset,
        candidate_upstream_asset,
    ],
    resources={
        "arango": arango_resource,
        "embedding": EmbeddingResource(),
    },
    jobs=[
        fec_pipeline_job,
        graph_rebuild_job,
        raw_data_job,
        enrichment_job,
        aggregation_job,
        upstream_job,
        employer_unification_job,
    ],
    schedules=[
        weekly_pipeline_schedule,
    ],
)
