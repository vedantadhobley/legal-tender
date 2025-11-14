"""Legal Tender - Congressional influence tracking and analysis.

This package contains the Dagster definitions for orchestrating data pipelines
that track political donations, congressional voting, and lobbying activity.

Architecture:
- Assets: 
  * sync/ → Data synchronization (downloads all FEC files)
  * fec/ → Raw FEC bulk data parsers (→ fec_YYYY databases)
  * mapping/ → ID mapping assets (→ aggregation database)
  * enrichment/ → Per-cycle enriched assets (→ enriched_YYYY databases)
  * aggregation/ → Cross-cycle aggregated assets (→ aggregation database)
- Jobs: One main pipeline job (fec_pipeline_job)
- Resources: MongoDB for data storage
- Schedules: Weekly automated refresh

Data Flow:
  FEC.gov bulk files → fec_2024 (raw) → enriched_2024 (filtered) → aggregation (rolled up)
                    → fec_2026 (raw) → enriched_2026 (filtered) ↗
"""

from dagster import Definitions
from src.assets import (
    # Data sync
    data_sync_asset,
    
    # FEC parsers (raw data → fec_YYYY databases) - 6 core files
    cn_asset,
    cm_asset,
    ccl_asset,
    pas2_asset,
    oth_asset,
    indiv_asset,
    
    # Mapping assets (ID mapping → aggregation database)
    member_fec_mapping_asset,
    
    # Enrichment assets (per-cycle enriched data → enriched_{cycle} databases)
    enriched_pas2_asset,
    enriched_candidate_financials_asset,
    enriched_donor_financials_asset,
    enriched_committee_funding_asset,
    
    # Aggregation assets (cross-cycle rollups → aggregation database)
    candidate_financials_asset,
    donor_financials_asset,
)
from src.jobs import fec_pipeline_job
from src.schedules import (
    weekly_pipeline_schedule,
)
from src.resources import mongo_resource

# ============================================================================
# DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        # Data sync (downloads all files)
        data_sync_asset,
        
        # FEC raw data parsers (→ fec_YYYY databases) - 6 core files
        cn_asset,
        cm_asset,
        ccl_asset,
        pas2_asset,
        oth_asset,
        indiv_asset,
        
        # Mapping assets (ID mapping → aggregation database)
        member_fec_mapping_asset,
        
        # Enrichment assets (per-cycle enriched data → enriched_{cycle} databases)
        enriched_pas2_asset,
        enriched_candidate_financials_asset,
        enriched_donor_financials_asset,
        enriched_committee_funding_asset,
        
        # Aggregation assets (cross-cycle rollups → aggregation database)
        candidate_financials_asset,
        donor_financials_asset,
    ],
    resources={
        "mongo": mongo_resource,
    },
    jobs=[
        fec_pipeline_job,  # One job to rule them all
    ],
    schedules=[
        weekly_pipeline_schedule,   # Full pipeline (download + mapping + aggregation) every Sunday 2 AM
    ],
)
