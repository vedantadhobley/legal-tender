"""Legal Tender - Congressional influence tracking and analysis.

This package contains the Dagster definitions for orchestrating data pipelines
that track political donations, congressional voting, and lobbying activity.

Architecture:
- Assets: 
  * fec/ → Raw FEC bulk data parsers (→ fec_YYYY databases)
  * lt/ → Per-cycle computation assets (→ lt_YYYY databases) [TODO]
  * legal_tender/ → Cross-cycle aggregated assets (→ legal_tender database)
- Jobs: One main pipeline job (fec_pipeline_job)
- Resources: MongoDB for data storage
- Schedules: Weekly automated refresh

Data Flow:
  FEC.gov bulk files → fec_2024 (raw) → lt_2024 (computed) → legal_tender (aggregated)
                    → fec_2026 (raw) → lt_2026 (computed) ↗
"""

from dagster import Definitions
from src.assets import (
    # Data sync
    data_sync_asset,
    
    # FEC parsers (raw data → fec_YYYY databases) - 9 files, 9 parsers
    cn_asset,
    cm_asset,
    ccl_asset,
    weball_asset,
    webl_asset,
    webk_asset,
    itpas2_asset,
    oppexp_asset,
    independent_expenditure_asset,
    
    # LT cycle computations (→ lt_{cycle} databases)
    lt_independent_expenditure_asset,
    lt_itpas2_asset,
    lt_oppexp_asset,
    lt_candidate_financials_asset,
    lt_donor_financials_asset,
    
    # Legal tender cross-cycle data (→ legal_tender database)
    member_fec_mapping_asset,
    candidate_financials,
    donor_financials,
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
        
        # FEC raw data parsers (→ fec_YYYY databases) - 9 files, 9 parsers
        cn_asset,
        cm_asset,
        ccl_asset,
        weball_asset,
        webl_asset,
        webk_asset,
        itpas2_asset,
        oppexp_asset,
        independent_expenditure_asset,
        
        # LT cycle computations (→ lt_{cycle} databases)
        lt_independent_expenditure_asset,
        lt_itpas2_asset,
        lt_oppexp_asset,
        lt_candidate_financials_asset,
        lt_donor_financials_asset,
        
        # Legal tender cross-cycle data (→ legal_tender database)
        member_fec_mapping_asset,
        candidate_financials,
        donor_financials,
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
