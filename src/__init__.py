"""Legal Tender - Congressional influence tracking and analysis.

This package contains the Dagster definitions for orchestrating data pipelines
that track political donations, congressional voting, and lobbying activity.

Architecture:
- Assets: Raw FEC data parsers + Member mapping (bulk data approach)
- Jobs: One main pipeline job (fec_pipeline_job)
- Resources: MongoDB for data storage
- Schedules: Weekly automated refresh
"""

from dagster import Definitions
from src.assets import (
    # Data sync
    data_sync_asset,
    
    # FEC parsers (raw data → fec_YYYY databases) - 8 files, 8 parsers
    cn_asset,
    cm_asset,
    ccl_asset,
    weball_asset,
    webl_asset,
    webk_asset,
    itpas2_asset,
    independent_expenditure_asset,
    
    # Processed data (aggregated → legal_tender database)
    member_fec_mapping_asset,
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
        
        # FEC raw data parsers (→ fec_YYYY databases) - 8 files, 8 parsers
        cn_asset,
        cm_asset,
        ccl_asset,
        weball_asset,
        webl_asset,
        webk_asset,
        itpas2_asset,
        independent_expenditure_asset,
        
        # Processed/aggregated data (→ legal_tender database)
        member_fec_mapping_asset,
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
