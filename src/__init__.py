"""Legal Tender - Congressional influence tracking and analysis.

This package contains the Dagster definitions for orchestrating data pipelines
that track political donations, congressional voting, and lobbying activity.

Architecture:
- Assets: Declarative data products (congress members, donor data)
- Jobs: Explicit materialization jobs (not relying on auto-generated __ASSET_JOB)
- Resources: Shared services (MongoDB, APIs)
- Schedules: TODO - Add automated refresh schedules
"""

from dagster import Definitions
from src.assets import (
    congress_members_asset,
    member_donor_data_asset,
    member_fec_mapping_asset,
    data_sync_asset,
    member_financial_summary_asset,
)
from src.jobs import (
    # Current jobs (bulk data approach)
    data_sync_job,
    member_fec_mapping_job,
    member_financial_summary_job,
    bulk_data_pipeline_job,
    # Deprecated jobs (API-based approach)
    congress_pipeline_job,
    donor_pipeline_job,
    full_pipeline_job,
)
from src.schedules import (
    weekly_data_sync_schedule,
    weekly_pipeline_schedule,
)
from src.resources import mongo_resource

# ============================================================================
# DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        congress_members_asset,
        member_donor_data_asset,
        member_fec_mapping_asset,  # NEW: Refactored bulk data approach
        data_sync_asset,  # NEW: Independent data download/sync
        member_financial_summary_asset,  # NEW: Aggregated FEC financial data
    ],
    resources={
        "mongo": mongo_resource,
    },
    jobs=[
        # Current jobs (bulk data approach)
        data_sync_job,
        member_fec_mapping_job,
        member_financial_summary_job,
        bulk_data_pipeline_job,
        # Deprecated jobs (API-based approach, kept for compatibility)
        congress_pipeline_job,
        donor_pipeline_job,
        full_pipeline_job,
    ],
    schedules=[
        weekly_data_sync_schedule,  # Downloads fresh data every Sunday 2 AM
        weekly_pipeline_schedule,   # Full pipeline every Sunday 3 AM
    ],
)
