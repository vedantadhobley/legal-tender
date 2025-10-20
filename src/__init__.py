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
    # Bulk data assets (active)
    member_fec_mapping_asset,
    data_sync_asset,
    member_financial_summary_asset,
    individual_contributions_asset,
    independent_expenditures_asset,
)
from src.jobs import (
    # Current jobs (bulk data approach)
    data_sync_job,
    member_fec_mapping_job,
    member_financial_summary_job,
    individual_contributions_job,
    independent_expenditures_job,
    bulk_data_pipeline_job,
)
from src.schedules import (
    weekly_pipeline_schedule,
)
from src.resources import mongo_resource

# ============================================================================
# DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        # Active assets (bulk data approach)
        data_sync_asset,  # Downloads legislators + FEC bulk files
        member_fec_mapping_asset,  # Builds member→FEC mapping
        member_financial_summary_asset,  # Aggregated financial summaries
        individual_contributions_asset,  # Individual donations (with employer data)
        independent_expenditures_asset,  # Super PAC spending (with negative values for opposition)
    ],
    resources={
        "mongo": mongo_resource,
    },
    jobs=[
        # Bulk data jobs (active)
        data_sync_job,
        member_fec_mapping_job,
        member_financial_summary_job,
        individual_contributions_job,
        independent_expenditures_job,
        bulk_data_pipeline_job,
    ],
    schedules=[
        weekly_pipeline_schedule,   # Full pipeline (download + mapping + aggregation) every Sunday 2 AM
    ],
)
