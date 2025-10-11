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
from src.assets import congress_members_asset, member_donor_data_asset
from src.jobs import (
    congress_pipeline_job,
    donor_pipeline_job,
    full_pipeline_job,
)
from src.resources import mongo_resource

# ============================================================================
# DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        congress_members_asset,
        member_donor_data_asset,
    ],
    resources={
        "mongo": mongo_resource,
    },
    jobs=[
        congress_pipeline_job,
        donor_pipeline_job,
        full_pipeline_job,
    ],
    
    # TODO: Add schedules for automated execution
    # schedules=[
    #     ScheduleDefinition(
    #         job=full_pipeline_job,
    #         cron_schedule="0 2 * * *",  # Daily at 2 AM
    #         default_status=DefaultScheduleStatus.RUNNING,
    #     ),
    # ],
    
    # TODO: Add sensors for event-driven execution
    # sensors=[...],
)
