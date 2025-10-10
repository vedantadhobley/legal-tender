"""Legal Tender - Congressional influence tracking and analysis.

This package contains the Dagster definitions for orchestrating data pipelines
that track political donations, congressional voting, and lobbying activity.
"""

from dagster import Definitions
from src.jobs import api_test_job, member_ingestion_job

# Define all Dagster resources, jobs, schedules, and sensors
defs = Definitions(
    jobs=[
        api_test_job,
        member_ingestion_job,
    ],
    # TODO: Add schedules when ready
    # schedules=[...],
    # TODO: Add sensors when ready
    # sensors=[...],
    # TODO: Add resources when ready (MongoDB connection, etc.)
    # resources={...},
)
