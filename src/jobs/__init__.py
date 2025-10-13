"""Export job definitions for the legal-tender pipeline."""

from src.jobs.asset_jobs import (
    # Current jobs (bulk data approach)
    data_sync_job,
    member_fec_mapping_job,
    bulk_data_pipeline_job,
    # Deprecated jobs (API-based approach)
    congress_pipeline_job,
    donor_pipeline_job,
    full_pipeline_job,
)

__all__ = [
    # Current jobs
    "data_sync_job",
    "member_fec_mapping_job",
    "bulk_data_pipeline_job",
    # Deprecated jobs
    "congress_pipeline_job",
    "donor_pipeline_job",
    "full_pipeline_job",
]
