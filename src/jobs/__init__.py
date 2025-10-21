"""Export job definitions for the legal-tender pipeline."""

from src.jobs.asset_jobs import fec_pipeline_job

__all__ = [
    "fec_pipeline_job",
]
