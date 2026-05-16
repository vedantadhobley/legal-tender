"""Dagster schedules for Legal Tender pipelines."""

import os

from dagster import ScheduleDefinition, DefaultScheduleStatus
from src.jobs import fec_pipeline_job


# Weekly FEC data refresh — runs every Sunday at 2am Eastern.
#
# Default RUNNING. Without this, dev/prod alike sit on stale FEC data
# indefinitely (we shipped 5+ weeks stale once; it broke trust in
# candidate-level claims for any candidate filing after the last manual
# sync — see docs/decisions.md 2026-05-16). Opt-out via the env var
# if you need to pause it, e.g. during a long-running manual pipeline.
_DEFAULT_STATUS = (
    DefaultScheduleStatus.STOPPED
    if os.environ.get("DAGSTER_SCHEDULES_ENABLED", "").lower() in ("0", "false", "off", "no")
    else DefaultScheduleStatus.RUNNING
)

weekly_pipeline_schedule = ScheduleDefinition(
    job=fec_pipeline_job,
    cron_schedule="0 2 * * 0",  # Every Sunday at 2:00 AM
    name="weekly_fec_refresh",
    default_status=_DEFAULT_STATUS,
    execution_timezone="America/New_York",
)

__all__ = ["weekly_pipeline_schedule"]
