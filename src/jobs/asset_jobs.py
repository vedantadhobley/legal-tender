"""Asset materialization jobs for the legal-tender pipeline.

Best Practices Applied:
- Use AssetSelection.keys() or groups() instead of importing asset objects
- Add tags for filtering and organization
- Be explicit with asset selection (avoid .all() in production)
- Include descriptions for observability
"""

from dagster import define_asset_job, AssetSelection

# ============================================================================
# INDIVIDUAL ASSET JOBS
# ============================================================================

congress_pipeline_job = define_asset_job(
    name="congress_pipeline",
    description="Fetch current Congress members from Congress.gov API and sync to MongoDB",
    selection=AssetSelection.keys("congress_members"),
    tags={
        "team": "data-engineering",
        "source": "congress-api",
        "priority": "high",
    },
)

finance_mapping_job = define_asset_job(
    name="finance_mapping",
    description="Map all Congress members to their FEC candidate IDs",
    selection=AssetSelection.keys("congress_members", "member_fec_mapping"),
    tags={
        "team": "data-engineering",
        "source": "openfec-api",
        "priority": "high",
    },
)

finance_pipeline_job = define_asset_job(
    name="finance_pipeline",
    description="Fetch complete financial data for all Congress members (contributions + Super PAC spending)",
    selection=AssetSelection.keys("member_fec_mapping", "member_finance"),
    tags={
        "team": "data-engineering",
        "source": "openfec-api",
        "priority": "medium",
    },
)

# ============================================================================
# FULL PIPELINE JOB
# ============================================================================

full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="Complete data refresh: Congress members + FEC mapping + financial influence data",
    selection=AssetSelection.keys("congress_members", "member_fec_mapping", "member_finance"),
    tags={
        "team": "data-engineering",
        "pipeline": "full-refresh",
        "priority": "high",
        "schedule": "daily",
    },
)
