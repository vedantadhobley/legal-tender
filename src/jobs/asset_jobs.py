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

donor_pipeline_job = define_asset_job(
    name="donor_pipeline",
    description="Fetch donor contributions for all Congress members from OpenFEC API",
    selection=AssetSelection.keys("member_donor_data"),
    tags={
        "team": "data-engineering",
        "source": "openfec-api",
        "priority": "medium",
        "depends_on": "congress_members",
    },
)

# ============================================================================
# FULL PIPELINE JOB
# ============================================================================

full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="Refresh all congressional data: members + donor contributions (in dependency order)",
    # Be explicit about which assets instead of using .all()
    # This prevents accidentally materializing future assets
    selection=AssetSelection.keys("congress_members", "member_donor_data"),
    tags={
        "team": "data-engineering",
        "pipeline": "full-refresh",
        "priority": "high",
        "schedule": "daily",
    },
)
