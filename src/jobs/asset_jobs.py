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
# FEC MAPPING JOB (NEW - REFACTORED APPROACH)
# ============================================================================

member_fec_mapping_job = define_asset_job(
    name="member_fec_mapping",
    description="Build complete member->FEC mapping from legislators file + FEC bulk data + ProPublica API",
    selection=AssetSelection.keys("member_fec_mapping"),
    tags={
        "team": "data-engineering",
        "source": "bulk-data",
        "priority": "high",
        "approach": "hybrid-bulk",
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

# ============================================================================
# DATA SYNC JOB (DOWNLOADS ONLY)
# ============================================================================

data_sync_job = define_asset_job(
    name="data_sync",
    description="Download and sync all external data sources (legislators, FEC bulk data)",
    selection=AssetSelection.keys("data_sync"),
    tags={
        "team": "data-engineering",
        "type": "download",
        "priority": "high",
        "schedule": "weekly-sunday",
    },
)

# ============================================================================
# NEW REFACTORED PIPELINE (BULK DATA APPROACH)
# ============================================================================

refactored_pipeline_job = define_asset_job(
    name="refactored_pipeline",
    description="Complete pipeline: Download data (data_sync) + Build member FEC mapping",
    selection=AssetSelection.keys("data_sync", "member_fec_mapping"),
    tags={
        "team": "data-engineering",
        "pipeline": "refactored",
        "priority": "high",
        "approach": "bulk-data",
        "schedule": "weekly-sunday",
    },
)
