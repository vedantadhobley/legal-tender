"""Asset materialization jobs for the legal-tender pipeline.

CURRENT APPROACH (Bulk Data):
- data_sync_job: Downloads legislators + FEC bulk files with smart caching
- member_fec_mapping_job: Builds member->FEC mapping from downloaded data
- bulk_data_pipeline_job: Full pipeline (download + mapping)

DEPRECATED (API-based, kept for compatibility):
- congress_pipeline_job: Old ProPublica API approach
- donor_pipeline_job: Old OpenFEC API approach  
- full_pipeline_job: Old combined pipeline

Best Practices Applied:
- Use AssetSelection.keys() or groups() instead of importing asset objects
- Add tags for filtering and organization
- Be explicit with asset selection (avoid .all() in production)
- Include descriptions for observability
"""

from dagster import define_asset_job, AssetSelection

# ============================================================================
# CURRENT JOBS (BULK DATA APPROACH)
# ============================================================================

data_sync_job = define_asset_job(
    name="data_sync_job",
    description="Download and sync all external data sources (legislators, FEC bulk data)",
    selection=AssetSelection.keys("data_sync"),
    tags={
        "team": "data-engineering",
        "type": "download",
        "priority": "high",
        "schedule": "weekly-sunday",
        "status": "active",
    },
)

member_fec_mapping_job = define_asset_job(
    name="member_fec_mapping_job",
    description="Build complete member->FEC mapping from legislators file + FEC bulk data + ProPublica API",
    selection=AssetSelection.keys("member_fec_mapping"),
    tags={
        "team": "data-engineering",
        "source": "bulk-data",
        "priority": "high",
        "approach": "hybrid-bulk",
        "status": "active",
    },
)

bulk_data_pipeline_job = define_asset_job(
    name="bulk_data_pipeline_job",
    description="Complete pipeline: Download data (data_sync) + Build member FEC mapping",
    selection=AssetSelection.keys("data_sync", "member_fec_mapping"),
    tags={
        "team": "data-engineering",
        "pipeline": "bulk-data",
        "priority": "high",
        "schedule": "weekly-sunday",
        "status": "active",
    },
)

# ============================================================================
# DEPRECATED JOBS (API-BASED APPROACH - KEPT FOR COMPATIBILITY)
# ============================================================================
# These jobs use the old API-based approach (ProPublica + OpenFEC APIs).
# They are kept for backward compatibility but should be migrated to the
# bulk data approach for better performance and lower API usage.
# ============================================================================

congress_pipeline_job = define_asset_job(
    name="congress_pipeline",
    description="[DEPRECATED] Fetch current Congress members from Congress.gov API and sync to MongoDB",
    selection=AssetSelection.keys("congress_members"),
    tags={
        "team": "data-engineering",
        "source": "congress-api",
        "priority": "low",
        "status": "deprecated",
    },
)

donor_pipeline_job = define_asset_job(
    name="donor_pipeline",
    description="[DEPRECATED] Fetch donor contributions for all Congress members from OpenFEC API",
    selection=AssetSelection.keys("member_donor_data"),
    tags={
        "team": "data-engineering",
        "source": "openfec-api",
        "priority": "low",
        "depends_on": "congress_members",
        "status": "deprecated",
    },
)

full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="[DEPRECATED] Refresh all congressional data: members + donor contributions (in dependency order)",
    selection=AssetSelection.keys("congress_members", "member_donor_data"),
    tags={
        "team": "data-engineering",
        "pipeline": "api-based",
        "priority": "low",
        "status": "deprecated",
    },
)
