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

member_financial_summary_job = define_asset_job(
    name="member_financial_summary_job",
    description="Load aggregated financial summaries (total raised/spent/cash) for all members from FEC pas2 files",
    selection=AssetSelection.keys("member_financial_summary"),
    tags={
        "team": "data-engineering",
        "source": "fec-pas2",
        "priority": "high",
        "status": "active",
    },
)

individual_contributions_job = define_asset_job(
    name="individual_contributions_job",
    description="Parse and store individual contributions (direct donations from people with employer data)",
    selection=AssetSelection.keys("individual_contributions"),
    tags={
        "team": "data-engineering",
        "source": "fec-indiv",
        "priority": "high",
        "status": "active",
        "phase": "4",
    },
)

independent_expenditures_job = define_asset_job(
    name="independent_expenditures_job",
    description="Parse and store independent expenditures (Super PAC spending FOR/AGAINST candidates with negative values for opposition)",
    selection=AssetSelection.keys("independent_expenditures"),
    tags={
        "team": "data-engineering",
        "source": "fec-oppexp",
        "priority": "high",
        "status": "active",
        "phase": "4",
    },
)

bulk_data_pipeline_job = define_asset_job(
    name="bulk_data_pipeline_job",
    description="Complete pipeline: Download data + Build member FEC mapping + Load financial summaries + Individual contributions + Independent expenditures",
    selection=AssetSelection.keys(
        "data_sync",
        "member_fec_mapping",
        "member_financial_summary",
        "individual_contributions",
        "independent_expenditures"
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "bulk-data",
        "priority": "high",
        "schedule": "weekly-sunday",
        "status": "active",
    },
)

# ============================================================================
# DEPRECATED JOBS REMOVED
# ============================================================================
# The old API-based jobs (congress_pipeline, donor_pipeline, full_pipeline)
# have been removed in favor of the bulk data approach which is:
# - Faster (no rate limits)
# - More complete (bulk files have more data)
# - More reliable (no API dependencies)
# 
# Old assets (congress_members_asset, member_donor_data_asset) are still in
# src/assets/ but are no longer registered in Dagster definitions.
# ============================================================================
