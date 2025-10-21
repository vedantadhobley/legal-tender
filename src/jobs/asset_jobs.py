"""Asset materialization jobs for the legal-tender pipeline.

Philosophy:
- Individual assets can be materialized directly in the UI
- Jobs are for scheduled/orchestrated workflows only
- Keep it simple: one main pipeline job

Available Job:
- fec_pipeline_job: Complete FEC data pipeline (download → mapping → parse all 8 files)

All assets are accessible directly in the UI for ad-hoc materialization.
"""

from dagster import define_asset_job, AssetSelection

# ============================================================================
# MAIN PIPELINE JOB
# ============================================================================

fec_pipeline_job = define_asset_job(
    name="fec_pipeline_job",
    description="Complete FEC data pipeline: Download → Member mapping → Parse all 8 FEC files",
    selection=AssetSelection.keys(
        # Phase 1: Download all source data
        "data_sync",
        
        # Phase 2: Build member→FEC mapping
        "member_fec_mapping",
        
        # Phase 3: Parse all 8 FEC files into per-year databases (using raw FEC names)
        "cn",                       # cn.zip - candidate master
        "cm",                       # cm.zip - committee master
        "ccl",                      # ccl.zip - candidate-committee linkages
        "weball",                   # weball.zip - candidate summary (all)
        "webl",                     # webl.zip - committee summary
        "webk",                     # webk.zip - PAC summary
        "itpas2",                   # pas2.zip - itemized transactions (ALL types)
        "independent_expenditure",  # independent_expenditure.csv
    ),
    tags={
        "team": "data-engineering",
        "pipeline": "fec-complete",
        "priority": "high",
        "schedule": "weekly-sunday",
    },
)
