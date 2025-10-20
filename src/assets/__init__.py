"""Export all assets for use in Dagster definitions."""
# Bulk data assets (active approach)
from src.assets.member_mapping import member_fec_mapping_asset
from src.assets.data_sync import data_sync_asset
from src.assets.financial_summary import member_financial_summary_asset
from src.assets.individual_contributions import individual_contributions_asset
from src.assets.independent_expenditures import independent_expenditures_asset

__all__ = [
    "data_sync_asset",
    "member_fec_mapping_asset",
    "member_financial_summary_asset",
    "individual_contributions_asset",
    "independent_expenditures_asset",
]
