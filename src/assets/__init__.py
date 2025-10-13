"""Dagster assets for Legal Tender."""
from src.assets.congress import congress_members_asset
from src.assets.donors import member_donor_data_asset
from src.assets.member_mapping import member_fec_mapping_asset
from src.assets.data_sync import data_sync_asset
from src.assets.financial_summary import member_financial_summary_asset

__all__ = [
    "congress_members_asset",
    "member_donor_data_asset",
    "member_fec_mapping_asset",
    "data_sync_asset",
    "member_financial_summary_asset",
]
