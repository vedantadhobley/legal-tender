"""Dagster assets for Legal Tender."""
from src.assets.congress import congress_members_asset
from src.assets.donors import member_donor_data_asset

__all__ = [
    "congress_members_asset",
    "member_donor_data_asset",
]
