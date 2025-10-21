"""Processed Data Assets

Aggregated/processed data assets that build analytics from raw FEC data.
Stores in the `legal_tender` MongoDB database for cross-cycle analysis.

Collections:
- members.py → legal_tender.members (politicians with FEC IDs)
- donors.py → legal_tender.donors (future - corporate PAC influence scores)
- congress.py → legal_tender.bills, votes, etc (future)
"""

from .members import member_fec_mapping_asset
from .donors import member_donor_data_asset
from .congress import congress_members_asset

__all__ = [
    "member_fec_mapping_asset",
    "member_donor_data_asset",
    "congress_members_asset",
]
