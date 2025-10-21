"""Export all assets for use in Dagster definitions."""

# Data sync (downloads all FEC files)
from .data_sync import data_sync_asset

# FEC Data Parsers (Raw → fec_YYYY databases) - ONE FILE = ONE PARSER
from .fec.candidates import candidates_asset
from .fec.committees import committees_asset
from .fec.linkages import linkages_asset
from .fec.candidate_summaries import candidate_summaries_asset
from .fec.committee_summaries import committee_summaries_asset
from .fec.pac_summaries import pac_summaries_asset
from .fec.committee_transfers import committee_transfers_asset
from .fec.independent_expenditures import independent_expenditures_asset

# Processed/Aggregated Data (→ legal_tender database)  
from .processed.members import member_fec_mapping_asset
from .processed.donors import member_donor_data_asset
from .processed.congress import congress_members_asset

__all__ = [
    # Data sync
    "data_sync_asset",
    
    # FEC parsers (raw data → fec_YYYY databases) - 8 files, 8 parsers
    "candidates_asset",
    "committees_asset",
    "linkages_asset",
    "candidate_summaries_asset",
    "committee_summaries_asset",
    "pac_summaries_asset",
    "committee_transfers_asset",
    "independent_expenditures_asset",
    
    # Processed data (aggregated → legal_tender database)
    "member_fec_mapping_asset",
    "member_donor_data_asset",
    "congress_members_asset",
]
