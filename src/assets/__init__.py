"""Export all assets for use in Dagster definitions."""

# Data sync (downloads all FEC files)
from .data_sync import data_sync_asset

# FEC Data Parsers (Raw → fec_YYYY databases) - Using original FEC file prefixes
from .fec.cn import cn_asset
from .fec.cm import cm_asset
from .fec.ccl import ccl_asset
from .fec.weball import weball_asset
from .fec.webl import webl_asset
from .fec.webk import webk_asset
from .fec.itpas2 import itpas2_asset
from .fec.independent_expenditure import independent_expenditure_asset

# Processed/Aggregated Data (→ legal_tender database)  
from .processed.members import member_fec_mapping_asset
from .processed.donors import member_donor_data_asset
from .processed.congress import congress_members_asset

__all__ = [
    # Data sync
    "data_sync_asset",
    
    # FEC parsers (raw data → fec_YYYY databases) - 8 files, 8 parsers, raw FEC names
    "cn_asset",            # cn.zip - candidate master
    "cm_asset",            # cm.zip - committee master
    "ccl_asset",           # ccl.zip - candidate-committee linkages
    "weball_asset",        # weball.zip - candidate summary (all)
    "webl_asset",          # webl.zip - committee summary
    "webk_asset",          # webk.zip - PAC summary
    "itpas2_asset",        # pas2.zip - itemized transactions (ALL types)
    "independent_expenditure_asset",  # independent_expenditure.csv
    
    # Processed data (aggregated → legal_tender database)
    "member_fec_mapping_asset",
    "member_donor_data_asset",
    "congress_members_asset",
]
