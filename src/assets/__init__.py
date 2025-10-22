"""Export all assets for use in Dagster definitions.

Asset Organization (matches MongoDB structure):
- fec/ → Raw FEC bulk data parsers (→ fec_YYYY databases)
- lt/ → Per-cycle computation assets (→ lt_YYYY databases)
- legal_tender/ → Cross-cycle aggregated assets (→ legal_tender database)
"""

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
from .fec.oppexp import oppexp_asset
from .fec.independent_expenditure import independent_expenditure_asset

# LT Cycle Computations (fec_{cycle} + member_fec_mapping → lt_{cycle} databases)
from .lt.lt_independent_expenditure import lt_independent_expenditure_asset
from .lt.lt_itpas2 import lt_itpas2_asset
from .lt.lt_oppexp import lt_oppexp_asset
from .lt.lt_candidate_financials import lt_candidate_financials_asset
from .lt.lt_donor_financials import lt_donor_financials_asset

# Legal Tender Cross-Cycle Assets (lt_YYYY → legal_tender database)
from .legal_tender.member_fec_mapping import member_fec_mapping_asset
from .legal_tender.candidate_financials import candidate_financials
from .legal_tender.donor_financials import donor_financials

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
    "oppexp_asset",        # oppexp.zip - operating expenditures
    "independent_expenditure_asset",  # independent_expenditure.csv
    
    # LT cycle computations (→ lt_{cycle} databases)
    "lt_independent_expenditure_asset",
    "lt_itpas2_asset",
    "lt_oppexp_asset",
    "lt_candidate_financials_asset",
    "lt_donor_financials_asset",
    
    # Legal tender cross-cycle data (aggregated → legal_tender database)
    "member_fec_mapping_asset",
    "candidate_financials",
    "donor_financials",
]
