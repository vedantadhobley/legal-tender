"""Export all assets for use in Dagster definitions.

Asset Organization (matches MongoDB structure):
- sync/ → Data synchronization (downloads all FEC files)
- fec/ → Raw FEC bulk data parsers (→ fec_YYYY databases)
- mapping/ → ID mapping assets (member bioguide → FEC IDs, etc.)
- enrichment/ → Per-cycle enriched assets (→ enriched_YYYY databases)
- aggregation/ → Cross-cycle aggregated assets (→ aggregation database)
"""

# Data sync (downloads all FEC files)
from .sync.data_sync import data_sync_asset

# FEC Data Parsers (Raw → fec_YYYY databases) - Using original FEC file prefixes
from .fec.cn import cn_asset
from .fec.cm import cm_asset
from .fec.ccl import ccl_asset
from .fec.weball import weball_asset
from .fec.webl import webl_asset
from .fec.webk import webk_asset
from .fec.itpas2 import itpas2_asset
from .fec.oppexp import oppexp_asset

# Mapping Assets (fec_{cycle} → aggregation database)
from .mapping.member_fec_mapping import member_fec_mapping_asset

# Enrichment Assets (fec_{cycle} + mappings → enriched_{cycle} databases)
from .enrichment.enriched_itpas2 import enriched_itpas2_asset
from .enrichment.enriched_oppexp import enriched_oppexp_asset
from .enrichment.enriched_candidate_financials import enriched_candidate_financials_asset
from .enrichment.enriched_donor_financials import enriched_donor_financials_asset
from .enrichment.enriched_webl import enriched_webl_asset
from .enrichment.enriched_weball import enriched_weball_asset
from .enrichment.enriched_webk import enriched_webk_asset
from .enrichment.enriched_committee_funding import enriched_committee_funding_asset

# Aggregation Assets (enriched_{cycle} → aggregation database)
from .aggregation.candidate_financials import candidate_financials_asset
from .aggregation.donor_financials import donor_financials_asset
from .aggregation.candidate_summaries import candidate_summaries_asset
from .aggregation.committee_summaries import committee_summaries_asset

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
    
    # Mapping assets (ID mapping → aggregation database)
    "member_fec_mapping_asset",
    
    # Enrichment assets (per-cycle enriched data → enriched_{cycle} databases)
    "enriched_itpas2_asset",
    "enriched_oppexp_asset",
    "enriched_candidate_financials_asset",
    "enriched_donor_financials_asset",
    "enriched_webl_asset",
    "enriched_weball_asset",
    "enriched_webk_asset",
    "enriched_committee_funding_asset",
    
    # Aggregation assets (cross-cycle rollups → aggregation database)
    "candidate_financials_asset",
    "donor_financials_asset",
    "candidate_summaries_asset",
    "committee_summaries_asset",
]
