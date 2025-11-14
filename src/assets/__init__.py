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

# FEC Data Parsers (Raw → fec_YYYY databases) - Using official FEC file names
from .fec.cn import cn_asset
from .fec.cm import cm_asset
from .fec.ccl import ccl_asset
from .fec.pas2 import pas2_asset
from .fec.oth import oth_asset
from .fec.indiv import indiv_asset

# DEPRECATED: weball, webl, webk moved to src/assets/deprecated/

# Mapping Assets (fec_{cycle} → aggregation database)
from .mapping.member_fec_mapping import member_fec_mapping_asset

# Enrichment Assets (fec_{cycle} + mappings → enriched_{cycle} databases)
from .enrichment.enriched_pas2 import enriched_pas2_asset
from .enrichment.enriched_candidate_financials import enriched_candidate_financials_asset
from .enrichment.enriched_donor_financials import enriched_donor_financials_asset
from .enrichment.enriched_committee_funding import enriched_committee_funding_asset

# DEPRECATED: enriched_webl, enriched_weball, enriched_webk moved to src/assets/deprecated/

# Aggregation Assets (enriched_{cycle} → aggregation database)
from .aggregation.candidate_financials import candidate_financials_asset
from .aggregation.donor_financials import donor_financials_asset

# DEPRECATED: candidate_summaries, committee_summaries moved to src/assets/deprecated/

__all__ = [
    # Data sync
    "data_sync_asset",
    
    # FEC parsers (raw data → fec_YYYY databases) - 6 core files using official FEC names
    "cn_asset",            # cn.zip - candidate master
    "cm_asset",            # cm.zip - committee master
    "ccl_asset",           # ccl.zip - candidate-committee linkages
    "pas2_asset",          # pas2.zip - itemized transactions (ALL types)
    "oth_asset",           # oth.zip - other receipts (PAC-to-PAC transfers)
    "indiv_asset",         # indiv.zip - individual contributions
    
    # Mapping assets (ID mapping → aggregation database)
    "member_fec_mapping_asset",
    
    # Enrichment assets (per-cycle enriched data → enriched_{cycle} databases)
    "enriched_pas2_asset",
    "enriched_candidate_financials_asset",
    "enriched_donor_financials_asset",
    "enriched_committee_funding_asset",
    
    # Aggregation assets (cross-cycle rollups → aggregation database)
    "candidate_financials_asset",
    "donor_financials_asset",
]
