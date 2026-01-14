"""Export all assets for use in Dagster definitions.

Asset Organization (matches ArangoDB structure):
- sync/ → Data synchronization (downloads all FEC files)
- fec/ → Raw FEC bulk data parsers (→ fec_YYYY databases) - ALL CONVERTED TO ARANGODB
- mapping/ → ID mapping assets (member bioguide → FEC IDs, etc.) - CONVERTED TO ARANGODB
- graph/ → Graph vertices and edges (→ aggregation database) - NEW GRAPH LAYER
- enrichment/ → Derived fields and classifications
"""

# Data sync (downloads all FEC files)
from .sync.data_sync import data_sync_asset

# FEC Data Parsers (Raw → fec_YYYY databases) - Using official FEC file names
# ALL CONVERTED TO ARANGO:
from .fec.cn import cn_asset
from .fec.cm import cm_asset
from .fec.ccl import ccl_asset
from .fec.pas2 import pas2_asset
from .fec.oth import oth_asset
from .fec.indiv import indiv_asset

# Mapping Assets (fec_{cycle} → aggregation database)
# CONVERTED TO ARANGO:
from .mapping.member_fec_mapping import member_fec_mapping_asset

# Graph Assets (→ aggregation database)
# NEW GRAPH LAYER:
from .graph.donors import donors_asset
from .graph.employers import employers_asset
from .graph.contributed_to import contributed_to_asset
from .graph.transferred_to import transferred_to_asset
from .graph.affiliated_with import affiliated_with_asset
from .graph.employed_by import employed_by_asset
from .graph.spent_on import spent_on_asset
from .graph.political_money_graph import political_money_graph_asset

# Enrichment Assets (→ aggregation database)
# Derived fields and classifications:
from .enrichment.committee_classification import committee_classification_asset
from .enrichment.donor_classification import donor_classification_asset
from .enrichment.committee_financials import committee_financials_asset
from .enrichment.committee_receipts import committee_receipts_asset
from .enrichment.canonical_employers import canonical_employers_asset
from .enrichment.employer_clustering import employer_clusters_asset
from .enrichment.employer_cluster_integration import employer_cluster_integration_asset
from .enrichment.corporate_hierarchy import corporate_hierarchy_asset
from .enrichment.wikidata_resolution import wikidata_corporate_resolution

# Aggregation Assets (→ aggregation database)
# Pre-computed summaries for UI/RAG:
from .aggregation.candidate_summaries import candidate_summaries_asset
from .aggregation.committee_summaries import committee_summaries_asset
from .aggregation.donor_summaries import donor_summaries_asset
from .aggregation.candidate_upstream import candidate_upstream_asset

__all__ = [
    # Data sync
    "data_sync_asset",
    
    # FEC parsers (raw data → fec_YYYY databases) - ALL Converted to ArangoDB
    "cn_asset",            # cn.zip - candidate master
    "cm_asset",            # cm.zip - committee master
    "ccl_asset",           # ccl.zip - candidate-committee linkages
    "pas2_asset",          # pas2.zip - itemized transactions (ALL types)
    "oth_asset",           # oth.zip - other receipts (PAC-to-PAC transfers)
    "indiv_asset",         # indiv.zip - individual contributions
    
    # Mapping assets - Converted to ArangoDB
    "member_fec_mapping_asset",
    
    # Graph assets (vertices + edges → aggregation database)
    "donors_asset",
    "employers_asset",
    "contributed_to_asset",
    "transferred_to_asset",
    "affiliated_with_asset",
    "employed_by_asset",
    "spent_on_asset",
    "political_money_graph_asset",
    
    # Enrichment assets
    "committee_classification_asset",
    "donor_classification_asset",
    "committee_financials_asset",
    "committee_receipts_asset",
    "canonical_employers_asset",
    "employer_clusters_asset",
    "employer_cluster_integration_asset",
    "corporate_hierarchy_asset",
    "wikidata_corporate_resolution",
    
    # Aggregation assets (pre-computed summaries)
    "candidate_summaries_asset",
    "committee_summaries_asset",
    "donor_summaries_asset",
    "candidate_upstream_asset",
]
