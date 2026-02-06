"""Enrichment assets - add derived fields to graph vertices."""

from .committee_classification import committee_classification_asset
from .donor_classification import donor_classification_asset
from .committee_financials import committee_financials_asset
from .committee_receipts import committee_receipts_asset
from .canonical_employers import canonical_employers_asset
from .employer_clustering import employer_clusters_asset
from .employer_cluster_integration import employer_cluster_integration_asset
from .wikidata_resolution import wikidata_corporate_resolution
from .corporate_hierarchy import corporate_hierarchy_asset

__all__ = [
    "committee_classification_asset",
    "donor_classification_asset",
    "committee_financials_asset",
    "committee_receipts_asset",
    "canonical_employers_asset",
    "employer_clusters_asset",
    "employer_cluster_integration_asset",
    "wikidata_corporate_resolution",
    "corporate_hierarchy_asset",
]
