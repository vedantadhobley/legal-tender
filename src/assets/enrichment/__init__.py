"""Enrichment assets - add derived fields to graph vertices."""

from .committee_classification import committee_classification_asset
from .donor_classification import donor_classification_asset
from .committee_financials import committee_financials_asset

__all__ = [
    "committee_classification_asset",
    "donor_classification_asset",
    "committee_financials_asset",
]
