"""Aggregation assets - pre-computed summaries from graph traversals.

These assets compute derived data that would be expensive to calculate
on-the-fly for UI queries. Unlike enrichments (which add fields to 
existing documents), aggregations create new computed summaries.

Assets:
- candidate_summaries: Pre-computed funding totals, top donors, breakdowns by cycle
- committee_summaries: Committee profiles with funding sources and recipients
- donor_summaries: Whale-tier donor profiles with political lean and recipients
"""

from src.assets.aggregation.candidate_summaries import candidate_summaries_asset
from src.assets.aggregation.committee_summaries import committee_summaries_asset
from src.assets.aggregation.donor_summaries import donor_summaries_asset

__all__ = [
    "candidate_summaries_asset",
    "committee_summaries_asset",
    "donor_summaries_asset",
]
