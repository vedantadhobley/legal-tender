"""Aggregation Database Assets

Assets that populate the `aggregation` MongoDB database with aggregated
cross-cycle data. These are the "final" assets that combine data from
multiple enriched_YYYY cycle databases.

Collections:
- candidate_financials → aggregation.candidate_financials
  Aggregates ALL money flows TO each tracked candidate across all cycles,
  keeping sources separate with support/oppose breakdowns.

- donor_financials → aggregation.donor_financials
  Aggregates ALL money flows FROM each committee across all cycles,
  with detailed breakdowns by candidates, categories, and payees.

- candidate_summaries → aggregation.candidate_summaries
  Aggregates FEC official candidate financial summaries (webl + weball) across
  all cycles, providing official totals for validation against our calculated values.

- committee_summaries → aggregation.committee_summaries
  Aggregates FEC official committee/PAC financial summaries (webk) across
  all cycles, including independent expenditure totals for validation.

Future:
- member_influence_scores → aggregated influence metrics across all cycles
- pac_relationship_graph → network analysis across all cycles
"""

from .candidate_financials import candidate_financials_asset
from .donor_financials import donor_financials_asset
from .candidate_summaries import candidate_summaries_asset
from .committee_summaries import committee_summaries_asset

__all__ = [
    "candidate_financials_asset",
    "donor_financials_asset",
    "candidate_summaries_asset",
    "committee_summaries_asset",
]
