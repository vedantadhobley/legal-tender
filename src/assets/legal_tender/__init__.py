"""Legal Tender Database Assets

Assets that populate the `legal_tender` MongoDB database with aggregated
cross-cycle data. These are the "final" assets that combine data from
multiple lt_YYYY cycle databases.

Collections:
- member_fec_mapping → legal_tender.member_fec_mapping
  Maps Congress members to their FEC candidate IDs and committee IDs
  across all cycles. This drives which members we track in lt_YYYY databases.

- candidate_financials → legal_tender.candidate_financials
  Aggregates ALL money flows TO each tracked candidate across all cycles,
  keeping sources separate with support/oppose breakdowns.

- donor_financials → legal_tender.donor_financials
  Aggregates ALL money flows FROM each committee across all cycles,
  with detailed breakdowns by candidates, categories, and payees.

- candidate_summaries → legal_tender.candidate_summaries
  Aggregates FEC official candidate financial summaries (webl + weball) across
  all cycles, providing official totals for validation against our calculated values.

- committee_summaries → legal_tender.committee_summaries
  Aggregates FEC official committee/PAC financial summaries (webk) across
  all cycles, including independent expenditure totals for validation.

Future:
- member_influence_scores → aggregated influence metrics across all cycles
- pac_relationship_graph → network analysis across all cycles
"""

from .member_fec_mapping import member_fec_mapping_asset
from .candidate_financials import candidate_financials
from .donor_financials import donor_financials
from .candidate_summaries import candidate_summaries
from .committee_summaries import committee_summaries

__all__ = [
    "member_fec_mapping_asset",
    "candidate_financials",
    "donor_financials",
    "candidate_summaries",
    "committee_summaries",
]
